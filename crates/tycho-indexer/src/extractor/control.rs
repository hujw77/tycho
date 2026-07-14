use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{
    mpsc::{self, error::SendError, Receiver, Sender},
    Mutex,
};
use tracing::instrument;
use tycho_common::models::ExtractorIdentity;

use crate::extractor::{ExtractionError, ExtractorMsg};

pub enum ControlMessage {
    Stop,
    Subscribe { extractor_id: ExtractorIdentity, sender: Sender<ExtractorMsg> },
}

/// A trait for a message sender that can be used to subscribe to messages
///
/// Extracted out of the [ExtractorHandle] to allow for easier testing
#[async_trait]
pub trait MessageSender: Send + Sync {
    async fn subscribe(&self) -> Result<Receiver<ExtractorMsg>, SendError<ControlMessage>>;
}

#[derive(Clone)]
pub struct ExtractorHandle {
    id: ExtractorIdentity,
    control_tx: Sender<ControlMessage>,
}

impl ExtractorHandle {
    pub(crate) fn new(id: ExtractorIdentity, control_tx: Sender<ControlMessage>) -> Self {
        Self { id, control_tx }
    }

    pub fn get_id(&self) -> ExtractorIdentity {
        self.id.clone()
    }

    #[instrument(skip(self))]
    pub async fn stop(&self) -> Result<(), ExtractionError> {
        self.control_tx
            .send(ControlMessage::Stop)
            .await
            .map_err(|err| ExtractionError::Unknown(err.to_string()))
    }
}

#[async_trait]
impl MessageSender for ExtractorHandle {
    #[instrument(skip(self))]
    async fn subscribe(&self) -> Result<Receiver<ExtractorMsg>, SendError<ControlMessage>> {
        let (tx, rx) = mpsc::channel(16);
        let timeout_duration = std::time::Duration::from_secs(5);

        let send_result = tokio::time::timeout(
            timeout_duration,
            self.control_tx
                .send(ControlMessage::Subscribe { extractor_id: self.id.clone(), sender: tx }),
        )
        .await;

        match send_result {
            Ok(Ok(())) => Ok(rx),
            Ok(Err(e)) => Err(e),
            Err(_) => panic!("Subscription timed out!"),
        }
    }
}

pub(crate) type SubscriptionsMap = HashMap<u64, Sender<ExtractorMsg>>;
pub(crate) type BranchSubscriptionsMap = HashMap<String, Arc<Mutex<SubscriptionsMap>>>;
