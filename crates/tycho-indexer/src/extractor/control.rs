use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{
    mpsc::{self, error::SendError, Receiver, Sender},
    Mutex,
};
use tracing::{debug, error, info, instrument, trace};
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

pub(crate) struct RuntimeControlWiring {
    pub(crate) control_rx: Receiver<ControlMessage>,
    pub(crate) handles: Vec<ExtractorHandle>,
}

pub(crate) fn new_control_channel(
) -> (Sender<ControlMessage>, Receiver<ControlMessage>) {
    mpsc::channel(128)
}

pub(crate) fn new_subscriptions_map() -> Arc<Mutex<SubscriptionsMap>> {
    Arc::new(Mutex::new(HashMap::new()))
}

pub(crate) fn new_branch_subscriptions_map(
    branch_protocol_systems: impl IntoIterator<Item = impl Into<String>>,
) -> BranchSubscriptionsMap {
    branch_protocol_systems
        .into_iter()
        .map(|protocol_system| (protocol_system.into(), new_subscriptions_map()))
        .collect()
}

pub(crate) fn build_extractor_handles(
    ids: impl IntoIterator<Item = ExtractorIdentity>,
    control_tx: &Sender<ControlMessage>,
) -> Vec<ExtractorHandle> {
    ids.into_iter()
        .map(|id| ExtractorHandle::new(id, control_tx.clone()))
        .collect()
}

pub(crate) fn build_runtime_control_handles(
    ids: impl IntoIterator<Item = ExtractorIdentity>,
) -> (Receiver<ControlMessage>, Vec<ExtractorHandle>) {
    let (control_tx, control_rx) = new_control_channel();
    let handles = build_extractor_handles(ids, &control_tx);
    (control_rx, handles)
}

pub(crate) fn build_runtime_control_wiring(
    ids: impl IntoIterator<Item = ExtractorIdentity>,
) -> RuntimeControlWiring {
    let (control_rx, handles) = build_runtime_control_handles(ids);
    RuntimeControlWiring { control_rx, handles }
}

pub(crate) fn allocate_subscriber_id(next_subscriber_id: &mut u64) -> u64 {
    let subscriber_id = *next_subscriber_id;
    *next_subscriber_id += 1;
    subscriber_id
}

pub(crate) async fn register_subscription(
    subscribers: &Arc<Mutex<SubscriptionsMap>>,
    subscriber_id: u64,
    sender: Sender<ExtractorMsg>,
) {
    subscribers
        .lock()
        .await
        .insert(subscriber_id, sender);
}

pub(crate) fn allocate_logged_subscription_id(
    next_subscriber_id: &mut u64,
    extractor_id: Option<&ExtractorIdentity>,
    message: &str,
) -> u64 {
    let subscriber_id = allocate_subscriber_id(next_subscriber_id);
    log_new_subscription(subscriber_id, extractor_id, message);
    subscriber_id
}

pub(crate) async fn register_logged_subscription(
    subscribers: &Arc<Mutex<SubscriptionsMap>>,
    next_subscriber_id: &mut u64,
    sender: Sender<ExtractorMsg>,
    extractor_id: Option<&ExtractorIdentity>,
    message: &str,
) -> u64 {
    let subscriber_id = allocate_logged_subscription_id(next_subscriber_id, extractor_id, message);
    register_subscription(subscribers, subscriber_id, sender).await;
    subscriber_id
}

pub(crate) fn log_new_subscription(
    subscriber_id: u64,
    extractor_id: Option<&ExtractorIdentity>,
    message: &str,
) {
    tracing::Span::current().record("subscriber_id", subscriber_id);
    match extractor_id {
        Some(extractor_id) => info!(?subscriber_id, ?extractor_id, "{message}"),
        None => info!(?subscriber_id, "{message}"),
    }
}

#[instrument(skip_all, fields(subscriber_count))]
pub(crate) async fn propagate_subscription_message(
    subscribers: &Arc<Mutex<SubscriptionsMap>>,
    message: ExtractorMsg,
) {
    trace!(msg = %message, "Propagating message to subscribers.");
    let arced_message = message;

    let mut to_remove = Vec::new();
    let mut subscribers = subscribers.lock().await;
    tracing::Span::current().record("subscriber_count", subscribers.len());

    for (counter, sender) in subscribers.iter_mut() {
        match sender.send(arced_message.clone()).await {
            Ok(_) => {
                trace!(subscriber_id = %counter, "Message sent successfully.");
            }
            Err(err) => {
                to_remove.push(*counter);
                error!(error = %err, counter, "Error while sending message to subscriber");
            }
        }
    }

    for counter in to_remove {
        subscribers.remove(&counter);
        debug!("Subscriber {} has been dropped", counter);
    }
}
