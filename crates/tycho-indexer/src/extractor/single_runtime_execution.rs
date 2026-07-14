use async_trait::async_trait;
use std::sync::Arc;

use metrics::gauge;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument};
use tycho_common::models::blockchain::BlockAggregatedChanges;

use crate::{
    extractor::{
        control::{ControlMessage, SubscriptionsMap},
        execution_loop::{
            handle_runtime_control_message, handle_runtime_stream_item,
            continue_after_control_action, continue_after_stream_action, runtime_step_select,
            runtime_block_response_dispatch, spawn_managed_runtime_loop, RuntimeLoopControlFlow,
        },
        ExtractionError, Extractor, ExtractorMsg,
    },
    pb::sf::substreams::rpc::v2::BlockScopedData,
    substreams::stream::{BlockResponse, SubstreamsStream},
};

pub struct ExtractorRunner {
    extractor: Arc<dyn Extractor>,
    substreams: SubstreamsStream,
    subscriptions: Arc<Mutex<SubscriptionsMap>>,
    next_subscriber_id: u64,
    control_rx: Receiver<ControlMessage>,
    /// Handle of the tokio runtime on which the extraction tasks will be run.
    /// If 'None' the default runtime will be used.
    runtime_handle: Option<Handle>,
    partial_blocks: bool,
}

impl ExtractorRunner {
    pub fn new(
        extractor: Arc<dyn Extractor>,
        substreams: SubstreamsStream,
        subscriptions: Arc<Mutex<SubscriptionsMap>>,
        control_rx: Receiver<ControlMessage>,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
    ) -> Self {
        ExtractorRunner {
            extractor,
            substreams,
            subscriptions,
            next_subscriber_id: 0,
            control_rx,
            runtime_handle,
            partial_blocks,
        }
    }

    pub fn run(self) -> JoinHandle<Result<(), ExtractionError>> {
        info!("Extractor {} started!", self.extractor.get_id());

        let runtime_handle = self.runtime_handle.clone();
        spawn_managed_runtime_loop(runtime_handle, SingleRuntimeLoopRunner::new(self))
    }

    #[instrument(skip_all)]
    async fn subscribe(&mut self, sender: Sender<ExtractorMsg>) {
        let subscriber_id = self.next_subscriber_id;
        self.next_subscriber_id += 1;
        tracing::Span::current().record("subscriber_id", subscriber_id);
        info!(?subscriber_id, "New subscription");
        self.subscriptions
            .lock()
            .await
            .insert(subscriber_id, sender);
    }

    #[instrument(skip_all, fields(partial_blocks_enabled, is_partial = data.is_partial))]
    pub(crate) async fn process_block_data(
        extractor: &dyn Extractor,
        data: &BlockScopedData,
        partial_blocks_enabled: bool,
    ) -> Result<Vec<ExtractorMsg>, ExtractionError> {
        let mut msgs = Vec::new();

        match extractor
            .handle_tick_scoped_data(data.clone())
            .await
        {
            Ok(Some(msg)) => {
                if partial_blocks_enabled && !data.is_partial {
                    msgs.push(Self::as_partial_message(&msg));
                }
                msgs.push(msg);
            }
            Ok(None) => {
                trace!("No message to propagate.");
            }
            Err(e) => return Err(e),
        }

        let is_final_partial = data.is_partial && data.is_last_partial == Some(true);
        if partial_blocks_enabled && is_final_partial {
            match extractor
                .collect_and_process_full_block(
                    data.cursor.clone(),
                    data.final_block_height,
                    data.clock.clone(),
                )
                .await
            {
                Ok(Some(msg)) => msgs.push(msg),
                Ok(None) => {
                    trace!("No message to propagate.");
                }
                Err(e) => return Err(e),
            }
        }

        Ok(msgs)
    }

    fn as_partial_message(msg: &ExtractorMsg) -> ExtractorMsg {
        let mut copy: BlockAggregatedChanges = (**msg).clone();
        copy.partial_block_index = Some(0);
        Arc::new(copy)
    }

    #[instrument(skip_all, fields(subscriber_count))]
    pub(crate) async fn propagate_msg(
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
}

struct SingleRuntimeLoopRunner {
    runner: ExtractorRunner,
    loop_state: SingleRuntimeLoopState,
}

impl SingleRuntimeLoopRunner {
    fn new(runner: ExtractorRunner) -> Self {
        let loop_state = SingleRuntimeLoopState::from_extractor(runner.extractor.as_ref());
        Self { runner, loop_state }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SingleRuntimeLoopState {
    extractor_id: String,
    partials_in_block: u32,
}

impl SingleRuntimeLoopState {
    pub(crate) fn from_extractor(extractor: &dyn Extractor) -> Self {
        Self { extractor_id: extractor.get_id().to_string(), partials_in_block: 0 }
    }

    pub(crate) fn extractor_id(&self) -> &str {
        &self.extractor_id
    }
}

pub(crate) async fn handle_single_control_message(
    runner: &mut ExtractorRunner,
    control_message: ControlMessage,
) -> Result<RuntimeLoopControlFlow, ExtractionError> {
    handle_runtime_control_message(
        control_message,
        || {
            warn!("Stop signal received; exiting!");
            Ok(RuntimeLoopControlFlow::Stop)
        },
        |_, sender| async move {
            runner.subscribe(sender).await;
            Ok(RuntimeLoopControlFlow::Continue)
        },
    )
    .await
}

pub(crate) async fn handle_single_block_response(
    runner: &mut ExtractorRunner,
    loop_state: &mut SingleRuntimeLoopState,
    response: BlockResponse,
) -> Result<RuntimeLoopControlFlow, ExtractionError> {
    let id = runner.extractor.get_id();
    runtime_block_response_dispatch! {
        response,
        new => |data| {
            let block_number = data.clock.as_ref().map(|v| v.number).unwrap_or(0);
            tracing::Span::current().record("block_number", block_number);
            gauge!(
                "extractor_current_block_number",
                "chain" => id.chain.to_string(),
                "extractor" => id.name.to_string()
            )
            .set(block_number as f64);

            if data.is_partial {
                loop_state.partials_in_block += 1;
            }

            if data.is_last_partial == Some(true) || data.partial_index.is_none() {
                gauge!(
                    "extractor_partials_per_block",
                    "chain" => id.chain.to_string(),
                    "extractor" => id.name.to_string()
                )
                .set(loop_state.partials_in_block as f64);
                loop_state.partials_in_block = 0;
            }

            let start_time = std::time::Instant::now();
            let msgs = ExtractorRunner::process_block_data(
                runner.extractor.as_ref(),
                &data,
                runner.partial_blocks,
            )
            .await
            .map_err(|err| {
                error!(error = %err, "Error while processing block data");
                tracing::Span::current().record("otel.status_code", "error");
                err
            })?;
            for msg in msgs {
                trace!("Propagating block data message.");
                ExtractorRunner::propagate_msg(&runner.subscriptions, msg).await
            }

            let duration_ms = start_time.elapsed().as_millis() as f64;
            let block_type = match (data.is_partial, data.is_last_partial) {
                (false, _) => "full",
                (true, Some(true)) => "final_partial",
                (true, _) => "partial",
            };

            gauge!(
                "block_processing_time_ms",
                "chain" => id.chain.to_string(),
                "extractor" => id.name.to_string(),
                "block_type" => block_type
            )
            .set(duration_ms);
            Ok(RuntimeLoopControlFlow::Continue)
        },
        undo => |undo_signal| {
            loop_state.partials_in_block = 0;
            info!(block=?&undo_signal.last_valid_block,  "Revert requested!");
            match runner
                .extractor
                .handle_revert(undo_signal.clone())
                .await
            {
                Ok(Some(msg)) => {
                    trace!("Propagating block undo message.");
                    ExtractorRunner::propagate_msg(&runner.subscriptions, msg).await;
                    Ok(RuntimeLoopControlFlow::Continue)
                }
                Ok(None) => {
                    trace!("No message to propagate.");
                    Ok(RuntimeLoopControlFlow::Continue)
                }
                Err(err) => {
                    error!(error = %err, "Error while processing revert!");
                    tracing::Span::current().record("otel.status_code", "error");
                    Err(err)
                }
            }
        },
        ended => {
            runner.extractor.flush().await?;
            tracing::Span::current().record("otel.status_code", "ok");
            Ok(RuntimeLoopControlFlow::Stop)
        },
    }
}

pub(crate) async fn handle_single_stream_item(
    runner: &mut ExtractorRunner,
    loop_state: &mut SingleRuntimeLoopState,
    stream_item: Option<Result<BlockResponse, anyhow::Error>>,
) -> Result<(u64, RuntimeLoopControlFlow), ExtractionError> {
    let extractor_id = loop_state.extractor_id().to_string();
    handle_runtime_stream_item(
        stream_item,
        || ExtractionError::SubstreamsError(format!("{extractor_id}: stream ended")),
        |response| async move {
            handle_single_block_response(runner, loop_state, response).await
        },
    )
    .await
    .map_err(|err| {
        if matches!(err, ExtractionError::SubstreamsError(_)) {
            error!(error = %err, "Stream terminated with error.");
        }
        err
    })
}

#[async_trait]
impl crate::extractor::execution_loop::ManagedRuntimeLoop for SingleRuntimeLoopRunner {
    fn extractor_loop_id(&self) -> String {
        self.loop_state.extractor_id().to_string()
    }

    fn runtime_loop_kind(&self) -> &'static str {
        "single"
    }

    async fn step(&mut self) -> Result<bool, ExtractionError> {
        runtime_step_select! {
            control = self.runner.control_rx.recv() => |ctrl| {
                let action = handle_single_control_message(&mut self.runner, ctrl).await?;
                Ok(continue_after_control_action(action))
            },
            stream = self.runner.substreams.next().instrument(info_span!("substreams_waiting")) => |val| {
                let (block_number, action) =
                    handle_single_stream_item(&mut self.runner, &mut self.loop_state, val).await?;
                Ok(continue_after_stream_action(block_number, action))
            },
        }
    }
}
