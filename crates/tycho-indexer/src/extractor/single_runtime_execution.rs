use async_trait::async_trait;
use std::sync::Arc;

use metrics::gauge;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::Receiver,
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{error, info, info_span, trace, warn, Instrument};
#[cfg(test)]
use tracing::instrument;

use crate::{
    extractor::{
        control::{
            register_logged_subscription, ControlMessage, SubscriptionsMap,
        },
        execution_loop::{
            collect_revert_subscription_messages_for_extractor,
            collect_subscription_messages_for_extractor, flush_extractors,
            handle_logged_runtime_stream_item,
            handle_runtime_control_message, ManagedRuntimeLoopState,
            propagate_pending_subscription_messages, run_managed_runtime_step,
            runtime_block_response_dispatch, spawn_managed_runtime_loop,
            stream_ended_error, RuntimeLoopControlFlow,
        },
        ExtractionError, Extractor,
    },
    substreams::stream::{BlockResponse, SubstreamsStream},
};
#[cfg(test)]
use crate::pb::sf::substreams::rpc::v2::BlockScopedData;
#[cfg(test)]
use crate::extractor::ExtractorMsg;
#[cfg(test)]
use crate::extractor::execution_loop::process_block_data_for_extractor;

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

    #[cfg(test)]
    #[instrument(skip_all, fields(partial_blocks_enabled, is_partial = data.is_partial))]
    pub(crate) async fn process_block_data(
        extractor: &dyn Extractor,
        data: &BlockScopedData,
        partial_blocks_enabled: bool,
    ) -> Result<Vec<ExtractorMsg>, ExtractionError> {
        process_block_data_for_extractor(extractor, data, partial_blocks_enabled).await
    }
}

struct SingleRuntimeLoopRunner {
    runner: ExtractorRunner,
    loop_state: ManagedRuntimeLoopState,
}

impl SingleRuntimeLoopRunner {
    fn new(runner: ExtractorRunner) -> Self {
        let loop_state = ManagedRuntimeLoopState::new(runner.extractor.get_id().to_string());
        Self { runner, loop_state }
    }
}

pub(crate) async fn handle_single_control_message(
    next_subscriber_id: &mut u64,
    subscriptions: &Arc<Mutex<SubscriptionsMap>>,
    control_message: ControlMessage,
) -> Result<RuntimeLoopControlFlow, ExtractionError> {
    handle_runtime_control_message(
        control_message,
        || {
            warn!("Stop signal received; exiting!");
            Ok(RuntimeLoopControlFlow::Stop)
        },
        |_, sender| async move {
            register_logged_subscription(
                subscriptions,
                next_subscriber_id,
                sender,
                None,
                "New subscription",
            )
            .await;
            Ok(RuntimeLoopControlFlow::Continue)
        },
    )
    .await
}

pub(crate) async fn handle_single_block_response(
    extractor: &Arc<dyn Extractor>,
    subscriptions: &Arc<Mutex<SubscriptionsMap>>,
    partial_blocks: bool,
    loop_state: &mut ManagedRuntimeLoopState,
    response: BlockResponse,
) -> Result<RuntimeLoopControlFlow, ExtractionError> {
    let id = extractor.get_id();
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

            if let Some(partials_in_block) = loop_state
                .partial_block_tracker_mut()
                .on_new_block(&data)
            {
                gauge!(
                    "extractor_partials_per_block",
                    "chain" => id.chain.to_string(),
                    "extractor" => id.name.to_string()
                )
                .set(partials_in_block as f64);
            }

            let start_time = std::time::Instant::now();
            let pending_msgs = collect_subscription_messages_for_extractor(
                extractor.as_ref(),
                subscriptions.clone(),
                partial_blocks,
                &data,
            )
            .await
            .map_err(|err| {
                error!(error = %err, "Error while processing block data");
                tracing::Span::current().record("otel.status_code", "error");
                err
            })?;
            propagate_pending_subscription_messages(pending_msgs).await;

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
            loop_state.partial_block_tracker_mut().reset();
            info!(block=?&undo_signal.last_valid_block,  "Revert requested!");
            let pending_msgs = collect_revert_subscription_messages_for_extractor(
                extractor.as_ref(),
                subscriptions.clone(),
                &undo_signal,
            )
            .await
            .map_err(|err| {
                error!(error = %err, "Error while processing revert!");
                tracing::Span::current().record("otel.status_code", "error");
                err
            })?;
            trace!("Propagating block undo message.");
            propagate_pending_subscription_messages(pending_msgs).await;
            Ok(RuntimeLoopControlFlow::Continue)
        },
        ended => {
            flush_extractors(std::iter::once(extractor)).await?;
            tracing::Span::current().record("otel.status_code", "ok");
            Ok(RuntimeLoopControlFlow::Stop)
        },
    }
}

pub(crate) async fn handle_single_stream_item(
    extractor: &Arc<dyn Extractor>,
    subscriptions: &Arc<Mutex<SubscriptionsMap>>,
    partial_blocks: bool,
    loop_state: &mut ManagedRuntimeLoopState,
    stream_item: Option<Result<BlockResponse, anyhow::Error>>,
) -> Result<(u64, RuntimeLoopControlFlow), ExtractionError> {
    let extractor_id = loop_state.loop_id().to_string();
    handle_logged_runtime_stream_item(
        stream_item,
        || stream_ended_error(&extractor_id),
        |response| async move {
            handle_single_block_response(
                extractor,
                subscriptions,
                partial_blocks,
                loop_state,
                response,
            )
            .await
        },
        |err| error!(error = %err, "Stream terminated with error."),
    )
    .await
}

#[async_trait]
impl crate::extractor::execution_loop::ManagedRuntimeLoop for SingleRuntimeLoopRunner {
    fn extractor_loop_id(&self) -> String {
        self.loop_state
            .loop_id()
            .to_string()
    }

    fn runtime_loop_kind(&self) -> &'static str {
        "single"
    }

    async fn step(&mut self) -> Result<bool, ExtractionError> {
        let control_rx = &mut self.runner.control_rx;
        let substreams = &mut self.runner.substreams;
        let mut step_context = (
            &mut self.runner.next_subscriber_id,
            &self.runner.extractor,
            &self.runner.subscriptions,
            self.runner.partial_blocks,
            &mut self.loop_state,
        );

        run_managed_runtime_step(
            &mut step_context,
            control_rx.recv(),
            substreams.next().instrument(info_span!("substreams_waiting")),
            |(next_subscriber_id, _, subscriptions, _, _), ctrl| {
                Box::pin(async move {
                    handle_single_control_message(next_subscriber_id, subscriptions, ctrl).await
                })
            },
            |(_, extractor, subscriptions, partial_blocks, loop_state), stream_item| {
                Box::pin(async move {
                    handle_single_stream_item(
                        extractor,
                        subscriptions,
                        *partial_blocks,
                        loop_state,
                        stream_item,
                    )
                    .await
                })
            },
        )
        .await
    }
}
