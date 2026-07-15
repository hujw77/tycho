use async_trait::async_trait;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use anyhow::Error as AnyhowError;
use tokio::sync::mpsc::Sender;
use tokio::{runtime::Handle, task::JoinHandle};
use tracing::Instrument;
use tycho_common::models::ExtractorIdentity;

use crate::{
    extractor::{
        control::{
            propagate_subscription_message, BranchSubscriptionsMap, ControlMessage,
            SubscriptionsMap,
        },
        ExtractionError, Extractor, ExtractorMsg,
    },
    pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal},
    substreams::stream::BlockResponse,
};
use tycho_common::models::blockchain::BlockAggregatedChanges;

#[async_trait]
pub(crate) trait ManagedRuntimeLoop: Send {
    fn extractor_loop_id(&self) -> String;

    fn runtime_loop_kind(&self) -> &'static str;

    async fn step(&mut self) -> Result<bool, ExtractionError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeLoopControlFlow {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PartialBlockTracker {
    partials_in_block: u32,
}

impl PartialBlockTracker {
    pub(crate) fn on_new_block(&mut self, data: &BlockScopedData) -> Option<u32> {
        if data.is_partial {
            self.partials_in_block += 1;
        }

        if data.is_last_partial == Some(true) || data.partial_index.is_none() {
            let completed_partials = self.partials_in_block;
            self.partials_in_block = 0;
            return Some(completed_partials);
        }

        None
    }

    pub(crate) fn reset(&mut self) {
        self.partials_in_block = 0;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRuntimeLoopState {
    loop_id: String,
    partial_block_tracker: PartialBlockTracker,
}

impl ManagedRuntimeLoopState {
    pub(crate) fn new(loop_id: impl Into<String>) -> Self {
        Self {
            loop_id: loop_id.into(),
            partial_block_tracker: PartialBlockTracker::default(),
        }
    }

    pub(crate) fn loop_id(&self) -> &str {
        &self.loop_id
    }

    pub(crate) fn partial_block_tracker_mut(&mut self) -> &mut PartialBlockTracker {
        &mut self.partial_block_tracker
    }
}

macro_rules! runtime_step_select {
    (
        control = $control:expr => |$ctrl:ident| $control_body:block,
        stream = $stream:expr => |$stream_item:ident| $stream_body:block $(,)?
    ) => {
        tokio::select! {
            Some($ctrl) = $control => $control_body,
            $stream_item = $stream => $stream_body,
        }
    };
}

macro_rules! runtime_block_response_dispatch {
    (
        $response:expr,
        new => |$data:ident| $new_body:block,
        undo => |$undo_signal:ident| $undo_body:block,
        ended => $ended_body:block $(,)?
    ) => {
        match $response {
            BlockResponse::New($data) => $new_body,
            BlockResponse::Undo($undo_signal) => $undo_body,
            BlockResponse::Ended => $ended_body,
        }
    };
}

pub(crate) use runtime_block_response_dispatch;

pub(crate) trait RuntimeLoopAction {
    fn should_continue(&self) -> bool;
}

impl RuntimeLoopAction for RuntimeLoopControlFlow {
    fn should_continue(&self) -> bool {
        matches!(self, Self::Continue)
    }
}

pub(crate) fn continue_after_control_action<A>(action: A) -> bool
where
    A: RuntimeLoopAction,
{
    action.should_continue()
}

pub(crate) fn continue_after_stream_action<A>(block_number: u64, action: A) -> bool
where
    A: RuntimeLoopAction,
{
    tracing::Span::current().record("block_number", block_number);
    let should_continue = action.should_continue();
    if !should_continue {
        tracing::Span::current().record("otel.status_code", "ok");
    }
    should_continue
}

pub(crate) async fn run_managed_runtime_step<
    Ctx,
    FCtrl,
    FStream,
    HCtrl,
    HStream,
>(
    context: &mut Ctx,
    control: FCtrl,
    stream: FStream,
    handle_control: HCtrl,
    handle_stream: HStream,
) -> Result<bool, ExtractionError>
where
    FCtrl: Future<Output = Option<ControlMessage>>,
    FStream: Future<Output = Option<Result<BlockResponse, AnyhowError>>>,
    HCtrl: for<'a> FnOnce(
        &'a mut Ctx,
        ControlMessage,
    ) -> Pin<Box<dyn Future<Output = Result<RuntimeLoopControlFlow, ExtractionError>> + Send + 'a>>,
    HStream: for<'a> FnOnce(
        &'a mut Ctx,
        Option<Result<BlockResponse, AnyhowError>>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(u64, RuntimeLoopControlFlow), ExtractionError>> + Send + 'a,
        >,
    >,
{
    runtime_step_select! {
        control = control => |ctrl| {
            let action = handle_control(context, ctrl).await?;
            Ok(continue_after_control_action(action))
        },
        stream = stream => |stream_item| {
            let (block_number, action) = handle_stream(context, stream_item).await?;
            Ok(continue_after_stream_action(block_number, action))
        },
    }
}

pub(crate) fn block_number_for_response(response: &BlockResponse) -> u64 {
    match response {
        BlockResponse::New(data) => data
            .clock
            .as_ref()
            .map(|v| v.number)
            .unwrap_or(0),
        BlockResponse::Undo(_) | BlockResponse::Ended => 0,
    }
}

pub(crate) fn stream_ended_error(loop_id: &str) -> ExtractionError {
    ExtractionError::SubstreamsError(format!("{loop_id}: stream ended"))
}

#[cfg(test)]
mod tests {
    use super::PartialBlockTracker;
    use crate::pb::sf::substreams::rpc::v2::BlockScopedData;

    fn block_scoped_data(
        is_partial: bool,
        is_last_partial: Option<bool>,
        partial_index: Option<u32>,
    ) -> BlockScopedData {
        BlockScopedData {
            output: Default::default(),
            clock: None,
            final_block_height: 0,
            cursor: String::new(),
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: String::new(),
            is_partial,
            is_last_partial,
            partial_index,
        }
    }

    #[test]
    fn partial_block_tracker_counts_then_resets_on_final_partial() {
        let mut tracker = PartialBlockTracker::default();

        assert_eq!(
            tracker.on_new_block(&block_scoped_data(true, Some(false), Some(0))),
            None
        );
        assert_eq!(
            tracker.on_new_block(&block_scoped_data(true, Some(true), Some(1))),
            Some(2)
        );
    }

    #[test]
    fn partial_block_tracker_resets_on_full_block_without_partials() {
        let mut tracker = PartialBlockTracker::default();

        assert_eq!(
            tracker.on_new_block(&block_scoped_data(false, None, None)),
            Some(0)
        );
        assert_eq!(
            tracker.on_new_block(&block_scoped_data(true, Some(false), Some(0))),
            None
        );
        tracker.reset();
        assert_eq!(
            tracker.on_new_block(&block_scoped_data(false, None, None)),
            Some(0)
        );
    }
}

pub(crate) async fn handle_runtime_stream_item<F, Fut, E>(
    stream_item: Option<Result<BlockResponse, AnyhowError>>,
    stream_ended_error: E,
    handle_response: F,
) -> Result<(u64, RuntimeLoopControlFlow), ExtractionError>
where
    F: FnOnce(BlockResponse) -> Fut,
    Fut: Future<Output = Result<RuntimeLoopControlFlow, ExtractionError>>,
    E: FnOnce() -> ExtractionError,
{
    match stream_item {
        None => {
            tracing::Span::current().record("otel.status_code", "error");
            Err(stream_ended_error())
        }
        Some(Ok(response)) => {
            let block_number = block_number_for_response(&response);
            let action = handle_response(response).await?;
            Ok((block_number, action))
        }
        Some(Err(err)) => {
            tracing::Span::current().record("otel.status_code", "error");
            Err(ExtractionError::SubstreamsError(err.to_string()))
        }
    }
}

pub(crate) async fn handle_logged_runtime_stream_item<F, Fut, E, L>(
    stream_item: Option<Result<BlockResponse, AnyhowError>>,
    stream_ended_error: E,
    handle_response: F,
    log_termination_error: L,
) -> Result<(u64, RuntimeLoopControlFlow), ExtractionError>
where
    F: FnOnce(BlockResponse) -> Fut,
    Fut: Future<Output = Result<RuntimeLoopControlFlow, ExtractionError>>,
    E: FnOnce() -> ExtractionError,
    L: FnOnce(&ExtractionError),
{
    handle_runtime_stream_item(stream_item, stream_ended_error, handle_response)
        .await
        .map_err(|err| {
            if matches!(err, ExtractionError::SubstreamsError(_)) {
                log_termination_error(&err);
            }
            err
        })
}

pub(crate) async fn handle_runtime_control_message<FS, FSub, Fut>(
    control_message: ControlMessage,
    handle_stop: FS,
    handle_subscribe: FSub,
) -> Result<RuntimeLoopControlFlow, ExtractionError>
where
    FS: FnOnce() -> Result<RuntimeLoopControlFlow, ExtractionError>,
    FSub: FnOnce(ExtractorIdentity, Sender<ExtractorMsg>) -> Fut,
    Fut: Future<Output = Result<RuntimeLoopControlFlow, ExtractionError>>,
{
    match control_message {
        ControlMessage::Stop => handle_stop(),
        ControlMessage::Subscribe { extractor_id, sender } => {
            handle_subscribe(extractor_id, sender).await
        }
    }
}

pub(crate) async fn process_block_data_for_extractor(
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
                msgs.push(as_partial_message(&msg));
            }
            msgs.push(msg);
        }
        Ok(None) => {
            tracing::trace!("No message to propagate.");
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
                tracing::trace!("No message to propagate.");
            }
            Err(e) => return Err(e),
        }
    }

    Ok(msgs)
}

pub(crate) fn as_partial_message(msg: &ExtractorMsg) -> ExtractorMsg {
    let mut copy: BlockAggregatedChanges = (**msg).clone();
    copy.partial_block_index = Some(0);
    Arc::new(copy)
}

pub(crate) async fn process_revert_for_extractor(
    extractor: &dyn Extractor,
    undo_signal: &BlockUndoSignal,
) -> Result<Option<ExtractorMsg>, ExtractionError> {
    extractor
        .handle_revert(undo_signal.clone())
        .await
}

pub(crate) type PendingSubscriptionMessages =
    Vec<(Arc<tokio::sync::Mutex<SubscriptionsMap>>, ExtractorMsg)>;

pub(crate) async fn collect_subscription_messages_for_extractor(
    extractor: &dyn Extractor,
    subscribers: Arc<tokio::sync::Mutex<SubscriptionsMap>>,
    partial_blocks_enabled: bool,
    data: &BlockScopedData,
) -> Result<PendingSubscriptionMessages, ExtractionError> {
    let msgs = process_block_data_for_extractor(extractor, data, partial_blocks_enabled).await?;
    Ok(msgs
        .into_iter()
        .map(|msg| (subscribers.clone(), msg))
        .collect())
}

pub(crate) async fn collect_revert_subscription_messages_for_extractor(
    extractor: &dyn Extractor,
    subscribers: Arc<tokio::sync::Mutex<SubscriptionsMap>>,
    undo_signal: &BlockUndoSignal,
) -> Result<PendingSubscriptionMessages, ExtractionError> {
    Ok(process_revert_for_extractor(extractor, undo_signal)
        .await?
        .into_iter()
        .map(|msg| (subscribers.clone(), msg))
        .collect())
}

pub(crate) async fn process_branch_payloads_for_extractors(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    partial_blocks_enabled: bool,
    branch_payloads: Vec<(String, BlockScopedData)>,
) -> Result<PendingSubscriptionMessages, ExtractionError> {
    let mut pending_msgs = Vec::new();

    for (extractor_id, branch_data) in branch_payloads {
        let Some(extractor) = extractors.get(&extractor_id) else {
            return Err(ExtractionError::Setup(format!(
                "family runner missing extractor for {extractor_id}"
            )));
        };
        let subscribers = subscriptions
            .get(&extractor_id)
            .expect("branch subscriptions initialized");
        pending_msgs.extend(
            collect_subscription_messages_for_extractor(
                extractor.as_ref(),
                subscribers.clone(),
                partial_blocks_enabled,
                &branch_data,
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    extractor_id = %extractor_id,
                    "Error while processing family branch block data"
                );
                err
            })?,
        );
    }

    Ok(pending_msgs)
}

pub(crate) async fn process_reverts_for_extractors(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    undo_signal: &BlockUndoSignal,
) -> Result<PendingSubscriptionMessages, ExtractionError> {
    let mut pending_msgs = Vec::new();

    for (extractor_id, extractor) in extractors {
        let subscribers = subscriptions
            .get(extractor_id)
            .expect("branch subscriptions initialized");
        pending_msgs.extend(
            collect_revert_subscription_messages_for_extractor(
                extractor.as_ref(),
                subscribers.clone(),
                undo_signal,
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    extractor_id = %extractor_id,
                    "Error while processing family revert"
                );
                err
            })?,
        );
    }

    Ok(pending_msgs)
}

pub(crate) async fn propagate_pending_subscription_messages(
    pending_msgs: PendingSubscriptionMessages,
) {
    for (subscribers, msg) in pending_msgs {
        propagate_subscription_message(&subscribers, msg).await;
    }
}

pub(crate) async fn flush_extractors<'a, I>(extractors: I) -> Result<(), ExtractionError>
where
    I: IntoIterator<Item = &'a Arc<dyn Extractor>>,
{
    for extractor in extractors {
        extractor.flush().await?;
    }

    Ok(())
}

pub(crate) fn spawn_managed_runtime_loop<T>(
    runtime_handle: Option<Handle>,
    mut state: T,
) -> JoinHandle<Result<(), ExtractionError>>
where
    T: ManagedRuntimeLoop + 'static,
{
    let runtime = runtime_handle.unwrap_or_else(Handle::current);

    runtime.spawn(async move {
        loop {
            let current_extractor_id = state.extractor_loop_id();
            let current_runtime_loop_kind = state.runtime_loop_kind();
            let loop_span = tracing::info_span!(
                "extractor_runtime",
                extractor_id = %current_extractor_id,
                runtime_loop = current_runtime_loop_kind,
                sf_trace_id = tracing::field::Empty,
                block_number = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );

            let should_continue = async {
                let should_continue = state.step().await?;
                tracing::Span::current().record("otel.status_code", "ok");
                Ok::<bool, ExtractionError>(should_continue)
            }
            .instrument(loop_span)
            .await?;

            if !should_continue {
                break Ok(());
            }
        }
    })
}
