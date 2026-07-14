use async_trait::async_trait;
use std::future::Future;

use anyhow::Error as AnyhowError;
use tokio::{runtime::Handle, task::JoinHandle};
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tycho_common::models::ExtractorIdentity;

use crate::{
    extractor::{control::ControlMessage, ExtractionError, ExtractorMsg},
    substreams::stream::BlockResponse,
};

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

pub(crate) use runtime_step_select;

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

pub(crate) fn continue_after_stream_action<A>(
    block_number: u64,
    action: A,
) -> bool
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

pub(crate) fn block_number_for_response(response: &BlockResponse) -> u64 {
    match response {
        BlockResponse::New(data) => data.clock.as_ref().map(|v| v.number).unwrap_or(0),
        BlockResponse::Undo(_) | BlockResponse::Ended => 0,
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
