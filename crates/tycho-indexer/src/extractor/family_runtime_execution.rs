use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{error, info_span, Instrument};
use tycho_common::models::ExtractorIdentity;

use crate::{
    extractor::{
        control::{BranchSubscriptionsMap, ControlMessage, SubscriptionsMap},
        execution_loop::{
            handle_runtime_control_message, handle_runtime_stream_item,
            continue_after_control_action, continue_after_stream_action, runtime_step_select,
            runtime_block_response_dispatch, spawn_managed_runtime_loop, RuntimeLoopControlFlow,
        },
        family_dispatch::FamilyBlockChangesDispatcher,
        family_runner_wiring::FamilyBranchSubscriptionIndex,
        protocol_cache::ProtocolMemoryCache,
        single_runtime_execution::ExtractorRunner,
        ExtractionError, Extractor, ExtractorMsg,
    },
    pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal},
    substreams::stream::{BlockResponse, SubstreamsStream},
};

pub struct FamilyExtractorRunner {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) substreams: SubstreamsStream,
    pub(crate) subscriptions: BranchSubscriptionsMap,
    pub(crate) control_rx: Receiver<ControlMessage>,
    pub(crate) runtime_handle: Option<Handle>,
    pub(crate) partial_blocks: bool,
    pub(crate) runtime_state: FamilyRuntimeState,
}

pub(crate) struct FamilyRuntimeState {
    pub(crate) branch_subscription_index: FamilyBranchSubscriptionIndex,
    pub(crate) next_subscriber_id: u64,
    pub(crate) dispatcher: FamilyBlockChangesDispatcher,
    pub(crate) protocol_cache: ProtocolMemoryCache,
}

impl FamilyRuntimeState {
    pub(crate) fn new(
        extractors: &HashMap<String, Arc<dyn Extractor>>,
        dispatcher: FamilyBlockChangesDispatcher,
        protocol_cache: ProtocolMemoryCache,
    ) -> Self {
        Self {
            branch_subscription_index: FamilyBranchSubscriptionIndex::from_extractors(extractors),
            next_subscriber_id: 0,
            dispatcher,
            protocol_cache,
        }
    }
}

impl FamilyExtractorRunner {
    pub fn run(self) -> JoinHandle<Result<(), ExtractionError>> {
        let runtime_handle = self.runtime_handle.clone();
        spawn_managed_runtime_loop(runtime_handle, FamilyRuntimeLoopRunner::new(self))
    }

    #[cfg(test)]
    pub(crate) async fn subscribe(
        &mut self,
        extractor_id: ExtractorIdentity,
        sender: Sender<ExtractorMsg>,
    ) {
        subscribe_family_branch(
            &mut self.runtime_state.next_subscriber_id,
            &mut self
                .runtime_state
                .branch_subscription_index,
            &self.extractors,
            &self.subscriptions,
            extractor_id,
            sender,
        )
        .await;
    }

    #[cfg(test)]
    pub(crate) fn branch_subscription_index(&self) -> &FamilyBranchSubscriptionIndex {
        &self
            .runtime_state
            .branch_subscription_index
    }
}

struct FamilyRuntimeLoopRunner {
    runner: FamilyExtractorRunner,
    loop_state: FamilyRuntimeLoopState,
}

impl FamilyRuntimeLoopRunner {
    fn new(runner: FamilyExtractorRunner) -> Self {
        let loop_state = FamilyRuntimeLoopState::from_extractors(&runner.extractors);
        Self { runner, loop_state }
    }
}

#[async_trait]
impl crate::extractor::execution_loop::ManagedRuntimeLoop for FamilyRuntimeLoopRunner {
    fn extractor_loop_id(&self) -> String {
        self.loop_state.family_id().to_string()
    }

    fn runtime_loop_kind(&self) -> &'static str {
        "family"
    }

    async fn step(&mut self) -> Result<bool, ExtractionError> {
        runtime_step_select! {
            control = self.runner.control_rx.recv() => |ctrl| {
                let action = handle_family_control_message(
                    &mut self.runner.runtime_state,
                    &self.runner.extractors,
                    &self.runner.subscriptions,
                    ctrl,
                ).await?;
                Ok(continue_after_control_action(action))
            },
            stream = self.runner.substreams.next().instrument(info_span!("substreams_waiting")) => |val| {
                let (block_number, action) = handle_family_stream_item(
                    &mut self.loop_state,
                    &mut self.runner.runtime_state,
                    &self.runner.extractors,
                    &self.runner.subscriptions,
                    self.runner.partial_blocks,
                    val,
                ).await.map_err(|err| {
                    tracing::Span::current().record("otel.status_code", "error");
                    err
                }).map_err(|err| {
                    error!(error = %err, "Family stream terminated with error.");
                    err
                })?;
                Ok(continue_after_stream_action(block_number, action))
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FamilyRuntimeLoopState {
    family_id: String,
    partials_in_block: u32,
}

impl FamilyRuntimeLoopState {
    pub(crate) fn from_extractors(extractors: &HashMap<String, Arc<dyn Extractor>>) -> Self {
        let family_id = extractors
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        Self { family_id, partials_in_block: 0 }
    }

    pub(crate) fn family_id(&self) -> &str {
        &self.family_id
    }
}

pub(crate) async fn handle_family_control_message(
    runtime_state: &mut FamilyRuntimeState,
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    control_message: ControlMessage,
) -> Result<RuntimeLoopControlFlow, ExtractionError> {
    handle_runtime_control_message(
        control_message,
        || {
            tracing::warn!("Family runner stop signal received; exiting!");
            Ok(RuntimeLoopControlFlow::Stop)
        },
        |extractor_id, sender| async move {
            subscribe_family_branch(
                &mut runtime_state.next_subscriber_id,
                &mut runtime_state.branch_subscription_index,
                extractors,
                subscriptions,
                extractor_id,
                sender,
            )
            .await;
            Ok(RuntimeLoopControlFlow::Continue)
        },
    )
    .await
}

pub(crate) async fn handle_family_block_response(
    dispatcher: &mut FamilyBlockChangesDispatcher,
    protocol_cache: &ProtocolMemoryCache,
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    partial_blocks: bool,
    partials_in_block: &mut u32,
    response: BlockResponse,
) -> Result<RuntimeLoopControlFlow, ExtractionError> {
    runtime_block_response_dispatch! {
        response,
        new => |data| {
            if data.is_partial {
                *partials_in_block += 1;
            }
            if data.is_last_partial == Some(true) || data.partial_index.is_none() {
                *partials_in_block = 0;
            }

            handle_family_new_block(
                dispatcher,
                protocol_cache,
                extractors,
                subscriptions,
                partial_blocks,
                data,
            )
            .await?;
            Ok(RuntimeLoopControlFlow::Continue)
        },
        undo => |undo_signal| {
            *partials_in_block = 0;
            handle_family_revert(extractors, subscriptions, undo_signal).await?;
            Ok(RuntimeLoopControlFlow::Continue)
        },
        ended => {
            flush_family_extractors(extractors).await?;
            Ok(RuntimeLoopControlFlow::Stop)
        },
    }
}

pub(crate) fn family_stream_ended_error(family_id: &str) -> ExtractionError {
    ExtractionError::SubstreamsError(format!("{family_id}: stream ended"))
}

pub(crate) async fn handle_family_stream_item(
    loop_state: &mut FamilyRuntimeLoopState,
    runtime_state: &mut FamilyRuntimeState,
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    partial_blocks: bool,
    stream_item: Option<Result<BlockResponse, anyhow::Error>>,
) -> Result<(u64, RuntimeLoopControlFlow), ExtractionError> {
    let family_id = loop_state.family_id().to_string();
    handle_runtime_stream_item(
        stream_item,
        || family_stream_ended_error(&family_id),
        |response| async move {
            handle_family_block_response(
                &mut runtime_state.dispatcher,
                &runtime_state.protocol_cache,
                extractors,
                subscriptions,
                partial_blocks,
                &mut loop_state.partials_in_block,
                response,
            )
            .await
        },
    )
    .await
}

pub(crate) async fn handle_family_new_block(
    dispatcher: &mut FamilyBlockChangesDispatcher,
    protocol_cache: &ProtocolMemoryCache,
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    partial_blocks: bool,
    data: BlockScopedData,
) -> Result<(), ExtractionError> {
    let dispatched = dispatcher
        .dispatch_block_scoped_data_with_protocol_cache_fallback(data, protocol_cache)
        .await?;
    let mut branch_payloads = dispatched
        .into_iter()
        .collect::<Vec<_>>();
    branch_payloads.sort_by(|(left, _), (right, _)| left.cmp(right));
    let pending_msgs =
        process_branch_payloads(extractors, subscriptions, partial_blocks, branch_payloads).await?;
    propagate_pending_msgs(pending_msgs).await;
    Ok(())
}

pub(crate) async fn process_branch_payloads(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    partial_blocks: bool,
    branch_payloads: Vec<(String, BlockScopedData)>,
) -> Result<Vec<(Arc<Mutex<SubscriptionsMap>>, ExtractorMsg)>, ExtractionError> {
    let mut pending_msgs = Vec::new();

    for (extractor_id, branch_data) in branch_payloads {
        let Some(extractor) = extractors.get(&extractor_id) else {
            return Err(ExtractionError::Setup(format!(
                "family runner missing extractor for {extractor_id}"
            )));
        };
        let msgs =
            ExtractorRunner::process_block_data(extractor.as_ref(), &branch_data, partial_blocks)
                .await
                .map_err(|err| {
                    tracing::error!(
                        error = %err,
                        extractor_id = %extractor_id,
                        "Error while processing family branch block data"
                    );
                    err
                })?;
        let subscribers = subscriptions
            .get(&extractor_id)
            .expect("branch subscriptions initialized")
            .clone();
        for msg in msgs {
            pending_msgs.push((subscribers.clone(), msg));
        }
    }

    Ok(pending_msgs)
}

pub(crate) async fn propagate_pending_msgs(
    pending_msgs: Vec<(Arc<Mutex<SubscriptionsMap>>, ExtractorMsg)>,
) {
    for (subscribers, msg) in pending_msgs {
        ExtractorRunner::propagate_msg(&subscribers, msg).await;
    }
}

pub(crate) async fn handle_family_revert(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    undo_signal: BlockUndoSignal,
) -> Result<(), ExtractionError> {
    for (extractor_id, extractor) in extractors {
        match extractor
            .handle_revert(undo_signal.clone())
            .await
        {
            Ok(Some(msg)) => {
                let subscribers = subscriptions
                    .get(extractor_id)
                    .expect("branch subscriptions initialized");
                ExtractorRunner::propagate_msg(subscribers, msg).await;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::error!(
                    error = %err,
                    extractor_id = %extractor_id,
                    "Error while processing family revert"
                );
                return Err(err);
            }
        }
    }

    Ok(())
}

pub(crate) async fn flush_family_extractors(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
) -> Result<(), ExtractionError> {
    for extractor in extractors.values() {
        extractor.flush().await?;
    }

    Ok(())
}

pub(crate) async fn subscribe_family_branch(
    next_subscriber_id: &mut u64,
    branch_subscription_index: &mut FamilyBranchSubscriptionIndex,
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    extractor_id: ExtractorIdentity,
    sender: Sender<ExtractorMsg>,
) {
    let subscriber_id = *next_subscriber_id;
    *next_subscriber_id += 1;
    tracing::Span::current().record("subscriber_id", subscriber_id);
    tracing::info!(?subscriber_id, ?extractor_id, "New family branch subscription");

    let subscription_key = branch_subscription_index.resolve_or_learn(&extractor_id, extractors);

    if let Some(subscription_key) = subscription_key {
        let subscribers = subscriptions
            .get(&subscription_key)
            .expect("resolved family subscription key should exist");
        subscribers
            .lock()
            .await
            .insert(subscriber_id, sender);
    } else {
        tracing::warn!(?extractor_id, "Ignoring subscription for unknown family branch extractor");
    }
}
