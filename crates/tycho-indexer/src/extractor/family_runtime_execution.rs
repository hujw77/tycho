use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{Receiver, Sender},
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{error, info_span, Instrument};
use tycho_common::models::ExtractorIdentity;

use crate::{
    extractor::{
        control::{
            allocate_logged_subscription_id, register_subscription,
            BranchSubscriptionsMap, ControlMessage,
        },
        execution_loop::{
            flush_extractors, handle_logged_runtime_stream_item,
            handle_runtime_control_message, ManagedRuntimeLoopState,
            process_branch_payloads_for_extractors, process_reverts_for_extractors,
            propagate_pending_subscription_messages, run_managed_runtime_step,
            runtime_block_response_dispatch, spawn_managed_runtime_loop, stream_ended_error,
            PartialBlockTracker, RuntimeLoopControlFlow,
        },
        family_dispatch::FamilyBlockChangesDispatcher,
        family_runner_wiring::FamilyBranchSubscriptionIndex,
        family_runtime_planning::ResolvedFamilyRuntimeContract,
        protocol_cache::ProtocolMemoryCache,
        ExtractionError, Extractor, ExtractorMsg,
    },
    pb::sf::substreams::rpc::v2::BlockScopedData,
    substreams::stream::{BlockResponse, SubstreamsStream},
};

pub struct FamilyExtractorRunner {
    pub(crate) runtime_contract: ResolvedFamilyRuntimeContract,
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
        contract: &ResolvedFamilyRuntimeContract,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
        dispatcher: FamilyBlockChangesDispatcher,
        protocol_cache: ProtocolMemoryCache,
    ) -> Self {
        Self {
            branch_subscription_index: FamilyBranchSubscriptionIndex::from_branch_protocol_systems(
                contract
                    .branch_protocol_systems()
                    .map(str::to_string),
                extractors,
            ),
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
    loop_state: ManagedRuntimeLoopState,
}

impl FamilyRuntimeLoopRunner {
    fn new(runner: FamilyExtractorRunner) -> Self {
        let loop_state = ManagedRuntimeLoopState::new(
            runner
                .runtime_contract
                .shared_extractor_id()
                .to_string(),
        );
        Self { runner, loop_state }
    }
}

#[async_trait]
impl crate::extractor::execution_loop::ManagedRuntimeLoop for FamilyRuntimeLoopRunner {
    fn extractor_loop_id(&self) -> String {
        self.loop_state.loop_id().to_string()
    }

    fn runtime_loop_kind(&self) -> &'static str {
        "family"
    }

    async fn step(&mut self) -> Result<bool, ExtractionError> {
        let control_rx = &mut self.runner.control_rx;
        let substreams = &mut self.runner.substreams;
        let mut step_context = (
            &mut self.runner.runtime_state,
            &self.runner.extractors,
            &self.runner.subscriptions,
            self.runner.partial_blocks,
            &mut self.loop_state,
        );

        run_managed_runtime_step(
            &mut step_context,
            control_rx.recv(),
            substreams.next().instrument(info_span!("substreams_waiting")),
            |(runtime_state, extractors, subscriptions, _, _), ctrl| {
                Box::pin(async move {
                    handle_family_control_message(
                        runtime_state,
                        extractors,
                        subscriptions,
                        ctrl,
                    )
                    .await
                })
            },
            |(runtime_state, extractors, subscriptions, partial_blocks, loop_state), stream_item| {
                Box::pin(async move {
                    handle_family_stream_item(
                        loop_state,
                        runtime_state,
                        extractors,
                        subscriptions,
                        *partial_blocks,
                        stream_item,
                    )
                    .await
                    .map_err(|err| {
                        tracing::Span::current().record("otel.status_code", "error");
                        err
                    })
                })
            },
        )
        .await
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
    partial_block_tracker: &mut PartialBlockTracker,
    response: BlockResponse,
) -> Result<RuntimeLoopControlFlow, ExtractionError> {
    runtime_block_response_dispatch! {
        response,
        new => |data| {
            partial_block_tracker.on_new_block(&data);

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
            partial_block_tracker.reset();
            let pending_msgs =
                process_reverts_for_extractors(extractors, subscriptions, &undo_signal).await?;
            propagate_pending_subscription_messages(pending_msgs).await;
            Ok(RuntimeLoopControlFlow::Continue)
        },
        ended => {
            flush_extractors(extractors.values()).await?;
            Ok(RuntimeLoopControlFlow::Stop)
        },
    }
}

pub(crate) async fn handle_family_stream_item(
    loop_state: &mut ManagedRuntimeLoopState,
    runtime_state: &mut FamilyRuntimeState,
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    subscriptions: &BranchSubscriptionsMap,
    partial_blocks: bool,
    stream_item: Option<Result<BlockResponse, anyhow::Error>>,
) -> Result<(u64, RuntimeLoopControlFlow), ExtractionError> {
    let family_id = loop_state.loop_id().to_string();
    handle_logged_runtime_stream_item(
        stream_item,
        || stream_ended_error(&family_id),
        |response| async move {
            handle_family_block_response(
                &mut runtime_state.dispatcher,
                &runtime_state.protocol_cache,
                extractors,
                subscriptions,
                partial_blocks,
                loop_state.partial_block_tracker_mut(),
                response,
            )
            .await
        },
        |err| error!(error = %err, "Family stream terminated with error."),
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
    let pending_msgs = process_branch_payloads_for_extractors(
        extractors,
        subscriptions,
        partial_blocks,
        branch_payloads,
    )
    .await?;
    propagate_pending_subscription_messages(pending_msgs).await;
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
    let subscriber_id = allocate_logged_subscription_id(
        next_subscriber_id,
        Some(&extractor_id),
        "New family branch subscription",
    );

    let subscription_key = branch_subscription_index.resolve_or_learn(&extractor_id, extractors);

    if let Some(subscription_key) = subscription_key {
        let subscribers = subscriptions
            .get(&subscription_key)
            .expect("resolved family subscription key should exist");
        register_subscription(subscribers, subscriber_id, sender).await;
    } else {
        tracing::warn!(?extractor_id, "Ignoring subscription for unknown family branch extractor");
    }
}
