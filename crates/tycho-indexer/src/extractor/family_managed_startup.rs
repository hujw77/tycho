use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use crate::{
    extractor::{
        control::{BranchSubscriptionsMap, ControlMessage, ExtractorHandle},
        family_dispatch::FamilyBlockChangesDispatcher,
        family_runner_wiring::FamilyBranchRuntimeWiring,
        family_runtime::ResolvedFamilyRuntime,
        family_runtime_execution::FamilyRuntimeState,
        family_runtime_resolution::ResolvedFamilyRuntimeContract,
        protocol_cache::ProtocolMemoryCache,
        managed_extractor_initialization::ManagedExtractorBuildContext,
        managed_substreams_request::FamilyPreparedRequestContext,
        runtime_targets_startup::{
            ManagedRunnerFactory, ManagedStartupLifecycleView, PreparedManagedRuntimeOwner,
        },
        runner::{FamilyExtractorRunner, ManagedRunner},
        ExtractionError, Extractor,
    },
    substreams::stream::SubstreamsStream,
};
use tokio::runtime::Handle;

#[derive(Clone)]
pub(crate) struct FamilyRuntimeRunnerFactory {
    extractors: HashMap<String, Arc<dyn Extractor>>,
    runtime_contract: ResolvedFamilyRuntimeContract,
    runtime_state: FamilyRuntimeState,
}

pub(crate) type PreparedFamilyRuntimeOwner =
    PreparedManagedRuntimeOwner<FamilyRuntimeRunnerFactory, FamilyPreparedRequestContext>;

impl FamilyRuntimeRunnerFactory {
    fn new(
        extractors: HashMap<String, Arc<dyn Extractor>>,
        runtime_contract: ResolvedFamilyRuntimeContract,
        runtime_state: FamilyRuntimeState,
    ) -> Self {
        Self { extractors, runtime_contract, runtime_state }
    }
}

pub(crate) async fn prepared_family_runtime_owner_for_runtime(
    runtime: &ResolvedFamilyRuntime<'_>,
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    protocol_cache: ProtocolMemoryCache,
) -> Result<PreparedFamilyRuntimeOwner, ExtractionError> {
    let prepared_request_context = runtime.prepared_request_context(extractors)?;
    let runtime_contract = runtime.runtime_contract().clone();
    let dispatcher = FamilyBlockChangesDispatcher::from_protocol_cache_for_runtime_contract(
        &runtime_contract,
        &protocol_cache,
    )
    .await?;
    Ok(PreparedManagedRuntimeOwner::new(
        FamilyRuntimeRunnerFactory::new(
            extractors.clone(),
            runtime_contract.clone(),
            FamilyRuntimeState::new(
                &runtime_contract,
                extractors,
                dispatcher,
                protocol_cache,
            ),
        ),
        prepared_request_context,
    ))
}

impl ManagedRunnerFactory for FamilyRuntimeRunnerFactory {
    fn into_managed_runner(
        self: Box<Self>,
        stream: SubstreamsStream,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        let wiring = FamilyBranchRuntimeWiring::from_extractors(self.extractors);
        let runner = crate::extractor::runner::FamilyExtractorRunner::new(
            self.runtime_contract.clone(),
            wiring.extractors,
            stream,
            wiring.subscriptions,
            wiring.control.control_rx,
            runtime,
            partial_blocks,
            self.runtime_state,
        );

        Ok((ManagedRunner::new_family(runner), wiring.control.handles))
    }
}

#[async_trait]
impl<'a> ManagedStartupLifecycleView<'a> for ResolvedFamilyRuntime<'a> {
    type RuntimeOwner = PreparedFamilyRuntimeOwner;

    async fn build_managed_runtime_owner(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<Self::RuntimeOwner, ExtractionError> {
        let extractors = extractor_build.build_runtime_target_extractors(self).await?;
        prepared_family_runtime_owner_for_runtime(
            self,
            &extractors,
            extractor_build.protocol_cache.clone(),
        )
        .await
    }
}
impl FamilyExtractorRunner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        runtime_contract: ResolvedFamilyRuntimeContract,
        extractors: HashMap<String, Arc<dyn Extractor>>,
        substreams: SubstreamsStream,
        subscriptions: BranchSubscriptionsMap,
        control_rx: tokio::sync::mpsc::Receiver<ControlMessage>,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
        runtime_state: FamilyRuntimeState,
    ) -> Self {
        Self {
            runtime_contract,
            runtime_state,
            extractors,
            substreams,
            subscriptions,
            control_rx,
            runtime_handle,
            partial_blocks,
        }
    }
}
