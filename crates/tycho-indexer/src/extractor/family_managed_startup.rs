use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use crate::{
    extractor::{
        control::{BranchSubscriptionsMap, ControlMessage, ExtractorHandle},
        family_dispatch::FamilyBlockChangesDispatcher,
        family_runner_wiring::{FamilyBootstrapCommitWiring, FamilyBranchRuntimeWiring},
        family_runtime_execution::FamilyRuntimeState,
        family_runtime_planning::ResolvedFamilyRuntime,
        managed_extractor_initialization::ManagedExtractorBuildContext,
        managed_substreams_request::FamilyPreparedRequestContext,
        runtime_targets_startup::{
            prepare_managed_startup_request_from_payload, ManagedStartupLifecycleView,
            ManagedStartupPreparedRequestPayload, PreparedManagedRunnerStartup,
            PreparedManagedStartupDraft, PreparedManagedStartupPayload,
        },
        runner::{FamilyExtractorRunner, ManagedRunner},
        ExtractionError, Extractor,
    },
    substreams::stream::SubstreamsStream,
};
use tokio::{runtime::Handle, sync::mpsc};

pub(crate) type PreparedFamilyRunnerDraft = PreparedManagedStartupDraft<PreparedFamilyRunnerPayload>;

pub(crate) struct PreparedFamilyRunnerPayload {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) runtime_contract:
        crate::extractor::family_runtime_planning::ResolvedFamilyRuntimeContract,
    pub(crate) runtime_state: FamilyRuntimeState,
    pub(crate) bootstrap_commit_wiring: FamilyBootstrapCommitWiring,
}

pub(crate) struct PreparedFamilyRunnerStartup {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) runtime_contract:
        crate::extractor::family_runtime_planning::ResolvedFamilyRuntimeContract,
    pub(crate) stream: SubstreamsStream,
    pub(crate) runtime_state: FamilyRuntimeState,
}

impl PreparedManagedStartupPayload for PreparedFamilyRunnerPayload {
    type PreparedStartup = PreparedFamilyRunnerStartup;

    fn into_prepared_startup(
        self,
        stream: SubstreamsStream,
    ) -> PreparedFamilyRunnerStartup {
        PreparedFamilyRunnerStartup {
            extractors: self.extractors,
            runtime_contract: self.runtime_contract,
            stream,
            runtime_state: self.runtime_state,
        }
    }
}

impl ManagedStartupPreparedRequestPayload for PreparedFamilyRunnerPayload {
    type PreparedRequestContext = FamilyPreparedRequestContext;

    fn prepared_request_context(
        &self,
        _extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Self::PreparedRequestContext {
        FamilyPreparedRequestContext {
            extractors: self.extractors.clone(),
            bootstrap_commit_wiring: self.bootstrap_commit_wiring.clone(),
        }
    }
}

impl PreparedManagedRunnerStartup for PreparedFamilyRunnerStartup {
    fn build_managed_runner(
        self: Box<Self>,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        let this = *self;
        let (ctrl_tx, ctrl_rx) = mpsc::channel(128);
        let wiring = FamilyBranchRuntimeWiring::from_extractors(this.extractors, &ctrl_tx);
        let runner = crate::extractor::runner::FamilyExtractorRunner::new(
            this.runtime_contract,
            wiring.extractors,
            this.stream,
            wiring.subscriptions,
            ctrl_rx,
            runtime,
            partial_blocks,
            this.runtime_state,
        );

        Ok((ManagedRunner::new(runner), wiring.handles))
    }

    #[cfg(test)]
    fn kind(&self) -> crate::extractor::runtime_targets_startup::PreparedRuntimeTargetKind {
        crate::extractor::runtime_targets_startup::PreparedRuntimeTargetKind::Family
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

impl<'a> ResolvedFamilyRuntime<'a> {
    pub(crate) async fn prepare_managed_startup_draft(
        self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedFamilyRunnerDraft, ExtractionError> {
        <Self as ManagedStartupLifecycleView>::prepare_managed_startup_draft(
            &self,
            extractor_build,
        )
        .await
    }
}

#[async_trait]
impl<'a> ManagedStartupLifecycleView<'a> for ResolvedFamilyRuntime<'a> {
    type Payload = PreparedFamilyRunnerPayload;

    async fn build_managed_startup_payload(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedFamilyRunnerPayload, ExtractionError> {
        let extractors = extractor_build.build_runtime_target_extractors(self).await?;
        let runtime_contract = self.runtime_contract();
        let dispatcher = FamilyBlockChangesDispatcher::from_protocol_cache_for_runtime_contract(
            &runtime_contract,
            extractor_build.protocol_cache,
        )
        .await?;
        let runtime_state = FamilyRuntimeState::new(
            &runtime_contract,
            &extractors,
            dispatcher,
            extractor_build.protocol_cache.clone(),
        );
        let bootstrap_commit_wiring =
            FamilyBootstrapCommitWiring::from_runtime_contract(&runtime_contract, &extractors)?;
        Ok(PreparedFamilyRunnerPayload {
            extractors,
            runtime_contract,
            runtime_state,
            bootstrap_commit_wiring,
        })
    }

    async fn prepare_substreams_request_for_managed_startup(
        &self,
        payload: &Self::Payload,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<crate::extractor::managed_substreams_request::PreparedSubstreamsRequest, ExtractionError>
    {
        prepare_managed_startup_request_from_payload(self, payload, extractor_build).await
    }
}
impl FamilyExtractorRunner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        runtime_contract: crate::extractor::family_runtime_planning::ResolvedFamilyRuntimeContract,
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
