use std::{collections::HashMap, sync::Arc};

use crate::{
    extractor::{
        control::{BranchSubscriptionsMap, ControlMessage, ExtractorHandle},
        family_dispatch::FamilyBlockChangesDispatcher,
        family_runner_wiring::FamilyBranchRuntimeWiring,
        family_runtime_planning::ResolvedFamilyRuntime,
        family_runtime_execution::FamilyRuntimeState,
        managed_extractor_initialization::ManagedExtractorBuildContext,
        managed_substreams_request::PreparedSubstreamsRequest,
        runner::{FamilyExtractorRunner, ManagedRunner},
        ExtractionError, Extractor,
    },
    substreams::stream::SubstreamsStream,
};
use tokio::{runtime::Handle, sync::mpsc};

pub(crate) struct PreparedFamilyRunnerStartup {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) stream: SubstreamsStream,
    pub(crate) runtime_state: FamilyRuntimeState,
}

impl PreparedFamilyRunnerStartup {
    pub(crate) async fn from_prepared_request(
        extractor_build: &ManagedExtractorBuildContext<'_>,
        extractors: HashMap<String, Arc<dyn Extractor>>,
        runtime_state: FamilyRuntimeState,
        prepared_request: PreparedSubstreamsRequest,
    ) -> Result<Self, ExtractionError> {
        let stream = extractor_build
            .load_stream_for_prepared_request(&prepared_request)
            .await?;
        Ok(Self { extractors, stream, runtime_state })
    }

    pub(crate) fn into_managed_runner(
        self,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        let (ctrl_tx, ctrl_rx) = mpsc::channel(128);
        let wiring = FamilyBranchRuntimeWiring::from_extractors(self.extractors, &ctrl_tx);
        let runner = crate::extractor::runner::FamilyExtractorRunner::new(
            wiring.extractors,
            self.stream,
            wiring.subscriptions,
            ctrl_rx,
            runtime,
            partial_blocks,
            self.runtime_state,
        );

        Ok((ManagedRunner::new(runner), wiring.handles))
    }
}

pub(crate) fn family_auxiliary_protocol_message_decoders_by_protocol_system<'a>(
    family: &'a ResolvedFamilyRuntime<'a>,
) -> &'a HashMap<
    String,
    Vec<crate::extractor::protocol_message_registry::AuxiliaryProtocolMessageDecoder>,
> {
    &family
        .execution
        .auxiliary_protocol_message_decoders_by_protocol_system
}

async fn build_extractors_for_family(
    family: &ResolvedFamilyRuntime<'_>,
    extractor_build: &ManagedExtractorBuildContext<'_>,
) -> Result<HashMap<String, Arc<dyn Extractor>>, ExtractionError> {
    extractor_build
        .build_protocol_system_keyed_extractors(
            &family.extractor_configs,
            family_auxiliary_protocol_message_decoders_by_protocol_system(family),
        )
        .await
}

impl<'a> ResolvedFamilyRuntime<'a> {
    pub(crate) async fn prepare_managed_startup(
        self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedFamilyRunnerStartup, ExtractionError> {
        let extractors = build_extractors_for_family(&self, &extractor_build).await?;
        let dispatcher = FamilyBlockChangesDispatcher::from_protocol_cache(
            &self.execution.branch_specs,
            extractor_build.protocol_cache,
        )
        .await?;
        let runtime_state = FamilyRuntimeState::new(
            &extractors,
            dispatcher,
            extractor_build.protocol_cache.clone(),
        );
        let prepared_request = self
            .prepare_substreams_request(&extractors, extractor_build.rpc_client)
            .await?;
        PreparedFamilyRunnerStartup::from_prepared_request(
            &extractor_build,
            extractors,
            runtime_state,
            prepared_request,
        )
        .await
    }
}
impl FamilyExtractorRunner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        extractors: HashMap<String, Arc<dyn Extractor>>,
        substreams: SubstreamsStream,
        subscriptions: BranchSubscriptionsMap,
        control_rx: tokio::sync::mpsc::Receiver<ControlMessage>,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
        runtime_state: FamilyRuntimeState,
    ) -> Self {
        Self {
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
