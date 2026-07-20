use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    bootstrap_lifecycle::resolve_resume_start_block,
    family_runner_wiring::FamilyBootstrapCommitWiring,
    family_bootstrap_registry::ResolvedSharedBootstrapRuntime,
    family_runtime::ResolvedFamilyRuntime,
    runtime_target_planning::ResolvedStandaloneRuntime,
    substreams_package_loader::{load_substreams_package, LoadedSubstreamsPackage},
    shared_bootstrap::{
        execute_materialized_bootstrap_plan, BootstrapBranchDescriptor,
        MaterializedBootstrapCommitTarget,
    },
    ExtractionError, Extractor,
};
use crate::substreams::stream::SubstreamsStream;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedSubstreamsRequest {
    pub request: crate::extractor::runtime_target_planning::ResolvedSubstreamsExecutionRequest,
    pub cursor: Option<String>,
}

impl PreparedSubstreamsRequest {
    pub(crate) fn build_stream(
        &self,
        loaded_substreams: LoadedSubstreamsPackage,
        final_block_only: bool,
        partial_blocks: bool,
    ) -> SubstreamsStream {
        SubstreamsStream::new(
            loaded_substreams.endpoint,
            self.cursor.clone(),
            Some(loaded_substreams.spkg),
            self.request.module.clone(),
            self.request.start_block,
            self.request.stop_block,
            final_block_only,
            self.request.extractor_id.clone(),
            partial_blocks,
            self.request.params.clone(),
        )
    }

    pub(crate) async fn load_stream(
        &self,
        s3_bucket: Option<&str>,
        endpoint_url: &str,
        substreams_api_token: &str,
        final_block_only: bool,
        partial_blocks: bool,
    ) -> Result<SubstreamsStream, ExtractionError> {
        let loaded_substreams = load_substreams_package(
            s3_bucket,
            &self.request.spkg,
            endpoint_url,
            Some(substreams_api_token.to_string()),
        )
        .await?;

        Ok(self.build_stream(
            loaded_substreams,
            final_block_only,
            partial_blocks,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedStreamPosition {
    pub start_block: i64,
    pub cursor: Option<String>,
}

#[derive(Clone)]
pub(crate) struct FamilyPreparedRequestContext {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    shared_extractor_id: String,
    durability_scope: String,
    configured_start_block: i64,
    pub(crate) shared_bootstrap: Option<PreparedSharedBootstrap>,
}

#[derive(Clone)]
pub(crate) struct StandalonePreparedRequestContext {
    pub(crate) extractor: Arc<dyn Extractor>,
    pub(crate) startup_scope_id: String,
    pub(crate) shared_bootstrap: Option<PreparedSharedBootstrap>,
}

#[derive(Clone)]
pub(crate) struct PreparedSharedBootstrap {
    runtime: ResolvedSharedBootstrapRuntime,
    commit_mode: PreparedBootstrapCommitMode,
}

#[derive(Clone)]
enum PreparedBootstrapCommitMode {
    Family(FamilyBootstrapCommitWiring),
    Standalone(Arc<dyn Extractor>),
}

#[async_trait]
pub(crate) trait PreparedBootstrapExecution: Sync {
    fn bootstrap_block(&self) -> u64;

    fn branches(&self) -> &[BootstrapBranchDescriptor];

    async fn execute(
        &self,
        rpc_client: &EthereumRpcClient,
    ) -> Result<(), ExtractionError>;
}

pub(crate) trait PreparedBootstrapContextView {
    fn startup_scope_id(&self) -> &str;

    fn startup_scope_kind(&self) -> &'static str;

    fn prepared_bootstrap_execution(&self) -> Option<&PreparedSharedBootstrap>;
}

pub(crate) trait PreparedSubstreamsRequestRuntimeView {
    fn prepared_substreams_request_with_stream_position(
        &self,
        start_block: i64,
        cursor: Option<String>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError>;
}

impl PreparedSubstreamsRequestRuntimeView for ResolvedFamilyRuntime<'_> {
    fn prepared_substreams_request_with_stream_position(
        &self,
        start_block: i64,
        cursor: Option<String>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        ResolvedFamilyRuntime::prepared_substreams_request_with_stream_position(
            self,
            start_block,
            cursor,
        )
    }
}

impl PreparedSubstreamsRequestRuntimeView for ResolvedStandaloneRuntime<'_> {
    fn prepared_substreams_request_with_stream_position(
        &self,
        start_block: i64,
        cursor: Option<String>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        ResolvedStandaloneRuntime::prepared_substreams_request_with_stream_position(
            self,
            start_block,
            cursor,
        )
    }
}

impl PreparedSharedBootstrap {
    pub(crate) fn for_family(
        runtime: ResolvedSharedBootstrapRuntime,
        commit_wiring: FamilyBootstrapCommitWiring,
    ) -> Self {
        Self { runtime, commit_mode: PreparedBootstrapCommitMode::Family(commit_wiring) }
    }

    pub(crate) fn for_standalone(
        runtime: ResolvedSharedBootstrapRuntime,
        extractor: Arc<dyn Extractor>,
    ) -> Self {
        Self { runtime, commit_mode: PreparedBootstrapCommitMode::Standalone(extractor) }
    }

    fn branch_targets(&self) -> Vec<MaterializedBootstrapCommitTarget> {
        match &self.commit_mode {
            PreparedBootstrapCommitMode::Family(commit_wiring) => commit_wiring.branch_targets(),
            PreparedBootstrapCommitMode::Standalone(extractor) => {
                vec![MaterializedBootstrapCommitTarget::whole_block(extractor.clone())]
            }
        }
    }

    fn completion_extractor(&self) -> Arc<dyn Extractor> {
        match &self.commit_mode {
            PreparedBootstrapCommitMode::Family(commit_wiring) => {
                commit_wiring.completion_extractor()
            }
            PreparedBootstrapCommitMode::Standalone(extractor) => extractor.clone(),
        }
    }
}

#[async_trait]
impl PreparedBootstrapExecution for PreparedSharedBootstrap {
    fn bootstrap_block(&self) -> u64 {
        self.runtime.plan.bootstrap_block
    }

    fn branches(&self) -> &[BootstrapBranchDescriptor] {
        &self.runtime.plan.branches
    }

    async fn execute(
        &self,
        rpc_client: &EthereumRpcClient,
    ) -> Result<(), ExtractionError> {
        execute_materialized_bootstrap_plan(
            rpc_client,
            &self.runtime.plan,
            &self.runtime.execution,
            self.branch_targets(),
            self.completion_extractor(),
        )
        .await
    }
}

impl FamilyPreparedRequestContext {
    pub(crate) fn shared_extractor_id(&self) -> &str {
        &self.shared_extractor_id
    }

    pub(crate) fn durability_scope(&self) -> &str {
        &self.durability_scope
    }

    pub(crate) fn configured_start_block(&self) -> i64 {
        self.configured_start_block
    }
}

impl PreparedBootstrapContextView for FamilyPreparedRequestContext {
    fn startup_scope_id(&self) -> &str {
        self.shared_extractor_id()
    }

    fn startup_scope_kind(&self) -> &'static str {
        "family"
    }

    fn prepared_bootstrap_execution(&self) -> Option<&PreparedSharedBootstrap> {
        self.shared_bootstrap.as_ref()
    }
}

impl PreparedBootstrapContextView for StandalonePreparedRequestContext {
    fn startup_scope_id(&self) -> &str {
        &self.startup_scope_id
    }

    fn startup_scope_kind(&self) -> &'static str {
        "extractor"
    }

    fn prepared_bootstrap_execution(&self) -> Option<&PreparedSharedBootstrap> {
        self.shared_bootstrap.as_ref()
    }
}

impl ResolvedFamilyRuntime<'_> {
    pub(crate) fn prepared_request_context(
        &self,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Result<FamilyPreparedRequestContext, ExtractionError> {
        let shared_bootstrap = self
            .shared_bootstrap_runtime()
            .cloned()
            .map(|runtime| {
                Ok::<PreparedSharedBootstrap, ExtractionError>(
                    PreparedSharedBootstrap::for_family(
                    runtime,
                    FamilyBootstrapCommitWiring::from_runtime_contract(
                        self.runtime_contract(),
                        extractors,
                    )?,
                ))
            })
            .transpose()?;
        Ok(FamilyPreparedRequestContext {
            extractors: extractors.clone(),
            shared_extractor_id: self.runtime_contract().shared_extractor_id().to_string(),
            durability_scope: self.runtime_contract().durability_scope().to_string(),
            configured_start_block: self.shared_runtime.configured_start_block,
            shared_bootstrap,
        })
    }
}

fn prepared_substreams_request_from_stream_position(
    runtime: &impl PreparedSubstreamsRequestRuntimeView,
    stream_position: ResolvedStreamPosition,
) -> Result<PreparedSubstreamsRequest, ExtractionError> {
    runtime.prepared_substreams_request_with_stream_position(
        stream_position.start_block,
        stream_position.cursor,
    )
}

pub(crate) fn resolved_stream_position_from_resume(
    last_processed_block: Option<u64>,
    default_start_block: i64,
    cursor: Option<String>,
) -> Result<ResolvedStreamPosition, ExtractionError> {
    let start_block = resolve_resume_start_block(last_processed_block, default_start_block)?;
    Ok(ResolvedStreamPosition { start_block, cursor })
}

#[async_trait]
pub(crate) trait PreparedSubstreamsRequestLifecycleView<C>:
    PreparedSubstreamsRequestRuntimeView + Sync
{
    async fn prepare_stream_position_for_prepared_request(
        &self,
        context: &C,
        rpc_client: &EthereumRpcClient,
    ) -> Result<ResolvedStreamPosition, ExtractionError>;
}

pub(crate) async fn prepare_substreams_request_for_runtime_target<R, C>(
    runtime: &R,
    context: &C,
    rpc_client: &EthereumRpcClient,
) -> Result<PreparedSubstreamsRequest, ExtractionError>
where
    R: PreparedSubstreamsRequestLifecycleView<C>,
{
    let stream_position = runtime
        .prepare_stream_position_for_prepared_request(context, rpc_client)
        .await?;
    prepared_substreams_request_from_stream_position(runtime, stream_position)
}

#[async_trait]
impl PreparedSubstreamsRequestLifecycleView<FamilyPreparedRequestContext> for ResolvedFamilyRuntime<'_> {
    async fn prepare_stream_position_for_prepared_request(
        &self,
        context: &FamilyPreparedRequestContext,
        rpc_client: &EthereumRpcClient,
    ) -> Result<ResolvedStreamPosition, ExtractionError> {
        self.prepare_stream_position_after_bootstrap(context, rpc_client)
            .await
    }
}

#[async_trait]
impl PreparedSubstreamsRequestLifecycleView<StandalonePreparedRequestContext>
    for ResolvedStandaloneRuntime<'_>
{
    async fn prepare_stream_position_for_prepared_request(
        &self,
        context: &StandalonePreparedRequestContext,
        rpc_client: &EthereumRpcClient,
    ) -> Result<ResolvedStreamPosition, ExtractionError> {
        self.prepare_stream_position_after_bootstrap(
            context,
            rpc_client,
        )
        .await
    }
}
