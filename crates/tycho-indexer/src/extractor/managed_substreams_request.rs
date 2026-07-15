use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tycho_common::models::ExtractorIdentity;
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    bootstrap_lifecycle::resolve_resume_start_block,
    family_runner_wiring::FamilyBootstrapCommitWiring,
    family_registry::FamilyRuntimeRegistry,
    family_runtime_planning::ResolvedFamilyRuntime,
    runtime_target_planning::ResolvedStandaloneRuntime,
    ExtractionError, Extractor,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedSubstreamsRequest {
    pub request: crate::extractor::runtime_target_planning::ResolvedSubstreamsExecutionRequest,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedStreamPosition {
    pub start_block: i64,
    pub cursor: Option<String>,
}

pub(crate) struct FamilyPreparedRequestContext {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) bootstrap_commit_wiring: FamilyBootstrapCommitWiring,
}

pub(crate) struct StandalonePreparedRequestContext {
    pub(crate) extractor: Arc<dyn Extractor>,
    pub(crate) extractor_id: ExtractorIdentity,
    pub(crate) registry: FamilyRuntimeRegistry<'static>,
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
            context.extractor.clone(),
            &context.extractor_id,
            rpc_client,
            context.registry,
        )
        .await
    }
}
