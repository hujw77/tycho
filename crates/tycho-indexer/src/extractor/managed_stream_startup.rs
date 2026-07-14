use std::{collections::HashMap, sync::Arc};

use tokio::{
    runtime::Handle,
    sync::{mpsc, Mutex},
};

use crate::{
    extractor::{
        control::{ExtractorHandle, SubscriptionsMap},
        managed_extractor_initialization::ManagedExtractorBuildContext,
        managed_substreams_request::PreparedSubstreamsRequest,
        runner::ManagedRunner,
        single_runtime_execution::ExtractorRunner,
        substreams_package_loader::{load_substreams_package, LoadedSubstreamsPackage},
        ExtractionError, Extractor,
    },
    substreams::stream::SubstreamsStream,
};

#[cfg(test)]
use crate::extractor::{
    extractor_config::ExtractorConfig,
    family_registry::default_family_runtime_registry,
    runtime_target_planning::ResolvedStandaloneRuntime,
};

pub(crate) struct PreparedSingleRunnerStartup {
    pub(crate) extractor: Arc<dyn Extractor>,
    pub(crate) extractor_id: tycho_common::models::ExtractorIdentity,
    pub(crate) stream: SubstreamsStream,
}

impl PreparedSingleRunnerStartup {
    pub(crate) async fn from_prepared_request(
        extractor_build: &ManagedExtractorBuildContext<'_>,
        extractor: Arc<dyn Extractor>,
        extractor_id: tycho_common::models::ExtractorIdentity,
        prepared_request: PreparedSubstreamsRequest,
    ) -> Result<Self, ExtractionError> {
        let stream = extractor_build
            .load_stream_for_prepared_request(&prepared_request)
            .await?;
        Ok(Self { extractor, extractor_id, stream })
    }

    pub(crate) fn into_managed_runner(
        self,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
    ) -> (ManagedRunner, Vec<ExtractorHandle>) {
        let (ctrl_tx, ctrl_rx) = mpsc::channel(128);
        let subscriptions: SubscriptionsMap = HashMap::new();
        let runner = ExtractorRunner::new(
            self.extractor,
            self.stream,
            Arc::new(Mutex::new(subscriptions)),
            ctrl_rx,
            runtime_handle,
            partial_blocks,
        );

        (
            ManagedRunner::new(runner),
            vec![ExtractorHandle::new(self.extractor_id, ctrl_tx)],
        )
    }
}

pub(crate) fn build_substreams_stream_from_prepared_request(
    prepared_request: &PreparedSubstreamsRequest,
    loaded_substreams: LoadedSubstreamsPackage,
    final_block_only: bool,
    partial_blocks: bool,
) -> SubstreamsStream {
    SubstreamsStream::new(
        loaded_substreams.endpoint,
        prepared_request.cursor.clone(),
        Some(loaded_substreams.spkg),
        prepared_request.request.module.clone(),
        prepared_request.request.start_block,
        prepared_request.request.stop_block,
        final_block_only,
        prepared_request
            .request
            .extractor_id
            .clone(),
        partial_blocks,
        prepared_request.request.params.clone(),
    )
}

pub(crate) async fn load_stream_for_prepared_request(
    prepared_request: &PreparedSubstreamsRequest,
    s3_bucket: Option<&str>,
    endpoint_url: &str,
    substreams_api_token: &str,
    final_block_only: bool,
    partial_blocks: bool,
) -> Result<SubstreamsStream, ExtractionError> {
    let loaded_substreams = load_substreams_package(
        s3_bucket,
        &prepared_request.request.spkg,
        endpoint_url,
        Some(substreams_api_token.to_string()),
    )
    .await?;

    Ok(build_substreams_stream_from_prepared_request(
        prepared_request,
        loaded_substreams,
        final_block_only,
        partial_blocks,
    ))
}

impl ManagedExtractorBuildContext<'_> {
    pub(crate) async fn load_stream_for_prepared_request(
        &self,
        prepared_request: &PreparedSubstreamsRequest,
    ) -> Result<SubstreamsStream, ExtractionError> {
        load_stream_for_prepared_request(
            prepared_request,
            self.s3_bucket,
            self.endpoint_url,
            self.substreams_api_token,
            self.final_block_only,
            self.partial_blocks,
        )
        .await
    }
}

#[cfg(test)]
pub(crate) async fn build_test_single_runner(
    config: &ExtractorConfig,
    extractor: Arc<dyn Extractor>,
    endpoint_url: &str,
    s3_bucket: Option<&str>,
    substreams_api_token: &str,
    final_block_only: bool,
    partial_blocks: bool,
    runtime_handle: Option<Handle>,
) -> Result<(ExtractorRunner, ExtractorHandle), ExtractionError> {
    let extractor_id = extractor.get_id();
    let prepared_request = ResolvedStandaloneRuntime {
        protocol_system: config.protocol_system(),
        extractor_config: config,
    }
    .prepare_substreams_request(
        extractor.clone(),
        &extractor_id,
        &tycho_ethereum::rpc::EthereumRpcClient::new("http://localhost:0000")
            .expect("build stub rpc client for request preparation"),
        default_family_runtime_registry(),
    )
    .await?;
    let stream = load_stream_for_prepared_request(
        &prepared_request,
        s3_bucket,
        endpoint_url,
        substreams_api_token,
        final_block_only,
        partial_blocks,
    )
    .await?;
    let prepared_startup = PreparedSingleRunnerStartup { extractor, extractor_id, stream };
    let (runner, mut handles) =
        prepared_startup.into_managed_runner(runtime_handle, partial_blocks);
    let runner: ExtractorRunner = runner.into_typed();
    Ok((
        runner,
        handles
            .pop()
            .expect("single runner handle"),
    ))
}
