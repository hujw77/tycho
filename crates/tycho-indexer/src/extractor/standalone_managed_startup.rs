use tokio::runtime::Handle;
use tycho_ethereum::{
    rpc::EthereumRpcClient,
    services::token_pre_processor::EthereumTokenPreProcessor,
};
use tycho_storage::postgres::cache::CachedGateway;

use crate::{
    extractor::{
        chain_state::ChainState,
        family_runtime::default_auxiliary_protocol_message_decoders_for_protocol_system,
        protocol_cache::ProtocolMemoryCache,
        runner::{
            load_substreams_package, ExtractorBuilder, ExtractorHandle, ManagedRunner,
            PreparedSingleRunnerStartup,
        },
        ExtractionError,
    },
    substreams::stream::SubstreamsStream,
};

#[derive(Clone, Copy)]
pub(crate) struct StandaloneRuntimeBuildContext<'a> {
    pub(crate) chain_state: ChainState,
    pub(crate) endpoint_url: &'a str,
    pub(crate) s3_bucket: Option<&'a str>,
    pub(crate) substreams_api_token: &'a str,
    pub(crate) cached_gw: &'a CachedGateway,
    pub(crate) database_insert_batch_size: usize,
    pub(crate) token_pre_processor: &'a EthereumTokenPreProcessor,
    pub(crate) protocol_cache: &'a ProtocolMemoryCache,
    pub(crate) rpc_client: &'a EthereumRpcClient,
    pub(crate) partial_blocks: bool,
    pub(crate) final_block_only: bool,
}

pub(crate) async fn prepare_standalone_managed_startup(
    extractor_config: &crate::extractor::runner::ExtractorConfig,
    context: StandaloneRuntimeBuildContext<'_>,
) -> Result<PreparedSingleRunnerStartup, ExtractionError> {
    let builder = ExtractorBuilder::new(
        extractor_config,
        context.endpoint_url,
        context.s3_bucket,
        context.substreams_api_token,
    )
    .database_insert_batch_size(context.database_insert_batch_size)
    .auxiliary_protocol_message_decoders(
        default_auxiliary_protocol_message_decoders_for_protocol_system(
            extractor_config.protocol_system(),
        ),
    )
    .partial_blocks(context.partial_blocks)
    .build(
        context.chain_state,
        context.cached_gw,
        context.token_pre_processor,
        context.protocol_cache,
        context.rpc_client,
    )
    .await?;
    let extractor = builder.initialized_extractor();
    let extractor_id = extractor.get_id();
    let prepared_request = builder
        .prepare_substreams_request(extractor.clone(), &extractor_id)
        .await?;
    let loaded_substreams = load_substreams_package(
        context.s3_bucket,
        &prepared_request.request.spkg,
        context.endpoint_url,
        Some(context.substreams_api_token.to_string()),
    )
    .await?;

    let stream = SubstreamsStream::new(
        loaded_substreams.endpoint,
        prepared_request.cursor.clone(),
        Some(loaded_substreams.spkg),
        prepared_request.request.module.clone(),
        prepared_request.request.start_block,
        prepared_request.request.stop_block,
        context.final_block_only,
        prepared_request.request.extractor_id.clone(),
        context.partial_blocks,
        prepared_request.request.params.clone(),
    );

    Ok(PreparedSingleRunnerStartup {
        extractor,
        extractor_id,
        stream,
    })
}

pub(crate) fn build_standalone_managed_runner_from_startup(
    prepared_startup: PreparedSingleRunnerStartup,
    runtime: Option<Handle>,
    partial_blocks: bool,
) -> (ManagedRunner, Vec<ExtractorHandle>) {
    let (runner, handle) =
        ExtractorBuilder::build_from_startup(prepared_startup, runtime, partial_blocks);
    (ManagedRunner::Single(runner), vec![handle])
}
