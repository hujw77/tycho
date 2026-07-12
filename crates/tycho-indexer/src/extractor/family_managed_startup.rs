use std::{collections::HashMap, sync::Arc};

use tokio::{runtime::Handle, sync::mpsc};
use tycho_ethereum::{
    rpc::EthereumRpcClient,
    services::token_pre_processor::EthereumTokenPreProcessor,
};
use tycho_storage::postgres::cache::CachedGateway;

use crate::{
    extractor::{
        chain_state::ChainState,
        family_dispatch::FamilyBlockChangesDispatcher,
        family_runner_wiring::{
            extractors_by_protocol_system, FamilyBranchRuntimeWiring,
        },
        family_runtime_execution::FamilyRuntimeState,
        family_runtime::ResolvedFamilyRuntime,
        protocol_cache::ProtocolMemoryCache,
        runner::{
            load_substreams_package, BranchSubscriptionsMap, ControlMessage, ExtractorBuilder,
            ExtractorHandle, FamilyExtractorRunner, ManagedRunner,
        },
        ExtractionError, Extractor,
    },
    substreams::stream::SubstreamsStream,
};

pub(crate) struct PreparedFamilyRunnerStartup {
    pub(crate) extractors: HashMap<String, Arc<dyn Extractor>>,
    pub(crate) stream: SubstreamsStream,
    pub(crate) runtime_state: FamilyRuntimeState,
}

#[derive(Clone, Copy)]
pub(crate) struct FamilyRuntimeBuildContext<'a> {
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
}

async fn build_extractors_for_family(
    family: &ResolvedFamilyRuntime<'_>,
    context: &FamilyRuntimeBuildContext<'_>,
) -> Result<HashMap<String, Arc<dyn Extractor>>, ExtractionError> {
    let mut builders = Vec::with_capacity(family.extractor_configs.len());

    for extractor_config in &family.extractor_configs {
        let builder = ExtractorBuilder::new(
            extractor_config,
            context.endpoint_url,
            context.s3_bucket,
            context.substreams_api_token,
        )
        .database_insert_batch_size(context.database_insert_batch_size)
        .auxiliary_protocol_message_decoders(
            family
                .execution
                .auxiliary_protocol_message_decoders_by_protocol_system
                .get(extractor_config.protocol_system())
                .cloned()
                .unwrap_or_default(),
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
        builders.push(builder);
    }

    Ok(extractors_by_protocol_system(builders))
}

pub(crate) async fn prepare_family_managed_startup(
    family: ResolvedFamilyRuntime<'_>,
    context: FamilyRuntimeBuildContext<'_>,
    final_block_only: bool,
) -> Result<PreparedFamilyRunnerStartup, ExtractionError> {
    let extractors = build_extractors_for_family(&family, &context).await?;
    let dispatcher = FamilyBlockChangesDispatcher::from_protocol_cache(
        &family.execution.branch_specs,
        context.protocol_cache,
    )
    .await?;
    let runtime_state = FamilyRuntimeState::new(
        &extractors,
        dispatcher,
        context.protocol_cache.clone(),
    );
    let prepared_request = family
        .prepare_substreams_request(&extractors, context.rpc_client)
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
        final_block_only,
        prepared_request.request.extractor_id.clone(),
        context.partial_blocks,
        prepared_request.request.params.clone(),
    );

    Ok(PreparedFamilyRunnerStartup {
        extractors,
        stream,
        runtime_state,
    })
}

pub(crate) async fn build_family_managed_runner(
    family: ResolvedFamilyRuntime<'_>,
    context: FamilyRuntimeBuildContext<'_>,
    runtime: Option<Handle>,
    partial_blocks: bool,
    final_block_only: bool,
) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
    let prepared_startup =
        prepare_family_managed_startup(family, context, final_block_only).await?;
    build_family_managed_runner_from_startup(prepared_startup, runtime, partial_blocks).await
}

pub(crate) async fn build_family_managed_runner_from_startup(
    prepared_startup: PreparedFamilyRunnerStartup,
    runtime: Option<Handle>,
    partial_blocks: bool,
) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
    let (ctrl_tx, ctrl_rx) = mpsc::channel(128);
    let wiring = FamilyBranchRuntimeWiring::from_extractors(prepared_startup.extractors, &ctrl_tx);
    let runner = crate::extractor::runner::FamilyExtractorRunner::new(
        wiring.extractors,
        prepared_startup.stream,
        wiring.subscriptions,
        ctrl_rx,
        runtime,
        partial_blocks,
        prepared_startup.runtime_state,
    );

    Ok((ManagedRunner::Family(runner), wiring.handles))
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
