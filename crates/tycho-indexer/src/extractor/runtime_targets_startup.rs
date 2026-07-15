#[cfg(test)]
use std::any::Any;

use async_trait::async_trait;
use tokio::runtime::Handle;
use tracing::info;
use tycho_ethereum::{
    rpc::EthereumRpcClient, services::token_pre_processor::EthereumTokenPreProcessor,
};
use tycho_storage::postgres::cache::CachedGateway;

use crate::extractor::{
    chain_state::ChainState, control::ExtractorHandle,
    family_registry::FamilyRuntimeRegistry,
    managed_extractor_initialization::ManagedExtractorBuildContext,
    managed_stream_startup::load_stream_for_prepared_request,
    managed_substreams_request::{
        prepare_substreams_request_for_runtime_target, PreparedSubstreamsRequest,
        PreparedSubstreamsRequestLifecycleView,
    },
    protocol_cache::ProtocolMemoryCache,
    runner::ManagedRunner,
    runtime_target_planning::{ResolvedRuntimeTarget, ResolvedRuntimeTargets},
    startup::initialize_runtime_target_accounts,
    ExtractionError,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreparedRuntimeTargetKind {
    Family,
    Standalone,
}

pub(crate) struct PreparedRuntimeTargetStartup {
    startup: Box<dyn PreparedManagedRunnerStartup>,
}

pub(crate) trait PreparedManagedRunnerStartup: Send {
    fn build_managed_runner(
        self: Box<Self>,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError>;

    #[cfg(test)]
    fn kind(&self) -> PreparedRuntimeTargetKind;

    #[cfg(test)]
    fn as_any(&self) -> &dyn Any;

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl PreparedRuntimeTargetStartup {
    #[cfg(test)]
    pub(crate) fn new(startup: impl Into<Self>) -> Self {
        startup.into()
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> PreparedRuntimeTargetKind {
        self.startup.kind()
    }

    #[cfg(test)]
    pub(crate) fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.startup.as_any().downcast_ref::<T>()
    }

    #[cfg(test)]
    pub(crate) fn into_typed<T: 'static>(self) -> T {
        *self
            .startup
            .into_any()
            .downcast::<T>()
            .expect("prepared runtime target startup should contain the requested concrete type")
    }

    pub(crate) fn build_managed_runner(
        self,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        self.startup
            .build_managed_runner(runtime, partial_blocks)
    }
}

impl<T> From<T> for PreparedRuntimeTargetStartup
where
    T: PreparedManagedRunnerStartup + 'static,
{
    fn from(startup: T) -> Self {
        Self { startup: Box::new(startup) }
    }
}

#[derive(Clone)]
pub struct ResolvedRuntimeTargetsBuildContext<'a> {
    pub chain_state: ChainState,
    pub endpoint_url: &'a str,
    pub s3_bucket: Option<&'a str>,
    pub substreams_api_token: &'a str,
    pub cached_gw: &'a CachedGateway,
    pub database_insert_batch_size: usize,
    pub token_pre_processor: &'a EthereumTokenPreProcessor,
    pub rpc_client: &'a EthereumRpcClient,
    pub runtime: Option<Handle>,
    pub final_block_only: bool,
    pub partial_blocks: bool,
    pub family_runtime_registry: FamilyRuntimeRegistry<'static>,
}

impl<'a> ResolvedRuntimeTargetsBuildContext<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_state: ChainState,
        endpoint_url: &'a str,
        s3_bucket: Option<&'a str>,
        substreams_api_token: &'a str,
        cached_gw: &'a CachedGateway,
        database_insert_batch_size: usize,
        token_pre_processor: &'a EthereumTokenPreProcessor,
        rpc_client: &'a EthereumRpcClient,
        runtime: Option<Handle>,
        final_block_only: bool,
        partial_blocks: bool,
        family_runtime_registry: FamilyRuntimeRegistry<'static>,
    ) -> Self {
        Self {
            chain_state,
            endpoint_url,
            s3_bucket,
            substreams_api_token,
            cached_gw,
            database_insert_batch_size,
            token_pre_processor,
            rpc_client,
            runtime,
            final_block_only,
            partial_blocks,
            family_runtime_registry,
        }
    }
}

pub(crate) struct PreparedRuntimeTargetsStartup {
    pub(crate) prepared_targets: Vec<PreparedRuntimeTargetStartup>,
    runtime: Option<Handle>,
    partial_blocks: bool,
}

impl PreparedRuntimeTargetsStartup {
    pub(crate) fn new(
        prepared_targets: Vec<PreparedRuntimeTargetStartup>,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Self {
        Self { prepared_targets, runtime, partial_blocks }
    }

    pub(crate) fn build_managed_runners(
        self,
    ) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
        let mut runners = Vec::new();
        let mut extractor_handles = Vec::new();

        let runtime = self.runtime;
        let partial_blocks = self.partial_blocks;
        for prepared_target in self.prepared_targets {
            let (runner, handles) =
                prepared_target.build_managed_runner(runtime.clone(), partial_blocks)?;
            runners.push(runner);
            extractor_handles.extend(handles);
        }

        Ok((runners, extractor_handles))
    }
}

impl ResolvedRuntimeTargetsBuildContext<'_> {
    async fn protocol_cache_for_runtime_targets(
        &self,
        runtime_targets: &ResolvedRuntimeTargets<'_>,
    ) -> Result<ProtocolMemoryCache, ExtractionError> {
        let chain = runtime_targets
            .as_slice()
            .first()
            .map(ResolvedRuntimeTarget::chain)
            .expect("resolved runtime targets should not be empty");

        info!("Building protocol cache");
        let protocol_cache = ProtocolMemoryCache::new(
            chain,
            chrono::Duration::seconds(900),
            std::sync::Arc::new(self.cached_gw.clone()),
        );
        protocol_cache.populate().await?;
        Ok(protocol_cache)
    }

    async fn initialize_runtime_target_accounts(
        &self,
        runtime_targets: &ResolvedRuntimeTargets<'_>,
    ) {
        initialize_runtime_target_accounts(
            runtime_targets.coalesced_initialized_accounts_requests(),
            self.rpc_client,
            self.cached_gw,
        )
        .await;
    }

    fn extractor_build_context<'a>(
        &'a self,
        protocol_cache: &'a ProtocolMemoryCache,
    ) -> ManagedExtractorBuildContext<'a> {
        ManagedExtractorBuildContext {
            chain_state: self.chain_state,
            endpoint_url: self.endpoint_url,
            s3_bucket: self.s3_bucket,
            substreams_api_token: self.substreams_api_token,
            cached_gw: self.cached_gw,
            database_insert_batch_size: self.database_insert_batch_size,
            token_pre_processor: self.token_pre_processor,
            protocol_cache,
            rpc_client: self.rpc_client,
            partial_blocks: self.partial_blocks,
            family_runtime_registry: self.family_runtime_registry,
        }
    }

    pub(crate) async fn prepare_runtime_targets_startup(
        &self,
        runtime_targets: &ResolvedRuntimeTargets<'_>,
    ) -> Result<PreparedRuntimeTargetsStartup, ExtractionError> {
        let protocol_cache = self
            .protocol_cache_for_runtime_targets(runtime_targets)
            .await?;
        self.initialize_runtime_target_accounts(runtime_targets)
            .await;

        let mut prepared_targets = Vec::new();
        for target in runtime_targets.as_slice().iter().cloned() {
            prepared_targets.push(
                target
                    .prepare_managed_startup(self, &protocol_cache)
                    .await?,
            );
        }

        Ok(PreparedRuntimeTargetsStartup::new(
            prepared_targets,
            self.runtime.clone(),
            self.partial_blocks,
        ))
    }
}

impl<'a> ResolvedRuntimeTarget<'a> {
    pub(crate) async fn prepare_managed_startup_draft(
        self,
        context: &ResolvedRuntimeTargetsBuildContext<'_>,
        protocol_cache: &ProtocolMemoryCache,
    ) -> Result<Box<dyn PreparedRuntimeTargetDraft>, ExtractionError> {
        let extractor_build = context.extractor_build_context(protocol_cache);
        match self {
            Self::Family(family) => family
                .prepare_managed_startup_draft(extractor_build)
                .await
                .map(|draft| Box::new(draft) as Box<dyn PreparedRuntimeTargetDraft>),
            Self::Standalone(standalone) => standalone
                .prepare_managed_startup_draft(extractor_build)
                .await
                .map(|draft| Box::new(draft) as Box<dyn PreparedRuntimeTargetDraft>),
        }
    }

    pub(crate) async fn prepare_managed_startup(
        self,
        context: &ResolvedRuntimeTargetsBuildContext<'_>,
        protocol_cache: &ProtocolMemoryCache,
    ) -> Result<PreparedRuntimeTargetStartup, ExtractionError> {
        let draft = self
            .prepare_managed_startup_draft(context, protocol_cache)
            .await?;
        prepare_runtime_target_startup_from_draft(draft, context)
            .await
            .map(Into::into)
    }
}

pub(crate) trait PreparedRuntimeTargetDraft: Send {
    fn prepared_request(&self) -> &PreparedSubstreamsRequest;

    fn into_prepared_startup(
        self: Box<Self>,
        stream: crate::substreams::stream::SubstreamsStream,
    ) -> PreparedRuntimeTargetStartup;
}

pub(crate) trait PreparedManagedStartupPayload: Sized {
    type PreparedStartup: PreparedManagedRunnerStartup + 'static;

    fn into_prepared_startup(
        self,
        stream: crate::substreams::stream::SubstreamsStream,
    ) -> Self::PreparedStartup;
}

pub(crate) trait ManagedStartupPreparedRequestPayload: PreparedManagedStartupPayload {
    type PreparedRequestContext: Send + Sync;

    fn prepared_request_context(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Self::PreparedRequestContext;
}

#[async_trait]
pub(crate) trait ManagedStartupLifecycleView<'a>: Sized + Sync {
    type Payload: PreparedManagedStartupPayload + Send + Sync + 'static;

    async fn build_managed_startup_payload(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<Self::Payload, ExtractionError>;

    async fn prepare_substreams_request_for_managed_startup(
        &self,
        payload: &Self::Payload,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError>;

    async fn prepare_managed_startup_draft(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedManagedStartupDraft<Self::Payload>, ExtractionError> {
        let payload = self
            .build_managed_startup_payload(extractor_build.clone())
            .await?;
        let prepared_request = self
            .prepare_substreams_request_for_managed_startup(&payload, extractor_build)
            .await?;
        Ok(PreparedManagedStartupDraft::new(payload, prepared_request))
    }
}

pub(crate) struct PreparedManagedStartupDraft<P> {
    pub(crate) payload: P,
    pub(crate) prepared_request: PreparedSubstreamsRequest,
}

impl<P> PreparedManagedStartupDraft<P> {
    pub(crate) fn new(payload: P, prepared_request: PreparedSubstreamsRequest) -> Self {
        Self { payload, prepared_request }
    }
}

pub(crate) async fn prepare_managed_startup_request_from_payload<R, P>(
    runtime: &R,
    payload: &P,
    extractor_build: ManagedExtractorBuildContext<'_>,
) -> Result<PreparedSubstreamsRequest, ExtractionError>
where
    R: PreparedSubstreamsRequestLifecycleView<P::PreparedRequestContext>,
    P: ManagedStartupPreparedRequestPayload,
{
    let context = payload.prepared_request_context(extractor_build);
    prepare_substreams_request_for_runtime_target(runtime, &context, extractor_build.rpc_client)
        .await
}

impl<P> PreparedRuntimeTargetDraft for PreparedManagedStartupDraft<P>
where
    P: PreparedManagedStartupPayload + Send,
{
    fn prepared_request(&self) -> &PreparedSubstreamsRequest {
        &self.prepared_request
    }

    fn into_prepared_startup(
        self: Box<Self>,
        stream: crate::substreams::stream::SubstreamsStream,
    ) -> PreparedRuntimeTargetStartup {
        let this = *self;
        let prepared_startup = this.payload.into_prepared_startup(stream);
        prepared_startup.into()
    }
}

pub(crate) async fn prepare_runtime_target_startup_from_draft(
    draft: Box<dyn PreparedRuntimeTargetDraft>,
    context: &ResolvedRuntimeTargetsBuildContext<'_>,
) -> Result<PreparedRuntimeTargetStartup, ExtractionError> {
    let stream = load_stream_for_prepared_request(
        draft.prepared_request(),
        context.s3_bucket,
        context.endpoint_url,
        context.substreams_api_token,
        context.final_block_only,
        context.partial_blocks,
    )
    .await?;
    Ok(draft.into_prepared_startup(stream))
}
