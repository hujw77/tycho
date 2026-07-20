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
use crate::substreams::stream::SubstreamsStream;

pub(crate) trait ManagedRunnerFactory: Send + 'static {
    fn into_managed_runner(
        self: Box<Self>,
        stream: SubstreamsStream,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError>;
}

pub(crate) struct PreparedManagedRuntimeOwner<S, C> {
    runner_factory: S,
    prepared_request_context: C,
}

impl<S, C> PreparedManagedRuntimeOwner<S, C> {
    pub(crate) fn new(runner_factory: S, prepared_request_context: C) -> Self {
        Self { runner_factory, prepared_request_context }
    }

    pub(crate) fn prepared_request_context_mut(&mut self) -> &mut C {
        &mut self.prepared_request_context
    }
}

impl<S, C> ManagedRunnerFactory for PreparedManagedRuntimeOwner<S, C>
where
    S: ManagedRunnerFactory,
    C: Send + Sync + 'static,
{
    fn into_managed_runner(
        self: Box<Self>,
        stream: SubstreamsStream,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        Box::new(self.runner_factory)
            .into_managed_runner(stream, runtime, partial_blocks)
    }
}

pub(crate) struct PreparedRuntimeTargetStartup {
    runner_factory: Box<dyn ManagedRunnerFactory>,
    stream: SubstreamsStream,
}

impl PreparedRuntimeTargetStartup {
    pub(crate) fn new<O>(runner_factory: O, stream: SubstreamsStream) -> Self
    where
        O: ManagedRunnerFactory,
    {
        Self { runner_factory: Box::new(runner_factory), stream }
    }

    pub(crate) fn build_managed_runner(
        self,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        self.runner_factory
            .into_managed_runner(self.stream, runtime, partial_blocks)
    }
}

#[async_trait]
pub(crate) trait PreparedRuntimeTargetDraftOwner: Send + 'static {
    async fn into_prepared_startup(
        self: Box<Self>,
        context: &ResolvedRuntimeTargetsBuildContext<'_>,
    ) -> Result<PreparedRuntimeTargetStartup, ExtractionError>;
}

pub(crate) struct PreparedRuntimeTargetDraft {
    inner: Box<dyn PreparedRuntimeTargetDraftOwner>,
}

impl PreparedRuntimeTargetDraft {
    pub(crate) fn new<D>(draft: D) -> Self
    where
        D: PreparedRuntimeTargetDraftOwner,
    {
        Self { inner: Box::new(draft) }
    }

    pub(crate) async fn into_prepared_startup(
        self,
        context: &ResolvedRuntimeTargetsBuildContext<'_>,
    ) -> Result<PreparedRuntimeTargetStartup, ExtractionError> {
        self.inner.into_prepared_startup(context).await
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
    pub(crate) family_targets: Vec<PreparedRuntimeTargetStartup>,
    pub(crate) standalone_targets: Vec<PreparedRuntimeTargetStartup>,
    runtime: Option<Handle>,
    partial_blocks: bool,
}

pub struct BuiltManagedRunnersBatch {
    pub(crate) family_runners: Vec<ManagedRunner>,
    pub(crate) standalone_runners: Vec<ManagedRunner>,
    pub(crate) extractor_handles: Vec<ExtractorHandle>,
}

impl BuiltManagedRunnersBatch {
    pub(crate) fn new(
        family_runners: Vec<ManagedRunner>,
        standalone_runners: Vec<ManagedRunner>,
        extractor_handles: Vec<ExtractorHandle>,
    ) -> Self {
        Self { family_runners, standalone_runners, extractor_handles }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn total_runners(&self) -> usize {
        self.family_runners.len() + self.standalone_runners.len()
    }

    pub fn into_parts(self) -> (Vec<ManagedRunner>, Vec<ManagedRunner>, Vec<ExtractorHandle>) {
        (self.family_runners, self.standalone_runners, self.extractor_handles)
    }

    pub(crate) fn into_flattened(self) -> (Vec<ManagedRunner>, Vec<ExtractorHandle>) {
        let mut runners = self.family_runners;
        runners.extend(self.standalone_runners);
        (runners, self.extractor_handles)
    }
}

impl PreparedRuntimeTargetsStartup {
    pub(crate) fn new(
        family_targets: Vec<PreparedRuntimeTargetStartup>,
        standalone_targets: Vec<PreparedRuntimeTargetStartup>,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Self {
        Self { family_targets, standalone_targets, runtime, partial_blocks }
    }

    pub(crate) fn total_targets(&self) -> usize {
        self.family_targets.len() + self.standalone_targets.len()
    }

    pub(crate) fn build_managed_runners(
        self,
    ) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
        self.build_managed_runners_batch()
            .map(BuiltManagedRunnersBatch::into_flattened)
    }

    pub(crate) fn build_managed_runners_batch(
        self,
    ) -> Result<BuiltManagedRunnersBatch, ExtractionError> {
        let mut family_runners = Vec::new();
        let mut standalone_runners = Vec::new();
        let mut extractor_handles = Vec::new();

        let runtime = self.runtime;
        let partial_blocks = self.partial_blocks;
        for prepared_target in self.family_targets {
            let (runner, handles) =
                prepared_target.build_managed_runner(runtime.clone(), partial_blocks)?;
            family_runners.push(runner);
            extractor_handles.extend(handles);
        }
        for prepared_target in self.standalone_targets {
            let (runner, handles) =
                prepared_target.build_managed_runner(runtime.clone(), partial_blocks)?;
            standalone_runners.push(runner);
            extractor_handles.extend(handles);
        }

        Ok(BuiltManagedRunnersBatch::new(
            family_runners,
            standalone_runners,
            extractor_handles,
        ))
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

        let mut family_targets = Vec::new();
        let mut standalone_targets = Vec::new();
        for target in runtime_targets.as_slice().iter().cloned() {
            let is_family = matches!(target, ResolvedRuntimeTarget::Family(_));
            let prepared_target = target
                .prepare_managed_startup(self, &protocol_cache)
                .await?;
            if is_family {
                family_targets.push(prepared_target);
            } else {
                standalone_targets.push(prepared_target);
            }
        }

        Ok(PreparedRuntimeTargetsStartup::new(
            family_targets,
            standalone_targets,
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
    ) -> Result<PreparedRuntimeTargetDraft, ExtractionError> {
        let extractor_build = context.extractor_build_context(protocol_cache);
        match self {
            Self::Family(family) =>
                <crate::extractor::family_runtime::ResolvedFamilyRuntime<'_> as ManagedStartupLifecycleView>::prepare_managed_startup_draft(
                    &family,
                    extractor_build,
                )
                .await
                .map(PreparedRuntimeTargetDraft::new),
            Self::Standalone(standalone) =>
                <crate::extractor::runtime_target_planning::ResolvedStandaloneRuntime<'_> as ManagedStartupLifecycleView>::prepare_managed_startup_draft(
                    &standalone,
                    extractor_build,
                )
                .await
                .map(PreparedRuntimeTargetDraft::new),
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
        draft.into_prepared_startup(context).await
    }
}

#[async_trait]
pub(crate) trait ManagedStartupLifecycleView<'a>: Sized + Sync {
    type RuntimeOwner: ManagedRunnerFactory + ManagedStartupPreparedRequestContext + Send + Sync + 'static;

    async fn build_managed_runtime_owner(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<Self::RuntimeOwner, ExtractionError>;

    async fn prepare_substreams_request_for_managed_startup(
        &self,
        runtime_owner: &Self::RuntimeOwner,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError>
    where
        Self:
            PreparedSubstreamsRequestLifecycleView<
                <Self::RuntimeOwner as ManagedStartupPreparedRequestContext>::PreparedRequestContext,
            >,
    {
        prepare_managed_startup_request_from_owner(self, runtime_owner, extractor_build).await
    }

    async fn prepare_managed_startup_draft(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedManagedStartupDraft<Self::RuntimeOwner>, ExtractionError>
    where
        Self:
            PreparedSubstreamsRequestLifecycleView<
                <Self::RuntimeOwner as ManagedStartupPreparedRequestContext>::PreparedRequestContext,
            >,
    {
        let runtime_owner = self
            .build_managed_runtime_owner(extractor_build.clone())
            .await?;
        let prepared_request = self
            .prepare_substreams_request_for_managed_startup(&runtime_owner, extractor_build)
            .await?;
        Ok(PreparedManagedStartupDraft::new(runtime_owner, prepared_request))
    }
}

pub(crate) struct PreparedManagedStartupDraft<O> {
    pub(crate) runtime_owner: O,
    pub(crate) prepared_request: PreparedSubstreamsRequest,
}

impl<O> PreparedManagedStartupDraft<O>
where
    O: ManagedRunnerFactory,
{
    pub(crate) fn new(runtime_owner: O, prepared_request: PreparedSubstreamsRequest) -> Self {
        Self { runtime_owner, prepared_request }
    }
}

pub(crate) async fn prepare_managed_startup_request_from_owner<R, O>(
    runtime: &R,
    runtime_owner: &O,
    extractor_build: ManagedExtractorBuildContext<'_>,
) -> Result<PreparedSubstreamsRequest, ExtractionError>
where
    R: PreparedSubstreamsRequestLifecycleView<O::PreparedRequestContext>,
    O: ManagedStartupPreparedRequestContext,
{
    let context = runtime_owner.prepared_request_context();
    prepare_substreams_request_for_runtime_target(runtime, context, extractor_build.rpc_client)
        .await
}

pub(crate) trait ManagedStartupPreparedRequestContext {
    type PreparedRequestContext: Send + Sync;

    fn prepared_request_context(&self) -> &Self::PreparedRequestContext;
}

impl<S, C> ManagedStartupPreparedRequestContext for PreparedManagedRuntimeOwner<S, C>
where
    S: ManagedRunnerFactory,
    C: Send + Sync + 'static,
{
    type PreparedRequestContext = C;

    fn prepared_request_context(&self) -> &Self::PreparedRequestContext {
        &self.prepared_request_context
    }
}

#[async_trait]
impl<O> PreparedRuntimeTargetDraftOwner for PreparedManagedStartupDraft<O>
where
    O: ManagedRunnerFactory,
{
    async fn into_prepared_startup(
        self: Box<Self>,
        context: &ResolvedRuntimeTargetsBuildContext<'_>,
    ) -> Result<PreparedRuntimeTargetStartup, ExtractionError> {
        let stream = self.prepared_request.load_stream(
            context.s3_bucket,
            context.endpoint_url,
            context.substreams_api_token,
            context.final_block_only,
            context.partial_blocks,
        )
        .await?;
        Ok(PreparedRuntimeTargetStartup::new(self.runtime_owner, stream))
    }
}
