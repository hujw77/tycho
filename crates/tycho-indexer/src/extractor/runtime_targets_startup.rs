use tokio::runtime::Handle;
use tycho_ethereum::{
    rpc::EthereumRpcClient, services::token_pre_processor::EthereumTokenPreProcessor,
};
use tycho_storage::postgres::cache::CachedGateway;

use crate::extractor::{
    chain_state::ChainState,
    control::ExtractorHandle,
    family_managed_startup::PreparedFamilyRunnerStartup,
    family_registry::FamilyRuntimeRegistry,
    managed_extractor_initialization::ManagedExtractorBuildContext,
    managed_stream_startup::PreparedSingleRunnerStartup,
    protocol_cache::ProtocolMemoryCache,
    runner::ManagedRunner,
    runtime_target_planning::ResolvedRuntimeTarget,
    ExtractionError,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreparedRuntimeTargetKind {
    Family,
    Standalone,
}

pub(crate) trait PreparedManagedRunnerStartupView: Send {
    fn build_managed_runner(
        self: Box<Self>,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError>;

    #[cfg(test)]
    fn kind(&self) -> PreparedRuntimeTargetKind;
}

impl PreparedManagedRunnerStartupView for PreparedFamilyRunnerStartup {
    fn build_managed_runner(
        self: Box<Self>,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        (*self).into_managed_runner(runtime, partial_blocks)
    }

    #[cfg(test)]
    fn kind(&self) -> PreparedRuntimeTargetKind {
        PreparedRuntimeTargetKind::Family
    }
}

impl PreparedManagedRunnerStartupView for PreparedSingleRunnerStartup {
    fn build_managed_runner(
        self: Box<Self>,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        Ok((*self).into_managed_runner(runtime, partial_blocks))
    }

    #[cfg(test)]
    fn kind(&self) -> PreparedRuntimeTargetKind {
        PreparedRuntimeTargetKind::Standalone
    }
}

pub(crate) struct PreparedRuntimeTargetStartup {
    startup: Box<dyn PreparedManagedRunnerStartupView>,
}

impl PreparedRuntimeTargetStartup {
    pub(crate) fn new(startup: impl PreparedManagedRunnerStartupView + 'static) -> Self {
        Self { startup: Box::new(startup) }
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> PreparedRuntimeTargetKind {
        self.startup.kind()
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

impl From<PreparedFamilyRunnerStartup> for PreparedRuntimeTargetStartup {
    fn from(startup: PreparedFamilyRunnerStartup) -> Self {
        Self::new(startup)
    }
}

impl From<PreparedSingleRunnerStartup> for PreparedRuntimeTargetStartup {
    fn from(startup: PreparedSingleRunnerStartup) -> Self {
        Self::new(startup)
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
            final_block_only: self.final_block_only,
            partial_blocks: self.partial_blocks,
            family_runtime_registry: self.family_runtime_registry,
        }
    }
}

impl<'a> ResolvedRuntimeTarget<'a> {
    pub(crate) async fn prepare_managed_startup(
        self,
        context: &ResolvedRuntimeTargetsBuildContext<'_>,
        protocol_cache: &ProtocolMemoryCache,
    ) -> Result<PreparedRuntimeTargetStartup, ExtractionError> {
        let extractor_build = context.extractor_build_context(protocol_cache);
        match self {
            Self::Family(family) => Ok(family.prepare_managed_startup(extractor_build).await?.into()),
            Self::Standalone(standalone) => {
                Ok(standalone.prepare_managed_startup(extractor_build).await?.into())
            }
        }
    }
}
