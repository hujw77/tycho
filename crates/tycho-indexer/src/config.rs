use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use actix_web::dev::ServerHandle;
use chrono::NaiveDateTime;
use serde::Deserialize;
use tokio::{
    runtime::Handle,
    signal::unix::{signal, SignalKind},
    task::{AbortHandle, JoinHandle},
};
use tracing::info;
use tycho_common::models::{Chain, ImplementationType};
use tycho_common::storage::Gateway;
use tycho_ethereum::{
    rpc::EthereumRpcClient, services::token_pre_processor::EthereumTokenPreProcessor,
};
use tycho_indexer::extractor::extractor_config::{
    BootstrapConfig, BootstrapStrategy, DCIType, ExtractorConfig, ProtocolTypeConfig,
};
#[cfg(test)]
use tycho_indexer::extractor::shared_config::{
    parse_bootstrap_params_file, parse_bootstrap_params_yaml_with_filter_and_registry,
    parse_substreams_params_file, parse_substreams_params_yaml_with_registry,
};
#[cfg(test)]
use tycho_indexer::extractor::extractor_config::extractor_config_by_protocol_system;
use tycho_indexer::extractor::ExtractionError;
use tycho_indexer::extractor::{
    chain_state::ChainState,
    control::ExtractorHandle,
    family_registry::default_family_runtime_registry,
    family_registry::FamilyRuntimeRegistry,
    family_runtime_metadata::{canonicalize_shared_route_protocol, FamilyRuntimeConfig},
    family_runtime_types::FamilyRuntimeBuildPlan,
    shared_bootstrap::SharedBootstrapPlan,
    runtime_target_planning::ResolvedRuntimeTargets,
    shared_config::{
        protocol_filter_for_protocol_system, resolve_bootstrap_params,
        resolve_substreams_params_map,
    },
    startup::ResolvedRuntimeTargetsBuildContext,
};
use tycho_indexer::services::PlansConfig;
use tycho_indexer::services::ServicesBuilder;
use tycho_storage::postgres::{builder::GatewayBuilder, cache::CachedGateway};

#[derive(Debug, Deserialize)]
pub(crate) struct ExtractorConfigs {
    pub(crate) extractors: HashMap<String, ExtractorConfig>,
    #[serde(skip, default = "default_family_runtime_registry")]
    family_runtime_registry: FamilyRuntimeRegistry<'static>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedIndexerServiceConfig {
    protocol_systems: Vec<String>,
    dci_protocol_systems: Vec<String>,
}

pub(crate) struct ResolvedIndexerRuntimePlan<'a> {
    runtime_targets: ResolvedRuntimeTargets<'a>,
    service_config: ResolvedIndexerServiceConfig,
    family_runtime_registry: FamilyRuntimeRegistry<'static>,
}

pub(crate) struct LoadedIndexerRuntimePlan {
    extractors_config: ExtractorConfigs,
}

#[derive(Debug)]
pub(crate) struct ResolvedServiceLaunchConfig {
    api_key: String,
    prefix: String,
    bind: String,
    port: u16,
    plans_config: PlansConfig,
}

pub(crate) struct ManagedServerTasks {
    pub(crate) server_url: String,
    pub(crate) server_task: JoinHandle<Result<(), ExtractionError>>,
    pub(crate) shutdown_task: JoinHandle<Result<(), ExtractionError>>,
    server_handle: ServerHandle,
    db_write_executor_abort: Option<AbortHandle>,
}

pub(crate) struct ManagedExtractionTasksBatch {
    pub(crate) family_extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>,
    pub(crate) standalone_extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>,
}

impl ManagedExtractionTasksBatch {
    pub(crate) fn new(
        family_extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>,
        standalone_extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>,
    ) -> Self {
        Self { family_extraction_tasks, standalone_extraction_tasks }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn total_tasks(&self) -> usize {
        self.family_extraction_tasks.len() + self.standalone_extraction_tasks.len()
    }

    pub(crate) fn into_flattened(self) -> Vec<JoinHandle<Result<(), ExtractionError>>> {
        let mut tasks = self.family_extraction_tasks;
        tasks.extend(self.standalone_extraction_tasks);
        tasks
    }

    pub(crate) fn from_managed_runners(
        family_runners: Vec<tycho_indexer::extractor::runner::ManagedRunner>,
        standalone_runners: Vec<tycho_indexer::extractor::runner::ManagedRunner>,
    ) -> Self {
        Self::new(
            family_runners.into_iter().map(|runner| runner.run()).collect(),
            standalone_runners.into_iter().map(|runner| runner.run()).collect(),
        )
    }
}

#[cfg_attr(test, allow(dead_code))]
pub(crate) struct ManagedIndexerTaskGroups {
    pub(crate) extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>,
    pub(crate) service_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>,
}

pub(crate) struct ManagedIndexerTasks {
    extraction_tasks: ManagedExtractionTasksBatch,
    service_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>,
    #[cfg_attr(not(test), allow(dead_code))]
    server_handle: ServerHandle,
    #[cfg_attr(not(test), allow(dead_code))]
    db_write_executor_abort: Option<AbortHandle>,
}

#[derive(Clone)]
pub(crate) struct ResolvedIndexerTaskContext<'a> {
    pub(crate) database_url: &'a str,
    pub(crate) chains: &'a [Chain],
    pub(crate) retention_horizon: NaiveDateTime,
    pub(crate) endpoint_url: &'a str,
    pub(crate) s3_bucket: Option<&'a str>,
    pub(crate) substreams_api_token: &'a str,
    pub(crate) database_insert_batch_size: usize,
    pub(crate) settlement_contract: alloy::primitives::Address,
    pub(crate) extraction_runtime: Option<Handle>,
    pub(crate) partial_blocks: bool,
}

impl<'a> ResolvedIndexerTaskContext<'a> {
    pub(crate) fn runtime_targets_build_context<'b>(
        &'b self,
        family_runtime_registry: FamilyRuntimeRegistry<'static>,
        chain_state: ChainState,
        cached_gw: &'b CachedGateway,
        token_pre_processor: &'b EthereumTokenPreProcessor,
        rpc_client: &'b EthereumRpcClient,
    ) -> ResolvedRuntimeTargetsBuildContext<'b> {
        ResolvedRuntimeTargetsBuildContext::new(
            chain_state,
            self.endpoint_url,
            self.s3_bucket,
            self.substreams_api_token,
            cached_gw,
            self.database_insert_batch_size,
            token_pre_processor,
            rpc_client,
            self.extraction_runtime.clone(),
            false,
            self.partial_blocks,
            family_runtime_registry,
        )
    }
}

impl ResolvedIndexerServiceConfig {
    pub(crate) fn from_runtime_targets(runtime_targets: &ResolvedRuntimeTargets<'_>) -> Self {
        Self {
            protocol_systems: runtime_targets.protocol_systems(),
            dci_protocol_systems: runtime_targets.dci_protocol_systems(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self { protocol_systems: Vec::new(), dci_protocol_systems: Vec::new() }
    }

    pub(crate) fn protocol_systems(&self) -> &[String] {
        &self.protocol_systems
    }

    pub(crate) fn dci_protocol_systems(&self) -> &[String] {
        &self.dci_protocol_systems
    }

    pub(crate) async fn build_gateway(
        &self,
        database_url: &str,
        chains: &[Chain],
        retention_horizon: NaiveDateTime,
    ) -> Result<(CachedGateway, tokio::task::JoinHandle<()>), ExtractionError> {
        GatewayBuilder::new(database_url)
            .set_chains(chains)
            .set_protocol_systems(self.protocol_systems())
            .set_retention_horizon(retention_horizon)
            .build()
            .await
            .map_err(Into::into)
    }

    pub(crate) fn configure_services_builder<G>(
        &self,
        builder: ServicesBuilder<G>,
        handles: Vec<ExtractorHandle>,
    ) -> ServicesBuilder<G>
    where
        G: Gateway + Send + Sync + 'static,
    {
        builder
            .dci_protocols(self.dci_protocol_systems().to_vec())
            .protocol_systems(self.protocol_systems().to_vec())
            .register_extractors(handles)
    }

    pub(crate) async fn start_services<G>(
        &self,
        db_gateway: G,
        rpc: EthereumRpcClient,
        api_key: String,
        handles: Vec<ExtractorHandle>,
        prefix: &str,
        bind: &str,
        port: u16,
        plans_config: tycho_indexer::services::PlansConfig,
    ) -> Result<(ServerHandle, JoinHandle<Result<(), ExtractionError>>), ExtractionError>
    where
        G: Gateway + Send + Sync + 'static,
    {
        self.configure_services_builder(ServicesBuilder::new(db_gateway, rpc, api_key), handles)
            .prefix(prefix)
            .bind(bind)
            .port(port)
            .plans_config(plans_config)
            .run()
            .await
    }
}

impl LoadedIndexerRuntimePlan {
    pub(crate) fn from_yaml_with_registry(
        path: &str,
        registry: FamilyRuntimeRegistry<'static>,
    ) -> Result<Self, tycho_indexer::extractor::ExtractionError> {
        let extractors_config =
            ExtractorConfigs::from_yaml_with_registry(path, registry).map_err(|err| {
                tycho_indexer::extractor::ExtractionError::Setup(format!(
                    "Failed to load extractors config. {err}"
                ))
            })?;
        Ok(Self { extractors_config })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn resolved_runtime_plan(
        &self,
    ) -> Result<ResolvedIndexerRuntimePlan<'_>, tycho_indexer::extractor::ExtractionError> {
        self.extractors_config
            .resolved_indexer_runtime_plan()
    }

    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn resolved_runtime_targets(
        &self,
    ) -> Result<ResolvedRuntimeTargets<'_>, tycho_indexer::extractor::ExtractionError> {
        self.extractors_config.resolved_runtime_targets()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn extractors_config(&self) -> &ExtractorConfigs {
        &self.extractors_config
    }

    pub(crate) async fn start_indexing(
        self,
        launch_config: &ResolvedServiceLaunchConfig,
        rpc_client: EthereumRpcClient,
        context: ResolvedIndexerTaskContext<'_>,
    ) -> Result<ManagedIndexerTasks, ExtractionError> {
        self.extractors_config
            .resolved_indexer_runtime_plan()?
            .start_indexing(launch_config, rpc_client, context)
            .await
    }
}

impl<'a> ResolvedIndexerRuntimePlan<'a> {
    pub(crate) fn new(
        runtime_targets: ResolvedRuntimeTargets<'a>,
        family_runtime_registry: FamilyRuntimeRegistry<'static>,
    ) -> Self {
        let service_config = ResolvedIndexerServiceConfig::from_runtime_targets(&runtime_targets);
        Self { runtime_targets, service_config, family_runtime_registry }
    }

    #[allow(dead_code)]
    pub(crate) fn service_config(&self) -> &ResolvedIndexerServiceConfig {
        &self.service_config
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn protocol_systems(&self) -> &[String] {
        self.service_config.protocol_systems()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn dci_protocol_systems(&self) -> &[String] {
        self.service_config
            .dci_protocol_systems()
    }

    #[allow(dead_code)]
    pub(crate) fn into_runtime_targets(self) -> ResolvedRuntimeTargets<'a> {
        self.runtime_targets
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn into_unique_runtime_target(
        self,
        context: &str,
    ) -> Result<
        tycho_indexer::extractor::runtime_target_planning::ResolvedRuntimeTarget<'a>,
        ExtractionError,
    > {
        self.runtime_targets
            .into_unique(context)
    }

    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn into_parts(
        self,
    ) -> (
        ResolvedIndexerServiceConfig,
        ResolvedRuntimeTargets<'a>,
        FamilyRuntimeRegistry<'static>,
    ) {
        (
            self.service_config,
            self.runtime_targets,
            self.family_runtime_registry,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn build_managed_runners(
        self,
        context: ResolvedRuntimeTargetsBuildContext<'_>,
    ) -> Result<
        (Vec<tycho_indexer::extractor::runner::ManagedRunner>, Vec<ExtractorHandle>),
        ExtractionError,
    > {
        self.runtime_targets
            .build_managed_runners(context)
            .await
    }

    pub(crate) async fn start_indexing(
        self,
        launch_config: &ResolvedServiceLaunchConfig,
        rpc_client: EthereumRpcClient,
        context: ResolvedIndexerTaskContext<'_>,
    ) -> Result<ManagedIndexerTasks, ExtractionError> {
        let (service_config, runtime_targets, family_runtime_registry) = self.into_parts();
        launch_config
            .start_indexing_tasks(
                &service_config,
                runtime_targets,
                family_runtime_registry,
                rpc_client,
                context,
            )
            .await
    }
}

impl ResolvedServiceLaunchConfig {
    pub(crate) fn from_runtime_args(
        prefix: &str,
        bind: &str,
        port: u16,
    ) -> Result<Self, ExtractionError> {
        let api_key = env::var("AUTH_API_KEY").map_err(|_| {
            ExtractionError::Setup("AUTH_API_KEY environment variable is not set".to_string())
        })?;
        let plans_config =
            PlansConfig::from_yaml("./plans.yaml").map_err(ExtractionError::Setup)?;

        Ok(Self { api_key, prefix: prefix.to_string(), bind: bind.to_string(), port, plans_config })
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(
        api_key: impl Into<String>,
        prefix: impl Into<String>,
        bind: impl Into<String>,
        port: u16,
        plans_config: PlansConfig,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            prefix: prefix.into(),
            bind: bind.into(),
            port,
            plans_config,
        }
    }

    pub(crate) fn server_url(&self) -> String {
        format!("http://{}:{}", self.bind, self.port)
    }

    pub(crate) async fn start_services<G>(
        &self,
        service_config: &ResolvedIndexerServiceConfig,
        db_gateway: G,
        rpc: EthereumRpcClient,
        handles: Vec<ExtractorHandle>,
    ) -> Result<(ServerHandle, JoinHandle<Result<(), ExtractionError>>), ExtractionError>
    where
        G: Gateway + Send + Sync + 'static,
    {
        service_config
            .start_services(
                db_gateway,
                rpc,
                self.api_key.clone(),
                handles,
                &self.prefix,
                &self.bind,
                self.port,
                self.plans_config.clone(),
            )
            .await
    }

    pub(crate) async fn start_managed_server<G>(
        &self,
        service_config: &ResolvedIndexerServiceConfig,
        db_gateway: G,
        rpc: EthereumRpcClient,
        handles: Vec<ExtractorHandle>,
        db_write_executor_handle: Option<JoinHandle<()>>,
    ) -> Result<ManagedServerTasks, ExtractionError>
    where
        G: Gateway + Send + Sync + 'static,
    {
        let server_url = self.server_url();
        let (server_handle, server_task) = self
            .start_services(service_config, db_gateway, rpc, handles.clone())
            .await?;
        let shutdown_server_handle = server_handle.clone();
        let db_write_executor_abort = db_write_executor_handle
            .as_ref()
            .map(JoinHandle::abort_handle);
        let shutdown_task = tokio::spawn(shutdown_handler(
            shutdown_server_handle,
            handles,
            db_write_executor_handle,
        ));

        Ok(ManagedServerTasks {
            server_url,
            server_task,
            shutdown_task,
            server_handle,
            db_write_executor_abort,
        })
    }

    pub(crate) async fn start_indexing_tasks(
        &self,
        service_config: &ResolvedIndexerServiceConfig,
        runtime_targets: ResolvedRuntimeTargets<'_>,
        family_runtime_registry: FamilyRuntimeRegistry<'static>,
        rpc_client: EthereumRpcClient,
        context: ResolvedIndexerTaskContext<'_>,
    ) -> Result<ManagedIndexerTasks, ExtractionError> {
        let block_number = rpc_client
            .get_block_number()
            .await
            .map_err(|err| {
                ExtractionError::Unknown(format!("Error getting block number: {err}"))
            })?;

        let chain_state = ChainState::new(chrono::Local::now().naive_utc(), block_number, 12);

        let (cached_gw, gw_writer_handle) = service_config
            .build_gateway(context.database_url, context.chains, context.retention_horizon)
            .await?;
        let token_processor = EthereumTokenPreProcessor::new(
            &rpc_client,
            *context
                .chains
                .first()
                .expect("No chain provided"),
            context.settlement_contract,
        );

        let (family_runners, standalone_runners, extractor_handles) = runtime_targets
            .build_managed_runners_batch(context.runtime_targets_build_context(
                family_runtime_registry,
                chain_state,
                &cached_gw,
                &token_processor,
                &rpc_client,
            ))
            .await?
            .into_parts();

        let managed_server = self
            .start_managed_server(
                service_config,
                cached_gw,
                rpc_client,
                extractor_handles,
                Some(gw_writer_handle),
            )
            .await?;
        info!(server_url = managed_server.server_url, "Http and Ws server started");

        Ok(ManagedIndexerTasks {
            extraction_tasks: ManagedExtractionTasksBatch::from_managed_runners(
                family_runners,
                standalone_runners,
            ),
            service_tasks: vec![managed_server.server_task, managed_server.shutdown_task],
            server_handle: managed_server.server_handle,
            db_write_executor_abort: managed_server.db_write_executor_abort,
        })
    }

}

impl ManagedIndexerTasks {
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn into_task_groups(self) -> ManagedIndexerTaskGroups {
        ManagedIndexerTaskGroups {
            extraction_tasks: self.extraction_tasks.into_flattened(),
            service_tasks: self.service_tasks,
        }
    }

    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn into_all_tasks(self) -> Vec<JoinHandle<Result<(), ExtractionError>>> {
        let ManagedIndexerTaskGroups { mut extraction_tasks, mut service_tasks } =
            self.into_task_groups();
        extraction_tasks.append(&mut service_tasks);
        extraction_tasks
    }

    #[cfg(test)]
    pub(crate) async fn shutdown(mut self) {
        for task in self.extraction_tasks.into_flattened() {
            task.abort();
            let _ = task.await;
        }
        self.server_handle.stop(true).await;
        if let Some(handle) = self.db_write_executor_abort.take() {
            handle.abort();
        }
        for task in self.service_tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }
}

async fn shutdown_handler(
    server_handle: ServerHandle,
    extractors: Vec<ExtractorHandle>,
    db_write_executor_handle: Option<JoinHandle<()>>,
) -> Result<(), ExtractionError> {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| ExtractionError::Unknown(e.to_string()))?;

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("SIGINT (Ctrl+C) received. Cleaning up...");
        },
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received. Cleaning up...");
        },
    }

    for extractor in &extractors {
        if let Err(err) = extractor.stop().await {
            tracing::warn!(extractor_id = %extractor.get_id(), error = %err, "Failed to stop extractor cleanly");
        }
    }
    server_handle.stop(true).await;
    if let Some(handle) = db_write_executor_handle {
        handle.abort();
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct RawExtractorConfigs {
    #[serde(default)]
    includes: Vec<String>,
    #[serde(default)]
    family_runtimes: HashMap<String, RawFamilyRuntimeDefaults>,
    extractors: HashMap<String, RawExtractorConfig>,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct RawFamilyRuntimeDefaults {
    #[serde(default)]
    shared_spkg: Option<String>,
    #[serde(default)]
    shared_module: Option<String>,
    #[serde(default)]
    durability_scope: Option<String>,
    #[serde(default)]
    stop_block: Option<i64>,
    #[serde(default)]
    bootstrap: Option<RawFamilyBootstrapDefaults>,
    #[serde(default)]
    members: HashMap<String, RawFamilyMemberDefaults>,
}

impl RawFamilyRuntimeDefaults {
    fn resolve_family_runtime_config(
        &self,
        protocol_system: &str,
        family_runtime: FamilyRuntimeConfig,
        registry: FamilyRuntimeRegistry<'_>,
    ) -> Result<FamilyRuntimeConfig, Box<dyn std::error::Error>> {
        registry
            .resolve_family_runtime_config(
                protocol_system,
                family_runtime,
                self.shared_spkg.clone(),
                self.shared_module.clone(),
                self.durability_scope.clone(),
            )
            .map_err(Into::into)
    }
}

#[derive(Debug, Deserialize, Clone)]
struct RawFamilyBootstrapDefaults {
    params: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct RawFamilyMemberDefaults {
    #[serde(default)]
    substreams_params: HashMap<String, String>,
    #[serde(default)]
    shared_route_protocols: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawExtractorConfig {
    name: String,
    #[serde(default)]
    protocol_system: Option<String>,
    chain: Chain,
    implementation_type: ImplementationType,
    sync_batch_size: usize,
    start_block: Option<i64>,
    stop_block: Option<i64>,
    protocol_types: Vec<ProtocolTypeConfig>,
    #[serde(default)]
    spkg: Option<String>,
    module_name: String,
    #[serde(default)]
    initialized_accounts: Vec<tycho_common::Bytes>,
    #[serde(default)]
    initialized_accounts_block: u64,
    #[serde(default)]
    post_processor: Option<String>,
    #[serde(default)]
    dci_plugin: Option<DCIType>,
    #[serde(default)]
    substreams_params: HashMap<String, String>,
    #[serde(default)]
    bootstrap: Option<RawBootstrapConfig>,
    #[serde(default)]
    family_runtime: Option<FamilyRuntimeConfig>,
}

#[derive(Debug, Deserialize)]
struct RawBootstrapConfig {
    strategy: BootstrapStrategy,
    params: String,
    #[serde(skip)]
    start_block: Option<i64>,
}

impl ExtractorConfigs {
    pub(crate) fn new_with_registry(
        extractors: HashMap<String, ExtractorConfig>,
        family_runtime_registry: FamilyRuntimeRegistry<'static>,
    ) -> Self {
        Self { extractors, family_runtime_registry }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn resolved_runtime_targets(
        &self,
    ) -> Result<ResolvedRuntimeTargets<'_>, tycho_indexer::extractor::ExtractionError> {
        self.family_runtime_registry.resolve_runtime_targets(&self.extractors)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn family_runtime_plan(
        &self,
    ) -> Result<FamilyRuntimeBuildPlan, tycho_indexer::extractor::ExtractionError> {
        self.family_runtime_registry.build_family_runtime_plan(&self.extractors)
    }

    pub(crate) fn from_yaml_with_registry(
        path: &str,
        registry: FamilyRuntimeRegistry<'static>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = load_raw_extractor_configs(Path::new(path), &mut HashSet::new())?;
        config.validate_family_runtime_defaults(registry)?;
        let base_dir = Path::new(path)
            .parent()
            .unwrap_or_else(|| Path::new("."));
        config.resolve_substreams_params(base_dir, registry)?;
        let config = config.try_into_with_registry(registry)?;
        config
            .resolved_runtime_targets()
            .map_err(|err| -> Box<dyn std::error::Error> { err.to_string().into() })?;
        Ok(config)
    }

    pub(crate) fn resolved_indexer_runtime_plan(
        &self,
    ) -> Result<ResolvedIndexerRuntimePlan<'_>, tycho_indexer::extractor::ExtractionError> {
        Ok(ResolvedIndexerRuntimePlan::new(
            self.resolved_runtime_targets()?,
            self.family_runtime_registry,
        ))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn family_runtime_registry(&self) -> FamilyRuntimeRegistry<'static> {
        self.family_runtime_registry
    }

    #[cfg(test)]
    pub(crate) fn extractor_config_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Result<&ExtractorConfig, tycho_indexer::extractor::ExtractionError> {
        extractor_config_by_protocol_system(&self.extractors, protocol_system)?.ok_or_else(|| {
            tycho_indexer::extractor::ExtractionError::Setup(format!(
                "missing extractor config for protocol_system `{protocol_system}`"
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn bootstrap_config_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Result<&BootstrapConfig, tycho_indexer::extractor::ExtractionError> {
        self.extractor_config_for_protocol_system(protocol_system)?
            .bootstrap
            .as_ref()
            .ok_or_else(|| {
                tycho_indexer::extractor::ExtractionError::Setup(format!(
                    "missing bootstrap config for protocol_system `{protocol_system}`"
                ))
            })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn shared_bootstrap_plan_for_family(
        &self,
        family_name: &str,
    ) -> Result<SharedBootstrapPlan, tycho_indexer::extractor::ExtractionError> {
        self.family_runtime_registry
            .build_shared_bootstrap_plan_for_family(family_name, &self.extractors)
    }

    #[cfg(test)]
    pub(crate) fn shared_bootstrap_plan_for_protocol_systems<'a>(
        &'a self,
        protocol_systems: impl IntoIterator<Item = &'a str>,
    ) -> Result<SharedBootstrapPlan, tycho_indexer::extractor::ExtractionError> {
        let mut branch_configs = Vec::new();

        for protocol_system in protocol_systems {
            let extractor = self.extractor_config_for_protocol_system(protocol_system)?;
            let bootstrap = self.bootstrap_config_for_protocol_system(protocol_system)?;
            branch_configs.push((extractor, bootstrap));
        }

        self.family_runtime_registry
            .build_shared_bootstrap_plan(branch_configs)
    }
}

impl RawExtractorConfigs {
    fn validate_family_runtime_defaults(
        &self,
        registry: FamilyRuntimeRegistry<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        registry.validate()?;

        for (family_name, defaults) in &self.family_runtimes {
            registry.require_family_spec(family_name, "family_runtime")?;
            if defaults.bootstrap.is_some() {
                registry.validate_shared_bootstrap_support_for_family(family_name)?;
            }
            registry.validate_family_member_defaults_for_family(
                family_name,
                defaults
                    .members
                    .keys()
                    .map(String::as_str),
            )?;
            validate_family_member_route_protocol_defaults(family_name, defaults)?;
        }

        Ok(())
    }

    fn resolve_substreams_params(
        &mut self,
        base_dir: &Path,
        registry: FamilyRuntimeRegistry<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let family_runtime_defaults = self.family_runtimes.clone();

        for (extractor_name, extractor) in &mut self.extractors {
            let protocol_system = extractor
                .protocol_system
                .as_deref()
                .unwrap_or(&extractor.name);
            let allowed_protocols = effective_protocol_filter_for_protocol_system(
                protocol_system,
                extractor.family_runtime.as_ref(),
                &family_runtime_defaults,
                registry,
            );
            let mut resolved_start_block = extractor.start_block;
            merge_family_member_substreams_params_defaults(
                protocol_system,
                extractor.family_runtime.as_ref(),
                &family_runtime_defaults,
                &mut extractor.substreams_params,
            );
            resolve_substreams_params_map(
                allowed_protocols.as_ref(),
                &mut resolved_start_block,
                &mut extractor.substreams_params,
                base_dir,
            )?;

            merge_family_bootstrap_defaults(
                protocol_system,
                extractor.family_runtime.as_ref(),
                &family_runtime_defaults,
                &mut extractor.bootstrap,
                registry,
            )?;
            merge_family_stop_block_defaults(
                protocol_system,
                extractor.family_runtime.as_ref(),
                &family_runtime_defaults,
                &mut extractor.stop_block,
            );

            if let Some(bootstrap) = &mut extractor.bootstrap {
                bootstrap.start_block = Some(resolve_bootstrap_params(
                    allowed_protocols.as_ref(),
                    &mut bootstrap.params,
                    base_dir,
                )?);

                if let Some(start_block) = bootstrap.start_block {
                    if let Some(existing_start_block) = resolved_start_block {
                        if existing_start_block != start_block {
                            return Err(format!(
                                "conflicting start_block values for extractor `{extractor_name}`: \
                                 {existing_start_block} vs {start_block} from bootstrap config"
                            )
                            .into());
                        }
                    } else {
                        resolved_start_block = Some(start_block);
                    }
                }
            }
            extractor.start_block = resolved_start_block;
        }
        Ok(())
    }
}

fn validate_family_member_route_protocol_defaults(
    family_name: &str,
    defaults: &RawFamilyRuntimeDefaults,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut route_alias_owners: HashMap<String, String> = HashMap::new();

    for (protocol_system, member_defaults) in &defaults.members {
        for route_protocol in &member_defaults.shared_route_protocols {
            let normalized = canonicalize_shared_route_protocol(route_protocol);
            if normalized.is_empty() {
                return Err(format!(
                    "family_runtime `{family_name}` member `{protocol_system}` declares an empty shared_route_protocol"
                )
                .into());
            }

            if let Some(existing_owner) = route_alias_owners.get(&normalized) {
                if existing_owner != protocol_system {
                    return Err(format!(
                        "family_runtime `{family_name}` assigns shared_route_protocol `{normalized}` to both `{existing_owner}` and `{protocol_system}`"
                    )
                    .into());
                }
            } else {
                route_alias_owners.insert(normalized, protocol_system.clone());
            }
        }
    }

    Ok(())
}

fn merge_family_stop_block_defaults(
    protocol_system: &str,
    family_runtime: Option<&FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    stop_block: &mut Option<i64>,
) {
    if stop_block.is_some() {
        return;
    }

    *stop_block = resolved_family_member_defaults(protocol_system, family_runtime, family_defaults)
        .and_then(|defaults| defaults.stop_block());
}

fn merge_family_bootstrap_defaults(
    protocol_system: &str,
    family_runtime: Option<&FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    bootstrap: &mut Option<RawBootstrapConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    if bootstrap.is_some() {
        return Ok(());
    }

    let Some(defaults) =
        resolved_family_member_defaults(protocol_system, family_runtime, family_defaults)
    else {
        return Ok(());
    };
    let Some(family_bootstrap) = defaults.bootstrap() else {
        return Ok(());
    };
    let strategy = registry.shared_bootstrap_strategy_for_family_member(
        defaults.family_name,
        protocol_system,
        "family bootstrap defaults for",
    )?;

    *bootstrap = Some(RawBootstrapConfig {
        strategy,
        params: family_bootstrap.params.clone(),
        start_block: None,
    });
    Ok(())
}

fn merge_family_member_substreams_params_defaults(
    protocol_system: &str,
    family_runtime: Option<&FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    substreams_params: &mut HashMap<String, String>,
) {
    let Some(defaults) =
        resolved_family_member_defaults(protocol_system, family_runtime, family_defaults)
    else {
        return;
    };
    let Some(member_substreams_params) = defaults.substreams_params() else {
        return;
    };

    for (module_name, params) in member_substreams_params {
        substreams_params
            .entry(module_name.clone())
            .or_insert_with(|| params.clone());
    }
}

impl RawExtractorConfigs {
    fn try_into_with_registry(
        self,
        registry: FamilyRuntimeRegistry<'static>,
    ) -> Result<ExtractorConfigs, Box<dyn std::error::Error>> {
        let RawExtractorConfigs { includes: _, family_runtimes, extractors: raw_extractors } = self;
        let mut extractors = HashMap::with_capacity(raw_extractors.len());

        for (extractor_id, extractor) in raw_extractors {
            let protocol_system = extractor
                .protocol_system
                .unwrap_or_else(|| extractor.name.clone());
            let family_runtime = merge_family_runtime_config(
                &protocol_system,
                extractor.family_runtime,
                &family_runtimes,
                registry,
            )?;
            let spkg =
                resolve_extractor_spkg(&protocol_system, extractor.spkg, family_runtime.as_ref())?;
            let start_block = extractor
                .start_block
                .ok_or_else(|| format!("extractor `{extractor_id}` is missing `start_block`"))?;

            extractors.insert(
                extractor_id,
                ExtractorConfig::new(
                    extractor.name,
                    extractor.chain,
                    extractor.implementation_type,
                    extractor.sync_batch_size,
                    start_block,
                    extractor.stop_block,
                    extractor.protocol_types,
                    spkg,
                    extractor.module_name,
                    extractor.initialized_accounts,
                    extractor.initialized_accounts_block,
                    extractor.post_processor,
                    extractor.dci_plugin,
                    extractor.substreams_params,
                    extractor
                        .bootstrap
                        .map(|bootstrap| BootstrapConfig {
                            strategy: bootstrap.strategy,
                            start_block: bootstrap.start_block.expect(
                                "bootstrap config start_block must be resolved before conversion",
                            ),
                            params: bootstrap.params,
                        }),
                )
                .with_protocol_system(protocol_system)
                .with_family_runtime(family_runtime),
            );
        }

        Ok(ExtractorConfigs::new_with_registry(extractors, registry))
    }
}

fn resolve_extractor_spkg(
    protocol_system: &str,
    spkg: Option<String>,
    family_runtime: Option<&FamilyRuntimeConfig>,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(spkg) = spkg {
        return Ok(spkg);
    }

    if let Some(shared_spkg) = family_runtime
        .and_then(FamilyRuntimeConfig::shared_spkg)
        .map(str::to_string)
    {
        return Ok(shared_spkg);
    }

    Err(format!(
        "extractor for protocol system `{protocol_system}` must declare `spkg` unless its family_runtime resolves `shared_spkg`"
    )
    .into())
}

fn merge_family_runtime_config(
    protocol_system: &str,
    family_runtime: Option<FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Option<FamilyRuntimeConfig>, Box<dyn std::error::Error>> {
    let Some(family_runtime) = family_runtime else {
        return Ok(None);
    };
    let defaults = family_defaults.get(&family_runtime.family);

    match defaults {
        Some(defaults) => Ok(Some(defaults.resolve_family_runtime_config(
            protocol_system,
            family_runtime,
            registry,
        )?)),
        None => Ok(Some(registry.resolve_family_runtime_config(
            protocol_system,
            family_runtime,
            None,
            None,
            None,
        )?)),
    }
}

fn load_raw_extractor_configs(
    path: &Path,
    visited: &mut HashSet<PathBuf>,
) -> Result<RawExtractorConfigs, Box<dyn std::error::Error>> {
    let resolved_path = canonicalize_for_include_tracking(path)?;
    if !visited.insert(resolved_path.clone()) {
        return Err(format!(
            "cyclic extractor config include detected at `{}`",
            resolved_path.display()
        )
        .into());
    }

    let mut file = File::open(&resolved_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let parsed: RawExtractorConfigs = serde_yaml::from_str(&contents)?;
    let base_dir = resolved_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let mut merged = RawExtractorConfigs {
        includes: vec![],
        family_runtimes: HashMap::new(),
        extractors: HashMap::new(),
    };

    for include in &parsed.includes {
        let included =
            load_raw_extractor_configs(&base_dir.join(normalize_include_path(include)), visited)?;
        merge_raw_extractor_configs(&mut merged, included)?;
    }

    merge_raw_extractor_configs(
        &mut merged,
        RawExtractorConfigs {
            includes: vec![],
            family_runtimes: parsed.family_runtimes,
            extractors: parsed.extractors,
        },
    )?;

    visited.remove(&resolved_path);
    Ok(merged)
}

fn merge_raw_extractor_configs(
    target: &mut RawExtractorConfigs,
    incoming: RawExtractorConfigs,
) -> Result<(), Box<dyn std::error::Error>> {
    for (family_name, defaults) in incoming.family_runtimes {
        if let Some(existing) = target
            .family_runtimes
            .get_mut(&family_name)
        {
            merge_raw_family_runtime_defaults(existing, defaults, &family_name)?;
        } else {
            target
                .family_runtimes
                .insert(family_name, defaults);
        }
    }

    for (extractor_id, extractor) in incoming.extractors {
        if target
            .extractors
            .insert(extractor_id.clone(), extractor)
            .is_some()
        {
            return Err(format!("duplicate extractor definition for `{extractor_id}`").into());
        }
    }
    Ok(())
}

fn merge_raw_family_runtime_defaults(
    target: &mut RawFamilyRuntimeDefaults,
    incoming: RawFamilyRuntimeDefaults,
    family_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    target.shared_spkg = merge_optional_string(
        target.shared_spkg.take(),
        incoming.shared_spkg,
        &format!("family_runtime `{family_name}` shared_spkg"),
    )?;
    target.shared_module = merge_optional_string(
        target.shared_module.take(),
        incoming.shared_module,
        &format!("family_runtime `{family_name}` shared_module"),
    )?;
    target.durability_scope = merge_optional_string(
        target.durability_scope.take(),
        incoming.durability_scope,
        &format!("family_runtime `{family_name}` durability_scope"),
    )?;
    target.stop_block = merge_optional_i64(
        target.stop_block,
        incoming.stop_block,
        &format!("family_runtime `{family_name}` stop_block"),
    )?;

    target.bootstrap = match (target.bootstrap.take(), incoming.bootstrap) {
        (Some(existing), Some(incoming)) => {
            Some(merge_raw_family_bootstrap_defaults(existing, incoming, family_name)?)
        }
        (Some(existing), None) => Some(existing),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
    };

    for (protocol_system, incoming_defaults) in incoming.members {
        if let Some(existing_defaults) = target.members.get_mut(&protocol_system) {
            merge_raw_family_member_defaults(
                existing_defaults,
                incoming_defaults,
                family_name,
                &protocol_system,
            )?;
        } else {
            target
                .members
                .insert(protocol_system, incoming_defaults);
        }
    }

    Ok(())
}

fn merge_raw_family_bootstrap_defaults(
    target: RawFamilyBootstrapDefaults,
    incoming: RawFamilyBootstrapDefaults,
    family_name: &str,
) -> Result<RawFamilyBootstrapDefaults, Box<dyn std::error::Error>> {
    Ok(RawFamilyBootstrapDefaults {
        params: merge_required_string(
            target.params,
            incoming.params,
            &format!("family_runtime `{family_name}` bootstrap params"),
        )?,
    })
}

fn merge_raw_family_member_defaults(
    target: &mut RawFamilyMemberDefaults,
    incoming: RawFamilyMemberDefaults,
    family_name: &str,
    protocol_system: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for (key, incoming_value) in incoming.substreams_params {
        if let Some(existing_value) = target.substreams_params.get(&key) {
            if existing_value != &incoming_value {
                return Err(format!(
                    "conflicting values for family_runtime `{family_name}` member `{protocol_system}` substreams param `{key}`"
                )
                .into());
            }
        } else {
            target
                .substreams_params
                .insert(key, incoming_value);
        }
    }

    for incoming_protocol in incoming.shared_route_protocols {
        if !target
            .shared_route_protocols
            .contains(&incoming_protocol)
        {
            target
                .shared_route_protocols
                .push(incoming_protocol);
        }
    }

    Ok(())
}

fn merge_optional_i64(
    existing: Option<i64>,
    incoming: Option<i64>,
    context: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    match (existing, incoming) {
        (Some(existing), Some(incoming)) if existing != incoming => {
            Err(format!("conflicting values for {context}: {existing} vs {incoming}").into())
        }
        (Some(existing), _) => Ok(Some(existing)),
        (None, Some(incoming)) => Ok(Some(incoming)),
        (None, None) => Ok(None),
    }
}

fn normalize_include_path(include: &str) -> &str {
    include
        .strip_prefix('@')
        .unwrap_or(include)
}

fn canonicalize_for_include_tracking(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    path.canonicalize()
        .map_err(|err| format!("failed to resolve config path `{}`: {err}", path.display()).into())
}

fn merge_optional_string(
    existing: Option<String>,
    incoming: Option<String>,
    context: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match (existing, incoming) {
        (Some(existing), Some(incoming)) if existing != incoming => {
            Err(format!("conflicting values for {context}: `{existing}` vs `{incoming}`").into())
        }
        (Some(existing), _) => Ok(Some(existing)),
        (None, Some(incoming)) => Ok(Some(incoming)),
        (None, None) => Ok(None),
    }
}

fn merge_required_string(
    existing: String,
    incoming: String,
    context: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if existing == incoming {
        Ok(existing)
    } else {
        Err(format!("conflicting values for {context}: `{existing}` vs `{incoming}`").into())
    }
}

struct ResolvedFamilyMemberDefaults<'a> {
    family_name: &'a str,
    family_defaults: &'a RawFamilyRuntimeDefaults,
    member_defaults: Option<&'a RawFamilyMemberDefaults>,
}

impl<'a> ResolvedFamilyMemberDefaults<'a> {
    fn stop_block(&self) -> Option<i64> {
        self.family_defaults.stop_block
    }

    fn bootstrap(&self) -> Option<&'a RawFamilyBootstrapDefaults> {
        self.family_defaults.bootstrap.as_ref()
    }

    fn substreams_params(&self) -> Option<&'a HashMap<String, String>> {
        self.member_defaults.map(|defaults| &defaults.substreams_params)
    }

    fn normalized_shared_route_protocol_filter(&self) -> Option<HashSet<String>> {
        self.member_defaults
            .and_then(normalized_shared_route_protocol_filter_for_member_defaults)
    }
}

fn resolved_family_member_defaults<'a>(
    protocol_system: &str,
    family_runtime: Option<&'a FamilyRuntimeConfig>,
    family_defaults: &'a HashMap<String, RawFamilyRuntimeDefaults>,
) -> Option<ResolvedFamilyMemberDefaults<'a>> {
    let family_runtime = family_runtime?;
    let family_defaults = family_defaults.get(&family_runtime.family)?;
    Some(ResolvedFamilyMemberDefaults {
        family_name: &family_runtime.family,
        family_defaults,
        member_defaults: family_defaults.members.get(protocol_system),
    })
}

fn effective_protocol_filter_for_protocol_system(
    protocol_system: &str,
    family_runtime: Option<&FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Option<HashSet<String>> {
    resolved_family_member_defaults(protocol_system, family_runtime, family_defaults)
        .and_then(|defaults| defaults.normalized_shared_route_protocol_filter())
        .or_else(|| protocol_filter_for_protocol_system(protocol_system, registry))
}

fn normalized_shared_route_protocol_filter_for_member_defaults(
    defaults: &RawFamilyMemberDefaults,
) -> Option<HashSet<String>> {
    if defaults
        .shared_route_protocols
        .is_empty()
    {
        return None;
    }

    Some(
        defaults
            .shared_route_protocols
            .iter()
            .map(|protocol| canonicalize_shared_route_protocol(protocol))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, process};

    use super::*;
    use crate::testing::{
        family_runtime_config_for_tests, swap_extractor_config_for_tests,
        uniswap_family_swap_extractors_for_tests,
        write_temp_substreams_package_for_tests,
    };
    use mockito::Server;
    use tycho_common::models::FinancialType;
    use tycho_ethereum::rpc::EthereumRpcClient;
    use tycho_indexer::canonical_shared_family_runtime_spec;
    use tycho_indexer::extractor::family_bootstrap_registry::SharedBootstrapParamsParser;
    use tycho_indexer::extractor::family_registry::{
        default_family_runtime_registry,
        FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec,
    };
    use tycho_indexer::extractor::models::BlockChanges;
    use tycho_indexer::extractor::shared_bootstrap::BootstrapBranchDescriptor;
    use tycho_indexer::extractor::ExtractionError;
    use tycho_storage::postgres::testing::run_against_db;

    fn future_materialize_branch<'a>(
        _: &'a EthereumRpcClient,
        _: &'a BootstrapBranchDescriptor,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>,
    > {
        Box::pin(async {
            Err(ExtractionError::Setup(
                "config tests should not materialize future bootstrap branches".to_string(),
            ))
        })
    }

    fn test_extractor_configs_new(
        extractors: HashMap<String, ExtractorConfig>,
    ) -> ExtractorConfigs {
        ExtractorConfigs::new_with_registry(extractors, default_family_runtime_registry())
    }

    fn test_extractor_configs_from_yaml(
        path: &str,
    ) -> Result<ExtractorConfigs, Box<dyn std::error::Error>> {
        ExtractorConfigs::from_yaml_with_registry(path, default_family_runtime_registry())
    }

    fn test_registry_with_future_family(
        future_family: FamilyRuntimeSpec,
    ) -> FamilyRuntimeRegistry<'static> {
        FamilyRuntimeRegistry::new(Box::leak(vec![future_family].into_boxed_slice()))
    }

    fn test_uniswap_shared_module() -> &'static str {
        default_family_runtime_registry()
            .shared_runtime_metadata_for_family("uniswap")
            .map(|metadata| metadata.output_module)
            .expect("registered uniswap output module")
    }

    fn test_uniswap_durability_scope() -> &'static str {
        default_family_runtime_registry()
            .shared_runtime_metadata_for_family("uniswap")
            .map(|metadata| metadata.durability_scope)
            .expect("registered uniswap durability scope")
    }

    fn test_uniswap_shared_start_block() -> i64 {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let shared_bootstrap =
            fs::read_to_string(root.join("config/shared_uniswap_bootstrap.yaml"))
                .expect("read repo shared bootstrap config");
        parse_substreams_params_yaml_with_registry(
            "uniswap_v2",
            &shared_bootstrap,
            default_family_runtime_registry(),
        )
            .expect("parse repo shared bootstrap config for v2")
            .0
            .expect("repo shared bootstrap start_block present")
    }

    #[test]
    fn production_config_and_recorder_surfaces_do_not_reintroduce_default_registry_fallbacks() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let config_src =
            fs::read_to_string(root.join("src/config.rs")).expect("read config.rs source");
        let config_production_prefix = config_src
            .split("#[cfg(test)]\nmod tests {")
            .next()
            .expect("config.rs should contain test module split");
        assert!(
            !config_production_prefix.contains("default_family_runtime_registry("),
            "production config surface should stay on explicit registry APIs"
        );

        let recorder_src = fs::read_to_string(root.join("src/record_substreams.rs"))
            .expect("read record_substreams.rs source");
        let recorder_production_prefix = recorder_src
            .split("#[cfg(test)]\nmod tests {")
            .next()
            .expect("record_substreams.rs should contain test module split");
        assert!(
            !recorder_production_prefix.contains("default_family_runtime_registry("),
            "record_substreams library surface should stay on explicit registry APIs"
        );
    }

    #[test]
    fn production_runtime_helpers_keep_default_registry_outside_the_cli_boundary() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for relative_path in [
            "src/extractor/runtime_target_planning.rs",
            "src/extractor/family_runtime_planning.rs",
            "src/extractor/managed_stream_startup.rs",
            "src/extractor/protocol_message_registry.rs",
            "src/extractor/runner.rs",
        ] {
            let src = fs::read_to_string(root.join(relative_path))
                .unwrap_or_else(|err| panic!("read {relative_path}: {err}"));
            let production_prefix = src
                .split("#[cfg(test)]")
                .next()
                .expect("source split should produce a production prefix");
            assert!(
                !production_prefix.contains("default_family_runtime_registry("),
                "production prefix for {relative_path} should stay on explicit registry APIs"
            );
        }

        let cli_src = fs::read_to_string(root.join("src/cli.rs")).expect("read cli.rs source");
        let cli_production_prefix = cli_src
            .split("#[cfg(test)]")
            .next()
            .expect("cli.rs should contain a production prefix");
        let cli_default_registry_occurrences = cli_production_prefix
            .match_indices("default_family_runtime_registry(")
            .count();
        assert_eq!(
            cli_default_registry_occurrences, 1,
            "production cli.rs should concentrate the built-in registry at one CLI boundary"
        );

        let main_src = fs::read_to_string(root.join("src/main.rs")).expect("read main.rs source");
        let main_production_prefix = main_src
            .split("#[cfg(test)]\nmod test_serial_db {")
            .next()
            .expect("main.rs should contain test module split");
        let main_default_registry_occurrences = main_production_prefix
            .match_indices("default_family_runtime_registry(")
            .count();
        assert_eq!(
            main_default_registry_occurrences, 0,
            "production main.rs should not own the built-in family registry once the CLI boundary is explicit"
        );
    }

    #[test]
    fn production_main_and_config_keep_runtime_plan_startup_on_runtime_owners() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let main_src = fs::read_to_string(root.join("src/main.rs")).expect("read main.rs source");
        let main_production_prefix = main_src
            .split("#[cfg(test)]\nmod test_serial_db {")
            .next()
            .expect("main.rs should contain test module split");
        assert!(
            !main_production_prefix.contains("async fn create_indexing_tasks("),
            "production main.rs should not keep a separate runtime-plan startup helper once runtime owners are explicit"
        );
        assert!(
            !main_production_prefix.contains(".start_indexing_runtime_plan("),
            "production main.rs should start runtime plans through the runtime-plan owner, not a launch-config shim"
        );

        let config_src =
            fs::read_to_string(root.join("src/config.rs")).expect("read config.rs source");
        let config_production_prefix = config_src
            .split("#[cfg(test)]\nmod tests {")
            .next()
            .expect("config.rs should contain test module split");
        assert!(
            config_production_prefix.contains("pub(crate) async fn start_indexing(\n        self,"),
            "config.rs should keep runtime-plan startup anchored on the runtime-plan owner"
        );
        assert!(
            !config_production_prefix.contains("pub(crate) async fn start_indexing_runtime_plan("),
            "launch config should no longer own a separate runtime-plan startup shim"
        );
    }

    #[test]
    fn production_main_consumes_managed_indexer_tasks_through_owned_methods() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let main_src = fs::read_to_string(root.join("src/main.rs")).expect("read main.rs source");
        let main_production_prefix = main_src
            .split("#[cfg(test)]\nmod test_serial_db {")
            .next()
            .expect("main.rs should contain test module split");

        assert!(
            main_production_prefix.contains("managed_indexer.into_task_groups()"),
            "production index startup should consume managed indexer task groups through the config-owned owner"
        );
        assert!(
            main_production_prefix.contains("managed_indexer.into_all_tasks()"),
            "production run startup should consume all managed tasks through the config-owned owner"
        );
        assert!(
            !main_production_prefix.contains("managed_indexer.extraction_tasks"),
            "production main.rs should not directly flatten managed extraction task fields"
        );
        assert!(
            !main_production_prefix.contains("managed_indexer.service_tasks"),
            "production main.rs should not directly pull managed service task fields"
        );
    }

    #[test]
    fn production_record_substreams_request_resolution_stays_on_runtime_target_owners() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let recorder_src = fs::read_to_string(root.join("src/record_substreams.rs"))
            .expect("read record_substreams.rs source");
        let recorder_production_prefix = recorder_src
            .split("#[cfg(test)]\nmod tests {")
            .next()
            .expect("record_substreams.rs should contain test module split");

        assert!(
            recorder_production_prefix.contains(".resolve_substreams_execution_request("),
            "record_substreams.rs should derive config-backed requests through the runtime-target owner"
        );
        assert!(
            !recorder_production_prefix.contains(".substreams_execution_request_with_overrides("),
            "record_substreams.rs should not reshape resolved target requests through a local override shim"
        );
    }

    #[test]
    fn production_stream_loading_stays_on_prepared_request_owners() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let startup_src = fs::read_to_string(root.join("src/extractor/managed_stream_startup.rs"))
            .expect("read managed_stream_startup.rs source");
        let startup_production_prefix = startup_src
            .split("#[cfg(test)]")
            .next()
            .expect("managed_stream_startup.rs should contain a production prefix");
        assert!(
            !startup_production_prefix.contains("pub(crate) fn build_substreams_stream_from_prepared_request("),
            "managed_stream_startup.rs should not keep a free-function prepared-request -> stream builder"
        );
        assert!(
            !startup_production_prefix.contains("pub(crate) async fn load_stream_for_prepared_request("),
            "managed_stream_startup.rs should not keep a free-function prepared-request -> loaded stream helper"
        );

        let request_src = fs::read_to_string(root.join("src/extractor/managed_substreams_request.rs"))
            .expect("read managed_substreams_request.rs source");
        let request_production_prefix = request_src
            .split("#[cfg(test)]")
            .next()
            .expect("managed_substreams_request.rs should contain a production prefix");
        assert!(
            request_production_prefix.contains("pub(crate) fn build_stream("),
            "PreparedSubstreamsRequest should own prepared-request -> stream construction"
        );
        assert!(
            request_production_prefix.contains("pub(crate) async fn load_stream("),
            "PreparedSubstreamsRequest should own prepared-request -> loaded stream acquisition"
        );
    }

    #[test]
    fn production_managed_runner_assembly_stays_on_one_factory_contract() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let runtime_targets_src = fs::read_to_string(
            root.join("src/extractor/runtime_targets_startup.rs"),
        )
        .expect("read runtime_targets_startup.rs source");
        let runtime_targets_production_prefix = runtime_targets_src
            .split("#[cfg(test)]")
            .next()
            .expect("runtime_targets_startup.rs should contain a production prefix");
        assert!(
            !runtime_targets_production_prefix.contains("trait ManagedRuntimeOwner"),
            "runtime_targets_startup.rs should not keep a second managed-runner owner trait once the factory contract is canonical"
        );
        assert!(
            runtime_targets_production_prefix.contains("impl<S, C> ManagedRunnerFactory for PreparedManagedRuntimeOwner<S, C>"),
            "PreparedManagedRuntimeOwner should directly satisfy the canonical managed-runner factory contract"
        );
        assert!(
            !runtime_targets_production_prefix.contains("PreparedRuntimeTargetStartupOwner"),
            "PreparedRuntimeTargetStartup should not keep a second startup-owner trait once managed-runner factories can be stored directly"
        );
        assert!(
            runtime_targets_production_prefix.contains("runner_factory: Box<dyn ManagedRunnerFactory>"),
            "PreparedRuntimeTargetStartup should store the canonical managed-runner factory contract directly"
        );
        assert!(
            !runtime_targets_production_prefix.contains("struct PreparedManagedRuntimeStartup"),
            "runtime_targets_startup.rs should not keep an extra prepared managed startup shell once PreparedRuntimeTargetStartup owns the final prepared container"
        );
    }

    #[test]
    fn production_lifecycle_owners_share_prepared_bootstrap_execution_wiring() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for relative_path in [
            "src/extractor/extractor_lifecycle.rs",
            "src/extractor/family_lifecycle.rs",
        ] {
            let src = fs::read_to_string(root.join(relative_path))
                .unwrap_or_else(|err| panic!("read {relative_path}: {err}"));
            assert!(
                src.match_indices("load_and_execute_context_prepared_bootstrap_run_and_resolve(")
                    .count()
                    >= 1,
                "{relative_path} should use the context-owned shared prepared-bootstrap lifecycle helper"
            );
            assert!(
                src.match_indices("load_and_execute_optional_bootstrap_run_and_resolve(")
                    .count()
                    == 0,
                "{relative_path} should not keep its own lower-level bootstrap execution wiring"
            );
        }
    }

    #[test]
    fn production_runtime_targets_startup_owns_managed_startup_draft_dispatch() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let runtime_targets_src = fs::read_to_string(
            root.join("src/extractor/runtime_targets_startup.rs"),
        )
        .expect("read runtime_targets_startup.rs source");
        assert!(
            runtime_targets_src.contains("as ManagedStartupLifecycleView>::prepare_managed_startup_draft("),
            "runtime_targets_startup.rs should dispatch managed-startup drafts through the shared lifecycle trait owner"
        );
        assert!(
            runtime_targets_src.contains("draft.into_prepared_startup(context).await"),
            "runtime_targets_startup.rs should let the prepared draft own draft-to-startup stream loading"
        );
        assert!(
            !runtime_targets_src.contains("pub(crate) async fn prepare_runtime_target_startup_from_draft("),
            "runtime_targets_startup.rs should not keep a separate draft-to-startup free-function shim once the draft owner is explicit"
        );

        for relative_path in [
            "src/extractor/family_managed_startup.rs",
            "src/extractor/standalone_managed_startup.rs",
        ] {
            let src = fs::read_to_string(root.join(relative_path))
                .unwrap_or_else(|err| panic!("read {relative_path}: {err}"));
            assert!(
                !src.contains("pub(crate) async fn prepare_managed_startup_draft("),
                "{relative_path} should not keep an inherent managed-startup draft wrapper once runtime_targets_startup owns the dispatch"
            );
        }
    }

    #[test]
    fn production_family_runtime_planning_stays_on_registry_owner() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let registry_types_src = fs::read_to_string(
            root.join("src/extractor/family_runtime_types.rs"),
        )
        .expect("read family_runtime_types.rs source");
        assert!(
            registry_types_src.contains("pub fn build_family_runtime_plan("),
            "family_runtime_types.rs should let FamilyRuntimeRegistry own family runtime plan construction"
        );
        assert!(
            registry_types_src.contains("pub fn build_resolved_family_runtime_plan<'b>("),
            "family_runtime_types.rs should let FamilyRuntimeRegistry own resolved family runtime planning"
        );
        assert!(
            registry_types_src.contains("pub fn resolve_runtime_targets<'b>("),
            "family_runtime_types.rs should let FamilyRuntimeRegistry own resolved runtime-target assembly"
        );

        let config_src =
            fs::read_to_string(root.join("src/config.rs")).expect("read config.rs source");
        let production_config_src = config_src
            .split("mod tests {")
            .next()
            .expect("config.rs production source prefix");
        assert!(
            production_config_src.contains("self.family_runtime_registry.resolve_runtime_targets(&self.extractors)"),
            "config.rs should delegate resolved runtime-target planning directly to the registry owner"
        );
        assert!(
            production_config_src.contains("self.family_runtime_registry.build_family_runtime_plan(&self.extractors)"),
            "config.rs should delegate family runtime planning directly to the registry owner"
        );
        assert!(
            !production_config_src.contains("family_runtime::resolve_runtime_targets_with_registry"),
            "config.rs should not keep importing the family_runtime facade for production runtime-target planning"
        );
        assert!(
            !production_config_src.contains("family_runtime::build_family_runtime_plan_via_registry"),
            "config.rs should not keep importing the family_runtime facade for production family runtime planning"
        );

        let facade_src = fs::read_to_string(root.join("src/extractor/family_runtime.rs"))
            .expect("read family_runtime.rs source");
        assert!(
            facade_src.contains("registry.resolve_runtime_targets(extractors)"),
            "family_runtime.rs compatibility facade should forward runtime-target planning back to the registry owner"
        );
        assert!(
            facade_src.contains("registry.build_family_runtime_plan(extractors)"),
            "family_runtime.rs compatibility facade should forward family runtime planning back to the registry owner"
        );
        assert!(
            facade_src.contains("registry.build_resolved_family_runtime_plan(extractors)"),
            "family_runtime.rs compatibility facade should forward resolved family runtime planning back to the registry owner"
        );
    }

    #[test]
    fn production_runtime_target_startup_keeps_typed_family_and_standalone_slots() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let runtime_targets_src = fs::read_to_string(
            root.join("src/extractor/runtime_targets_startup.rs"),
        )
        .expect("read runtime_targets_startup.rs source");
        assert!(
            runtime_targets_src.contains("pub(crate) family_targets: Vec<PreparedRuntimeTargetStartup>"),
            "runtime_targets_startup.rs should retain an explicit prepared family-startup collection"
        );
        assert!(
            runtime_targets_src.contains("pub(crate) standalone_targets: Vec<PreparedRuntimeTargetStartup>"),
            "runtime_targets_startup.rs should retain an explicit prepared standalone-startup collection"
        );
        assert!(
            runtime_targets_src.contains("pub(crate) fn total_targets(&self) -> usize"),
            "runtime_targets_startup.rs should expose one canonical total prepared-target count across typed startup slots"
        );
        assert!(
            runtime_targets_src.contains("let is_family = matches!(target, ResolvedRuntimeTarget::Family(_));"),
            "runtime_targets_startup.rs should classify prepared startup artifacts by target kind before final runner fan-out"
        );
        assert!(
            runtime_targets_src.contains("family_targets.push(prepared_target);"),
            "runtime_targets_startup.rs should keep prepared family-startup artifacts in the family slot"
        );
        assert!(
            runtime_targets_src.contains("standalone_targets.push(prepared_target);"),
            "runtime_targets_startup.rs should keep prepared standalone-startup artifacts in the standalone slot"
        );
        assert!(
            !runtime_targets_src.contains("pub(crate) prepared_targets: Vec<PreparedRuntimeTargetStartup>"),
            "runtime_targets_startup.rs should not collapse prepared startup artifacts back into one generic vector"
        );
    }

    #[test]
    fn production_managed_indexer_tasks_keep_typed_family_and_standalone_extraction_batches() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let config_src =
            fs::read_to_string(root.join("src/config.rs")).expect("read config.rs source");
        let config_production_prefix = config_src
            .split("#[cfg(test)]\nmod tests {")
            .next()
            .expect("config.rs should contain test module split");
        let managed_indexer_struct_src = config_src
            .split("pub(crate) struct ManagedIndexerTasks {")
            .nth(1)
            .and_then(|tail| tail.split("}\n\n#[derive(Clone)]").next())
            .expect("extract ManagedIndexerTasks struct source");
        assert!(
            config_production_prefix.contains("pub(crate) struct ManagedExtractionTasksBatch"),
            "config.rs should retain an explicit typed extraction-task batch owner for managed indexing startup"
        );
        assert!(
            config_production_prefix.contains(
                "pub(crate) family_extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>"
            ),
            "config.rs should retain the family extraction-task slot on the managed indexing task batch"
        );
        assert!(
            config_production_prefix.contains(
                "pub(crate) standalone_extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>"
            ),
            "config.rs should retain the standalone extraction-task slot on the managed indexing task batch"
        );
        assert!(
            config_production_prefix.contains("pub(crate) fn into_flattened(self) -> Vec<JoinHandle<Result<(), ExtractionError>>>"),
            "config.rs should keep one explicit compatibility flattening step for extraction tasks instead of flattening them at construction time"
        );
        assert!(
            config_production_prefix.contains("ManagedExtractionTasksBatch::from_managed_runners("),
            "config.rs should build managed indexing tasks through the typed extraction-task batch owner instead of repartitioning runner enums locally"
        );
        assert!(
            config_production_prefix.contains(".build_managed_runners_batch(context.runtime_targets_build_context("),
            "config.rs should consume the typed managed-runner batch from the runtime-target owner"
        );
        assert!(
            !config_production_prefix
                .contains(".partition(|runner| matches!(runner, ManagedRunner::Family(_)))"),
            "config.rs should not re-partition managed runners by enum after runtime-target startup has already kept typed family and standalone slots"
        );
        assert!(
            !managed_indexer_struct_src.contains(
                "pub(crate) extraction_tasks: Vec<JoinHandle<Result<(), ExtractionError>>>"
            ),
            "config.rs should not collapse managed indexing extraction tasks back into one generic vector at construction time"
        );
    }

    #[test]
    fn production_shared_bootstrap_runtime_resolution_stays_on_registry_owner() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let registry_src = fs::read_to_string(
            root.join("src/extractor/family_bootstrap_registry.rs"),
        )
        .expect("read family_bootstrap_registry.rs source");
        assert!(
            registry_src.contains("pub fn resolve_optional_shared_bootstrap_runtime<'b>("),
            "family_bootstrap_registry.rs should own the optional shared-bootstrap runtime resolution surface"
        );

        let runtime_target_src = fs::read_to_string(
            root.join("src/extractor/runtime_target_planning.rs"),
        )
        .expect("read runtime_target_planning.rs source");
        assert!(
            runtime_target_src.contains("registry.resolve_optional_shared_bootstrap_runtime([extractor_config])"),
            "runtime_target_planning.rs should delegate standalone optional shared-bootstrap runtime resolution to the registry owner"
        );

        let family_resolution_src = fs::read_to_string(
            root.join("src/extractor/family_runtime_resolution.rs"),
        )
        .expect("read family_runtime_resolution.rs source");
        assert!(
            family_resolution_src.contains("registry.resolve_optional_shared_bootstrap_runtime(extractor_configs.iter().copied())"),
            "family_runtime_resolution.rs should delegate optional shared-bootstrap runtime filtering/resolution to the registry owner"
        );
        assert!(
            !family_resolution_src.contains("resolve_shared_bootstrap_runtime_for_configs("),
            "family_runtime_resolution.rs should not keep a local optional shared-bootstrap runtime wrapper once the registry owns that surface"
        );
    }

    fn write_future_family_yaml_fixture_for_tests() -> std::path::PathBuf {
        let temp_root =
            std::env::temp_dir().join(format!("tycho-indexer-future-family-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/future_bootstrap.yaml"),
            r#"
start_block: 99
params:
  routes:
    - token0: "0x00000000000000000000000000000000000000a1"
      token1: "0x00000000000000000000000000000000000000b1"
      routers:
        - pool: "0x0000000000000000000000000000000000000011"
          protocol: futurev1
        - pool: "0x0000000000000000000000000000000000000022"
          protocol: futurev2
"#,
        )
        .expect("write future bootstrap config");
        fs::write(
            temp_root.join("config/future_substreams.yaml"),
            r#"
start_block: 99
params:
  bootstrap_block: 99
  routes:
    - token0: "0x00000000000000000000000000000000000000a1"
      token1: "0x00000000000000000000000000000000000000b1"
      routers:
        - pool: "0x0000000000000000000000000000000000000011"
          protocol: futurev1
        - pool: "0x0000000000000000000000000000000000000022"
          protocol: futurev2
"#,
        )
        .expect("write future substreams config");
        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
family_runtimes:
  future_swap:
    shared_spkg: "protocols/substreams/future-swap-combined/test.spkg"
    shared_module: "map_future_swap_family_protocol_changes"
    bootstrap:
      params: "@config/future_bootstrap.yaml"
    members:
      future_v1:
        substreams_params:
          future_v1_map_events: "@config/future_substreams.yaml"
      future_v2:
        substreams_params:
          future_v2_map_events: "@config/future_substreams.yaml"
extractors:
  future_v1:
    name: "future_v1"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "future_pool"
        financial_type: "Swap"
    module_name: "map_protocol_changes"
    family_runtime:
      family: "future_swap"
  future_v2:
    name: "future_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "future_pool"
        financial_type: "Swap"
    module_name: "map_protocol_changes"
    family_runtime:
      family: "future_swap"
"#,
        )
        .expect("write future extractor config");

        temp_root
    }

    fn make_temp_config_root(prefix: &str) -> std::path::PathBuf {
        let temp_root =
            std::env::temp_dir().join(format!("tycho-indexer-{prefix}-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");
        temp_root
    }

    fn write_shared_uniswap_bootstrap_fixture(root: &std::path::Path) {
        fs::write(
            root.join("config/shared_uniswap_bootstrap.yaml"),
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#,
        )
        .expect("write shared bootstrap config");
    }

    fn write_uniswap_extractor_fixture(
        root: &std::path::Path,
        file_name: &str,
        include_v2: bool,
        include_v3: bool,
        include_v2_substreams_params: bool,
        include_v3_substreams_params: bool,
    ) {
        let mut body = String::from("extractors:\n");

        if include_v2 {
            body.push_str(
                r#"  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
"#,
            );
            if include_v2_substreams_params {
                body.push_str(
                    r#"    substreams_params:
      map_pool_events: "@config/shared_uniswap_bootstrap.yaml"
"#,
                );
            }
            body.push_str(
                r#"    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
            );
        }

        if include_v3 {
            body.push_str(
                r#"  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
"#,
            );
            if include_v3_substreams_params {
                body.push_str(
                    r#"    substreams_params:
      map_events: "@config/shared_uniswap_bootstrap.yaml"
"#,
                );
            }
            body.push_str(
                r#"    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
            );
        }

        fs::write(root.join(file_name), body).expect("write uniswap extractor fixture");
    }

    #[test]
    fn extractor_configs_load_substreams_params_from_file() {
        let temp_root =
            std::env::temp_dir().join(format!("tycho-indexer-substreams-params-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/uniswap_v3_bootstrap.yaml"),
            r#"
start_block: 1
params:
  pools:
    - "0xabc"
"#,
        )
        .expect("write config file");
        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
extractors:
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_protocol_changes"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/uniswap_v3_bootstrap.yaml"
"#,
        )
        .expect("write extractor config");

        let config = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load extractor configs");

        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .map(ExtractorConfig::start_block),
            Some(1)
        );
        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .and_then(|extractor| extractor.bootstrap.as_ref())
                .map(|bootstrap| bootstrap.strategy.clone()),
            Some(BootstrapStrategy::UniswapV3Rpc)
        );
        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .and_then(|extractor| extractor.bootstrap.as_ref())
                .map(|bootstrap| bootstrap.params.as_str()),
            Some("bootstrap_block=1&pools=0xabc")
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_protocol_system_views_prefer_explicit_protocol_systems_over_keys() {
        let extractors = HashMap::from([
            (
                "uniswap_v2_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v2_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![],
                    "protocols/substreams/test-v2.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    Some(DCIType::RPC),
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v2"),
            ),
            (
                "curve_alias".to_string(),
                ExtractorConfig::new(
                    "curve_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![],
                    "protocols/substreams/test-curve.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
            (
                "curve_duplicate_alias".to_string(),
                ExtractorConfig::new(
                    "curve_duplicate_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![],
                    "protocols/substreams/test-curve-2.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
        ]);
        let extractors_config = test_extractor_configs_new(extractors);
        let runtime_targets = extractors_config
            .resolved_runtime_targets()
            .expect("resolved runtime targets");

        assert_eq!(
            runtime_targets.protocol_systems(),
            vec!["curve".to_string(), "uniswap_v2".to_string()]
        );
        assert_eq!(runtime_targets.dci_protocol_systems(), vec!["uniswap_v2".to_string()]);
    }

    #[test]
    fn service_config_derives_protocol_views_from_runtime_targets() {
        let extractors = HashMap::from([
            (
                "uniswap_v2_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v2_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![],
                    "protocols/substreams/test-v2.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    Some(DCIType::RPC),
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v2"),
            ),
            (
                "curve_alias".to_string(),
                ExtractorConfig::new(
                    "curve_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![],
                    "protocols/substreams/test-curve.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
        ]);
        let extractors_config = test_extractor_configs_new(extractors);

        let runtime_targets = extractors_config
            .resolved_runtime_targets()
            .expect("resolved runtime targets");
        let service_config = ResolvedIndexerServiceConfig::from_runtime_targets(&runtime_targets);

        assert_eq!(
            service_config.protocol_systems(),
            vec!["curve".to_string(), "uniswap_v2".to_string()]
        );
        assert_eq!(service_config.dci_protocol_systems(), vec!["uniswap_v2".to_string()]);
        assert_eq!(runtime_targets.len(), 2);
    }

    #[test]
    fn resolved_indexer_runtime_plan_keeps_service_and_runtime_views_in_sync() {
        let extractors = HashMap::from([
            (
                "uniswap_v2_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v2_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![],
                    "protocols/substreams/test-v2.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    Some(DCIType::RPC),
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v2"),
            ),
            (
                "curve_alias".to_string(),
                ExtractorConfig::new(
                    "curve_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![],
                    "protocols/substreams/test-curve.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
        ]);
        let extractors_config = test_extractor_configs_new(extractors);

        let runtime_plan = extractors_config
            .resolved_indexer_runtime_plan()
            .expect("resolved runtime plan");

        assert_eq!(
            runtime_plan.protocol_systems(),
            &["curve".to_string(), "uniswap_v2".to_string()]
        );
        assert_eq!(runtime_plan.dci_protocol_systems(), &["uniswap_v2".to_string()]);
    }

    #[test]
    fn resolved_service_launch_config_formats_server_url() {
        let launch_config = ResolvedServiceLaunchConfig::new_for_tests(
            "test-api-key",
            "v1",
            "127.0.0.1",
            4242,
            PlansConfig::default(),
        );

        assert_eq!(launch_config.server_url(), "http://127.0.0.1:4242");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_indexing_tasks_builds_one_extraction_task_for_uniswap_family_runtime() {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let mut rpc_server = Server::new_async().await;
            let _block_number_mock = rpc_server
                .mock("POST", "/")
                .with_status(200)
                .with_header("content-type", "application/json")
                .with_body(r#"{"jsonrpc":"2.0","id":0,"result":"0x1"}"#)
                .create_async()
                .await;

            let chain = Chain::Ethereum;
            let shared_spkg_path =
                write_temp_substreams_package_for_tests("config-start-indexing-tasks-family");

            let extractors = uniswap_family_swap_extractors_for_tests(
                chain,
                42,
                shared_spkg_path.clone(),
                "/tmp/uniswap-v2-member-only.spkg",
                "/tmp/uniswap-v3-member-only.spkg",
            );
            let extractors_config = test_extractor_configs_new(extractors);
            let runtime_targets = extractors_config
                .resolved_runtime_targets()
                .expect("resolved runtime targets");
            let service_config =
                ResolvedIndexerServiceConfig::from_runtime_targets(&runtime_targets);
            let launch_config = ResolvedServiceLaunchConfig::new_for_tests(
                "test-api-key",
                "v1",
                "127.0.0.1",
                0,
                PlansConfig::default(),
            );
            let rpc_client =
                EthereumRpcClient::new(&rpc_server.url()).expect("create mock RPC client");

            let managed_indexer = launch_config
                .start_indexing_tasks(
                    &service_config,
                    runtime_targets,
                    default_family_runtime_registry(),
                    rpc_client,
                    ResolvedIndexerTaskContext {
                        database_url: &db_url,
                        chains: &[chain],
                        retention_horizon: chrono::Utc::now().naive_utc(),
                        endpoint_url: "http://127.0.0.1:1",
                        s3_bucket: None,
                        substreams_api_token: "",
                        database_insert_batch_size: 1000,
                        settlement_contract: alloy::primitives::Address::ZERO,
                        extraction_runtime: None,
                        partial_blocks: false,
                    },
                )
                .await
                .expect("start indexing tasks");

            assert_eq!(managed_indexer.extraction_tasks.total_tasks(), 1);
            assert_eq!(managed_indexer.extraction_tasks.family_extraction_tasks.len(), 1);
            assert_eq!(managed_indexer.extraction_tasks.standalone_extraction_tasks.len(), 0);
            assert_eq!(managed_indexer.service_tasks.len(), 2);

            managed_indexer.shutdown().await;
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_indexing_runtime_plan_builds_one_extraction_task_for_uniswap_family_runtime() {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let mut rpc_server = Server::new_async().await;
            let _block_number_mock = rpc_server
                .mock("POST", "/")
                .with_status(200)
                .with_header("content-type", "application/json")
                .with_body(r#"{"jsonrpc":"2.0","id":0,"result":"0x1"}"#)
                .create_async()
                .await;

            let chain = Chain::Ethereum;
            let shared_spkg_path = write_temp_substreams_package_for_tests(
                "config-start-indexing-runtime-plan-family",
            );

            let extractors = uniswap_family_swap_extractors_for_tests(
                chain,
                42,
                shared_spkg_path.clone(),
                "/tmp/uniswap-v2-member-only.spkg",
                "/tmp/uniswap-v3-member-only.spkg",
            );
            let extractors_config = test_extractor_configs_new(extractors);
            let runtime_plan = extractors_config
                .resolved_indexer_runtime_plan()
                .expect("resolved runtime plan");
            let launch_config = ResolvedServiceLaunchConfig::new_for_tests(
                "test-api-key",
                "v1",
                "127.0.0.1",
                0,
                PlansConfig::default(),
            );
            let rpc_client =
                EthereumRpcClient::new(&rpc_server.url()).expect("create mock RPC client");

            let managed_indexer = runtime_plan
                .start_indexing(
                    &launch_config,
                    rpc_client,
                    ResolvedIndexerTaskContext {
                        database_url: &db_url,
                        chains: &[chain],
                        retention_horizon: chrono::Utc::now().naive_utc(),
                        endpoint_url: "http://127.0.0.1:1",
                        s3_bucket: None,
                        substreams_api_token: "",
                        database_insert_batch_size: 1000,
                        settlement_contract: alloy::primitives::Address::ZERO,
                        extraction_runtime: None,
                        partial_blocks: false,
                    },
                )
                .await
                .expect("start indexing runtime plan");

            assert_eq!(managed_indexer.extraction_tasks.total_tasks(), 1);
            assert_eq!(managed_indexer.extraction_tasks.family_extraction_tasks.len(), 1);
            assert_eq!(managed_indexer.extraction_tasks.standalone_extraction_tasks.len(), 0);
            assert_eq!(managed_indexer.service_tasks.len(), 2);

            managed_indexer.shutdown().await;
        })
        .await;
    }

    #[test]
    fn runtime_target_protocol_views_cover_family_and_standalone_members() {
        let extractors = HashMap::from([
            (
                "uniswap_v2_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v2_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new("pool".to_string(), FinancialType::Swap)],
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                    "map_protocol_changes".to_string(),
                    vec![],
                    0,
                    None,
                    Some(DCIType::RPC),
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v2")
                .with_family_runtime(Some({
                    let mut runtime = family_runtime_config_for_tests(
                        "uniswap",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    );
                    runtime.durability_scope = None;
                    runtime
                })),
            ),
            (
                "uniswap_v3_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v3_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new("pool".to_string(), FinancialType::Swap)],
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                    "map_protocol_changes".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v3")
                .with_family_runtime(Some({
                    let mut runtime = family_runtime_config_for_tests(
                        "uniswap",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    );
                    runtime.durability_scope = None;
                    runtime
                })),
            ),
            (
                "curve_alias".to_string(),
                ExtractorConfig::new(
                    "curve_alias".to_string(),
                    Chain::Ethereum,
                    ImplementationType::Vm,
                    10,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new("curve".to_string(), FinancialType::Swap)],
                    "protocols/substreams/test-curve.spkg".to_string(),
                    "map_events".to_string(),
                    vec![],
                    0,
                    None,
                    Some(DCIType::RPC),
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
        ]);

        let extractors_config = test_extractor_configs_new(extractors);
        let runtime_targets = extractors_config
            .resolved_runtime_targets()
            .expect("resolved runtime targets");

        assert_eq!(
            runtime_targets.protocol_systems(),
            vec!["curve".to_string(), "uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(
            runtime_targets.dci_protocol_systems(),
            vec!["curve".to_string(), "uniswap_v2".to_string()]
        );
    }

    #[test]
    fn extractor_configs_support_recursive_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-extractor-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("fragments")).expect("create temp fragment dir");

        fs::write(
            temp_root.join("fragments/uniswap_v2.yaml"),
            r#"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
"#,
        )
        .expect("write v2 fragment");
        fs::write(
            temp_root.join("fragments/uniswap_v3.yaml"),
            r#"
extractors:
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 43
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
"#,
        )
        .expect("write v3 fragment");
        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
includes:
  - "fragments/uniswap_v2.yaml"
  - "fragments/uniswap_v3.yaml"
extractors: {}
"#,
        )
        .expect("write extractor root");

        let config = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load included extractor configs");

        assert_eq!(
            config
                .extractors
                .get("uniswap_v2")
                .map(ExtractorConfig::start_block),
            Some(25384600)
        );
        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .map(ExtractorConfig::start_block),
            Some(43)
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_merge_family_runtime_defaults_across_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-family-runtime-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("fragments")).expect("create temp fragment dir");
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");
        let v2_substreams = temp_root.join("config/v2_substreams.yaml");
        let v3_substreams = temp_root.join("config/v3_substreams.yaml");
        let shared_bootstrap = temp_root.join("config/shared_bootstrap.yaml");

        fs::write(
            &v2_substreams,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
          protocol: "uniswap_v2"
"#,
        )
        .expect("write v2 substreams params");
        fs::write(
            &v3_substreams,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0xe0554a476a092703abdb3ef35c80e0d76d32939f"
          protocol: "uniswap_v3"
"#,
        )
        .expect("write v3 substreams params");
        fs::write(
            &shared_bootstrap,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
          protocol: "uniswap_v2"
        - pool: "0xe0554a476a092703abdb3ef35c80e0d76d32939f"
          protocol: "uniswap_v3"
"#,
        )
        .expect("write shared bootstrap params");

        fs::write(
            temp_root.join("fragments/family_runtime_base.yaml"),
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    members:
      uniswap_v2:
        substreams_params:
          v2_map_pool_events: "@{}"
extractors: {{}}
"#,
                v2_substreams.display()
            ),
        )
        .expect("write family runtime base fragment");
        fs::write(
            temp_root.join("fragments/family_runtime_overlay.yaml"),
            format!(
                r#"
family_runtimes:
  uniswap:
    stop_block: 1234
    bootstrap:
      params: "@{}"
    members:
      uniswap_v3:
        substreams_params:
          v3_map_events: "@{}"
extractors: {{}}
"#,
                shared_bootstrap.display(),
                v3_substreams.display()
            ),
        )
        .expect("write family runtime overlay fragment");
        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
includes:
  - "fragments/family_runtime_base.yaml"
  - "fragments/family_runtime_overlay.yaml"
extractors:
  alias_v2:
    name: "alias_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write extractor root");

        let config = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load included family runtime defaults");

        let v2 = config
            .extractors
            .get("alias_v2")
            .expect("v2 extractor present");
        let v3 = config
            .extractors
            .get("alias_v3")
            .expect("v3 extractor present");

        assert_eq!(v2.stop_block(), Some(1234));
        assert_eq!(v3.stop_block(), Some(1234));
        assert_eq!(
            v2.family_runtime()
                .and_then(FamilyRuntimeConfig::shared_spkg),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(
            v3.family_runtime()
                .and_then(FamilyRuntimeConfig::shared_spkg),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert!(v2
            .substreams_params
            .get("v2_map_pool_events")
            .expect("v2 member defaults present")
            .contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"));
        assert!(v3
            .substreams_params
            .get("v3_map_events")
            .expect("v3 member defaults present")
            .contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"));
        let v2_bootstrap = v2
            .bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.params.clone())
            .expect("v2 bootstrap defaults present");
        let v3_bootstrap = v3
            .bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.params.clone())
            .expect("v3 bootstrap defaults present");
        assert!(
            v2_bootstrap.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "v2 bootstrap defaults should resolve the v2 branch pool"
        );
        assert!(
            !v2_bootstrap.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "v2 bootstrap defaults should exclude the v3 branch pool"
        );
        assert!(
            v3_bootstrap.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "v3 bootstrap defaults should resolve the v3 branch pool"
        );
        assert!(
            !v3_bootstrap.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "v3 bootstrap defaults should exclude the v2 branch pool"
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_reject_conflicting_family_runtime_defaults_across_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-family-runtime-conflict-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("fragments")).expect("create temp fragment dir");

        fs::write(
            temp_root.join("fragments/family_runtime_base.yaml"),
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/a.spkg"
extractors: {}
"#,
        )
        .expect("write family runtime base fragment");
        fs::write(
            temp_root.join("fragments/family_runtime_conflict.yaml"),
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/b.spkg"
extractors: {}
"#,
        )
        .expect("write conflicting family runtime fragment");
        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
includes:
  - "fragments/family_runtime_base.yaml"
  - "fragments/family_runtime_conflict.yaml"
extractors: {}
"#,
        )
        .expect("write extractor root");

        let err = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect_err("conflicting family runtime defaults should fail");

        assert!(err
            .to_string()
            .contains("conflicting values for family_runtime `uniswap` shared_spkg"));

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn bootstrap_config_supports_recursive_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-bootstrap-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/v2.yaml"),
            r#"
start_block: 42
params:
  routes:
    - token0: "0xaaaa"
      token1: "0xbbbb"
      routers:
        - pool: "0x1111"
          protocol: uniswap_v2
"#,
        )
        .expect("write v2 include");
        fs::write(
            temp_root.join("config/v3.yaml"),
            r#"
start_block: 42
params:
  routes:
    - token0: "0xcccc"
      token1: "0xdddd"
      routers:
        - pool: "0x2222"
          protocol: uniswap_v3
"#,
        )
        .expect("write v3 include");
        fs::write(
            temp_root.join("config/shared.yaml"),
            r#"
includes:
  - "v2.yaml"
  - "v3.yaml"
"#,
        )
        .expect("write shared include");

        let registry = default_family_runtime_registry();
        let v2_allowed = protocol_filter_for_protocol_system("uniswap_v2", registry);
        let (v2_start_block, v2_params) =
            parse_bootstrap_params_file(v2_allowed.as_ref(), &temp_root.join("config/shared.yaml"))
                .expect("parse v2 shared include");
        let v3_allowed = protocol_filter_for_protocol_system("uniswap_v3", registry);
        let (v3_start_block, v3_params) =
            parse_bootstrap_params_file(v3_allowed.as_ref(), &temp_root.join("config/shared.yaml"))
                .expect("parse v3 shared include");

        assert_eq!(v2_start_block, Some(42));
        assert_eq!(v3_start_block, Some(42));
        assert_eq!(v2_params, "bootstrap_block=42&pools=0x1111");
        assert_eq!(v3_params, "bootstrap_block=42&pools=0x2222");

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn substreams_config_supports_recursive_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-substreams-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/shared.yaml"),
            r#"
start_block: 42
params:
  routes:
    - token0: "0xaaaa"
      token1: "0xbbbb"
      routers:
        - pool: "0x1111"
          protocol: uniswap_v2
        - pool: "0x2222"
          protocol: uniswap_v3
"#,
        )
        .expect("write shared include");
        fs::write(
            temp_root.join("config/v2-substreams.yaml"),
            r#"
includes:
  - "shared.yaml"
params:
  extra_flag: "enabled"
"#,
        )
        .expect("write v2 overlay");

        let registry = default_family_runtime_registry();
        let allowed = protocol_filter_for_protocol_system("uniswap_v2", registry);
        let (start_block, params) = parse_substreams_params_file(
            allowed.as_ref(),
            &temp_root.join("config/v2-substreams.yaml"),
        )
        .expect("parse v2 substreams include");

        assert_eq!(start_block, Some(42));
        assert_eq!(
            params,
            "bootstrap_block=42&extra_flag=enabled&pool_tokens=0x1111:0xaaaa:0xbbbb&pools=0x1111"
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn bootstrap_config_supports_route_format() {
        let (start_block, params) = parse_bootstrap_params_yaml_with_filter_and_registry(
            Some("test_protocol"),
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
          protocol: bebop
        - pool: "0x8710039d5de6840ede452a85672b32270a709ae2"
          protocol: fluid
        - pool: "0xc1cd3d0913f4633b43fcddbcd7342bc9b71c676f"
          protocol: uniswapv3
"#,
            default_family_runtime_registry(),
        )
        .expect("route-format bootstrap should parse");

        assert_eq!(start_block, Some(25377208));
        assert_eq!(
            params,
            "bootstrap_block=25377208&pools=0x6f40d4a6237c257fff2db00fa0510deeecd303eb,0x8710039d5de6840ede452a85672b32270a709ae2,0xc1cd3d0913f4633b43fcddbcd7342bc9b71c676f"
        );
    }

    #[test]
    fn extractor_configs_reject_mismatched_start_and_bootstrap_blocks() {
        let err = parse_substreams_params_yaml_with_registry(
            "test_protocol",
            r#"
start_block: 1
params:
  bootstrap_block: 2
  pools:
    - "0xabc"
"#,
            default_family_runtime_registry(),
        )
        .expect_err("mismatched config should fail");

        assert!(err.to_string().contains("must match"));
    }

    #[test]
    fn substreams_params_support_route_format_with_pool_token_metadata() {
        let (start_block, params) = parse_substreams_params_yaml_with_registry(
            "uniswap_v2",
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
          protocol: uniswap_v2
        - pool: "0x8710039d5de6840ede452a85672b32270a709ae2"
          protocol: uniswap_v2
"#,
            default_family_runtime_registry(),
        )
        .expect("route-format substreams params should parse");

        assert_eq!(start_block, Some(25377208));
        assert_eq!(
            params,
            "bootstrap_block=25377208&pool_tokens=0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0x8710039d5de6840ede452a85672b32270a709ae2:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2&pools=0x6f40d4a6237c257fff2db00fa0510deeecd303eb,0x8710039d5de6840ede452a85672b32270a709ae2"
        );
    }

    #[test]
    fn bootstrap_route_format_filters_by_extractor_protocol() {
        let contents = r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#;

        let (_, v2_params) = parse_bootstrap_params_yaml_with_filter_and_registry(
            Some("uniswap_v2"),
            contents,
            default_family_runtime_registry(),
        )
            .expect("v2 bootstrap should parse");
        let (_, v3_params) = parse_bootstrap_params_yaml_with_filter_and_registry(
            Some("uniswap_v3"),
            contents,
            default_family_runtime_registry(),
        )
            .expect("v3 bootstrap should parse");

        assert_eq!(
            v2_params,
            "bootstrap_block=25377208&pools=0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            v3_params,
            "bootstrap_block=25377208&pools=0x2222222222222222222222222222222222222222"
        );
    }

    #[test]
    fn substreams_route_format_filters_by_extractor_protocol() {
        let contents = r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#;

        let (_, v2_params) = parse_substreams_params_yaml_with_registry(
            "uniswap_v2",
            contents,
            default_family_runtime_registry(),
        )
            .expect("v2 substreams params should parse");
        let (_, v3_params) = parse_substreams_params_yaml_with_registry(
            "uniswap_v3",
            contents,
            default_family_runtime_registry(),
        )
            .expect("v3 substreams params should parse");

        assert_eq!(
            v2_params,
            "bootstrap_block=25377208&pool_tokens=0x1111111111111111111111111111111111111111:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2&pools=0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            v3_params,
            "bootstrap_block=25377208&pool_tokens=0x2222222222222222222222222222222222222222:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2&pools=0x2222222222222222222222222222222222222222"
        );
    }

    #[test]
    fn extractor_configs_keep_v2_params_consistent_between_v2_only_and_v2_v3() {
        let temp_root = make_temp_config_root("uniswap-shared-bootstrap");
        write_shared_uniswap_bootstrap_fixture(&temp_root);

        write_uniswap_extractor_fixture(
            &temp_root,
            "extractors.uniswap_v2.yaml",
            true,
            false,
            true,
            false,
        );
        write_uniswap_extractor_fixture(
            &temp_root,
            "extractors.uniswap_v2_v3.yaml",
            true,
            true,
            true,
            true,
        );

        let v2_only = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.uniswap_v2.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v2-only extractor config");
        let v2_v3 = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v2+v3 extractor config");

        let v2_only_extractor = v2_only
            .extractors
            .get("uniswap_v2")
            .expect("v2-only extractor present");
        let v2_v3_extractor = v2_v3
            .extractors
            .get("uniswap_v2")
            .expect("v2 extractor present in combined config");

        assert_eq!(v2_only_extractor.start_block(), v2_v3_extractor.start_block());
        assert_eq!(
            v2_only_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            v2_v3_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            v2_only_extractor
                .substreams_params
                .get("map_pool_events"),
            v2_v3_extractor
                .substreams_params
                .get("map_pool_events")
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_keep_v2_params_consistent_between_default_and_combined() {
        let temp_root = make_temp_config_root("uniswap-default-parity");
        write_shared_uniswap_bootstrap_fixture(&temp_root);

        write_uniswap_extractor_fixture(&temp_root, "extractors.yaml", true, true, true, false);
        write_uniswap_extractor_fixture(
            &temp_root,
            "extractors.uniswap_v2_v3.yaml",
            true,
            true,
            true,
            false,
        );

        let default_config = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load default extractor config");
        let combined_config = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load combined extractor config");

        let default_v2 = default_config
            .extractors
            .get("uniswap_v2")
            .expect("default v2 extractor present");
        let combined_v2 = combined_config
            .extractors
            .get("uniswap_v2")
            .expect("combined v2 extractor present");

        assert_eq!(default_v2.start_block(), combined_v2.start_block());

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_keep_v3_params_consistent_between_v3_only_and_v2_v3() {
        let temp_root = make_temp_config_root("uniswap-v3-shared-bootstrap");
        write_shared_uniswap_bootstrap_fixture(&temp_root);

        write_uniswap_extractor_fixture(
            &temp_root,
            "extractors.uniswap_v3.yaml",
            false,
            true,
            false,
            true,
        );
        write_uniswap_extractor_fixture(
            &temp_root,
            "extractors.uniswap_v2_v3.yaml",
            true,
            true,
            true,
            true,
        );

        let v3_only = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.uniswap_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v3-only extractor config");
        let v2_v3 = test_extractor_configs_from_yaml(
            temp_root
                .join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v2+v3 extractor config");

        let v3_only_extractor = v3_only
            .extractors
            .get("uniswap_v3")
            .expect("v3-only extractor present");
        let v2_v3_extractor = v2_v3
            .extractors
            .get("uniswap_v3")
            .expect("v3 extractor present in combined config");

        assert_eq!(v3_only_extractor.start_block(), v2_v3_extractor.start_block());
        assert_eq!(
            v3_only_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            v2_v3_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            v3_only_extractor
                .substreams_params
                .get("map_events"),
            v2_v3_extractor
                .substreams_params
                .get("map_events")
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn repo_uniswap_v2_combined_substream_config_keeps_v2_filters_and_rejects_legacy_entrypoints() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let default_config_err = test_extractor_configs_from_yaml(
            root.join("extractors.yaml")
                .to_str()
                .expect("utf8 default config path"),
        )
        .expect_err("default repo config should reject standalone uniswap family members");
        let v2_only_config_err = test_extractor_configs_from_yaml(
            root.join("extractors.uniswap_v2.yaml")
                .to_str()
                .expect("utf8 v2-only config path"),
        )
        .expect_err("v2-only repo config should reject standalone uniswap family members");
        let combined_substream_config = test_extractor_configs_from_yaml(
            root.join("extractors.uniswap_v2_v3.combined.yaml")
                .to_str()
                .expect("utf8 combined-substream config path"),
        )
        .expect("load combined-substream extractors config");

        let combined_substream_v2 = combined_substream_config
            .extractors
            .get("uniswap_v2")
            .expect("combined-substream v2 extractor present");

        assert!(default_config_err
            .to_string()
            .contains("belongs to registered family runtime `uniswap`"));
        assert!(v2_only_config_err
            .to_string()
            .contains("belongs to registered family runtime `uniswap`"));
        let combined_substream_v2_bootstrap = combined_substream_v2
            .bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.params.clone())
            .expect("combined-substream v2 bootstrap params present");
        assert!(
            combined_substream_v2_bootstrap.contains("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"),
            "combined-substream v2 bootstrap should keep v2 pools"
        );
        assert!(
            !combined_substream_v2_bootstrap.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "combined-substream v2 bootstrap should exclude v3 pools"
        );
        let combined_substream_v2_params = combined_substream_v2
            .substreams_params
            .get("v2_map_pool_events")
            .expect("combined-substream v2 params present");
        assert!(
            combined_substream_v2_params.contains("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"),
            "combined-substream v2 params should keep v2 pools"
        );
        assert!(
            !combined_substream_v2_params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "combined-substream v2 params should exclude v3 pools"
        );

        let combined_substream_yaml =
            fs::read_to_string(root.join("extractors.uniswap_v2_v3.combined.yaml"))
                .expect("read combined-substream config");
        let combined_v2_fragment =
            fs::read_to_string(root.join("extractors.fragments/uniswap_v2_combined.yaml"))
                .expect("read combined-substream v2 fragment");

        assert!(
            combined_substream_yaml.contains("extractors.fragments/uniswap_v2_combined.yaml"),
            "combined-substream config should include the v2 combined fragment"
        );
        assert!(
            combined_v2_fragment.contains("module_name: \"v2_map_pool_events\""),
            "combined-substream v2 fragment should point at the combined package module"
        );
        assert!(
            combined_substream_yaml.contains("members:"),
            "combined-substream config should declare top-level family member defaults"
        );
        assert!(
            combined_substream_yaml
                .contains("v2_map_pool_events: \"@config/uniswap_v2_substreams.yaml\""),
            "combined-substream config should centralize v2 substreams params at the family level"
        );
        assert!(
            combined_substream_yaml
                .contains("v3_map_events: \"@config/uniswap_v3_substreams.yaml\""),
            "combined-substream config should centralize v3 substreams params at the family level"
        );
        assert!(combined_substream_yaml.contains("family_runtimes:"));
        assert!(
            !combined_substream_yaml.contains("shared_module:"),
            "combined-substream config should rely on the registry-owned shared module default"
        );
    }

    #[test]
    fn repo_shared_uniswap_bootstrap_is_materialized_and_legacy_wrappers_remain_compatible() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let shared_bootstrap =
            fs::read_to_string(root.join("config/shared_uniswap_bootstrap.yaml"))
                .expect("read shared bootstrap config");
        let registry = default_family_runtime_registry();
        let v2_allowed = protocol_filter_for_protocol_system("uniswap_v2", registry);
        let v3_allowed = protocol_filter_for_protocol_system("uniswap_v3", registry);

        assert!(
            shared_bootstrap.contains("routes:"),
            "shared bootstrap should be a materialized route-format config"
        );
        assert!(
            !shared_bootstrap.contains("includes:"),
            "shared bootstrap should no longer be only an include wrapper"
        );

        let (shared_v2_start_block, shared_v2_params) = parse_substreams_params_yaml_with_registry(
            "uniswap_v2",
            &shared_bootstrap,
            default_family_runtime_registry(),
        )
        .expect("parse shared bootstrap config for v2");
        let (shared_v3_start_block, shared_v3_params) =
            parse_bootstrap_params_yaml_with_filter_and_registry(
                Some("uniswap_v3"),
                &shared_bootstrap,
                default_family_runtime_registry(),
            )
            .expect("parse shared bootstrap config for v3");
        let (v2_wrapper_start_block, v2_wrapper_params) = parse_substreams_params_file(
            v2_allowed.as_ref(),
            &root.join("config/uniswap_v2_bootstrap.yaml"),
        )
        .expect("parse v2 bootstrap wrapper");
        let (v3_wrapper_start_block, v3_wrapper_params) = parse_bootstrap_params_file(
            v3_allowed.as_ref(),
            &root.join("config/uniswap_v3_bootstrap.yaml"),
        )
        .expect("parse v3 bootstrap wrapper");

        assert_eq!(shared_v2_start_block, v2_wrapper_start_block);
        assert_eq!(shared_v3_start_block, v3_wrapper_start_block);
        assert_eq!(shared_v2_params, v2_wrapper_params);
        assert_eq!(shared_v3_params, v3_wrapper_params);
        assert_eq!(v2_wrapper_start_block, v3_wrapper_start_block);
        assert!(
            shared_v2_params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "shared bootstrap v2 filter should keep canonical v2 pools"
        );
        assert!(
            !shared_v2_params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "shared bootstrap v2 filter should exclude canonical v3 pools"
        );
        assert!(
            shared_v3_params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "shared bootstrap v3 filter should keep canonical v3 pools"
        );
        assert!(
            !shared_v3_params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "shared bootstrap v3 filter should exclude canonical v2 pools"
        );
    }

    #[test]
    fn repo_shared_uniswap_substreams_is_materialized_and_member_wrappers_remain_compatible() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let shared_substreams =
            fs::read_to_string(root.join("config/shared_uniswap_substreams.yaml"))
                .expect("read shared substreams config");
        let registry = default_family_runtime_registry();
        let v2_allowed = protocol_filter_for_protocol_system("uniswap_v2", registry);
        let v3_allowed = protocol_filter_for_protocol_system("uniswap_v3", registry);

        assert!(
            shared_substreams.contains("routes:"),
            "shared substreams should be a materialized route-format config"
        );
        assert!(
            !shared_substreams.contains("includes:"),
            "shared substreams should no longer be only an include wrapper"
        );

        let (shared_v2_start_block, shared_v2_params) = parse_substreams_params_yaml_with_registry(
            "uniswap_v2",
            &shared_substreams,
            default_family_runtime_registry(),
        )
        .expect("parse shared substreams config for v2");
        let (shared_v3_start_block, shared_v3_params) = parse_substreams_params_yaml_with_registry(
            "uniswap_v3",
            &shared_substreams,
            default_family_runtime_registry(),
        )
        .expect("parse shared substreams config for v3");
        let (v2_wrapper_start_block, v2_wrapper_params) = parse_substreams_params_file(
            v2_allowed.as_ref(),
            &root.join("config/uniswap_v2_substreams.yaml"),
        )
        .expect("parse v2 substreams wrapper");
        let (v3_wrapper_start_block, v3_wrapper_params) = parse_substreams_params_file(
            v3_allowed.as_ref(),
            &root.join("config/uniswap_v3_substreams.yaml"),
        )
        .expect("parse v3 substreams wrapper");

        let split_params = |params: &str| {
            params
                .split('&')
                .filter(|part| !part.is_empty())
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        };

        assert_eq!(shared_v2_start_block, v2_wrapper_start_block);
        assert_eq!(shared_v2_params, v2_wrapper_params);
        assert_eq!(shared_v3_start_block, v3_wrapper_start_block);
        assert!(
            v3_wrapper_params.contains("factory=0x1F98431c8aD98523631AE4a59f267346ea31F984"),
            "v3 substreams wrapper should preserve the factory filter"
        );
        let mut expected_v3_parts = split_params(&shared_v3_params);
        expected_v3_parts.insert("factory=0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string());
        assert_eq!(expected_v3_parts, split_params(&v3_wrapper_params));
        assert_eq!(v2_wrapper_start_block, v3_wrapper_start_block);
        assert!(
            shared_v2_params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "shared substreams v2 filter should keep canonical v2 pools"
        );
        assert!(
            !shared_v2_params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "shared substreams v2 filter should exclude canonical v3 pools"
        );
        assert!(
            shared_v3_params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "shared substreams v3 filter should keep canonical v3 pools"
        );
        assert!(
            !shared_v3_params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "shared substreams v3 filter should exclude canonical v2 pools"
        );
    }

    #[test]
    fn repo_combined_uniswap_config_builds_one_family_runtime_plan() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let combined_config = test_extractor_configs_from_yaml(
            root.join("extractors.uniswap_v2_v3.combined.yaml")
                .to_str()
                .expect("utf8 combined-substream config path"),
        )
        .expect("load combined-substream extractors config");
        let standard_config_err = test_extractor_configs_from_yaml(
            root.join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 standard combined config path"),
        )
        .expect_err("legacy non-combined V2+V3 config should now be rejected");

        let combined_plan = combined_config
            .family_runtime_plan()
            .expect("combined config should build a family runtime plan");
        let combined_targets = combined_config
            .resolved_runtime_targets()
            .expect("combined config should build resolved runtime targets");

        assert_eq!(combined_plan.families.len(), 1);
        assert_eq!(combined_plan.families[0].family_name(), "uniswap");
        assert_eq!(
            combined_plan.families[0].member_protocol_systems(),
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert!(combined_plan.families[0]
            .shared_spkg()
            .contains("ethereum-uniswap-v2-v3-combined"));
        assert_eq!(
            combined_plan.families[0].output_module(),
            default_family_runtime_registry()
                .output_module_for_family("uniswap")
                .expect("registered uniswap output module")
        );
        assert!(combined_plan
            .standalone_protocol_systems
            .is_empty());
        assert_eq!(combined_targets.len(), 1);
        let combined_family = combined_targets.as_slice()[0]
            .family()
            .expect("combined config should resolve to one shared family runtime target");
        assert_eq!(combined_family.family_name(), "uniswap");
        assert_eq!(
            combined_family
                .extractor_configs
                .iter()
                .map(|config| config.protocol_system().to_string())
                .collect::<Vec<_>>(),
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        let shared_bootstrap_plan = combined_config
            .shared_bootstrap_plan_for_family("uniswap")
            .expect("combined config should build one shared bootstrap plan");
        assert_eq!(shared_bootstrap_plan.family_name, "uniswap");
        assert_eq!(
            shared_bootstrap_plan
                .branches
                .iter()
                .map(|branch| branch.protocol_system.as_str())
                .collect::<Vec<_>>(),
            vec!["uniswap_v2", "uniswap_v3"]
        );

        assert!(standard_config_err
            .to_string()
            .contains("belongs to registered family runtime `uniswap`"));
    }

    #[test]
    fn shared_bootstrap_plan_family_helper_preserves_custom_family_registry() {
        const FUTURE_FAMILY_MEMBERS: &[FamilyMemberSpec] = &[
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v1",
                &["futurev1"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v2",
                &["futurev2"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
        ];
        const FUTURE_FAMILY: FamilyRuntimeSpec =
            canonical_shared_family_runtime_spec!(
                "future_swap",
                FUTURE_FAMILY_MEMBERS,
                None,
                shared_progress_owner_protocol_system: "future_v1",
                durability_scope: "family::future_swap_runtime",
            );

        let registry = test_registry_with_future_family(FUTURE_FAMILY);
        let temp_root = write_future_family_yaml_fixture_for_tests();
        let config = ExtractorConfigs::from_yaml_with_registry(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
            registry,
        )
        .expect("load future family config through yaml entrypoint");

        let plan = config
            .shared_bootstrap_plan_for_family("future_swap")
            .expect("custom future-family config should build one shared bootstrap plan");

        assert_eq!(plan.family_name, "future_swap");
        assert_eq!(plan.bootstrap_block, 99);
        assert_eq!(
            plan.branches
                .iter()
                .map(|branch| branch.protocol_system.as_str())
                .collect::<Vec<_>>(),
            vec!["future_v1", "future_v2"]
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn shared_bootstrap_plan_family_helper_rejects_unknown_family() {
        let extractors = HashMap::from([(
            "curve".to_string(),
            swap_extractor_config_for_tests(
                "curve",
                "curve",
                Chain::Ethereum,
                ImplementationType::Vm,
                0,
                "curve_pool",
                "protocols/substreams/curve/curve.spkg",
                "map_curve",
                None,
            ),
        )]);
        let config =
            ExtractorConfigs::new_with_registry(extractors, default_family_runtime_registry());

        let err = config
            .shared_bootstrap_plan_for_family("missing_family")
            .expect_err("unknown family should be rejected");

        assert!(
            err.to_string()
                .contains("shared bootstrap family `missing_family` does not match any registered family runtime")
        );
    }

    #[test]
    fn shared_bootstrap_plan_helper_rejects_duplicate_protocol_system_configs() {
        let chain = Chain::Ethereum;
        let shared_spkg_path =
            write_temp_substreams_package_for_tests("config-shared-bootstrap-duplicate-protocol");
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                swap_extractor_config_for_tests(
                    "uniswap_v2",
                    "uniswap_v2",
                    chain,
                    ImplementationType::Custom,
                    42,
                    "uniswap_v2_pool",
                    "/tmp/uniswap-v2-member-only.spkg",
                    "v2_map_pool_events",
                    Some(family_runtime_config_for_tests("uniswap", shared_spkg_path.clone())),
                ),
            ),
            (
                "duplicate_uniswap_v2".to_string(),
                swap_extractor_config_for_tests(
                    "duplicate_uniswap_v2",
                    "uniswap_v2",
                    chain,
                    ImplementationType::Custom,
                    42,
                    "uniswap_v2_pool",
                    "/tmp/uniswap-v2-member-only-duplicate.spkg",
                    "v2_map_pool_events",
                    Some(family_runtime_config_for_tests("uniswap", shared_spkg_path)),
                ),
            ),
        ]);
        let config = test_extractor_configs_new(extractors);

        let err = config
            .shared_bootstrap_plan_for_protocol_systems(["uniswap_v2"])
            .expect_err("duplicate protocol_system configs should be rejected");

        assert!(err
            .to_string()
            .contains("multiple extractor configs declare protocol_system `uniswap_v2`"));
    }

    #[test]
    fn extractor_config_defaults_protocol_system_to_name() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-extractor-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  sample:
    name: "sample"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
"#,
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("sample")
            .expect("sample extractor present");

        assert_eq!(extractor.name(), "sample");
        assert_eq!(extractor.protocol_system(), "sample");

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_family_runtime_defaults_from_top_level() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    durability_scope: "{durability_scope}"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    spkg: "sample-v3.spkg"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                durability_scope = test_uniswap_durability_scope(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");
        let family_runtime = extractor
            .family_runtime()
            .expect("family runtime present");

        assert_eq!(family_runtime.family, "uniswap");
        assert_eq!(
            family_runtime.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(
            family_runtime.shared_module.as_deref(),
            default_family_runtime_registry().output_module_for_family("uniswap")
        );
        assert_eq!(
            family_runtime
                .durability_scope
                .as_deref(),
            Some(test_uniswap_durability_scope())
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn aliased_family_member_names_still_resolve_to_one_shared_family_runtime_target() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-aliased-members-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
extractors:
  alias_v2_member:
    name: "alias_v2_member"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool_v2"
        financial_type: "Swap"
    spkg: "sample-v2.spkg"
    module_name: "v2_map_pool_events"
    family_runtime:
      family: "uniswap"
  alias_v3_member:
    name: "alias_v3_member"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    spkg: "sample-v3.spkg"
    module_name: "v3_map_protocol_changes"
    family_runtime:
      family: "uniswap"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let resolved_targets = config
            .resolved_runtime_targets()
            .expect("aliased family members should resolve runtime targets");

        assert_eq!(resolved_targets.len(), 1);
        let family = resolved_targets.as_slice()[0]
            .family()
            .expect("aliased family members should collapse to one shared family target");

        assert_eq!(family.family_name(), "uniswap");
        assert_eq!(
            family.member_protocol_systems(),
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(
            family
                .extractor_configs
                .iter()
                .map(|config| config.name().to_string())
                .collect::<Vec<_>>(),
            vec!["alias_v2_member".to_string(), "alias_v3_member".to_string()]
        );
        assert_eq!(
            family
                .extractor_configs
                .iter()
                .map(|config| config.protocol_system().to_string())
                .collect::<Vec<_>>(),
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(
            family.output_module(),
            default_family_runtime_registry()
                .output_module_for_family("uniswap")
                .expect("registered uniswap output module")
        );
        assert!(
            family
                .shared_spkg()
                .contains("ethereum-uniswap-v2-v3-combined"),
            "shared family runtime should still use the combined shared spkg"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn top_level_family_runtime_defaults_resolve_member_runtime_through_one_entrypoint() {
        let defaults = RawFamilyRuntimeDefaults {
            shared_spkg: Some(
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
            ),
            shared_module: None,
            durability_scope: Some(test_uniswap_durability_scope().to_string()),
            stop_block: None,
            bootstrap: None,
            members: HashMap::new(),
        };

        let resolved = defaults
            .resolve_family_runtime_config(
                "uniswap_v2",
                {
                    let mut runtime =
                        family_runtime_config_for_tests("uniswap", "/tmp/member-only.spkg");
                    runtime.shared_spkg = None;
                    runtime.shared_module = None;
                    runtime.durability_scope = None;
                    runtime
                },
                default_family_runtime_registry(),
            )
            .expect("top-level defaults should resolve member runtime");

        assert_eq!(
            resolved.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(
            resolved.shared_module.as_deref(),
            default_family_runtime_registry().output_module_for_family("uniswap")
        );
        assert_eq!(resolved.durability_scope.as_deref(), Some(test_uniswap_durability_scope()));
    }

    #[test]
    fn top_level_family_runtime_defaults_do_not_enable_shared_runtime_without_member_opt_in() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-no-opt-in-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample-v2.spkg"
    module_name: "map_sample_v2"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample-v3.spkg"
    module_name: "map_sample_v3"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let plan = config
            .family_runtime_plan()
            .expect("build runtime plan");

        assert!(
            plan.families.is_empty(),
            "top-level family defaults alone must not opt members into the shared runtime"
        );
        assert_eq!(
            plan.standalone_protocol_systems,
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn config_load_rejects_explicit_family_runtime_with_missing_member_extractor() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-missing-member-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample_v2"
    family_runtime:
      family: "uniswap"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("missing family member should fail during config load");

        assert!(err
            .to_string()
            .contains("requires every declared member extractor to be present once any member opts into the shared runtime"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn config_load_rejects_partial_shared_bootstrap_family_config() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-partial-bootstrap-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample_v2"
    family_runtime:
      family: "uniswap"
    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "bootstrap_block=42&pools=0x0000000000000000000000000000000000000001"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("partial shared bootstrap config should fail during config load");

        assert!(err
            .to_string()
            .contains("requires shared bootstrap configuration consistency across members"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_spkg_from_family_runtime_defaults() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-spkg-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");

        assert_eq!(
            extractor.spkg(),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_stop_block_from_family_runtime_defaults() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-stop-block-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
    stop_block: 1234
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");

        assert_eq!(extractor.stop_block(), Some(1234));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_explicit_stop_block_overrides_family_runtime_default() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-stop-block-override-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
    stop_block: 1234
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    stop_block: 5678
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");

        assert_eq!(extractor.stop_block(), Some(5678));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_family_member_substreams_params_from_top_level() {
        let v2_substreams_config =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("config/uniswap_v2_substreams.yaml");
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-member-substreams-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
    members:
      uniswap_v2:
        substreams_params:
          v2_map_pool_events: "@{v2_substreams_config}"
extractors:
  alias_v2:
    name: "alias_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: {start_block}
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: {start_block}
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                shared_module = test_uniswap_shared_module(),
                start_block = test_uniswap_shared_start_block(),
                v2_substreams_config = v2_substreams_config.display(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("alias_v2")
            .expect("aliased v2 extractor present");
        let params = extractor
            .substreams_params
            .get("v2_map_pool_events")
            .expect("resolved params present");

        assert!(
            params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "inherited family member defaults should resolve v2 pool filters"
        );
        assert!(
            !params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "v2 inherited params should exclude v3 pools"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_rejects_unknown_family_runtime() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-unknown-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "nonexistent_family"
"#,
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("unknown family runtime should fail");

        assert!(err.to_string().contains(
            "family_runtime `nonexistent_family` does not match any registered family runtime"
        ));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn top_level_family_runtime_defaults_reject_unknown_family() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-defaults-unknown-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  nonexistent_family:
    shared_spkg: "protocols/substreams/nonexistent/test.spkg"
    shared_module: "map_nonexistent_family_protocol_changes"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
"#,
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("unknown top-level family runtime should fail");

        assert!(err.to_string().contains(
            "family_runtime `nonexistent_family` does not match any registered family runtime"
        ));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn top_level_family_runtime_defaults_reject_unknown_member_protocol_system() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-defaults-unknown-member-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    members:
      curve:
        shared_route_protocols:
          - "curve"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
"#,
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("unknown top-level family member defaults should fail");

        assert!(err.to_string().contains(
            "family_runtime member defaults for `uniswap` cannot be applied to protocol system `curve`"
        ));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_rejects_protocol_system_not_in_declared_family() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-membership-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
extractors:
  sample_curve:
    name: "sample_curve"
    protocol_system: "curve"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
      shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
      shared_module: "{shared_module}"
"#,
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("mismatched protocol family should fail");

        assert!(err
            .to_string()
            .contains("cannot be applied to protocol system `curve` because that protocol is not a declared member of the family"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_requires_spkg_without_family_shared_spkg() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-spkg-missing-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
"#,
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("missing spkg should fail");

        assert!(err
            .to_string()
            .contains("must declare `spkg` unless its family_runtime resolves `shared_spkg`"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_family_bootstrap_defaults_from_top_level() {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        );
        let config_path = std::env::temp_dir()
            .join(format!("tycho-indexer-family-bootstrap-config-{unique}.yaml"));
        let bootstrap_path = std::env::temp_dir()
            .join(format!("tycho-indexer-family-bootstrap-params-{unique}.yaml"));

        fs::write(
            &bootstrap_path,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0x0000000000000000000000000000000000000003"
          protocol: "uniswapv2"
        - pool: "0x0000000000000000000000000000000000000004"
          protocol: "uniswapv3"
"#,
        )
        .expect("write family bootstrap params");
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
    bootstrap:
      params: "@{}"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    spkg: "sample-v3.spkg"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                bootstrap_path.display(),
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");
        let bootstrap = extractor
            .bootstrap
            .as_ref()
            .expect("bootstrap present");
        let v3_extractor = config
            .extractors
            .get("sample_v3")
            .expect("sample v3 extractor present");
        let v3_bootstrap = v3_extractor
            .bootstrap
            .as_ref()
            .expect("sample v3 bootstrap present");

        assert_eq!(bootstrap.strategy, BootstrapStrategy::UniswapV2Rpc);
        assert_eq!(bootstrap.start_block, 42);
        assert_eq!(
            bootstrap.params,
            "bootstrap_block=42&pools=0x0000000000000000000000000000000000000003"
        );
        assert_eq!(v3_bootstrap.strategy, BootstrapStrategy::UniswapV3Rpc);
        assert_eq!(v3_bootstrap.start_block, 42);
        assert_eq!(
            v3_bootstrap.params,
            "bootstrap_block=42&pools=0x0000000000000000000000000000000000000004"
        );

        let _ = fs::remove_file(config_path);
        let _ = fs::remove_file(bootstrap_path);
    }

    #[test]
    fn family_bootstrap_defaults_reject_protocol_not_declared_in_family() {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        );
        let config_path = std::env::temp_dir()
            .join(format!("tycho-indexer-family-bootstrap-mismatch-{unique}.yaml"));
        let bootstrap_path = std::env::temp_dir()
            .join(format!("tycho-indexer-family-bootstrap-mismatch-params-{unique}.yaml"));

        fs::write(
            &bootstrap_path,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0x0000000000000000000000000000000000000003"
          protocol: "uniswapv2"
"#,
        )
        .expect("write family bootstrap params");
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "{shared_module}"
    bootstrap:
      params: "@{}"
extractors:
  sample_curve:
    name: "sample_curve"
    protocol_system: "curve"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
                bootstrap_path.display(),
                shared_module = test_uniswap_shared_module(),
            ),
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("protocol outside family should fail");

        assert!(err
            .to_string()
            .contains("protocol system `curve` is not a declared member of that family"));

        let _ = fs::remove_file(config_path);
        let _ = fs::remove_file(bootstrap_path);
    }

    #[test]
    fn shared_route_filter_uses_protocol_system_not_extractor_key() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let shared_substreams = root.join("config/shared_uniswap_substreams.yaml");
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-protocol-filter-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
extractors:
  uniswap_v2_member:
    name: "uniswap_v2_member"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
      shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
      shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    substreams_params:
      v3_map_events: "@{}"
"#,
                shared_substreams.display()
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("alias_v3")
            .expect("aliased v3 extractor present");
        let params = extractor
            .substreams_params
            .get("v3_map_events")
            .expect("resolved params present");

        assert!(
            params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "v3 params should keep v3 pools from shared bootstrap routes"
        );
        assert!(
            !params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "v3 params should exclude v2 pools even when extractor key is aliased"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn explicit_protocol_system_drives_route_filter_without_family_runtime() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let shared_bootstrap = root.join("config/shared_uniswap_bootstrap.yaml");
        let shared_substreams = root.join("config/shared_uniswap_substreams.yaml");
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-protocol-filter-standalone-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            format!(
                r#"
extractors:
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 25384600
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    substreams_params:
      v3_map_events: "@{}"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@{}"
"#,
                shared_substreams.display(),
                shared_bootstrap.display()
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("alias_v3")
            .expect("aliased standalone v3 extractor present");
        let params = extractor
            .substreams_params
            .get("v3_map_events")
            .expect("resolved params present");
        let bootstrap = extractor
            .bootstrap
            .as_ref()
            .expect("resolved bootstrap present");

        assert!(
            params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "standalone v3 params should keep v3 pools from shared bootstrap routes"
        );
        assert!(
            !params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "standalone v3 params should exclude v2 pools even when extractor name is aliased"
        );
        assert!(
            bootstrap
                .params
                .contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "standalone v3 bootstrap should keep v3 pools from shared bootstrap routes"
        );
        assert!(
            !bootstrap
                .params
                .contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "standalone v3 bootstrap should exclude v2 pools even when extractor name is aliased"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn protocol_filter_for_protocol_system_comes_from_family_registry() {
        let registry = default_family_runtime_registry();
        assert_eq!(
            protocol_filter_for_protocol_system("uniswap_v2", registry),
            Some(HashSet::from(["uniswapv2".to_string()]))
        );
        assert_eq!(
            protocol_filter_for_protocol_system("uniswap_v3", registry),
            Some(HashSet::from(["uniswapv3".to_string()]))
        );
        assert_eq!(protocol_filter_for_protocol_system("curve", registry), None);
    }

    #[test]
    fn family_member_shared_route_protocols_override_registry_defaults() {
        let params_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-route-filter-override-params-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-route-filter-override-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &params_path,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
          protocol: "custom-v2"
        - pool: "0xe0554a476a092703abdb3ef35c80e0d76d32939f"
          protocol: "uniswap_v3"
"#,
        )
        .expect("write params");
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    members:
      uniswap_v2:
        shared_route_protocols:
          - "custom-v2"
extractors:
  alias_v2:
    name: "alias_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
    substreams_params:
      v2_map_pool_events: "@{}"
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
                params_path.display()
            ),
        )
        .expect("write config");

        let config = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect("load config");
        let extractor = config
            .extractors
            .get("alias_v2")
            .expect("aliased v2 extractor present");
        let params = extractor
            .substreams_params
            .get("v2_map_pool_events")
            .expect("resolved params present");

        assert!(
            params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "member route defaults should include pools selected by the custom v2 alias"
        );
        assert!(
            !params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "member route defaults should exclude pools outside the overridden v2 alias set"
        );

        let _ = fs::remove_file(params_path);
        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn family_member_shared_route_protocol_defaults_reject_conflicting_aliases() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-route-filter-conflict-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    members:
      uniswap_v2:
        shared_route_protocols:
          - "shared-alias"
      uniswap_v3:
        shared_route_protocols:
          - "shared_alias"
extractors: {}
"#,
        )
        .expect("write config");

        let err = test_extractor_configs_from_yaml(
            config_path
                .to_str()
                .expect("utf8 config path"),
        )
        .expect_err("conflicting member route aliases should fail");

        let err_text = err.to_string();
        assert!(err_text.contains("family_runtime `uniswap` assigns shared_route_protocol"));
        assert!(err_text.contains("uniswap_v2"));
        assert!(err_text.contains("uniswap_v3"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn custom_registry_loads_future_family_from_yaml_entrypoint() {
        const FUTURE_FAMILY_MEMBERS: &[FamilyMemberSpec] = &[
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v1",
                &["futurev1"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v2",
                &["futurev2"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
        ];
        const FUTURE_FAMILY: FamilyRuntimeSpec =
            canonical_shared_family_runtime_spec!(
                "future_swap",
                FUTURE_FAMILY_MEMBERS,
                None,
                shared_progress_owner_protocol_system: "future_v1",
                durability_scope: "family::future_swap_runtime",
            );

        let registry = test_registry_with_future_family(FUTURE_FAMILY);
        let temp_root = write_future_family_yaml_fixture_for_tests();

        let config = ExtractorConfigs::from_yaml_with_registry(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
            registry,
        )
        .expect("load future family config through yaml entrypoint");

        for (protocol_system, expected_pool, unexpected_pool) in [
            (
                "future_v1",
                "0x0000000000000000000000000000000000000011",
                "0x0000000000000000000000000000000000000022",
            ),
            (
                "future_v2",
                "0x0000000000000000000000000000000000000022",
                "0x0000000000000000000000000000000000000011",
            ),
        ] {
            let extractor = config
                .extractors
                .get(protocol_system)
                .expect("future extractor present");
            let runtime = extractor
                .family_runtime()
                .expect("future family runtime resolved");
            let substreams_params = extractor
                .substreams_params
                .get(&format!("{protocol_system}_map_events"))
                .expect("future shared substreams params resolved");
            let bootstrap = extractor
                .bootstrap
                .as_ref()
                .expect("future shared bootstrap resolved");

            assert_eq!(extractor.start_block(), 99);
            assert_eq!(
                runtime.shared_spkg.as_deref(),
                Some("protocols/substreams/future-swap-combined/test.spkg")
            );
            assert_eq!(
                runtime.shared_module.as_deref(),
                Some("map_future_swap_family_protocol_changes")
            );
            assert_eq!(runtime.durability_scope.as_deref(), Some("family::future_swap_runtime"));
            assert!(substreams_params.contains(expected_pool));
            assert!(!substreams_params.contains(unexpected_pool));
            assert!(substreams_params.contains(&format!(
                "pool_tokens={expected_pool}:0x00000000000000000000000000000000000000a1:0x00000000000000000000000000000000000000b1"
            )));
            assert!(!substreams_params.contains(unexpected_pool));
            assert_eq!(bootstrap.strategy, BootstrapStrategy::UniswapV2Rpc);
            assert_eq!(bootstrap.start_block, 99);
            assert!(bootstrap.params.contains(expected_pool));
            assert!(!bootstrap
                .params
                .contains(unexpected_pool));
        }

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn resolved_indexer_runtime_plan_preserves_custom_family_registry_for_startup() {
        const FUTURE_FAMILY_MEMBERS: &[FamilyMemberSpec] = &[
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v1",
                &["futurev1"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v2",
                &["futurev2"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
        ];
        const FUTURE_FAMILY: FamilyRuntimeSpec =
            canonical_shared_family_runtime_spec!(
                "future_swap",
                FUTURE_FAMILY_MEMBERS,
                None,
                shared_progress_owner_protocol_system: "future_v1",
                durability_scope: "family::future_swap_runtime",
            );

        let registry = test_registry_with_future_family(FUTURE_FAMILY);
        let temp_root = write_future_family_yaml_fixture_for_tests();
        let loaded_runtime_plan = LoadedIndexerRuntimePlan::from_yaml_with_registry(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
            registry,
        )
        .expect("load runtime owner with custom registry");
        let runtime_plan = loaded_runtime_plan
            .resolved_runtime_plan()
            .expect("build runtime plan with custom registry");

        loaded_runtime_plan
            .extractors_config()
            .family_runtime_registry()
            .require_family_spec("future_swap", "runtime plan startup registry")
            .expect("loaded runtime owner should preserve the custom family registry used for planning");

        assert_eq!(
            runtime_plan.protocol_systems(),
            &["future_v1".to_string(), "future_v2".to_string()],
            "resolved runtime targets should still reflect the future family members planned through the preserved registry"
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_default_helpers_preserve_custom_family_registry() {
        const FUTURE_FAMILY_MEMBERS: &[FamilyMemberSpec] = &[
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v1",
                &["futurev1"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
            tycho_indexer::extractor::family_registry::shared_family_member_with_bootstrap(
                "future_v2",
                &["futurev2"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                future_materialize_branch,
            ),
        ];
        const FUTURE_FAMILY: FamilyRuntimeSpec =
            canonical_shared_family_runtime_spec!(
                "future_swap",
                FUTURE_FAMILY_MEMBERS,
                None,
                shared_progress_owner_protocol_system: "future_v1",
                durability_scope: "family::future_swap_runtime",
            );

        let registry = test_registry_with_future_family(FUTURE_FAMILY);
        let temp_root = write_future_family_yaml_fixture_for_tests();
        let config = ExtractorConfigs::from_yaml_with_registry(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
            registry,
        )
        .expect("load future family config through yaml entrypoint");

        config
            .family_runtime_registry()
            .require_family_spec("future_swap", "extractor config registry")
            .expect("config should preserve the custom family registry it was loaded with");

        let runtime_targets = config
            .resolved_runtime_targets()
            .expect("default helper should use preserved custom registry");
        let runtime_plan = config
            .resolved_indexer_runtime_plan()
            .expect("runtime plan helper should use preserved custom registry");

        assert_eq!(
            runtime_targets.protocol_systems(),
            &["future_v1".to_string(), "future_v2".to_string()],
        );
        assert_eq!(
            runtime_plan.protocol_systems(),
            &["future_v1".to_string(), "future_v2".to_string()],
        );
        config
            .family_runtime_registry()
            .require_family_spec("future_swap", "resolved runtime plan helper")
            .expect("config helper should keep the custom registry");

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn repo_uniswap_v3_combined_substream_config_keeps_v3_filters_and_rejects_legacy_default_entrypoint() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let default_config_err = test_extractor_configs_from_yaml(
            root.join("extractors.yaml")
                .to_str()
                .expect("utf8 default config path"),
        )
        .expect_err("default repo config should reject standalone uniswap family members");
        let combined_substream_config = test_extractor_configs_from_yaml(
            root.join("extractors.uniswap_v2_v3.combined.yaml")
                .to_str()
                .expect("utf8 combined-substream config path"),
        )
        .expect("load combined-substream extractors config");

        let combined_substream_v3 = combined_substream_config
            .extractors
            .get("uniswap_v3")
            .expect("combined-substream v3 extractor present");

        assert!(default_config_err
            .to_string()
            .contains("belongs to registered family runtime `uniswap`"));
        let combined_substream_v3_bootstrap = combined_substream_v3
            .bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.params.clone())
            .expect("combined-substream v3 bootstrap params present");
        assert!(
            combined_substream_v3_bootstrap.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "combined-substream v3 bootstrap should keep v3 pools"
        );
        assert!(
            !combined_substream_v3_bootstrap.contains("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"),
            "combined-substream v3 bootstrap should exclude v2 pools"
        );
        let combined_v3_events_params = combined_substream_v3
            .substreams_params
            .get("v3_map_events")
            .expect("combined-substream v3 map_events params present");
        assert!(
            combined_v3_events_params
                .contains("factory=0x1F98431c8aD98523631AE4a59f267346ea31F984"),
            "combined-substream v3 map_events params should preserve the factory filter"
        );
        assert!(
            combined_v3_events_params.contains("&pools="),
            "combined-substream v3 map_events params should add an explicit pool allowlist"
        );

        let combined_substream_yaml =
            fs::read_to_string(root.join("extractors.uniswap_v2_v3.combined.yaml"))
                .expect("read combined-substream config");
        let combined_v3_fragment = fs::read_to_string(
            root.join("extractors.fragments/uniswap_v3_combined_protocol_changes.yaml"),
        )
        .expect("read combined-substream v3 fragment");
        let combined_v3_substreams =
            fs::read_to_string(root.join("config/uniswap_v3_substreams.yaml"))
                .expect("read combined-substream v3 substreams config");

        assert!(
            combined_substream_yaml
                .contains("extractors.fragments/uniswap_v3_combined_protocol_changes.yaml"),
            "combined-substream config should include the v3 combined fragment"
        );
        assert!(
            combined_substream_yaml.contains("v3_map_events: \"@config/uniswap_v3_substreams.yaml\""),
            "combined-substream config should centralize v3_map_events params in the shared family defaults"
        );
        assert!(
            combined_v3_fragment.contains("module_name: \"v3_map_protocol_changes\""),
            "combined-substream v3 fragment should point at the combined package module"
        );
        assert!(
            combined_substream_yaml.contains("shared_spkg: \"protocols/substreams/ethereum-uniswap-v2-v3-combined/"),
            "combined-substream config should centralize the combined package path in the shared family defaults"
        );
        assert!(
            combined_v3_substreams.contains("shared_uniswap_substreams.yaml"),
            "combined-substream v3 substreams config should derive its pool filter from the shared substreams config"
        );
    }
}
