use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tycho_common::{
    models::{Address, Chain, ProtocolType},
    traits::AccountExtractor,
};
use tycho_ethereum::{
    rpc::EthereumRpcClient,
    services::{
        account_extractor::EVMAccountExtractor, entrypoint_tracer::tracer::EVMEntrypointService,
        token_pre_processor::EthereumTokenPreProcessor,
    },
};
use tycho_storage::postgres::cache::CachedGateway;

use crate::extractor::{
    chain_state::ChainState,
    dynamic_contract_indexer::{
        dci::DynamicContractIndexer,
        hooks::{hook_dci::UniswapV4HookDCI, hooks_dci_builder::UniswapV4HookDCIBuilder},
    },
    extractor_config::ExtractorConfig,
    family_registry::FamilyRuntimeRegistry,
    post_processors::POST_PROCESSOR_REGISTRY,
    protocol_cache::ProtocolMemoryCache,
    protocol_extractor::{ExtractorPgGateway, ProtocolExtractor},
    protocol_message_registry::{
        AuxiliaryProtocolMessageDecoder, AuxiliaryProtocolStateHydrator,
    },
    ExtractionError, Extractor, ExtractorExtension,
};

use super::extractor_config::DCIType;

/// Enum to handle both standard DCI and UniswapV4 Hook DCI
#[allow(clippy::large_enum_variant)]
pub(crate) enum DCIPlugin<AE: AccountExtractor + Send + Sync> {
    Standard(DynamicContractIndexer<AE, EVMEntrypointService, CachedGateway>),
    UniswapV4Hooks(Box<UniswapV4HookDCI<AE, EVMEntrypointService, CachedGateway>>),
}

#[async_trait]
impl<AE: AccountExtractor + Send + Sync> ExtractorExtension for DCIPlugin<AE> {
    async fn process_block_update(
        &mut self,
        block_changes: &mut crate::extractor::models::BlockChanges,
    ) -> Result<(), ExtractionError> {
        match self {
            DCIPlugin::Standard(dci) => {
                dci.process_block_update(block_changes)
                    .await
            }
            DCIPlugin::UniswapV4Hooks(hooks_dci) => {
                hooks_dci
                    .process_block_update(block_changes)
                    .await
            }
        }
    }

    async fn process_revert(
        &mut self,
        target_block: &tycho_common::models::BlockHash,
    ) -> Result<(), ExtractionError> {
        match self {
            DCIPlugin::Standard(dci) => dci.process_revert(target_block).await,
            DCIPlugin::UniswapV4Hooks(hooks_dci) => {
                hooks_dci
                    .process_revert(target_block)
                    .await
            }
        }
    }

    fn cache_size(&self) -> usize {
        match self {
            DCIPlugin::Standard(dci) => dci.cache_size(),
            DCIPlugin::UniswapV4Hooks(hooks_dci) => hooks_dci.cache_size(),
        }
    }

    fn emit_cache_metrics(&self, chain: &str, extractor: &str) {
        match self {
            DCIPlugin::Standard(dci) => dci.emit_cache_metrics(chain, extractor),
            DCIPlugin::UniswapV4Hooks(hooks_dci) => hooks_dci.emit_cache_metrics(chain, extractor),
        }
    }
}

pub(crate) struct ExtractorBuilder {
    config: ExtractorConfig,
    extractor: Option<Arc<dyn Extractor>>,
    rpc_client: Option<EthereumRpcClient>,
    database_insert_batch_size: Option<usize>,
    auxiliary_protocol_message_decoders: Vec<AuxiliaryProtocolMessageDecoder>,
    auxiliary_protocol_state_hydrators: Vec<AuxiliaryProtocolStateHydrator>,
    family_runtime_registry: Option<FamilyRuntimeRegistry<'static>>,
    partial_blocks: bool,
}

impl ExtractorBuilder {
    pub fn new(
        config: &ExtractorConfig,
        _endpoint_url: &str,
        _s3_bucket: Option<&str>,
        _substreams_api_token: &str,
    ) -> Self {
        Self {
            config: config.clone(),
            extractor: None,
            rpc_client: None,
            database_insert_batch_size: None,
            auxiliary_protocol_message_decoders: Vec::new(),
            auxiliary_protocol_state_hydrators: Vec::new(),
            family_runtime_registry: None,
            partial_blocks: false,
        }
    }

    pub fn partial_blocks(mut self, val: bool) -> Self {
        self.partial_blocks = val;
        self
    }

    pub fn database_insert_batch_size(mut self, database_insert_batch_size: usize) -> Self {
        self.database_insert_batch_size = Some(database_insert_batch_size);
        self
    }

    pub(crate) fn auxiliary_protocol_message_decoders(
        mut self,
        decoders: Vec<AuxiliaryProtocolMessageDecoder>,
    ) -> Self {
        self.auxiliary_protocol_message_decoders = decoders;
        self
    }

    pub(crate) fn auxiliary_protocol_state_hydrators(
        mut self,
        hydrators: Vec<AuxiliaryProtocolStateHydrator>,
    ) -> Self {
        self.auxiliary_protocol_state_hydrators = hydrators;
        self
    }

    pub(crate) fn family_runtime_registry(
        mut self,
        registry: FamilyRuntimeRegistry<'static>,
    ) -> Self {
        self.family_runtime_registry = Some(registry);
        self
    }

    pub(crate) fn initialized_extractor(&self) -> Arc<dyn Extractor> {
        self.extractor
            .clone()
            .expect("extractor initialized in build()")
    }

    async fn create_rpc_dci(
        rpc_client: &EthereumRpcClient,
        chain: Chain,
        extractor_name: String,
        cached_gw: &CachedGateway,
    ) -> Result<
        DynamicContractIndexer<EVMAccountExtractor, EVMEntrypointService, CachedGateway>,
        ExtractionError,
    > {
        let account_extractor = EVMAccountExtractor::new(rpc_client, chain);

        let tracer_rpc_client = if let Ok(tracer_rpc_url) = std::env::var("TRACE_RPC_URL") {
            EthereumRpcClient::new(&tracer_rpc_url).map_err(|err| {
                ExtractionError::Setup(format!(
                    "Failed to create RPC client for {tracer_rpc_url}: {err}"
                ))
            })?
        } else {
            rpc_client.clone()
        };

        let max_retries = std::env::var("TRACE_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let retry_delay_ms = std::env::var("TRACE_RETRY_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);

        let tracer =
            EVMEntrypointService::new_with_config(&tracer_rpc_client, max_retries, retry_delay_ms);

        let mut rpc_dci = DynamicContractIndexer::new(
            chain,
            extractor_name,
            cached_gw.clone(),
            account_extractor,
            tracer,
        );
        rpc_dci.initialize().await?;

        Ok(rpc_dci)
    }

    pub async fn build(
        mut self,
        chain_state: ChainState,
        cached_gw: &CachedGateway,
        token_pre_processor: &EthereumTokenPreProcessor,
        protocol_cache: &ProtocolMemoryCache,
        rpc_client: &EthereumRpcClient,
    ) -> Result<Self, ExtractionError> {
        self.rpc_client = Some(rpc_client.clone());

        let protocol_types = self
            .config
            .protocol_types
            .iter()
            .map(|pt| {
                (
                    pt.name.clone(),
                    ProtocolType::new(
                        pt.name.clone(),
                        pt.financial_type.clone(),
                        None,
                        self.config.implementation_type.clone(),
                    ),
                )
            })
            .collect();

        let family_durability_scope = self
            .config
            .resolve_family_runtime_metadata(self.family_runtime_registry)?
            .map(|metadata| metadata.durability_scope.to_string());

        let gw = ExtractorPgGateway::new(
            &self.config.name,
            self.config.chain,
            self.config.sync_batch_size,
            cached_gw.clone(),
            family_durability_scope,
        );

        let post_processor = self
            .config
            .post_processor
            .as_ref()
            .map(|name| {
                POST_PROCESSOR_REGISTRY
                    .get(name)
                    .cloned()
                    .ok_or_else(|| {
                        ExtractionError::Setup(format!(
                            "Post processor '{name}' not found in registry"
                        ))
                    })
            })
            .transpose()?;

        let dci_plugin = if let Some(ref dci_type) = self.config.dci_plugin {
            Some(match dci_type {
                DCIType::RPC => {
                    let rpc_dci = Self::create_rpc_dci(
                        rpc_client,
                        self.config.chain,
                        self.config.name.clone(),
                        cached_gw,
                    )
                    .await?;
                    DCIPlugin::Standard(rpc_dci)
                }
                DCIType::UniswapV4Hooks { pool_manager_address } => {
                    let router_address =
                        Address::from("0x2e234DAe75C793f67A35089C9d99245E1C58470b");
                    let pool_manager = Address::from(pool_manager_address.as_str());
                    let base_dci = Self::create_rpc_dci(
                        rpc_client,
                        self.config.chain,
                        self.config.name.clone(),
                        cached_gw,
                    )
                    .await?;

                    let mut hooks_dci = UniswapV4HookDCIBuilder::new(
                        base_dci,
                        rpc_client,
                        router_address,
                        pool_manager,
                        cached_gw.clone(),
                        self.config.chain,
                    )
                    .pause_after_retries(3)
                    .max_retries(5)
                    .build()?;

                    hooks_dci.initialize().await?;
                    DCIPlugin::UniswapV4Hooks(Box::new(hooks_dci))
                }
            })
        } else {
            None
        };

        let extractor = ProtocolExtractor::<
            ExtractorPgGateway,
            EthereumTokenPreProcessor,
            DCIPlugin<_>,
        >::new_with_runtime_support(
                gw,
                self.database_insert_batch_size
                    .unwrap_or_default(),
                &self.config.name,
                self.config.chain,
                chain_state,
                self.config
                    .protocol_system()
                    .to_string(),
                protocol_cache.clone(),
                protocol_types,
                self.auxiliary_protocol_message_decoders
                    .clone(),
                self.auxiliary_protocol_state_hydrators
                    .clone(),
                token_pre_processor.clone(),
                post_processor,
                dci_plugin,
                self.rpc_client.clone(),
            )
            .await?;

        self.extractor = Some(Arc::new(extractor));
        Ok(self)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ManagedExtractorBuildContext<'a> {
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
    pub(crate) family_runtime_registry: FamilyRuntimeRegistry<'static>,
}

impl ManagedExtractorBuildContext<'_> {
    pub(crate) async fn build_initialized_extractor(
        &self,
        extractor_config: &ExtractorConfig,
        auxiliary_protocol_message_decoders: Vec<AuxiliaryProtocolMessageDecoder>,
        auxiliary_protocol_state_hydrators: Vec<AuxiliaryProtocolStateHydrator>,
    ) -> Result<Arc<dyn Extractor>, ExtractionError> {
        let builder = ExtractorBuilder::new(
            extractor_config,
            self.endpoint_url,
            self.s3_bucket,
            self.substreams_api_token,
        )
        .database_insert_batch_size(self.database_insert_batch_size)
        .auxiliary_protocol_message_decoders(auxiliary_protocol_message_decoders)
        .auxiliary_protocol_state_hydrators(auxiliary_protocol_state_hydrators)
        .family_runtime_registry(self.family_runtime_registry)
        .partial_blocks(self.partial_blocks);

        let builder = builder
            .build(
                self.chain_state,
                self.cached_gw,
                self.token_pre_processor,
                self.protocol_cache,
                self.rpc_client,
            )
            .await?;

        Ok(builder.initialized_extractor())
    }

    pub(crate) async fn build_protocol_system_keyed_extractors(
        &self,
        extractor_configs: &[&ExtractorConfig],
        auxiliary_protocol_message_decoders_by_protocol_system: &HashMap<
            String,
            Vec<AuxiliaryProtocolMessageDecoder>,
        >,
        auxiliary_protocol_state_hydrators_by_protocol_system: &HashMap<
            String,
            Vec<AuxiliaryProtocolStateHydrator>,
        >,
    ) -> Result<HashMap<String, Arc<dyn Extractor>>, ExtractionError> {
        let mut extractors = HashMap::with_capacity(extractor_configs.len());

        for extractor_config in extractor_configs {
            let protocol_system = extractor_config
                .protocol_system()
                .to_string();
            let extractor = self
                .build_initialized_extractor(
                    extractor_config,
                    auxiliary_protocol_message_decoders_by_protocol_system
                        .get(extractor_config.protocol_system())
                        .cloned()
                        .unwrap_or_default(),
                    auxiliary_protocol_state_hydrators_by_protocol_system
                        .get(extractor_config.protocol_system())
                        .cloned()
                        .unwrap_or_default(),
                )
                .await?;

            if extractors
                .insert(protocol_system.clone(), extractor)
                .is_some()
            {
                return Err(ExtractionError::Setup(format!(
                    "duplicate protocol_system `{protocol_system}` while building managed extractors"
                )));
            }
        }

        Ok(extractors)
    }
}
