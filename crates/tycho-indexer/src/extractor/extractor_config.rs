use std::collections::HashMap;

use serde::Deserialize;
use tycho_common::{
    models::{Chain, FinancialType, ImplementationType},
    Bytes,
};

use crate::extractor::{family_runtime_metadata::FamilyRuntimeConfig, ExtractionError};

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
pub struct ProtocolTypeConfig {
    pub(crate) name: String,
    pub(crate) financial_type: FinancialType,
}

impl ProtocolTypeConfig {
    pub fn new(name: String, financial_type: FinancialType) -> Self {
        Self { name, financial_type }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn financial_type(&self) -> FinancialType {
        self.financial_type.clone()
    }
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapStrategy {
    #[default]
    UniswapV3Rpc,
    UniswapV2Rpc,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct BootstrapConfig {
    pub strategy: BootstrapStrategy,
    pub start_block: i64,
    pub params: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ExtractorConfig {
    pub(crate) name: String,
    pub(crate) protocol_system: String,
    pub(crate) chain: Chain,
    pub(crate) implementation_type: ImplementationType,
    pub(crate) sync_batch_size: usize,
    pub(crate) start_block: i64,
    pub(crate) stop_block: Option<i64>,
    pub(crate) protocol_types: Vec<ProtocolTypeConfig>,
    pub(crate) spkg: String,
    pub(crate) module_name: String,
    #[serde(default)]
    pub initialized_accounts: Vec<Bytes>,
    #[serde(default)]
    pub initialized_accounts_block: u64,
    #[serde(default)]
    pub post_processor: Option<String>,
    #[serde(default)]
    pub dci_plugin: Option<DCIType>,
    #[serde(default)]
    pub substreams_params: HashMap<String, String>,
    #[serde(default)]
    pub bootstrap: Option<BootstrapConfig>,
    #[serde(default)]
    pub family_runtime: Option<FamilyRuntimeConfig>,
}

impl ExtractorConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        chain: Chain,
        implementation_type: ImplementationType,
        sync_batch_size: usize,
        start_block: i64,
        stop_block: Option<i64>,
        protocol_types: Vec<ProtocolTypeConfig>,
        spkg: String,
        module_name: String,
        initialized_accounts: Vec<Bytes>,
        initialized_accounts_block: u64,
        post_processor: Option<String>,
        dci_plugin: Option<DCIType>,
        substreams_params: HashMap<String, String>,
        bootstrap: Option<BootstrapConfig>,
    ) -> Self {
        Self {
            protocol_system: name.clone(),
            name,
            chain,
            implementation_type,
            sync_batch_size,
            start_block,
            stop_block,
            protocol_types,
            spkg,
            module_name,
            initialized_accounts,
            initialized_accounts_block,
            post_processor,
            dci_plugin,
            substreams_params,
            bootstrap,
            family_runtime: None,
        }
    }

    pub fn start_block(&self) -> i64 {
        self.start_block
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn protocol_system(&self) -> &str {
        &self.protocol_system
    }

    pub fn chain(&self) -> Chain {
        self.chain
    }

    pub fn stop_block(&self) -> Option<i64> {
        self.stop_block
    }

    pub fn protocol_types(&self) -> &[ProtocolTypeConfig] {
        &self.protocol_types
    }

    pub fn spkg(&self) -> &str {
        &self.spkg
    }

    pub fn module_name(&self) -> &str {
        &self.module_name
    }

    pub fn implementation_type(&self) -> &ImplementationType {
        &self.implementation_type
    }

    pub fn sync_batch_size(&self) -> usize {
        self.sync_batch_size
    }

    pub fn with_protocol_system(mut self, protocol_system: impl Into<String>) -> Self {
        self.protocol_system = protocol_system.into();
        self
    }
}

pub fn configured_stream_start_block(config: &ExtractorConfig) -> Result<i64, ExtractionError> {
    if config.bootstrap.is_some() {
        config
            .start_block
            .checked_add(1)
            .ok_or_else(|| ExtractionError::Setup("stream start block overflow".to_string()))
    } else {
        Ok(config.start_block)
    }
}

pub fn extractor_config_by_protocol_system<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    protocol_system: &str,
) -> Result<Option<&'a ExtractorConfig>, ExtractionError> {
    let mut matches = extractors
        .values()
        .filter(|config| config.protocol_system() == protocol_system);
    let first = matches.next();
    if matches.next().is_some() {
        return Err(ExtractionError::Setup(format!(
            "multiple extractor configs declare protocol_system `{protocol_system}`"
        )));
    }
    Ok(first)
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DCIType {
    #[serde(rename = "rpc")]
    RPC,
    UniswapV4Hooks {
        pool_manager_address: String,
    },
}
