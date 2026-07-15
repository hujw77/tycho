use std::collections::{BTreeSet, HashMap};

use serde::Serialize;
use tycho_common::models::Address;
use tycho_common::models::{Chain, ExtractorIdentity};

use crate::extractor::{
    extractor_config::configured_stream_start_block, extractor_config::ExtractorConfig,
    family_runtime_planning::ResolvedFamilyRuntime,
    managed_substreams_request::PreparedSubstreamsRequest, ExtractionError,
};

#[derive(Clone, Debug)]
pub struct ResolvedRuntimeTargets<'a> {
    pub(crate) targets: Vec<ResolvedRuntimeTarget<'a>>,
}

#[derive(Clone, Debug)]
pub struct ResolvedStandaloneRuntime<'a> {
    pub protocol_system: &'a str,
    pub extractor_config: &'a ExtractorConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedSubstreamsExecutionRequest {
    pub spkg: String,
    pub module: String,
    pub start_block: i64,
    pub stop_block: u64,
    pub params: HashMap<String, String>,
    pub extractor_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedInitializedAccountsRequest {
    pub chain: Chain,
    pub accounts: Vec<Address>,
    pub block_id: u64,
}

#[derive(Clone, Debug)]
pub enum ResolvedRuntimeTarget<'a> {
    Family(ResolvedFamilyRuntime<'a>),
    Standalone(ResolvedStandaloneRuntime<'a>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedRuntimeTargetSelector<'a> {
    Family(&'a str),
    StandaloneProtocolSystem(&'a str),
}

trait RuntimeTargetPlanningView<'a> {
    fn selector_label(&self) -> String;

    fn chain(&self) -> Chain;

    fn extractor_configs(&self) -> Vec<&'a ExtractorConfig>;

    fn protocol_systems(&self) -> Vec<&'a str>;

    fn substreams_execution_request(
        &self,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError>;

    fn substreams_execution_request_with_start_block(
        &self,
        start_block: i64,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError>;

    fn prepared_substreams_request_with_stream_position(
        &self,
        start_block: i64,
        cursor: Option<String>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        Ok(PreparedSubstreamsRequest {
            request: self.substreams_execution_request_with_start_block(start_block)?,
            cursor,
        })
    }

    fn initialized_accounts_requests(&self) -> Vec<ResolvedInitializedAccountsRequest> {
        initialized_accounts_requests_for_configs(self.chain(), self.extractor_configs())
    }
}

fn initialized_accounts_requests_for_configs(
    chain: Chain,
    extractor_configs: Vec<&ExtractorConfig>,
) -> Vec<ResolvedInitializedAccountsRequest> {
    let mut requests: Vec<ResolvedInitializedAccountsRequest> = Vec::new();

    for config in extractor_configs {
        if config.initialized_accounts.is_empty() {
            continue;
        }

        if let Some(existing) = requests.iter_mut().find(|request| {
            request.chain == chain && request.block_id == config.initialized_accounts_block
        }) {
            for account in &config.initialized_accounts {
                if !existing.accounts.contains(account) {
                    existing.accounts.push(account.clone());
                }
            }
            continue;
        }

        requests.push(ResolvedInitializedAccountsRequest {
            chain,
            accounts: config.initialized_accounts.clone(),
            block_id: config.initialized_accounts_block,
        });
    }

    requests
}

impl<'a> ResolvedFamilyRuntime<'a> {
    pub fn substreams_execution_request(
        &self,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        self.substreams_execution_request_with_start_block(self.configured_start_block())
    }

    pub fn substreams_execution_request_with_start_block(
        &self,
        start_block: i64,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        Ok(self.shared_stream_runtime.request.with_start_block(start_block))
    }

    pub fn prepared_substreams_request_with_stream_position(
        &self,
        start_block: i64,
        cursor: Option<String>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        <Self as RuntimeTargetPlanningView>::prepared_substreams_request_with_stream_position(
            self,
            start_block,
            cursor,
        )
    }
}

impl<'a> ResolvedStandaloneRuntime<'a> {
    pub fn substreams_execution_request(
        &self,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        let start_block = configured_stream_start_block(self.extractor_config)?;
        self.substreams_execution_request_with_start_block(start_block)
    }

    pub fn substreams_execution_request_with_start_block(
        &self,
        start_block: i64,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        Ok(ResolvedSubstreamsExecutionRequest {
            spkg: self.extractor_config.spkg().to_string(),
            module: self
                .extractor_config
                .module_name()
                .to_string(),
            start_block,
            stop_block: self
                .extractor_config
                .stop_block()
                .unwrap_or_default() as u64,
            params: self
                .extractor_config
                .substreams_params
                .clone(),
            extractor_id: ExtractorIdentity::new(
                self.extractor_config.chain(),
                self.extractor_config.name(),
            )
            .to_string(),
        })
    }

    pub fn prepared_substreams_request_with_stream_position(
        &self,
        start_block: i64,
        cursor: Option<String>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        <Self as RuntimeTargetPlanningView>::prepared_substreams_request_with_stream_position(
            self,
            start_block,
            cursor,
        )
    }
}

impl<'a> RuntimeTargetPlanningView<'a> for ResolvedFamilyRuntime<'a> {
    fn selector_label(&self) -> String {
        format!("family:{}", self.family.family_name)
    }

    fn chain(&self) -> Chain {
        self.family.chain
    }

    fn extractor_configs(&self) -> Vec<&'a ExtractorConfig> {
        self.extractor_configs.clone()
    }

    fn protocol_systems(&self) -> Vec<&'a str> {
        self.extractor_configs
            .iter()
            .map(|config| config.protocol_system())
            .collect()
    }

    fn substreams_execution_request(
        &self,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        ResolvedFamilyRuntime::substreams_execution_request(self)
    }

    fn substreams_execution_request_with_start_block(
        &self,
        start_block: i64,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        ResolvedFamilyRuntime::substreams_execution_request_with_start_block(self, start_block)
    }
}

impl<'a> RuntimeTargetPlanningView<'a> for ResolvedStandaloneRuntime<'a> {
    fn selector_label(&self) -> String {
        format!("protocol_system:{}", self.protocol_system)
    }

    fn chain(&self) -> Chain {
        self.extractor_config.chain()
    }

    fn extractor_configs(&self) -> Vec<&'a ExtractorConfig> {
        vec![self.extractor_config]
    }

    fn protocol_systems(&self) -> Vec<&'a str> {
        vec![self.protocol_system]
    }

    fn substreams_execution_request(
        &self,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        ResolvedStandaloneRuntime::substreams_execution_request(self)
    }

    fn substreams_execution_request_with_start_block(
        &self,
        start_block: i64,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        ResolvedStandaloneRuntime::substreams_execution_request_with_start_block(self, start_block)
    }
}

impl<'a> ResolvedRuntimeTarget<'a> {
    pub fn family(&self) -> Option<&ResolvedFamilyRuntime<'a>> {
        match self {
            Self::Family(family) => Some(family),
            Self::Standalone(_) => None,
        }
    }

    pub fn standalone(&self) -> Option<&ResolvedStandaloneRuntime<'a>> {
        match self {
            Self::Family(_) => None,
            Self::Standalone(standalone) => Some(standalone),
        }
    }

    fn planning_view(&self) -> &(dyn RuntimeTargetPlanningView<'a> + '_) {
        match self {
            Self::Family(family) => family,
            Self::Standalone(standalone) => standalone,
        }
    }

    pub fn selector_label(&self) -> String {
        self.planning_view().selector_label()
    }

    pub fn chain(&self) -> Chain {
        self.planning_view().chain()
    }

    pub fn prepared_substreams_request_with_stream_position(
        &self,
        start_block: i64,
        cursor: Option<String>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        self.planning_view()
            .prepared_substreams_request_with_stream_position(start_block, cursor)
    }

    pub fn extractor_configs(&self) -> Vec<&'a ExtractorConfig> {
        self.planning_view().extractor_configs()
    }

    pub fn protocol_systems(&self) -> Vec<&'a str> {
        self.planning_view().protocol_systems()
    }

    pub fn initialized_accounts_requests(&self) -> Vec<ResolvedInitializedAccountsRequest> {
        self.planning_view()
            .initialized_accounts_requests()
    }

    pub fn matches_selector(&self, selector: ResolvedRuntimeTargetSelector<'_>) -> bool {
        match (selector, self) {
            (ResolvedRuntimeTargetSelector::Family(family_name), Self::Family(family)) => {
                family.family.family_name == family_name
            }
            (
                ResolvedRuntimeTargetSelector::StandaloneProtocolSystem(protocol_system),
                Self::Standalone(standalone),
            ) => standalone.protocol_system == protocol_system,
            _ => false,
        }
    }

    pub fn substreams_execution_request(
        &self,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        self.planning_view()
            .substreams_execution_request()
    }

    pub fn substreams_execution_request_with_start_block(
        &self,
        start_block: i64,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        self.planning_view()
            .substreams_execution_request_with_start_block(start_block)
    }

    pub fn substreams_execution_request_with_overrides(
        &self,
        start_block: Option<i64>,
        stop_block: Option<i64>,
        params_overrides: &HashMap<String, String>,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        let mut request = match start_block {
            Some(start_block) => self.substreams_execution_request_with_start_block(start_block)?,
            None => self.substreams_execution_request()?,
        };

        if let Some(stop_block) = stop_block {
            request.stop_block = stop_block as u64;
        }

        for (key, value) in params_overrides {
            request
                .params
                .insert(key.clone(), value.clone());
        }

        Ok(request)
    }

    pub fn effective_substreams_start_block(
        &self,
        start_block: Option<i64>,
    ) -> Result<i64, ExtractionError> {
        Ok(match start_block {
            Some(start_block) => start_block,
            None => {
                self.substreams_execution_request()?
                    .start_block
            }
        })
    }
}

impl<'a> ResolvedRuntimeTargetSelector<'a> {
    pub fn not_found_error(&self, selector_context: &str) -> ExtractionError {
        match self {
            Self::Family(family_name) => ExtractionError::Setup(format!(
                "No family runtime `{family_name}` found in `{selector_context}`"
            )),
            Self::StandaloneProtocolSystem(protocol_system) => ExtractionError::Setup(format!(
                "No standalone protocol system `{protocol_system}` found in `{selector_context}`"
            )),
        }
    }
}

impl<'a> ResolvedRuntimeTargets<'a> {
    pub fn new(targets: Vec<ResolvedRuntimeTarget<'a>>) -> Self {
        Self { targets }
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub fn as_slice(&self) -> &[ResolvedRuntimeTarget<'a>] {
        &self.targets
    }

    pub fn iter(&self) -> impl Iterator<Item = &ResolvedRuntimeTarget<'a>> {
        self.targets.iter()
    }

    pub fn into_selected(
        self,
        selector: ResolvedRuntimeTargetSelector<'_>,
    ) -> Option<ResolvedRuntimeTarget<'a>> {
        self.targets
            .into_iter()
            .find(|target| target.matches_selector(selector))
    }

    pub fn into_inner(self) -> Vec<ResolvedRuntimeTarget<'a>> {
        self.targets
    }

    pub fn require_by_selector(
        &self,
        selector: ResolvedRuntimeTargetSelector<'_>,
        selector_context: &str,
    ) -> Result<&ResolvedRuntimeTarget<'a>, ExtractionError> {
        self.targets
            .iter()
            .find(|target| target.matches_selector(selector))
            .ok_or_else(|| selector.not_found_error(selector_context))
    }

    pub fn resolve_target(
        &self,
        selector: Option<ResolvedRuntimeTargetSelector<'_>>,
        unique_context: &str,
        selector_context: &str,
    ) -> Result<&ResolvedRuntimeTarget<'a>, ExtractionError> {
        match selector {
            Some(selector) => self.require_by_selector(selector, selector_context),
            None => self.require_unique(unique_context),
        }
    }

    pub fn require_unique(
        &self,
        context: &str,
    ) -> Result<&ResolvedRuntimeTarget<'a>, ExtractionError> {
        if let Some(target) = self
            .targets
            .first()
            .filter(|_| self.targets.len() == 1)
        {
            return Ok(target);
        }

        let available = self
            .targets
            .iter()
            .map(ResolvedRuntimeTarget::selector_label)
            .collect::<Vec<_>>();

        Err(ExtractionError::Setup(format!(
            "{context}; available targets: {}",
            available.join(", ")
        )))
    }

    pub fn into_unique(self, context: &str) -> Result<ResolvedRuntimeTarget<'a>, ExtractionError> {
        if self.targets.len() == 1 {
            return Ok(self
                .targets
                .into_iter()
                .next()
                .expect("validated single target must exist"));
        }

        let available = self
            .targets
            .iter()
            .map(ResolvedRuntimeTarget::selector_label)
            .collect::<Vec<_>>();

        Err(ExtractionError::Setup(format!(
            "{context}; available targets: {}",
            available.join(", ")
        )))
    }

    pub fn coalesced_initialized_accounts_requests(
        &self,
    ) -> Vec<ResolvedInitializedAccountsRequest> {
        let mut requests: Vec<ResolvedInitializedAccountsRequest> = Vec::new();

        for target in &self.targets {
            for request in target.initialized_accounts_requests() {
                if let Some(existing) = requests.iter_mut().find(|existing| {
                    existing.chain == request.chain && existing.block_id == request.block_id
                }) {
                    for account in request.accounts {
                        if !existing.accounts.contains(&account) {
                            existing.accounts.push(account);
                        }
                    }
                    continue;
                }

                requests.push(request);
            }
        }

        requests
    }

    pub fn protocol_systems(&self) -> Vec<String> {
        self.targets
            .iter()
            .flat_map(ResolvedRuntimeTarget::extractor_configs)
            .map(|config| config.protocol_system().to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn dci_protocol_systems(&self) -> Vec<String> {
        self.targets
            .iter()
            .flat_map(ResolvedRuntimeTarget::extractor_configs)
            .filter(|config| config.dci_plugin.is_some())
            .map(|config| config.protocol_system().to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn resolve_substreams_execution_request(
        &self,
        selector: Option<ResolvedRuntimeTargetSelector<'_>>,
        unique_context: &str,
        selector_context: &str,
        start_block: Option<i64>,
        stop_block: Option<i64>,
        params_overrides: &HashMap<String, String>,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        self.resolve_target(selector, unique_context, selector_context)?
            .substreams_execution_request_with_overrides(start_block, stop_block, params_overrides)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};
    use tycho_common::Bytes;

    use crate::extractor::{
        extractor_config::{
            BootstrapConfig, BootstrapStrategy, ExtractorConfig, ProtocolTypeConfig,
        },
        family_registry::default_family_runtime_registry,
        family_runtime_metadata::{FamilyRuntimeConfig, ResolvedSharedFamilyStream},
        family_runtime_planning::build_resolved_runtime_targets,
    };

    use super::{
        ResolvedRuntimeTarget, ResolvedRuntimeTargetSelector, ResolvedRuntimeTargets,
        ResolvedStandaloneRuntime,
    };

    fn family_shared_stream(
        chain: Chain,
        family_name: &str,
        shared_spkg: &str,
    ) -> ResolvedSharedFamilyStream {
        default_family_runtime_registry()
            .resolved_shared_stream_for_family(chain, family_name, shared_spkg)
            .expect("registered shared stream")
    }

    fn uniswap_shared_stream(shared_spkg: &str) -> ResolvedSharedFamilyStream {
        family_shared_stream(Chain::Ethereum, "uniswap", shared_spkg)
    }

    fn with_resolved_family_runtime(
        config: ExtractorConfig,
        chain: Chain,
        family_name: &str,
        shared_spkg: &str,
    ) -> ExtractorConfig {
        let shared_stream = family_shared_stream(chain, family_name, shared_spkg);
        config.with_family_runtime(Some(FamilyRuntimeConfig {
            family: family_name.to_string(),
            shared_spkg: Some(shared_spkg.to_string()),
            shared_module: Some(shared_stream.module),
            durability_scope: Some(shared_stream.durability_scope),
        }))
    }

    fn make_config(name: &str, spkg: &str) -> ExtractorConfig {
        ExtractorConfig::new(
            name.to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            42,
            None,
            vec![ProtocolTypeConfig::new(format!("{name}_pool"), FinancialType::Swap)],
            spkg.to_string(),
            "map_protocol_changes".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::new(),
            None,
        )
    }

    fn with_resolved_uniswap_family_runtime(
        config: ExtractorConfig,
        shared_spkg: &str,
    ) -> ExtractorConfig {
        with_resolved_family_runtime(config, Chain::Ethereum, "uniswap", shared_spkg)
    }

    #[test]
    fn resolved_runtime_target_derives_family_substreams_execution_request() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        Some(120),
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::from([(
                            "map_pool_events".to_string(),
                            "factory=0x01".to_string(),
                        )]),
                        Some(BootstrapConfig {
                            strategy: BootstrapStrategy::UniswapV2Rpc,
                            start_block: 42,
                            params: "bootstrap_block=42&pools=0x01".to_string(),
                        }),
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        Some(120),
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
                        Some(BootstrapConfig {
                            strategy: BootstrapStrategy::UniswapV3Rpc,
                            start_block: 42,
                            params: "bootstrap_block=42&pools=0x02".to_string(),
                        }),
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");
        let family_target = targets
            .into_iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Family(_)))
            .expect("family target present");
        let expected_shared_stream =
            uniswap_shared_stream("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg");

        let execution = family_target
            .substreams_execution_request()
            .expect("family execution request derives");

        assert_eq!(
            execution.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(execution.module, expected_shared_stream.module);
        assert_eq!(execution.start_block, 43);
        assert_eq!(execution.stop_block, 120);
        assert_eq!(execution.extractor_id, expected_shared_stream.extractor_id);
        assert_eq!(
            execution.params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
    }

    #[test]
    fn resolved_runtime_target_derives_standalone_substreams_execution_request() {
        let curve = ExtractorConfig::new(
            "curve".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            100,
            Some(150),
            vec![ProtocolTypeConfig::new("curve_pool".to_string(), FinancialType::Swap)],
            "protocols/substreams/curve/curve.spkg".to_string(),
            "map_curve".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::from([("curve_events".to_string(), "factory=0x03".to_string())]),
            Some(BootstrapConfig {
                strategy: BootstrapStrategy::UniswapV2Rpc,
                start_block: 100,
                params: "bootstrap_block=100&pools=0x03".to_string(),
            }),
        );
        let target = ResolvedRuntimeTarget::Standalone(ResolvedStandaloneRuntime {
            protocol_system: "curve",
            extractor_config: &curve,
        });

        let execution = target
            .substreams_execution_request()
            .expect("standalone execution request derives");

        assert_eq!(execution.spkg, "protocols/substreams/curve/curve.spkg");
        assert_eq!(execution.module, "map_curve");
        assert_eq!(execution.start_block, 101);
        assert_eq!(execution.stop_block, 150);
        assert_eq!(execution.extractor_id, "ethereum:curve");
        assert_eq!(
            execution.params,
            HashMap::from([("curve_events".to_string(), "factory=0x03".to_string())])
        );
    }

    #[test]
    fn resolved_runtime_target_derives_family_substreams_execution_request_with_start_block() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        Some(120),
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::from([(
                            "map_pool_events".to_string(),
                            "factory=0x01".to_string(),
                        )]),
                        Some(BootstrapConfig {
                            strategy: BootstrapStrategy::UniswapV2Rpc,
                            start_block: 42,
                            params: "bootstrap_block=42&pools=0x01".to_string(),
                        }),
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        Some(120),
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
                        Some(BootstrapConfig {
                            strategy: BootstrapStrategy::UniswapV3Rpc,
                            start_block: 42,
                            params: "bootstrap_block=42&pools=0x02".to_string(),
                        }),
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");
        let family_target = targets
            .into_iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Family(_)))
            .expect("family target present");
        let expected_shared_stream =
            uniswap_shared_stream("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg");

        let execution = family_target
            .substreams_execution_request_with_start_block(88)
            .expect("family execution request derives with explicit start block");

        assert_eq!(
            execution.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(execution.module, expected_shared_stream.module);
        assert_eq!(execution.start_block, 88);
        assert_eq!(execution.stop_block, 120);
        assert_eq!(execution.extractor_id, expected_shared_stream.extractor_id);
        assert_eq!(
            execution.params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
    }

    #[test]
    fn resolved_runtime_target_derives_family_prepared_substreams_request_with_cursor() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");
        let family_target = targets
            .into_iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Family(_)))
            .expect("family target present");

        let prepared_request = family_target
            .prepared_substreams_request_with_stream_position(
                88,
                Some("cursor:shared-family".to_string()),
            )
            .expect("family prepared request derives with cursor");

        assert_eq!(
            prepared_request.request.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(prepared_request.request.start_block, 88);
        assert_eq!(prepared_request.request.extractor_id, "ethereum:uniswap_family");
        assert_eq!(prepared_request.cursor, Some("cursor:shared-family".to_string()));
    }

    #[test]
    fn resolved_runtime_target_derives_standalone_prepared_substreams_request_with_cursor() {
        let curve = ExtractorConfig::new(
            "curve".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            100,
            Some(150),
            vec![ProtocolTypeConfig::new("curve_pool".to_string(), FinancialType::Swap)],
            "protocols/substreams/curve/curve.spkg".to_string(),
            "map_curve".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::from([("curve_events".to_string(), "factory=0x03".to_string())]),
            Some(BootstrapConfig {
                strategy: BootstrapStrategy::UniswapV2Rpc,
                start_block: 100,
                params: "bootstrap_block=100&pools=0x03".to_string(),
            }),
        );
        let target = ResolvedRuntimeTarget::Standalone(ResolvedStandaloneRuntime {
            protocol_system: "curve",
            extractor_config: &curve,
        });

        let prepared_request = target
            .prepared_substreams_request_with_stream_position(111, Some("cursor:curve".to_string()))
            .expect("standalone prepared request derives with cursor");

        assert_eq!(prepared_request.request.spkg, "protocols/substreams/curve/curve.spkg");
        assert_eq!(prepared_request.request.module, "map_curve");
        assert_eq!(prepared_request.request.start_block, 111);
        assert_eq!(prepared_request.request.extractor_id, "ethereum:curve");
        assert_eq!(prepared_request.cursor, Some("cursor:curve".to_string()));
    }

    #[test]
    fn resolved_runtime_target_derives_family_substreams_execution_request_with_overrides() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");
        let family_target = targets
            .into_iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Family(_)))
            .expect("family target present");
        let override_params = HashMap::from([
            ("extra_flag".to_string(), "enabled".to_string()),
            ("map_pool_events".to_string(), "factory=0xoverride".to_string()),
        ]);

        let execution = family_target
            .substreams_execution_request_with_overrides(Some(88), Some(99), &override_params)
            .expect("family execution request derives with overrides");

        assert_eq!(
            execution.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(execution.start_block, 88);
        assert_eq!(execution.stop_block, 99);
        assert_eq!(execution.extractor_id, "ethereum:uniswap_family");
        assert_eq!(
            execution.params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0xoverride".to_string()),
                ("extra_flag".to_string(), "enabled".to_string()),
            ])
        );
    }

    #[test]
    fn select_resolved_runtime_target_selects_family_target_by_family_name() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let selected = targets
            .into_selected(ResolvedRuntimeTargetSelector::Family("uniswap"))
            .expect("family target selected");

        match selected {
            ResolvedRuntimeTarget::Family(family) => {
                assert_eq!(family.family.family_name, "uniswap");
                assert_eq!(family.extractor_configs.len(), 2);
            }
            ResolvedRuntimeTarget::Standalone(_) => panic!("expected family target"),
        }
    }

    #[test]
    fn select_resolved_runtime_target_selects_standalone_target_by_protocol_system() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let selected = targets
            .into_selected(ResolvedRuntimeTargetSelector::StandaloneProtocolSystem("curve"))
            .expect("standalone target selected");

        match selected {
            ResolvedRuntimeTarget::Standalone(standalone) => {
                assert_eq!(standalone.protocol_system, "curve");
                assert_eq!(standalone.extractor_config.name(), "curve");
            }
            ResolvedRuntimeTarget::Family(_) => panic!("expected standalone target"),
        }
    }

    #[test]
    fn require_resolved_runtime_target_by_selector_selects_family_target_by_family_name() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let selected = targets
            .require_by_selector(
                ResolvedRuntimeTargetSelector::Family("uniswap"),
                "test-runtime-targets",
            )
            .expect("family target selected");

        match selected {
            ResolvedRuntimeTarget::Family(family) => {
                assert_eq!(family.family.family_name, "uniswap");
                assert_eq!(family.extractor_configs.len(), 2);
            }
            ResolvedRuntimeTarget::Standalone(_) => panic!("expected family target"),
        }
    }

    #[test]
    fn require_resolved_runtime_target_by_selector_returns_shared_not_found_error() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let err = targets
            .require_by_selector(
                ResolvedRuntimeTargetSelector::StandaloneProtocolSystem("curve"),
                "test-runtime-targets",
            )
            .expect_err("missing standalone target should return selector error");

        assert!(
            err.to_string()
                .contains("No standalone protocol system `curve` found in `test-runtime-targets`"),
            "missing selector should reuse the shared selector not-found error surface"
        );
    }

    #[test]
    fn resolved_runtime_targets_resolve_target_uses_unique_or_selector_paths() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let family = targets
            .resolve_target(
                Some(ResolvedRuntimeTargetSelector::Family("uniswap")),
                "single target required",
                "test-runtime-targets",
            )
            .expect("family selector should resolve");
        assert!(matches!(family, ResolvedRuntimeTarget::Family(_)));

        let single_target = ResolvedRuntimeTargets::new(vec![(*family).clone()]);
        let unique = single_target
            .resolve_target(None, "single target required", "test-runtime-targets")
            .expect("single target should resolve without selector");
        assert!(matches!(unique, ResolvedRuntimeTarget::Family(_)));
    }

    #[test]
    fn resolved_runtime_targets_resolve_substreams_execution_request_applies_selector_and_overrides(
    ) {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let override_params = HashMap::from([("extra_flag".to_string(), "enabled".to_string())]);

        let request = targets
            .resolve_substreams_execution_request(
                Some(ResolvedRuntimeTargetSelector::Family("uniswap")),
                "single target required",
                "test-runtime-targets",
                Some(88),
                Some(99),
                &override_params,
            )
            .expect("family request should resolve");

        assert_eq!(request.module, "map_uniswap_family_protocol_changes");
        assert_eq!(request.start_block, 88);
        assert_eq!(request.stop_block, 99);
        assert_eq!(request.params.get("extra_flag"), Some(&"enabled".to_string()));
        assert_eq!(request.extractor_id, "ethereum:uniswap_family");
    }

    #[test]
    fn resolved_runtime_targets_wrapper_into_unique_returns_selected_target() {
        let extractors = HashMap::from([(
            "curve".to_string(),
            make_config("curve", "protocols/substreams/curve/curve.spkg"),
        )]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let selected = targets
            .into_unique("test-runtime-targets should contain exactly one target")
            .expect("single target should be selected");

        match selected {
            ResolvedRuntimeTarget::Standalone(standalone) => {
                assert_eq!(standalone.protocol_system, "curve");
            }
            ResolvedRuntimeTarget::Family(_) => panic!("expected standalone target"),
        }
    }

    #[test]
    fn resolved_runtime_targets_wrapper_into_unique_reuses_available_targets_error_surface() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let err = targets
            .into_unique("test-runtime-targets should contain exactly one target")
            .expect_err("multiple targets should fail unique selection");

        assert!(err
            .to_string()
            .contains("available targets:"));
        assert!(err
            .to_string()
            .contains("family:uniswap"));
        assert!(err
            .to_string()
            .contains("protocol_system:curve"));
    }

    #[test]
    fn resolved_runtime_target_derives_initialized_accounts_requests_for_family_members() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![Bytes::from([0x11; 20])],
                        101,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![Bytes::from([0x22; 20]), Bytes::from([0x33; 20])],
                        202,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");
        let family_target = targets
            .into_iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Family(_)))
            .expect("family target present");

        let requests = family_target.initialized_accounts_requests();

        assert_eq!(
            requests,
            vec![
                super::ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![Bytes::from([0x11; 20])],
                    block_id: 101,
                },
                super::ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![Bytes::from([0x22; 20]), Bytes::from([0x33; 20])],
                    block_id: 202,
                },
            ]
        );
    }

    #[test]
    fn resolved_runtime_target_coalesces_initialized_accounts_requests_by_block() {
        let shared_account = Bytes::from([0xaa; 20]);
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![shared_account.clone(), Bytes::from([0x11; 20])],
                        101,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        Chain::Ethereum,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            FinancialType::Swap,
                        )],
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                        "map_protocol_changes".to_string(),
                        vec![shared_account, Bytes::from([0x22; 20])],
                        101,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");
        let family_target = targets
            .into_iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Family(_)))
            .expect("family target present");

        let requests = family_target.initialized_accounts_requests();

        assert_eq!(
            requests,
            vec![super::ResolvedInitializedAccountsRequest {
                chain: Chain::Ethereum,
                accounts: vec![
                    Bytes::from([0xaa; 20]),
                    Bytes::from([0x11; 20]),
                    Bytes::from([0x22; 20]),
                ],
                block_id: 101,
            }]
        );
    }

    #[test]
    fn coalesced_initialized_accounts_requests_merge_across_runtime_targets() {
        let shared_account = Bytes::from([0xaa; 20]);
        let standalone_account = Bytes::from([0xbb; 20]);
        let later_block_account = Bytes::from([0xcc; 20]);

        let family_v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                None,
                vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![shared_account.clone(), Bytes::from([0x11; 20])],
                101,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let family_v3 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v3".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                None,
                vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![shared_account.clone(), Bytes::from([0x22; 20])],
                101,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let standalone_curve = ExtractorConfig::new(
            "curve".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            42,
            None,
            vec![ProtocolTypeConfig::new("curve_pool".to_string(), FinancialType::Swap)],
            "/tmp/curve.spkg".to_string(),
            "map_protocol_changes".to_string(),
            vec![shared_account, standalone_account],
            101,
            None,
            None,
            HashMap::new(),
            None,
        );
        let standalone_balancer = ExtractorConfig::new(
            "balancer".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            42,
            None,
            vec![ProtocolTypeConfig::new("balancer_pool".to_string(), FinancialType::Swap)],
            "/tmp/balancer.spkg".to_string(),
            "map_protocol_changes".to_string(),
            vec![later_block_account],
            202,
            None,
            None,
            HashMap::new(),
            None,
        );

        let extractors = HashMap::from([
            ("uniswap_v2".to_string(), family_v2),
            ("uniswap_v3".to_string(), family_v3),
            ("curve".to_string(), standalone_curve),
            ("balancer".to_string(), standalone_balancer),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let requests = targets.coalesced_initialized_accounts_requests();

        assert_eq!(
            requests,
            vec![
                super::ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![
                        Bytes::from([0xaa; 20]),
                        Bytes::from([0x11; 20]),
                        Bytes::from([0x22; 20]),
                        Bytes::from([0xbb; 20]),
                    ],
                    block_id: 101,
                },
                super::ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![Bytes::from([0xcc; 20])],
                    block_id: 202,
                },
            ]
        );
    }

    #[test]
    fn resolved_runtime_targets_wrapper_coalesces_initialized_accounts_requests() {
        let shared_account = Bytes::from([0xaa; 20]);
        let standalone_account = Bytes::from([0xbb; 20]);
        let later_block_account = Bytes::from([0xcc; 20]);

        let family_v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                None,
                vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![shared_account.clone(), Bytes::from([0x11; 20])],
                101,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let family_v3 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v3".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                None,
                vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![shared_account.clone(), Bytes::from([0x22; 20])],
                101,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let standalone_curve = ExtractorConfig::new(
            "curve".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            42,
            None,
            vec![ProtocolTypeConfig::new("curve_pool".to_string(), FinancialType::Swap)],
            "/tmp/curve.spkg".to_string(),
            "map_protocol_changes".to_string(),
            vec![shared_account, standalone_account],
            101,
            None,
            None,
            HashMap::new(),
            None,
        );
        let standalone_balancer = ExtractorConfig::new(
            "balancer".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            42,
            None,
            vec![ProtocolTypeConfig::new("balancer_pool".to_string(), FinancialType::Swap)],
            "/tmp/balancer.spkg".to_string(),
            "map_protocol_changes".to_string(),
            vec![later_block_account],
            202,
            None,
            None,
            HashMap::new(),
            None,
        );

        let extractors = HashMap::from([
            ("uniswap_v2".to_string(), family_v2),
            ("uniswap_v3".to_string(), family_v3),
            ("curve".to_string(), standalone_curve),
            ("balancer".to_string(), standalone_balancer),
        ]);

        let targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );
        let requests = targets.coalesced_initialized_accounts_requests();

        assert_eq!(
            requests,
            vec![
                super::ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![
                        Bytes::from([0xaa; 20]),
                        Bytes::from([0x11; 20]),
                        Bytes::from([0x22; 20]),
                        Bytes::from([0xbb; 20]),
                    ],
                    block_id: 101,
                },
                super::ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![Bytes::from([0xcc; 20])],
                    block_id: 202,
                },
            ]
        );
    }

    #[test]
    fn runtime_target_protocol_projections_cover_family_and_standalone_members() {
        let shared_stream =
            uniswap_shared_stream("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg");
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
                    Some(crate::extractor::extractor_config::DCIType::RPC),
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v2")
                .with_family_runtime(Some(FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: Some(
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                    ),
                    shared_module: Some(shared_stream.module.clone()),
                    durability_scope: Some(shared_stream.durability_scope.clone()),
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
                .with_family_runtime(Some(FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: Some(
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                            .to_string(),
                    ),
                    shared_module: Some(shared_stream.module.clone()),
                    durability_scope: Some(shared_stream.durability_scope.clone()),
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
                    Some(crate::extractor::extractor_config::DCIType::RPC),
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
        ]);

        let runtime_targets = ResolvedRuntimeTargets::new(
            build_resolved_runtime_targets(&extractors).expect("resolved targets"),
        );

        assert_eq!(
            runtime_targets.protocol_systems(),
            vec!["curve".to_string(), "uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(
            runtime_targets.dci_protocol_systems(),
            vec!["curve".to_string(), "uniswap_v2".to_string()]
        );
    }
}
