use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use serde::{Deserialize, Serialize};
use tycho_common::models::{Address, Chain, ExtractorIdentity};
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    family_dispatch::FamilyBranchSpec,
    models::BlockChanges,
    protocol_message_registry::AuxiliaryProtocolMessageDecoder,
    runner::{
        configured_stream_start_block, BootstrapConfig, BootstrapStrategy, ExtractorConfig,
    },
    shared_bootstrap::{
        materialize_plan_by_branch_runtimes, parse_and_validate_bootstrap_params,
        parse_pool_list_bootstrap_params, BootstrapBranchDescriptor, SharedBootstrapParams,
        SharedBootstrapPlan,
    },
    ExtractionError,
};

pub type ParseBootstrapParamsFn = fn(&str) -> Result<SharedBootstrapParams, ExtractionError>;
pub type MaterializeBootstrapBranchFn = for<'a> fn(
    &'a EthereumRpcClient,
    &'a BootstrapBranchDescriptor,
) -> Pin<
    Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>,
>;
#[derive(Clone, Copy, Debug)]
pub struct ResolvedSharedBootstrapBranchRuntime {
    pub protocol_system: &'static str,
    pub materialize_branch: MaterializeBootstrapBranchFn,
}

pub type MaterializeBootstrapPlanFn = for<'a> fn(
    &'a EthereumRpcClient,
    &'a SharedBootstrapPlan,
    &'a [ResolvedSharedBootstrapBranchRuntime],
) -> Pin<
    Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>,
>;

fn generic_shared_bootstrap_plan_materializer<'a>(
    rpc: &'a EthereumRpcClient,
    plan: &'a SharedBootstrapPlan,
    branch_runtimes: &'a [ResolvedSharedBootstrapBranchRuntime],
) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
    Box::pin(async move { materialize_plan_by_branch_runtimes(rpc, plan, branch_runtimes).await })
}

pub(crate) fn default_shared_bootstrap_plan_materializer() -> MaterializeBootstrapPlanFn {
    generic_shared_bootstrap_plan_materializer
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct FamilyRuntimeConfig {
    pub family: String,
    #[serde(default)]
    pub shared_spkg: Option<String>,
    #[serde(default)]
    pub shared_module: Option<String>,
    #[serde(default)]
    pub durability_scope: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedStreamTarget<'a> {
    pub spkg: &'a str,
    pub module: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedFamilyRuntimeMetadata<'a> {
    pub family: &'a str,
    pub shared_stream: SharedStreamTarget<'a>,
    pub durability_scope: &'a str,
}

impl FamilyRuntimeConfig {
    pub fn shared_spkg(&self) -> Option<&str> {
        self.shared_spkg.as_deref()
    }

    pub fn shared_module(&self) -> Option<&str> {
        self.shared_module.as_deref()
    }

    pub fn durability_scope(&self) -> Option<&str> {
        self.durability_scope.as_deref()
    }
}

impl ExtractorConfig {
    pub fn family_runtime(&self) -> Option<&FamilyRuntimeConfig> {
        self.family_runtime.as_ref()
    }

    pub fn require_resolved_family_runtime_metadata(
        &self,
    ) -> Result<Option<ResolvedFamilyRuntimeMetadata<'_>>, ExtractionError> {
        match self.family_runtime() {
            Some(runtime) => {
                let spkg = runtime.shared_spkg().ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "extractor `{}` uses family runtime `{}` without a resolved shared_spkg",
                        self.name(),
                        runtime.family
                    ))
                })?;
                let module = runtime.shared_module().ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "extractor `{}` uses family runtime `{}` without a resolved shared_module",
                        self.name(),
                        runtime.family
                    ))
                })?;
                let durability_scope = runtime.durability_scope().ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "extractor `{}` uses family runtime `{}` without a resolved durability_scope",
                        self.name(),
                        runtime.family
                    ))
                })?;
                Ok(Some(ResolvedFamilyRuntimeMetadata {
                    family: &runtime.family,
                    shared_stream: SharedStreamTarget { spkg, module },
                    durability_scope,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn with_family_runtime(mut self, family_runtime: Option<FamilyRuntimeConfig>) -> Self {
        self.family_runtime = family_runtime;
        self
    }
}

pub(crate) fn merge_substreams_params(
    merged: &mut HashMap<String, String>,
    incoming: &HashMap<String, String>,
    extractor_name: &str,
) -> Result<(), ExtractionError> {
    for (key, value) in incoming {
        if let Some(existing) = merged.get(key) {
            if existing != value {
                return Err(ExtractionError::Setup(format!(
                    "conflicting substreams param `{key}` while building family runner for `{extractor_name}`"
                )));
            }
        } else {
            merged.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}

pub(crate) fn merged_family_substreams_params(
    extractor_configs: &[&ExtractorConfig],
) -> Result<HashMap<String, String>, ExtractionError> {
    let mut merged_substreams_params = HashMap::new();

    for config in extractor_configs {
        merge_substreams_params(
            &mut merged_substreams_params,
            &config.substreams_params,
            config.name(),
        )?;
    }

    Ok(merged_substreams_params)
}

pub(crate) fn default_auxiliary_protocol_message_decoders_for_protocol_system(
    protocol_system: &str,
) -> Vec<AuxiliaryProtocolMessageDecoder> {
    default_family_runtime_registry()
        .auxiliary_protocol_message_decoders_for_protocol_system(protocol_system)
        .map(|decoders| decoders.to_vec())
        .unwrap_or_default()
}

#[derive(Clone, Copy, Debug)]
pub enum SharedBootstrapParamsParser {
    PoolList,
    Custom(ParseBootstrapParamsFn),
}

#[derive(Clone, Copy, Debug)]
pub struct SharedBootstrapMemberRuntime {
    pub strategy: BootstrapStrategy,
    pub params_parser: SharedBootstrapParamsParser,
    pub materialize_branch: MaterializeBootstrapBranchFn,
}

#[derive(Clone, Copy, Debug)]
pub struct SharedFamilyBootstrapRuntime {
    pub materialize_plan: MaterializeBootstrapPlanFn,
}

#[derive(Clone, Copy, Debug)]
pub struct FamilyMemberSpec {
    pub protocol_system: &'static str,
    pub shared_route_protocols: &'static [&'static str],
    pub shared_bootstrap: Option<SharedBootstrapMemberRuntime>,
}

#[derive(Clone, Debug)]
pub struct FamilyRuntimeSpec {
    pub family_name: &'static str,
    pub members: &'static [FamilyMemberSpec],
    pub output_module: &'static str,
    pub shared_stream_name: &'static str,
    pub durability_scope: &'static str,
    pub shared_bootstrap_runtime: Option<SharedFamilyBootstrapRuntime>,
    pub(crate) auxiliary_protocol_message_decoders: &'static [AuxiliaryProtocolMessageDecoder],
}

#[derive(Clone, Copy, Debug)]
pub struct FamilyRuntimeRegistry<'a> {
    specs: &'a [FamilyRuntimeSpec],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetectedFamilyRuntime {
    pub family_name: String,
    pub chain: Chain,
    pub member_protocol_systems: Vec<String>,
    pub shared_stream_name: String,
    shared_stream: ResolvedSharedFamilyStream,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedSharedFamilyStream {
    pub spkg: String,
    pub module: String,
    pub extractor_id: String,
    pub durability_scope: String,
}

pub struct FamilySharedStreamIdentity<'a> {
    pub output_module: &'a str,
    pub shared_stream_name: &'a str,
    pub extractor_id: String,
    pub durability_scope: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilySharedRuntimeMetadata<'a> {
    pub output_module: &'a str,
    pub shared_stream_name: &'a str,
    pub durability_scope: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyRuntimeBuildPlan {
    pub families: Vec<DetectedFamilyRuntime>,
    pub standalone_protocol_systems: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyRuntime<'a> {
    pub family: DetectedFamilyRuntime,
    pub extractor_configs: Vec<&'a ExtractorConfig>,
    pub execution: ResolvedFamilyExecutionConfig,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyExecutionConfig {
    pub branch_specs: Vec<FamilyBranchSpec>,
    pub shared_stream: ResolvedSharedFamilyStream,
    pub shared_bootstrap_execution: ResolvedSharedBootstrapExecution,
    pub(crate) auxiliary_protocol_message_decoders_by_protocol_system:
        HashMap<String, Vec<AuxiliaryProtocolMessageDecoder>>,
    pub merged_substreams_params: HashMap<String, String>,
    pub stop_block: u64,
    pub configured_start_block: i64,
    pub bootstrap_plan: Option<SharedBootstrapPlan>,
}

#[derive(Clone, Debug)]
pub struct ResolvedSharedBootstrapExecution {
    pub plan_materializer: MaterializeBootstrapPlanFn,
    pub branch_runtimes: Vec<ResolvedSharedBootstrapBranchRuntime>,
}

impl ResolvedSharedBootstrapExecution {
    pub async fn materialize_plan(
        &self,
        rpc: &EthereumRpcClient,
        plan: &SharedBootstrapPlan,
    ) -> Result<BlockChanges, ExtractionError> {
        (self.plan_materializer)(rpc, plan, &self.branch_runtimes).await
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyRuntimePlan<'a> {
    pub families: Vec<ResolvedFamilyRuntime<'a>>,
    pub standalone_extractors: Vec<ResolvedStandaloneRuntime<'a>>,
}

#[derive(Clone, Debug)]
pub struct ResolvedRuntimeTargets<'a> {
    targets: Vec<ResolvedRuntimeTarget<'a>>,
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
pub struct PreparedSubstreamsRequest {
    pub request: ResolvedSubstreamsExecutionRequest,
    pub cursor: Option<String>,
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

impl<'a> ResolvedRuntimeTarget<'a> {
    pub fn selector_label(&self) -> String {
        match self {
            Self::Family(family) => format!("family:{}", family.family.family_name),
            Self::Standalone(standalone) => {
                format!("protocol_system:{}", standalone.protocol_system)
            }
        }
    }

    pub fn chain(&self) -> Chain {
        match self {
            Self::Family(family) => family.family.chain,
            Self::Standalone(standalone) => standalone.extractor_config.chain(),
        }
    }

    pub fn extractor_configs(&self) -> Vec<&'a ExtractorConfig> {
        match self {
            Self::Family(family) => family.extractor_configs.clone(),
            Self::Standalone(standalone) => vec![standalone.extractor_config],
        }
    }

    pub fn protocol_systems(&self) -> Vec<&'a str> {
        match self {
            Self::Family(family) => family
                .extractor_configs
                .iter()
                .map(|config| config.protocol_system())
                .collect(),
            Self::Standalone(standalone) => vec![standalone.protocol_system],
        }
    }

    pub fn initialized_accounts_requests(&self) -> Vec<ResolvedInitializedAccountsRequest> {
        let chain = self.chain();
        let mut requests: Vec<ResolvedInitializedAccountsRequest> = Vec::new();

        for config in self.extractor_configs() {
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
        let start_block = match self {
            Self::Family(family) => family.execution.configured_start_block,
            Self::Standalone(standalone) => {
                configured_stream_start_block(standalone.extractor_config)?
            }
        };

        self.substreams_execution_request_with_start_block(start_block)
    }

    pub fn substreams_execution_request_with_start_block(
        &self,
        start_block: i64,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
        match self {
            Self::Family(family) => Ok(ResolvedSubstreamsExecutionRequest {
                spkg: family
                    .execution
                    .shared_stream
                    .spkg
                    .clone(),
                module: family
                    .execution
                    .shared_stream
                    .module
                    .clone(),
                start_block,
                stop_block: family.execution.stop_block,
                params: family
                    .execution
                    .merged_substreams_params
                    .clone(),
                extractor_id: family
                    .execution
                    .shared_stream
                    .extractor_id
                    .clone(),
            }),
            Self::Standalone(standalone) => Ok(ResolvedSubstreamsExecutionRequest {
                spkg: standalone
                    .extractor_config
                    .spkg()
                    .to_string(),
                module: standalone
                    .extractor_config
                    .module_name()
                    .to_string(),
                start_block,
                stop_block: standalone
                    .extractor_config
                    .stop_block()
                    .unwrap_or_default() as u64,
                params: standalone
                    .extractor_config
                    .substreams_params
                    .clone(),
                extractor_id: ExtractorIdentity::new(
                    standalone.extractor_config.chain(),
                    standalone.extractor_config.name(),
                )
                .to_string(),
            }),
        }
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
            request.params.insert(key.clone(), value.clone());
        }

        Ok(request)
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
            .substreams_execution_request_with_overrides(
                start_block,
                stop_block,
                params_overrides,
            )
    }
}

impl DetectedFamilyRuntime {
    pub fn stream_extractor_id(&self) -> String {
        self.shared_stream.extractor_id.clone()
    }

    pub fn resolved_shared_stream(&self) -> ResolvedSharedFamilyStream {
        self.shared_stream.clone()
    }

    pub fn durability_scope(&self) -> String {
        self.shared_stream
            .durability_scope
            .clone()
    }

    pub fn shared_spkg(&self) -> &str {
        &self.shared_stream.spkg
    }

    pub fn output_module(&self) -> &str {
        &self.shared_stream.module
    }
}

impl<'a> FamilyRuntimeRegistry<'a> {
    pub const fn new(specs: &'a [FamilyRuntimeSpec]) -> Self {
        Self { specs }
    }

    pub fn specs(&self) -> &'a [FamilyRuntimeSpec] {
        self.specs
    }

    pub fn family_spec_by_name(&self, family_name: &str) -> Option<&'a FamilyRuntimeSpec> {
        self.specs
            .iter()
            .find(|spec| spec.family_name == family_name)
    }

    pub fn shared_runtime_metadata_for_family(
        &self,
        family_name: &str,
    ) -> Option<FamilySharedRuntimeMetadata<'a>> {
        let spec = self.family_spec_by_name(family_name)?;
        Some(FamilySharedRuntimeMetadata {
            output_module: spec.output_module,
            shared_stream_name: spec.shared_stream_name,
            durability_scope: spec.durability_scope,
        })
    }

    pub fn shared_runtime_metadata_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<FamilySharedRuntimeMetadata<'a>> {
        self.family_name_for_protocol_system(protocol_system)
            .and_then(|family_name| self.shared_runtime_metadata_for_family(family_name))
    }

    pub fn output_module_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_family(family_name)
            .map(|metadata| metadata.output_module)
    }

    pub fn shared_stream_name_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_family(family_name)
            .map(|metadata| metadata.shared_stream_name)
    }

    pub fn member_protocol_systems_for_family(&self, family_name: &str) -> Option<Vec<&'a str>> {
        self.family_spec_by_name(family_name)
            .map(|spec| {
                spec.members
                    .iter()
                    .map(|member| member.protocol_system)
                    .collect()
            })
    }

    pub fn durability_scope_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_family(family_name)
            .map(|metadata| metadata.durability_scope)
    }

    pub fn require_shared_runtime_metadata_for_family(
        &self,
        family_name: &str,
        context: &str,
    ) -> Result<FamilySharedRuntimeMetadata<'a>, ExtractionError> {
        self.shared_runtime_metadata_for_family(family_name)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "{context} `{family_name}` does not match any registered family runtime"
                ))
            })
    }

    pub fn shared_stream_identity_for_family(
        &self,
        chain: Chain,
        family_name: &str,
    ) -> Option<FamilySharedStreamIdentity<'a>> {
        let metadata = self.shared_runtime_metadata_for_family(family_name)?;
        Some(FamilySharedStreamIdentity {
            output_module: metadata.output_module,
            shared_stream_name: metadata.shared_stream_name,
            extractor_id: format!("{chain}:{}", metadata.shared_stream_name),
            durability_scope: metadata.durability_scope,
        })
    }

    pub fn resolved_shared_stream_for_family(
        &self,
        chain: Chain,
        family_name: &str,
        shared_spkg: impl Into<String>,
    ) -> Result<ResolvedSharedFamilyStream, ExtractionError> {
        self.require_family_spec(family_name, "family runtime")?;
        let identity = self
            .shared_stream_identity_for_family(chain, family_name)
            .expect("required family spec must expose shared stream identity");
        Ok(ResolvedSharedFamilyStream {
            spkg: shared_spkg.into(),
            module: identity.output_module.to_string(),
            extractor_id: identity.extractor_id,
            durability_scope: identity.durability_scope.to_string(),
        })
    }

    pub fn detected_family_runtime(
        &self,
        family_name: &str,
        chain: Chain,
        shared_spkg: impl Into<String>,
    ) -> Result<DetectedFamilyRuntime, ExtractionError> {
        let spec = self.require_family_spec(family_name, "family runtime")?;
        let shared_metadata =
            self.require_shared_runtime_metadata_for_family(family_name, "family runtime")?;
        Ok(DetectedFamilyRuntime {
            family_name: spec.family_name.to_string(),
            chain,
            member_protocol_systems: spec
                .members
                .iter()
                .map(|member| member.protocol_system.to_string())
                .collect(),
            shared_stream_name: shared_metadata.shared_stream_name.to_string(),
            shared_stream: self.resolved_shared_stream_for_family(
                chain,
                family_name,
                shared_spkg,
            )?,
        })
    }

    pub fn output_module_for_protocol_system(&self, protocol_system: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_protocol_system(protocol_system)
            .map(|metadata| metadata.output_module)
    }

    pub fn require_family_spec(
        &self,
        family_name: &str,
        context: &str,
    ) -> Result<&'a FamilyRuntimeSpec, ExtractionError> {
        self.family_spec_by_name(family_name)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "{context} `{family_name}` does not match any registered family runtime"
                ))
            })
    }

    pub fn member_spec_by_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<&'a FamilyMemberSpec> {
        self.specs
            .iter()
            .flat_map(|spec| spec.members.iter())
            .find(|member| member.protocol_system == protocol_system)
    }

    pub fn member_spec_for_family(
        &self,
        family_name: &str,
        protocol_system: &str,
    ) -> Option<&'a FamilyMemberSpec> {
        self.family_spec_by_name(family_name)
            .into_iter()
            .flat_map(|spec| spec.members.iter())
            .find(|member| member.protocol_system == protocol_system)
    }

    pub fn require_member_spec_for_family(
        &self,
        family_name: &str,
        protocol_system: &str,
        context: &str,
    ) -> Result<&'a FamilyMemberSpec, ExtractionError> {
        self.require_family_spec(family_name, context)?;
        self.member_spec_for_family(family_name, protocol_system)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "{context} `{family_name}` cannot be applied to protocol system `{protocol_system}` because that protocol is not a declared member of the family"
                ))
            })
    }

    pub fn family_name_for_protocol_system(&self, protocol_system: &str) -> Option<&'a str> {
        self.specs
            .iter()
            .find(|spec| {
                spec.members
                    .iter()
                    .any(|member| member.protocol_system == protocol_system)
            })
            .map(|spec| spec.family_name)
    }

    pub(crate) fn auxiliary_protocol_message_decoders_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<&'a [AuxiliaryProtocolMessageDecoder]> {
        self.specs
            .iter()
            .find(|spec| {
                spec.members
                    .iter()
                    .any(|member| member.protocol_system == protocol_system)
            })
            .map(|spec| spec.auxiliary_protocol_message_decoders)
    }

    pub fn require_family_name_for_protocol_systems<'b>(
        &self,
        protocol_systems: impl IntoIterator<Item = &'b str>,
        context: &str,
    ) -> Result<&'a str, ExtractionError> {
        let mut family_name = None;

        for protocol_system in protocol_systems {
            let candidate = self
                .family_name_for_protocol_system(protocol_system)
                .ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "{context} could not resolve a registered family for protocol system `{protocol_system}`"
                    ))
                })?;

            if let Some(existing) = family_name {
                if existing != candidate {
                    return Err(ExtractionError::Setup(format!(
                        "{context} requires one registered family, found `{existing}` and `{candidate}`"
                    )));
                }
            } else {
                family_name = Some(candidate);
            }
        }

        family_name.ok_or_else(|| {
            ExtractionError::Setup(format!("{context} requires at least one protocol system"))
        })
    }

    pub fn shared_route_protocols_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<&'a [&'static str]> {
        self.member_spec_by_protocol_system(protocol_system)
            .map(|member| member.shared_route_protocols)
    }

    pub fn normalized_shared_route_protocol_filter_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<HashSet<String>> {
        self.member_spec_by_protocol_system(protocol_system)
            .map(|member| normalized_shared_route_protocols_for_member(member))
    }

    pub fn validate_family_runtime_config(
        &self,
        protocol_system: &str,
        family_runtime: &FamilyRuntimeConfig,
    ) -> Result<&'a FamilyMemberSpec, ExtractionError> {
        self.require_member_spec_for_family(
            &family_runtime.family,
            protocol_system,
            "family_runtime",
        )
    }

    pub fn resolve_family_runtime_config(
        &self,
        protocol_system: &str,
        mut family_runtime: FamilyRuntimeConfig,
        shared_spkg: Option<String>,
        shared_module: Option<String>,
        durability_scope: Option<String>,
    ) -> Result<FamilyRuntimeConfig, ExtractionError> {
        self.validate_family_runtime_config(protocol_system, &family_runtime)?;
        let shared_metadata =
            self.require_shared_runtime_metadata_for_family(&family_runtime.family, "family_runtime")?;

        if family_runtime.shared_spkg.is_none() {
            family_runtime.shared_spkg = shared_spkg;
        }
        if family_runtime.shared_module.is_none() {
            family_runtime.shared_module =
                shared_module.or_else(|| Some(shared_metadata.output_module.to_string()));
        }
        if family_runtime
            .durability_scope
            .is_none()
        {
            family_runtime.durability_scope =
                durability_scope.or_else(|| Some(shared_metadata.durability_scope.to_string()));
        }

        if family_runtime.shared_spkg.is_none() {
            return Err(ExtractionError::Setup(format!(
                "family_runtime `{}` must resolve `shared_spkg` either inline or via top-level family_runtimes",
                family_runtime.family
            )));
        }

        Ok(family_runtime)
    }

    pub fn validate_shared_bootstrap_support_for_family(
        &self,
        family_name: &str,
    ) -> Result<&'a FamilyRuntimeSpec, ExtractionError> {
        let spec = self.require_family_spec(family_name, "family bootstrap defaults for")?;
        for member in spec.members {
            if member.shared_bootstrap.is_none() {
                return Err(ExtractionError::Setup(format!(
                    "family bootstrap defaults for `{family_name}` require every member to declare a shared bootstrap strategy, but `{}` does not",
                    member.protocol_system
                )));
            }
        }
        Ok(spec)
    }

    pub fn validate_family_member_defaults_for_family<'b>(
        &self,
        family_name: &str,
        protocol_systems: impl IntoIterator<Item = &'b str>,
    ) -> Result<(), ExtractionError> {
        self.require_family_spec(family_name, "family_runtime")?;
        for protocol_system in protocol_systems {
            self.require_member_spec_for_family(
                family_name,
                protocol_system,
                "family_runtime member defaults for",
            )?;
        }
        Ok(())
    }

    pub fn materialize_shared_bootstrap_plan<'b>(
        &'b self,
        family_name: &str,
        rpc: &'b EthereumRpcClient,
        plan: &'b SharedBootstrapPlan,
    ) -> Result<
        Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'b>>,
        ExtractionError,
    > {
        let execution = self.resolve_shared_bootstrap_execution(family_name)?;
        Ok(Box::pin(async move { execution.materialize_plan(rpc, plan).await }))
    }

    pub fn resolve_shared_bootstrap_plan_materializer(
        &self,
        family_name: &str,
    ) -> Result<MaterializeBootstrapPlanFn, ExtractionError> {
        let spec =
            self.require_family_spec(family_name, "shared bootstrap plan materializer for")?;
        Ok(spec
            .shared_bootstrap_runtime
            .map(|runtime| runtime.materialize_plan)
            .unwrap_or_else(default_shared_bootstrap_plan_materializer))
    }

    pub fn resolve_shared_bootstrap_execution(
        &self,
        family_name: &str,
    ) -> Result<ResolvedSharedBootstrapExecution, ExtractionError> {
        Ok(ResolvedSharedBootstrapExecution {
            plan_materializer: self.resolve_shared_bootstrap_plan_materializer(family_name)?,
            branch_runtimes: self.resolve_shared_bootstrap_branch_runtimes(family_name)?,
        })
    }

    pub fn resolve_shared_bootstrap_execution_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Result<ResolvedSharedBootstrapExecution, ExtractionError> {
        let family_name = self.family_name_for_protocol_system(protocol_system).ok_or_else(|| {
            ExtractionError::Setup(format!(
                "shared bootstrap execution for protocol system `{protocol_system}` does not match any registered family runtime"
            ))
        })?;
        self.resolve_shared_bootstrap_execution(family_name)
    }

    pub fn resolve_shared_bootstrap_plan_family_name(
        &self,
        configs: &[(&ExtractorConfig, &BootstrapConfig)],
    ) -> Result<Option<String>, ExtractionError> {
        let mut expected_chain = None;
        let mut expected_family = None;
        let mut saw_family_runtime = false;
        let mut saw_missing_family_runtime = false;
        let mut seen_protocol_systems = HashSet::new();
        let mut inferred_protocol_systems = Vec::new();

        for (config, _) in configs {
            if let Some(chain) = expected_chain {
                if config.chain() != chain {
                    return Err(ExtractionError::Setup(format!(
                        "shared bootstrap plan requires one chain, found `{}` and `{}`",
                        chain,
                        config.chain()
                    )));
                }
            } else {
                expected_chain = Some(config.chain());
            }

            if !seen_protocol_systems.insert(config.protocol_system().to_string()) {
                return Err(ExtractionError::Setup(format!(
                    "shared bootstrap plan received duplicate protocol system `{}`",
                    config.protocol_system()
                )));
            }

            if let Some(runtime) = config.family_runtime() {
                saw_family_runtime = true;
                if let Some(family) = &expected_family {
                    if runtime.family != *family {
                        return Err(ExtractionError::Setup(format!(
                            "shared bootstrap plan requires one family runtime, found `{}` and `{}`",
                            family, runtime.family
                        )));
                    }
                } else {
                    expected_family = Some(runtime.family.clone());
                }
            } else {
                saw_missing_family_runtime = true;
                inferred_protocol_systems.push(config.protocol_system());
            }
        }

        if configs.len() > 1 && saw_family_runtime && saw_missing_family_runtime {
            return Err(ExtractionError::Setup(
                "shared bootstrap plan for multiple extractors requires either a family runtime on every config or on none of them".to_string(),
            ));
        }

        if !inferred_protocol_systems.is_empty() {
            let inferred_family = self.require_family_name_for_protocol_systems(
                inferred_protocol_systems.iter().copied(),
                "shared bootstrap plan inferred family runtime",
            )?;

            if let Some(family) = &expected_family {
                if inferred_family != family {
                    return Err(ExtractionError::Setup(format!(
                        "shared bootstrap plan requires one inferred family runtime, found `{}` and `{}`",
                        family, inferred_family
                    )));
                }
            } else {
                expected_family = Some(inferred_family.to_string());
            }
        }

        Ok(expected_family)
    }

    pub fn build_shared_bootstrap_plan<'b>(
        &self,
        configs: impl IntoIterator<Item = (&'b ExtractorConfig, &'b BootstrapConfig)>,
    ) -> Result<SharedBootstrapPlan, ExtractionError> {
        let configs = configs.into_iter().collect::<Vec<_>>();
        self.validate()?;
        let family_name = self.resolve_shared_bootstrap_plan_family_name(&configs)?;

        let mut branches = Vec::new();
        let mut bootstrap_block = None;

        for (config, bootstrap) in configs {
            let params = parse_and_validate_bootstrap_params(config, bootstrap, *self)?;

            if let Some(expected_block) = bootstrap_block {
                if expected_block != params.bootstrap_block {
                    return Err(ExtractionError::Setup(format!(
                        "shared bootstrap plan requires one bootstrap_block, found {} and {}",
                        expected_block, params.bootstrap_block
                    )));
                }
            } else {
                bootstrap_block = Some(params.bootstrap_block);
            }

            branches.push(BootstrapBranchDescriptor {
                extractor_name: config.name().to_owned(),
                protocol_system: config.protocol_system().to_owned(),
                chain: config.chain(),
                strategy: bootstrap.strategy.clone(),
                params,
            });
        }

        Ok(SharedBootstrapPlan {
            family_name,
            bootstrap_block: bootstrap_block.ok_or_else(|| {
                ExtractionError::Setup("shared bootstrap plan contained no extractors".to_string())
            })?,
            branches,
        })
    }

    pub fn require_shared_bootstrap_member_for_family(
        &self,
        family_name: &str,
        protocol_system: &str,
        context: &str,
    ) -> Result<&'a FamilyMemberSpec, ExtractionError> {
        let member = self.require_member_spec_for_family(family_name, protocol_system, context)?;
        if member.shared_bootstrap.is_none() {
            return Err(ExtractionError::Setup(format!(
                "{context} `{family_name}` requires protocol system `{protocol_system}` to declare a shared bootstrap strategy"
            )));
        }
        Ok(member)
    }

    pub fn shared_bootstrap_strategy_for_family_member(
        &self,
        family_name: &str,
        protocol_system: &str,
        context: &str,
    ) -> Result<BootstrapStrategy, ExtractionError> {
        let member =
            self.require_shared_bootstrap_member_for_family(family_name, protocol_system, context)?;
        Ok(member
            .shared_bootstrap
            .expect("validated shared bootstrap member must have runtime")
            .strategy)
    }

    pub fn parse_shared_bootstrap_params(
        &self,
        protocol_system: &str,
        strategy: BootstrapStrategy,
        params: &str,
    ) -> Result<SharedBootstrapParams, ExtractionError> {
        let member =
            self.require_bootstrap_member_for_protocol_system(protocol_system, strategy)?;
        let parser = member
            .shared_bootstrap
            .expect("validated shared bootstrap member must have runtime")
            .params_parser;
        match parser {
            SharedBootstrapParamsParser::PoolList => parse_pool_list_bootstrap_params(params),
            SharedBootstrapParamsParser::Custom(parse) => parse(params),
        }
    }

    pub fn materialize_shared_bootstrap_branch<'b>(
        &'b self,
        rpc: &'b EthereumRpcClient,
        branch: &'b BootstrapBranchDescriptor,
    ) -> Result<
        Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'b>>,
        ExtractionError,
    > {
        let member = self.require_bootstrap_member_for_protocol_system(
            &branch.protocol_system,
            branch.strategy,
        )?;
        let materialize = member
            .shared_bootstrap
            .expect("validated shared bootstrap member must have runtime")
            .materialize_branch;
        Ok(materialize(rpc, branch))
    }

    pub fn resolve_shared_bootstrap_branch_runtimes(
        &self,
        family_name: &str,
    ) -> Result<Vec<ResolvedSharedBootstrapBranchRuntime>, ExtractionError> {
        let spec = self.require_family_spec(family_name, "shared bootstrap branch runtime for")?;
        let runtimes = spec
            .members
            .iter()
            .filter_map(|member| {
                member
                    .shared_bootstrap
                    .map(|bootstrap| ResolvedSharedBootstrapBranchRuntime {
                        protocol_system: member.protocol_system,
                        materialize_branch: bootstrap.materialize_branch,
                    })
            })
            .collect::<Vec<_>>();
        Ok(runtimes)
    }

    pub fn validate(&self) -> Result<(), ExtractionError> {
        let mut seen_protocol_systems = HashMap::new();
        let mut seen_route_protocols = HashMap::new();

        for spec in self.specs {
            for member in spec.members {
                if let Some(existing_family) =
                    seen_protocol_systems.insert(member.protocol_system, spec.family_name)
                {
                    return Err(ExtractionError::Setup(format!(
                        "family runtime registry assigns protocol system `{}` to both `{existing_family}` and `{}`",
                        member.protocol_system, spec.family_name
                    )));
                }

                for normalized in normalized_shared_route_protocols_for_member(member) {
                    if normalized.is_empty() {
                        return Err(ExtractionError::Setup(format!(
                            "family `{}` member `{}` declares an empty shared route protocol alias",
                            spec.family_name, member.protocol_system
                        )));
                    }

                    if let Some(existing_protocol_system) =
                        seen_route_protocols.insert(normalized.clone(), member.protocol_system)
                    {
                        return Err(ExtractionError::Setup(format!(
                            "shared route protocol alias `{normalized}` is assigned to both `{existing_protocol_system}` and `{}`",
                            member.protocol_system
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    fn require_bootstrap_member_for_protocol_system(
        &self,
        protocol_system: &str,
        strategy: BootstrapStrategy,
    ) -> Result<&'a FamilyMemberSpec, ExtractionError> {
        let member = self
            .member_spec_by_protocol_system(protocol_system)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "shared bootstrap registry is missing protocol system `{protocol_system}`"
                ))
            })?;

        match member
            .shared_bootstrap
            .map(|bootstrap| bootstrap.strategy)
        {
            Some(member_strategy) if member_strategy == strategy => Ok(member),
            Some(member_strategy) => Err(ExtractionError::Setup(format!(
                "protocol system `{protocol_system}` expects bootstrap strategy `{:?}`, got `{:?}`",
                member_strategy, strategy
            ))),
            None => Err(ExtractionError::Setup(format!(
                "protocol system `{protocol_system}` does not declare a shared bootstrap strategy"
            ))),
        }
    }
}

pub const fn default_family_runtime_registry() -> FamilyRuntimeRegistry<'static> {
    FamilyRuntimeRegistry::new(crate::extractor::family_registry::default_family_runtime_specs())
}

fn normalized_shared_route_protocols_for_member(member: &FamilyMemberSpec) -> HashSet<String> {
    if member.shared_route_protocols.is_empty() {
        HashSet::from([canonicalize_shared_route_protocol(member.protocol_system)])
    } else {
        member
            .shared_route_protocols
            .iter()
            .map(|protocol| canonicalize_shared_route_protocol(protocol))
            .collect()
    }
}

pub fn canonicalize_shared_route_protocol(protocol: &str) -> String {
    protocol
        .chars()
        .filter(|char| char.is_ascii_alphanumeric())
        .flat_map(|char| char.to_lowercase())
        .collect()
}

pub fn detect_family_runtimes(
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<Vec<DetectedFamilyRuntime>, ExtractionError> {
    detect_family_runtimes_with_registry(extractors, default_family_runtime_registry())
}

pub fn detect_family_runtimes_with_registry(
    extractors: &HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Vec<DetectedFamilyRuntime>, ExtractionError> {
    registry.validate()?;
    let mut detected = Vec::new();
    let mut claimed_members = HashMap::new();

    for spec in registry.specs() {
        let Some((shared_spkg, output_module)) = detect_shared_runtime(spec, extractors)? else {
            continue;
        };
        let chain = detect_shared_chain(spec, extractors)?;

        for member in spec.members {
            if let Some(existing_family) =
                claimed_members.insert(member.protocol_system, spec.family_name)
            {
                return Err(ExtractionError::Setup(format!(
                    "protocol system `{}` is assigned to multiple family runtimes: `{existing_family}` and `{}`",
                    member.protocol_system,
                    spec.family_name
                )));
            }
        }

        let detected_family =
            registry.detected_family_runtime(spec.family_name, chain, shared_spkg)?;
        debug_assert_eq!(detected_family.output_module(), output_module);
        detected.push(detected_family);
    }

    Ok(detected)
}

fn detect_shared_chain(
    spec: &FamilyRuntimeSpec,
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<Chain, ExtractionError> {
    let mut shared_chain = None;

    for member in spec.members {
        let protocol_system = member.protocol_system;
        let config = extractor_config_by_protocol_system(extractors, protocol_system)?
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "family `{}` is missing extractor config for `{protocol_system}` while resolving chain",
                    spec.family_name
                ))
            })?;

        if let Some(existing) = shared_chain {
            if existing != config.chain() {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires all members to share one chain, but `{}` uses `{}` while another member uses `{}`",
                    spec.family_name,
                    protocol_system,
                    config.chain(),
                    existing,
                )));
            }
        } else {
            shared_chain = Some(config.chain());
        }
    }

    shared_chain.ok_or_else(|| {
        ExtractionError::Setup(format!(
            "family `{}` has no members to resolve chain from",
            spec.family_name
        ))
    })
}

fn detect_shared_runtime(
    spec: &FamilyRuntimeSpec,
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<Option<(String, String)>, ExtractionError> {
    detect_explicit_shared_runtime(spec, extractors)
}

fn detect_explicit_shared_runtime(
    spec: &FamilyRuntimeSpec,
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<Option<(String, String)>, ExtractionError> {
    let mut family_members: Vec<(&str, &ExtractorConfig)> = Vec::new();
    let explicitly_enabled_protocols = extractors
        .values()
        .filter_map(|config| {
            config
                .family_runtime()
                .filter(|runtime| runtime.family == spec.family_name)
                .map(|_| config.protocol_system().to_string())
        })
        .collect::<Vec<_>>();
    let any_explicit_opt_in = !explicitly_enabled_protocols.is_empty();

    for member in spec.members {
        let protocol_system = member.protocol_system;
        let Some(config) = extractor_config_by_protocol_system(extractors, protocol_system)? else {
            if any_explicit_opt_in {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires every declared member extractor to be present once any member opts into the shared runtime; configured members: {:?}, missing member: `{}`",
                    spec.family_name,
                    explicitly_enabled_protocols,
                    protocol_system,
                )));
            }
            return Ok(None);
        };
        family_members.push((protocol_system, config));
    }

    let explicitly_enabled = family_members
        .iter()
        .filter(|(_, config)| {
            config
                .family_runtime()
                .is_some_and(|runtime| runtime.family == spec.family_name)
        })
        .count();

    if explicitly_enabled == 0 {
        return Ok(None);
    }

    if explicitly_enabled != family_members.len() {
        let configured_members = family_members
            .iter()
            .filter_map(|(protocol_system, config)| {
                config
                    .family_runtime()
                    .filter(|runtime| runtime.family == spec.family_name)
                    .map(|_| (*protocol_system).to_string())
            })
            .collect::<Vec<_>>();
        return Err(ExtractionError::Setup(format!(
            "family `{}` requires every member to opt into the shared runtime; configured members: {:?}, expected members: {:?}",
            spec.family_name,
            configured_members,
            spec.members
                .iter()
                .map(|member| member.protocol_system)
                .collect::<Vec<_>>(),
        )));
    }

    let mut shared_spkg = None;
    let mut output_module = None;

    for (protocol_system, config) in family_members {
        let target = config
            .require_resolved_family_runtime_metadata()?
            .expect("explicitly enabled members must resolve one shared stream target");
        let candidate_spkg = target.shared_stream.spkg;
        let candidate_module = target.shared_stream.module;

        if let Some(existing) = &shared_spkg {
            if existing != candidate_spkg {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires all members to share one spkg, but `{}` resolves `{}` while another member resolves `{existing}`",
                    spec.family_name,
                    protocol_system,
                    candidate_spkg,
                )));
            }
        } else {
            shared_spkg = Some(candidate_spkg.to_string());
        }

        if let Some(existing) = &output_module {
            if existing != candidate_module {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires all members to share one output module, but `{}` resolves `{}` while another member resolves `{existing}`",
                    spec.family_name,
                    protocol_system,
                    candidate_module,
                )));
            }
        } else {
            output_module = Some(candidate_module.to_string());
        }
    }

    Ok(Some((
        shared_spkg.expect("shared spkg resolved for explicit family"),
        output_module.expect("shared output module resolved for explicit family"),
    )))
}

fn extractor_config_by_protocol_system<'a>(
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

pub fn family_member_set(detected: &[DetectedFamilyRuntime]) -> HashSet<String> {
    detected
        .iter()
        .flat_map(|family| {
            family
                .member_protocol_systems
                .iter()
                .cloned()
        })
        .collect()
}

pub fn standalone_protocol_systems(
    extractors: &HashMap<String, ExtractorConfig>,
    detected: &[DetectedFamilyRuntime],
) -> Vec<String> {
    let handled = family_member_set(detected);
    let mut standalone = extractors
        .values()
        .map(|config| config.protocol_system().to_string())
        .filter(|name| !handled.contains(name))
        .collect::<Vec<_>>();
    standalone.sort();
    standalone.dedup();
    standalone
}

pub fn build_family_runtime_plan(
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<FamilyRuntimeBuildPlan, ExtractionError> {
    build_family_runtime_plan_with_registry(extractors, default_family_runtime_registry())
}

pub fn build_family_runtime_plan_with_registry(
    extractors: &HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<FamilyRuntimeBuildPlan, ExtractionError> {
    let families = detect_family_runtimes_with_registry(extractors, registry)?;
    let standalone_protocol_systems = standalone_protocol_systems(extractors, &families);

    Ok(FamilyRuntimeBuildPlan { families, standalone_protocol_systems })
}

pub fn family_extractor_configs<'a>(
    family: &DetectedFamilyRuntime,
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<Vec<&'a ExtractorConfig>, ExtractionError> {
    let extractor_configs = family
        .member_protocol_systems
        .iter()
        .map(|name| {
            extractor_config_by_protocol_system(extractors, name)?.ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "family `{}` is missing extractor config for `{name}`",
                    family.family_name
                ))
            })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;

    validate_family_runtime_membership(family, &extractor_configs)?;
    validate_resolved_family_stream_config(family, &extractor_configs)?;

    Ok(extractor_configs)
}

pub(crate) fn validate_family_runtime_membership(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    for config in extractor_configs {
        if config.chain() != family.chain {
            return Err(ExtractionError::Setup(format!(
                "family runner for `{}` requires chain `{}`, but extractor `{}` uses `{}`",
                family.family_name,
                family.chain,
                config.name(),
                config.chain()
            )));
        }

        if let Some(runtime) = config.family_runtime() {
            if runtime.family != family.family_name {
                return Err(ExtractionError::Setup(format!(
                    "family runner for `{}` cannot include extractor `{}` declared for family `{}`",
                    family.family_name,
                    config.name(),
                    runtime.family
                )));
            }
        }

        if config.protocol_types().is_empty() {
            return Err(ExtractionError::Setup(format!(
                "family runner for `{}` requires extractor `{}` to declare at least one protocol type for branch routing",
                family.family_name,
                config.name()
            )));
        }
    }

    let actual = extractor_configs
        .iter()
        .map(|config| config.protocol_system().to_string())
        .collect::<HashSet<_>>();
    let expected = family
        .member_protocol_systems
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    if actual != expected {
        return Err(ExtractionError::Setup(format!(
            "family runner for `{}` requires exact member protocol systems {:?}, got {:?}",
            family.family_name, family.member_protocol_systems, actual
        )));
    }

    Ok(())
}

pub(crate) fn validate_resolved_family_stream_config(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    validate_family_shared_bootstrap_config(family, extractor_configs)?;
    validate_family_shared_start_block(family, extractor_configs)?;
    validate_family_shared_stop_block(family, extractor_configs)?;
    validate_family_shared_substreams_params(family, extractor_configs)?;
    Ok(())
}

pub(crate) fn resolve_resolved_family_execution_config(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilyExecutionConfig, ExtractionError> {
    validate_resolved_family_stream_config(family, extractor_configs)?;

    let branch_specs = FamilyBranchSpec::from_extractor_configs(extractor_configs)?;
    let merged_substreams_params =
        merged_family_substreams_params(extractor_configs).map_err(|err| match err {
            ExtractionError::Setup(message) => ExtractionError::Setup(format!(
                "family `{}` has incompatible shared substreams params: {message}",
                family.family_name
            )),
            other => other,
        })?;

    let first_config = extractor_configs
        .first()
        .ok_or_else(|| {
            ExtractionError::Setup(format!(
                "family `{}` has no extractor configs to resolve execution settings",
                family.family_name
            ))
        })?;

    let stop_block = u64::try_from(first_config.stop_block().unwrap_or(0)).map_err(|_| {
        ExtractionError::Setup(format!(
            "family `{}` resolved stop_block exceeds u64",
            family.family_name
        ))
    })?;
    let configured_start_block = configured_stream_start_block(first_config)?;
    let bootstrap_plan = resolve_family_bootstrap_plan(extractor_configs, registry)?;
    let shared_bootstrap_execution =
        registry.resolve_shared_bootstrap_execution(&family.family_name)?;
    let auxiliary_protocol_message_decoders_by_protocol_system = extractor_configs
        .iter()
        .map(|config| {
            (
                config.protocol_system().to_string(),
                registry
                    .auxiliary_protocol_message_decoders_for_protocol_system(
                        config.protocol_system(),
                    )
                    .map(|decoders| decoders.to_vec())
                    .unwrap_or_default(),
            )
        })
        .collect::<HashMap<_, _>>();

    Ok(ResolvedFamilyExecutionConfig {
        branch_specs,
        shared_stream: family.resolved_shared_stream(),
        shared_bootstrap_execution,
        auxiliary_protocol_message_decoders_by_protocol_system,
        merged_substreams_params,
        stop_block,
        configured_start_block,
        bootstrap_plan,
    })
}

#[cfg(test)]
pub(crate) fn resolved_family_execution_config_from_extractor_configs_for_tests(
    extractor_configs: &[&ExtractorConfig],
) -> Result<ResolvedFamilyExecutionConfig, ExtractionError> {
    let registry = default_family_runtime_registry();
    let detected_family = detect_single_test_family_runtime(extractor_configs, registry)?;

    resolve_resolved_family_execution_config(&detected_family, extractor_configs, registry)
}

#[cfg(test)]
fn infer_single_test_family_name(
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<String, ExtractionError> {
    registry
        .require_family_name_for_protocol_systems(
            extractor_configs
                .iter()
                .map(|config| config.protocol_system()),
            "family execution test helper",
        )
        .map(str::to_string)
}

#[cfg(test)]
fn detect_single_test_family_runtime(
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<DetectedFamilyRuntime, ExtractionError> {
    let family_name = infer_single_test_family_name(extractor_configs, registry)?;
    let extractors = extractor_configs
        .iter()
        .map(|config| (config.protocol_system().to_string(), (*config).clone()))
        .collect::<HashMap<_, _>>();
    let detected = detect_family_runtimes_with_registry(&extractors, registry)?;
    let detected = if detected.is_empty() {
        let family_spec = registry.require_family_spec(&family_name, "test family runtime")?;
        let synthetic_shared_spkg = format!("/tmp/{family_name}-family-test.spkg");
        let enriched = extractor_configs
            .iter()
            .map(|config| {
                let mut cloned = (*config).clone();
                cloned.family_runtime = Some(FamilyRuntimeConfig {
                    family: family_name.clone(),
                    shared_spkg: Some(synthetic_shared_spkg.clone()),
                    shared_module: Some(family_spec.output_module.to_string()),
                    durability_scope: Some(family_spec.durability_scope.to_string()),
                });
                (cloned.protocol_system().to_string(), cloned)
            })
            .collect::<HashMap<_, _>>();
        detect_family_runtimes_with_registry(&enriched, registry)?
    } else {
        detected
    };
    let mut matches = detected
        .into_iter()
        .filter(|family| family.family_name == family_name)
        .collect::<Vec<_>>();

    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(ExtractionError::Setup(format!(
            "family execution test helper could not detect family runtime `{family_name}` from provided extractor configs"
        ))),
        _ => Err(ExtractionError::Setup(format!(
            "family execution test helper expected exactly one detected family runtime `{family_name}`, found {}",
            matches.len()
        ))),
    }
}

fn validate_family_shared_bootstrap_config(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    let bootstrapped = extractor_configs
        .iter()
        .filter(|config| config.bootstrap.is_some())
        .map(|config| config.protocol_system().to_string())
        .collect::<Vec<_>>();
    let missing = extractor_configs
        .iter()
        .filter(|config| config.bootstrap.is_none())
        .map(|config| config.protocol_system().to_string())
        .collect::<Vec<_>>();

    if !bootstrapped.is_empty() && !missing.is_empty() {
        return Err(ExtractionError::Setup(format!(
            "family `{}` requires shared bootstrap configuration consistency across members; bootstrapped members: {:?}, missing bootstrap members: {:?}",
            family.family_name, bootstrapped, missing
        )));
    }

    Ok(())
}

fn validate_family_shared_start_block(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    let mut starts = Vec::new();

    for config in extractor_configs {
        starts.push((config.protocol_system().to_string(), configured_stream_start_block(config)?));
    }

    if let Some((_, first_start)) = starts.first() {
        if starts
            .iter()
            .any(|(_, start_block)| start_block != first_start)
        {
            return Err(ExtractionError::Setup(format!(
                "family `{}` requires aligned branch start blocks, found {:?}",
                family.family_name, starts
            )));
        }
    }

    Ok(())
}

fn validate_family_shared_stop_block(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    let mut stop_blocks = Vec::new();

    for config in extractor_configs {
        stop_blocks.push((config.protocol_system().to_string(), config.stop_block()));
    }

    if let Some((_, first_stop_block)) = stop_blocks.first() {
        if stop_blocks
            .iter()
            .any(|(_, stop_block)| stop_block != first_stop_block)
        {
            return Err(ExtractionError::Setup(format!(
                "family `{}` requires one shared stop_block, found {:?}",
                family.family_name, stop_blocks
            )));
        }
    }

    Ok(())
}

fn validate_family_shared_substreams_params(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    merged_family_substreams_params(extractor_configs).map_err(|err| match err {
        ExtractionError::Setup(message) => ExtractionError::Setup(format!(
            "family `{}` has incompatible shared substreams params: {message}",
            family.family_name
        )),
        other => other,
    })?;

    Ok(())
}

fn resolve_family_bootstrap_plan(
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Option<SharedBootstrapPlan>, ExtractionError> {
    let plan_inputs = extractor_configs
        .iter()
        .filter_map(|config| {
            config
                .bootstrap
                .as_ref()
                .map(|bootstrap| (*config, bootstrap))
        })
        .collect::<Vec<_>>();

    if plan_inputs.is_empty() {
        Ok(None)
    } else {
        registry
            .build_shared_bootstrap_plan(plan_inputs)
            .map(Some)
    }
}

pub fn build_resolved_family_runtime_plan<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<ResolvedFamilyRuntimePlan<'a>, ExtractionError> {
    build_resolved_family_runtime_plan_with_registry(extractors, default_family_runtime_registry())
}

pub fn build_resolved_runtime_targets<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<Vec<ResolvedRuntimeTarget<'a>>, ExtractionError> {
    build_resolved_runtime_targets_with_registry(extractors, default_family_runtime_registry())
}

pub fn build_resolved_family_runtime_plan_with_registry<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilyRuntimePlan<'a>, ExtractionError> {
    let runtime_plan = build_family_runtime_plan_with_registry(extractors, registry)?;
    let families = runtime_plan
        .families
        .into_iter()
        .map(|family| {
            let extractor_configs = family_extractor_configs(&family, extractors)?;
            let execution =
                resolve_resolved_family_execution_config(&family, &extractor_configs, registry)?;
            Ok(ResolvedFamilyRuntime { family, extractor_configs, execution })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;
    let standalone_extractors = runtime_plan
        .standalone_protocol_systems
        .into_iter()
        .map(|protocol_system| {
            extractor_config_by_protocol_system(extractors, &protocol_system)?
                .map(|extractor_config| ResolvedStandaloneRuntime {
                    protocol_system: extractor_config.protocol_system(),
                    extractor_config,
                })
                .ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "standalone extractor config `{protocol_system}` disappeared during resolution"
                    ))
                })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;

    Ok(ResolvedFamilyRuntimePlan { families, standalone_extractors })
}

pub fn build_resolved_runtime_targets_with_registry<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Vec<ResolvedRuntimeTarget<'a>>, ExtractionError> {
    let resolved = build_resolved_family_runtime_plan_with_registry(extractors, registry)?;
    let mut targets = resolved
        .families
        .into_iter()
        .map(ResolvedRuntimeTarget::Family)
        .collect::<Vec<_>>();
    targets.extend(
        resolved
            .standalone_extractors
            .into_iter()
            .map(ResolvedRuntimeTarget::Standalone),
    );
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use prost::Message;
    use tycho_common::models::{Chain, FinancialType, ImplementationType};
    use tycho_common::Bytes;
    use tycho_ethereum::rpc::EthereumRpcClient;

    use crate::extractor::runner::{
        BootstrapConfig, BootstrapStrategy, ExtractorConfig, ProtocolTypeConfig,
    };
    use crate::extractor::{
        family_registry::{
            canonical_shared_family_runtime_spec, pool_list_bootstrap_member_runtime,
            shared_bootstrap_member_runtime, shared_family_member_spec, shared_family_runtime_spec,
        },
        family_uniswap::materialize_uniswap_v2_branch,
        family_runtime::FamilyRuntimeConfig,
        protocol_message_registry::{AuxiliaryProtocolMessage, AuxiliaryProtocolMessageDecoder},
        shared_bootstrap::{BootstrapBranchDescriptor, SharedBootstrapParams, SharedBootstrapPlan},
        uniswap_v3_stream,
        ExtractionError,
    };

    use super::{
        build_family_runtime_plan, build_family_runtime_plan_with_registry,
        build_resolved_family_runtime_plan, build_resolved_family_runtime_plan_with_registry,
        build_resolved_runtime_targets, build_resolved_runtime_targets_with_registry,
        canonicalize_shared_route_protocol, default_family_runtime_registry,
        detect_family_runtimes, detect_family_runtimes_with_registry, family_extractor_configs,
        standalone_protocol_systems, FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec,
        ResolvedInitializedAccountsRequest, ResolvedRuntimeTarget, ResolvedRuntimeTargetSelector,
        ResolvedRuntimeTargets, ResolvedSharedFamilyStream, ResolvedStandaloneRuntime,
        SharedBootstrapParamsParser,
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

    fn base_uniswap_shared_stream(shared_spkg: &str) -> ResolvedSharedFamilyStream {
        default_family_runtime_registry()
            .resolved_shared_stream_for_family(Chain::Base, "uniswap", shared_spkg)
            .expect("registered base uniswap shared stream")
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

    fn decode_future_family_events(
        value: &[u8],
    ) -> Result<AuxiliaryProtocolMessage, ExtractionError> {
        Ok(AuxiliaryProtocolMessage::UniswapV3Events(
            uniswap_v3_stream::Events::decode(value)?,
        ))
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
    fn does_not_detect_uniswap_family_runtime_without_explicit_opt_in() {
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

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");

        assert!(detected.is_empty());
    }

    #[test]
    fn does_not_detect_family_when_one_member_missing() {
        let extractors = HashMap::from([(
            "uniswap_v2".to_string(),
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
        )]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");

        assert!(detected.is_empty());
    }

    #[test]
    fn explicit_family_runtime_rejects_mismatched_family_spkgs() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    "/tmp/a.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v3", "/tmp/v3-only.spkg"),
                    "/tmp/b.spkg",
                ),
            ),
        ]);

        let err = detect_family_runtimes(&extractors).expect_err("mismatched spkgs should fail");

        assert!(err
            .to_string()
            .contains("requires all members to share one spkg"));
    }

    #[test]
    fn explicit_family_runtime_rejects_mismatched_family_chains() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    "/tmp/a.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                ExtractorConfig::new(
                    "uniswap_v3".to_string(),
                    Chain::Base,
                    ImplementationType::Custom,
                    1000,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    "/tmp/v3-only.spkg".to_string(),
                    "map_protocol_changes".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_family_runtime(Some(FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: Some("/tmp/a.spkg".to_string()),
                    shared_module: Some(uniswap_shared_stream("/tmp/a.spkg").module),
                    durability_scope: None,
                })),
            ),
        ]);

        let err = detect_family_runtimes(&extractors).expect_err("mismatched chains should fail");

        assert!(err
            .to_string()
            .contains("requires all members to share one chain"));
    }

    #[test]
    fn preserves_standalone_extractors_outside_detected_families() {
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

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let standalone = standalone_protocol_systems(&extractors, &detected);

        assert_eq!(standalone, vec!["curve".to_string()]);
    }

    #[test]
    fn builds_runtime_plan_with_family_and_standalone_extractors() {
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

        let plan = build_family_runtime_plan(&extractors).expect("build plan succeeds");

        assert_eq!(plan.families.len(), 1);
        assert_eq!(plan.families[0].family_name, "uniswap");
        assert_eq!(plan.families[0].chain, Chain::Ethereum);
        assert_eq!(plan.standalone_protocol_systems, vec!["curve".to_string()]);
    }

    #[test]
    fn resolves_family_member_configs_from_detected_runtime() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                make_config(
                    "uniswap_v2",
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                make_config(
                    "uniswap_v3",
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let resolved =
            family_extractor_configs(&detected[0], &extractors).expect("family configs resolve");

        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].name(), "uniswap_v2");
        assert_eq!(resolved[1].name(), "uniswap_v3");
    }

    #[test]
    fn builds_resolved_runtime_plan() {
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

        let resolved = build_resolved_family_runtime_plan(&extractors).expect("resolved plan");

        assert_eq!(resolved.families.len(), 1);
        assert_eq!(resolved.families[0].family.family_name, "uniswap");
        assert_eq!(resolved.families[0].family.chain, Chain::Ethereum);
        assert_eq!(
            resolved.families[0]
                .extractor_configs
                .len(),
            2
        );
        assert_eq!(resolved.standalone_extractors.len(), 1);
        assert_eq!(resolved.standalone_extractors[0].protocol_system, "curve");
        assert_eq!(
            resolved.standalone_extractors[0]
                .extractor_config
                .name(),
            "curve"
        );
    }

    #[test]
    fn builds_resolved_runtime_targets() {
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

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");

        assert_eq!(targets.len(), 2);
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Family(family)
                if family.family.family_name == "uniswap" && family.extractor_configs.len() == 2
        )));
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Standalone(ResolvedStandaloneRuntime { extractor_config, .. })
                if extractor_config.name() == "curve"
        )));

        let standalone_target = targets
            .iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Standalone(_)))
            .expect("standalone target present");
        assert_eq!(standalone_target.chain(), Chain::Ethereum);
        assert_eq!(standalone_target.protocol_systems(), vec!["curve"]);
        assert_eq!(
            standalone_target
                .extractor_configs()
                .into_iter()
                .map(|config| config.name())
                .collect::<Vec<_>>(),
            vec!["curve"]
        );
    }

    #[test]
    fn test_family_execution_helper_reuses_production_family_execution_resolution() {
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

        let registry = default_family_runtime_registry();
        let family = detect_family_runtimes_with_registry(&extractors, registry)
            .expect("family detection succeeds")
            .into_iter()
            .next()
            .expect("uniswap family should be detected");
        let extractor_configs =
            family_extractor_configs(&family, &extractors).expect("family configs resolve");

        let from_production =
            super::resolve_resolved_family_execution_config(&family, &extractor_configs, registry)
                .expect("production family execution config resolves");
        let from_test_helper =
            super::resolved_family_execution_config_from_extractor_configs_for_tests(
                &extractor_configs,
            )
            .expect("test helper family execution config resolves");

        assert_eq!(
            from_test_helper.branch_specs, from_production.branch_specs,
            "test helper should reuse production branch routing"
        );
        assert_eq!(
            from_test_helper.shared_stream, from_production.shared_stream,
            "test helper should reuse production shared stream identity"
        );
        assert_eq!(
            from_test_helper.merged_substreams_params, from_production.merged_substreams_params,
            "test helper should reuse production shared substreams params"
        );
        assert_eq!(
            from_test_helper.stop_block, from_production.stop_block,
            "test helper should reuse production stop block resolution"
        );
        assert_eq!(
            from_test_helper.configured_start_block, from_production.configured_start_block,
            "test helper should reuse production start block resolution"
        );
        assert_eq!(
            from_test_helper.bootstrap_plan, from_production.bootstrap_plan,
            "test helper should reuse production shared bootstrap planning"
        );
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
    fn resolved_runtime_targets_resolve_substreams_execution_request_applies_selector_and_overrides() {
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

        assert!(
            err.to_string()
                .contains("available targets:"),
            "multiple-target unique selection should surface the available target list"
        );
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
                ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![Bytes::from([0x11; 20])],
                    block_id: 101,
                },
                ResolvedInitializedAccountsRequest {
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
            vec![ResolvedInitializedAccountsRequest {
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
                ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![
                        Bytes::from([0xaa; 20]),
                        Bytes::from([0x11; 20]),
                        Bytes::from([0x22; 20]),
                        Bytes::from([0xbb; 20]),
                    ],
                    block_id: 101,
                },
                ResolvedInitializedAccountsRequest {
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
                ResolvedInitializedAccountsRequest {
                    chain: Chain::Ethereum,
                    accounts: vec![
                        Bytes::from([0xaa; 20]),
                        Bytes::from([0x11; 20]),
                        Bytes::from([0x22; 20]),
                        Bytes::from([0xbb; 20]),
                    ],
                    block_id: 101,
                },
                ResolvedInitializedAccountsRequest {
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
                    Some(crate::extractor::runner::DCIType::RPC),
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
                    Some(crate::extractor::runner::DCIType::RPC),
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

    #[test]
    fn resolved_runtime_plan_precomputes_family_execution_settings() {
        let v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(120),
                vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::from([("map_pool_events".to_string(), "factory=0x01".to_string())]),
                Some(crate::extractor::runner::BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params: "bootstrap_block=42&pools=0x01".to_string(),
                }),
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let v3 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v3".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(120),
                vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
                Some(crate::extractor::runner::BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 42,
                    params: "bootstrap_block=42&pools=0x02".to_string(),
                }),
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let resolved = build_resolved_family_runtime_plan(&extractors)
            .expect("resolved family runtime plan should build");

        let family = resolved
            .families
            .first()
            .expect("one uniswap family should be resolved");
        let expected_shared_stream =
            uniswap_shared_stream("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg");

        assert_eq!(
            family.execution.shared_stream.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(family.execution.shared_stream.module, expected_shared_stream.module);
        assert_eq!(
            family
                .execution
                .shared_stream
                .extractor_id,
            expected_shared_stream.extractor_id
        );
        assert_eq!(
            family
                .execution
                .shared_bootstrap_execution
                .branch_runtimes
                .len(),
            2
        );
        assert_eq!(family.execution.stop_block, 120);
        assert_eq!(family.execution.configured_start_block, 43);
        assert_eq!(
            family
                .execution
                .merged_substreams_params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
        let bootstrap_plan = family
            .execution
            .bootstrap_plan
            .as_ref()
            .expect("family execution should precompute shared bootstrap plan");
        assert_eq!(bootstrap_plan.bootstrap_block, 42);
        assert_eq!(bootstrap_plan.branches.len(), 2);
    }

    #[test]
    fn resolved_runtime_plan_rejects_misaligned_effective_start_blocks() {
        let mut v2 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v2.bootstrap = Some(crate::extractor::runner::BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pools=0x01".to_string(),
        });
        let v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("misaligned effective start blocks should fail");

        assert!(err
            .to_string()
            .contains("family `uniswap` requires aligned branch start blocks"));
    }

    #[test]
    fn resolved_runtime_plan_rejects_partial_shared_bootstrap_config() {
        let mut v2 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v2.bootstrap = Some(crate::extractor::runner::BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pools=0x01".to_string(),
        });
        let v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("partial shared bootstrap config should fail during planning");

        assert!(err.to_string().contains(
            "family `uniswap` requires shared bootstrap configuration consistency across members"
        ));
    }

    #[test]
    fn resolved_runtime_plan_rejects_misaligned_stop_blocks() {
        let v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(100),
                vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let v3 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v3".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(200),
                vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("misaligned stop blocks should fail");

        assert!(err
            .to_string()
            .contains("family `uniswap` requires one shared stop_block"));
    }

    #[test]
    fn resolved_runtime_plan_rejects_conflicting_substreams_params() {
        let mut v2 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v2.substreams_params =
            HashMap::from([("map_pool_events".to_string(), "factory=0x01".to_string())]);

        let mut v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v3.substreams_params =
            HashMap::from([("map_pool_events".to_string(), "factory=0x02".to_string())]);

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("conflicting substreams params should fail");

        assert!(err
            .to_string()
            .contains("family `uniswap` has incompatible shared substreams params"));
    }

    #[test]
    fn resolved_runtime_plan_rejects_missing_protocol_types() {
        let v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                None,
                vec![],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("missing protocol types should fail");

        assert!(err
            .to_string()
            .contains("requires extractor `uniswap_v2` to declare at least one protocol type"));
    }

    #[test]
    fn stream_extractor_id_uses_detected_chain() {
        let spkg = "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg";
        let family = default_family_runtime_registry()
            .detected_family_runtime("uniswap", Chain::Base, spkg)
            .expect("registered uniswap family runtime");
        let expected_shared_stream = base_uniswap_shared_stream(spkg);

        assert_eq!(family.stream_extractor_id(), expected_shared_stream.extractor_id);
    }

    #[test]
    fn durability_scope_uses_detected_family_name() {
        let spkg = "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg";
        let family = default_family_runtime_registry()
            .detected_family_runtime("uniswap", Chain::Base, spkg)
            .expect("registered uniswap family runtime");
        let expected_shared_stream = base_uniswap_shared_stream(spkg);

        assert_eq!(family.durability_scope(), expected_shared_stream.durability_scope);
    }

    #[test]
    fn registry_resolves_shared_bootstrap_plan_family_name() {
        let registry = default_family_runtime_registry();
        let v2 = with_resolved_uniswap_family_runtime(
            make_config("uniswap_v2", "/tmp/v2-only.spkg"),
            "/tmp/a.spkg",
        );
        let v3 = with_resolved_uniswap_family_runtime(
            make_config("uniswap_v3", "/tmp/v3-only.spkg"),
            "/tmp/a.spkg",
        );
        let v2_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678"
                .to_string(),
        };
        let v3_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                .to_string(),
        };

        let family_name = registry
            .resolve_shared_bootstrap_plan_family_name(&[
                (&v2, &v2_bootstrap),
                (&v3, &v3_bootstrap),
            ])
            .expect("family name should resolve");

        assert_eq!(family_name, Some("uniswap".to_string()));
    }

    #[test]
    fn registry_builds_shared_bootstrap_plan_for_family_members() {
        let registry = default_family_runtime_registry();
        let v2 = with_resolved_uniswap_family_runtime(
            make_config("uniswap_v2", "/tmp/v2-only.spkg"),
            "/tmp/a.spkg",
        );
        let v3 = with_resolved_uniswap_family_runtime(
            make_config("uniswap_v3", "/tmp/v3-only.spkg"),
            "/tmp/a.spkg",
        );
        let v2_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678"
                .to_string(),
        };
        let v3_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                .to_string(),
        };

        let plan = registry
            .build_shared_bootstrap_plan([(&v2, &v2_bootstrap), (&v3, &v3_bootstrap)])
            .expect("shared bootstrap plan should build");

        assert_eq!(plan.family_name, Some("uniswap".to_string()));
        assert_eq!(plan.bootstrap_block, 42);
        assert_eq!(plan.branches.len(), 2);
        assert_eq!(plan.branches[0].protocol_system, "uniswap_v2");
        assert_eq!(plan.branches[1].protocol_system, "uniswap_v3");
    }

    #[test]
    fn registry_validates_family_member_defaults_for_family() {
        let registry = default_family_runtime_registry();

        registry
            .validate_family_member_defaults_for_family("uniswap", ["uniswap_v2", "uniswap_v3"])
            .expect("declared family members should validate");

        let err = registry
            .validate_family_member_defaults_for_family("uniswap", ["curve"])
            .expect_err("non-member defaults should fail");

        assert!(err
            .to_string()
            .contains("family_runtime member defaults for `uniswap` cannot be applied to protocol system `curve`"));
    }

    #[test]
    fn registry_resolves_shared_bootstrap_strategy_for_family_member() {
        let registry = default_family_runtime_registry();

        let strategy = registry
            .shared_bootstrap_strategy_for_family_member(
                "uniswap",
                "uniswap_v3",
                "family bootstrap defaults for",
            )
            .expect("strategy should resolve");

        assert_eq!(strategy, BootstrapStrategy::UniswapV3Rpc);
    }

    #[test]
    fn registry_parses_uniswap_v2_bootstrap_params() {
        let registry = default_family_runtime_registry();

        let params = registry
            .parse_shared_bootstrap_params(
                "uniswap_v2",
                BootstrapStrategy::UniswapV2Rpc,
                "bootstrap_block=42&pools=0x0000000000000000000000000000000000000001,0x0000000000000000000000000000000000000002",
            )
            .expect("v2 params parse");

        assert_eq!(params.bootstrap_block, 42);
        assert_eq!(
            params.pools,
            vec![
                Bytes::from("0x0000000000000000000000000000000000000001"),
                Bytes::from("0x0000000000000000000000000000000000000002"),
            ]
        );
    }

    #[test]
    fn registry_parses_uniswap_v3_bootstrap_params() {
        let registry = default_family_runtime_registry();

        let params = registry
            .parse_shared_bootstrap_params(
                "uniswap_v3",
                BootstrapStrategy::UniswapV3Rpc,
                "bootstrap_block=42&pool=0x0000000000000000000000000000000000000003",
            )
            .expect("v3 params parse");

        assert_eq!(params.bootstrap_block, 42);
        assert_eq!(params.pools, vec![Bytes::from("0x0000000000000000000000000000000000000003")]);
    }

    #[test]
    fn custom_registry_detects_future_family_without_runner_changes() {
        const FUTURE_AUXILIARY_PROTOCOL_MESSAGE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
            &[AuxiliaryProtocolMessageDecoder {
                protocol_system: "future_v1",
                type_url_suffix: "FutureEvents",
                decode: decode_future_family_events,
            }];
        const FUTURE_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "future_swap",
            members: &[
                FamilyMemberSpec {
                    protocol_system: "future_v1",
                    shared_route_protocols: &["futurev1"],
                    shared_bootstrap: None,
                },
                FamilyMemberSpec {
                    protocol_system: "future_v2",
                    shared_route_protocols: &["futurev2"],
                    shared_bootstrap: None,
                },
            ],
            output_module: "map_future_swap_family_protocol_changes",
            shared_stream_name: "future_swap_family",
            durability_scope: "family::future_swap_runtime",
            shared_bootstrap_runtime: None,
            auxiliary_protocol_message_decoders: FUTURE_AUXILIARY_PROTOCOL_MESSAGE_DECODERS,
        };
        const SPECS: &[FamilyRuntimeSpec] = &[FUTURE_FAMILY];
        let registry = FamilyRuntimeRegistry::new(SPECS);
        let extractors = HashMap::from([
            (
                "future_v1".to_string(),
                make_config("future_v1", "/tmp/future-v1-only.spkg").with_family_runtime(Some(
                    FamilyRuntimeConfig {
                        family: "future_swap".to_string(),
                        shared_spkg: Some(
                            "protocols/substreams/future-swap-combined/test.spkg".to_string(),
                        ),
                        shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                        durability_scope: Some("family::future_swap_runtime".to_string()),
                    },
                )),
            ),
            (
                "future_v2".to_string(),
                make_config("future_v2", "/tmp/future-v2-only.spkg").with_family_runtime(Some(
                    FamilyRuntimeConfig {
                        family: "future_swap".to_string(),
                        shared_spkg: Some(
                            "protocols/substreams/future-swap-combined/test.spkg".to_string(),
                        ),
                        shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                        durability_scope: Some("family::future_swap_runtime".to_string()),
                    },
                )),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let detected = detect_family_runtimes_with_registry(&extractors, registry)
            .expect("custom family detection succeeds");
        let plan = build_family_runtime_plan_with_registry(&extractors, registry)
            .expect("custom family plan builds");
        let resolved = build_resolved_family_runtime_plan_with_registry(&extractors, registry)
            .expect("custom resolved plan builds");

        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].family_name, "future_swap");
        assert_eq!(
            detected[0].member_protocol_systems,
            vec!["future_v1".to_string(), "future_v2".to_string()]
        );
        assert_eq!(detected[0].output_module(), "map_future_swap_family_protocol_changes");
        assert_eq!(detected[0].shared_stream_name, "future_swap_family");
        assert_eq!(detected[0].durability_scope(), "family::future_swap_runtime");
        assert_eq!(plan.standalone_protocol_systems, vec!["curve".to_string()]);
        assert_eq!(resolved.families.len(), 1);
        assert_eq!(
            resolved.families[0]
                .extractor_configs
                .len(),
            2
        );
        assert_eq!(resolved.standalone_extractors[0].protocol_system, "curve");

        let targets = build_resolved_runtime_targets_with_registry(&extractors, registry)
            .expect("custom resolved targets build");
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Family(family)
                if family.family.family_name == "future_swap" && family.extractor_configs.len() == 2
        )));
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Standalone(ResolvedStandaloneRuntime { extractor_config, .. })
                if extractor_config.name() == "curve"
        )));
        assert_eq!(
            registry
                .auxiliary_protocol_message_decoders_for_protocol_system("future_v1")
                .map(|decoders| decoders.len()),
            Some(1)
        );
        assert_eq!(
            resolved.families[0]
                .execution
                .auxiliary_protocol_message_decoders_by_protocol_system
                .get("future_v1")
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn registry_rejects_duplicate_member_protocol_systems_across_families() {
        const FAMILY_A: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "family_a",
            members: &[FamilyMemberSpec {
                protocol_system: "shared_protocol",
                shared_route_protocols: &[],
                shared_bootstrap: None,
            }],
            output_module: "map_family_a",
            shared_stream_name: "family_a_stream",
            durability_scope: "family::family_a",
            shared_bootstrap_runtime: None,
            auxiliary_protocol_message_decoders: &[],
        };
        const FAMILY_B: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "family_b",
            members: &[FamilyMemberSpec {
                protocol_system: "shared_protocol",
                shared_route_protocols: &[],
                shared_bootstrap: None,
            }],
            output_module: "map_family_b",
            shared_stream_name: "family_b_stream",
            durability_scope: "family::family_b",
            shared_bootstrap_runtime: None,
            auxiliary_protocol_message_decoders: &[],
        };
        const SPECS: &[FamilyRuntimeSpec] = &[FAMILY_A, FAMILY_B];
        let registry = FamilyRuntimeRegistry::new(SPECS);

        let err = registry
            .validate()
            .expect_err("duplicate protocol system across families should fail");

        assert!(err.to_string().contains(
            "assigns protocol system `shared_protocol` to both `family_a` and `family_b`"
        ));
    }

    fn parse_future_params(params: &str) -> Result<SharedBootstrapParams, ExtractionError> {
        let pool = params
            .split("pool=")
            .nth(1)
            .ok_or_else(|| ExtractionError::Setup("missing pool param".to_string()))?;
        Ok(SharedBootstrapParams { bootstrap_block: 99, pools: vec![Bytes::from(pool)] })
    }

    fn materialize_future_branch_fallback<'a>(
        _rpc: &'a EthereumRpcClient,
        _branch: &'a BootstrapBranchDescriptor,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::extractor::models::BlockChanges, ExtractionError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(ExtractionError::Setup("future branch fallback materializer reached".to_string()))
        })
    }

    #[test]
    fn custom_registry_parses_future_family_bootstrap_params() {
        const FUTURE_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "future_swap",
            &[shared_family_member_spec(
                "future_v1",
                &["futurev1"],
                Some(shared_bootstrap_member_runtime(
                    BootstrapStrategy::UniswapV2Rpc,
                    SharedBootstrapParamsParser::Custom(parse_future_params),
                    |_rpc, _branch| {
                        Box::pin(async {
                            Err(ExtractionError::Setup("not used in this test".to_string()))
                        })
                    },
                )),
            )],
            "map_future_swap_family_protocol_changes",
            "future_swap_family",
            "family::future_swap",
            None,
        );
        let registry = FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]);

        let params = registry
            .parse_shared_bootstrap_params(
                "future_v1",
                BootstrapStrategy::UniswapV2Rpc,
                "bootstrap_block=99&pool=0x0000000000000000000000000000000000000099",
            )
            .expect("custom registry params parse");

        assert_eq!(params.bootstrap_block, 99);
        assert_eq!(params.pools, vec![Bytes::from("0x0000000000000000000000000000000000000099")]);
    }

    #[tokio::test]
    async fn custom_registry_defaults_shared_bootstrap_plan_materializer_from_branch_runtimes() {
        const FUTURE_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "future_swap",
            &[shared_family_member_spec(
                "future_v1",
                &["futurev1"],
                Some(shared_bootstrap_member_runtime(
                    BootstrapStrategy::UniswapV2Rpc,
                    SharedBootstrapParamsParser::Custom(parse_future_params),
                    materialize_future_branch_fallback,
                )),
            )],
            "map_future_swap_family_protocol_changes",
            "future_swap_family",
            "family::future_swap",
            None,
        );
        let registry = FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]);
        let rpc = EthereumRpcClient::new("http://localhost:0000").expect("stub rpc client builds");
        let plan = SharedBootstrapPlan {
            family_name: Some("future_swap".to_string()),
            bootstrap_block: 99,
            branches: vec![BootstrapBranchDescriptor {
                extractor_name: "future_v1".to_string(),
                protocol_system: "future_v1".to_string(),
                chain: Chain::Ethereum,
                strategy: BootstrapStrategy::UniswapV2Rpc,
                params: SharedBootstrapParams {
                    bootstrap_block: 99,
                    pools: vec![Bytes::from("0x0000000000000000000000000000000000000099")],
                },
            }],
        };

        let err = registry
            .materialize_shared_bootstrap_plan("future_swap", &rpc, &plan)
            .expect("registry should resolve default plan materializer")
            .await
            .expect_err("default branch-level materializer should run");

        assert!(err
            .to_string()
            .contains("future branch fallback materializer reached"));
    }

    #[test]
    fn registry_uses_shared_pool_list_parser_for_builtin_uniswap_members() {
        let registry = default_family_runtime_registry();

        let params = registry
            .parse_shared_bootstrap_params(
                "uniswap_v3",
                BootstrapStrategy::UniswapV3Rpc,
                "bootstrap_block=42&pools=0x0000000000000000000000000000000000000001,0x0000000000000000000000000000000000000002",
            )
            .expect("built-in uniswap member should parse shared pool-list params");

        assert_eq!(params.bootstrap_block, 42);
        assert_eq!(
            params.pools,
            vec![
                Bytes::from("0x0000000000000000000000000000000000000001"),
                Bytes::from("0x0000000000000000000000000000000000000002"),
            ]
        );
    }

    #[test]
    fn registry_defaults_bootstrap_member_route_aliases_from_protocol_system() {
        const BROKEN_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "broken_family",
            &[shared_family_member_spec(
                "broken_protocol_v2",
                &[],
                Some(shared_bootstrap_member_runtime(
                    BootstrapStrategy::UniswapV2Rpc,
                    SharedBootstrapParamsParser::PoolList,
                    materialize_uniswap_v2_branch,
                )),
            )],
            "map_broken_family",
            "broken_family_stream",
            "family::broken_family",
            None,
        );
        let registry = FamilyRuntimeRegistry::new(&[BROKEN_FAMILY]);

        registry
            .validate()
            .expect("bootstrap-capable member without explicit route aliases should default from protocol system");
        assert_eq!(
            registry
                .normalized_shared_route_protocol_filter_for_protocol_system("broken_protocol_v2"),
            Some(HashSet::from(["brokenprotocolv2".to_string()]))
        );
    }

    #[test]
    fn registry_rejects_duplicate_normalized_route_aliases() {
        const BROKEN_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "broken_family",
            members: &[
                FamilyMemberSpec {
                    protocol_system: "protocol_a",
                    shared_route_protocols: &["Example-V2"],
                    shared_bootstrap: None,
                },
                FamilyMemberSpec {
                    protocol_system: "protocol_b",
                    shared_route_protocols: &["example_v2"],
                    shared_bootstrap: None,
                },
            ],
            output_module: "map_broken_family",
            shared_stream_name: "broken_family_stream",
            durability_scope: "family::broken_family",
            shared_bootstrap_runtime: None,
            auxiliary_protocol_message_decoders: &[],
        };
        let registry = FamilyRuntimeRegistry::new(&[BROKEN_FAMILY]);

        let err = registry
            .validate()
            .expect_err("duplicate normalized route aliases should fail");

        assert!(err
            .to_string()
            .contains("shared route protocol alias `examplev2` is assigned to both `protocol_a` and `protocol_b`"));
    }

    #[test]
    fn detects_explicit_family_runtime_without_spkg_hint() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    shared_spkg,
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v3", "/tmp/v3-only.spkg"),
                    shared_spkg,
                ),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let expected_shared_stream = uniswap_shared_stream(shared_spkg);

        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].shared_spkg(), shared_spkg);
        assert_eq!(detected[0].output_module(), expected_shared_stream.module);
    }

    #[test]
    fn rejects_partially_configured_explicit_family_runtime() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    shared_spkg,
                ),
            ),
            ("uniswap_v3".to_string(), make_config("uniswap_v3", "/tmp/v3-only.spkg")),
        ]);

        let err = detect_family_runtimes(&extractors)
            .expect_err("partially configured explicit family should fail");

        assert!(err
            .to_string()
            .contains("requires every member to opt into the shared runtime"));
    }

    #[test]
    fn rejects_explicit_family_runtime_when_declared_member_extractor_is_missing() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([(
            "uniswap_v2".to_string(),
            with_resolved_uniswap_family_runtime(
                make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                shared_spkg,
            ),
        )]);

        let err = detect_family_runtimes(&extractors)
            .expect_err("missing family member should fail once explicit runtime is enabled");

        assert!(err
            .to_string()
            .contains("requires every declared member extractor to be present once any member opts into the shared runtime"));
    }

    #[test]
    fn detects_family_by_explicit_protocol_system_not_config_key() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([
            (
                "uniswap_v2_primary".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2_indexer", "/tmp/v2-only.spkg")
                        .with_protocol_system("uniswap_v2"),
                    shared_spkg,
                ),
            ),
            (
                "uniswap_v3_primary".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v3_indexer", "/tmp/v3-only.spkg")
                        .with_protocol_system("uniswap_v3"),
                    shared_spkg,
                ),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let resolved = build_resolved_family_runtime_plan(&extractors).expect("resolved plan");

        assert_eq!(detected.len(), 1);
        assert_eq!(resolved.families.len(), 1);
        assert_eq!(
            resolved.families[0]
                .extractor_configs
                .iter()
                .map(|cfg| cfg.protocol_system().to_string())
                .collect::<Vec<_>>(),
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
    }

    #[test]
    fn rejects_duplicate_protocol_system_declarations() {
        let extractors = HashMap::from([
            (
                "first_v2".to_string(),
                make_config("first_v2", "/tmp/a.spkg").with_protocol_system("uniswap_v2"),
            ),
            (
                "second_v2".to_string(),
                make_config("second_v2", "/tmp/b.spkg").with_protocol_system("uniswap_v2"),
            ),
            (
                "v3".to_string(),
                make_config("v3", "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
                    .with_protocol_system("uniswap_v3"),
            ),
        ]);

        let err = detect_family_runtimes(&extractors)
            .expect_err("duplicate protocol_system declarations should fail");

        assert!(err
            .to_string()
            .contains("multiple extractor configs declare protocol_system `uniswap_v2`"));
    }

    #[test]
    fn registry_resolves_member_within_specific_family() {
        let registry = default_family_runtime_registry();

        let member = registry
            .member_spec_for_family("uniswap", "uniswap_v2")
            .expect("member in family");

        assert_eq!(member.protocol_system, "uniswap_v2");
        assert!(registry
            .member_spec_for_family("future_swap", "uniswap_v2")
            .is_none());
        assert!(registry
            .member_spec_for_family("uniswap", "future_v1")
            .is_none());
    }

    #[test]
    fn registry_exposes_normalized_shared_route_protocol_aliases() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry.normalized_shared_route_protocol_filter_for_protocol_system("uniswap_v2"),
            Some(HashSet::from([String::from("uniswapv2")]))
        );
        assert_eq!(
            registry.normalized_shared_route_protocol_filter_for_protocol_system("uniswap_v3"),
            Some(HashSet::from([String::from("uniswapv3")]))
        );
        assert_eq!(
            registry.normalized_shared_route_protocol_filter_for_protocol_system("curve"),
            None
        );
    }

    #[test]
    fn registry_resolves_family_name_for_protocol_system() {
        let registry = default_family_runtime_registry();

        assert_eq!(registry.family_name_for_protocol_system("uniswap_v2"), Some("uniswap"));
        assert_eq!(registry.family_name_for_protocol_system("uniswap_v3"), Some("uniswap"));
        assert_eq!(registry.family_name_for_protocol_system("curve"), None);
    }

    #[test]
    fn registry_requires_single_registered_family_for_protocol_systems() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry
                .require_family_name_for_protocol_systems(
                    ["uniswap_v2", "uniswap_v3"],
                    "family resolution test",
                )
                .expect("uniswap members should resolve one family"),
            "uniswap"
        );
    }

    #[test]
    fn registry_rejects_unknown_protocol_system_when_requiring_family_name() {
        let registry = default_family_runtime_registry();

        let err = registry
            .require_family_name_for_protocol_systems(["curve"], "family resolution test")
            .expect_err("unknown protocol system should fail family resolution");

        assert!(err
            .to_string()
            .contains("could not resolve a registered family for protocol system `curve`"));
    }

    #[test]
    fn registry_rejects_mixed_registered_families_when_requiring_family_name() {
        const OTHER_MEMBER: FamilyMemberSpec =
            shared_family_member_spec("other_v1", &[], None);
        const OTHER_MEMBERS: &[FamilyMemberSpec] = &[OTHER_MEMBER];
        const OTHER_FAMILY: FamilyRuntimeSpec =
            canonical_shared_family_runtime_spec!("other_swap", OTHER_MEMBERS, None);
        let mut specs = default_family_runtime_registry().specs().to_vec();
        specs.push(OTHER_FAMILY);

        let registry = FamilyRuntimeRegistry::new(&specs);
        let err = registry
            .require_family_name_for_protocol_systems(
                ["uniswap_v2", "other_v1"],
                "family resolution test",
            )
            .expect_err("mixed protocol systems should fail family resolution");

        assert!(err
            .to_string()
            .contains("requires one registered family, found `uniswap` and `other_swap`"));
    }

    #[test]
    fn registry_exposes_output_module_for_family_and_protocol_system() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-registry-output-module.spkg");

        assert_eq!(
            registry.output_module_for_family("uniswap"),
            Some(expected_shared_stream.module.as_str())
        );
        assert_eq!(
            registry.output_module_for_protocol_system("uniswap_v2"),
            Some(expected_shared_stream.module.as_str())
        );
        assert_eq!(
            registry.output_module_for_protocol_system("uniswap_v3"),
            Some(expected_shared_stream.module.as_str())
        );
        assert_eq!(registry.output_module_for_family("curve"), None);
        assert_eq!(registry.output_module_for_protocol_system("curve"), None);
    }

    #[test]
    fn registry_exposes_shared_runtime_metadata_for_family() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-shared-runtime-metadata.spkg");
        let metadata = registry
            .shared_runtime_metadata_for_family("uniswap")
            .expect("uniswap shared runtime metadata");

        assert_eq!(metadata.output_module, expected_shared_stream.module);
        assert_eq!(metadata.shared_stream_name, "uniswap_family");
        assert_eq!(metadata.durability_scope, expected_shared_stream.durability_scope);
        assert_eq!(registry.shared_runtime_metadata_for_family("curve"), None);
    }

    #[test]
    fn registry_exposes_shared_runtime_metadata_for_protocol_system() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-shared-runtime-metadata-by-protocol.spkg");
        let metadata = registry
            .shared_runtime_metadata_for_protocol_system("uniswap_v2")
            .expect("uniswap_v2 shared runtime metadata");

        assert_eq!(metadata.output_module, expected_shared_stream.module);
        assert_eq!(metadata.shared_stream_name, "uniswap_family");
        assert_eq!(metadata.durability_scope, expected_shared_stream.durability_scope);
        assert_eq!(
            registry.shared_runtime_metadata_for_protocol_system("curve"),
            None
        );
    }

    #[test]
    fn registry_resolves_shared_bootstrap_execution_for_protocol_system() {
        let registry = default_family_runtime_registry();

        let execution = registry
            .resolve_shared_bootstrap_execution_for_protocol_system("uniswap_v3")
            .expect("uniswap_v3 shared bootstrap execution");

        assert_eq!(execution.branch_runtimes.len(), 2);
        assert!(execution
            .branch_runtimes
            .iter()
            .any(|runtime| runtime.protocol_system == "uniswap_v2"));
        assert!(execution
            .branch_runtimes
            .iter()
            .any(|runtime| runtime.protocol_system == "uniswap_v3"));

        let err = registry
            .resolve_shared_bootstrap_execution_for_protocol_system("curve")
            .expect_err("curve should not resolve a shared bootstrap execution");
        assert!(err
            .to_string()
            .contains("does not match any registered family runtime"));
    }

    #[test]
    fn registry_exposes_member_protocol_systems_for_family() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry.member_protocol_systems_for_family("uniswap"),
            Some(vec!["uniswap_v2", "uniswap_v3"])
        );
        assert_eq!(registry.member_protocol_systems_for_family("curve"), None);
    }

    #[test]
    fn registry_builds_detected_family_runtime_from_registered_metadata() {
        let registry = default_family_runtime_registry();
        let family = registry
            .detected_family_runtime("uniswap", Chain::Ethereum, "/tmp/test.spkg")
            .expect("registered uniswap family runtime");
        let shared_stream = registry
            .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", "/tmp/test.spkg")
            .expect("registered uniswap shared stream");

        assert_eq!(family.family_name, "uniswap");
        assert_eq!(
            family.member_protocol_systems,
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(family.shared_spkg(), "/tmp/test.spkg");
        assert_eq!(family.output_module(), shared_stream.module);
        assert_eq!(
            family.shared_stream_name,
            registry
                .shared_stream_name_for_family("uniswap")
                .expect("uniswap shared stream name")
        );
        assert_eq!(family.durability_scope(), shared_stream.durability_scope);
    }

    #[test]
    fn registry_exposes_shared_stream_identity_for_family() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-shared-stream-identity.spkg");
        let identity = registry
            .shared_stream_identity_for_family(Chain::Ethereum, "uniswap")
            .expect("uniswap family stream identity");

        assert_eq!(
            registry.shared_stream_name_for_family("uniswap"),
            Some(identity.shared_stream_name)
        );
        assert_eq!(
            registry.durability_scope_for_family("uniswap"),
            Some(
                expected_shared_stream
                    .durability_scope
                    .as_str()
            )
        );
        assert_eq!(registry.shared_stream_name_for_family("curve"), None);
        assert_eq!(registry.durability_scope_for_family("curve"), None);
        assert_eq!(identity.output_module, expected_shared_stream.module);
        assert_eq!(
            identity.shared_stream_name,
            registry
                .shared_stream_name_for_family("uniswap")
                .expect("uniswap shared stream name")
        );
        assert_eq!(identity.extractor_id, expected_shared_stream.extractor_id);
        assert_eq!(identity.durability_scope, expected_shared_stream.durability_scope);
        assert!(registry
            .shared_stream_identity_for_family(Chain::Ethereum, "curve")
            .is_none());
    }

    #[test]
    fn registry_validates_family_runtime_membership_with_shared_error_surface() {
        let registry = default_family_runtime_registry();

        let member = registry
            .validate_family_runtime_config(
                "uniswap_v2",
                &FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
            )
            .expect("uniswap v2 belongs to uniswap family");

        assert_eq!(member.protocol_system, "uniswap_v2");

        let err = registry
            .validate_family_runtime_config(
                "curve",
                &FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
            )
            .expect_err("curve should not belong to uniswap family");
        assert!(err
            .to_string()
            .contains("family_runtime `uniswap` cannot be applied to protocol system `curve`"));
    }

    #[test]
    fn registry_resolves_family_runtime_config_with_top_level_defaults() {
        let registry = default_family_runtime_registry();
        let shared_stream = registry
            .resolved_shared_stream_for_family(
                Chain::Ethereum,
                "uniswap",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            )
            .expect("registered uniswap shared stream");

        let resolved = registry
            .resolve_family_runtime_config(
                "uniswap_v2",
                FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
                Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string()),
                Some(shared_stream.module.clone()),
                None,
            )
            .expect("family runtime defaults should resolve");

        assert_eq!(resolved.family, "uniswap");
        assert_eq!(
            resolved.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(resolved.shared_module.as_deref(), Some(shared_stream.module.as_str()));
        assert_eq!(
            resolved.durability_scope.as_deref(),
            Some(shared_stream.durability_scope.as_str())
        );
    }

    #[test]
    fn registry_resolves_family_runtime_config_with_registry_output_module_default() {
        let registry = default_family_runtime_registry();
        let shared_stream = registry
            .resolved_shared_stream_for_family(
                Chain::Ethereum,
                "uniswap",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            )
            .expect("registered uniswap shared stream");

        let resolved = registry
            .resolve_family_runtime_config(
                "uniswap_v2",
                FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
                Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string()),
                None,
                None,
            )
            .expect("family runtime should inherit output module from registry");

        assert_eq!(resolved.family, "uniswap");
        assert_eq!(
            resolved.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(resolved.shared_module.as_deref(), Some(shared_stream.module.as_str()));
        assert_eq!(
            resolved.durability_scope.as_deref(),
            Some(shared_stream.durability_scope.as_str())
        );
    }

    #[test]
    fn registry_resolves_family_runtime_config_with_registry_durability_scope_default() {
        const FUTURE_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "future_swap",
            members: &[FamilyMemberSpec {
                protocol_system: "future_v1",
                shared_route_protocols: &["futurev1"],
                shared_bootstrap: None,
            }],
            output_module: "map_future_swap_family_protocol_changes",
            shared_stream_name: "future_swap_family",
            durability_scope: "family::future_swap_runtime",
            shared_bootstrap_runtime: None,
            auxiliary_protocol_message_decoders: &[],
        };
        const SPECS: &[FamilyRuntimeSpec] = &[FUTURE_FAMILY];
        let registry = FamilyRuntimeRegistry::new(SPECS);

        let resolved = registry
            .resolve_family_runtime_config(
                "future_v1",
                FamilyRuntimeConfig {
                    family: "future_swap".to_string(),
                    shared_spkg: Some(
                        "protocols/substreams/future-swap-combined/test.spkg".to_string(),
                    ),
                    shared_module: None,
                    durability_scope: None,
                },
                None,
                None,
                None,
            )
            .expect("family runtime should inherit durability scope from registry");

        assert_eq!(resolved.durability_scope.as_deref(), Some("family::future_swap_runtime"));
    }

    #[test]
    fn registry_rejects_shared_bootstrap_defaults_for_family_without_full_bootstrap_support() {
        const PARTIAL_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec!(
            "partial_swap",
            &[
                shared_family_member_spec(
                    "partial_v1",
                    &["partialv1"],
                    Some(pool_list_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        materialize_uniswap_v2_branch,
                    )),
                ),
                shared_family_member_spec("partial_v2", &["partialv2"], None),
            ],
            None,
        );
        let registry = FamilyRuntimeRegistry::new(&[PARTIAL_FAMILY]);

        let err = registry
            .validate_shared_bootstrap_support_for_family("partial_swap")
            .expect_err("partial family should not allow shared bootstrap defaults");

        assert!(err.to_string().contains(
            "family bootstrap defaults for `partial_swap` require every member to declare a shared bootstrap strategy"
        ));
    }

    #[test]
    fn registry_rejects_family_bootstrap_member_without_shared_bootstrap_support() {
        const PARTIAL_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec!(
            "partial_swap",
            &[
                shared_family_member_spec(
                    "partial_v1",
                    &["partialv1"],
                    Some(pool_list_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        materialize_uniswap_v2_branch,
                    )),
                ),
                shared_family_member_spec("partial_v2", &["partialv2"], None),
            ],
            None,
        );
        let registry = FamilyRuntimeRegistry::new(&[PARTIAL_FAMILY]);

        let err = registry
            .require_shared_bootstrap_member_for_family(
                "partial_swap",
                "partial_v2",
                "family bootstrap defaults for",
            )
            .expect_err("partial_v2 should be rejected for shared bootstrap defaults");

        assert!(err.to_string().contains(
            "family bootstrap defaults for `partial_swap` requires protocol system `partial_v2` to declare a shared bootstrap strategy"
        ));
    }

    #[test]
    fn registry_exposes_normalized_shared_route_protocol_filter() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry.normalized_shared_route_protocol_filter_for_protocol_system("uniswap_v2"),
            Some(HashSet::from(["uniswapv2".to_string()]))
        );
        assert_eq!(canonicalize_shared_route_protocol("Uniswap-V3"), "uniswapv3".to_string());
    }
}
