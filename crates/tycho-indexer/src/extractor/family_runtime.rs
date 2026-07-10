use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use serde::Serialize;
use tycho_common::models::{Address, Chain, ExtractorIdentity};
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    family_dispatch::FamilyBranchSpec,
    models::BlockChanges,
    runner::{
        configured_stream_start_block, merged_family_substreams_params, BootstrapConfig,
        BootstrapStrategy, ExtractorConfig, FamilyRuntimeConfig,
    },
    shared_bootstrap::{
        materialize_plan_by_branch_runtimes, parse_pool_list_bootstrap_params,
        BootstrapBranchDescriptor, SharedBootstrapParams, SharedBootstrapPlan,
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
    pub shared_spkg: String,
    pub output_module: String,
    pub shared_stream_name: String,
    pub durability_scope: String,
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
    pub shared_bootstrap_plan_materializer: MaterializeBootstrapPlanFn,
    pub shared_bootstrap_branches: Vec<ResolvedSharedBootstrapBranchRuntime>,
    pub merged_substreams_params: HashMap<String, String>,
    pub stop_block: u64,
    pub configured_start_block: i64,
    pub bootstrap_plan: Option<SharedBootstrapPlan>,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyRuntimePlan<'a> {
    pub families: Vec<ResolvedFamilyRuntime<'a>>,
    pub standalone_extractors: Vec<ResolvedStandaloneRuntime<'a>>,
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

impl<'a> ResolvedRuntimeTarget<'a> {
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

pub fn select_resolved_runtime_target<'a>(
    targets: Vec<ResolvedRuntimeTarget<'a>>,
    selector: ResolvedRuntimeTargetSelector<'_>,
) -> Option<ResolvedRuntimeTarget<'a>> {
    targets
        .into_iter()
        .find(|target| target.matches_selector(selector))
}

pub fn coalesce_initialized_accounts_requests<'a>(
    targets: impl IntoIterator<Item = &'a ResolvedRuntimeTarget<'a>>,
) -> Vec<ResolvedInitializedAccountsRequest> {
    let mut requests: Vec<ResolvedInitializedAccountsRequest> = Vec::new();

    for target in targets {
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

impl DetectedFamilyRuntime {
    pub fn stream_extractor_id(&self) -> String {
        format!("{}:{}", self.chain, self.shared_stream_name)
    }

    pub fn resolved_shared_stream(&self) -> ResolvedSharedFamilyStream {
        ResolvedSharedFamilyStream {
            spkg: self.shared_spkg.clone(),
            module: self.output_module.clone(),
            extractor_id: self.stream_extractor_id(),
            durability_scope: self.durability_scope(),
        }
    }

    pub fn durability_scope(&self) -> String {
        self.durability_scope.clone()
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

    pub fn output_module_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.family_spec_by_name(family_name)
            .map(|spec| spec.output_module)
    }

    pub fn shared_stream_name_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.family_spec_by_name(family_name)
            .map(|spec| spec.shared_stream_name)
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
        self.family_spec_by_name(family_name)
            .map(|spec| spec.durability_scope)
    }

    pub fn shared_stream_identity_for_family(
        &self,
        chain: Chain,
        family_name: &str,
    ) -> Option<FamilySharedStreamIdentity<'a>> {
        let spec = self.family_spec_by_name(family_name)?;
        Some(FamilySharedStreamIdentity {
            output_module: spec.output_module,
            shared_stream_name: spec.shared_stream_name,
            extractor_id: format!("{chain}:{}", spec.shared_stream_name),
            durability_scope: spec.durability_scope,
        })
    }

    pub fn detected_family_runtime(
        &self,
        family_name: &str,
        chain: Chain,
        shared_spkg: impl Into<String>,
    ) -> Result<DetectedFamilyRuntime, ExtractionError> {
        let spec = self.require_family_spec(family_name, "family runtime")?;
        Ok(DetectedFamilyRuntime {
            family_name: spec.family_name.to_string(),
            chain,
            member_protocol_systems: spec
                .members
                .iter()
                .map(|member| member.protocol_system.to_string())
                .collect(),
            shared_spkg: shared_spkg.into(),
            output_module: spec.output_module.to_string(),
            shared_stream_name: spec.shared_stream_name.to_string(),
            durability_scope: spec.durability_scope.to_string(),
        })
    }

    pub fn output_module_for_protocol_system(&self, protocol_system: &str) -> Option<&'a str> {
        self.family_name_for_protocol_system(protocol_system)
            .and_then(|family_name| self.output_module_for_family(family_name))
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
        let family_spec = self.require_family_spec(&family_runtime.family, "family_runtime")?;

        if family_runtime.shared_spkg.is_none() {
            family_runtime.shared_spkg = shared_spkg;
        }
        if family_runtime.shared_module.is_none() {
            family_runtime.shared_module =
                shared_module.or_else(|| Some(family_spec.output_module.to_string()));
        }
        if family_runtime
            .durability_scope
            .is_none()
        {
            family_runtime.durability_scope =
                durability_scope.or_else(|| Some(family_spec.durability_scope.to_string()));
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
        let spec = self.require_family_spec(family_name, "shared bootstrap plan for")?;
        let branch_runtimes = self.resolve_shared_bootstrap_branch_runtimes(family_name)?;
        let materialize_plan = spec
            .shared_bootstrap_runtime
            .map(|runtime| runtime.materialize_plan)
            .unwrap_or_else(default_shared_bootstrap_plan_materializer);
        Ok(Box::pin(async move { materialize_plan(rpc, plan, &branch_runtimes).await }))
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

    pub fn resolve_shared_bootstrap_plan_family_name(
        &self,
        configs: &[(&ExtractorConfig, &BootstrapConfig)],
    ) -> Result<Option<String>, ExtractionError> {
        let mut expected_chain = None;
        let mut expected_family = None;
        let mut saw_family_runtime = false;
        let mut saw_missing_family_runtime = false;
        let mut seen_protocol_systems = HashSet::new();

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
                if let Some(inferred_family) =
                    self.family_name_for_protocol_system(config.protocol_system())
                {
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
            }
        }

        if configs.len() > 1 && saw_family_runtime && saw_missing_family_runtime {
            return Err(ExtractionError::Setup(
                "shared bootstrap plan for multiple extractors requires either a family runtime on every config or on none of them".to_string(),
            ));
        }

        Ok(expected_family)
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
        debug_assert_eq!(detected_family.output_module, output_module);
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
            .require_family_shared_stream_target()?
            .expect("explicitly enabled members must resolve one shared stream target");
        let candidate_spkg = target.spkg;
        let candidate_module = target.module;

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
    let shared_bootstrap_branches =
        registry.resolve_shared_bootstrap_branch_runtimes(&family.family_name)?;
    let shared_bootstrap_plan_materializer =
        registry.resolve_shared_bootstrap_plan_materializer(&family.family_name)?;

    Ok(ResolvedFamilyExecutionConfig {
        branch_specs,
        shared_stream: family.resolved_shared_stream(),
        shared_bootstrap_plan_materializer,
        shared_bootstrap_branches,
        merged_substreams_params,
        stop_block,
        configured_start_block,
        bootstrap_plan,
    })
}

#[cfg(test)]
pub(crate) fn empty_resolved_family_execution_config_for_tests() -> ResolvedFamilyExecutionConfig {
    ResolvedFamilyExecutionConfig {
        branch_specs: Vec::new(),
        shared_stream: ResolvedSharedFamilyStream {
            spkg: String::new(),
            module: String::new(),
            extractor_id: String::new(),
            durability_scope: String::new(),
        },
        shared_bootstrap_plan_materializer: default_shared_bootstrap_plan_materializer(),
        shared_bootstrap_branches: Vec::new(),
        merged_substreams_params: HashMap::new(),
        stop_block: 0,
        configured_start_block: 0,
        bootstrap_plan: None,
    }
}

#[cfg(test)]
pub(crate) fn resolved_family_execution_config_from_extractor_configs_for_tests(
    extractor_configs: &[&ExtractorConfig],
) -> Result<ResolvedFamilyExecutionConfig, ExtractionError> {
    let branch_specs = FamilyBranchSpec::from_extractor_configs(extractor_configs)?;
    let merged_substreams_params = merged_family_substreams_params(extractor_configs)?;
    let stop_block = resolve_family_stop_block_for_tests(extractor_configs)?;
    let configured_start_block =
        resolve_family_configured_start_block_for_tests(extractor_configs)?;
    let registry = default_family_runtime_registry();
    let family_name = infer_single_test_family_name(extractor_configs, registry)?;
    let chain = extractor_configs
        .first()
        .map(|config| config.chain())
        .ok_or_else(|| {
            ExtractionError::Setup(
                "family execution test helper requires at least one extractor config".to_string(),
            )
        })?;
    let bootstrap_plan = resolve_family_bootstrap_plan(extractor_configs, registry)?;
    let shared_bootstrap_branches =
        registry.resolve_shared_bootstrap_branch_runtimes(&family_name)?;
    let shared_bootstrap_plan_materializer =
        registry.resolve_shared_bootstrap_plan_materializer(&family_name)?;
    let shared_stream = registry
        .detected_family_runtime(&family_name, chain, String::new())?
        .resolved_shared_stream();

    Ok(ResolvedFamilyExecutionConfig {
        branch_specs,
        shared_stream,
        shared_bootstrap_plan_materializer,
        shared_bootstrap_branches,
        merged_substreams_params,
        stop_block,
        configured_start_block,
        bootstrap_plan,
    })
}

#[cfg(test)]
fn infer_single_test_family_name(
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<String, ExtractionError> {
    let mut family_name = None;

    for config in extractor_configs {
        let candidate = registry
            .family_name_for_protocol_system(config.protocol_system())
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "family execution test helper could not resolve a registered family for protocol system `{}`",
                    config.protocol_system()
                ))
            })?;

        if let Some(existing) = family_name.as_deref() {
            if existing != candidate {
                return Err(ExtractionError::Setup(format!(
                    "family execution test helper requires one registered family, found `{existing}` and `{candidate}`"
                )));
            }
        } else {
            family_name = Some(candidate.to_string());
        }
    }

    family_name.ok_or_else(|| {
        ExtractionError::Setup(
            "family execution test helper requires at least one extractor config".to_string(),
        )
    })
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
        SharedBootstrapPlan::for_extractor_configs_with_registry(plan_inputs, registry).map(Some)
    }
}

#[cfg(test)]
fn resolve_family_stop_block_for_tests(
    extractor_configs: &[&ExtractorConfig],
) -> Result<u64, ExtractionError> {
    let mut resolved = None;

    for config in extractor_configs {
        let candidate = config.stop_block();
        if let Some(existing) = resolved {
            if existing != candidate {
                return Err(ExtractionError::Setup(format!(
                    "family runner requires one shared stop_block, found {:?} on `{}` and {:?} on another family member",
                    candidate,
                    config.name(),
                    existing
                )));
            }
        } else {
            resolved = Some(candidate);
        }
    }

    Ok(resolved.flatten().unwrap_or(0) as u64)
}

#[cfg(test)]
fn resolve_family_configured_start_block_for_tests(
    extractor_configs: &[&ExtractorConfig],
) -> Result<i64, ExtractionError> {
    let mut configured_starts = Vec::new();
    for cfg in extractor_configs {
        let configured_stream_start = configured_stream_start_block(cfg)?;
        configured_starts.push((cfg.protocol_system().to_string(), configured_stream_start));
    }

    let (_, first_start) = configured_starts
        .first()
        .ok_or_else(|| ExtractionError::Setup("family runner has no branch configs".to_string()))?;
    if configured_starts
        .iter()
        .any(|(_, start_block)| start_block != first_start)
    {
        return Err(ExtractionError::Setup(format!(
            "family runner requires aligned branch start blocks, found {:?}",
            configured_starts
        )));
    }

    Ok(*first_start)
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

    use tycho_common::models::{Chain, FinancialType, ImplementationType};
    use tycho_common::Bytes;
    use tycho_ethereum::rpc::EthereumRpcClient;

    use crate::extractor::runner::{
        BootstrapConfig, BootstrapStrategy, ExtractorConfig, FamilyRuntimeConfig,
        ProtocolTypeConfig,
    };
    use crate::extractor::{
        family_registry::{
            shared_bootstrap_member_runtime, shared_family_member_spec, shared_family_runtime_spec,
        },
        family_uniswap::materialize_uniswap_v2_branch,
        shared_bootstrap::{BootstrapBranchDescriptor, SharedBootstrapParams, SharedBootstrapPlan},
        ExtractionError,
    };

    use super::{
        build_family_runtime_plan, build_family_runtime_plan_with_registry,
        build_resolved_family_runtime_plan, build_resolved_family_runtime_plan_with_registry,
        build_resolved_runtime_targets, build_resolved_runtime_targets_with_registry,
        canonicalize_shared_route_protocol, coalesce_initialized_accounts_requests,
        default_family_runtime_registry, detect_family_runtimes,
        detect_family_runtimes_with_registry, family_extractor_configs,
        select_resolved_runtime_target, standalone_protocol_systems, FamilyMemberSpec,
        FamilyRuntimeRegistry, FamilyRuntimeSpec, ResolvedInitializedAccountsRequest,
        ResolvedRuntimeTarget, ResolvedRuntimeTargetSelector, ResolvedStandaloneRuntime,
        SharedBootstrapMemberRuntime, SharedBootstrapParamsParser,
    };

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

    fn with_uniswap_family(config: ExtractorConfig, shared_spkg: &str) -> ExtractorConfig {
        config.with_family_runtime(Some(FamilyRuntimeConfig {
            family: "uniswap".to_string(),
            shared_spkg: Some(shared_spkg.to_string()),
            shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
            durability_scope: None,
        }))
    }

    #[test]
    fn does_not_detect_uniswap_family_runtime_without_explicit_opt_in() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_uniswap_family(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(
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
                with_uniswap_family(make_config("uniswap_v2", "/tmp/v2-only.spkg"), "/tmp/a.spkg"),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(make_config("uniswap_v3", "/tmp/v3-only.spkg"), "/tmp/b.spkg"),
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
                with_uniswap_family(make_config("uniswap_v2", "/tmp/v2-only.spkg"), "/tmp/a.spkg"),
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
                    shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
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
                with_uniswap_family(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(
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
                with_uniswap_family(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(
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
                with_uniswap_family(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(
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
                with_uniswap_family(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(
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
    fn resolved_runtime_target_derives_family_substreams_execution_request() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_uniswap_family(
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
                with_uniswap_family(
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

        let execution = family_target
            .substreams_execution_request()
            .expect("family execution request derives");

        assert_eq!(
            execution.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(execution.module, "map_uniswap_family_protocol_changes");
        assert_eq!(execution.start_block, 43);
        assert_eq!(execution.stop_block, 120);
        assert_eq!(execution.extractor_id, "ethereum:uniswap_family");
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
                with_uniswap_family(
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
                with_uniswap_family(
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

        let execution = family_target
            .substreams_execution_request_with_start_block(88)
            .expect("family execution request derives with explicit start block");

        assert_eq!(
            execution.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(execution.module, "map_uniswap_family_protocol_changes");
        assert_eq!(execution.start_block, 88);
        assert_eq!(execution.stop_block, 120);
        assert_eq!(execution.extractor_id, "ethereum:uniswap_family");
        assert_eq!(
            execution.params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
    }

    #[test]
    fn select_resolved_runtime_target_selects_family_target_by_family_name() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_uniswap_family(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(
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
        let selected = select_resolved_runtime_target(
            targets,
            ResolvedRuntimeTargetSelector::Family("uniswap"),
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
    fn select_resolved_runtime_target_selects_standalone_target_by_protocol_system() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_uniswap_family(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(
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
        let selected = select_resolved_runtime_target(
            targets,
            ResolvedRuntimeTargetSelector::StandaloneProtocolSystem("curve"),
        )
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
    fn resolved_runtime_target_derives_initialized_accounts_requests_for_family_members() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_uniswap_family(
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
                with_uniswap_family(
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
                with_uniswap_family(
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
                with_uniswap_family(
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

        let family_v2 = with_uniswap_family(
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
        let family_v3 = with_uniswap_family(
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

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");
        let requests = coalesce_initialized_accounts_requests(targets.iter());

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
    fn resolved_runtime_plan_precomputes_family_execution_settings() {
        let v2 = with_uniswap_family(
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

        let v3 = with_uniswap_family(
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

        assert_eq!(
            family.execution.shared_stream.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(family.execution.shared_stream.module, "map_uniswap_family_protocol_changes");
        assert_eq!(
            family
                .execution
                .shared_stream
                .extractor_id,
            "ethereum:uniswap_family"
        );
        assert_eq!(
            family
                .execution
                .shared_bootstrap_branches
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
        let mut v2 = with_uniswap_family(
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
        let v3 = with_uniswap_family(
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
        let mut v2 = with_uniswap_family(
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
        let v3 = with_uniswap_family(
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
        let v2 = with_uniswap_family(
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
        let v3 = with_uniswap_family(
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
        let mut v2 = with_uniswap_family(
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v2.substreams_params =
            HashMap::from([("map_pool_events".to_string(), "factory=0x01".to_string())]);

        let mut v3 = with_uniswap_family(
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
        let v2 = with_uniswap_family(
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
        let v3 = with_uniswap_family(
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
        let family = default_family_runtime_registry()
            .detected_family_runtime(
                "uniswap",
                Chain::Base,
                "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg",
            )
            .expect("registered uniswap family runtime");

        assert_eq!(family.stream_extractor_id(), "base:uniswap_family");
    }

    #[test]
    fn durability_scope_uses_detected_family_name() {
        let family = default_family_runtime_registry()
            .detected_family_runtime(
                "uniswap",
                Chain::Base,
                "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg",
            )
            .expect("registered uniswap family runtime");

        assert_eq!(family.durability_scope(), "family::uniswap");
    }

    #[test]
    fn registry_resolves_shared_bootstrap_plan_family_name() {
        let registry = default_family_runtime_registry();
        let v2 = with_uniswap_family(make_config("uniswap_v2", "/tmp/v2-only.spkg"), "/tmp/a.spkg");
        let v3 = with_uniswap_family(make_config("uniswap_v3", "/tmp/v3-only.spkg"), "/tmp/a.spkg");
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
                        durability_scope: None,
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
                        durability_scope: None,
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
        assert_eq!(detected[0].output_module, "map_future_swap_family_protocol_changes");
        assert_eq!(detected[0].shared_stream_name, "future_swap_family");
        assert_eq!(detected[0].durability_scope, "family::future_swap_runtime");
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
                with_uniswap_family(make_config("uniswap_v2", "/tmp/v2-only.spkg"), shared_spkg),
            ),
            (
                "uniswap_v3".to_string(),
                with_uniswap_family(make_config("uniswap_v3", "/tmp/v3-only.spkg"), shared_spkg),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");

        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].shared_spkg, shared_spkg);
        assert_eq!(detected[0].output_module, "map_uniswap_family_protocol_changes");
    }

    #[test]
    fn rejects_partially_configured_explicit_family_runtime() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_uniswap_family(make_config("uniswap_v2", "/tmp/v2-only.spkg"), shared_spkg),
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
            with_uniswap_family(make_config("uniswap_v2", "/tmp/v2-only.spkg"), shared_spkg),
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
                with_uniswap_family(
                    make_config("uniswap_v2_indexer", "/tmp/v2-only.spkg")
                        .with_protocol_system("uniswap_v2"),
                    shared_spkg,
                ),
            ),
            (
                "uniswap_v3_primary".to_string(),
                with_uniswap_family(
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
    fn registry_exposes_output_module_for_family_and_protocol_system() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry.output_module_for_family("uniswap"),
            Some("map_uniswap_family_protocol_changes")
        );
        assert_eq!(
            registry.output_module_for_protocol_system("uniswap_v2"),
            Some("map_uniswap_family_protocol_changes")
        );
        assert_eq!(
            registry.output_module_for_protocol_system("uniswap_v3"),
            Some("map_uniswap_family_protocol_changes")
        );
        assert_eq!(registry.output_module_for_family("curve"), None);
        assert_eq!(registry.output_module_for_protocol_system("curve"), None);
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
        let family = default_family_runtime_registry()
            .detected_family_runtime("uniswap", Chain::Ethereum, "/tmp/test.spkg")
            .expect("registered uniswap family runtime");

        assert_eq!(family.family_name, "uniswap");
        assert_eq!(
            family.member_protocol_systems,
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(family.shared_spkg, "/tmp/test.spkg");
        assert_eq!(family.output_module, "map_uniswap_family_protocol_changes");
        assert_eq!(family.shared_stream_name, "uniswap_family");
        assert_eq!(family.durability_scope, "family::uniswap");
    }

    #[test]
    fn registry_exposes_shared_stream_identity_for_family() {
        let registry = default_family_runtime_registry();

        assert_eq!(registry.shared_stream_name_for_family("uniswap"), Some("uniswap_family"));
        assert_eq!(registry.durability_scope_for_family("uniswap"), Some("family::uniswap"));
        assert_eq!(registry.shared_stream_name_for_family("curve"), None);
        assert_eq!(registry.durability_scope_for_family("curve"), None);

        let identity = registry
            .shared_stream_identity_for_family(Chain::Ethereum, "uniswap")
            .expect("uniswap family stream identity");
        assert_eq!(identity.output_module, "map_uniswap_family_protocol_changes");
        assert_eq!(identity.shared_stream_name, "uniswap_family");
        assert_eq!(identity.extractor_id, "ethereum:uniswap_family");
        assert_eq!(identity.durability_scope, "family::uniswap");
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
                Some("map_uniswap_family_protocol_changes".to_string()),
                None,
            )
            .expect("family runtime defaults should resolve");

        assert_eq!(resolved.family, "uniswap");
        assert_eq!(
            resolved.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(resolved.shared_module.as_deref(), Some("map_uniswap_family_protocol_changes"));
        assert_eq!(resolved.durability_scope.as_deref(), Some("family::uniswap"));
    }

    #[test]
    fn registry_resolves_family_runtime_config_with_registry_output_module_default() {
        let registry = default_family_runtime_registry();

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
        assert_eq!(resolved.shared_module.as_deref(), Some("map_uniswap_family_protocol_changes"));
        assert_eq!(resolved.durability_scope.as_deref(), Some("family::uniswap"));
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
        const PARTIAL_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "partial_swap",
            members: &[
                FamilyMemberSpec {
                    protocol_system: "partial_v1",
                    shared_route_protocols: &["partialv1"],
                    shared_bootstrap: Some(SharedBootstrapMemberRuntime {
                        strategy: BootstrapStrategy::UniswapV2Rpc,
                        params_parser: SharedBootstrapParamsParser::PoolList,
                        materialize_branch: materialize_uniswap_v2_branch,
                    }),
                },
                FamilyMemberSpec {
                    protocol_system: "partial_v2",
                    shared_route_protocols: &["partialv2"],
                    shared_bootstrap: None,
                },
            ],
            output_module: "map_partial_swap_family_protocol_changes",
            shared_stream_name: "partial_swap_family",
            durability_scope: "family::partial_swap",
            shared_bootstrap_runtime: None,
        };
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
        const PARTIAL_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec {
            family_name: "partial_swap",
            members: &[
                FamilyMemberSpec {
                    protocol_system: "partial_v1",
                    shared_route_protocols: &["partialv1"],
                    shared_bootstrap: Some(SharedBootstrapMemberRuntime {
                        strategy: BootstrapStrategy::UniswapV2Rpc,
                        params_parser: SharedBootstrapParamsParser::PoolList,
                        materialize_branch: materialize_uniswap_v2_branch,
                    }),
                },
                FamilyMemberSpec {
                    protocol_system: "partial_v2",
                    shared_route_protocols: &["partialv2"],
                    shared_bootstrap: None,
                },
            ],
            output_module: "map_partial_swap_family_protocol_changes",
            shared_stream_name: "partial_swap_family",
            durability_scope: "family::partial_swap",
            shared_bootstrap_runtime: None,
        };
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
