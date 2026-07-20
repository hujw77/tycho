use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use futures03::future::try_join_all;
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    extractor_config::{
        extractor_config_by_protocol_system, BootstrapConfig, BootstrapStrategy, ExtractorConfig,
    },
    family_registry::{FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec},
    family_runtime_metadata::normalized_shared_route_protocols_for_member,
    models::BlockChanges,
    shared_bootstrap::{
        materialize_plan_by_branch_materializers, parse_and_validate_bootstrap_params,
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

pub type MaterializeBootstrapPlanFn = for<'a> fn(
    &'a EthereumRpcClient,
    &'a SharedBootstrapPlan,
    &'a HashMap<String, MaterializeBootstrapBranchFn>,
) -> Pin<
    Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>,
>;

fn generic_shared_bootstrap_plan_materializer<'a>(
    rpc: &'a EthereumRpcClient,
    plan: &'a SharedBootstrapPlan,
    branch_materializers: &'a HashMap<String, MaterializeBootstrapBranchFn>,
) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
    Box::pin(async move { materialize_plan_by_branch_materializers(rpc, plan, branch_materializers).await })
}

pub(crate) fn parallel_shared_bootstrap_plan_materializer<'a>(
    rpc: &'a EthereumRpcClient,
    plan: &'a SharedBootstrapPlan,
    branch_materializers: &'a HashMap<String, MaterializeBootstrapBranchFn>,
) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
    Box::pin(async move {
        let mut branch_futures = Vec::with_capacity(plan.branches.len());

        for branch in &plan.branches {
            let materialize_branch =
                branch_materializers.get(&branch.protocol_system).ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "shared bootstrap plan is missing materializer for protocol system `{}`",
                        branch.protocol_system
                    ))
                })?;
            branch_futures.push((materialize_branch)(rpc, branch));
        }

        let branch_changes = try_join_all(branch_futures).await?;
        let mut merged = None;
        for branch_change in branch_changes {
            merged = Some(match merged {
                Some(existing) => crate::extractor::shared_bootstrap::merge_family_block_changes(
                    existing,
                    branch_change,
                )?,
                None => branch_change,
            });
        }

        merged.ok_or_else(|| {
            ExtractionError::Setup("shared bootstrap plan contained no branches".to_string())
        })
    })
}

pub(crate) const fn default_shared_bootstrap_plan_materializer() -> MaterializeBootstrapPlanFn {
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

#[derive(Clone, Debug)]
pub struct ResolvedSharedBootstrapExecution {
    pub plan_materializer: MaterializeBootstrapPlanFn,
    pub branch_materializers: HashMap<String, MaterializeBootstrapBranchFn>,
}

#[derive(Clone, Debug)]
pub struct ResolvedSharedBootstrapRuntime {
    pub plan: SharedBootstrapPlan,
    pub execution: ResolvedSharedBootstrapExecution,
}

impl ResolvedSharedBootstrapExecution {
    pub async fn materialize_plan(
        &self,
        rpc: &EthereumRpcClient,
        plan: &SharedBootstrapPlan,
    ) -> Result<BlockChanges, ExtractionError> {
        (self.plan_materializer)(rpc, plan, &self.branch_materializers).await
    }
}

impl<'a> FamilyRuntimeRegistry<'a> {
    pub fn validate_shared_bootstrap_support_for_family(
        &self,
        family_name: &str,
    ) -> Result<&'a FamilyRuntimeSpec, ExtractionError> {
        let spec = self.require_family_spec(family_name, "family bootstrap defaults for")?;
        for member in spec.members() {
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

    pub fn resolve_shared_bootstrap_execution(
        &self,
        family_name: &str,
    ) -> Result<ResolvedSharedBootstrapExecution, ExtractionError> {
        let spec = self.validate_shared_bootstrap_support_for_family(family_name)?;
        Ok(ResolvedSharedBootstrapExecution {
            plan_materializer: spec
                .shared_bootstrap_runtime()
                .map(|runtime| runtime.materialize_plan)
                .unwrap_or_else(default_shared_bootstrap_plan_materializer),
            branch_materializers: self.resolve_shared_bootstrap_branch_materializers(family_name)?,
        })
    }

    #[cfg(test)]
    pub fn resolve_shared_bootstrap_execution_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Result<ResolvedSharedBootstrapExecution, ExtractionError> {
        let defaults = self.require_registered_protocol_system_defaults(
            protocol_system,
            "shared bootstrap execution",
        )?;
        self.resolve_shared_bootstrap_execution(defaults.family_name())
    }

    pub fn resolve_shared_bootstrap_execution_for_plan(
        &self,
        plan: &SharedBootstrapPlan,
    ) -> Result<ResolvedSharedBootstrapExecution, ExtractionError> {
        self.resolve_shared_bootstrap_execution(&plan.family_name)
    }

    pub fn resolve_shared_bootstrap_plan_family_name(
        &self,
        configs: &[(&ExtractorConfig, &BootstrapConfig)],
    ) -> Result<String, ExtractionError> {
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
                inferred_protocol_systems
                    .iter()
                    .copied(),
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

        let family_name = expected_family.ok_or_else(|| {
            ExtractionError::Setup("shared bootstrap plan contained no extractors".to_string())
        })?;

        self.validate_family_member_defaults_for_family(
            &family_name,
            configs
                .iter()
                .map(|(config, _)| config.protocol_system()),
        )?;

        Ok(family_name)
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

    pub fn build_shared_bootstrap_plan_for_family(
        &self,
        family_name: &str,
        extractors: &HashMap<String, ExtractorConfig>,
    ) -> Result<SharedBootstrapPlan, ExtractionError> {
        let spec = self.require_family_spec(family_name, "shared bootstrap family")?;
        let mut branch_configs = Vec::new();

        for member in spec.members() {
            let protocol_system = member.protocol_system;
            let extractor = extractor_config_by_protocol_system(extractors, protocol_system)?
                .ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "missing extractor config for family `{family_name}` member `{protocol_system}`"
                    ))
                })?;
            let bootstrap = extractor.bootstrap.as_ref().ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "missing bootstrap config for family `{family_name}` member `{protocol_system}`"
                ))
            })?;
            branch_configs.push((extractor, bootstrap));
        }

        self.build_shared_bootstrap_plan(branch_configs)
    }

    pub fn build_shared_bootstrap_runtime<'b>(
        &self,
        configs: impl IntoIterator<Item = (&'b ExtractorConfig, &'b BootstrapConfig)>,
    ) -> Result<ResolvedSharedBootstrapRuntime, ExtractionError> {
        let plan = self.build_shared_bootstrap_plan(configs)?;
        let execution = self.resolve_shared_bootstrap_execution_for_plan(&plan)?;

        Ok(ResolvedSharedBootstrapRuntime { plan, execution })
    }

    pub fn resolve_optional_shared_bootstrap_runtime<'b>(
        &self,
        configs: impl IntoIterator<Item = &'b ExtractorConfig>,
    ) -> Result<Option<ResolvedSharedBootstrapRuntime>, ExtractionError> {
        let branch_configs = configs
            .into_iter()
            .filter_map(|config| config.bootstrap.as_ref().map(|bootstrap| (config, bootstrap)))
            .collect::<Vec<_>>();

        if branch_configs.is_empty() {
            Ok(None)
        } else {
            self.build_shared_bootstrap_runtime(branch_configs).map(Some)
        }
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

    pub fn resolve_shared_bootstrap_branch_materializers(
        &self,
        family_name: &str,
    ) -> Result<HashMap<String, MaterializeBootstrapBranchFn>, ExtractionError> {
        let spec = self.require_family_spec(family_name, "shared bootstrap branch runtime for")?;
        let runtimes = spec
            .members()
            .iter()
            .filter_map(|member| {
                member
                    .shared_bootstrap
                    .map(|bootstrap| {
                        (
                            member.protocol_system.to_string(),
                            bootstrap.materialize_branch,
                        )
                    })
            })
            .collect::<HashMap<_, _>>();
        Ok(runtimes)
    }

    pub fn validate(&self) -> Result<(), ExtractionError> {
        let mut seen_protocol_systems = HashMap::new();
        let mut seen_route_protocols = HashMap::new();

        for spec in self.specs() {
            if !spec
                .members()
                .iter()
                .any(|member| {
                    member.protocol_system == spec.shared_progress_owner_protocol_system()
                })
            {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` declares shared progress owner `{}` that is not a declared member",
                    spec.family_name(),
                    spec.shared_progress_owner_protocol_system()
                )));
            }

            for member in spec.members() {
                if let Some(existing_family) =
                    seen_protocol_systems.insert(member.protocol_system, spec.family_name())
                {
                    return Err(ExtractionError::Setup(format!(
                        "family runtime registry assigns protocol system `{}` to both `{existing_family}` and `{}`",
                        member.protocol_system, spec.family_name()
                    )));
                }

                for normalized in normalized_shared_route_protocols_for_member(member) {
                    if normalized.is_empty() {
                        return Err(ExtractionError::Setup(format!(
                            "family `{}` member `{}` declares an empty shared route protocol alias",
                            spec.family_name(),
                            member.protocol_system
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
        let defaults = self.require_registered_protocol_system_defaults(
            protocol_system,
            "shared bootstrap registry",
        )?;

        match defaults
            .shared_bootstrap()
            .map(|bootstrap| bootstrap.strategy)
        {
            Some(member_strategy) if member_strategy == strategy => Ok(defaults.member_spec()),
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use std::{future::Future, pin::Pin};
    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use tycho_common::Bytes;
    use tycho_ethereum::rpc::EthereumRpcClient;

    use crate::extractor::{
        extractor_config::{
            BootstrapConfig, BootstrapStrategy, ExtractorConfig, ProtocolTypeConfig,
        },
        family_registry::{
            default_family_runtime_registry,
            pool_list_bootstrap_member_runtime, shared_family_member_spec,
            shared_family_member_with_bootstrap, FamilyRuntimeRegistry, FamilyRuntimeSpec,
        },
        family_runtime_metadata::FamilyRuntimeConfig,
        models::BlockChanges,
        shared_bootstrap::{BootstrapBranchDescriptor, SharedBootstrapParams, SharedBootstrapPlan},
        ExtractionError,
    };
    use tycho_indexer::canonical_shared_family_runtime_spec_with_explicit_owner;

    use super::SharedBootstrapParamsParser;

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

    fn with_resolved_family_runtime(
        config: ExtractorConfig,
        family_name: &str,
        shared_spkg: &str,
    ) -> ExtractorConfig {
        config.with_family_runtime(Some(FamilyRuntimeConfig::from_resolved_shared_stream(
            family_name,
            default_family_runtime_registry()
                .resolved_shared_stream_for_family(Chain::Ethereum, family_name, shared_spkg)
                .expect("registered family shared stream"),
        )))
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

    fn noop_materialize_branch<'a>(
        _rpc: &'a EthereumRpcClient,
        branch: &'a BootstrapBranchDescriptor,
    ) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(BlockChanges::new(
                branch.extractor_name.clone(),
                branch.chain,
                Default::default(),
                branch.params.bootstrap_block,
                false,
                Vec::new(),
                Vec::new(),
            ))
        })
    }

    #[test]
    fn registry_resolves_shared_bootstrap_plan_family_name() {
        let registry = default_family_runtime_registry();
        let v2 = with_resolved_family_runtime(
            make_config("uniswap_v2", "/tmp/v2-only.spkg"),
            "uniswap",
            "/tmp/a.spkg",
        );
        let v3 = with_resolved_family_runtime(
            make_config("uniswap_v3", "/tmp/v3-only.spkg"),
            "uniswap",
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

        assert_eq!(family_name, "uniswap".to_string());
    }

    #[test]
    fn registry_rejects_shared_bootstrap_plan_family_name_when_explicit_family_member_is_not_registered() {
        let registry = default_family_runtime_registry();
        let curve = with_resolved_family_runtime(
            make_config("curve", "/tmp/curve-only.spkg"),
            "uniswap",
            "/tmp/a.spkg",
        );
        let curve_bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000009999"
                .to_string(),
        };

        let err = registry
            .resolve_shared_bootstrap_plan_family_name(&[(&curve, &curve_bootstrap)])
            .expect_err("non-member explicit family config should be rejected");

        assert!(err.to_string().contains(
            "family_runtime member defaults for `uniswap` cannot be applied to protocol system `curve`"
        ));
    }

    #[test]
    fn registry_builds_shared_bootstrap_plan_for_family_members() {
        let registry = default_family_runtime_registry();
        let v2 = with_resolved_family_runtime(
            make_config("uniswap_v2", "/tmp/v2-only.spkg"),
            "uniswap",
            "/tmp/a.spkg",
        );
        let v3 = with_resolved_family_runtime(
            make_config("uniswap_v3", "/tmp/v3-only.spkg"),
            "uniswap",
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

        assert_eq!(plan.family_name, "uniswap".to_string());
        assert_eq!(plan.bootstrap_block, 42);
        assert_eq!(plan.branches.len(), 2);
        assert_eq!(plan.branches[0].protocol_system, "uniswap_v2");
        assert_eq!(plan.branches[1].protocol_system, "uniswap_v3");
        assert_eq!(
            plan.branch_protocol_systems(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );
    }

    #[test]
    fn registry_builds_shared_bootstrap_plan_for_registered_family() {
        let registry = default_family_runtime_registry();
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
        let mut v2 = with_resolved_family_runtime(
            make_config("uniswap_v2", "/tmp/v2-only.spkg"),
            "uniswap",
            "/tmp/a.spkg",
        );
        v2.bootstrap = Some(v2_bootstrap);
        let mut v3 = with_resolved_family_runtime(
            make_config("uniswap_v3", "/tmp/v3-only.spkg"),
            "uniswap",
            "/tmp/a.spkg",
        );
        v3.bootstrap = Some(v3_bootstrap);

        let plan = registry
            .build_shared_bootstrap_plan_for_family(
                "uniswap",
                &HashMap::from([
                    ("uniswap_v2".to_string(), v2),
                    ("uniswap_v3".to_string(), v3),
                ]),
            )
            .expect("registered family bootstrap plan should build");

        assert_eq!(plan.family_name, "uniswap");
        assert_eq!(plan.bootstrap_block, 42);
        assert_eq!(
            plan.branch_protocol_systems(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );
    }

    #[test]
    fn registry_builds_shared_bootstrap_runtime_for_family_members() {
        let registry = default_family_runtime_registry();
        let v2 = with_resolved_family_runtime(
            make_config("uniswap_v2", "/tmp/v2-only.spkg"),
            "uniswap",
            "/tmp/a.spkg",
        );
        let v3 = with_resolved_family_runtime(
            make_config("uniswap_v3", "/tmp/v3-only.spkg"),
            "uniswap",
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

        let runtime = registry
            .build_shared_bootstrap_runtime([(&v2, &v2_bootstrap), (&v3, &v3_bootstrap)])
            .expect("shared bootstrap runtime should build");

        assert_eq!(runtime.plan.family_name, "uniswap");
        assert_eq!(runtime.plan.bootstrap_block, 42);
        assert_eq!(runtime.execution.branch_materializers.len(), 2);
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
    fn custom_registry_parses_future_family_bootstrap_params() {
        const FUTURE_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "future_swap",
            &[shared_family_member_with_bootstrap(
                "future_v1",
                &["futurev1"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::Custom(parse_future_params),
                |_rpc, _branch| {
                    Box::pin(async {
                        Err(ExtractionError::Setup("not used in this test".to_string()))
                    })
                },
            )],
            None,
            "future_v1",
            durability_scope: "family::future_swap",
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
        const FUTURE_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "future_swap",
            &[shared_family_member_with_bootstrap(
                "future_v1",
                &["futurev1"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::Custom(parse_future_params),
                materialize_future_branch_fallback,
            )],
            None,
            "future_v1",
            durability_scope: "family::future_swap",
        );
        let registry = FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]);
        let rpc = EthereumRpcClient::new("http://localhost:0000").expect("stub rpc client builds");
        let plan = SharedBootstrapPlan {
            family_name: "future_swap".to_string(),
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
            .resolve_shared_bootstrap_execution("future_swap")
            .expect("registry should resolve default bootstrap execution")
            .materialize_plan(&rpc, &plan)
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
    fn registry_resolves_shared_bootstrap_execution_for_protocol_system() {
        let registry = default_family_runtime_registry();

        let execution = registry
            .resolve_shared_bootstrap_execution_for_protocol_system("uniswap_v3")
            .expect("uniswap_v3 shared bootstrap execution");

        assert_eq!(execution.branch_materializers.len(), 2);
        assert!(execution
            .branch_materializers
            .contains_key("uniswap_v2"));
        assert!(execution
            .branch_materializers
            .contains_key("uniswap_v3"));

        let err = registry
            .resolve_shared_bootstrap_execution_for_protocol_system("curve")
            .expect_err("curve should not resolve a shared bootstrap execution");
        assert!(err
            .to_string()
            .contains("could not resolve registered protocol defaults"));
    }

    #[test]
    fn registry_rejects_shared_bootstrap_defaults_for_family_without_full_bootstrap_support() {
        const PARTIAL_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "partial_swap",
            &[
                shared_family_member_spec(
                    "partial_v1",
                    &["partialv1"],
                    Some(pool_list_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        noop_materialize_branch,
                    )),
                ),
                shared_family_member_spec("partial_v2", &["partialv2"], None),
            ],
            None,
            "partial_v1",
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
        const PARTIAL_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "partial_swap",
            &[
                shared_family_member_spec(
                    "partial_v1",
                    &["partialv1"],
                    Some(pool_list_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        noop_materialize_branch,
                    )),
                ),
                shared_family_member_spec("partial_v2", &["partialv2"], None),
            ],
            None,
            "partial_v1",
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
    fn registry_rejects_shared_bootstrap_execution_when_family_membership_exceeds_bootstrap_support() {
        const PARTIAL_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "partial_swap",
            &[
                shared_family_member_spec(
                    "partial_v1",
                    &["partialv1"],
                    Some(pool_list_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        noop_materialize_branch,
                    )),
                ),
                shared_family_member_spec("partial_v2", &["partialv2"], None),
            ],
            None,
            "partial_v1",
        );
        let registry = FamilyRuntimeRegistry::new(&[PARTIAL_FAMILY]);

        let err = registry
            .resolve_shared_bootstrap_execution("partial_swap")
            .expect_err("shared bootstrap execution should reject partial family bootstrap support");

        assert!(err.to_string().contains(
            "family bootstrap defaults for `partial_swap` require every member to declare a shared bootstrap strategy"
        ));
    }

    #[test]
    fn registry_rejects_shared_bootstrap_execution_for_protocol_system_when_family_is_partial() {
        const PARTIAL_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "partial_swap",
            &[
                shared_family_member_spec(
                    "partial_v1",
                    &["partialv1"],
                    Some(pool_list_bootstrap_member_runtime(
                        BootstrapStrategy::UniswapV2Rpc,
                        noop_materialize_branch,
                    )),
                ),
                shared_family_member_spec("partial_v2", &["partialv2"], None),
            ],
            None,
            "partial_v1",
        );
        let registry = FamilyRuntimeRegistry::new(&[PARTIAL_FAMILY]);

        let err = registry
            .resolve_shared_bootstrap_execution_for_protocol_system("partial_v1")
            .expect_err("protocol-scoped shared bootstrap execution should reject partial family support");

        assert!(err.to_string().contains(
            "family bootstrap defaults for `partial_swap` require every member to declare a shared bootstrap strategy"
        ));
    }

    #[test]
    fn registry_rejects_duplicate_member_protocol_systems_across_families() {
        const FAMILY_A: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "family_a",
            &[crate::extractor::family_registry::shared_family_member_spec(
                "shared_protocol",
                &[],
                None,
            )],
            None,
            "shared_protocol",
        );
        const FAMILY_B: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "family_b",
            &[crate::extractor::family_registry::shared_family_member_spec(
                "shared_protocol",
                &[],
                None,
            )],
            None,
            "shared_protocol",
        );
        const SPECS: &[FamilyRuntimeSpec] = &[FAMILY_A, FAMILY_B];
        let registry = FamilyRuntimeRegistry::new(SPECS);

        let err = registry
            .validate()
            .expect_err("duplicate protocol system across families should fail");

        assert!(err.to_string().contains(
            "assigns protocol system `shared_protocol` to both `family_a` and `family_b`"
        ));
    }

    #[test]
    fn registry_rejects_duplicate_normalized_route_aliases() {
        const BROKEN_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "broken_family",
            &[
                crate::extractor::family_registry::shared_family_member_spec(
                    "protocol_a",
                    &["Example-V2"],
                    None,
                ),
                crate::extractor::family_registry::shared_family_member_spec(
                    "protocol_b",
                    &["example_v2"],
                    None,
                ),
            ],
            None,
            "protocol_a",
        );
        let registry = FamilyRuntimeRegistry::new(&[BROKEN_FAMILY]);

        let err = registry
            .validate()
            .expect_err("duplicate normalized route aliases should fail");

        assert!(err
            .to_string()
            .contains("shared route protocol alias `examplev2` is assigned to both `protocol_a` and `protocol_b`"));
    }

    #[test]
    fn registry_rejects_shared_progress_owner_outside_member_set() {
        const BROKEN_FAMILY: FamilyRuntimeSpec = canonical_shared_family_runtime_spec_with_explicit_owner!(
            "broken_family",
            &[crate::extractor::family_registry::shared_family_member_spec(
                "protocol_a",
                &[],
                None,
            )],
            None,
            "protocol_b",
        );
        let registry = FamilyRuntimeRegistry::new(&[BROKEN_FAMILY]);

        let err = registry
            .validate()
            .expect_err("owner outside member set should fail");

        assert!(err.to_string().contains(
            "family `broken_family` declares shared progress owner `protocol_b` that is not a declared member"
        ));
    }
}
