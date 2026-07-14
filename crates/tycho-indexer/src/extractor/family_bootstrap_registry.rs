use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    extractor_config::{BootstrapConfig, BootstrapStrategy, ExtractorConfig},
    family_registry::{FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec},
    family_runtime_metadata::normalized_shared_route_protocols_for_member,
    models::BlockChanges,
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
        Ok(Box::pin(async move {
            execution
                .materialize_plan(rpc, plan)
                .await
        }))
    }

    pub fn resolve_shared_bootstrap_plan_materializer(
        &self,
        family_name: &str,
    ) -> Result<MaterializeBootstrapPlanFn, ExtractionError> {
        let spec =
            self.require_family_spec(family_name, "shared bootstrap plan materializer for")?;
        Ok(spec
            .shared_bootstrap_runtime()
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
        let defaults = self.require_registered_protocol_system_defaults(
            protocol_system,
            "shared bootstrap execution",
        )?;
        self.resolve_shared_bootstrap_execution(defaults.family_name())
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
            .members()
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

        for spec in self.specs() {
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
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};
    use std::{future::Future, pin::Pin};

    use tycho_common::Bytes;
    use tycho_ethereum::rpc::EthereumRpcClient;

    use crate::extractor::{
        extractor_config::{BootstrapConfig, BootstrapStrategy, ExtractorConfig, ProtocolTypeConfig},
        family_registry::{
            canonical_shared_family_runtime_spec, default_family_runtime_registry,
            pool_list_bootstrap_member_runtime, shared_family_member_spec, FamilyMemberSpec,
            shared_family_member_with_bootstrap, shared_family_runtime_spec, FamilyRuntimeRegistry,
            FamilyRuntimeSpec,
        },
        family_runtime_metadata::FamilyRuntimeConfig,
        models::BlockChanges,
        shared_bootstrap::{BootstrapBranchDescriptor, SharedBootstrapParams, SharedBootstrapPlan},
        ExtractionError,
    };

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

    fn with_resolved_uniswap_family_runtime(
        config: ExtractorConfig,
        shared_spkg: &str,
    ) -> ExtractorConfig {
        let shared_stream = default_family_runtime_registry()
            .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", shared_spkg)
            .expect("registered uniswap shared stream");
        config.with_family_runtime(Some(FamilyRuntimeConfig {
            family: "uniswap".to_string(),
            shared_spkg: Some(shared_spkg.to_string()),
            shared_module: Some(shared_stream.module),
            durability_scope: Some(shared_stream.durability_scope),
        }))
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
            .resolve_shared_bootstrap_plan_family_name(&[(&v2, &v2_bootstrap), (&v3, &v3_bootstrap)])
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
    fn custom_registry_parses_future_family_bootstrap_params() {
        const FUTURE_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
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
            &[shared_family_member_with_bootstrap(
                "future_v1",
                &["futurev1"],
                BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::Custom(parse_future_params),
                materialize_future_branch_fallback,
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
            .contains("could not resolve registered protocol defaults"));
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
                        noop_materialize_branch,
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
                        noop_materialize_branch,
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
    fn registry_rejects_duplicate_member_protocol_systems_across_families() {
        const FAMILY_A: FamilyRuntimeSpec = FamilyRuntimeSpec::new(
            "family_a",
            &[FamilyMemberSpec {
                protocol_system: "shared_protocol",
                shared_route_protocols: &[],
                shared_bootstrap: None,
            }],
            "map_family_a",
            "family_a_stream",
            "family::family_a",
            None,
            &[],
        );
        const FAMILY_B: FamilyRuntimeSpec = FamilyRuntimeSpec::new(
            "family_b",
            &[FamilyMemberSpec {
                protocol_system: "shared_protocol",
                shared_route_protocols: &[],
                shared_bootstrap: None,
            }],
            "map_family_b",
            "family_b_stream",
            "family::family_b",
            None,
            &[],
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
        const BROKEN_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec::new(
            "broken_family",
            &[
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
            "map_broken_family",
            "broken_family_stream",
            "family::broken_family",
            None,
            &[],
        );
        let registry = FamilyRuntimeRegistry::new(&[BROKEN_FAMILY]);

        let err = registry
            .validate()
            .expect_err("duplicate normalized route aliases should fail");

        assert!(err
            .to_string()
            .contains("shared route protocol alias `examplev2` is assigned to both `protocol_a` and `protocol_b`"));
    }
}
