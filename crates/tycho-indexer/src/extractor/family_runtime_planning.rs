#[cfg(test)]
use crate::extractor::family_runtime_types::FamilyRuntimeBuildPlan;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use crate::extractor::{
        extractor_config::{
            BootstrapConfig, BootstrapStrategy, ExtractorConfig, ProtocolTypeConfig,
        },
        family_dispatch::FamilyBranchSpec,
        family_registry::default_family_runtime_registry,
        family_runtime_metadata::{FamilyRuntimeConfig, ResolvedSharedFamilyStream},
        protocol_message_registry::{
            AuxiliaryProtocolMessageBuildFuture, AuxiliaryProtocolMessageContext,
            AuxiliaryProtocolMessageDecoder,
        },
        runtime_target_planning::{ResolvedRuntimeTarget, ResolvedStandaloneRuntime},
        ExtractionError,
    };
    use crate::testing::family_output_module_for_tests;

    use super::{
        FamilyRuntimeBuildPlan,
    };
    use crate::extractor::family_runtime_resolution::{
        family_extractor_configs, resolve_resolved_family_execution_config,
        resolve_resolved_family_shared_runtime,
    };
    use crate::extractor::family_runtime_detection::{
        detect_family_runtimes, detect_family_runtimes_with_registry,
    };
    use crate::extractor::family_runtime::{
        build_family_runtime_plan, build_family_runtime_plan_via_registry,
        build_resolved_family_runtime_plan, build_resolved_family_runtime_plan_via_registry,
        resolve_runtime_targets as build_resolved_runtime_targets,
        resolve_runtime_targets_with_registry as build_resolved_runtime_targets_with_registry,
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
        config.with_family_runtime(Some(FamilyRuntimeConfig::from_resolved_shared_stream(
            family_name,
            family_shared_stream(chain, family_name, shared_spkg),
        )))
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

    fn build_future_family_events<'a>(
        _context: &'a dyn AuxiliaryProtocolMessageContext,
        _value: &'a [u8],
        _finalized_block_height: u64,
        _partial_block_index: Option<u32>,
    ) -> AuxiliaryProtocolMessageBuildFuture<'a> {
        Box::pin(async {
            Err(ExtractionError::Unknown("test-only decoder should not run".to_string()))
        })
    }

    #[test]
    fn custom_registry_detects_future_family_without_runner_changes() {
        const FUTURE_AUXILIARY_PROTOCOL_MESSAGE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
            &[AuxiliaryProtocolMessageDecoder {
                protocol_system: "future_v1",
                type_url_suffix: "FutureEvents",
                build_block_changes: build_future_family_events,
            }];
        let registry =
            crate::extractor::test_support::future_family_runtime_registry_with_auxiliary_decoders_and_explicit_progress_owner_for_tests(
                &["future_v1", "future_v2"],
                "future_v1",
                "family::future_swap_runtime",
                FUTURE_AUXILIARY_PROTOCOL_MESSAGE_DECODERS,
            );
        let extractors = HashMap::from([
            (
                "future_v1".to_string(),
                make_config("future_v1", "/tmp/future-v1-only.spkg").with_family_runtime(Some(
                    crate::extractor::test_support::future_family_runtime_config_for_tests(
                        "protocols/substreams/future-swap-combined/test.spkg",
                        "family::future_swap_runtime",
                    ),
                )),
            ),
            (
                "future_v2".to_string(),
                make_config("future_v2", "/tmp/future-v2-only.spkg").with_family_runtime(Some(
                    crate::extractor::test_support::future_family_runtime_config_for_tests(
                        "protocols/substreams/future-swap-combined/test.spkg",
                        "family::future_swap_runtime",
                    ),
                )),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let detected = detect_family_runtimes_with_registry(&extractors, registry)
            .expect("custom family detection succeeds");
        let plan = build_family_runtime_plan_via_registry(&extractors, registry)
            .expect("custom family plan builds");
        let resolved = build_resolved_family_runtime_plan_via_registry(&extractors, registry)
            .expect("custom resolved plan builds");

        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].family_name(), "future_swap");
        assert_eq!(
            detected[0].member_protocol_systems(),
            vec!["future_v1".to_string(), "future_v2".to_string()]
        );
        let detected_shared_stream = detected[0]
            .resolved_shared_stream_with_registry(registry)
            .expect("custom family shared stream resolves");
        assert_eq!(detected[0].output_module(), "map_future_swap_family_protocol_changes");
        assert_eq!(detected_shared_stream.shared_stream_name, "future_swap_family");
        assert_eq!(
            detected_shared_stream.durability_scope,
            "family::future_swap_runtime"
        );
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
                if family.family_name() == "future_swap" && family.extractor_configs.len() == 2
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
                .auxiliary_runtime_hooks_by_protocol_system()
                .get("future_v1")
                .map(|hooks| hooks.message_decoders.len()),
            Some(1)
        );
    }

    #[test]
    fn test_family_runtime_helper_reuses_production_family_resolution() {
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
        let family = detect_family_runtimes(&extractors)
            .expect("family detection succeeds")
            .into_iter()
            .next()
            .expect("uniswap family should be detected");
        let extractor_configs =
            family_extractor_configs(&family, &extractors).expect("family configs resolve");

        let execution_from_production =
            resolve_resolved_family_execution_config(&family, &extractor_configs, registry)
                .expect("production family execution config resolves");
        let shared_runtime_from_production = resolve_resolved_family_shared_runtime(
            &family,
            &execution_from_production.runtime_contract,
            &extractor_configs,
            registry,
        )
        .expect("production family shared runtime resolves");
        let runtime_from_test_helper = crate::extractor::family_runtime::resolved_family_runtime_from_extractor_configs_for_tests(
            &extractor_configs,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        )
        .expect("test helper family runtime resolves");

        assert_eq!(
            runtime_from_test_helper.branch_specs(),
            execution_from_production.runtime_contract.branch_specs(),
            "test helper should reuse production branch routing"
        );
        assert_eq!(
            runtime_from_test_helper.merged_substreams_params(),
            shared_runtime_from_production.merged_substreams_params().as_map(),
            "test helper should reuse production shared substreams params"
        );
        assert_eq!(
            runtime_from_test_helper.stop_block(),
            shared_runtime_from_production.stop_block(),
            "test helper should reuse production stop block resolution"
        );
        assert_eq!(
            runtime_from_test_helper.configured_start_block(),
            shared_runtime_from_production.configured_start_block,
            "test helper should reuse production start block resolution"
        );
        assert_eq!(
            runtime_from_test_helper.shared_bootstrap_plan(),
            shared_runtime_from_production
                .shared_bootstrap_runtime
                .as_ref()
                .map(|runtime| &runtime.plan),
            "test helper should reuse production shared bootstrap planning"
        );
        assert_eq!(
            runtime_from_test_helper.shared_extractor_id(),
            execution_from_production.runtime_contract.shared_extractor_id(),
            "test helper should reuse production shared extractor identity"
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
                Some(BootstrapConfig {
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
                Some(BootstrapConfig {
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
            family.shared_spkg(),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(family.output_module(), expected_shared_stream.module);
        assert_eq!(
            family.runtime_contract().resolved_shared_stream(),
            &expected_shared_stream
        );
        assert_eq!(
            family.runtime_contract().shared_stream_name(),
            expected_shared_stream.shared_stream_name
        );
        assert_eq!(family.shared_extractor_id(), expected_shared_stream.extractor_id);
        assert_eq!(
            family
                .shared_bootstrap_execution()
                .branch_materializers
                .len(),
            2
        );
        assert_eq!(family.stop_block(), 120);
        assert_eq!(family.configured_start_block(), 43);
        assert_eq!(
            family.merged_substreams_params(),
            &HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
        assert_eq!(family.shared_progress_owner_protocol_system(), "uniswap_v2");
        assert_eq!(
            family.runtime_contract().shared_progress_owner_protocol_system(),
            "uniswap_v2"
        );
        let bootstrap_plan = family
            .shared_bootstrap_plan()
            .expect("family execution should precompute shared bootstrap plan");
        assert_eq!(bootstrap_plan.bootstrap_block, 42);
        assert_eq!(bootstrap_plan.branches.len(), 2);
        assert_eq!(
            bootstrap_plan.branch_protocol_systems(),
            FamilyBranchSpec::protocol_system_set(family.branch_specs().iter())
        );
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
        v2.bootstrap = Some(BootstrapConfig {
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
        v2.bootstrap = Some(BootstrapConfig {
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
        let standalone =
            FamilyRuntimeBuildPlan::from_detected_families(&extractors, detected)
                .standalone_protocol_systems;

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
        assert_eq!(plan.families[0].family_name(), "uniswap");
        assert_eq!(plan.families[0].chain(), Chain::Ethereum);
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
        assert_eq!(resolved.families[0].family_name(), "uniswap");
        assert_eq!(resolved.families[0].chain(), Chain::Ethereum);
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
                if family.family_name() == "uniswap" && family.extractor_configs.len() == 2
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
    fn stream_extractor_id_uses_detected_chain() {
        let spkg = "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg";
        let registry = default_family_runtime_registry();
        let family = registry
            .detected_family_runtime("uniswap", Chain::Base, spkg)
            .expect("registered uniswap family runtime");
        let expected_shared_stream = base_uniswap_shared_stream(spkg);

        let resolved = family
            .resolved_shared_stream_with_registry(registry)
            .expect("registered uniswap shared stream");
        assert_eq!(resolved.extractor_id, expected_shared_stream.extractor_id);
    }

    #[test]
    fn durability_scope_uses_detected_family_name() {
        let spkg = "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg";
        let registry = default_family_runtime_registry();
        let family = registry
            .detected_family_runtime("uniswap", Chain::Base, spkg)
            .expect("registered uniswap family runtime");
        let expected_shared_stream = base_uniswap_shared_stream(spkg);

        let resolved = family
            .resolved_shared_stream_with_registry(registry)
            .expect("registered uniswap shared stream");
        assert_eq!(resolved.durability_scope, expected_shared_stream.durability_scope);
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

        assert_eq!(family.family_name(), "uniswap");
        assert_eq!(
            family.member_protocol_systems(),
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(family.shared_spkg(), "/tmp/test.spkg");
        assert_eq!(family.output_module(), shared_stream.module);
        let resolved = family
            .resolved_shared_stream_with_registry(registry)
            .expect("registered uniswap shared stream");
        assert_eq!(
            resolved.shared_stream_name,
            registry
                .shared_stream_name_for_family("uniswap")
                .expect("uniswap shared stream name")
        );
        assert_eq!(resolved.durability_scope, shared_stream.durability_scope);
    }

    #[test]
    fn resolved_family_runtime_helper_builds_full_runtime_with_requested_shared_spkg() {
        let shared_spkg = "/tmp/uniswap-family-runtime-test.spkg";
        let v2 = make_config("uniswap_v2", "/tmp/v2-only.spkg");
        let v3 = make_config("uniswap_v3", "/tmp/v3-only.spkg");

        let runtime =
            crate::extractor::family_runtime::resolved_family_runtime_from_extractor_configs_for_tests(&[&v2, &v3], shared_spkg)
                .expect("test configs should resolve full family runtime");

        assert_eq!(runtime.family_name(), "uniswap");
        assert_eq!(runtime.shared_spkg(), shared_spkg);
        assert_eq!(runtime.output_module(), family_output_module_for_tests("uniswap"));
        assert_eq!(
            runtime.shared_extractor_id(),
            runtime.runtime_contract().shared_extractor_id()
        );
        assert_eq!(runtime.extractor_configs.len(), 2);
        assert_eq!(runtime.branch_specs().len(), 2);
    }

}
