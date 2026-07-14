use super::*;
use crate::extractor::family_runtime_metadata::ResolvedSharedFamilyStream;

#[test]
fn test_resolved_family_execution_config_rejects_partial_shared_bootstrap_config() {
    let configs = [
        ExtractorConfig {
            name: "uniswap_v2".to_owned(),
            protocol_system: "uniswap_v2".to_string(),
            start_block: 42,
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v2_pool".to_string(),
                FinancialType::Swap,
            )],
            bootstrap: Some(BootstrapConfig {
                strategy: BootstrapStrategy::UniswapV2Rpc,
                start_block: 42,
                params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                    .to_owned(),
            }),
            ..Default::default()
        },
        ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            protocol_system: "uniswap_v3".to_string(),
            start_block: 43,
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            bootstrap: None,
            ..Default::default()
        },
    ];
    let config_refs = configs.iter().collect::<Vec<_>>();
    let err = resolved_family_execution_config_from_extractor_configs_for_tests(&config_refs)
        .expect_err("partial shared bootstrap config should fail");

    let err_text = err.to_string();
    assert!(
        err_text.contains("shared bootstrap configuration consistency")
            || err_text.contains("bootstrapped branches"),
        "unexpected partial-bootstrap error: {err_text}"
    );
}

#[test]
fn test_resolved_family_execution_config_derives_shared_branch_and_stream_settings() {
    let v2 = ExtractorConfig {
        name: "uniswap_v2".to_owned(),
        protocol_system: "uniswap_v2".to_string(),
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([(
            "map_pool_events".to_string(),
            "factory=0x01".to_string(),
        )]),
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3".to_owned(),
        protocol_system: "uniswap_v3".to_string(),
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
        ..Default::default()
    };

    let execution = resolved_family_execution_config_from_extractor_configs_for_tests(&[&v2, &v3])
        .expect("family execution config derives");
    let shared_stream = uniswap_shared_stream_for_tests("/tmp/shared-family.spkg");
    let expected_shared_stream = ResolvedSharedFamilyStream {
        spkg: "/tmp/shared-family.spkg".to_string(),
        ..shared_stream
    };

    assert_eq!(execution.stop_block, 120);
    assert_eq!(execution.configured_start_block, 0);
    assert!(execution.bootstrap_plan.is_none());
    assert_eq!(execution.shared_stream.module, expected_shared_stream.module);
    assert_eq!(execution.shared_stream.extractor_id, expected_shared_stream.extractor_id);
    assert_eq!(execution.shared_stream.durability_scope, expected_shared_stream.durability_scope);
    assert_eq!(
        FamilyBranchSpec::protocol_system_set(execution.branch_specs.iter()),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    assert_eq!(execution.branch_specs.len(), 2);
    assert_eq!(
        execution.merged_substreams_params,
        HashMap::from([
            ("map_pool_events".to_string(), "factory=0x01".to_string()),
            ("map_events".to_string(), "factory=0x02".to_string()),
        ])
    );
}

#[test]
fn test_resolved_family_execution_config_uses_protocol_systems_for_aliased_members() {
    let v2 = ExtractorConfig {
        name: "uniswap_v2_alias".to_owned(),
        protocol_system: "uniswap_v2".to_string(),
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([(
            "map_pool_events".to_string(),
            "factory=0x01".to_string(),
        )]),
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3_alias".to_owned(),
        protocol_system: "uniswap_v3".to_string(),
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
        ..Default::default()
    };

    let execution = resolved_family_execution_config_from_extractor_configs_for_tests(&[&v2, &v3])
        .expect("aliased family execution config derives");
    let shared_stream = uniswap_shared_stream_for_tests("/tmp/shared-family.spkg");
    let expected_shared_stream = ResolvedSharedFamilyStream {
        spkg: "/tmp/shared-family.spkg".to_string(),
        ..shared_stream
    };

    assert_eq!(execution.stop_block, 120);
    assert_eq!(execution.configured_start_block, 0);
    assert!(execution.bootstrap_plan.is_none());
    assert_eq!(execution.shared_stream.module, expected_shared_stream.module);
    assert_eq!(execution.shared_stream.extractor_id, expected_shared_stream.extractor_id);
    assert_eq!(execution.shared_stream.durability_scope, expected_shared_stream.durability_scope);
    assert_eq!(
        FamilyBranchSpec::protocol_system_set(execution.branch_specs.iter()),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    assert_eq!(execution.branch_specs.len(), 2);
    assert_eq!(
        execution.merged_substreams_params,
        HashMap::from([
            ("map_pool_events".to_string(), "factory=0x01".to_string()),
            ("map_events".to_string(), "factory=0x02".to_string()),
        ])
    );
}

#[test]
fn test_resolved_family_execution_config_is_reused_from_resolved_family_runtime() {
    let v2 = ExtractorConfig {
        name: "uniswap_v2_primary".to_owned(),
        protocol_system: "uniswap_v2".to_string(),
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([(
            "map_pool_events".to_string(),
            "factory=0x01".to_string(),
        )]),
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3_primary".to_owned(),
        protocol_system: "uniswap_v3".to_string(),
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
        ..Default::default()
    };
    let config_refs = vec![&v2, &v3];
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/uniswap-family.spkg");

    let execution = &resolved_family.execution;

    assert_eq!(execution.stop_block, 120);
    assert_eq!(execution.configured_start_block, 0);
    assert!(execution.bootstrap_plan.is_none());
    assert_eq!(
        FamilyBranchSpec::protocol_system_set(execution.branch_specs.iter()),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    assert_eq!(execution.branch_specs.len(), 2);
    assert_eq!(
        execution.merged_substreams_params,
        HashMap::from([
            ("map_pool_events".to_string(), "factory=0x01".to_string()),
            ("map_events".to_string(), "factory=0x02".to_string()),
        ])
    );
}

#[test]
fn test_resolved_family_execution_config_precomputes_shared_bootstrap_plan_and_start_block() {
    let v2 = ExtractorConfig {
        name: "uniswap_v2".to_owned(),
        protocol_system: "uniswap_v2".to_string(),
        start_block: 42,
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        bootstrap: Some(BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        }),
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3".to_owned(),
        protocol_system: "uniswap_v3".to_string(),
        start_block: 42,
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        bootstrap: Some(BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678".to_owned(),
        }),
        ..Default::default()
    };

    let execution = resolved_family_execution_config_from_extractor_configs_for_tests(&[&v2, &v3])
        .expect("family execution config derives");

    assert_eq!(execution.configured_start_block, 43);
    let bootstrap_plan = execution
        .bootstrap_plan
        .as_ref()
        .expect("shared bootstrap plan should be precomputed");
    assert_eq!(bootstrap_plan.bootstrap_block, 42);
    assert_eq!(bootstrap_plan.branches.len(), 2);
    assert_eq!(bootstrap_plan.family_name.as_deref(), Some("uniswap"));
}

#[test]
fn test_resolved_family_execution_config_rejects_conflicting_stop_blocks() {
    let v2 = ExtractorConfig {
        name: "uniswap_v2".to_owned(),
        protocol_system: "uniswap_v2".to_string(),
        stop_block: Some(110),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3".to_owned(),
        protocol_system: "uniswap_v3".to_string(),
        stop_block: Some(120),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };

    let err = resolved_family_execution_config_from_extractor_configs_for_tests(&[&v2, &v3])
        .expect_err("conflicting stop blocks should fail");

    assert!(err
        .to_string()
        .contains("family runner requires one shared stop_block"));
}

#[test]
fn test_resolved_family_execution_config_rejects_conflicting_substreams_params() {
    let v2 = ExtractorConfig {
        name: "uniswap_v2".to_owned(),
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([(
            "map_pool_events".to_string(),
            "factory=0x01".to_string(),
        )]),
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3".to_owned(),
        protocol_system: "uniswap_v3".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        substreams_params: HashMap::from([(
            "map_pool_events".to_string(),
            "factory=0x02".to_string(),
        )]),
        ..Default::default()
    };

    let err = resolved_family_execution_config_from_extractor_configs_for_tests(&[&v2, &v3])
        .expect_err("conflicting family params should fail");

    assert!(err
        .to_string()
        .contains("conflicting substreams param `map_pool_events`"));
}

#[test]
fn test_validate_family_progress_consistency_allows_all_resumed_or_all_fresh() {
    validate_family_progress_consistency(
        &[("uniswap_v2".to_string(), 100), ("uniswap_v3".to_string(), 100)],
        &[],
        "before stream start",
    )
    .expect("all resumed should be allowed");

    validate_family_progress_consistency(
        &[],
        &["uniswap_v2".to_string(), "uniswap_v3".to_string()],
        "before bootstrap",
    )
    .expect("all fresh should be allowed");
}

#[test]
fn test_validate_family_progress_consistency_rejects_mixed_progress() {
    let err = validate_family_progress_consistency(
        &[("uniswap_v2".to_string(), 100)],
        &["uniswap_v3".to_string()],
        "before stream start",
    )
    .expect_err("mixed progress should fail");

    assert!(err
        .to_string()
        .contains("family runner requires consistent branch progress before stream start"));
}

#[test]
fn test_validate_family_runner_membership_accepts_exact_member_set() {
    let v2 = ExtractorConfig {
        name: "uniswap_v2".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v3".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let family = family_detected_runtime_from_configs_for_tests(
        &[&v2, &v3],
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
    );

    validate_family_runner_membership(&family, &[&v2, &v3])
        .expect("exact family members should be accepted");
}

#[test]
fn test_validate_family_runner_membership_rejects_missing_or_extra_members() {
    let only_v2 = ExtractorConfig {
        name: "uniswap_v2".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let v3 = ExtractorConfig {
        name: "uniswap_v3".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v3".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let curve = ExtractorConfig {
        name: "curve".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "curve".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "curve_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let family = family_detected_runtime_from_configs_for_tests(
        &[&only_v2, &v3],
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
    );

    let missing_err = validate_family_runner_membership(&family, &[&only_v2])
        .expect_err("missing member should fail");
    assert!(missing_err
        .to_string()
        .contains("requires exact member protocol systems"));

    let extra_err = validate_family_runner_membership(&family, &[&only_v2, &curve])
        .expect_err("extra non-family member should fail");
    assert!(extra_err
        .to_string()
        .contains("requires exact member protocol systems"));
}

#[test]
fn test_validate_family_runner_membership_rejects_chain_mismatch() {
    let family = family_detected_runtime_with_members_for_tests(
        "uniswap",
        Chain::Ethereum,
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        ["uniswap_v2"],
    );
    let base_v2 = ExtractorConfig {
        name: "base_v2".to_string(),
        protocol_system: "uniswap_v2".to_string(),
        chain: Chain::Base,
        ..Default::default()
    };

    let err = validate_family_runner_membership(&family, &[&base_v2])
        .expect_err("chain mismatch should fail");
    assert!(err
        .to_string()
        .contains("requires chain `ethereum`, but extractor `base_v2` uses `base`"));
}

#[test]
fn test_validate_family_runner_membership_rejects_explicit_family_mismatch() {
    let family = family_detected_runtime_with_members_for_tests(
        "uniswap",
        Chain::Ethereum,
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        ["uniswap_v2"],
    );
    let wrong_family_v2 = ExtractorConfig {
        name: "wrong_family_v2".to_string(),
        protocol_system: "uniswap_v2".to_string(),
        family_runtime: Some(FamilyRuntimeConfig {
            family: "future_swap".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let err = validate_family_runner_membership(&family, &[&wrong_family_v2])
        .expect_err("explicit family mismatch should fail");
    assert!(err
        .to_string()
        .contains("cannot include extractor `wrong_family_v2` declared for family `future_swap`"));
}

#[test]
fn test_validate_family_runner_membership_rejects_missing_protocol_types() {
    let family = family_detected_runtime_with_members_for_tests(
        "uniswap",
        Chain::Ethereum,
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        ["uniswap_v2"],
    );
    let typeless_v2 = ExtractorConfig {
        name: "typeless_v2".to_string(),
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![],
        ..Default::default()
    };

    let err = validate_family_runner_membership(&family, &[&typeless_v2])
        .expect_err("missing protocol types should fail");
    assert!(err.to_string().contains(
        "requires extractor `typeless_v2` to declare at least one protocol type for branch routing"
    ));
}
