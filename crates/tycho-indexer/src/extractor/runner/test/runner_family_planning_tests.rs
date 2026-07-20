use super::*;

#[test]
fn test_resolved_family_execution_config_rejects_partial_shared_bootstrap_config() {
    let configs = [
        ExtractorConfig {
            bootstrap: Some(make_uniswap_member_bootstrap_config("uniswap_v2", 42)),
            ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 42)
        },
        make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 43),
    ];
    let config_refs = configs.iter().collect::<Vec<_>>();
    let err = try_resolved_family_runtime_from_configs_for_tests(
        &config_refs,
        "/tmp/partial-shared-bootstrap.spkg",
    )
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
        stop_block: Some(120),
        substreams_params: make_uniswap_member_substreams_params("uniswap_v2"),
        ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 0)
    };
    let v3 = ExtractorConfig {
        stop_block: Some(120),
        substreams_params: make_uniswap_member_substreams_params("uniswap_v3"),
        ..make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 0)
    };

    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&[&v2, &v3], "/tmp/shared-family.spkg");
    let expected_shared_stream = uniswap_shared_stream_for_tests("/tmp/shared-family.spkg");

    assert_eq!(resolved_family.stop_block(), 120);
    assert_eq!(resolved_family.configured_start_block(), 0);
    assert!(resolved_family
        .shared_bootstrap_plan()
        .is_none());
    assert_eq!(resolved_family.output_module(), expected_shared_stream.module);
    assert_eq!(resolved_family.shared_extractor_id(), expected_shared_stream.extractor_id);
    assert_eq!(resolved_family.durability_scope(), expected_shared_stream.durability_scope);
    assert_eq!(
        FamilyBranchSpec::protocol_system_set(resolved_family.branch_specs().iter()),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    assert_eq!(resolved_family.branch_specs().len(), 2);
    assert_eq!(
        resolved_family.merged_substreams_params(),
        &HashMap::from([
            ("map_pool_events".to_string(), "factory=0x01".to_string()),
            ("map_events".to_string(), "factory=0x02".to_string()),
        ])
    );
}

#[test]
fn test_resolved_family_execution_config_uses_protocol_systems_for_aliased_members() {
    let v2 = ExtractorConfig {
        stop_block: Some(120),
        substreams_params: make_uniswap_member_substreams_params("uniswap_v2"),
        ..make_uniswap_member_runtime_test_config("uniswap_v2_alias", "uniswap_v2", 0)
    };
    let v3 = ExtractorConfig {
        stop_block: Some(120),
        substreams_params: make_uniswap_member_substreams_params("uniswap_v3"),
        ..make_uniswap_member_runtime_test_config("uniswap_v3_alias", "uniswap_v3", 0)
    };

    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&[&v2, &v3], "/tmp/shared-family.spkg");
    let expected_shared_stream = uniswap_shared_stream_for_tests("/tmp/shared-family.spkg");

    assert_eq!(resolved_family.stop_block(), 120);
    assert_eq!(resolved_family.configured_start_block(), 0);
    assert!(resolved_family
        .shared_bootstrap_plan()
        .is_none());
    assert_eq!(resolved_family.output_module(), expected_shared_stream.module);
    assert_eq!(resolved_family.shared_extractor_id(), expected_shared_stream.extractor_id);
    assert_eq!(resolved_family.durability_scope(), expected_shared_stream.durability_scope);
    assert_eq!(
        FamilyBranchSpec::protocol_system_set(resolved_family.branch_specs().iter()),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    assert_eq!(resolved_family.branch_specs().len(), 2);
    assert_eq!(
        resolved_family.merged_substreams_params(),
        &HashMap::from([
            ("map_pool_events".to_string(), "factory=0x01".to_string()),
            ("map_events".to_string(), "factory=0x02".to_string()),
        ])
    );
}

#[test]
fn test_resolved_family_execution_config_is_reused_from_resolved_family_runtime() {
    let v2 = ExtractorConfig {
        stop_block: Some(120),
        substreams_params: make_uniswap_member_substreams_params("uniswap_v2"),
        ..make_uniswap_member_runtime_test_config("uniswap_v2_primary", "uniswap_v2", 0)
    };
    let v3 = ExtractorConfig {
        stop_block: Some(120),
        substreams_params: make_uniswap_member_substreams_params("uniswap_v3"),
        ..make_uniswap_member_runtime_test_config("uniswap_v3_primary", "uniswap_v3", 0)
    };
    let config_refs = vec![&v2, &v3];
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/uniswap-family.spkg");

    assert_eq!(resolved_family.stop_block(), 120);
    assert_eq!(resolved_family.configured_start_block(), 0);
    assert!(resolved_family
        .shared_bootstrap_plan()
        .is_none());
    assert_eq!(
        FamilyBranchSpec::protocol_system_set(resolved_family.branch_specs().iter()),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    assert_eq!(resolved_family.branch_specs().len(), 2);
    assert_eq!(
        resolved_family.merged_substreams_params(),
        &HashMap::from([
            ("map_pool_events".to_string(), "factory=0x01".to_string()),
            ("map_events".to_string(), "factory=0x02".to_string()),
        ])
    );
}

#[test]
fn test_resolved_family_runtime_precomputes_shared_bootstrap_plan_and_start_block() {
    let v2 = ExtractorConfig {
        stop_block: Some(120),
        bootstrap: Some(make_uniswap_member_bootstrap_config("uniswap_v2", 42)),
        ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 42)
    };
    let v3 = ExtractorConfig {
        stop_block: Some(120),
        bootstrap: Some(make_uniswap_member_bootstrap_config("uniswap_v3", 42)),
        ..make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 42)
    };

    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&[&v2, &v3], "/tmp/shared-family.spkg");

    assert_eq!(resolved_family.configured_start_block(), 43);
    let bootstrap_plan = resolved_family
        .shared_bootstrap_plan()
        .expect("shared bootstrap plan should be precomputed");
    assert_eq!(bootstrap_plan.bootstrap_block, 42);
    assert_eq!(bootstrap_plan.branches.len(), 2);
    assert_eq!(bootstrap_plan.family_name, "uniswap");
}

#[test]
fn test_resolved_family_execution_config_rejects_conflicting_stop_blocks() {
    let v2 = ExtractorConfig {
        stop_block: Some(110),
        ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 0)
    };
    let v3 = ExtractorConfig {
        stop_block: Some(120),
        ..make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 0)
    };

    let err = try_resolved_family_runtime_from_configs_for_tests(
        &[&v2, &v3],
        "/tmp/conflicting-stop-blocks.spkg",
    )
        .expect_err("conflicting stop blocks should fail");

    assert!(err
        .to_string()
        .contains("family `uniswap` requires one shared stop_block"));
}

#[test]
fn test_resolved_family_execution_config_rejects_conflicting_substreams_params() {
    let v2 = ExtractorConfig {
        substreams_params: HashMap::from([(
            "map_pool_events".to_string(),
            "factory=0x01".to_string(),
        )]),
        ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 0)
    };
    let v3 = ExtractorConfig {
        substreams_params: HashMap::from([(
            "map_pool_events".to_string(),
            "factory=0x02".to_string(),
        )]),
        ..make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 0)
    };

    let err = try_resolved_family_runtime_from_configs_for_tests(
        &[&v2, &v3],
        "/tmp/conflicting-substreams-params.spkg",
    )
        .expect_err("conflicting family params should fail");

    assert!(err
        .to_string()
        .contains("conflicting substreams param `map_pool_events`"));
}

#[test]
fn test_validate_family_progress_consistency_allows_all_resumed_or_all_fresh() {
    validate_shared_progress_consistency(
        "family runner",
        &[("uniswap_v2".to_string(), 100), ("uniswap_v3".to_string(), 100)],
        &[],
        "before stream start",
    )
    .expect("all resumed should be allowed");

    validate_shared_progress_consistency(
        "family runner",
        &[],
        &["uniswap_v2".to_string(), "uniswap_v3".to_string()],
        "before bootstrap",
    )
    .expect("all fresh should be allowed");
}

#[test]
fn test_validate_family_progress_consistency_rejects_mixed_progress() {
    let err = validate_shared_progress_consistency(
        "family runner",
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
        chain: Chain::Ethereum,
        ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 0)
    };
    let v3 = ExtractorConfig {
        chain: Chain::Ethereum,
        ..make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 0)
    };
    let resolved_family = resolved_family_runtime_from_configs_for_tests(
        &[&v2, &v3],
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
    );

    validate_family_runner_membership(&resolved_family.family, &[&v2, &v3])
        .expect("exact family members should be accepted");
}

#[test]
fn test_validate_family_runner_membership_rejects_missing_or_extra_members() {
    let only_v2 = ExtractorConfig {
        chain: Chain::Ethereum,
        ..make_uniswap_member_runtime_test_config("uniswap_v2", "uniswap_v2", 0)
    };
    let v3 = ExtractorConfig {
        chain: Chain::Ethereum,
        ..make_uniswap_member_runtime_test_config("uniswap_v3", "uniswap_v3", 0)
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
    let resolved_family = resolved_family_runtime_from_configs_for_tests(
        &[&only_v2, &v3],
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
    );

    let missing_err = validate_family_runner_membership(&resolved_family.family, &[&only_v2])
        .expect_err("missing member should fail");
    assert!(missing_err
        .to_string()
        .contains("requires exact member protocol systems"));

    let extra_err = validate_family_runner_membership(&resolved_family.family, &[&only_v2, &curve])
        .expect_err("extra non-family member should fail");
    assert!(extra_err
        .to_string()
        .contains("requires exact member protocol systems"));
}

#[test]
fn test_validate_family_runner_membership_rejects_chain_mismatch() {
    let eth_v2_seed = ExtractorConfig {
        name: "eth_v2_seed".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let eth_v3_seed = ExtractorConfig {
        name: "eth_v3_seed".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v3".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let resolved_family = resolved_family_runtime_from_configs_for_tests(
        &[&eth_v2_seed, &eth_v3_seed],
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
    );
    let base_v2 = ExtractorConfig {
        name: "base_v2".to_string(),
        protocol_system: "uniswap_v2".to_string(),
        chain: Chain::Base,
        ..Default::default()
    };

    let err = validate_family_runner_membership(&resolved_family.family, &[&base_v2])
        .expect_err("chain mismatch should fail");
    assert!(err
        .to_string()
        .contains("requires chain `ethereum`, but extractor `base_v2` uses `base`"));
}

#[test]
fn test_validate_family_runner_membership_rejects_explicit_family_mismatch() {
    let eth_v2_seed = ExtractorConfig {
        name: "eth_v2_seed".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let eth_v3_seed = ExtractorConfig {
        name: "eth_v3_seed".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v3".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let resolved_family = resolved_family_runtime_from_configs_for_tests(
        &[&eth_v2_seed, &eth_v3_seed],
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
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

    let err = validate_family_runner_membership(&resolved_family.family, &[&wrong_family_v2])
        .expect_err("explicit family mismatch should fail");
    assert!(err
        .to_string()
        .contains("cannot include extractor `wrong_family_v2` declared for family `future_swap`"));
}

#[test]
fn test_validate_family_runner_membership_rejects_missing_protocol_types() {
    let eth_v2_seed = ExtractorConfig {
        name: "eth_v2_seed".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v2_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let eth_v3_seed = ExtractorConfig {
        name: "eth_v3_seed".to_string(),
        chain: Chain::Ethereum,
        protocol_system: "uniswap_v3".to_string(),
        protocol_types: vec![ProtocolTypeConfig::new(
            "uniswap_v3_pool".to_string(),
            FinancialType::Swap,
        )],
        ..Default::default()
    };
    let resolved_family = resolved_family_runtime_from_configs_for_tests(
        &[&eth_v2_seed, &eth_v3_seed],
        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
    );
    let typeless_v2 = ExtractorConfig {
        name: "typeless_v2".to_string(),
        protocol_system: "uniswap_v2".to_string(),
        protocol_types: vec![],
        ..Default::default()
    };

    let err = validate_family_runner_membership(&resolved_family.family, &[&typeless_v2])
        .expect_err("missing protocol types should fail");
    assert!(err.to_string().contains(
        "requires extractor `typeless_v2` to declare at least one protocol type for branch routing"
    ));
}
