use super::*;
use crate::extractor::family_registry::default_family_runtime_registry;

#[test]
fn family_runtime_config_exposes_explicit_durability_scope_only() {
    let runtime = FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: None,
        shared_module: None,
        durability_scope: None,
    };

    assert_eq!(runtime.durability_scope(), None);
}

#[test]
fn family_runtime_config_exposes_explicit_shared_stream_fields_only() {
    let runtime = FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: None,
        shared_module: None,
        durability_scope: None,
    };

    assert_eq!(runtime.shared_spkg(), None);
    assert_eq!(runtime.shared_module(), None);
}

#[test]
fn extractor_config_exposes_resolved_family_runtime_metadata() {
    let config = ExtractorConfig::default().with_family_runtime(Some(FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: None,
        shared_module: None,
        durability_scope: Some("family::custom_uniswap".to_string()),
    }));

    let err = config
        .require_resolved_family_runtime_metadata()
        .expect_err("runtime build should reject unresolved shared stream metadata first");
    assert!(err
        .to_string()
        .contains("uses family runtime `uniswap` without a resolved shared_spkg"));
}

#[test]
fn extractor_config_rejects_unresolved_family_durability_scope_for_runtime_build() {
    let config = ExtractorConfig::default().with_family_runtime(Some(FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: Some("/tmp/family-runtime.spkg".to_string()),
        shared_module: Some(uniswap_shared_stream_for_tests("/tmp/family-runtime.spkg").module),
        durability_scope: None,
    }));

    let err = config
        .require_resolved_family_runtime_metadata()
        .expect_err("runtime build should reject unresolved family durability scope");

    assert!(err
        .to_string()
        .contains("uses family runtime `uniswap` without a resolved durability_scope"));
}

#[test]
fn extractor_config_exposes_resolved_family_shared_spkg() {
    let config = ExtractorConfig::new(
        "uniswap_v2".to_string(),
        Chain::Ethereum,
        ImplementationType::Custom,
        1000,
        42,
        None,
        vec![],
        "/tmp/member-only.spkg".to_string(),
        "map_pool_events".to_string(),
        vec![],
        0,
        None,
        None,
        HashMap::new(),
        None,
    )
    .with_family_runtime(Some(FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: Some("/tmp/family-runtime.spkg".to_string()),
        shared_module: None,
        durability_scope: None,
    }));

    let target = config
        .require_resolved_family_runtime_metadata()
        .expect_err("runtime build should reject unresolved family shared module");
    assert!(target
        .to_string()
        .contains("uses family runtime `uniswap` without a resolved shared_module"));
}

#[test]
fn extractor_config_exposes_resolved_family_shared_module() {
    let config = ExtractorConfig::new(
        "uniswap_v2".to_string(),
        Chain::Ethereum,
        ImplementationType::Custom,
        1000,
        42,
        None,
        vec![],
        "/tmp/member-only.spkg".to_string(),
        "map_pool_events".to_string(),
        vec![],
        0,
        None,
        None,
        HashMap::new(),
        None,
    )
    .with_family_runtime(Some(FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: None,
        shared_module: Some(uniswap_shared_stream_for_tests("/tmp/family-runtime.spkg").module),
        durability_scope: None,
    }));

    let err = config
        .require_resolved_family_runtime_metadata()
        .expect_err("runtime build should reject unresolved family shared spkg");
    assert!(err
        .to_string()
        .contains("uses family runtime `uniswap` without a resolved shared_spkg"));
}

#[test]
fn extractor_config_accepts_resolved_family_shared_stream_target_for_runtime_build() {
    let expected_shared_stream = uniswap_shared_stream_for_tests("/tmp/family-runtime.spkg");
    let config = ExtractorConfig::new(
        "uniswap_v2".to_string(),
        Chain::Ethereum,
        ImplementationType::Custom,
        1000,
        42,
        None,
        vec![],
        "/tmp/member-only.spkg".to_string(),
        "map_pool_events".to_string(),
        vec![],
        0,
        None,
        None,
        HashMap::new(),
        None,
    )
    .with_family_runtime(Some(FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: Some("/tmp/family-runtime.spkg".to_string()),
        shared_module: Some(expected_shared_stream.module.clone()),
        durability_scope: Some(
            expected_shared_stream
                .durability_scope
                .clone(),
        ),
    }));

    let target = config
        .require_resolved_family_runtime_metadata()
        .expect("resolved family target should be accepted")
        .expect("family runtime target should be present");
    assert_eq!(target.family, "uniswap");
    assert_eq!(target.shared_stream.spkg, "/tmp/family-runtime.spkg");
    assert_eq!(target.shared_stream.module, expected_shared_stream.module);
    assert_eq!(target.durability_scope, expected_shared_stream.durability_scope);
}

#[test]
fn extractor_config_resolves_missing_family_runtime_fields_from_registry() {
    let expected_shared_stream = uniswap_shared_stream_for_tests("/tmp/family-runtime.spkg");
    let config = ExtractorConfig::new(
        "uniswap_v2".to_string(),
        Chain::Ethereum,
        ImplementationType::Custom,
        1000,
        42,
        None,
        vec![],
        "/tmp/member-only.spkg".to_string(),
        "map_pool_events".to_string(),
        vec![],
        0,
        None,
        None,
        HashMap::new(),
        None,
    )
    .with_family_runtime(Some(FamilyRuntimeConfig {
        family: "uniswap".to_string(),
        shared_spkg: Some("/tmp/family-runtime.spkg".to_string()),
        shared_module: None,
        durability_scope: None,
    }));

    let target = config
        .resolve_family_runtime_metadata(Some(default_family_runtime_registry()))
        .expect("registry-backed family metadata should resolve")
        .expect("family runtime target should be present");

    assert_eq!(target.family, "uniswap");
    assert_eq!(target.shared_stream.spkg, "/tmp/family-runtime.spkg");
    assert_eq!(target.shared_stream.module, expected_shared_stream.module);
    assert_eq!(target.durability_scope, expected_shared_stream.durability_scope);
}
