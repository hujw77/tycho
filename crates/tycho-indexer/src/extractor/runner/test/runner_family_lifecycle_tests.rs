use std::{collections::HashMap, sync::Arc};

use super::*;
use crate::extractor::managed_substreams_request::{
    prepare_substreams_request_for_runtime_target,
    PreparedSubstreamsRequest,
};
use crate::extractor::runtime_target_planning::ResolvedRuntimeTarget;
use crate::extractor::substreams_package_loader::LoadedSubstreamsPackage;
use crate::extractor::{Extractor, PersistedExtractorStateScope};
use crate::pb::sf::substreams::v1::Package;
use crate::substreams::SubstreamsEndpoint;
use futures03::StreamExt;
use tycho_ethereum::rpc::EthereumRpcClient;

#[tokio::test]
async fn test_resolve_family_stream_position_uses_next_aligned_resume_block() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-100".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor-100".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let position = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect("aligned progress should resolve");

    assert_eq!(
        position,
        ResolvedFamilyStreamPosition { start_block: 101, cursor: Some("cursor-100".to_string()) }
    );
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_misaligned_resumed_branch_cursors() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-v2".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor-v3".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();

    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("misaligned resumed branch cursors should fail");
    assert!(err
        .to_string()
        .contains("family runner requires aligned branch cursors"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_returns_aligned_resume_start_and_cursor() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-100".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor-100".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();

    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let position = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect("aligned resumed family stream position should resolve");

    assert_eq!(
        position,
        ResolvedFamilyStreamPosition { start_block: 101, cursor: Some("cursor-100".to_string()) }
    );
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_legacy_fallback_cursor_scope() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(true);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-100".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    v2.expect_get_cursor_state_scope()
        .once()
        .returning(|| Ok(PersistedExtractorStateScope::LegacyExtractorFallback));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(true);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor-100".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    v3.expect_get_cursor_state_scope()
        .once()
        .returning(|| Ok(PersistedExtractorStateScope::LegacyExtractorFallback));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("legacy fallback cursor scope should fail shared family resume");

    assert!(err
        .to_string()
        .contains("legacy extractor-scoped fallback cursor state"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_uses_shared_cursor_for_alias_named_members() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-100-shared".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor-100-shared".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2_alias".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3_alias".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let position = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect("aligned resumed alias family stream position should resolve");

    assert_eq!(
        position,
        ResolvedFamilyStreamPosition {
            start_block: 101,
            cursor: Some("cursor-100-shared".to_string()),
        }
    );
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_empty_resumed_shared_cursor() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .times(0..=1)
        .returning(String::new);
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .times(0..=1)
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("empty resumed shared cursor should fail");

    assert!(err
        .to_string()
        .contains("family runner requires a persisted shared cursor"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_treats_bootstrap_marker_as_no_resume_cursor() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 42, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "bootstrap@42".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 42, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "bootstrap@42".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let position = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect("bootstrap-only marker cursors should resolve as no shared stream cursor");

    assert_eq!(position, ResolvedFamilyStreamPosition { start_block: 43, cursor: None });
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_mixed_bootstrap_marker_and_stream_cursor() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 42, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "bootstrap@42".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 42, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor@42".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("mixed bootstrap-only markers and stream cursors should fail");

    assert!(err.to_string().contains("cannot mix"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_bootstrap_marker_block_drift() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 42, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "bootstrap@41".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 42, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "bootstrap@41".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("bootstrap-only marker block drift should fail");

    assert!(err
        .to_string()
        .contains("bootstrap-only marker cursor block"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_resume_block_overflow() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: u64::MAX, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-max".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: u64::MAX, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor-max".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("resume block overflow should fail");

    assert!(err
        .to_string()
        .contains("block number overflow"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_misaligned_resume_blocks() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-v2".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 101, ..Default::default() }));
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(|| "cursor-v3".to_string());
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("misaligned progress should fail");

    assert!(err
        .to_string()
        .contains("family runner requires aligned branch progress"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_rejects_mixed_resumed_and_fresh_branches() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-v2".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_runtime_test_configs(42, 42);
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");

    let err = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect_err("mixed branch progress should fail");

    assert!(err
        .to_string()
        .contains("family runner requires consistent branch progress"));
}

#[tokio::test]
async fn test_resolve_family_stream_position_uses_bootstrap_adjusted_aligned_fresh_start() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(true);
    v2.expect_get_cursor()
        .once()
        .returning(String::new);
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(true);
    v3.expect_get_cursor()
        .once()
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
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
            start_block: 42,
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            bootstrap: Some(BootstrapConfig {
                strategy: BootstrapStrategy::UniswapV3Rpc,
                start_block: 42,
                params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678"
                    .to_owned(),
            }),
            ..Default::default()
        },
    ];
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "test-shared.spkg");

    let position = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect("aligned fresh bootstrap branches should resolve");

    assert_eq!(position, ResolvedFamilyStreamPosition { start_block: 43, cursor: None });
}

#[tokio::test]
async fn test_resolve_family_stream_start_rejects_misaligned_fresh_branch_starts() {
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
            start_block: 45,
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            bootstrap: Some(BootstrapConfig {
                strategy: BootstrapStrategy::UniswapV3Rpc,
                start_block: 45,
                params: "bootstrap_block=45&pool=0x0000000000000000000000000000000000005678"
                    .to_owned(),
            }),
            ..Default::default()
        },
    ];
    let config_refs = configs.iter().collect::<Vec<_>>();
    let family_execution = try_resolved_family_runtime_from_configs_for_tests(
        &config_refs,
        "/tmp/misaligned-fresh-starts.spkg",
    )
    .expect_err("misaligned fresh family starts should fail in full family runtime resolution");

    assert!(family_execution
        .to_string()
        .contains("family `uniswap` requires aligned branch start blocks"));
}

#[tokio::test]
async fn test_run_family_bootstrap_if_needed_rejects_mixed_progress_before_bootstrap() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| Some(Block { number: 100, ..Default::default() }));
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(|| "cursor-v2".to_string());
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_bootstrap_test_configs();
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");
    let rpc_client = EthereumRpcClient::new("http://localhost:8545")
        .expect("rpc client builds for non-networked preflight");

    let err = run_family_bootstrap_if_needed(&extractors, &resolved_family, &rpc_client)
        .await
        .expect_err("mixed progress should fail before bootstrap materialization");

    assert!(err
        .to_string()
        .contains("family runner requires consistent branch progress before bootstrap"));
}

fn fail_if_shared_bootstrap_materialized<'a>(
    _rpc_client: &'a EthereumRpcClient,
    _plan: &'a SharedBootstrapPlan,
    _branch_materializers: &'a std::collections::HashMap<
        String,
        crate::extractor::family_bootstrap_registry::MaterializeBootstrapBranchFn,
    >,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<crate::extractor::models::BlockChanges, ExtractionError>,
            > + Send
            + 'a,
    >,
> {
    Box::pin(async move { panic!("shared bootstrap materialization should have been skipped") })
}

#[tokio::test]
async fn test_run_family_bootstrap_if_needed_skips_materialization_when_shared_completion_exists() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(String::new);
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_bootstrap_test_configs();
    let config_refs = configs.iter().collect::<Vec<_>>();
    let mut resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");
    resolved_family
        .shared_bootstrap_runtime_mut()
        .expect("family bootstrap runtime should be present")
        .execution
        .plan_materializer = fail_if_shared_bootstrap_materialized;

    let rpc_client =
        EthereumRpcClient::new("http://localhost:0000").expect("build stub rpc client");

    run_family_bootstrap_if_needed(&extractors, &resolved_family, &rpc_client)
        .await
        .expect("completed shared bootstrap should be skipped before materialization");
}

#[tokio::test]
async fn test_run_family_bootstrap_if_needed_rejects_misaligned_completed_bootstrap_blocks() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v2.expect_get_cursor()
        .once()
        .returning(String::new);
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(false);
    v3.expect_get_cursor()
        .once()
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(43)));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_bootstrap_test_configs();
    let config_refs = configs.iter().collect::<Vec<_>>();
    let mut resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");
    resolved_family
        .shared_bootstrap_runtime_mut()
        .expect("family bootstrap runtime should be present")
        .execution
        .plan_materializer = fail_if_shared_bootstrap_materialized;

    let rpc_client =
        EthereumRpcClient::new("http://localhost:0000").expect("build stub rpc client");

    let err = run_family_bootstrap_if_needed(&extractors, &resolved_family, &rpc_client)
        .await
        .expect_err("misaligned shared bootstrap completion should fail before materialization");
    assert!(err
        .to_string()
        .contains("family runner requires aligned shared bootstrap completion blocks"));
}

#[tokio::test]
async fn test_run_family_bootstrap_if_needed_rejects_legacy_fallback_bootstrap_scope() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v2.expect_supports_persisted_state_scope()
        .once()
        .return_const(true);
    v2.expect_get_cursor()
        .once()
        .returning(String::new);
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));
    v2.expect_get_completed_bootstrap_state_scope()
        .once()
        .returning(|| Ok(PersistedExtractorStateScope::LegacyExtractorFallback));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .once()
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .once()
        .return_const(true);
    v3.expect_get_cursor()
        .once()
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));
    v3.expect_get_completed_bootstrap_state_scope()
        .once()
        .returning(|| Ok(PersistedExtractorStateScope::LegacyExtractorFallback));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_bootstrap_test_configs();
    let config_refs = configs.iter().collect::<Vec<_>>();
    let mut resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "/tmp/test-family.spkg");
    resolved_family
        .shared_bootstrap_runtime_mut()
        .expect("family bootstrap runtime should be present")
        .execution
        .plan_materializer = fail_if_shared_bootstrap_materialized;

    let rpc_client =
        EthereumRpcClient::new("http://localhost:0000").expect("build stub rpc client");

    let err = run_family_bootstrap_if_needed(&extractors, &resolved_family, &rpc_client)
        .await
        .expect_err("legacy fallback bootstrap scope should fail shared bootstrap skip path");
    assert!(err
        .to_string()
        .contains("legacy extractor-scoped fallback bootstrap state"));
}

#[tokio::test]
async fn test_prepare_family_substreams_request_uses_bootstrap_adjusted_start_after_completed_shared_bootstrap(
) {
    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .times(1)
        .returning(|| None);
    v2.expect_supports_persisted_state_scope()
        .times(1)
        .return_const(false);
    v2.expect_get_cursor()
        .times(1)
        .returning(String::new);
    v2.expect_get_completed_bootstrap_block()
        .times(1)
        .returning(|| Ok(Some(42)));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .times(1)
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .times(1)
        .return_const(false);
    v3.expect_get_cursor()
        .times(1)
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .times(1)
        .returning(|| Ok(Some(42)));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let configs = make_uniswap_family_bootstrap_test_configs();
    let config_refs = configs.iter().collect::<Vec<_>>();
    let resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "test-shared.spkg");
    let rpc_client =
        EthereumRpcClient::new("http://localhost:0000").expect("build stub rpc client");
    let request_context = resolved_family
        .prepared_request_context(&extractors)
        .expect("resolved family prepared request context");

    let prepared_request = prepare_substreams_request_for_runtime_target(
        &resolved_family,
        &request_context,
        &rpc_client,
    )
    .await
    .expect("completed shared bootstrap should shape request without rerunning bootstrap");

    assert_eq!(prepared_request.request.spkg, resolved_family.shared_spkg());
    assert_eq!(prepared_request.request.module, family_output_module_for_tests("uniswap"));
    assert_eq!(prepared_request.request.start_block, 43);
    assert_eq!(prepared_request.cursor, None);
}

#[tokio::test]
async fn test_family_stream_request_starts_after_completed_shared_bootstrap() {
    use crate::substreams::mock::start_mock_substreams;

    let (captured, addr) = start_mock_substreams().await;

    let mut v2 = MockExtractor::new();
    v2.expect_get_last_processed_block()
        .times(2)
        .returning(|| None);
    v2.expect_supports_persisted_state_scope()
        .times(2)
        .return_const(false);
    v2.expect_get_cursor()
        .times(2)
        .returning(String::new);
    v2.expect_get_completed_bootstrap_block()
        .times(2)
        .returning(|| Ok(Some(42)));

    let mut v3 = MockExtractor::new();
    v3.expect_get_last_processed_block()
        .times(2)
        .returning(|| None);
    v3.expect_supports_persisted_state_scope()
        .times(2)
        .return_const(false);
    v3.expect_get_cursor()
        .times(2)
        .returning(String::new);
    v3.expect_get_completed_bootstrap_block()
        .times(2)
        .returning(|| Ok(Some(42)));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);

    let configs = make_uniswap_family_bootstrap_test_configs();
    let config_refs = configs.iter().collect::<Vec<_>>();
    let mut resolved_family =
        resolved_family_runtime_from_configs_for_tests(&config_refs, "test-family.spkg");
    resolved_family
        .shared_bootstrap_runtime_mut()
        .expect("family bootstrap runtime should be present")
        .execution
        .plan_materializer = fail_if_shared_bootstrap_materialized;

    let rpc_client =
        EthereumRpcClient::new("http://localhost:0000").expect("build stub rpc client");

    run_family_bootstrap_if_needed(&extractors, &resolved_family, &rpc_client)
        .await
        .expect("completed shared bootstrap should skip materialization");

    let stream_position = resolve_family_stream_position(&extractors, &resolved_family)
        .await
        .expect("completed shared bootstrap should produce fresh family stream position");

    let request = ResolvedRuntimeTarget::Family(resolved_family)
        .substreams_execution_request_with_start_block(stream_position.start_block)
        .unwrap();

    let loaded_substreams = LoadedSubstreamsPackage {
        spkg: Package::default(),
        endpoint: Arc::new(
            SubstreamsEndpoint::new(format!("http://{addr}"), None)
                .await
                .expect("mock substreams endpoint builds"),
        ),
    };
    let mut stream = PreparedSubstreamsRequest { request, cursor: stream_position.cursor }
        .build_stream(
        loaded_substreams,
        false,
        false,
    );
    let _ = stream
        .next()
        .await
        .expect("mock stream should yield one terminal response")
        .expect("mock stream response should be ok");

    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
    assert_eq!(
        requests[0].start_block_num, 43,
        "completed shared bootstrap should shift the family stream to bootstrap block + 1"
    );
    assert!(
        requests[0].start_cursor.is_empty(),
        "fresh shared-family start should not send a resume cursor"
    );
    assert_eq!(requests[0].output_module, family_output_module_for_tests("uniswap"));
    assert_eq!(
        requests[0].params,
        HashMap::from([
            ("map_pool_events".to_string(), "factory=0x01".to_string()),
            ("map_events".to_string(), "factory=0x02".to_string()),
        ])
    );
}
