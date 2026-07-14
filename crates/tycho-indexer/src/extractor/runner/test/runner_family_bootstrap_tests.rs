use std::{collections::HashMap, sync::Arc};

use super::*;
use crate::extractor::Extractor;

#[tokio::test]
async fn test_apply_family_bootstrap_plan_splits_once_and_updates_each_branch() {
    let plan = SharedBootstrapPlan {
        family_name: Some("uniswap".to_string()),
        bootstrap_block: 42,
        branches: vec![
            crate::extractor::shared_bootstrap::BootstrapBranchDescriptor {
                extractor_name: "uniswap_v2".to_string(),
                protocol_system: "uniswap_v2".to_string(),
                chain: Chain::Ethereum,
                strategy: BootstrapStrategy::UniswapV2Rpc,
                params: crate::extractor::shared_bootstrap::SharedBootstrapParams {
                    bootstrap_block: 42,
                    pools: vec![],
                },
            },
            crate::extractor::shared_bootstrap::BootstrapBranchDescriptor {
                extractor_name: "uniswap_v3".to_string(),
                protocol_system: "uniswap_v3".to_string(),
                chain: Chain::Ethereum,
                strategy: BootstrapStrategy::UniswapV3Rpc,
                params: crate::extractor::shared_bootstrap::SharedBootstrapParams {
                    bootstrap_block: 42,
                    pools: vec![],
                },
            },
        ],
    };

    let block = Block {
        number: 42,
        hash: Bytes::from(vec![0x01; 32]),
        parent_hash: Bytes::from(vec![0x02; 32]),
        chain: Chain::Ethereum,
        ts: chrono::NaiveDateTime::default(),
    };
    let tx = tycho_common::models::blockchain::Transaction {
        hash: Bytes::from(vec![0xaa; 32]),
        block_hash: block.hash.clone(),
        from: Bytes::from(vec![0x11; 20]),
        to: None,
        index: 0,
    };
    let merged_changes = crate::extractor::models::BlockChanges::new(
        family_shared_stream_name_for_tests("uniswap"),
        Chain::Ethereum,
        block.clone(),
        42,
        false,
        vec![tycho_common::models::blockchain::TxWithChanges {
            tx: tx.clone(),
            protocol_components: HashMap::from([
                (
                    "v2-pool".to_string(),
                    tycho_common::models::protocol::ProtocolComponent {
                        id: "v2-pool".to_string(),
                        protocol_system: "uniswap_v2".to_string(),
                        ..Default::default()
                    },
                ),
                (
                    "v3-pool".to_string(),
                    tycho_common::models::protocol::ProtocolComponent {
                        id: "v3-pool".to_string(),
                        protocol_system: "uniswap_v3".to_string(),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        }],
        vec![],
    );

    let mut v2 = MockExtractor::new();
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    v2.expect_protocol_system()
        .return_const("uniswap_v2".to_string());
    v2.expect_handle_block_changes()
        .once()
        .returning(|changes, cursor| {
            assert_eq!(cursor, "bootstrap@42");
            assert_eq!(changes.txs_with_update.len(), 1);
            assert_eq!(
                changes.txs_with_update[0]
                    .protocol_components
                    .len(),
                1
            );
            assert!(changes.txs_with_update[0]
                .protocol_components
                .contains_key("v2-pool"));
            Ok(None)
        });
    v2.expect_flush()
        .once()
        .returning(|| Ok(()));
    v2.expect_mark_bootstrap_completed()
        .once()
        .returning(|bootstrap_block, block_hash| {
            assert_eq!(bootstrap_block, 42);
            assert_eq!(block_hash, Bytes::from(vec![0x01; 32]));
            Ok(())
        });

    let mut v3 = MockExtractor::new();
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));
    v3.expect_protocol_system()
        .return_const("uniswap_v3".to_string());
    v3.expect_handle_block_changes()
        .once()
        .returning(|changes, cursor| {
            assert_eq!(cursor, "bootstrap@42");
            assert_eq!(changes.txs_with_update.len(), 1);
            assert_eq!(
                changes.txs_with_update[0]
                    .protocol_components
                    .len(),
                1
            );
            assert!(changes.txs_with_update[0]
                .protocol_components
                .contains_key("v3-pool"));
            Ok(None)
        });
    v3.expect_flush()
        .once()
        .returning(|| Ok(()));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);

    apply_family_bootstrap_plan(&extractors, &plan, merged_changes)
        .await
        .expect("shared family bootstrap should apply");
}

#[tokio::test]
async fn test_apply_family_bootstrap_plan_skips_completed_family() {
    let plan = SharedBootstrapPlan {
        family_name: Some("uniswap".to_string()),
        bootstrap_block: 42,
        branches: vec![crate::extractor::shared_bootstrap::BootstrapBranchDescriptor {
            extractor_name: "uniswap_v2".to_string(),
            protocol_system: "uniswap_v2".to_string(),
            chain: Chain::Ethereum,
            strategy: BootstrapStrategy::UniswapV2Rpc,
            params: crate::extractor::shared_bootstrap::SharedBootstrapParams {
                bootstrap_block: 42,
                pools: vec![],
            },
        }],
    };
    let block = Block {
        number: 42,
        hash: Bytes::from(vec![0x01; 32]),
        parent_hash: Bytes::from(vec![0x02; 32]),
        chain: Chain::Ethereum,
        ts: chrono::NaiveDateTime::default(),
    };
    let merged_changes = crate::extractor::models::BlockChanges::new(
        family_shared_stream_name_for_tests("uniswap"),
        Chain::Ethereum,
        block,
        42,
        false,
        vec![tycho_common::models::blockchain::TxWithChanges {
            tx: tycho_common::models::blockchain::Transaction {
                hash: Bytes::from(vec![0xaa; 32]),
                block_hash: Bytes::from(vec![0x01; 32]),
                from: Bytes::from(vec![0x11; 20]),
                to: None,
                index: 0,
            },
            protocol_components: HashMap::from([(
                "v2-pool".to_string(),
                tycho_common::models::protocol::ProtocolComponent {
                    id: "v2-pool".to_string(),
                    protocol_system: "uniswap_v2".to_string(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }],
        vec![],
    );

    let mut v2 = MockExtractor::new();
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));

    let extractors: HashMap<String, Arc<dyn Extractor>> =
        HashMap::from([("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>)]);

    apply_family_bootstrap_plan(&extractors, &plan, merged_changes)
        .await
        .expect("completed family bootstrap should be skipped cleanly");
}

#[tokio::test]
async fn test_apply_family_bootstrap_plan_rejects_missing_branch_extractor() {
    let plan = SharedBootstrapPlan {
        family_name: Some("uniswap".to_string()),
        bootstrap_block: 42,
        branches: vec![crate::extractor::shared_bootstrap::BootstrapBranchDescriptor {
            extractor_name: "uniswap_v2_alias".to_string(),
            protocol_system: "uniswap_v2".to_string(),
            chain: Chain::Ethereum,
            strategy: BootstrapStrategy::UniswapV2Rpc,
            params: crate::extractor::shared_bootstrap::SharedBootstrapParams {
                bootstrap_block: 42,
                pools: vec![],
            },
        }],
    };
    let block = Block {
        number: 42,
        hash: Bytes::from(vec![0x01; 32]),
        parent_hash: Bytes::from(vec![0x02; 32]),
        chain: Chain::Ethereum,
        ts: chrono::NaiveDateTime::default(),
    };
    let merged_changes = crate::extractor::models::BlockChanges::new(
        family_shared_stream_name_for_tests("uniswap"),
        Chain::Ethereum,
        block,
        42,
        false,
        vec![],
        vec![],
    );

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::new();

    let err = apply_family_bootstrap_plan(&extractors, &plan, merged_changes)
        .await
        .expect_err("missing branch extractor should fail");

    assert!(err
        .to_string()
        .contains("missing family bootstrap extractor for uniswap_v2"));
}

#[tokio::test]
async fn test_family_bootstrap_already_completed_rejects_mixed_completion_state() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));

    let mut v3 = MockExtractor::new();
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(None));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);

    let err = family_bootstrap_already_completed(&extractors, 42)
        .await
        .expect_err("mixed bootstrap completion state should fail");

    assert!(err.to_string().contains(
        "family runner requires consistent shared bootstrap completion before bootstrap run"
    ));
}

#[tokio::test]
async fn test_family_bootstrap_already_completed_rejects_misaligned_completed_blocks() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(42)));

    let mut v3 = MockExtractor::new();
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(43)));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);

    let err = family_bootstrap_already_completed(&extractors, 42)
        .await
        .expect_err("misaligned bootstrap completion blocks should fail");

    assert!(err
        .to_string()
        .contains("family runner requires aligned shared bootstrap completion blocks"));
}

#[tokio::test]
async fn test_family_bootstrap_already_completed_rejects_configured_block_drift() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(43)));

    let mut v3 = MockExtractor::new();
    v3.expect_get_completed_bootstrap_block()
        .once()
        .returning(|| Ok(Some(43)));

    let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);

    let err = family_bootstrap_already_completed(&extractors, 42)
        .await
        .expect_err("configured bootstrap block drift should fail");

    assert!(err.to_string().contains(
        "requires configured shared bootstrap block `42` to match persisted completed bootstrap block `43`"
    ));
}
