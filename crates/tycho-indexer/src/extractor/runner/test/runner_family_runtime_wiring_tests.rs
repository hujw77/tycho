use std::{collections::HashMap, sync::Arc};

use futures03::StreamExt;
use tokio::sync::Mutex;
use tycho_ethereum::rpc::EthereumRpcClient;

use super::*;
use crate::{
    extractor::{
        protocol_cache::ProtocolMemoryCache,
        runtime_targets_startup::{PreparedRuntimeTargetKind, PreparedRuntimeTargetStartup},
        Extractor,
    },
    pb::sf::substreams::rpc::v2::BlockScopedData,
};

#[tokio::test]
async fn test_family_runner_does_not_durably_persist_failing_block_across_branches() {
    use std::sync::Arc;

    use alloy::primitives::Address as AlloyAddress;
    use tycho_common::{
        models::ProtocolType,
        storage::{ExtractionStateGateway, ProtocolGateway, StorageError},
    };
    use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
    use tycho_storage::postgres::{builder::GatewayBuilder, testing::run_against_db};

    use crate::extractor::{
        chain_state::ChainState,
        protocol_cache::ProtocolMemoryCache,
        protocol_extractor::{ExtractorPgGateway, ProtocolExtractor},
        MockExtractorExtension,
    };

    fn family_block_with_branch_ids(
        number: u64,
        v2_component_id: &str,
        v3_component_id: &str,
        reserve0: u64,
        token0: &Bytes,
        token1: &Bytes,
    ) -> BlockScopedData {
        use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

        let family_changes = substreams::BlockChanges {
            block: Some(substreams::Block {
                number,
                hash: vec![number as u8; 32],
                parent_hash: vec![number.saturating_sub(1) as u8; 32],
                ts: 1_718_000_000,
            }),
            changes: vec![substreams::TransactionChanges {
                tx: Some(substreams::Transaction {
                    hash: vec![number as u8; 32],
                    from: vec![0x01; 20],
                    to: vec![0x02; 20],
                    index: 0,
                }),
                contract_changes: vec![],
                entity_changes: vec![substreams::EntityChanges {
                    component_id: v2_component_id.to_string(),
                    attributes: vec![substreams::Attribute {
                        name: "reserve0".to_string(),
                        value: Bytes::from(reserve0)
                            .lpad(32, 0)
                            .to_vec(),
                        change: substreams::ChangeType::Creation as i32,
                    }],
                }],
                component_changes: vec![
                    substreams::ProtocolComponent {
                        id: v2_component_id.to_string(),
                        tokens: vec![token0.to_vec(), token1.to_vec()],
                        contracts: vec![],
                        static_att: vec![],
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v2_pool".to_string(),
                            financial_type: substreams::FinancialType::Swap as i32,
                            attribute_schema: vec![],
                            implementation_type: substreams::ImplementationType::Custom as i32,
                        }),
                        change: substreams::ChangeType::Creation as i32,
                    },
                    substreams::ProtocolComponent {
                        id: v3_component_id.to_string(),
                        tokens: vec![token0.to_vec(), token1.to_vec()],
                        contracts: vec![],
                        static_att: vec![],
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v3_pool".to_string(),
                            financial_type: substreams::FinancialType::Swap as i32,
                            attribute_schema: vec![],
                            implementation_type: substreams::ImplementationType::Custom as i32,
                        }),
                        change: substreams::ChangeType::Creation as i32,
                    },
                ],
                balance_changes: vec![],
                entrypoints: vec![],
                entrypoint_params: vec![],
            }],
            storage_changes: vec![],
        };

        BlockScopedData {
            output: Some(MapModuleOutput {
                name: family_output_module_for_tests("uniswap"),
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: family_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock { id: number.to_string(), number, timestamp: None }),
            cursor: format!("cursor@{number}"),
            final_block_height: number,
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: String::new(),
            is_partial: false,
            partial_index: None,
            is_last_partial: None,
        }
    }

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
    });
    std::env::set_var("DATABASE_URL", &db_url);

    run_against_db(|_| async move {
        let chain = Chain::Ethereum;
        let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
        let token0 = Bytes::from(vec![0xa0; 20]);
        let token1 = Bytes::from(vec![0xc0; 20]);

        let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&protocol_systems)
            .build()
            .await
            .expect("Failed to create Gateway");
        let direct_gw = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&protocol_systems)
            .build_direct_gw()
            .await
            .expect("Failed to create DirectGateway");
        direct_gw
            .add_tokens(&[
                Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
            ])
            .await
            .expect("seed tokens for family persistence isolation");

        let rpc = EthereumRpcClient::new("http://localhost:0000")
            .expect("Failed to create stub RPC client");
        let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);
        let protocol_cache = ProtocolMemoryCache::new(
            chain,
            chrono::Duration::seconds(900),
            Arc::new(direct_gw.clone()),
        );
        protocol_cache
            .populate()
            .await
            .expect("populate protocol cache");

        let v2_gateway = ExtractorPgGateway::new("uniswap_v2", chain, 1000, cached_gw.clone(), None);
        let v2_extractor = Arc::new(
            ProtocolExtractor::<
                ExtractorPgGateway,
                EthereumTokenPreProcessor,
                MockExtractorExtension,
            >::new(
                v2_gateway,
                1,
                "uniswap_v2",
                chain,
                ChainState::default(),
                "uniswap_v2".to_string(),
                protocol_cache,
                HashMap::from([(
                    "uniswap_v2_pool".to_string(),
                    ProtocolType::new(
                        "uniswap_v2_pool".to_string(),
                        tycho_common::models::FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                )]),
                vec![],
                token_processor,
                None,
                None,
            )
            .await
            .expect("build real v2 extractor"),
        );
        v2_extractor
            .ensure_protocol_types()
            .await
            .expect("persist v2 protocol types");

        let v3_call_count = Arc::new(std::sync::Mutex::new(0usize));
        let mut v3 = MockExtractor::new();
        v3.expect_get_id()
            .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
        {
            let v3_call_count = Arc::clone(&v3_call_count);
            v3.expect_handle_tick_scoped_data()
                .times(0..)
                .returning(move |_| {
                    let mut count = v3_call_count.lock().expect("lock v3 call count");
                    *count += 1;
                    if *count == 1 {
                        Ok(Some(Arc::new(BlockAggregatedChanges::default())))
                    } else {
                        Err(ExtractionError::Unknown("simulated v3 branch failure".to_string()))
                    }
                });
        }

        let dispatcher = FamilyBlockChangesDispatcher::new([
            FamilyBranchSpec {
                protocol_system: "uniswap_v2".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
            },
            FamilyBranchSpec {
                protocol_system: "uniswap_v3".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
            },
        ])
        .expect("dispatcher builds");

        let runner = family_runner_for_tests(
            HashMap::from([
                ("uniswap_v2".to_string(), v2_extractor.clone() as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
                Ok(BlockResponse::New(family_block_with_branch_ids(
                    100,
                    "v2-block-100-pool",
                    "v3-block-100-pool",
                    1_000,
                    &token0,
                    &token1,
                ))),
                Ok(BlockResponse::New(family_block_with_branch_ids(
                    101,
                    "v2-block-101-pool",
                    "v3-block-101-pool",
                    2_000,
                    &token0,
                    &token1,
                ))),
            ]))),
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ]),
            dispatcher,
        );

        let err = runner.run().await.unwrap().expect_err("family runner should fail");
        assert!(
            matches!(err, ExtractionError::Unknown(ref message) if message == "simulated v3 branch failure"),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            *v3_call_count.lock().expect("lock v3 call count"),
            2,
            "expected both family blocks to reach the v3 branch before the synthetic failure"
        );
        v2_extractor
            .await_pending_commit_for_test()
            .await
            .expect("complete v2 commit task");

        let mut persisted_state = None;
        for _ in 0..20 {
            match cached_gw.get_state("uniswap_v2", &chain).await {
                Ok(state) => {
                    persisted_state = Some(state);
                    break;
                }
                Err(StorageError::NotFound(_, _)) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(err) => panic!("unexpected read error while waiting for v2 state: {err}"),
            }
        }
        let persisted_state = persisted_state.unwrap_or_else(|| {
            panic!("expected block 100 extraction state to become durable within retry window")
        });
        assert_eq!(persisted_state.cursor, b"cursor@100".to_vec());
        assert_eq!(persisted_state.block_hash, Bytes::from(vec![100u8; 32]));

        let components = cached_gw
            .get_protocol_components(&chain, None, None, None, None)
            .await
            .expect("read protocol components after mixed success/failure family run");
        let component_ids = components
            .entity
            .iter()
            .map(|component| component.id.clone())
            .collect::<Vec<_>>();
        assert!(component_ids.contains(&"v2-block-100-pool".to_string()));
        assert!(!component_ids.contains(&"v2-block-101-pool".to_string()));

        let v2_states = cached_gw
            .get_protocol_states(
                &chain,
                None,
                None,
                Some(&["v2-block-100-pool", "v2-block-101-pool"]),
                false,
                None,
            )
            .await
            .expect("read protocol states after mixed success/failure family run");
        let state_ids = v2_states
            .entity
            .iter()
            .map(|state| state.component_id.clone())
            .collect::<Vec<_>>();
        assert!(state_ids.contains(&"v2-block-100-pool".to_string()));
        assert!(!state_ids.contains(&"v2-block-101-pool".to_string()));

        assert!(matches!(
            cached_gw.get_state("uniswap_v3", &chain).await,
            Err(StorageError::NotFound(_, _))
        ));
    })
    .await;
}

#[tokio::test]
async fn test_family_runner_flushes_all_branches_on_stream_end() {
    let mut v2 = MockExtractor::new();
    v2.expect_flush()
        .once()
        .returning(|| Ok(()));
    let mut v3 = MockExtractor::new();
    v3.expect_flush()
        .once()
        .returning(|| Ok(()));

    let dispatcher = FamilyBlockChangesDispatcher::new([
        FamilyBranchSpec {
            protocol_system: "uniswap_v2".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
        },
        FamilyBranchSpec {
            protocol_system: "uniswap_v3".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
        },
    ])
    .expect("dispatcher builds");

    let runner = family_runner_for_tests(
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]),
        SubstreamsStream::from_stream(Box::pin(stream::iter(vec![Ok(BlockResponse::Ended)]))),
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
        ]),
        dispatcher,
    );

    runner.run().await.unwrap().unwrap();
}

#[tokio::test]
async fn test_family_runner_subscribe_resolves_alias_named_handle_to_protocol_system() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v2_alias".to_string(),
        });
    v2.expect_protocol_system()
        .return_const("uniswap_v2".to_string());

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v3_alias".to_string(),
        });
    v3.expect_protocol_system()
        .return_const("uniswap_v3".to_string());

    let dispatcher = FamilyBlockChangesDispatcher::new([
        FamilyBranchSpec {
            protocol_system: "uniswap_v2".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
        },
        FamilyBranchSpec {
            protocol_system: "uniswap_v3".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
        },
    ])
    .expect("dispatcher builds");

    let v2_subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let v3_subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let mut runner = family_runner_for_tests(
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]),
        SubstreamsStream::from_stream(Box::pin(stream::empty())),
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::clone(&v2_subscriptions)),
            ("uniswap_v3".to_string(), Arc::clone(&v3_subscriptions)),
        ]),
        dispatcher,
    );

    let (tx, _rx) = mpsc::channel(4);
    runner
        .subscribe(
            ExtractorIdentity { chain: Chain::Ethereum, name: "uniswap_v2_alias".to_string() },
            tx,
        )
        .await;

    assert_eq!(v2_subscriptions.lock().await.len(), 1);
    assert_eq!(v3_subscriptions.lock().await.len(), 0);
    assert_eq!(
        runner
            .branch_subscription_index()
            .get("uniswap_v2"),
        Some("uniswap_v2")
    );
}

#[test]
fn test_family_branch_subscription_index_learns_aliases_from_extractors() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v2_alias".to_string(),
        });
    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v3_alias".to_string(),
        });

    let extractors = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let mut index = FamilyBranchSubscriptionIndex::from_extractors(&extractors);

    assert_eq!(index.get("uniswap_v2"), Some("uniswap_v2"));
    assert_eq!(index.get("uniswap_v2_alias"), Some("uniswap_v2"));
    assert_eq!(
        index.resolve_or_learn(
            &ExtractorIdentity { chain: Chain::Ethereum, name: "uniswap_v2_alias".to_string() },
            &extractors,
        ),
        Some("uniswap_v2".to_string())
    );
    assert_eq!(index.get("uniswap_v2_alias"), Some("uniswap_v2"));
}

#[tokio::test]
async fn test_family_branch_runtime_wiring_uses_protocol_system_keys_and_emits_handles() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v2_alias".to_string(),
        });
    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v3_alias".to_string(),
        });

    let extractors = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let (control_tx, _control_rx) = mpsc::channel(4);

    let wiring = FamilyBranchRuntimeWiring::from_extractors(extractors, &control_tx);

    let subscription_keys = wiring
        .subscriptions
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    assert_eq!(
        subscription_keys,
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    for subscribers in wiring.subscriptions.values() {
        assert!(subscribers.lock().await.is_empty());
    }

    let handle_names = wiring
        .handles
        .into_iter()
        .map(|handle| handle.get_id().name)
        .collect::<HashSet<_>>();
    assert_eq!(
        handle_names,
        HashSet::from(["uniswap_v2_alias".to_string(), "uniswap_v3_alias".to_string()])
    );
}

#[test]
fn test_family_runner_extractors_by_protocol_system_uses_protocol_system_keys_for_aliased_members()
{
    let v2_config = ExtractorConfig {
        name: "uniswap_v2_alias".to_string(),
        protocol_system: "uniswap_v2".to_string(),
        ..Default::default()
    };
    let v3_config = ExtractorConfig {
        name: "uniswap_v3_alias".to_string(),
        protocol_system: "uniswap_v3".to_string(),
        ..Default::default()
    };
    let v2_extractor: Arc<dyn Extractor> = Arc::new(MockExtractor::new());
    let v3_extractor: Arc<dyn Extractor> = Arc::new(MockExtractor::new());

    let extractors = extractors_by_protocol_system(vec![
        (v2_config.protocol_system().to_string(), Arc::clone(&v2_extractor)),
        (v3_config.protocol_system().to_string(), Arc::clone(&v3_extractor)),
    ]);

    let keys = extractors
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    assert_eq!(keys, HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()]));
    assert!(Arc::ptr_eq(
        extractors
            .get("uniswap_v2")
            .expect("v2 extractor"),
        &v2_extractor
    ));
    assert!(Arc::ptr_eq(
        extractors
            .get("uniswap_v3")
            .expect("v3 extractor"),
        &v3_extractor
    ));
}

#[tokio::test]
async fn test_family_runner_build_managed_from_startup_preserves_protocol_system_keyed_shape() {
    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v2_alias".to_string(),
        });
    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "uniswap_v3_alias".to_string(),
        });

    let configs = make_uniswap_family_bootstrap_test_configs();
    let config_refs = configs.iter().collect::<Vec<_>>();
    let family_execution =
        resolved_family_execution_config_from_extractor_configs_for_tests(&config_refs)
            .expect("family execution should derive from test configs");
    let protocol_cache = ProtocolMemoryCache::new(
        Chain::Ethereum,
        chrono::Duration::seconds(60),
        Arc::new(MockGateway::new()),
    );
    let extractors = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let dispatcher = FamilyBlockChangesDispatcher::from_protocol_cache(
        &family_execution.branch_specs,
        &protocol_cache,
    )
    .await
    .expect("dispatcher should seed from protocol cache");
    let prepared_startup = PreparedFamilyRunnerStartup {
        extractors: extractors.clone(),
        stream: SubstreamsStream::from_stream(Box::pin(stream::empty())),
        runtime_state: FamilyRuntimeState::new(&extractors, dispatcher, protocol_cache.clone()),
    };

    let (runner, handles) = PreparedRuntimeTargetStartup::new(prepared_startup)
        .build_managed_runner(None, false)
        .expect("family-owned managed build should assemble runner from prepared startup");

    let handle_names = handles
        .into_iter()
        .map(|handle| handle.get_id().name)
        .collect::<HashSet<_>>();
    assert_eq!(
        handle_names,
        HashSet::from(["uniswap_v2_alias".to_string(), "uniswap_v3_alias".to_string()])
    );

    let runner = runner.into_typed::<FamilyExtractorRunner>();
    assert_eq!(
        runner
            .extractors
            .keys()
            .cloned()
            .collect::<HashSet<_>>(),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
    assert_eq!(
        runner
            .subscriptions
            .keys()
            .cloned()
            .collect::<HashSet<_>>(),
        HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
    );
}

#[tokio::test]
async fn test_prepare_family_managed_startup_uses_shared_spkg_and_preserves_protocol_system_shape()
{
    use alloy::primitives::Address as AlloyAddress;
    use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
    use tycho_storage::postgres::{builder::GatewayBuilder, testing::run_against_db};

    use crate::extractor::{
        chain_state::ChainState, managed_extractor_initialization::ManagedExtractorBuildContext,
        protocol_cache::ProtocolMemoryCache,
    };
    use crate::testing::write_temp_substreams_package_for_tests;

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
    });
    std::env::set_var("DATABASE_URL", &db_url);

    run_against_db(|_| async move {
        let chain = Chain::Ethereum;
        let shared_spkg_path =
            write_temp_substreams_package_for_tests("family-managed-startup-shared-spkg");
        let missing_member_v2_spkg = std::env::temp_dir()
            .join(format!("missing-member-v2-{}-{}.spkg", std::process::id(), "family-startup"))
            .to_string_lossy()
            .to_string();
        let missing_member_v3_spkg = std::env::temp_dir()
            .join(format!("missing-member-v3-{}-{}.spkg", std::process::id(), "family-startup"))
            .to_string_lossy()
            .to_string();

        let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&["uniswap_v2".to_string(), "uniswap_v3".to_string()])
            .build()
            .await
            .expect("Failed to create cached gateway");
        let direct_gw = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&["uniswap_v2".to_string(), "uniswap_v3".to_string()])
            .build_direct_gw()
            .await
            .expect("Failed to create direct gateway");

        let rpc = EthereumRpcClient::new("http://localhost:0000")
            .expect("Failed to create stub RPC client");
        let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);
        let protocol_cache =
            ProtocolMemoryCache::new(chain, chrono::Duration::seconds(900), Arc::new(direct_gw));
        protocol_cache
            .populate()
            .await
            .expect("populate protocol cache for family startup");

        let configs = [
            ExtractorConfig::new(
                "uniswap_v2_alias".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
                missing_member_v2_spkg,
                "v2_map_pool_events".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            )
            .with_protocol_system("uniswap_v2")
            .with_family_runtime(Some(FamilyRuntimeConfig {
                family: "uniswap".to_string(),
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some(family_output_module_for_tests("uniswap")),
                durability_scope: Some(crate::testing::family_durability_scope_for_tests(
                    "uniswap",
                )),
            })),
            ExtractorConfig::new(
                "uniswap_v3_alias".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
                missing_member_v3_spkg,
                "v3_map_protocol_changes".to_string(),
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
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some(family_output_module_for_tests("uniswap")),
                durability_scope: Some(crate::testing::family_durability_scope_for_tests(
                    "uniswap",
                )),
            })),
        ];
        let config_refs = configs.iter().collect::<Vec<_>>();
        let family =
            resolved_family_runtime_from_configs_for_tests(&config_refs, &shared_spkg_path);

        let prepared_startup = family
            .prepare_managed_startup(ManagedExtractorBuildContext {
                chain_state: ChainState::default(),
                endpoint_url: "https://mainnet.eth.streamingfast.io",
                s3_bucket: None,
                substreams_api_token: "",
                cached_gw: &cached_gw,
                database_insert_batch_size: 1000,
                token_pre_processor: &token_processor,
                protocol_cache: &protocol_cache,
                rpc_client: &rpc,
                final_block_only: false,
                partial_blocks: false,
                family_runtime_registry:
                    crate::extractor::family_registry::default_family_runtime_registry(),
            })
            .await
            .expect(
                "family startup should use shared spkg even when member spkg paths are missing",
            );

        assert_eq!(
            prepared_startup
                .extractors
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );

        let (runner, handles) = PreparedRuntimeTargetStartup::new(prepared_startup)
            .build_managed_runner(None, false)
            .expect("prepared startup should build a managed family runner");
        let handle_names = handles
            .into_iter()
            .map(|handle| handle.get_id().name)
            .collect::<HashSet<_>>();
        assert_eq!(
            handle_names,
            HashSet::from(["uniswap_v2_alias".to_string(), "uniswap_v3_alias".to_string()])
        );

        let runner = runner.into_typed::<FamilyExtractorRunner>();
        assert_eq!(
            runner
                .extractors
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );

        let _ = std::fs::remove_file(&shared_spkg_path);
    })
    .await;
}

#[tokio::test]
async fn test_prepare_family_managed_startup_reuses_persisted_shared_bootstrap_completion() {
    use alloy::primitives::Address as AlloyAddress;
    use tycho_common::{
        models::ExtractionState,
        storage::{ChainGateway, ExtractionStateGateway},
    };
    use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
    use tycho_storage::postgres::{builder::GatewayBuilder, testing::run_against_db};

    use crate::testing::write_temp_substreams_package_for_tests;
    use crate::{
        extractor::{
            chain_state::ChainState,
            managed_extractor_initialization::ManagedExtractorBuildContext,
            protocol_cache::ProtocolMemoryCache,
            protocol_extractor::{ExtractorGateway, ExtractorPgGateway},
        },
        substreams::mock::start_mock_substreams,
    };

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
    });
    std::env::set_var("DATABASE_URL", &db_url);

    run_against_db(|_| async move {
        let chain = Chain::Ethereum;
        let (captured, addr) = start_mock_substreams().await;
        let shared_spkg_path =
            write_temp_substreams_package_for_tests("family-managed-startup-bootstrap-resume");
        let family_scope = crate::testing::family_durability_scope_for_tests("uniswap");

        let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&["uniswap_v2".to_string(), "uniswap_v3".to_string()])
            .build()
            .await
            .expect("Failed to create cached gateway");
        let direct_gw = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&["uniswap_v2".to_string(), "uniswap_v3".to_string()])
            .build_direct_gw()
            .await
            .expect("Failed to create direct gateway");

        let rpc = EthereumRpcClient::new("http://localhost:0000")
            .expect("Failed to create stub RPC client");
        let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);
        let protocol_cache = ProtocolMemoryCache::new(
            chain,
            chrono::Duration::seconds(900),
            Arc::new(direct_gw),
        );
        protocol_cache
            .populate()
            .await
            .expect("populate protocol cache for bootstrap resume startup");

        let persisted_block = Block {
            number: 42,
            chain,
            hash: Bytes::from(vec![0x42; 32]),
            parent_hash: Bytes::from(vec![0x41; 32]),
            ts: chrono::NaiveDateTime::default(),
        };
        let bootstrap_gateway = ExtractorPgGateway::new(
            "uniswap_v2",
            chain,
            1000,
            cached_gw.clone(),
            Some(family_scope.clone()),
        );
        cached_gw
            .start_transaction(&persisted_block, Some("seed-shared-bootstrap-startup-progress"))
            .await;
        cached_gw
            .upsert_block(std::slice::from_ref(&persisted_block))
            .await
            .expect("persist shared bootstrap marker block");
        cached_gw
            .save_state(&ExtractionState::new(
                family_scope.clone(),
                chain,
                None,
                b"bootstrap@42",
                persisted_block.hash.clone(),
            ))
            .await
            .expect("persist shared family bootstrap marker cursor");
        cached_gw
            .commit_transaction(0)
            .await
            .expect("commit shared bootstrap marker cursor state");
        bootstrap_gateway
            .save_bootstrap_state(42, persisted_block.hash.clone())
            .await
            .expect("persist shared bootstrap completion state");

        let missing_member_v2_spkg = std::env::temp_dir()
            .join(format!(
                "missing-member-v2-bootstrap-resume-{}-{}.spkg",
                std::process::id(),
                "family-startup"
            ))
            .to_string_lossy()
            .to_string();
        let missing_member_v3_spkg = std::env::temp_dir()
            .join(format!(
                "missing-member-v3-bootstrap-resume-{}-{}.spkg",
                std::process::id(),
                "family-startup"
            ))
            .to_string_lossy()
            .to_string();
        let configs = [
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new(
                    "uniswap_v2_pool".to_string(),
                    FinancialType::Swap,
                )],
                missing_member_v2_spkg,
                "v2_map_pool_events".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::from([("map_pool_events".to_string(), "factory=0x01".to_string())]),
                Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                            .to_string(),
                }),
            )
            .with_protocol_system("uniswap_v2")
            .with_family_runtime(Some(FamilyRuntimeConfig {
                family: "uniswap".to_string(),
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some(family_output_module_for_tests("uniswap")),
                durability_scope: Some(family_scope.clone()),
            })),
            ExtractorConfig::new(
                "uniswap_v3".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new(
                    "uniswap_v3_pool".to_string(),
                    FinancialType::Swap,
                )],
                missing_member_v3_spkg,
                "v3_map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
                Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678"
                            .to_string(),
                }),
            )
            .with_protocol_system("uniswap_v3")
            .with_family_runtime(Some(FamilyRuntimeConfig {
                family: "uniswap".to_string(),
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some(family_output_module_for_tests("uniswap")),
                durability_scope: Some(family_scope.clone()),
            })),
        ];
        let config_refs = configs.iter().collect::<Vec<_>>();
        let family =
            resolved_family_runtime_from_configs_for_tests(&config_refs, &shared_spkg_path);

        let mut prepared_startup = family
            .prepare_managed_startup(
                ManagedExtractorBuildContext {
                    chain_state: ChainState::default(),
                    endpoint_url: &format!("http://{addr}"),
                    s3_bucket: None,
                    substreams_api_token: "",
                    cached_gw: &cached_gw,
                    database_insert_batch_size: 1000,
                    token_pre_processor: &token_processor,
                    protocol_cache: &protocol_cache,
                    rpc_client: &rpc,
                    final_block_only: false,
                    partial_blocks: false,
                    family_runtime_registry:
                        crate::extractor::family_registry::default_family_runtime_registry(),
                },
            )
            .await
            .expect("fresh startup should reuse persisted shared bootstrap completion");

        let _ = prepared_startup
            .stream
            .next()
            .await
            .expect("mock stream should yield one terminal response")
            .expect("mock stream response should be ok");

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
        assert_eq!(
            requests[0].start_block_num, 43,
            "fresh startup should begin at bootstrap block + 1 when shared completion already exists"
        );
        assert!(
            requests[0].start_cursor.is_empty(),
            "bootstrap marker resume should not send a stream cursor on fresh startup"
        );
        assert_eq!(requests[0].output_module, family_output_module_for_tests("uniswap"));

        let _ = std::fs::remove_file(&shared_spkg_path);
    })
    .await;
}

#[tokio::test]
async fn test_prepare_family_managed_startup_injects_custom_registry_decoders() {
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use crate::extractor::{
        extractor_config::{ExtractorConfig, ProtocolTypeConfig},
        family_managed_startup::family_auxiliary_protocol_message_decoders_by_protocol_system,
        family_registry::{FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec},
        family_runtime_metadata::FamilyRuntimeConfig,
        family_runtime_planning::build_resolved_runtime_targets_with_registry,
        protocol_message_registry::{
            AuxiliaryProtocolMessageBuildFuture, AuxiliaryProtocolMessageContext,
            AuxiliaryProtocolMessageDecoder,
        },
        runtime_target_planning::ResolvedRuntimeTarget,
        ExtractionError,
    };

    fn build_future_events_for_family_startup_test<'a>(
        _context: &'a dyn AuxiliaryProtocolMessageContext,
        _value: &'a [u8],
        _finalized_block_height: u64,
        _partial_block_index: Option<u32>,
    ) -> AuxiliaryProtocolMessageBuildFuture<'a> {
        Box::pin(async {
            Err(ExtractionError::Unknown(
                "test-only decoder should not run".to_string(),
            ))
        })
    }

    const FUTURE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
        &[AuxiliaryProtocolMessageDecoder {
            protocol_system: "future_v1",
            type_url_suffix: "FutureEvents",
            build_block_changes: build_future_events_for_family_startup_test,
        }];
    const FUTURE_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec::new(
        "future_swap",
        &[
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
        "map_future_swap_family_protocol_changes",
        "future_swap_family",
        "family::future_swap",
        None,
        FUTURE_DECODERS,
    );

    let chain = Chain::Ethereum;
    let registry = FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]);
    let shared_spkg_path = "/tmp/future-swap-family-test.spkg".to_string();

    let extractors = HashMap::from([
        (
            "future_v1_alias".to_string(),
            ExtractorConfig::new(
                "future_v1_alias".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new("future_v1_pool".to_string(), FinancialType::Swap)],
                std::env::temp_dir()
                    .join(format!(
                        "missing-member-future-v1-family-startup-{}-test.spkg",
                        std::process::id()
                    ))
                    .to_string_lossy()
                    .to_string(),
                "map_future_pool_events".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            )
            .with_protocol_system("future_v1")
            .with_family_runtime(Some(FamilyRuntimeConfig {
                family: "future_swap".to_string(),
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                durability_scope: Some("family::future_swap".to_string()),
            })),
        ),
        (
            "future_v2_alias".to_string(),
            ExtractorConfig::new(
                "future_v2_alias".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new("future_v2_pool".to_string(), FinancialType::Swap)],
                std::env::temp_dir()
                    .join(format!(
                        "missing-member-future-v2-family-startup-{}-test.spkg",
                        std::process::id()
                    ))
                    .to_string_lossy()
                    .to_string(),
                "map_future_pool_events_v2".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            )
            .with_protocol_system("future_v2")
            .with_family_runtime(Some(FamilyRuntimeConfig {
                family: "future_swap".to_string(),
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                durability_scope: Some("family::future_swap".to_string()),
            })),
        ),
    ]);

    let family = build_resolved_runtime_targets_with_registry(&extractors, registry)
        .expect("future family runtime target should resolve")
        .into_iter()
        .find_map(|target| match target {
            ResolvedRuntimeTarget::Family(family) => Some(family),
            ResolvedRuntimeTarget::Standalone(_) => None,
        })
        .expect("custom family target should be present");

    let decoders_by_protocol_system =
        family_auxiliary_protocol_message_decoders_by_protocol_system(&family);

    assert_eq!(decoders_by_protocol_system.len(), 2);
    let future_v1_decoders = decoders_by_protocol_system
        .get("future_v1")
        .expect("future_v1 decoders should be present");
    assert_eq!(future_v1_decoders.len(), 1);
    assert_eq!(future_v1_decoders[0].protocol_system, "future_v1");
    assert_eq!(future_v1_decoders[0].type_url_suffix, "FutureEvents");
    assert_eq!(
        decoders_by_protocol_system
            .get("future_v2")
            .expect("future_v2 decoder slot should be present for the family member")
            .len(),
        1,
        "family startup wiring should carry the family-level auxiliary decoder set across all members that share the stream"
    );
}

#[tokio::test]
async fn test_build_all_extractors_managed_startup_collapses_custom_family_into_one_shared_runner()
{
    use std::collections::{HashMap, HashSet};

    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use crate::extractor::{
        extractor_config::{ExtractorConfig, ProtocolTypeConfig},
        family_runner_wiring::FamilyBranchRuntimeWiring,
        family_registry::{FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec},
        family_runtime_metadata::FamilyRuntimeConfig,
        family_runtime_planning::build_resolved_runtime_targets_with_registry,
        runtime_target_planning::ResolvedRuntimeTarget,
    };

    const FUTURE_FAMILY: FamilyRuntimeSpec = FamilyRuntimeSpec::new(
        "future_swap",
        &[
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
        "map_future_swap_family_protocol_changes",
        "future_swap_family",
        "family::future_swap",
        None,
        &[],
    );

    let chain = Chain::Ethereum;
    let shared_spkg_path = "/tmp/future-family-shared-startup.spkg".to_string();

    let configs = HashMap::from([
        (
            "future_v1_alias".to_string(),
            ExtractorConfig::new(
                "future_v1_alias".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new("future_v1_pool".to_string(), FinancialType::Swap)],
                "/tmp/future-v1-member-startup.spkg".to_string(),
                "map_future_pool_events_v1".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            )
            .with_protocol_system("future_v1")
            .with_family_runtime(Some(FamilyRuntimeConfig {
                family: "future_swap".to_string(),
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                durability_scope: Some("family::future_swap".to_string()),
            })),
        ),
        (
            "future_v2_alias".to_string(),
            ExtractorConfig::new(
                "future_v2_alias".to_string(),
                chain,
                ImplementationType::Custom,
                1,
                42,
                None,
                vec![ProtocolTypeConfig::new("future_v2_pool".to_string(), FinancialType::Swap)],
                "/tmp/future-v2-member-startup.spkg".to_string(),
                "map_future_pool_events_v2".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            )
            .with_protocol_system("future_v2")
            .with_family_runtime(Some(FamilyRuntimeConfig {
                family: "future_swap".to_string(),
                shared_spkg: Some(shared_spkg_path.clone()),
                shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                durability_scope: Some("family::future_swap".to_string()),
            })),
        ),
    ]);

    let registry = FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]);
    let runtime_targets = build_resolved_runtime_targets_with_registry(&configs, registry)
        .expect("runtime targets should resolve custom future family startup");
    assert_eq!(
        runtime_targets.len(),
        1,
        "custom future family should collapse into a single shared runtime target"
    );

    let family = runtime_targets
        .into_iter()
        .find_map(|target| match target {
            ResolvedRuntimeTarget::Family(family) => Some(family),
            ResolvedRuntimeTarget::Standalone(_) => None,
        })
        .expect("custom future family target should be present");
    assert_eq!(
        family
            .extractor_configs
            .iter()
            .map(|config| config.protocol_system().to_string())
            .collect::<HashSet<_>>(),
        HashSet::from(["future_v1".to_string(), "future_v2".to_string()]),
        "custom future family target should cover both member protocol systems"
    );

    let mut v1 = MockExtractor::new();
    v1.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "future_v1_alias".to_string(),
        });
    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .return_const(ExtractorIdentity {
            chain: Chain::Ethereum,
            name: "future_v2_alias".to_string(),
        });

    let extractors = HashMap::from([
        ("future_v1".to_string(), Arc::new(v1) as Arc<dyn Extractor>),
        ("future_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
    ]);
    let (control_tx, _control_rx) = mpsc::channel(8);
    let wiring = FamilyBranchRuntimeWiring::from_extractors(extractors, &control_tx);

    assert_eq!(
        wiring
            .extractors
            .keys()
            .cloned()
            .collect::<HashSet<_>>(),
        HashSet::from(["future_v1".to_string(), "future_v2".to_string()]),
        "custom family runner should remain keyed by protocol_system under the shared runner"
    );

    let handle_names = wiring
        .handles
        .into_iter()
        .map(|handle| handle.get_id().name)
        .collect::<HashSet<_>>();
    assert_eq!(
        handle_names,
        HashSet::from(["future_v1_alias".to_string(), "future_v2_alias".to_string()]),
        "startup should preserve alias-shaped handle identities while collapsing custom family members into one shared runner"
    );
}

#[tokio::test]
async fn test_build_all_extractors_managed_startup_supports_family_and_standalone_targets_together()
{
    use std::collections::{HashMap, HashSet};

    use alloy::primitives::Address as AlloyAddress;
    use tycho_common::models::FinancialType;
    use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
    use tycho_storage::postgres::{builder::GatewayBuilder, testing::run_against_db};

    use crate::{
        config::ExtractorConfigs,
        extractor::{
            chain_state::ChainState,
            extractor_config::ExtractorConfig,
            family_registry::default_family_runtime_registry,
            family_runtime_metadata::FamilyRuntimeConfig,
        },
        testing::{
            build_all_extractors_for_tests, family_durability_scope_for_tests,
            family_output_module_for_tests, write_temp_substreams_package_for_tests,
            BuildExtractorsTestContext,
        },
    };

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
    });
    std::env::set_var("DATABASE_URL", &db_url);

    run_against_db(|_| async move {
        let chain = Chain::Ethereum;
        let protocol_systems = vec![
            "curve".to_string(),
            "uniswap_v2".to_string(),
            "uniswap_v3".to_string(),
        ];
        let shared_spkg_path =
            write_temp_substreams_package_for_tests("family-and-standalone-shared-startup");
        let standalone_spkg_path =
            write_temp_substreams_package_for_tests("family-and-standalone-standalone-startup");
        let missing_member_v2_spkg = std::env::temp_dir()
            .join(format!(
                "missing-member-v2-family-plus-standalone-{}-{}.spkg",
                std::process::id(),
                "startup"
            ))
            .to_string_lossy()
            .to_string();
        let missing_member_v3_spkg = std::env::temp_dir()
            .join(format!(
                "missing-member-v3-family-plus-standalone-{}-{}.spkg",
                std::process::id(),
                "startup"
            ))
            .to_string_lossy()
            .to_string();

        let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&protocol_systems)
            .build()
            .await
            .expect("Failed to create cached gateway");

        let rpc = EthereumRpcClient::new("http://localhost:0000")
            .expect("Failed to create stub RPC client");
        let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

        let extractors = ExtractorConfigs::new(HashMap::from([
            (
                "uniswap_v2_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v2_alias".to_string(),
                    chain,
                    ImplementationType::Custom,
                    1,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "uniswap_v2_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    missing_member_v2_spkg,
                    "v2_map_pool_events".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v2")
                .with_family_runtime(Some(FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: Some(shared_spkg_path.clone()),
                    shared_module: Some(family_output_module_for_tests("uniswap")),
                    durability_scope: Some(family_durability_scope_for_tests("uniswap")),
                })),
            ),
            (
                "uniswap_v3_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v3_alias".to_string(),
                    chain,
                    ImplementationType::Custom,
                    1,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    missing_member_v3_spkg,
                    "v3_map_protocol_changes".to_string(),
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
                    shared_spkg: Some(shared_spkg_path.clone()),
                    shared_module: Some(family_output_module_for_tests("uniswap")),
                    durability_scope: Some(family_durability_scope_for_tests("uniswap")),
                })),
            ),
            (
                "curve_alias".to_string(),
                ExtractorConfig::new(
                    "curve_alias".to_string(),
                    chain,
                    ImplementationType::Custom,
                    1,
                    84,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "curve_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    standalone_spkg_path.clone(),
                    "curve_map_protocol_changes".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
        ]));
        let registry = default_family_runtime_registry();
        let runtime_targets = extractors
            .resolved_runtime_targets_with_registry(registry)
            .expect("runtime targets should resolve mixed family + standalone startup");

        assert_eq!(runtime_targets.len(), 2, "expected one family target and one standalone target");

        let (runners, handles) = build_all_extractors_for_tests(
            &extractors,
            BuildExtractorsTestContext {
                chain_state: ChainState::default(),
                endpoint_url: "https://mainnet.eth.streamingfast.io",
                s3_bucket: None,
                substreams_api_token: "",
                cached_gw: &cached_gw,
                database_insert_batch_size: 1000,
                token_pre_processor: &token_processor,
                rpc_client: &rpc,
                runtime: None,
                partial_blocks: false,
                family_runtime_registry: registry,
            },
        )
        .await
        .expect("mixed runtime targets should build one family runner plus one standalone runner");

        assert_eq!(runners.len(), 2, "expected exactly two managed runners");
        assert_eq!(
            runners
                .iter()
                .filter(|runner| runner.kind() == ManagedRunnerKind::Family)
                .count(),
            1,
            "expected exactly one managed family runner"
        );
        assert_eq!(
            runners
                .iter()
                .filter(|runner| runner.kind() == ManagedRunnerKind::Single)
                .count(),
            1,
            "expected exactly one managed standalone runner"
        );

        let family_runner = runners
            .iter()
            .find_map(|runner| runner.downcast_ref::<FamilyExtractorRunner>())
            .expect("family runner should be present");
        assert_eq!(
            family_runner
                .extractors
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()]),
            "family runner should remain keyed by protocol_system"
        );

        let handle_names = handles
            .into_iter()
            .map(|handle| handle.get_id().name)
            .collect::<HashSet<_>>();
        assert_eq!(
            handle_names,
            HashSet::from([
                "curve_alias".to_string(),
                "uniswap_v2_alias".to_string(),
                "uniswap_v3_alias".to_string(),
            ]),
            "startup should preserve alias-shaped handle identities while collapsing family members into one shared runner"
        );

        let _ = std::fs::remove_file(&shared_spkg_path);
        let _ = std::fs::remove_file(&standalone_spkg_path);
    })
    .await;
}

#[tokio::test]
async fn test_resolved_runtime_targets_prepare_startup_prepares_family_and_standalone_targets_together(
) {
    use std::collections::HashMap;

    use alloy::primitives::Address as AlloyAddress;
    use tycho_common::models::FinancialType;
    use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
    use tycho_storage::postgres::{builder::GatewayBuilder, testing::run_against_db};

    use crate::{
        config::ExtractorConfigs,
        extractor::{
            chain_state::ChainState,
            extractor_config::ExtractorConfig,
            family_registry::default_family_runtime_registry,
            family_runtime_metadata::FamilyRuntimeConfig,
            startup::ResolvedRuntimeTargetsBuildContext,
        },
        testing::{
            family_durability_scope_for_tests, family_output_module_for_tests,
            write_temp_substreams_package_for_tests,
        },
    };

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
    });
    std::env::set_var("DATABASE_URL", &db_url);

    run_against_db(|_| async move {
        let chain = Chain::Ethereum;
        let protocol_systems = vec![
            "curve".to_string(),
            "uniswap_v2".to_string(),
            "uniswap_v3".to_string(),
        ];
        let shared_spkg_path =
            write_temp_substreams_package_for_tests("family-and-standalone-prepare-startup");
        let standalone_spkg_path =
            write_temp_substreams_package_for_tests("standalone-prepare-startup");
        let missing_member_v2_spkg = std::env::temp_dir()
            .join(format!(
                "missing-member-v2-family-plus-standalone-{}-{}.spkg",
                std::process::id(),
                "prepare-startup"
            ))
            .to_string_lossy()
            .to_string();
        let missing_member_v3_spkg = std::env::temp_dir()
            .join(format!(
                "missing-member-v3-family-plus-standalone-{}-{}.spkg",
                std::process::id(),
                "prepare-startup"
            ))
            .to_string_lossy()
            .to_string();

        let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
            .set_chains(&[chain])
            .set_protocol_systems(&protocol_systems)
            .build()
            .await
            .expect("Failed to create cached gateway");

        let rpc = EthereumRpcClient::new("http://localhost:0000")
            .expect("Failed to create stub RPC client");
        let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

        let extractors = ExtractorConfigs::new(HashMap::from([
            (
                "uniswap_v2_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v2_alias".to_string(),
                    chain,
                    ImplementationType::Custom,
                    1,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "uniswap_v2_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    missing_member_v2_spkg,
                    "v2_map_pool_events".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("uniswap_v2")
                .with_family_runtime(Some(FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: Some(shared_spkg_path.clone()),
                    shared_module: Some(family_output_module_for_tests("uniswap")),
                    durability_scope: Some(family_durability_scope_for_tests("uniswap")),
                })),
            ),
            (
                "uniswap_v3_alias".to_string(),
                ExtractorConfig::new(
                    "uniswap_v3_alias".to_string(),
                    chain,
                    ImplementationType::Custom,
                    1,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    missing_member_v3_spkg,
                    "v3_map_protocol_changes".to_string(),
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
                    shared_spkg: Some(shared_spkg_path.clone()),
                    shared_module: Some(family_output_module_for_tests("uniswap")),
                    durability_scope: Some(family_durability_scope_for_tests("uniswap")),
                })),
            ),
            (
                "curve_alias".to_string(),
                ExtractorConfig::new(
                    "curve_alias".to_string(),
                    chain,
                    ImplementationType::Custom,
                    1,
                    84,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "curve_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    standalone_spkg_path.clone(),
                    "curve_map_protocol_changes".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_protocol_system("curve"),
            ),
        ]));
        let registry = default_family_runtime_registry();
        let runtime_targets = extractors
            .resolved_runtime_targets_with_registry(registry)
            .expect("runtime targets should resolve mixed family + standalone startup");

        let prepared_startup = runtime_targets
            .prepare_startup(&ResolvedRuntimeTargetsBuildContext {
                chain_state: ChainState::default(),
                endpoint_url: "https://mainnet.eth.streamingfast.io",
                s3_bucket: None,
                substreams_api_token: "",
                cached_gw: &cached_gw,
                database_insert_batch_size: 1000,
                token_pre_processor: &token_processor,
                rpc_client: &rpc,
                runtime: None,
                final_block_only: false,
                partial_blocks: false,
                family_runtime_registry: registry,
            })
            .await
            .expect("mixed runtime targets should prepare one family startup plus one standalone startup");

        assert_eq!(
            prepared_startup.prepared_targets.len(),
            2,
            "expected exactly two prepared startup artifacts"
        );
        assert_eq!(
            prepared_startup
                .prepared_targets
                .iter()
                .filter(|startup| startup.kind() == PreparedRuntimeTargetKind::Family)
                .count(),
            1,
            "expected exactly one prepared family startup"
        );
        assert_eq!(
            prepared_startup
                .prepared_targets
                .iter()
                .filter(|startup| startup.kind() == PreparedRuntimeTargetKind::Standalone)
                .count(),
            1,
            "expected exactly one prepared standalone startup"
        );

        let (runners, handles) = prepared_startup
            .build_managed_runners()
            .expect("prepared mixed runtime targets should build one family runner plus one standalone runner");

        assert_eq!(runners.len(), 2, "expected exactly two managed runners");
        assert_eq!(
            runners
                .iter()
                .filter(|runner| runner.kind() == ManagedRunnerKind::Family)
                .count(),
            1,
            "expected exactly one managed family runner from prepared startup"
        );
        assert_eq!(
            runners
                .iter()
                .filter(|runner| runner.kind() == ManagedRunnerKind::Single)
                .count(),
            1,
            "expected exactly one managed standalone runner from prepared startup"
        );
        assert_eq!(handles.len(), 3, "expected two family handles plus one standalone handle");

        let _ = std::fs::remove_file(&shared_spkg_path);
        let _ = std::fs::remove_file(&standalone_spkg_path);
    })
    .await;
}
