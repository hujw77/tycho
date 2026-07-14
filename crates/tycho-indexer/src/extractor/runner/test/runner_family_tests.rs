use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures03::stream;
use prost::Message;
use tokio::sync::{mpsc, Mutex};
use tycho_common::models::{blockchain::BlockAggregatedChanges, Chain, ExtractorIdentity};
use tycho_substreams::pb::tycho::evm::v1 as substreams;

use super::*;
use crate::{
    extractor::{protocol_cache::ProtocolMemoryCache, ExtractionError, Extractor},
    pb::sf::substreams::rpc::v2::BlockScopedData,
    substreams::stream::{BlockResponse, SubstreamsStream},
};

#[tokio::test]
async fn test_family_runner_dispatches_shared_stream_into_branch_extractors() {
    let family_block = make_family_block_scoped_data();

    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
    v2.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(
                decoded.changes[0]
                    .component_changes
                    .len(),
                1
            );
            assert_eq!(decoded.changes[0].component_changes[0].id, "v2-pool");
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });
    v2.expect_flush()
        .once()
        .returning(|| Ok(()));

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
    v3.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(
                decoded.changes[0]
                    .component_changes
                    .len(),
                1
            );
            assert_eq!(decoded.changes[0].component_changes[0].id, "v3-pool");
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });
    v3.expect_flush()
        .once()
        .returning(|| Ok(()));

    let v2_subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let v3_subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let (v2_tx, mut v2_rx) = mpsc::channel(4);
    let (v3_tx, mut v3_rx) = mpsc::channel(4);
    v2_subscriptions
        .lock()
        .await
        .insert(0, v2_tx);
    v3_subscriptions
        .lock()
        .await
        .insert(0, v3_tx);

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
        SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
            Ok(BlockResponse::New(family_block)),
            Ok(BlockResponse::Ended),
        ]))),
        HashMap::from([
            ("uniswap_v2".to_string(), v2_subscriptions),
            ("uniswap_v3".to_string(), v3_subscriptions),
        ]),
        dispatcher,
    );

    runner.run().await.unwrap().unwrap();

    assert!(v2_rx.recv().await.is_some(), "v2 subscriber should receive a message");
    assert!(v3_rx.recv().await.is_some(), "v3 subscriber should receive a message");
    assert!(v2_rx.try_recv().is_err(), "v2 should receive exactly one message");
    assert!(v3_rx.try_recv().is_err(), "v3 should receive exactly one message");
}

#[tokio::test]
async fn test_family_runner_does_not_propagate_partial_branch_results_when_later_branch_fails() {
    let family_block = make_family_block_scoped_data();

    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
    v2.expect_handle_tick_scoped_data()
        .once()
        .returning(|_| Ok(Some(Arc::new(BlockAggregatedChanges::default()))));

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
    v3.expect_handle_tick_scoped_data()
        .once()
        .returning(|_| Err(ExtractionError::Unknown("simulated v3 failure".to_string())));

    let v2_subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let v3_subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let (v2_tx, mut v2_rx) = mpsc::channel(4);
    let (v3_tx, mut v3_rx) = mpsc::channel(4);
    v2_subscriptions
        .lock()
        .await
        .insert(0, v2_tx);
    v3_subscriptions
        .lock()
        .await
        .insert(0, v3_tx);

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
        SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
            Ok(BlockResponse::New(family_block)),
            Ok(BlockResponse::Ended),
        ]))),
        HashMap::from([
            ("uniswap_v2".to_string(), v2_subscriptions),
            ("uniswap_v3".to_string(), v3_subscriptions),
        ]),
        dispatcher,
    );

    let err = runner
        .run()
        .await
        .unwrap()
        .expect_err("family runner should fail");
    assert!(
        matches!(err, ExtractionError::Unknown(ref message) if message == "simulated v3 failure"),
        "unexpected error: {err:?}"
    );
    assert!(
        v2_rx.try_recv().is_err(),
        "v2 subscriber should not receive a message from a failed family block"
    );
    assert!(
        v3_rx.try_recv().is_err(),
        "v3 subscriber should not receive a message from a failed family block"
    );
}

#[tokio::test]
async fn test_family_runner_reconnects_and_dispatches_follow_up_updates() {
    use std::sync::Mutex as StdMutex;

    use crate::{
        pb::sf::substreams::rpc::v2::{response::Message, Response, SessionInit},
        substreams::{
            mock::{start_scripted_mock_substreams, MockSubstreamsScript},
            SubstreamsEndpoint,
        },
    };

    fn session_response(start_block: u64) -> Response {
        Response {
            message: Some(Message::Session(SessionInit {
                trace_id: format!("trace-{start_block}"),
                resolved_start_block: start_block,
                linear_handoff_block: start_block,
                max_parallel_workers: 1,
                attestation_public_key: String::new(),
                chain_head: start_block,
                blocks_to_process_before_start_block: 0,
                effective_blocks_to_process_before_start_block: 0,
                blocks_to_process_after_start_block: 0,
                effective_blocks_to_process_after_start_block: 0,
            })),
        }
    }

    fn block_response(block: BlockScopedData) -> Response {
        Response { message: Some(Message::BlockScopedData(block)) }
    }

    let first_block = make_family_block_scoped_data();
    let second_block = make_family_follow_up_block_scoped_data(43, "cursor-43");
    let (captured, addr) = start_scripted_mock_substreams(vec![
        MockSubstreamsScript {
            responses: vec![session_response(42), block_response(first_block.clone())],
            grpc_status: "13",
            grpc_message: Some("forced-reconnect"),
        },
        MockSubstreamsScript {
            responses: vec![session_response(43), block_response(second_block.clone())],
            grpc_status: "0",
            grpc_message: None,
        },
    ])
    .await;

    let v2_calls = Arc::new(StdMutex::new(0usize));
    let v3_calls = Arc::new(StdMutex::new(0usize));

    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
    v2.expect_flush()
        .once()
        .returning(|| Ok(()));
    {
        let v2_calls = v2_calls.clone();
        v2.expect_handle_tick_scoped_data()
            .times(2)
            .returning(move |inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
                let mut call = v2_calls.lock().unwrap();
                *call += 1;
                match *call {
                    1 => {
                        assert_eq!(inp.cursor, "cursor-42");
                        assert_eq!(
                            decoded.changes[0]
                                .component_changes
                                .len(),
                            1
                        );
                        assert_eq!(decoded.changes[0].component_changes[0].id, "v2-pool");
                    }
                    2 => {
                        assert_eq!(inp.cursor, "cursor-43");
                        assert_eq!(
                            decoded.changes[0]
                                .component_changes
                                .len(),
                            0
                        );
                        assert_eq!(decoded.changes[0].entity_changes.len(), 1);
                        assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v2-pool");
                    }
                    _ => panic!("unexpected v2 call count"),
                }
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });
    }

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
    v3.expect_flush()
        .once()
        .returning(|| Ok(()));
    {
        let v3_calls = v3_calls.clone();
        v3.expect_handle_tick_scoped_data()
            .times(2)
            .returning(move |inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
                let mut call = v3_calls.lock().unwrap();
                *call += 1;
                match *call {
                    1 => {
                        assert_eq!(inp.cursor, "cursor-42");
                        assert_eq!(
                            decoded.changes[0]
                                .component_changes
                                .len(),
                            1
                        );
                        assert_eq!(decoded.changes[0].component_changes[0].id, "v3-pool");
                    }
                    2 => {
                        assert_eq!(inp.cursor, "cursor-43");
                        assert_eq!(
                            decoded.changes[0]
                                .component_changes
                                .len(),
                            0
                        );
                        assert_eq!(decoded.changes[0].entity_changes.len(), 1);
                        assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v3-pool");
                    }
                    _ => panic!("unexpected v3 call count"),
                }
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });
    }

    let endpoint = Arc::new(
        SubstreamsEndpoint::new(format!("http://{addr}"), None)
            .await
            .expect("endpoint builds"),
    );
    let stream = SubstreamsStream::new(
        endpoint,
        None,
        None,
        family_output_module_for_tests("uniswap"),
        42,
        0,
        false,
        family_shared_extractor_id_for_tests("uniswap", Chain::Ethereum),
        false,
        HashMap::new(),
    );
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
        stream,
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
        ]),
        dispatcher,
    );

    runner.run().await.unwrap().unwrap();

    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 2, "expected initial request and reconnect");
    assert!(requests[0].start_cursor.is_empty());
    assert_eq!(requests[1].start_cursor, "cursor-42");
    assert_eq!(*v2_calls.lock().unwrap(), 2);
    assert_eq!(*v3_calls.lock().unwrap(), 2);
}

#[tokio::test]
async fn test_family_runner_routes_existing_components_after_restart_style_preseed() {
    let follow_up_block = make_family_follow_up_block_scoped_data(43, "cursor-43");

    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
    v2.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
            assert_eq!(inp.cursor, "cursor-43");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(
                decoded.changes[0]
                    .component_changes
                    .len(),
                0
            );
            assert_eq!(decoded.changes[0].entity_changes.len(), 1);
            assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v2-pool");
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
    v3.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
            assert_eq!(inp.cursor, "cursor-43");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(
                decoded.changes[0]
                    .component_changes
                    .len(),
                0
            );
            assert_eq!(decoded.changes[0].entity_changes.len(), 1);
            assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v3-pool");
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });

    let dispatcher = {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
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
        dispatcher.register_component_systems(HashMap::from([
            ("v2-pool".to_string(), "uniswap_v2".to_string()),
            ("v3-pool".to_string(), "uniswap_v3".to_string()),
        ]));
        dispatcher
    };

    let runner = family_runner_for_tests(
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]),
        SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
            Ok(BlockResponse::New(follow_up_block)),
            Ok(BlockResponse::Ended),
        ]))),
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
        ]),
        dispatcher,
    );

    runner.run().await.unwrap().unwrap();
}

#[tokio::test]
async fn test_family_runner_routes_contract_and_storage_follow_ups_after_restart_style_preseed() {
    let follow_up_block =
        make_family_contract_and_storage_follow_up_block_scoped_data(44, "cursor-44");

    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
    v2.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
            assert_eq!(inp.cursor, "cursor-44");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(
                decoded.changes[0]
                    .contract_changes
                    .len(),
                1
            );
            assert_eq!(decoded.changes[0].contract_changes[0].address, vec![0x44; 20]);
            assert!(decoded.storage_changes.is_empty());
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
    v3.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
            assert_eq!(inp.cursor, "cursor-44");
            assert!(decoded.changes.is_empty());
            assert_eq!(decoded.storage_changes.len(), 1);
            assert_eq!(decoded.storage_changes[0].storage_changes[0].address, vec![0x55; 20]);
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });

    let dispatcher = {
        let mut dispatcher = FamilyBlockChangesDispatcher::new([
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
        dispatcher.register_contract_systems(HashMap::from([
            (vec![0x44; 20], "uniswap_v2".to_string()),
            (vec![0x55; 20], "uniswap_v3".to_string()),
        ]));
        dispatcher
    };

    let runner = family_runner_for_tests(
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]),
        SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
            Ok(BlockResponse::New(follow_up_block)),
            Ok(BlockResponse::Ended),
        ]))),
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
        ]),
        dispatcher,
    );

    runner.run().await.unwrap().unwrap();
}

#[tokio::test]
async fn test_family_dispatcher_from_protocol_cache_preseeds_component_and_contract_ownership() {
    let protocol_cache = ProtocolMemoryCache::new(
        Chain::Ethereum,
        chrono::Duration::seconds(60),
        Arc::new(MockGateway::new()),
    );
    protocol_cache
        .add_components(vec![
            ProtocolComponent::new(
                "v2-pool",
                "uniswap_v2",
                "pool",
                Chain::Ethereum,
                vec![],
                vec![Bytes::from(vec![0x44; 20])],
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
            ProtocolComponent::new(
                "v3-pool",
                "uniswap_v3",
                "pool",
                Chain::Ethereum,
                vec![],
                vec![Bytes::from(vec![0x55; 20])],
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
        ])
        .await
        .expect("seed protocol cache");

    let branch_specs = vec![
        FamilyBranchSpec {
            protocol_system: "uniswap_v2".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
        },
        FamilyBranchSpec {
            protocol_system: "uniswap_v3".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
        },
    ];
    let mut dispatcher =
        FamilyBlockChangesDispatcher::from_protocol_cache(&branch_specs, &protocol_cache)
            .await
            .expect("dispatcher builds from cache");

    let dispatched = dispatcher
        .dispatch_block_scoped_data(make_family_contract_and_storage_follow_up_block_scoped_data(
            44,
            "cursor-44",
        ))
        .expect("contract/storage follow-up routes from cache preload");

    let v2 = dispatched
        .get("uniswap_v2")
        .expect("v2 branch present");
    let v2_changes = substreams::BlockChanges::decode(
        v2.output
            .as_ref()
            .and_then(|output| output.map_output.as_ref())
            .expect("v2 map output")
            .value
            .as_slice(),
    )
    .expect("decode v2 block changes");
    assert_eq!(v2_changes.changes.len(), 1);
    assert_eq!(v2_changes.storage_changes.len(), 0);
    assert_eq!(
        v2_changes.changes[0]
            .contract_changes
            .len(),
        1
    );
    assert_eq!(v2_changes.changes[0].contract_changes[0].address, vec![0x44; 20]);

    let v3 = dispatched
        .get("uniswap_v3")
        .expect("v3 branch present");
    let v3_changes = substreams::BlockChanges::decode(
        v3.output
            .as_ref()
            .and_then(|output| output.map_output.as_ref())
            .expect("v3 map output")
            .value
            .as_slice(),
    )
    .expect("decode v3 block changes");
    assert_eq!(v3_changes.changes.len(), 0);
    assert_eq!(v3_changes.storage_changes.len(), 1);
    assert_eq!(
        v3_changes.storage_changes[0]
            .storage_changes
            .len(),
        1
    );
    assert_eq!(v3_changes.storage_changes[0].storage_changes[0].address, vec![0x55; 20]);
}

#[tokio::test]
async fn test_build_family_dispatcher_from_populated_cache_uses_gateway_seeded_components() {
    let chain = Chain::Ethereum;
    let mut gateway = MockGateway::new();
    gateway
        .expect_get_tokens()
        .return_once(move |_, _, _, _, _| {
            let token = Token::new(&Bytes::from(vec![0xaa; 20]), "TKN", 18, 0, &[], chain, 100);
            Box::pin(async move { Ok(WithTotal { entity: vec![token], total: Some(1) }) })
        });
    gateway
        .expect_get_protocol_components()
        .return_once(|_, _, _, _, _| {
            Box::pin(async move {
                Ok(WithTotal {
                    entity: vec![
                        ProtocolComponent::new(
                            "v2-pool",
                            "uniswap_v2",
                            "pool",
                            Chain::Ethereum,
                            vec![],
                            vec![Bytes::from(vec![0x44; 20])],
                            HashMap::new(),
                            ChangeType::Creation,
                            Bytes::default(),
                            NaiveDateTime::default(),
                        ),
                        ProtocolComponent::new(
                            "v3-pool",
                            "uniswap_v3",
                            "pool",
                            Chain::Ethereum,
                            vec![],
                            vec![Bytes::from(vec![0x55; 20])],
                            HashMap::new(),
                            ChangeType::Creation,
                            Bytes::default(),
                            NaiveDateTime::default(),
                        ),
                    ],
                    total: Some(2),
                })
            })
        });
    gateway
        .expect_get_token_prices()
        .with(mockall::predicate::eq(chain))
        .times(1)
        .return_once(|_| Box::pin(async { Ok(HashMap::new()) }));

    let protocol_cache =
        ProtocolMemoryCache::new(chain, chrono::Duration::seconds(60), Arc::new(gateway));
    protocol_cache
        .populate()
        .await
        .expect("populate protocol cache from gateway");

    let branch_specs = vec![
        FamilyBranchSpec {
            protocol_system: "uniswap_v2".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
        },
        FamilyBranchSpec {
            protocol_system: "uniswap_v3".to_string(),
            protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
        },
    ];
    let mut dispatcher =
        FamilyBlockChangesDispatcher::from_protocol_cache(&branch_specs, &protocol_cache)
            .await
            .expect("dispatcher builds from populated cache");

    let dispatched = dispatcher
        .dispatch_block_scoped_data(make_family_contract_and_storage_follow_up_block_scoped_data(
            44,
            "cursor-44",
        ))
        .expect("dispatch follow-up block after populated-cache preseed");

    assert!(dispatched.contains_key("uniswap_v2"));
    assert!(dispatched.contains_key("uniswap_v3"));
}

#[tokio::test]
async fn test_family_runner_hydrates_missing_component_ownership_from_protocol_cache() {
    let follow_up_block = make_family_follow_up_block_scoped_data(43, "cursor-43");

    let protocol_cache = ProtocolMemoryCache::new(
        Chain::Ethereum,
        chrono::Duration::seconds(60),
        Arc::new(MockGateway::new()),
    );
    protocol_cache
        .add_components(vec![
            ProtocolComponent::new(
                "v2-pool",
                "uniswap_v2",
                "pool",
                Chain::Ethereum,
                vec![],
                vec![Bytes::from(vec![0x44; 20])],
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
            ProtocolComponent::new(
                "v3-pool",
                "uniswap_v3",
                "pool",
                Chain::Ethereum,
                vec![],
                vec![Bytes::from(vec![0x55; 20])],
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
        ])
        .await
        .expect("seed protocol cache");

    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
    v2.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(decoded.changes[0].entity_changes.len(), 1);
            assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v2-pool");
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });
    v2.expect_flush()
        .once()
        .returning(|| Ok(()));

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
    v3.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(decoded.changes[0].entity_changes.len(), 1);
            assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v3-pool");
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });
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

    let extractors = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let runtime_state = FamilyRuntimeState::new(&extractors, dispatcher, protocol_cache);
    let runner = FamilyExtractorRunner::new(
        extractors,
        SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
            Ok(BlockResponse::New(follow_up_block)),
            Ok(BlockResponse::Ended),
        ]))),
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
        ]),
        mpsc::channel(4).1,
        None,
        false,
        runtime_state,
    );

    runner.run().await.unwrap().unwrap();
}

#[tokio::test]
async fn test_family_runner_hydrates_missing_contract_and_storage_ownership_from_protocol_cache() {
    let follow_up_block =
        make_family_contract_and_storage_follow_up_block_scoped_data(44, "cursor-44");

    let protocol_cache = ProtocolMemoryCache::new(
        Chain::Ethereum,
        chrono::Duration::seconds(60),
        Arc::new(MockGateway::new()),
    );
    protocol_cache
        .add_components(vec![
            ProtocolComponent::new(
                "v2-pool",
                "uniswap_v2",
                "pool",
                Chain::Ethereum,
                vec![],
                vec![Bytes::from(vec![0x44; 20])],
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
            ProtocolComponent::new(
                "v3-pool",
                "uniswap_v3",
                "pool",
                Chain::Ethereum,
                vec![],
                vec![Bytes::from(vec![0x55; 20])],
                HashMap::new(),
                ChangeType::Creation,
                Bytes::default(),
                NaiveDateTime::default(),
            ),
        ])
        .await
        .expect("seed protocol cache");

    let mut v2 = MockExtractor::new();
    v2.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v2"));
    v2.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
            assert_eq!(decoded.changes.len(), 1);
            assert_eq!(
                decoded.changes[0]
                    .contract_changes
                    .len(),
                1
            );
            assert_eq!(decoded.changes[0].contract_changes[0].address, vec![0x44; 20]);
            assert!(
                decoded.storage_changes.is_empty(),
                "v2 branch should not receive v3 storage-only follow-up"
            );
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });
    v2.expect_flush()
        .once()
        .returning(|| Ok(()));

    let mut v3 = MockExtractor::new();
    v3.expect_get_id()
        .returning(|| ExtractorIdentity::new(Chain::Ethereum, "uniswap_v3"));
    v3.expect_handle_tick_scoped_data()
        .once()
        .returning(|inp: BlockScopedData| {
            let raw = &inp
                .output
                .as_ref()
                .expect("output")
                .map_output
                .as_ref()
                .expect("map output")
                .value;
            let decoded =
                substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
            assert!(
                decoded.changes.is_empty()
                    || decoded.changes[0]
                        .contract_changes
                        .is_empty(),
                "v3 branch should not receive v2 contract-only follow-up"
            );
            assert_eq!(decoded.storage_changes.len(), 1);
            assert_eq!(
                decoded.storage_changes[0]
                    .storage_changes
                    .len(),
                1
            );
            assert_eq!(decoded.storage_changes[0].storage_changes[0].address, vec![0x55; 20]);
            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
        });
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

    let extractors = HashMap::from([
        ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
        ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
    ]);
    let runtime_state = FamilyRuntimeState::new(&extractors, dispatcher, protocol_cache);
    let runner = FamilyExtractorRunner::new(
        extractors,
        SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
            Ok(BlockResponse::New(follow_up_block)),
            Ok(BlockResponse::Ended),
        ]))),
        HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
        ]),
        mpsc::channel(4).1,
        None,
        false,
        runtime_state,
    );

    runner.run().await.unwrap().unwrap();
}
