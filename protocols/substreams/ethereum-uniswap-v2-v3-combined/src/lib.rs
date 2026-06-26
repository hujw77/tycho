#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::collections::HashMap;

use ethereum_uniswap_v2::core::{
    build_pool_created_block_changes as build_v2_pool_created_block_changes,
    build_pool_event_block_changes as build_v2_pool_event_block_changes,
    parse_pool_created_params as parse_v2_pool_created_params,
};
use ethereum_uniswap_v3_logs_only::{
    core::{
        build_balance_deltas as build_v3_balance_deltas,
        build_pool_created_block_entity_changes as build_v3_pool_created_block_entity_changes,
        build_liquidity_changes as build_v3_liquidity_changes,
        build_pool_events as build_v3_pool_events,
        build_protocol_changes as build_v3_protocol_changes,
        build_tick_deltas as build_v3_tick_deltas,
        collect_current_tick_updates as collect_v3_current_tick_updates,
    },
    pb::uniswap::v3::{Events as V3Events, LiquidityChanges as V3LiquidityChanges, Pool as V3Pool, TickDeltas as V3TickDeltas},
};
use substreams::store::{
    StoreAdd, StoreAddBigInt, StoreGet, StoreGetInt64, StoreGetProto, StoreNew, StoreSet,
    StoreSetIfNotExists, StoreSetIfNotExistsProto, StoreSetInt64, StoreSetSum, StoreSetSumBigInt,
};
use substreams_ethereum::pb::eth::v2 as eth;
use tycho_substreams::{models::BlockBalanceDeltas, prelude::*};

pub use ethereum_uniswap_v2::core::{
    build_pool_created_block_changes as build_family_v2_pool_created_block_changes,
    build_pool_event_block_changes as build_family_v2_pool_event_block_changes,
    parse_pool_created_params as parse_family_v2_pool_created_params,
};
pub use ethereum_uniswap_v3_logs_only::{
    core::{
        build_pool_created_block_entity_changes as build_family_v3_pool_created_block_entity_changes,
        build_pool_events as build_family_v3_pool_events,
        build_protocol_changes as build_family_v3_protocol_changes,
    },
    pb::uniswap::v3::{
        Events as FamilyV3Events, LiquidityChanges as FamilyV3LiquidityChanges,
        Pool as FamilyV3Pool, TickDeltas as FamilyV3TickDeltas,
    },
};

#[substreams::handlers::map]
pub fn v2_map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockChanges, substreams::errors::Error> {
    run_v2_map_pools_created(params, block)
}

pub fn run_v2_map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockChanges, substreams::errors::Error> {
    let params = parse_v2_pool_created_params(&params);
    Ok(build_v2_pool_created_block_changes(&block, &params))
}

#[substreams::handlers::store]
pub fn v2_store_pools(
    pools_created: BlockChanges,
    store: StoreSetIfNotExistsProto<ProtocolComponent>,
) {
    for change in pools_created.changes {
        for new_protocol_component in change.component_changes {
            store.set_if_not_exists(
                0,
                format!("Pool:{}", new_protocol_component.id),
                &new_protocol_component,
            );
        }
    }
}

#[substreams::handlers::map]
pub fn v2_map_pool_events(
    params: String,
    block: eth::Block,
    block_entity_changes: BlockChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
) -> Result<BlockChanges, substreams::errors::Error> {
    run_v2_map_pool_events(params, block, block_entity_changes, &pools_store)
}

pub fn run_v2_map_pool_events<S>(
    params: String,
    block: eth::Block,
    block_entity_changes: BlockChanges,
    pools_store: &S,
) -> Result<BlockChanges, substreams::errors::Error>
where
    S: StoreGet<ProtocolComponent>,
{
    Ok(build_v2_pool_event_block_changes(
        &params,
        &block,
        block_entity_changes,
        pools_store,
    ))
}

#[substreams::handlers::map]
pub fn v3_map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(build_v3_pool_created_block_entity_changes(&params, &block))
}

#[substreams::handlers::store]
pub fn v3_store_pools(
    pools_created: BlockEntityChanges,
    store: StoreSetIfNotExistsProto<V3Pool>,
) {
    for change in pools_created.changes {
        for component_change in &change.component_changes {
            let pool_address = &component_change.id;
            let pool = V3Pool {
                address: hex::decode(pool_address.trim_start_matches("0x")).unwrap(),
                token0: component_change.tokens[0].clone(),
                token1: component_change.tokens[1].clone(),
                created_tx_hash: change.tx.as_ref().unwrap().hash.clone(),
            };
            store.set_if_not_exists(0, format!("Pool:{pool_address}"), &pool);
        }
    }
}

#[substreams::handlers::map]
pub fn v3_map_events(
    params: String,
    block: eth::Block,
    pools_store: StoreGetProto<V3Pool>,
) -> Result<V3Events, anyhow::Error> {
    Ok(build_v3_pool_events(&params, block, &pools_store))
}

#[substreams::handlers::map]
pub fn v3_map_balance_changes(
    events: V3Events,
    pools_store: StoreGetProto<V3Pool>,
) -> Result<BlockBalanceDeltas, anyhow::Error> {
    Ok(build_v3_balance_deltas(events, &pools_store))
}

#[substreams::handlers::store]
pub fn v3_store_pools_balances(balances_deltas: BlockBalanceDeltas, store: StoreAddBigInt) {
    tycho_substreams::balances::store_balance_changes(balances_deltas, store);
}

#[substreams::handlers::map]
pub fn v3_map_ticks_changes(events: V3Events) -> Result<V3TickDeltas, anyhow::Error> {
    Ok(build_v3_tick_deltas(events))
}

#[substreams::handlers::store]
pub fn v3_store_ticks_liquidity(ticks_deltas: V3TickDeltas, store: StoreAddBigInt) {
    let mut deltas = ticks_deltas.deltas;
    deltas.sort_unstable_by_key(|delta| delta.ordinal);

    deltas.iter().for_each(|delta| {
        store.add(
            delta.ordinal,
            format!("pool:{}:tick:{}", hex::encode(&delta.pool_address), delta.tick_index),
            substreams::scalar::BigInt::from_signed_bytes_be(&delta.liquidity_net_delta),
        );
    });
}

#[substreams::handlers::store]
pub fn v3_store_pool_current_tick(events: V3Events, store: StoreSetInt64) {
    collect_v3_current_tick_updates(events)
        .into_iter()
        .for_each(|(pool, ordinal, new_tick_index)| {
            store.set(ordinal, format!("pool:{pool}"), &new_tick_index.into())
        });
}

#[substreams::handlers::map]
pub fn v3_map_liquidity_changes(
    events: V3Events,
    pools_current_tick_store: StoreGetInt64,
) -> Result<V3LiquidityChanges, anyhow::Error> {
    Ok(build_v3_liquidity_changes(events, &pools_current_tick_store))
}

#[substreams::handlers::store]
pub fn v3_store_liquidity(ticks_deltas: V3LiquidityChanges, store: StoreSetSumBigInt) {
    ticks_deltas
        .changes
        .iter()
        .for_each(|change| match change.change_type() {
            ethereum_uniswap_v3_logs_only::pb::uniswap::v3::LiquidityChangeType::Delta => {
                store.sum(
                    change.ordinal,
                    format!("pool:{}", hex::encode(&change.pool_address)),
                    substreams::scalar::BigInt::from_signed_bytes_be(&change.value),
                );
            }
            ethereum_uniswap_v3_logs_only::pb::uniswap::v3::LiquidityChangeType::Absolute => {
                store.set(
                    change.ordinal,
                    format!("pool:{}", hex::encode(&change.pool_address)),
                    substreams::scalar::BigInt::from_signed_bytes_be(&change.value),
                );
            }
        });
}

#[substreams::handlers::map]
pub fn v3_map_protocol_changes(
    block: eth::Block,
    created_pools: BlockEntityChanges,
    events: V3Events,
    balances_map_deltas: BlockBalanceDeltas,
    balances_store_deltas: substreams::pb::substreams::StoreDeltas,
    ticks_map_deltas: V3TickDeltas,
    ticks_store_deltas: substreams::pb::substreams::StoreDeltas,
    pool_liquidity_changes: V3LiquidityChanges,
    pool_liquidity_store_deltas: substreams::pb::substreams::StoreDeltas,
) -> Result<BlockChanges, substreams::errors::Error> {
    Ok(build_v3_protocol_changes(
        block,
        created_pools,
        events,
        balances_map_deltas,
        balances_store_deltas,
        ticks_map_deltas,
        ticks_store_deltas,
        pool_liquidity_changes,
        pool_liquidity_store_deltas,
    ))
}

#[substreams::handlers::map]
pub fn map_uniswap_family_protocol_changes(
    v2_changes: BlockChanges,
    v3_changes: BlockChanges,
) -> Result<BlockChanges, substreams::errors::Error> {
    run_map_uniswap_family_protocol_changes(v2_changes, v3_changes)
}

pub fn run_map_uniswap_family_protocol_changes(
    v2_changes: BlockChanges,
    v3_changes: BlockChanges,
) -> Result<BlockChanges, substreams::errors::Error> {
    Ok(build_uniswap_family_protocol_changes(v2_changes, v3_changes))
}

pub fn build_uniswap_family_protocol_changes(
    v2_changes: BlockChanges,
    v3_changes: BlockChanges,
) -> BlockChanges {
    merge_block_changes([v2_changes, v3_changes])
}

pub fn build_uniswap_family_protocol_changes_from_v2(
    v2_changes: BlockChanges,
) -> BlockChanges {
    let empty_v3 = BlockChanges {
        block: v2_changes.block.clone(),
        changes: vec![],
        storage_changes: vec![],
    };
    build_uniswap_family_protocol_changes(v2_changes, empty_v3)
}

pub fn build_uniswap_family_protocol_changes_from_v3_created_pools(
    params: &str,
    block: eth::Block,
) -> BlockChanges {
    let created_pools = build_v3_pool_created_block_entity_changes(params, &block);
    let events = V3Events {
        block: None,
        pool_events: vec![],
    };
    let protocol_changes = build_v3_protocol_changes(
        block,
        created_pools,
        events,
        BlockBalanceDeltas {
            balance_deltas: vec![],
        },
        substreams::pb::substreams::StoreDeltas { deltas: vec![] },
        V3TickDeltas { deltas: vec![] },
        substreams::pb::substreams::StoreDeltas { deltas: vec![] },
        V3LiquidityChanges { changes: vec![] },
        substreams::pb::substreams::StoreDeltas { deltas: vec![] },
    );

    let empty_v2 = BlockChanges {
        block: protocol_changes.block.clone(),
        changes: vec![],
        storage_changes: vec![],
    };

    build_uniswap_family_protocol_changes(empty_v2, protocol_changes)
}

pub fn build_uniswap_family_protocol_changes_from_v3_protocol_changes(
    v3_changes: BlockChanges,
) -> BlockChanges {
    let empty_v2 = BlockChanges {
        block: v3_changes.block.clone(),
        changes: vec![],
        storage_changes: vec![],
    };
    build_uniswap_family_protocol_changes(empty_v2, v3_changes)
}

fn merge_block_changes(block_changes: impl IntoIterator<Item = BlockChanges>) -> BlockChanges {
    let mut merged_block = None;
    let mut merged_storage_changes = Vec::new();
    let mut tx_changes_by_hash: HashMap<Vec<u8>, TransactionChanges> = HashMap::new();

    for block_changes in block_changes {
        if merged_block.is_none() {
            merged_block = block_changes.block.clone();
        }

        merged_storage_changes.extend(block_changes.storage_changes);

        for change in block_changes.changes {
            let Some(tx) = change.tx.as_ref() else {
                continue;
            };

            tx_changes_by_hash
                .entry(tx.hash.clone())
                .and_modify(|existing| merge_transaction_changes(existing, &change))
                .or_insert(change);
        }
    }

    merged_storage_changes.sort_by_key(|change| {
        change
            .tx
            .as_ref()
            .map(|tx| tx.index)
            .unwrap_or(u64::MAX)
    });

    let mut changes = tx_changes_by_hash
        .into_values()
        .collect::<Vec<_>>();
    changes.sort_by_key(|change| {
        change
            .tx
            .as_ref()
            .map(|tx| tx.index)
            .unwrap_or(u64::MAX)
    });

    BlockChanges {
        block: merged_block,
        changes,
        storage_changes: merged_storage_changes,
    }
}

fn merge_transaction_changes(existing: &mut TransactionChanges, incoming: &TransactionChanges) {
    existing
        .contract_changes
        .extend(incoming.contract_changes.clone());
    existing
        .entity_changes
        .extend(incoming.entity_changes.clone());
    existing
        .component_changes
        .extend(incoming.component_changes.clone());
    existing
        .balance_changes
        .extend(incoming.balance_changes.clone());
    existing
        .entrypoints
        .extend(incoming.entrypoints.clone());
    existing
        .entrypoint_params
        .extend(incoming.entrypoint_params.clone());
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ethabi::{ethereum_types::{Address, U256}, Token as AbiToken};
    use prost_types::Timestamp;
    use substreams::store::StoreGet;
    use substreams_ethereum::pb::eth::v2::{
        block::DetailLevel, Block as EthBlock, BlockHeader as EthBlockHeader, Log as EthLog,
        TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
        transaction_trace::Type as EthTransactionType, TransactionTraceStatus,
    };

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct MockPoolStore {
        values: HashMap<String, ProtocolComponent>,
    }

    impl MockPoolStore {
        fn with_pool<K: Into<String>>(mut self, key: K, value: ProtocolComponent) -> Self {
            self.values.insert(key.into(), value);
            self
        }
    }

    impl StoreGet<ProtocolComponent> for MockPoolStore {
        fn new(_idx: u32) -> Self {
            Self::default()
        }

        fn get_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> Option<ProtocolComponent> {
            self.get_last(key)
        }

        fn get_last<K: AsRef<str>>(&self, key: K) -> Option<ProtocolComponent> {
            self.values.get(key.as_ref()).cloned()
        }

        fn get_first<K: AsRef<str>>(&self, key: K) -> Option<ProtocolComponent> {
            self.get_last(key)
        }

        fn has_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> bool {
            self.has_last(key)
        }

        fn has_last<K: AsRef<str>>(&self, key: K) -> bool {
            self.values.contains_key(key.as_ref())
        }

        fn has_first<K: AsRef<str>>(&self, key: K) -> bool {
            self.has_last(key)
        }
    }

    fn test_block() -> Block {
        Block {
            number: 42,
            hash: vec![0x01; 32],
            parent_hash: vec![0x02; 32],
            ts: 1_718_000_000,
        }
    }

    fn test_tx(hash: &[u8], index: u64) -> Transaction {
        Transaction {
            hash: hash.to_vec(),
            from: vec![0x11; 20],
            to: vec![0x22; 20],
            index,
        }
    }

    fn test_component(id: &str, protocol_type_name: &str, contract: Vec<u8>) -> ProtocolComponent {
        ProtocolComponent {
            id: id.to_string(),
            tokens: vec![vec![0xa0; 20], vec![0xc0; 20]],
            contracts: vec![contract],
            protocol_type: Some(ProtocolType {
                name: protocol_type_name.to_string(),
                ..Default::default()
            }),
            change: ChangeType::Creation as i32,
            ..Default::default()
        }
    }

    fn address(byte: u8) -> Vec<u8> {
        vec![byte; 20]
    }

    fn topic_address(byte: u8) -> Vec<u8> {
        ethabi::encode(&[AbiToken::Address(Address::from_slice(&address(byte)))])
    }

    fn v2_pair_created_block(factory: u8, token0: u8, token1: u8, pair: u8) -> EthBlock {
        let data = ethabi::encode(&[
            AbiToken::Address(Address::from_slice(&address(pair))),
            AbiToken::Uint(U256::from(1u64)),
        ]);

        let log = EthLog {
            address: address(factory),
            topics: vec![
                vec![
                    13, 54, 72, 189, 15, 107, 168, 1, 52, 163, 59, 169, 39, 90, 197, 133, 217,
                    211, 21, 240, 173, 131, 85, 205, 222, 253, 227, 26, 250, 40, 208, 233,
                ],
                topic_address(token0),
                topic_address(token1),
            ],
            data,
            index: 0,
            block_index: 0,
            ordinal: 1,
        };

        EthBlock {
            hash: vec![0xaa; 32],
            number: 43,
            size: 0,
            header: Some(EthBlockHeader {
                parent_hash: vec![0xbb; 32],
                timestamp: Some(Timestamp {
                    seconds: 1_718_000_043,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            transaction_traces: vec![EthTransactionTrace {
                index: 0,
                hash: vec![0xcc; 32],
                from: vec![0x11; 20],
                to: address(factory),
                status: TransactionTraceStatus::Succeeded as i32,
                receipt: Some(EthTransactionReceipt {
                    logs: vec![log],
                    ..Default::default()
                }),
                r#type: EthTransactionType::TrxTypeLegacy as i32,
                ..Default::default()
            }],
            detail_level: DetailLevel::DetaillevelBase as i32,
            ..Default::default()
        }
    }

    fn v2_sync_block(pool: u8, reserve0: u64, reserve1: u64) -> EthBlock {
        let log = EthLog {
            address: address(pool),
            topics: vec![vec![
                28, 65, 30, 154, 150, 224, 113, 36, 28, 47, 33, 247, 114, 107, 23, 174, 137,
                227, 202, 180, 199, 139, 229, 14, 6, 43, 3, 169, 255, 251, 186, 209,
            ]],
            data: ethabi::encode(&[
                AbiToken::Uint(U256::from(reserve0)),
                AbiToken::Uint(U256::from(reserve1)),
            ]),
            index: 0,
            block_index: 0,
            ordinal: 1,
        };

        EthBlock {
            hash: vec![0xdd; 32],
            number: 44,
            size: 0,
            header: Some(EthBlockHeader {
                parent_hash: vec![0xaa; 32],
                timestamp: Some(Timestamp {
                    seconds: 1_718_000_044,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            transaction_traces: vec![EthTransactionTrace {
                index: 1,
                hash: vec![0xee; 32],
                from: vec![0x22; 20],
                to: address(pool),
                status: TransactionTraceStatus::Succeeded as i32,
                receipt: Some(EthTransactionReceipt {
                    logs: vec![log],
                    ..Default::default()
                }),
                r#type: EthTransactionType::TrxTypeLegacy as i32,
                ..Default::default()
            }],
            detail_level: DetailLevel::DetaillevelBase as i32,
            ..Default::default()
        }
    }

    #[test]
    fn merges_v2_and_v3_changes_into_one_family_block() {
        let tx_hash = vec![0xaa; 32];
        let tx = test_tx(&tx_hash, 0);
        let v2_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![TransactionChanges {
                tx: Some(tx.clone()),
                component_changes: vec![test_component(
                    "v2-pool",
                    "uniswap_v2_pool",
                    vec![0x44; 20],
                )],
                ..Default::default()
            }],
            storage_changes: vec![],
        };
        let v3_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![TransactionChanges {
                tx: Some(tx),
                component_changes: vec![test_component(
                    "v3-pool",
                    "uniswap_v3_pool",
                    vec![0x55; 20],
                )],
                ..Default::default()
            }],
            storage_changes: vec![],
        };

        let merged = merge_block_changes([v2_changes, v3_changes]);

        assert_eq!(merged.changes.len(), 1);
        assert_eq!(merged.block.as_ref().map(|block| block.number), Some(42));
        assert_eq!(merged.changes[0].component_changes.len(), 2);
        assert_eq!(
            merged.changes[0].component_changes[0]
                .protocol_type
                .as_ref()
                .map(|pt| pt.name.as_str()),
            Some("uniswap_v2_pool")
        );
        assert_eq!(
            merged.changes[0].component_changes[1]
                .protocol_type
                .as_ref()
                .map(|pt| pt.name.as_str()),
            Some("uniswap_v3_pool")
        );
    }

    #[test]
    fn merged_family_block_preserves_transaction_index_order() {
        let v2_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![TransactionChanges {
                tx: Some(test_tx(&[0xbb; 32], 2)),
                component_changes: vec![test_component(
                    "v2-pool",
                    "uniswap_v2_pool",
                    vec![0x44; 20],
                )],
                ..Default::default()
            }],
            storage_changes: vec![],
        };
        let v3_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![TransactionChanges {
                tx: Some(test_tx(&[0xaa; 32], 1)),
                component_changes: vec![test_component(
                    "v3-pool",
                    "uniswap_v3_pool",
                    vec![0x55; 20],
                )],
                ..Default::default()
            }],
            storage_changes: vec![],
        };

        let merged = merge_block_changes([v2_changes, v3_changes]);
        let indexes = merged
            .changes
            .iter()
            .map(|change| change.tx.as_ref().expect("tx").index)
            .collect::<Vec<_>>();

        assert_eq!(indexes, vec![1, 2]);
    }

    #[test]
    fn merged_family_block_preserves_storage_change_transaction_index_order() {
        let v2_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![],
            storage_changes: vec![TransactionStorageChanges {
                tx: Some(test_tx(&[0xbb; 32], 2)),
                storage_changes: vec![],
            }],
        };
        let v3_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![],
            storage_changes: vec![TransactionStorageChanges {
                tx: Some(test_tx(&[0xaa; 32], 1)),
                storage_changes: vec![],
            }],
        };

        let merged = merge_block_changes([v2_changes, v3_changes]);
        let indexes = merged
            .storage_changes
            .iter()
            .map(|change| change.tx.as_ref().expect("tx").index)
            .collect::<Vec<_>>();

        assert_eq!(indexes, vec![1, 2]);
    }

    #[test]
    fn merged_family_block_preserves_all_change_vectors_for_same_transaction_hash() {
        let tx_hash = vec![0xdd; 32];
        let tx = test_tx(&tx_hash, 3);

        let v2_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![TransactionChanges {
                tx: Some(tx.clone()),
                contract_changes: vec![ContractChange {
                    address: vec![0x44; 20],
                    change: ChangeType::Creation as i32,
                    ..Default::default()
                }],
                component_changes: vec![test_component(
                    "v2-created-pool",
                    "uniswap_v2_pool",
                    vec![0x44; 20],
                )],
                balance_changes: vec![BalanceChange {
                    token: vec![0xa0; 20],
                    balance: vec![0x01],
                    component_id: b"v2-created-pool".to_vec(),
                }],
                ..Default::default()
            }],
            storage_changes: vec![],
        };

        let v3_changes = BlockChanges {
            block: Some(test_block()),
            changes: vec![TransactionChanges {
                tx: Some(tx),
                entity_changes: vec![EntityChanges {
                    component_id: "v3-created-pool".to_string(),
                    attributes: vec![Attribute {
                        name: "tick".to_string(),
                        value: vec![0x02],
                        change: ChangeType::Update as i32,
                    }],
                }],
                component_changes: vec![test_component(
                    "v3-created-pool",
                    "uniswap_v3_pool",
                    vec![0x55; 20],
                )],
                balance_changes: vec![BalanceChange {
                    token: vec![0xc0; 20],
                    balance: vec![0x03],
                    component_id: b"v3-created-pool".to_vec(),
                }],
                ..Default::default()
            }],
            storage_changes: vec![],
        };

        let merged = merge_block_changes([v2_changes, v3_changes]);
        let tx_changes = merged.changes.first().expect("one merged tx");

        assert_eq!(merged.changes.len(), 1);
        assert_eq!(tx_changes.component_changes.len(), 2);
        assert_eq!(tx_changes.contract_changes.len(), 1);
        assert_eq!(tx_changes.entity_changes.len(), 1);
        assert_eq!(tx_changes.balance_changes.len(), 2);
        assert_eq!(tx_changes.component_changes[0].id, "v2-created-pool");
        assert_eq!(tx_changes.component_changes[1].id, "v3-created-pool");
        assert_eq!(tx_changes.entity_changes[0].component_id, "v3-created-pool");
    }

    #[test]
    fn v2_factory_created_pool_can_flow_through_store_backed_follow_up_into_family_output() {
        let created_block = v2_pair_created_block(0xf1, 0xa0, 0xc0, 0x45);
        let created_changes = build_family_v2_pool_created_block_changes(
            &created_block,
            &parse_family_v2_pool_created_params(
                "factory_address=0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1&protocol_type_name=uniswap_v2_pool",
            ),
        );
        let created_pool = created_changes.changes[0].component_changes[0].clone();
        let pool_id = created_pool.id.clone();
        let pool_store = MockPoolStore::new(0).with_pool(
            format!("Pool:{pool_id}"),
            created_pool,
        );

        let follow_up_block = v2_sync_block(0x45, 2_000, 3_000);
        let follow_up_changes = build_family_v2_pool_event_block_changes(
            &format!("pools={pool_id}"),
            &follow_up_block,
            tycho_substreams::models::BlockChanges {
                block: Some((&follow_up_block).into()),
                changes: vec![],
                storage_changes: vec![],
            },
            &pool_store,
        );
        let family_follow_up = build_uniswap_family_protocol_changes_from_v2(follow_up_changes);

        assert_eq!(family_follow_up.changes.len(), 1);
        assert_eq!(family_follow_up.changes[0].entity_changes.len(), 1);
        assert_eq!(family_follow_up.changes[0].balance_changes.len(), 2);
        assert_eq!(family_follow_up.changes[0].entity_changes[0].component_id, pool_id);
    }
}
