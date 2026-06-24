use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use ethabi::ethereum_types::Address;
use itertools::Itertools;
use serde::Deserialize;
use substreams::prelude::BigInt;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};
use substreams_helper::{event_handler::EventHandler, hex::Hexable};

use crate::{
    abi::{factory::events::PairCreated, pool::events::Sync},
    store_key::StoreKey,
    traits::PoolAddresser,
};
use tycho_substreams::prelude::*;

#[derive(Debug, Deserialize)]
pub struct PoolCreatedParams {
    pub factory_address: String,
    pub protocol_type_name: String,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct ComponentKey<T> {
    component_id: String,
    name: T,
}

impl<T> ComponentKey<T> {
    fn new(component_id: String, name: T) -> Self {
        Self { component_id, name }
    }
}

#[derive(Clone)]
struct PartialChanges {
    transaction: Transaction,
    entity_changes: HashMap<ComponentKey<String>, Attribute>,
    balance_changes: HashMap<ComponentKey<Vec<u8>>, BalanceChange>,
}

impl PartialChanges {
    fn consolidate_entity_changes(self) -> Vec<EntityChanges> {
        self.entity_changes
            .into_iter()
            .map(|(key, attribute)| (key.component_id, attribute))
            .into_group_map()
            .into_iter()
            .map(|(component_id, attributes)| EntityChanges { component_id, attributes })
            .collect()
    }
}

#[derive(Clone)]
struct PoolMetadata {
    token0: Vec<u8>,
    token1: Vec<u8>,
}

pub fn parse_pool_created_params(params: &str) -> PoolCreatedParams {
    serde_qs::from_str(params).expect("Unable to deserialize params")
}

pub fn build_pool_created_block_changes(
    block: &eth::Block,
    params: &PoolCreatedParams,
) -> BlockChanges {
    let mut new_pools = Vec::new();
    collect_new_pools(block, &mut new_pools, params);

    BlockChanges { block: Some(block.into()), changes: new_pools, storage_changes: vec![] }
}

pub fn build_pool_event_block_changes(
    params: &str,
    block: &eth::Block,
    mut block_entity_changes: BlockChanges,
    pools_store: &StoreGetProto<ProtocolComponent>,
) -> BlockChanges {
    let bootstrap_pool_tokens = parse_bootstrap_pool_tokens(params);
    let bootstrap_pools = bootstrap_pool_tokens
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let mut tx_changes: HashMap<Vec<u8>, PartialChanges> = HashMap::new();

    handle_sync(
        block,
        &mut tx_changes,
        pools_store,
        &bootstrap_pool_tokens,
        &bootstrap_pools,
    );
    merge_block(&mut tx_changes, &mut block_entity_changes);

    block_entity_changes
}

fn collect_new_pools(
    block: &eth::Block,
    new_pools: &mut Vec<TransactionChanges>,
    params: &PoolCreatedParams,
) {
    let mut on_pair_created = |event: PairCreated, tx: &eth::TransactionTrace, _log: &eth::Log| {
        let tycho_tx: Transaction = tx.into();

        new_pools.push(TransactionChanges {
            tx: Some(tycho_tx.clone()),
            contract_changes: vec![],
            entity_changes: vec![EntityChanges {
                component_id: event.pair.to_hex(),
                attributes: vec![
                    Attribute {
                        name: "reserve0".to_string(),
                        value: BigInt::from(0).to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                    Attribute {
                        name: "reserve1".to_string(),
                        value: BigInt::from(0).to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                ],
            }],
            component_changes: vec![ProtocolComponent {
                id: event.pair.to_hex(),
                tokens: vec![event.token0.clone(), event.token1.clone()],
                contracts: vec![],
                static_att: vec![
                    Attribute {
                        name: "fee".to_string(),
                        value: BigInt::from(30).to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                    Attribute {
                        name: "pool_address".to_string(),
                        value: event.pair.clone(),
                        change: ChangeType::Creation.into(),
                    },
                ],
                change: i32::from(ChangeType::Creation),
                protocol_type: Some(ProtocolType {
                    name: params.protocol_type_name.to_string(),
                    financial_type: FinancialType::Swap.into(),
                    attribute_schema: vec![],
                    implementation_type: ImplementationType::Custom.into(),
                }),
            }],
            balance_changes: vec![
                BalanceChange {
                    token: event.token0,
                    balance: BigInt::from(0).to_signed_bytes_be(),
                    component_id: event.pair.to_hex().as_bytes().to_vec(),
                },
                BalanceChange {
                    token: event.token1,
                    balance: BigInt::from(0).to_signed_bytes_be(),
                    component_id: event.pair.to_hex().as_bytes().to_vec(),
                },
            ],
            entrypoints: vec![],
            entrypoint_params: vec![],
        })
    };

    let mut eh = EventHandler::new(block);
    eh.filter_by_address(vec![Address::from_str(&params.factory_address).unwrap()]);
    eh.on::<PairCreated, _>(&mut on_pair_created);
    eh.handle_events();
}

fn handle_sync(
    block: &eth::Block,
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    store: &StoreGetProto<ProtocolComponent>,
    bootstrap_pool_tokens: &HashMap<String, PoolMetadata>,
    bootstrap_pools: &HashSet<String>,
) {
    let mut on_sync = |event: Sync, tx: &eth::TransactionTrace, log: &eth::Log| {
        let pool_address_hex = log.address.to_hex();
        let pool = store.get_last(StoreKey::Pool.get_unique_pool_key(pool_address_hex.as_str()));
        let reserves_bytes = [event.reserve0, event.reserve1];

        let tx_change = tx_changes
            .entry(tx.hash.clone())
            .or_insert_with(|| PartialChanges {
                transaction: tx.into(),
                entity_changes: HashMap::new(),
                balance_changes: HashMap::new(),
            });

        for (i, reserve_bytes) in reserves_bytes.iter().enumerate() {
            let attribute_name = format!("reserve{}", i);
            tx_change.entity_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), attribute_name.clone()),
                Attribute {
                    name: attribute_name,
                    value: reserve_bytes.clone().to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
        }

        let tokens = if let Some(pool) = pool {
            Some([pool.tokens[0].clone(), pool.tokens[1].clone()])
        } else {
            bootstrap_pool_tokens
                .get(&pool_address_hex)
                .map(|meta| [meta.token0.clone(), meta.token1.clone()])
        };

        if let Some(tokens) = tokens {
            for (index, token) in tokens.iter().enumerate() {
                tx_change.balance_changes.insert(
                    ComponentKey::new(pool_address_hex.clone(), token.clone()),
                    BalanceChange {
                        token: token.clone(),
                        balance: reserves_bytes[index].clone().to_signed_bytes_be(),
                        component_id: pool_address_hex.as_bytes().to_vec(),
                    },
                );
            }
        }
    };

    let mut eh = EventHandler::new(block);
    eh.filter_by_address(PoolAddresser { store, bootstrap_pools });
    eh.on::<Sync, _>(&mut on_sync);
    eh.handle_events();
}

fn parse_bootstrap_pool_tokens(params: &str) -> HashMap<String, PoolMetadata> {
    let mut pool_tokens = HashMap::new();

    for pair in params.split('&').filter(|part| !part.is_empty()) {
        let Some(value) = pair.strip_prefix("pool_tokens=") else {
            continue;
        };

        for entry in value.split(',').filter(|entry| !entry.is_empty()) {
            let mut parts = entry.split(':');
            let (Some(pool), Some(token0), Some(token1), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                continue;
            };

            pool_tokens.insert(
                pool.to_lowercase(),
                PoolMetadata {
                    token0: Address::from_str(token0)
                        .map(|address| address.as_bytes().to_vec())
                        .unwrap_or_default(),
                    token1: Address::from_str(token1)
                        .map(|address| address.as_bytes().to_vec())
                        .unwrap_or_default(),
                },
            );
        }
    }

    pool_tokens
}

fn merge_block(
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    block_entity_changes: &mut BlockChanges,
) {
    let mut tx_entity_changes_map = HashMap::new();

    for change in block_entity_changes.changes.clone() {
        let transaction = change.tx.as_ref().unwrap();
        tx_entity_changes_map
            .entry(transaction.hash.clone())
            .and_modify(|existing: &mut TransactionChanges| {
                existing.component_changes.extend(change.component_changes.clone());
                existing.entity_changes.extend(change.entity_changes.clone());
            })
            .or_insert(change);
    }

    for change in tx_entity_changes_map.values_mut() {
        let tx = change.clone().tx.expect("Transaction not found");

        if let Some(partial_changes) = tx_changes.remove(&tx.hash) {
            change.entity_changes = partial_changes.clone().consolidate_entity_changes();
            change.balance_changes = partial_changes.balance_changes.into_values().collect();
        }
    }

    for partial_changes in tx_changes.values() {
        tx_entity_changes_map.insert(
            partial_changes.transaction.hash.clone(),
            TransactionChanges {
                tx: Some(partial_changes.transaction.clone()),
                contract_changes: vec![],
                entity_changes: partial_changes.clone().consolidate_entity_changes(),
                balance_changes: partial_changes
                    .balance_changes
                    .clone()
                    .into_values()
                    .collect(),
                component_changes: vec![],
                entrypoints: vec![],
                entrypoint_params: vec![],
            },
        );
    }

    block_entity_changes.changes = tx_entity_changes_map.into_values().collect();
}
