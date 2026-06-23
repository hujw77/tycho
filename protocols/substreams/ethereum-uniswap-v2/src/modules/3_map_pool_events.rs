use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};

use substreams_helper::{event_handler::EventHandler, hex::Hexable};

use crate::{abi::pool::events::Sync, store_key::StoreKey, traits::PoolAddresser};
use tycho_substreams::prelude::*;

// Auxiliary struct to serve as a key for the HashMaps.
#[derive(Clone, Hash, Eq, PartialEq)]
struct ComponentKey<T> {
    component_id: String,
    name: T,
}

impl<T> ComponentKey<T> {
    fn new(component_id: String, name: T) -> Self {
        ComponentKey { component_id, name }
    }
}

#[derive(Clone)]
struct PartialChanges {
    transaction: Transaction,
    entity_changes: HashMap<ComponentKey<String>, Attribute>,
    balance_changes: HashMap<ComponentKey<Vec<u8>>, BalanceChange>,
}

impl PartialChanges {
    // Consolidate the entity changes into a vector of EntityChanges. Initially, the entity changes
    // are in a map to prevent duplicates. For each transaction, we need to have only one final
    // state change, per state. Example:
    // If we have two sync events for the same pool (in the same tx), we need to have only one final
    // state change for the reserves. This will be the last sync event, as it is the final state
    // of the pool after the transaction.
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

#[substreams::handlers::map]
pub fn map_pool_events(
    params: String,
    block: eth::Block,
    block_entity_changes: BlockChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
) -> Result<BlockChanges, substreams::errors::Error> {
    // Sync event is sufficient for our use-case. Since it's emitted on every reserve-altering
    // function call, we can use it as the only event to update the reserves of a pool.
    let bootstrap_pool_tokens = parse_bootstrap_pool_tokens(&params);
    let bootstrap_pools = bootstrap_pool_tokens
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let mut block_entity_changes = block_entity_changes;
    let mut tx_changes: HashMap<Vec<u8>, PartialChanges> = HashMap::new();

    handle_sync(
        &block,
        &mut tx_changes,
        &pools_store,
        &bootstrap_pool_tokens,
        &bootstrap_pools,
    );
    merge_block(&mut tx_changes, &mut block_entity_changes);

    Ok(block_entity_changes)
}

/// Handle the sync events and update the reserves of the pools.
///
/// This function is called for each block, and it will handle the sync events for each transaction.
/// On UniswapV2, Sync events are emitted on every reserve-altering function call, so we can use
/// only this event to keep track of the pool state.
///
/// This function also relies on an intermediate HashMap to store the changes for each transaction.
/// This is necessary because we need to consolidate the changes for each transaction before adding
/// them to the block_entity_changes. This HashMap prevents us from having duplicate changes for the
/// same pool and token. See the PartialChanges struct for more details.
fn handle_sync(
    block: &eth::Block,
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    store: &StoreGetProto<ProtocolComponent>,
    bootstrap_pool_tokens: &HashMap<String, PoolMetadata>,
    bootstrap_pools: &HashSet<String>,
) {
    let mut on_sync = |event: Sync, _tx: &eth::TransactionTrace, _log: &eth::Log| {
        let pool_address_hex = _log.address.to_hex();

        let pool = store
            .get_last(StoreKey::Pool.get_unique_pool_key(pool_address_hex.as_str()));
        // Convert reserves to bytes
        let reserves_bytes = [event.reserve0, event.reserve1];

        let tx_change = tx_changes
            .entry(_tx.hash.clone())
            .or_insert_with(|| PartialChanges {
                transaction: _tx.into(),
                entity_changes: HashMap::new(),
                balance_changes: HashMap::new(),
            });

        for (i, reserve_bytes) in reserves_bytes.iter().enumerate() {
            let attribute_name = format!("reserve{}", i);
            // By using a HashMap, we can overwrite the previous value of the reserve attribute if
            // it is for the same pool and the same attribute name (reserves).
            tx_change.entity_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), attribute_name.clone()),
                Attribute {
                    name: attribute_name,
                    value: reserve_bytes
                        .clone()
                        .to_signed_bytes_be(),
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
                let balance = &reserves_bytes[index];
                // HashMap also prevents having duplicate balance changes for the same pool and token.
                tx_change.balance_changes.insert(
                    ComponentKey::new(pool_address_hex.clone(), token.clone()),
                    BalanceChange {
                        token: token.clone(),
                        balance: balance.clone().to_signed_bytes_be(),
                        component_id: pool_address_hex.as_bytes().to_vec(),
                    },
                );
            }
        }
    };

    let mut eh = EventHandler::new(block);
    // Filter the sync events by the pool address, to make sure we don't process events for other
    // Protocols that use the same event signature.
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
                    token0: ethabi::ethereum_types::Address::from_str(token0)
                        .map(|address| address.as_bytes().to_vec())
                        .unwrap_or_default(),
                    token1: ethabi::ethereum_types::Address::from_str(token1)
                        .map(|address| address.as_bytes().to_vec())
                        .unwrap_or_default(),
                },
            );
        }
    }

    pool_tokens
}

/// Merge the changes from the sync events with the create_pool events previously mapped on
/// block_entity_changes.
///
/// Parameters:
/// - tx_changes: HashMap with the changes for each transaction. This is the same HashMap used in
///   handle_sync
/// - block_entity_changes: The BlockChanges struct that will be updated with the changes from the
///   sync events.
///
/// This HashMap comes pre-filled with the changes for the create_pool events, mapped in
///   1_map_pool_created.
///
/// This function is called after the handle_sync function, and it is expected that
/// block_entity_changes will be complete after this function ends.
fn merge_block(
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    block_entity_changes: &mut BlockChanges,
) {
    let mut tx_entity_changes_map = HashMap::new();

    // Add created pools to the tx_changes_map
    for change in block_entity_changes
        .changes
        .clone()
        .into_iter()
    {
        let transaction = change.tx.as_ref().unwrap();
        tx_entity_changes_map
            .entry(transaction.hash.clone())
            .and_modify(|c: &mut TransactionChanges| {
                c.component_changes
                    .extend(change.component_changes.clone());
                c.entity_changes
                    .extend(change.entity_changes.clone());
            })
            .or_insert(change);
    }

    // First, iterate through the previously created transactions, extracted from the
    // map_pool_created step. If there are sync events for this transaction, add them to the
    // block_entity_changes and the corresponding balance changes.
    for change in tx_entity_changes_map.values_mut() {
        let tx = change
            .clone()
            .tx
            .expect("Transaction not found")
            .clone();

        // If there are sync events for this transaction, add them to the block_entity_changes
        if let Some(partial_changes) = tx_changes.remove(&tx.hash) {
            change.entity_changes = partial_changes
                .clone()
                .consolidate_entity_changes();
            change.balance_changes = partial_changes
                .balance_changes
                .into_values()
                .collect();
        }
    }

    // If there are any transactions left in the tx_changes, it means that they are transactions
    // that changed the state of the pools, but were not included in the block_entity_changes.
    // This happens for every regular transaction that does not actually create a pool. By the
    // end of this function, we expect block_entity_changes to be up-to-date with the changes
    // for all sync and new_pools in the block.
    for partial_changes in tx_changes.values() {
        tx_entity_changes_map.insert(
            partial_changes.transaction.hash.clone(),
            TransactionChanges {
                tx: Some(partial_changes.transaction.clone()),
                contract_changes: vec![],
                entity_changes: partial_changes
                    .clone()
                    .consolidate_entity_changes(),
                balance_changes: partial_changes
                    .balance_changes
                    .clone()
                    .into_values()
                    .collect(),
                component_changes: vec![],
            },
        );
    }

    block_entity_changes.changes = tx_entity_changes_map
        .into_values()
        .collect();
}
