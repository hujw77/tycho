use std::str::FromStr;

use substreams::store::{StoreGet, StoreGetInt64, StoreNew, StoreSet, StoreSetInt64};
use tycho_substreams::pb::tycho::evm::v1::TransactionEntityChanges;

use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events, LiquidityChange, LiquidityChangeType, LiquidityChanges,
};

use substreams::scalar::BigInt;

#[substreams::handlers::store]
pub fn store_pool_current_tick_bootstrap(
    bootstrap_pools: tycho_substreams::models::BlockEntityChanges,
    events: Events,
    store: StoreSetInt64,
) {
    for change in bootstrap_pools.changes.iter() {
        let Some(tx) = change.tx.as_ref() else {
            continue;
        };

        for entity_change in change.entity_changes.iter() {
            let Some(attribute) = entity_change
                .attributes
                .iter()
                .find(|attribute| attribute.name == "tick")
            else {
                continue;
            };

            let tick = BigInt::from_signed_bytes_be(&attribute.value)
                .to_string()
                .parse::<i64>()
                .expect("bootstrap tick should fit into i64");
            store.set(tx.index, format!("pool:{}", entity_change.component_id), &tick.into());
        }
    }

    events
        .pool_events
        .into_iter()
        .filter_map(event_to_current_tick)
        .for_each(|(pool, ordinal, new_tick_index)| {
            store.set(ordinal, format!("pool:{pool}"), &new_tick_index.into())
        });
}

#[substreams::handlers::map]
pub fn map_liquidity_changes_bootstrap(
    bootstrap_pools: tycho_substreams::models::BlockEntityChanges,
    events: Events,
    pools_current_tick_store: StoreGetInt64,
) -> Result<LiquidityChanges, anyhow::Error> {
    let mut changes = bootstrap_pools
        .changes
        .iter()
        .flat_map(created_pool_liquidity_changes)
        .collect::<Vec<_>>();

    changes.extend(
        events
            .pool_events
            .into_iter()
            .filter(pool_event_can_introduce_liquidity_changes)
            .map(|event| {
                (
                    pools_current_tick_store
                        .get_at(event.log_ordinal, format!("pool:{}", &event.pool_address))
                        .unwrap_or(0),
                    event,
                )
            })
            .filter_map(|(current_tick, event)| event_to_liquidity_deltas(current_tick, event)),
    );

    changes.sort_unstable_by_key(|change| change.ordinal);
    Ok(LiquidityChanges { changes })
}

fn created_pool_liquidity_changes(change: &TransactionEntityChanges) -> Vec<LiquidityChange> {
    let Some(tx) = change.tx.as_ref() else {
        return vec![];
    };

    change
        .entity_changes
        .iter()
        .filter_map(|entity_change| {
            entity_change
                .attributes
                .iter()
                .find(|attribute| attribute.name == "liquidity")
                .map(|attribute| LiquidityChange {
                    pool_address: hex::decode(
                        entity_change
                            .component_id
                            .trim_start_matches("0x"),
                    )
                    .unwrap(),
                    value: attribute.value.clone(),
                    change_type: LiquidityChangeType::Absolute.into(),
                    ordinal: tx.index,
                    transaction: Some(tx.clone().into()),
                })
        })
        .collect()
}

fn event_to_liquidity_deltas(current_tick: i64, event: PoolEvent) -> Option<LiquidityChange> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Mint(mint) => {
            if current_tick >= mint.tick_lower.into() && current_tick < mint.tick_upper.into() {
                Some(LiquidityChange {
                    pool_address: hex::decode(event.pool_address).unwrap(),
                    value: BigInt::from_str(&mint.amount)
                        .unwrap()
                        .to_signed_bytes_be(),
                    change_type: LiquidityChangeType::Delta.into(),
                    ordinal: event.log_ordinal,
                    transaction: Some(event.transaction.unwrap()),
                })
            } else {
                None
            }
        }
        pool_event::Type::Burn(burn) => {
            if current_tick >= burn.tick_lower.into() && current_tick < burn.tick_upper.into() {
                Some(LiquidityChange {
                    pool_address: hex::decode(event.pool_address).unwrap(),
                    value: BigInt::from_str(&burn.amount)
                        .unwrap()
                        .neg()
                        .to_signed_bytes_be(),
                    change_type: LiquidityChangeType::Delta.into(),
                    ordinal: event.log_ordinal,
                    transaction: Some(event.transaction.unwrap()),
                })
            } else {
                None
            }
        }
        pool_event::Type::Swap(swap) => Some(LiquidityChange {
            pool_address: hex::decode(event.pool_address).unwrap(),
            value: BigInt::from_str(&swap.liquidity)
                .unwrap()
                .to_signed_bytes_be(),
            change_type: LiquidityChangeType::Absolute.into(),
            ordinal: event.log_ordinal,
            transaction: Some(event.transaction.unwrap()),
        }),
        _ => None,
    }
}

fn pool_event_can_introduce_liquidity_changes(event: &PoolEvent) -> bool {
    matches!(
        event.r#type.as_ref().unwrap(),
        pool_event::Type::Mint(_) | pool_event::Type::Burn(_) | pool_event::Type::Swap(_)
    )
}

fn event_to_current_tick(event: PoolEvent) -> Option<(String, u64, i32)> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Initialize(initialize) => {
            Some((event.pool_address, event.log_ordinal, initialize.tick))
        }
        pool_event::Type::Swap(swap) => Some((event.pool_address, event.log_ordinal, swap.tick)),
        _ => None,
    }
}
