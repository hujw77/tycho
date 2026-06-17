use std::str::FromStr;

use tycho_substreams::pb::tycho::evm::v1::TransactionEntityChanges;

use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events, TickDelta, TickDeltas,
};

use substreams::scalar::BigInt;

#[substreams::handlers::map]
pub fn map_ticks_changes_bootstrap(
    bootstrap_pools: tycho_substreams::models::BlockEntityChanges,
    events: Events,
) -> Result<TickDeltas, anyhow::Error> {
    let mut deltas = bootstrap_pools
        .changes
        .iter()
        .flat_map(created_pool_tick_deltas)
        .collect::<Vec<_>>();
    deltas.extend(
        events
            .pool_events
            .into_iter()
            .flat_map(event_to_ticks_deltas),
    );

    Ok(TickDeltas { deltas })
}

fn created_pool_tick_deltas(change: &TransactionEntityChanges) -> Vec<TickDelta> {
    let Some(tx) = change.tx.as_ref() else {
        return vec![];
    };

    change
        .entity_changes
        .iter()
        .flat_map(|entity_change| {
            entity_change
                .attributes
                .iter()
                .filter_map(|attribute| {
                    if !attribute.name.starts_with("ticks/") {
                        return None;
                    }

                    let tick_index = attribute
                        .name
                        .split('/')
                        .nth(1)?
                        .parse::<i32>()
                        .ok()?;

                    Some(TickDelta {
                        pool_address: hex::decode(
                            entity_change
                                .component_id
                                .trim_start_matches("0x"),
                        )
                        .unwrap(),
                        tick_index,
                        liquidity_net_delta: attribute.value.clone(),
                        ordinal: tx.index,
                        transaction: Some(tx.clone().into()),
                    })
                })
        })
        .collect()
}

fn event_to_ticks_deltas(event: PoolEvent) -> Vec<TickDelta> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Mint(mint) => {
            vec![
                TickDelta {
                    pool_address: hex::decode(&event.pool_address).unwrap(),
                    tick_index: mint.tick_lower,
                    liquidity_net_delta: BigInt::from_str(&mint.amount)
                        .unwrap()
                        .to_signed_bytes_be(),
                    ordinal: event.log_ordinal,
                    transaction: event.transaction.clone(),
                },
                TickDelta {
                    pool_address: hex::decode(&event.pool_address).unwrap(),
                    tick_index: mint.tick_upper,
                    liquidity_net_delta: BigInt::from_str(&mint.amount)
                        .unwrap()
                        .neg()
                        .to_signed_bytes_be(),
                    ordinal: event.log_ordinal,
                    transaction: event.transaction,
                },
            ]
        }
        pool_event::Type::Burn(burn) => vec![
            TickDelta {
                pool_address: hex::decode(&event.pool_address).unwrap(),
                tick_index: burn.tick_lower,
                liquidity_net_delta: BigInt::from_str(&burn.amount)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                ordinal: event.log_ordinal,
                transaction: event.transaction.clone(),
            },
            TickDelta {
                pool_address: hex::decode(&event.pool_address).unwrap(),
                tick_index: burn.tick_upper,
                liquidity_net_delta: BigInt::from_str(&burn.amount)
                    .unwrap()
                    .to_signed_bytes_be(),
                ordinal: event.log_ordinal,
                transaction: event.transaction,
            },
        ],
        _ => vec![],
    }
}
