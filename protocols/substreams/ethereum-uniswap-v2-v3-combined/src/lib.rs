#![allow(clippy::not_unsafe_ptr_arg_deref)]

use ethereum_uniswap_v2::core::{
    build_pool_created_block_changes as build_v2_pool_created_block_changes,
    build_pool_event_block_changes as build_v2_pool_event_block_changes,
    parse_pool_created_params as parse_v2_pool_created_params,
};
use ethereum_uniswap_v3_logs_only::{
    core::{
        build_balance_deltas as build_v3_balance_deltas,
        build_liquidity_changes as build_v3_liquidity_changes,
        build_pool_created_block_entity_changes as build_v3_pool_created_block_entity_changes,
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

#[substreams::handlers::map]
pub fn v2_map_pools_created(
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
    Ok(build_v2_pool_event_block_changes(
        &params,
        &block,
        block_entity_changes,
        &pools_store,
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
