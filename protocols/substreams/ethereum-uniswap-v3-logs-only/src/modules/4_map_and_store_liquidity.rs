use substreams::store::{
    StoreGet, StoreGetInt64, StoreSet, StoreSetInt64, StoreSetSum, StoreSetSumBigInt,
};

use crate::{core::{build_liquidity_changes, collect_current_tick_updates}, pb::uniswap::v3::{Events, LiquidityChangeType, LiquidityChanges}};

use substreams::{scalar::BigInt, store::StoreNew};

#[substreams::handlers::store]
pub fn store_pool_current_tick(events: Events, store: StoreSetInt64) {
    collect_current_tick_updates(events)
        .into_iter()
        .for_each(|(pool, ordinal, new_tick_index)| {
            store.set(ordinal, format!("pool:{pool}"), &new_tick_index.into())
        });
}

#[substreams::handlers::map]
pub fn map_liquidity_changes(
    events: Events,
    pools_current_tick_store: StoreGetInt64,
) -> Result<LiquidityChanges, anyhow::Error> {
    Ok(build_liquidity_changes(events, &pools_current_tick_store))
}

#[substreams::handlers::store]
pub fn store_liquidity(ticks_deltas: LiquidityChanges, store: StoreSetSumBigInt) {
    ticks_deltas
        .changes
        .iter()
        .for_each(|changes| match changes.change_type() {
            LiquidityChangeType::Delta => {
                store.sum(
                    changes.ordinal,
                    format!("pool:{0}", hex::encode(&changes.pool_address)),
                    BigInt::from_signed_bytes_be(&changes.value),
                );
            }
            LiquidityChangeType::Absolute => {
                store.set(
                    changes.ordinal,
                    format!("pool:{0}", hex::encode(&changes.pool_address)),
                    BigInt::from_signed_bytes_be(&changes.value),
                );
            }
        });
}
