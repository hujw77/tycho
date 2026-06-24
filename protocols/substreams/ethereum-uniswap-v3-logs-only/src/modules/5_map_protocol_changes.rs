use crate::{core::build_protocol_changes, pb::uniswap::v3::{Events, LiquidityChanges, TickDeltas}};
use substreams::pb::substreams::StoreDeltas;
use substreams_ethereum::pb::eth::v2::{self as eth};
use tycho_substreams::{models::BlockBalanceDeltas, prelude::*};

#[substreams::handlers::map]
pub fn map_protocol_changes(
    block: eth::Block,
    created_pools: BlockEntityChanges,
    events: Events,
    balances_map_deltas: BlockBalanceDeltas,
    balances_store_deltas: StoreDeltas,
    ticks_map_deltas: TickDeltas,
    ticks_store_deltas: StoreDeltas,
    pool_liquidity_changes: LiquidityChanges,
    pool_liquidity_store_deltas: StoreDeltas,
) -> Result<BlockChanges, substreams::errors::Error> {
    Ok(build_protocol_changes(
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
