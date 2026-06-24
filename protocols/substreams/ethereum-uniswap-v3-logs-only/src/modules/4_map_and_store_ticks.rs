use substreams::store::StoreAddBigInt;

use crate::{core::build_tick_deltas, pb::uniswap::v3::{Events, TickDeltas}};

use substreams::{
    scalar::BigInt,
    store::{StoreAdd, StoreNew},
};

#[substreams::handlers::map]
pub fn map_ticks_changes(events: Events) -> Result<TickDeltas, anyhow::Error> {
    Ok(build_tick_deltas(events))
}

#[substreams::handlers::store]
pub fn store_ticks_liquidity(ticks_deltas: TickDeltas, store: StoreAddBigInt) {
    let mut deltas = ticks_deltas.deltas;

    deltas.sort_unstable_by_key(|delta| delta.ordinal);

    deltas.iter().for_each(|delta| {
        store.add(
            delta.ordinal,
            format!("pool:{0}:tick:{1}", hex::encode(&delta.pool_address), delta.tick_index,),
            BigInt::from_signed_bytes_be(&delta.liquidity_net_delta),
        );
    });
}
