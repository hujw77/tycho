use tycho_substreams::models::BlockBalanceDeltas;

use crate::{core::build_balance_deltas, pb::uniswap::v3::{Events, Pool}};
use substreams::{
    store::{StoreAddBigInt, StoreGet, StoreGetProto, StoreNew},
};

#[substreams::handlers::map]
pub fn map_balance_changes(
    events: Events,
    pools_store: StoreGetProto<Pool>,
) -> Result<BlockBalanceDeltas, anyhow::Error> {
    Ok(build_balance_deltas(events, &pools_store))
}

#[substreams::handlers::store]
pub fn store_pools_balances(balances_deltas: BlockBalanceDeltas, store: StoreAddBigInt) {
    tycho_substreams::balances::store_balance_changes(balances_deltas, store);
}
