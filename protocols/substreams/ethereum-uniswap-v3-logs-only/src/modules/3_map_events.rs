use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};

use crate::{core::build_pool_events, pb::uniswap::v3::{Events, Pool}};

#[substreams::handlers::map]
pub fn map_events(
    params: String,
    block: eth::Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<Events, anyhow::Error> {
    Ok(build_pool_events(&params, block, &pools_store))
}
