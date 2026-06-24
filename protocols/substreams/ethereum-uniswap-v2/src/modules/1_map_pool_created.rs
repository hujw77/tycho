use substreams_ethereum::pb::eth::v2::{self as eth};
use crate::core::{build_pool_created_block_changes, parse_pool_created_params};
use tycho_substreams::prelude::*;

#[substreams::handlers::map]
pub fn map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockChanges, substreams::errors::Error> {
    let params = parse_pool_created_params(&params);
    Ok(build_pool_created_block_changes(&block, &params))
}
