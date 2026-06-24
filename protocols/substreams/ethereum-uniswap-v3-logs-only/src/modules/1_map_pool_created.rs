use substreams_ethereum::pb::eth::v2::{self as eth};
use crate::core::build_pool_created_block_entity_changes;
use tycho_substreams::prelude::*;

#[substreams::handlers::map]
pub fn map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(build_pool_created_block_entity_changes(&params, &block))
}
