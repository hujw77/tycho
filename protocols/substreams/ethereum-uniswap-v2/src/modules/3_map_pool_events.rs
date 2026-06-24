use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};

use crate::core::build_pool_event_block_changes;
use tycho_substreams::prelude::*;

#[substreams::handlers::map]
pub fn map_pool_events(
    params: String,
    block: eth::Block,
    block_entity_changes: BlockChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
) -> Result<BlockChanges, substreams::errors::Error> {
    Ok(build_pool_event_block_changes(
        &params,
        &block,
        block_entity_changes,
        &pools_store,
    ))
}
