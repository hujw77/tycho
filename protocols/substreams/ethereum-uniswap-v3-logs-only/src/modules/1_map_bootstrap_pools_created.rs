use tycho_substreams::models::BlockEntityChanges;

use crate::modules::bootstrap::build_bootstrap_changes;

#[substreams::handlers::map]
pub fn map_bootstrap_pools_created(
    params: String,
    block: substreams_ethereum::pb::eth::v2::Block,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    build_bootstrap_changes(&params, &block).map_err(Into::into)
}
