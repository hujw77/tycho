pub use map_balance_changes_bootstrap::map_balance_changes_bootstrap;
pub use map_bootstrap_pools_created::map_bootstrap_pools_created;
pub use map_events_bootstrap::map_events_bootstrap;
pub use map_liquidity_changes_bootstrap::{
    map_liquidity_changes_bootstrap, store_pool_current_tick_bootstrap,
};
pub use map_pool_created::map_pools_created;
pub use map_protocol_changes::map_protocol_changes;
pub use map_protocol_changes_bootstrap::map_protocol_changes_bootstrap;
pub use map_ticks_changes_bootstrap::map_ticks_changes_bootstrap;
pub use store_pools::store_pools;
use substreams_ethereum::pb::eth::v2::TransactionTrace;

use crate::pb::uniswap::v3::Transaction;

mod bootstrap;

#[path = "1_map_pool_created.rs"]
mod map_pool_created;

#[path = "1_map_bootstrap_pools_created.rs"]
mod map_bootstrap_pools_created;

#[path = "2_store_pools.rs"]
mod store_pools;

#[path = "3_map_events.rs"]
mod map_events;

#[path = "3_map_events_bootstrap.rs"]
mod map_events_bootstrap;

#[path = "4_map_and_store_balance_changes.rs"]
mod map_store_balance_changes;

#[path = "4_map_and_store_balance_changes_bootstrap.rs"]
mod map_balance_changes_bootstrap;

#[path = "4_map_and_store_ticks.rs"]
mod map_store_ticks;

#[path = "4_map_and_store_ticks_bootstrap.rs"]
mod map_ticks_changes_bootstrap;

#[path = "4_map_and_store_liquidity.rs"]
mod map_store_liquidity;

#[path = "4_map_and_store_liquidity_bootstrap.rs"]
mod map_liquidity_changes_bootstrap;

#[path = "5_map_protocol_changes.rs"]
mod map_protocol_changes;

#[path = "5_map_protocol_changes_bootstrap.rs"]
mod map_protocol_changes_bootstrap;

impl From<TransactionTrace> for Transaction {
    fn from(value: TransactionTrace) -> Self {
        Self { hash: value.hash, from: value.from, to: value.to, index: value.index.into() }
    }
}

impl From<&TransactionTrace> for Transaction {
    fn from(value: &TransactionTrace) -> Self {
        Self {
            hash: value.hash.clone(),
            from: value.from.clone(),
            to: value.to.clone(),
            index: value.index.into(),
        }
    }
}

impl From<&Transaction> for tycho_substreams::prelude::Transaction {
    fn from(value: &Transaction) -> Self {
        Self {
            hash: value.hash.clone(),
            from: value.from.clone(),
            to: value.to.clone(),
            index: value.index,
        }
    }
}

impl From<Transaction> for tycho_substreams::prelude::Transaction {
    fn from(value: Transaction) -> Self {
        Self { hash: value.hash, from: value.from, to: value.to, index: value.index }
    }
}

impl From<&tycho_substreams::prelude::Transaction> for Transaction {
    fn from(value: &tycho_substreams::prelude::Transaction) -> Self {
        Self {
            hash: value.hash.clone(),
            from: value.from.clone(),
            to: value.to.clone(),
            index: value.index,
        }
    }
}

impl From<tycho_substreams::prelude::Transaction> for Transaction {
    fn from(value: tycho_substreams::prelude::Transaction) -> Self {
        Self { hash: value.hash, from: value.from, to: value.to, index: value.index }
    }
}
