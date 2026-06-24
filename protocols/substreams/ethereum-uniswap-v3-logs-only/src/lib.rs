#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod abi;
pub mod core;
pub mod pb;
#[cfg(feature = "standalone-handlers")]
mod modules;

#[cfg(feature = "standalone-handlers")]
pub use modules::*;

use substreams_ethereum::pb::eth::v2::TransactionTrace;

use crate::pb::uniswap::v3::Transaction;

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
