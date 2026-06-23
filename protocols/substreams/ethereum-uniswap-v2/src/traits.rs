use ethabi::ethereum_types::Address;
use substreams::store::{StoreGet, StoreGetProto};
use std::collections::HashSet;

use substreams_helper::{common::HasAddresser, hex::Hexable};

use tycho_substreams::prelude::*;

use crate::store_key::StoreKey;

pub struct PoolAddresser<'a> {
    pub store: &'a StoreGetProto<ProtocolComponent>,
    pub bootstrap_pools: &'a HashSet<String>,
}

impl HasAddresser for PoolAddresser<'_> {
    fn has_address(&self, key: Address) -> bool {
        let key_hex = key.to_hex();
        if self.bootstrap_pools.contains(&key_hex) {
            return true;
        }

        let pool = self
            .store
            .get_last(StoreKey::Pool.get_unique_pool_key(&key_hex));

        pool.is_some()
    }
}
