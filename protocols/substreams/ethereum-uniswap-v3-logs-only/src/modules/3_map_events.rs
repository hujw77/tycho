use std::collections::HashSet;

use anyhow::Ok;
use substreams::{
    store::{StoreGet, StoreGetProto},
    Hex,
};
use substreams_ethereum::{
    pb::eth::v2::{self as eth, Log, TransactionTrace},
    Event,
};
use substreams_helper::hex::Hexable;

use crate::{
    abi::factory::events::PoolCreated,
    abi::pool::events::{
        Burn, Collect, CollectProtocol, Flash, Initialize, Mint, SetFeeProtocol, Swap,
    },
    pb::uniswap::v3::{
        events::{
            pool_event::{self, Type},
            PoolEvent,
        },
        Block, Events, Pool,
    },
};

#[substreams::handlers::map]
pub fn map_events(
    params: String,
    block: eth::Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<Events, anyhow::Error> {
    let filter = parse_event_filter(&params);
    let block_ts = block.timestamp_seconds();
    let mut pool_events = block
        .transaction_traces
        .into_iter()
        .filter(|tx| tx.status == 1)
        .flat_map(|tx| {
            let receipt = tx
                .receipt
                .as_ref()
                .expect("all transaction traces have a receipt");

            receipt
                .logs
                .iter()
                .filter_map(|log| log_to_event(log, &tx, &filter, &pools_store))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    pool_events.sort_unstable_by_key(|e| e.log_ordinal);

    Ok(Events {
        block: Some(Block {
            hash: block.hash,
            parent_hash: block
                .header
                .as_ref()
                .expect("block header should be present")
                .parent_hash
                .clone(),
            number: block.number,
            ts: block_ts,
        }),
        pool_events,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventFilter {
    factory_address: String,
    allowed_pools: HashSet<String>,
}

fn log_to_event(
    event: &Log,
    tx: &TransactionTrace,
    filter: &EventFilter,
    pools_store: &StoreGetProto<Pool>,
) -> Option<PoolEvent> {
    let log_address = event.address.to_hex();

    if log_address.eq_ignore_ascii_case(&filter.factory_address) {
        if let Some(created) = PoolCreated::match_and_decode(event) {
            let pool_address = created.pool.to_hex().to_lowercase();
            if !filter.allows_pool(&pool_address) {
                return None;
            }

            return Some(PoolEvent {
                log_ordinal: event.ordinal,
                pool_address: pool_address,
                token0: created.token0.to_hex(),
                token1: created.token1.to_hex(),
                transaction: Some(tx.into()),
                r#type: Some(Type::PoolCreated(pool_event::PoolCreated {
                    fee: created.fee.to_u64(),
                    tick_spacing: created.tick_spacing.into(),
                })),
            });
        }
    }

    if !filter.allows_pool(&log_address) {
        return None;
    }

    let (token0, token1) = pools_store
        .get_last(format!("Pool:{}", &log_address))
        .map(|pool| {
            (
                Hex(pool.token0.clone()).to_string(),
                Hex(pool.token1.clone()).to_string(),
            )
        })
        .unwrap_or_else(|| (String::new(), String::new()));

    if let Some(init) = Initialize::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::Initialize(pool_event::Initialize {
                sqrt_price: init.sqrt_price_x96.to_string(),
                tick: init.tick.into(),
            })),
        })
    } else if let Some(swap) = Swap::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::Swap(pool_event::Swap {
                sender: swap.sender.to_hex(),
                recipient: swap.recipient.to_hex(),
                amount_0: swap.amount0.to_string(),
                amount_1: swap.amount1.to_string(),
                sqrt_price: swap.sqrt_price_x96.to_string(),
                liquidity: swap.liquidity.to_string(),
                tick: swap.tick.into(),
            })),
        })
    } else if let Some(flash) = Flash::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::Flash(pool_event::Flash {
                sender: flash.sender.to_hex(),
                recipient: flash.recipient.to_hex(),
                amount_0: flash.amount0.to_string(),
                amount_1: flash.amount1.to_string(),
                paid_0: flash.paid0.to_string(),
                paid_1: flash.paid1.to_string(),
            })),
        })
    } else if let Some(mint) = Mint::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::Mint(pool_event::Mint {
                sender: mint.sender.to_hex(),
                owner: mint.owner.to_hex(),
                tick_lower: mint.tick_lower.into(),
                tick_upper: mint.tick_upper.into(),
                amount: mint.amount.to_string(),
                amount_0: mint.amount0.to_string(),
                amount_1: mint.amount1.to_string(),
            })),
        })
    } else if let Some(burn) = Burn::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::Burn(pool_event::Burn {
                owner: burn.owner.to_hex(),
                tick_lower: burn.tick_lower.into(),
                tick_upper: burn.tick_upper.into(),
                amount: burn.amount.to_string(),
                amount_0: burn.amount0.to_string(),
                amount_1: burn.amount1.to_string(),
            })),
        })
    } else if let Some(collect) = Collect::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::Collect(pool_event::Collect {
                owner: collect.owner.to_hex(),
                recipient: collect.recipient.to_hex(),
                tick_lower: collect.tick_lower.into(),
                tick_upper: collect.tick_upper.into(),
                amount_0: collect.amount0.to_string(),
                amount_1: collect.amount1.to_string(),
            })),
        })
    } else if let Some(set_fp) = SetFeeProtocol::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::SetFeeProtocol(pool_event::SetFeeProtocol {
                fee_protocol_0_old: set_fp.fee_protocol0_old.to_u64(),
                fee_protocol_1_old: set_fp.fee_protocol1_old.to_u64(),
                fee_protocol_0_new: set_fp.fee_protocol0_new.to_u64(),
                fee_protocol_1_new: set_fp.fee_protocol1_new.to_u64(),
            })),
        })
    } else if let Some(cp) = CollectProtocol::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0,
            token1,
            transaction: Some(tx.into()),
            r#type: Some(Type::CollectProtocol(pool_event::CollectProtocol {
                sender: cp.sender.to_hex(),
                recipient: cp.recipient.to_hex(),
                amount_0: cp.amount0.to_string(),
                amount_1: cp.amount1.to_string(),
            })),
        })
    } else {
        None
    }
}

impl EventFilter {
    fn allows_pool(&self, pool_address: &str) -> bool {
        self.allowed_pools.is_empty() || self.allowed_pools.contains(&pool_address.to_lowercase())
    }
}

fn parse_event_filter(params: &str) -> EventFilter {
    let mut factory_address = None;
    let mut allowed_pools = HashSet::new();

    for part in params.split('&').filter(|part| !part.is_empty()) {
        if let Some(address) = part.strip_prefix("factory=") {
            factory_address = Some(address.to_lowercase());
            continue;
        }

        if let Some(pool) = part.strip_prefix("pool=") {
            allowed_pools.insert(pool.to_lowercase());
            continue;
        }

        if let Some(pools) = part.strip_prefix("pools=") {
            for pool in pools.split(',').filter(|pool| !pool.is_empty()) {
                allowed_pools.insert(pool.to_lowercase());
            }
            continue;
        }
    }

    EventFilter {
        factory_address: factory_address.unwrap_or_else(|| params.to_lowercase()),
        allowed_pools,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_event_filter;

    #[test]
    fn parse_event_filter_supports_legacy_factory_only_param() {
        let filter = parse_event_filter("1F98431c8aD98523631AE4a59f267346ea31F984");

        assert_eq!(filter.factory_address, "1f98431c8ad98523631ae4a59f267346ea31f984");
        assert!(filter.allowed_pools.is_empty());
    }

    #[test]
    fn parse_event_filter_supports_single_and_multiple_pool_params() {
        let filter = parse_event_filter(
            "factory=0x1F98431c8aD98523631AE4a59f267346ea31F984&pool=0xe0554a476a092703abdb3ef35c80e0d76d32939f&pools=0x1111111111111111111111111111111111111111,0x2222222222222222222222222222222222222222",
        );

        assert_eq!(filter.factory_address, "0x1f98431c8ad98523631ae4a59f267346ea31f984");
        assert!(filter.allowed_pools.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"));
        assert!(filter.allowed_pools.contains("0x1111111111111111111111111111111111111111"));
        assert!(filter.allowed_pools.contains("0x2222222222222222222222222222222222222222"));
        assert!(!filter.allows_pool("0x3333333333333333333333333333333333333333"));
    }
}
