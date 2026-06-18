use anyhow::Ok;
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
        Block, Events,
    },
};

#[substreams::handlers::map]
pub fn map_events(params: String, block: eth::Block) -> Result<Events, anyhow::Error> {
    let factory_address = parse_factory_address(&params);
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
                .filter_map(|log| log_to_event(log, &tx, &factory_address))
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

fn log_to_event(
    event: &Log,
    tx: &TransactionTrace,
    factory_address: &str,
) -> Option<PoolEvent> {
    let log_address = event.address.to_hex();

    if log_address.eq_ignore_ascii_case(factory_address) {
        if let Some(created) = PoolCreated::match_and_decode(event) {
            return Some(PoolEvent {
                log_ordinal: event.ordinal,
                pool_address: created.pool.to_hex(),
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

    if let Some(init) = Initialize::match_and_decode(event) {
        Some(PoolEvent {
            log_ordinal: event.ordinal,
            pool_address: log_address,
            token0: String::new(),
            token1: String::new(),
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
            token0: String::new(),
            token1: String::new(),
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
            token0: String::new(),
            token1: String::new(),
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
            token0: String::new(),
            token1: String::new(),
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
            token0: String::new(),
            token1: String::new(),
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
            token0: String::new(),
            token1: String::new(),
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
            token0: String::new(),
            token1: String::new(),
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
            token0: String::new(),
            token1: String::new(),
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

fn parse_factory_address(params: &str) -> String {
    params
        .split('&')
        .find_map(|part| part.strip_prefix("factory="))
        .unwrap_or(params)
        .to_lowercase()
}
