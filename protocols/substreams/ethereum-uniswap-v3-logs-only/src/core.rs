use std::{
    collections::{HashMap, HashSet, VecDeque},
    str::FromStr,
};

use ethabi::ethereum_types::Address;
use itertools::Itertools;
use substreams::{
    pb::substreams::{StoreDelta, StoreDeltas},
    scalar::BigInt,
    store::{StoreGet, StoreGetInt64, StoreGetProto},
    Hex,
};
use substreams_ethereum::{
    pb::eth::v2::{self as eth, Log, TransactionTrace},
    Event,
};
use substreams_helper::{event_handler::EventHandler, hex::Hexable};
use tycho_substreams::{
    balances::aggregate_balances_changes,
    models::BlockBalanceDeltas,
    prelude::*,
};

use crate::{
    abi::{
        factory::events::PoolCreated,
        pool::events::{
            Burn, Collect, CollectProtocol, Flash, Initialize, Mint, SetFeeProtocol, Swap,
        },
    },
    pb::uniswap::v3::{
        events::{
            pool_event::{self, Type},
            PoolEvent,
        },
        Block, Events, LiquidityChange, LiquidityChangeType, LiquidityChanges, Pool, TickDelta,
        TickDeltas,
    },
};

type PoolAddress = Vec<u8>;

pub fn build_pool_created_block_entity_changes(
    params: &str,
    block: &eth::Block,
) -> BlockEntityChanges {
    let mut new_pools: Vec<TransactionEntityChanges> = vec![];
    collect_new_pools(block, &mut new_pools, params);
    BlockEntityChanges { block: None, changes: new_pools }
}

pub fn build_pool_events(
    params: &str,
    block: eth::Block,
    pools_store: &StoreGetProto<Pool>,
) -> Events {
    let filter = parse_event_filter(params);
    let block_ts = block.timestamp_seconds();
    let mut discovered_pools = HashMap::<String, PoolMetadata>::new();
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
                .filter_map(|log| {
                    log_to_event(log, &tx, &filter, pools_store, &mut discovered_pools)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    pool_events.sort_unstable_by_key(|e| e.log_ordinal);

    Events {
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
    }
}

pub fn build_balance_deltas(
    events: Events,
    pools_store: &StoreGetProto<Pool>,
) -> BlockBalanceDeltas {
    let balance_deltas = events
        .pool_events
        .into_iter()
        .flat_map(|event| event_to_balance_deltas(event, pools_store))
        .collect();

    BlockBalanceDeltas { balance_deltas }
}

pub fn build_tick_deltas(events: Events) -> TickDeltas {
    let deltas = events
        .pool_events
        .into_iter()
        .flat_map(event_to_ticks_deltas)
        .collect();

    TickDeltas { deltas }
}

pub fn collect_current_tick_updates(events: Events) -> Vec<(String, u64, i32)> {
    events
        .pool_events
        .into_iter()
        .filter_map(event_to_current_tick)
        .collect()
}

pub fn build_liquidity_changes(
    events: Events,
    pools_current_tick_store: &StoreGetInt64,
) -> LiquidityChanges {
    let mut changes = events
        .pool_events
        .into_iter()
        .filter(PoolEvent::can_introduce_liquidity_changes)
        .map(|event| {
            (
                pools_current_tick_store
                    .get_at(event.log_ordinal, format!("pool:{0}", &event.pool_address))
                    .unwrap_or(0),
                event,
            )
        })
        .filter_map(|(current_tick, event)| event_to_liquidity_deltas(current_tick, event))
        .collect::<Vec<_>>();

    changes.sort_unstable_by_key(|change| change.ordinal);
    LiquidityChanges { changes }
}

#[allow(clippy::too_many_arguments)]
pub fn build_protocol_changes(
    block: eth::Block,
    created_pools: BlockEntityChanges,
    events: Events,
    balances_map_deltas: BlockBalanceDeltas,
    balances_store_deltas: StoreDeltas,
    ticks_map_deltas: TickDeltas,
    ticks_store_deltas: StoreDeltas,
    pool_liquidity_changes: LiquidityChanges,
    pool_liquidity_store_deltas: StoreDeltas,
) -> BlockChanges {
    let mut transaction_changes: HashMap<_, TransactionChangesBuilder> = HashMap::new();

    for change in created_pools.changes {
        let tx = change.tx.as_ref().unwrap();
        let builder = transaction_changes
            .entry(tx.index)
            .or_insert_with(|| TransactionChangesBuilder::new(tx));

        change
            .component_changes
            .iter()
            .for_each(|component| {
                builder.add_protocol_component(component);
                component
                    .contracts
                    .iter()
                    .for_each(|contract| {
                        builder.add_contract_changes(&InterimContractChange::new(contract, true))
                    });
            });
        change
            .entity_changes
            .iter()
            .for_each(|entity_change| builder.add_entity_change(entity_change));
        change
            .balance_changes
            .iter()
            .for_each(|balance_change| builder.add_balance_change(balance_change));
    }

    aggregate_balances_changes(balances_store_deltas, balances_map_deltas)
        .into_iter()
        .for_each(|(_, (tx, balances))| {
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx));
            balances
                .values()
                .for_each(|token_bc_map| {
                    token_bc_map
                        .values()
                        .for_each(|balance_change| builder.add_balance_change(balance_change))
                });
        });

    let mut indexed_tick_store_deltas = index_store_deltas(ticks_store_deltas.deltas);
    ticks_map_deltas
        .deltas
        .into_iter()
        .for_each(|tick_delta| {
            let tick_store_key = format!(
                "pool:{}:tick:{}",
                hex::encode(&tick_delta.pool_address),
                tick_delta.tick_index
            );
            let store_delta = pop_matching_store_delta(
                &mut indexed_tick_store_deltas,
                &tick_store_key,
                tick_delta.ordinal,
            );
            let new_value_bigint =
                BigInt::from_str(&String::from_utf8(store_delta.new_value).unwrap()).unwrap();

            let is_creation = store_delta.old_value.is_empty() ||
                BigInt::from_str(&String::from_utf8(store_delta.old_value).unwrap())
                    .unwrap()
                    .is_zero();
            let attribute = Attribute {
                name: format!("ticks/{}/net-liquidity", tick_delta.tick_index),
                value: new_value_bigint.to_signed_bytes_be(),
                change: if is_creation {
                    ChangeType::Creation.into()
                } else if new_value_bigint.is_zero() {
                    ChangeType::Deletion.into()
                } else {
                    ChangeType::Update.into()
                },
            };
            let tx = tick_delta.transaction.unwrap();
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx.into()));

            builder.add_entity_change(&EntityChanges {
                component_id: tick_delta.pool_address.to_hex(),
                attributes: vec![attribute],
            });
        });
    assert_all_store_deltas_consumed(indexed_tick_store_deltas);

    let mut indexed_liquidity_store_deltas = index_store_deltas(pool_liquidity_store_deltas.deltas);
    pool_liquidity_changes
        .changes
        .into_iter()
        .for_each(|change| {
            let liquidity_store_key = format!("pool:{}", hex::encode(&change.pool_address));
            let store_delta = pop_matching_store_delta(
                &mut indexed_liquidity_store_deltas,
                &liquidity_store_key,
                change.ordinal,
            );
            let new_value_bigint = BigInt::from_str(
                String::from_utf8(store_delta.new_value)
                    .unwrap()
                    .split(':')
                    .nth(1)
                    .unwrap(),
            )
            .unwrap();
            let tx = change.transaction.unwrap();
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx.into()));

            builder.add_entity_change(&EntityChanges {
                component_id: change.pool_address.to_hex(),
                attributes: vec![Attribute {
                    name: "liquidity".to_string(),
                    value: new_value_bigint.to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                }],
            });
        });
    assert_all_store_deltas_consumed(indexed_liquidity_store_deltas);

    events
        .pool_events
        .into_iter()
        .flat_map(event_to_attributes_updates)
        .for_each(|(tx, pool_address, attribute)| {
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx));
            builder.add_entity_change(&EntityChanges {
                component_id: pool_address.to_hex(),
                attributes: vec![attribute],
            });
        });

    BlockChanges {
        block: Some((&block).into()),
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect::<Vec<_>>(),
        ..Default::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventFilter {
    factory_address: String,
    allowed_pools: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PoolMetadata {
    token0: String,
    token1: String,
}

fn collect_new_pools(
    block: &eth::Block,
    new_pools: &mut Vec<TransactionEntityChanges>,
    factory_address: &str,
) {
    let mut on_pool_created = |event: PoolCreated, tx: &eth::TransactionTrace, _log: &eth::Log| {
        let tycho_tx: Transaction = tx.into();

        new_pools.push(TransactionEntityChanges {
            tx: Some(tycho_tx.clone()),
            entity_changes: vec![EntityChanges {
                component_id: event.pool.clone().to_hex(),
                attributes: vec![
                    Attribute {
                        name: "liquidity".to_string(),
                        value: BigInt::from(0).to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                    Attribute {
                        name: "tick".to_string(),
                        value: BigInt::from(0).to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                    Attribute {
                        name: "sqrt_price_x96".to_string(),
                        value: BigInt::from(0).to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                ],
            }],
            component_changes: vec![ProtocolComponent {
                id: event.pool.to_hex(),
                tokens: vec![event.token0.clone(), event.token1.clone()],
                contracts: vec![event.pool.clone()],
                static_att: vec![
                    Attribute {
                        name: "fee".to_string(),
                        value: event.fee.to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                    Attribute {
                        name: "tick_spacing".to_string(),
                        value: event.tick_spacing.to_signed_bytes_be(),
                        change: ChangeType::Creation.into(),
                    },
                    Attribute {
                        name: "pool_address".to_string(),
                        value: event.pool.clone(),
                        change: ChangeType::Creation.into(),
                    },
                ],
                change: i32::from(ChangeType::Creation),
                protocol_type: Option::from(ProtocolType {
                    name: "uniswap_v3_pool".to_string(),
                    financial_type: FinancialType::Swap.into(),
                    attribute_schema: vec![],
                    implementation_type: ImplementationType::Custom.into(),
                }),
            }],
            balance_changes: vec![
                BalanceChange {
                    token: event.token0,
                    balance: BigInt::from(0).to_signed_bytes_be(),
                    component_id: event.pool.clone().to_hex().as_bytes().to_vec(),
                },
                BalanceChange {
                    token: event.token1,
                    balance: BigInt::from(0).to_signed_bytes_be(),
                    component_id: event.pool.to_hex().as_bytes().to_vec(),
                },
            ],
        })
    };

    let mut eh = EventHandler::new(block);
    eh.filter_by_address(vec![Address::from_str(factory_address).unwrap()]);
    eh.on::<PoolCreated, _>(&mut on_pool_created);
    eh.handle_events();
}

fn log_to_event(
    event: &Log,
    tx: &TransactionTrace,
    filter: &EventFilter,
    pools_store: &StoreGetProto<Pool>,
    discovered_pools: &mut HashMap<String, PoolMetadata>,
) -> Option<PoolEvent> {
    let log_address = event.address.to_hex();

    if log_address.eq_ignore_ascii_case(&filter.factory_address) {
        if let Some(created) = PoolCreated::match_and_decode(event) {
            let pool_address = created.pool.to_hex().to_lowercase();
            discovered_pools.insert(
                pool_address.clone(),
                PoolMetadata {
                    token0: created.token0.to_hex(),
                    token1: created.token1.to_hex(),
                },
            );

            return Some(PoolEvent {
                log_ordinal: event.ordinal,
                pool_address,
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

    let metadata = pools_store
        .get_last(format!("Pool:{}", &log_address))
        .map(|pool| PoolMetadata {
            token0: Hex(pool.token0).to_string(),
            token1: Hex(pool.token1).to_string(),
        })
        .or_else(|| discovered_pools.get(&log_address).cloned());

    if !filter.allows_pool_log(&log_address, metadata.is_some()) {
        return None;
    }

    let (token0, token1) = metadata
        .map(|pool| (pool.token0, pool.token1))
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

fn event_to_balance_deltas(event: PoolEvent, pools_store: &StoreGetProto<Pool>) -> Vec<BalanceDelta> {
    let address = event.pool_address.as_bytes().to_vec();
    let pool = match pools_store.get_last(format!("Pool:{}", event.pool_address)) {
        Some(pool) => pool,
        None => return vec![],
    };

    let ordinal = event.log_ordinal;
    let transaction = event.transaction.clone();

    match event.r#type.unwrap() {
        pool_event::Type::Mint(mint) => vec![
            BalanceDelta {
                token: pool.token0.clone(),
                delta: BigInt::from_str(&mint.amount_0)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: ordinal,
                tx: transaction.as_ref().map(Into::into),
            },
            BalanceDelta {
                token: pool.token1.clone(),
                delta: BigInt::from_str(&mint.amount_1)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: ordinal,
                tx: transaction.map(Into::into),
            },
        ],
        pool_event::Type::Collect(collect) => vec![
            BalanceDelta {
                token: pool.token0.clone(),
                delta: BigInt::from_str(&collect.amount_0)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: ordinal,
                tx: transaction.as_ref().map(Into::into),
            },
            BalanceDelta {
                token: pool.token1.clone(),
                delta: BigInt::from_str(&collect.amount_1)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: ordinal,
                tx: transaction.map(Into::into),
            },
        ],
        pool_event::Type::Burn(_) => vec![],
        pool_event::Type::Swap(swap) => vec![
            BalanceDelta {
                token: pool.token0.clone(),
                delta: BigInt::from_str(&swap.amount_0)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: ordinal,
                tx: transaction.as_ref().map(Into::into),
            },
            BalanceDelta {
                token: pool.token1.clone(),
                delta: BigInt::from_str(&swap.amount_1)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: ordinal,
                tx: transaction.map(Into::into),
            },
        ],
        pool_event::Type::Flash(flash) => vec![
            BalanceDelta {
                token: pool.token0.clone(),
                delta: BigInt::from_str(&flash.paid_0)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: ordinal,
                tx: transaction.as_ref().map(Into::into),
            },
            BalanceDelta {
                token: pool.token1.clone(),
                delta: BigInt::from_str(&flash.paid_1)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: ordinal,
                tx: transaction.map(Into::into),
            },
        ],
        pool_event::Type::CollectProtocol(collect_protocol) => vec![
            BalanceDelta {
                token: pool.token0.clone(),
                delta: BigInt::from_str(&collect_protocol.amount_0)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: ordinal,
                tx: transaction.as_ref().map(Into::into),
            },
            BalanceDelta {
                token: pool.token1.clone(),
                delta: BigInt::from_str(&collect_protocol.amount_1)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: ordinal,
                tx: transaction.map(Into::into),
            },
        ],
        _ => vec![],
    }
}

fn event_to_ticks_deltas(event: PoolEvent) -> Vec<TickDelta> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Mint(mint) => vec![
            TickDelta {
                pool_address: decode_prefixed_hex(&event.pool_address),
                tick_index: mint.tick_lower,
                liquidity_net_delta: BigInt::from_str(&mint.amount)
                    .unwrap()
                    .to_signed_bytes_be(),
                ordinal: event.log_ordinal,
                transaction: event.transaction.clone(),
            },
            TickDelta {
                pool_address: decode_prefixed_hex(&event.pool_address),
                tick_index: mint.tick_upper,
                liquidity_net_delta: BigInt::from_str(&mint.amount)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                ordinal: event.log_ordinal,
                transaction: event.transaction,
            },
        ],
        pool_event::Type::Burn(burn) => vec![
            TickDelta {
                pool_address: decode_prefixed_hex(&event.pool_address),
                tick_index: burn.tick_lower,
                liquidity_net_delta: BigInt::from_str(&burn.amount)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                ordinal: event.log_ordinal,
                transaction: event.transaction.clone(),
            },
            TickDelta {
                pool_address: decode_prefixed_hex(&event.pool_address),
                tick_index: burn.tick_upper,
                liquidity_net_delta: BigInt::from_str(&burn.amount)
                    .unwrap()
                    .to_signed_bytes_be(),
                ordinal: event.log_ordinal,
                transaction: event.transaction,
            },
        ],
        _ => vec![],
    }
}

fn event_to_liquidity_deltas(current_tick: i64, event: PoolEvent) -> Option<LiquidityChange> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Mint(mint) => {
            if current_tick >= mint.tick_lower.into() && current_tick < mint.tick_upper.into() {
                Some(LiquidityChange {
                    pool_address: decode_prefixed_hex(&event.pool_address),
                    value: BigInt::from_str(&mint.amount)
                        .unwrap()
                        .to_signed_bytes_be(),
                    change_type: LiquidityChangeType::Delta.into(),
                    ordinal: event.log_ordinal,
                    transaction: Some(event.transaction.unwrap()),
                })
            } else {
                None
            }
        }
        pool_event::Type::Burn(burn) => {
            if current_tick >= burn.tick_lower.into() && current_tick < burn.tick_upper.into() {
                Some(LiquidityChange {
                    pool_address: decode_prefixed_hex(&event.pool_address),
                    value: BigInt::from_str(&burn.amount)
                        .unwrap()
                        .neg()
                        .to_signed_bytes_be(),
                    change_type: LiquidityChangeType::Delta.into(),
                    ordinal: event.log_ordinal,
                    transaction: Some(event.transaction.unwrap()),
                })
            } else {
                None
            }
        }
        pool_event::Type::Swap(swap) => Some(LiquidityChange {
            pool_address: decode_prefixed_hex(&event.pool_address),
            value: BigInt::from_str(&swap.liquidity)
                .unwrap()
                .to_signed_bytes_be(),
            change_type: LiquidityChangeType::Absolute.into(),
            ordinal: event.log_ordinal,
            transaction: Some(event.transaction.unwrap()),
        }),
        _ => None,
    }
}

fn event_to_current_tick(event: PoolEvent) -> Option<(String, u64, i32)> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Initialize(initialize) => {
            Some((event.pool_address, event.log_ordinal, initialize.tick))
        }
        pool_event::Type::Swap(swap) => Some((event.pool_address, event.log_ordinal, swap.tick)),
        _ => None,
    }
}

fn event_to_attributes_updates(event: PoolEvent) -> Vec<(Transaction, PoolAddress, Attribute)> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Initialize(initialize) => vec![
            (
                event.transaction.as_ref().unwrap().into(),
                decode_prefixed_hex(&event.pool_address),
                Attribute {
                    name: "sqrt_price_x96".to_string(),
                    value: BigInt::from_str(&initialize.sqrt_price)
                        .unwrap()
                        .to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
            (
                event.transaction.unwrap().into(),
                decode_prefixed_hex(&event.pool_address),
                Attribute {
                    name: "tick".to_string(),
                    value: BigInt::from(initialize.tick).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
        ],
        pool_event::Type::Swap(swap) => vec![
            (
                event.transaction.as_ref().unwrap().into(),
                decode_prefixed_hex(&event.pool_address),
                Attribute {
                    name: "sqrt_price_x96".to_string(),
                    value: BigInt::from_str(&swap.sqrt_price)
                        .unwrap()
                        .to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
            (
                event.transaction.unwrap().into(),
                decode_prefixed_hex(&event.pool_address),
                Attribute {
                    name: "tick".to_string(),
                    value: BigInt::from(swap.tick).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
        ],
        pool_event::Type::SetFeeProtocol(set_fee_protocol) => vec![
            (
                event.transaction.as_ref().unwrap().into(),
                decode_prefixed_hex(&event.pool_address),
                Attribute {
                    name: "protocol_fees/token0".to_string(),
                    value: BigInt::from(set_fee_protocol.fee_protocol_0_new).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
            (
                event.transaction.unwrap().into(),
                decode_prefixed_hex(&event.pool_address),
                Attribute {
                    name: "protocol_fees/token1".to_string(),
                    value: BigInt::from(set_fee_protocol.fee_protocol_1_new).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
        ],
        _ => vec![],
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
        }
    }

    EventFilter {
        factory_address: factory_address.unwrap_or_else(|| params.to_lowercase()),
        allowed_pools,
    }
}

fn decode_prefixed_hex(value: &str) -> Vec<u8> {
    hex::decode(value.trim_start_matches("0x")).unwrap()
}

fn index_store_deltas(deltas: Vec<StoreDelta>) -> HashMap<(String, u64), VecDeque<StoreDelta>> {
    let mut indexed = HashMap::<(String, u64), VecDeque<StoreDelta>>::new();

    deltas.into_iter().for_each(|delta| {
        indexed
            .entry((delta.key.clone(), delta.ordinal))
            .or_default()
            .push_back(delta);
    });

    indexed
}

fn pop_matching_store_delta(
    indexed_store_deltas: &mut HashMap<(String, u64), VecDeque<StoreDelta>>,
    key: &str,
    ordinal: u64,
) -> StoreDelta {
    indexed_store_deltas
        .get_mut(&(key.to_string(), ordinal))
        .and_then(|queue| queue.pop_front())
        .unwrap_or_else(|| panic!("Missing matching store delta for key `{}` at ordinal {}", key, ordinal))
}

fn assert_all_store_deltas_consumed(
    indexed_store_deltas: HashMap<(String, u64), VecDeque<StoreDelta>>,
) {
    let leftovers = indexed_store_deltas
        .into_iter()
        .filter_map(|((key, ordinal), mut queue)| queue.pop_front().map(|_| (key, ordinal)))
        .collect::<Vec<_>>();

    if !leftovers.is_empty() {
        panic!("Unmatched store deltas remaining: {:?}", leftovers);
    }
}

impl EventFilter {
    fn allows_pool(&self, pool_address: &str) -> bool {
        self.allowed_pools.is_empty() || self.allowed_pools.contains(&pool_address.to_lowercase())
    }

    fn allows_pool_log(&self, pool_address: &str, is_known_pool: bool) -> bool {
        self.allows_pool(pool_address) || is_known_pool
    }
}

impl PoolEvent {
    fn can_introduce_liquidity_changes(&self) -> bool {
        matches!(
            self.r#type.as_ref().unwrap(),
            pool_event::Type::Mint(_) | pool_event::Type::Burn(_) | pool_event::Type::Swap(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use ethabi::{ethereum_types::{Address, U256}, Token};
    use prost_types::Timestamp;
    use substreams::pb::substreams::StoreDeltas;
    use substreams_ethereum::pb::eth::v2::{
        block::DetailLevel, Block, BlockHeader, Log, TransactionReceipt, TransactionTrace,
        transaction_trace::Type as TransactionType, TransactionTraceStatus,
    };
    use tycho_substreams::models::BlockBalanceDeltas;

    use crate::{
        pb::uniswap::v3::{Events, LiquidityChanges, TickDeltas},
    };

    use super::{
        build_pool_created_block_entity_changes, build_protocol_changes, parse_event_filter,
    };

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

    #[test]
    fn known_pool_logs_are_allowed_even_if_not_in_bootstrap_allowlist() {
        let filter = parse_event_filter(
            "factory=0x1F98431c8aD98523631AE4a59f267346ea31F984&pools=0x1111111111111111111111111111111111111111",
        );

        assert!(!filter.allows_pool("0x2222222222222222222222222222222222222222"));
        assert!(filter.allows_pool_log(
            "0x2222222222222222222222222222222222222222",
            true
        ));
        assert!(!filter.allows_pool_log(
            "0x2222222222222222222222222222222222222222",
            false
        ));
    }

    fn address(byte: u8) -> Vec<u8> {
        vec![byte; 20]
    }

    fn topic_address(byte: u8) -> Vec<u8> {
        ethabi::encode(&[Token::Address(Address::from_slice(&address(byte)))])
    }

    fn topic_uint24(value: u32) -> Vec<u8> {
        ethabi::encode(&[Token::Uint(U256::from(value))])
    }

    fn pool_created_log(factory: u8, token0: u8, token1: u8, fee: u32, tick_spacing: i32, pool: u8) -> Log {
        let data = ethabi::encode(&[
            Token::Int(tick_spacing.into()),
            Token::Address(Address::from_slice(&address(pool))),
        ]);

        Log {
            address: address(factory),
            topics: vec![
                vec![
                    120, 60, 202, 28, 4, 18, 221, 13, 105, 94, 120, 69, 104, 201, 109, 162, 233,
                    194, 47, 249, 137, 53, 122, 46, 139, 29, 155, 43, 78, 107, 113, 24,
                ],
                topic_address(token0),
                topic_address(token1),
                topic_uint24(fee),
            ],
            data,
            index: 0,
            block_index: 0,
            ordinal: 1,
        }
    }

    fn pool_created_block(
        factory: u8,
        token0: u8,
        token1: u8,
        fee: u32,
        tick_spacing: i32,
        pool: u8,
    ) -> Block {
        Block {
            hash: vec![0xaa; 32],
            number: 42,
            size: 0,
            header: Some(BlockHeader {
                parent_hash: vec![0xbb; 32],
                timestamp: Some(Timestamp {
                    seconds: 1_718_000_000,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            transaction_traces: vec![TransactionTrace {
                index: 0,
                hash: vec![0xcc; 32],
                from: vec![0x11; 20],
                to: address(factory),
                status: TransactionTraceStatus::Succeeded as i32,
                receipt: Some(TransactionReceipt {
                    logs: vec![pool_created_log(
                        factory,
                        token0,
                        token1,
                        fee,
                        tick_spacing,
                        pool,
                    )],
                    ..Default::default()
                }),
                r#type: TransactionType::TrxTypeLegacy as i32,
                ..Default::default()
            }],
            detail_level: DetailLevel::DetaillevelBase as i32,
            ..Default::default()
        }
    }

    #[test]
    fn pool_created_changes_include_pool_contract_address() {
        let block = pool_created_block(0xf1, 0xa0, 0xc0, 500, 10, 0x45);
        let changes = build_pool_created_block_entity_changes(
            "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
            &block,
        );
        let created = &changes.changes[0].component_changes[0];

        assert_eq!(created.id, "0x4545454545454545454545454545454545454545");
        assert_eq!(created.contracts, vec![vec![0x45; 20]]);
    }

    #[test]
    fn protocol_changes_promote_created_pool_contracts_into_contract_changes() {
        let block = pool_created_block(0xf1, 0xa0, 0xc0, 500, 10, 0x45);
        let created_pools = build_pool_created_block_entity_changes(
            "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
            &block,
        );
        let protocol_changes = build_protocol_changes(
            block,
            created_pools,
            Events {
                block: None,
                pool_events: vec![],
            },
            BlockBalanceDeltas {
                balance_deltas: vec![],
            },
            StoreDeltas { deltas: vec![] },
            TickDeltas { deltas: vec![] },
            StoreDeltas { deltas: vec![] },
            LiquidityChanges { changes: vec![] },
            StoreDeltas { deltas: vec![] },
        );

        assert_eq!(protocol_changes.changes.len(), 1);
        assert_eq!(protocol_changes.changes[0].contract_changes.len(), 1);
        assert_eq!(
            protocol_changes.changes[0].contract_changes[0].address,
            vec![0x45; 20]
        );
    }
}
