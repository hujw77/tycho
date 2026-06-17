use std::str::FromStr;

use tycho_substreams::{
    models::{BalanceDelta, BlockBalanceDeltas},
    pb::tycho::evm::v1::TransactionEntityChanges,
};

use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events,
};

use substreams::scalar::BigInt;

#[substreams::handlers::map]
pub fn map_balance_changes_bootstrap(
    bootstrap_pools: tycho_substreams::models::BlockEntityChanges,
    events: Events,
) -> Result<BlockBalanceDeltas, anyhow::Error> {
    let mut balance_deltas = bootstrap_pools
        .changes
        .iter()
        .flat_map(created_pool_balance_deltas)
        .collect::<Vec<_>>();
    balance_deltas.extend(
        events
            .pool_events
            .into_iter()
            .flat_map(event_to_balance_deltas),
    );

    Ok(BlockBalanceDeltas { balance_deltas })
}

fn created_pool_balance_deltas(change: &TransactionEntityChanges) -> Vec<BalanceDelta> {
    let Some(tx) = change.tx.as_ref() else {
        return vec![];
    };

    change
        .balance_changes
        .iter()
        .cloned()
        .map(|balance_change| BalanceDelta {
            token: balance_change.token,
            delta: balance_change.balance,
            component_id: balance_change.component_id,
            ord: tx.index,
            tx: Some(tx.clone()),
        })
        .collect()
}

fn event_to_balance_deltas(event: PoolEvent) -> Vec<BalanceDelta> {
    let address = format!("0x{}", event.pool_address)
        .as_bytes()
        .to_vec();
    match event.r#type.unwrap() {
        pool_event::Type::Mint(e) => vec![
            BalanceDelta {
                token: hex::decode(event.token0).unwrap(),
                delta: BigInt::from_str(&e.amount_0)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: hex::decode(event.token1).unwrap(),
                delta: BigInt::from_str(&e.amount_1)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        pool_event::Type::Collect(e) => vec![
            BalanceDelta {
                token: hex::decode(event.token0).unwrap(),
                delta: BigInt::from_str(&e.amount_0)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: hex::decode(event.token1).unwrap(),
                delta: BigInt::from_str(&e.amount_1)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        pool_event::Type::Burn(_) => vec![],
        pool_event::Type::Swap(e) => vec![
            BalanceDelta {
                token: hex::decode(event.token0).unwrap(),
                delta: BigInt::from_str(&e.amount_0)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: hex::decode(event.token1).unwrap(),
                delta: BigInt::from_str(&e.amount_1)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        pool_event::Type::Flash(e) => vec![
            BalanceDelta {
                token: hex::decode(event.token0).unwrap(),
                delta: BigInt::from_str(&e.paid_0)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: hex::decode(event.token1).unwrap(),
                delta: BigInt::from_str(&e.paid_1)
                    .unwrap()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        pool_event::Type::CollectProtocol(e) => vec![
            BalanceDelta {
                token: hex::decode(event.token0).unwrap(),
                delta: BigInt::from_str(&e.amount_0)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: hex::decode(event.token1).unwrap(),
                delta: BigInt::from_str(&e.amount_1)
                    .unwrap()
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        _ => vec![],
    }
}
