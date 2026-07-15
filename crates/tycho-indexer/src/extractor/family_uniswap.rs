use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    str::FromStr,
};

use chrono::{DateTime, NaiveDateTime};
use num_bigint::BigInt;
use prost::Message;
use tracing::{trace, warn};
use tycho_common::{
    models::{
        blockchain::{Block, Transaction, TxWithChanges},
        protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
        Address, Chain, ChangeType, ComponentId,
    },
    storage::StorageError,
    Bytes,
};
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    models::BlockChanges,
    protocol_message_registry::{
        AuxiliaryProtocolMessageBuildFuture, AuxiliaryProtocolMessageContext,
        AuxiliaryProtocolMessageDecoder, AuxiliaryProtocolStateHydrationFuture,
        AuxiliaryProtocolStateHydrator, ChainHydratedComponentState,
    },
    shared_bootstrap::BootstrapBranchDescriptor,
    u256_num::bytes_to_f64,
    uniswap_v2_bootstrap, uniswap_v3_bootstrap, uniswap_v3_stream, ExtractionError,
};

#[derive(Clone, Debug)]
struct UniswapV3PoolRuntimeState {
    component_id: String,
    token0: Address,
    token1: Address,
    liquidity: BigInt,
    tick: i32,
    sqrt_price_x96: BigInt,
    protocol_fee_token0: BigInt,
    protocol_fee_token1: BigInt,
    tick_liquidity_net: HashMap<i32, BigInt>,
    balances: HashMap<Address, BigInt>,
}

#[derive(Clone, Default)]
struct UniswapV3TxAccumulator {
    protocol_components: HashMap<ComponentId, ProtocolComponent>,
    touched_attributes: HashMap<ComponentId, HashMap<String, Option<Bytes>>>,
    touched_balances: HashMap<ComponentId, HashMap<Address, Option<Bytes>>>,
    created_components: HashSet<ComponentId>,
}

fn build_uniswap_v3_auxiliary_block_changes<'a>(
    context: &'a dyn AuxiliaryProtocolMessageContext,
    value: &'a [u8],
    finalized_block_height: u64,
    partial_block_index: Option<u32>,
) -> AuxiliaryProtocolMessageBuildFuture<'a> {
    Box::pin(async move {
        let raw_events = uniswap_v3_stream::Events::decode(value)?;
        trace!(n_events = raw_events.pool_events.len(), "Received uniswap_v3 Events message");
        build_uniswap_v3_block_changes_from_events(
            context,
            raw_events,
            finalized_block_height,
            partial_block_index,
        )
        .await
    })
}

pub(crate) const AUXILIARY_PROTOCOL_MESSAGE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
    &[AuxiliaryProtocolMessageDecoder {
        protocol_system: "uniswap_v3",
        type_url_suffix: "Events",
        build_block_changes: build_uniswap_v3_auxiliary_block_changes,
    }];

fn hydrate_uniswap_v3_components_from_chain<'a>(
    context: &'a dyn AuxiliaryProtocolMessageContext,
    protocol_components: &'a [ProtocolComponent],
    block_number: u64,
) -> AuxiliaryProtocolStateHydrationFuture<'a> {
    Box::pin(async move {
        let Some(rpc_client) = context.rpc_client() else {
            return Ok(HashMap::new());
        };

        uniswap_v3_bootstrap::hydrate_uniswap_v3_components_from_chain(
            &rpc_client,
            protocol_components,
            block_number,
        )
        .await
    })
}

pub(crate) const AUXILIARY_PROTOCOL_STATE_HYDRATORS: &[AuxiliaryProtocolStateHydrator] =
    &[AuxiliaryProtocolStateHydrator {
        protocol_system: "uniswap_v3",
        hydrate_components_from_chain: hydrate_uniswap_v3_components_from_chain,
    }];

pub(crate) async fn build_uniswap_v3_block_changes_from_events(
    context: &dyn AuxiliaryProtocolMessageContext,
    raw_events: uniswap_v3_stream::Events,
    finalized_block_height: u64,
    partial_block_index: Option<u32>,
) -> Result<BlockChanges, ExtractionError> {
    let block = block_from_uniswap_v3_events(raw_events.block, context.chain())?;
    let mut pool_events = raw_events.pool_events;
    pool_events.sort_unstable_by_key(|event| event.log_ordinal);

    if pool_events.is_empty() {
        let mut changes = BlockChanges::new(
            context.extractor_name().to_string(),
            context.chain(),
            block,
            finalized_block_height,
            false,
            Vec::new(),
            Vec::new(),
        );
        changes.set_partial_block_index(partial_block_index);
        return Ok(changes);
    }

    let created_in_block = pool_events
        .iter()
        .filter_map(|event| {
            matches!(
                event.r#type,
                Some(uniswap_v3_stream::events::pool_event::Type::PoolCreated(_))
            )
            .then(|| normalize_hex_address(&event.pool_address))
        })
        .collect::<Result<HashSet<_>, _>>()?;

    let mut existing_component_ids = HashSet::new();

    for event in &pool_events {
        let component_id = normalize_hex_address(&event.pool_address)?;
        if created_in_block.contains(&component_id) {
            continue;
        }

        existing_component_ids.insert(component_id.clone());
    }

    let existing_component_ids_vec = existing_component_ids
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let existing_components = context
        .get_protocol_components(&existing_component_ids_vec)
        .await?;
    let tracked_existing_component_ids = existing_components
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let protocol_state_values = context
        .get_protocol_states_at_tip(
            &tracked_existing_component_ids
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
        )
        .await?;

    let balance_lookup_keys = existing_components
        .iter()
        .flat_map(|(component_id, component)| {
            component
                .tokens
                .iter()
                .take(2)
                .map(move |token| (component_id, token))
        })
        .collect::<Vec<_>>();
    let component_balances = context
        .get_component_balances_at_tip(&balance_lookup_keys)
        .await?;

    let mut current_states = HashMap::new();
    for component_id in &tracked_existing_component_ids {
        let component = existing_components
            .get(component_id)
            .ok_or_else(|| {
                ExtractionError::Storage(StorageError::NotFound(
                    "ProtocolComponent".to_string(),
                    component_id.clone(),
                ))
            })?;
        current_states.insert(
            component_id.clone(),
            runtime_state_from_snapshot(
                component,
                protocol_state_values.get(component_id),
                component_balances.get(component_id),
            )?,
        );
    }

    let filtered_pool_events = pool_events
        .into_iter()
        .filter(|event| {
            let Ok(component_id) = normalize_hex_address(&event.pool_address) else {
                return true;
            };
            created_in_block.contains(&component_id)
                || tracked_existing_component_ids.contains(&component_id)
        })
        .collect::<Vec<_>>();

    let mut txs_with_update = Vec::new();
    let mut active_tx: Option<(Transaction, UniswapV3TxAccumulator)> = None;

    for event in filtered_pool_events {
        let component_id = normalize_hex_address(&event.pool_address)?;
        let transaction =
            transaction_from_uniswap_v3_event(event.transaction.clone(), &block.hash)?;
        if active_tx
            .as_ref()
            .is_some_and(|(current_tx, _)| current_tx.index != transaction.index)
        {
            let (completed_tx, completed_acc) = active_tx
                .take()
                .expect("active tx exists");
            hydrate_uniswap_v3_pool_states_if_needed(
                context,
                &mut current_states,
                &completed_acc,
                &existing_components,
                block.number,
            )
            .await?;
            if let Some(tx_update) =
                finalize_uniswap_v3_tx(completed_tx, completed_acc, &current_states)
            {
                txs_with_update.push(tx_update);
            }
        }
        let tx_hash = transaction.hash.clone();
        let (_, tx_acc) = active_tx
            .get_or_insert_with(|| (transaction.clone(), UniswapV3TxAccumulator::default()));
        let created_in_tx = tx_acc
            .created_components
            .contains(&component_id);

        if let Some(uniswap_v3_stream::events::pool_event::Type::PoolCreated(created)) =
            &event.r#type
        {
            let token0 = parse_address(&event.token0)?;
            let token1 = parse_address(&event.token1)?;
            current_states.insert(
                component_id.clone(),
                new_uniswap_v3_pool_runtime_state(
                    component_id.clone(),
                    token0.clone(),
                    token1.clone(),
                ),
            );
            tx_acc
                .created_components
                .insert(component_id.clone());
            tx_acc.protocol_components.insert(
                component_id.clone(),
                build_uniswap_v3_protocol_component(
                    &component_id,
                    context.protocol_system(),
                    context.chain(),
                    token0,
                    token1,
                    BigInt::from(created.fee),
                    BigInt::from(created.tick_spacing),
                    &tx_hash,
                    block.ts,
                ),
            );
            continue;
        }

        let state = current_states
            .get_mut(&component_id)
            .ok_or_else(|| {
                ExtractionError::Storage(StorageError::NotFound(
                    "ProtocolComponent".to_string(),
                    component_id.clone(),
                ))
            })?;

        match event.r#type.as_ref() {
            Some(uniswap_v3_stream::events::pool_event::Type::Initialize(init)) => {
                if !created_in_tx {
                    capture_state_attr_before(tx_acc, state, "sqrt_price_x96");
                    capture_state_attr_before(tx_acc, state, "tick");
                }
                state.sqrt_price_x96 = parse_big_int_str(&init.sqrt_price)?;
                state.tick = init.tick;
            }
            Some(uniswap_v3_stream::events::pool_event::Type::Mint(mint)) => {
                let amount = parse_big_int_str(&mint.amount)?;
                let amount_0 = parse_big_int_str(&mint.amount_0)?;
                let amount_1 = parse_big_int_str(&mint.amount_1)?;
                if !created_in_tx {
                    capture_tick_attr_before(tx_acc, state, mint.tick_lower);
                    capture_tick_attr_before(tx_acc, state, mint.tick_upper);
                    capture_balance_before(tx_acc, state, &state.token0);
                    capture_balance_before(tx_acc, state, &state.token1);
                    if state.tick >= mint.tick_lower && state.tick < mint.tick_upper {
                        capture_state_attr_before(tx_acc, state, "liquidity");
                    }
                }

                adjust_tick_liquidity(state, mint.tick_lower, amount.clone());
                adjust_tick_liquidity(state, mint.tick_upper, -amount.clone());
                if state.tick >= mint.tick_lower && state.tick < mint.tick_upper {
                    state.liquidity += amount;
                }
                adjust_balance(state, &state.token0.clone(), amount_0);
                adjust_balance(state, &state.token1.clone(), amount_1);
            }
            Some(uniswap_v3_stream::events::pool_event::Type::Burn(burn)) => {
                let amount = parse_big_int_str(&burn.amount)?;
                if !created_in_tx {
                    capture_tick_attr_before(tx_acc, state, burn.tick_lower);
                    capture_tick_attr_before(tx_acc, state, burn.tick_upper);
                    if state.tick >= burn.tick_lower && state.tick < burn.tick_upper {
                        capture_state_attr_before(tx_acc, state, "liquidity");
                    }
                }

                adjust_tick_liquidity(state, burn.tick_lower, -amount.clone());
                adjust_tick_liquidity(state, burn.tick_upper, amount.clone());
                if state.tick >= burn.tick_lower && state.tick < burn.tick_upper {
                    state.liquidity -= amount;
                }
            }
            Some(uniswap_v3_stream::events::pool_event::Type::Collect(collect)) => {
                let amount_0 = parse_big_int_str(&collect.amount_0)?;
                let amount_1 = parse_big_int_str(&collect.amount_1)?;
                if !created_in_tx {
                    capture_balance_before(tx_acc, state, &state.token0);
                    capture_balance_before(tx_acc, state, &state.token1);
                }
                adjust_balance(state, &state.token0.clone(), -amount_0);
                adjust_balance(state, &state.token1.clone(), -amount_1);
            }
            Some(uniswap_v3_stream::events::pool_event::Type::Swap(swap)) => {
                if !created_in_tx {
                    capture_balance_before(tx_acc, state, &state.token0);
                    capture_balance_before(tx_acc, state, &state.token1);
                    capture_state_attr_before(tx_acc, state, "sqrt_price_x96");
                    capture_state_attr_before(tx_acc, state, "tick");
                    capture_state_attr_before(tx_acc, state, "liquidity");
                }
                adjust_balance(state, &state.token0.clone(), parse_big_int_str(&swap.amount_0)?);
                adjust_balance(state, &state.token1.clone(), parse_big_int_str(&swap.amount_1)?);
                state.sqrt_price_x96 = parse_big_int_str(&swap.sqrt_price)?;
                state.tick = swap.tick;
                state.liquidity = parse_big_int_str(&swap.liquidity)?;
            }
            Some(uniswap_v3_stream::events::pool_event::Type::Flash(flash)) => {
                let paid_0 = parse_big_int_str(&flash.paid_0)?;
                let paid_1 = parse_big_int_str(&flash.paid_1)?;
                if !created_in_tx {
                    capture_balance_before(tx_acc, state, &state.token0);
                    capture_balance_before(tx_acc, state, &state.token1);
                }
                adjust_balance(state, &state.token0.clone(), paid_0);
                adjust_balance(state, &state.token1.clone(), paid_1);
            }
            Some(uniswap_v3_stream::events::pool_event::Type::SetFeeProtocol(set_fp)) => {
                if !created_in_tx {
                    capture_state_attr_before(tx_acc, state, "protocol_fees/token0");
                    capture_state_attr_before(tx_acc, state, "protocol_fees/token1");
                }
                state.protocol_fee_token0 = BigInt::from(set_fp.fee_protocol_0_new);
                state.protocol_fee_token1 = BigInt::from(set_fp.fee_protocol_1_new);
            }
            Some(uniswap_v3_stream::events::pool_event::Type::CollectProtocol(
                collect_protocol,
            )) => {
                let amount_0 = parse_big_int_str(&collect_protocol.amount_0)?;
                let amount_1 = parse_big_int_str(&collect_protocol.amount_1)?;
                if !created_in_tx {
                    capture_balance_before(tx_acc, state, &state.token0);
                    capture_balance_before(tx_acc, state, &state.token1);
                }
                adjust_balance(state, &state.token0.clone(), -amount_0);
                adjust_balance(state, &state.token1.clone(), -amount_1);
            }
            Some(uniswap_v3_stream::events::pool_event::Type::PoolCreated(_)) => {}
            None => {
                return Err(ExtractionError::DecodeError(format!(
                    "uniswap_v3 event for pool `{component_id}` has no event type"
                )));
            }
        }
    }

    if let Some((completed_tx, completed_acc)) = active_tx.take() {
        hydrate_uniswap_v3_pool_states_if_needed(
            context,
            &mut current_states,
            &completed_acc,
            &existing_components,
            block.number,
        )
        .await?;
        if let Some(tx_update) =
            finalize_uniswap_v3_tx(completed_tx, completed_acc, &current_states)
        {
            txs_with_update.push(tx_update);
        }
    }

    let mut changes = BlockChanges::new(
        context.extractor_name().to_string(),
        context.chain(),
        block,
        finalized_block_height,
        false,
        txs_with_update,
        Vec::new(),
    );
    changes.set_partial_block_index(partial_block_index);
    Ok(changes)
}

fn block_from_uniswap_v3_events(
    block: Option<uniswap_v3_stream::Block>,
    chain: Chain,
) -> Result<Block, ExtractionError> {
    let block = block.ok_or_else(|| {
        ExtractionError::DecodeError("uniswap_v3 events payload is missing block".to_string())
    })?;
    Ok(Block::new(
        block.number,
        chain,
        block.hash.into(),
        block.parent_hash.into(),
        DateTime::from_timestamp(block.ts as i64, 0)
            .ok_or_else(|| {
                ExtractionError::DecodeError(format!(
                    "failed to convert timestamp {} to datetime",
                    block.ts
                ))
            })?
            .naive_utc(),
    ))
}

fn transaction_from_uniswap_v3_event(
    tx: Option<uniswap_v3_stream::Transaction>,
    block_hash: &Bytes,
) -> Result<Transaction, ExtractionError> {
    let tx = tx.ok_or_else(|| {
        ExtractionError::DecodeError("uniswap_v3 event is missing transaction".to_string())
    })?;
    let to = if tx.to.is_empty() { None } else { Some(tx.to.into()) };
    Ok(Transaction::new(tx.hash.into(), block_hash.clone(), tx.from.into(), to, tx.index))
}

fn runtime_state_from_snapshot(
    component: &ProtocolComponent,
    state_values: Option<&HashMap<String, Bytes>>,
    balance_values: Option<&HashMap<Bytes, ComponentBalance>>,
) -> Result<UniswapV3PoolRuntimeState, ExtractionError> {
    let token0 = component
        .tokens
        .first()
        .cloned()
        .ok_or_else(|| {
            ExtractionError::DecodeError(format!("component `{}` is missing token0", component.id))
        })?;
    let token1 = component
        .tokens
        .get(1)
        .cloned()
        .ok_or_else(|| {
            ExtractionError::DecodeError(format!("component `{}` is missing token1", component.id))
        })?;

    let values = state_values
        .cloned()
        .unwrap_or_default();
    let balances = balance_values
        .cloned()
        .unwrap_or_default();
    let tick_liquidity_net = values
        .iter()
        .filter_map(|(attr, value)| {
            attr.strip_prefix("ticks/")
                .and_then(|suffix| suffix.strip_suffix("/net-liquidity"))
                .map(|tick| {
                    let tick_index = tick.parse::<i32>().map_err(|err| {
                        ExtractionError::DecodeError(format!(
                            "failed to parse tick index `{tick}` for component `{}`: {err}",
                            component.id
                        ))
                    })?;
                    Ok((tick_index, parse_big_int_bytes(value)))
                })
        })
        .collect::<Result<HashMap<_, _>, ExtractionError>>()?;

    Ok(UniswapV3PoolRuntimeState {
        component_id: component.id.clone(),
        token0: token0.clone(),
        token1: token1.clone(),
        liquidity: values
            .get("liquidity")
            .map(parse_big_int_bytes)
            .unwrap_or_default(),
        tick: values
            .get("tick")
            .map(parse_i32_bytes)
            .transpose()?
            .unwrap_or_default(),
        sqrt_price_x96: values
            .get("sqrt_price_x96")
            .map(parse_big_int_bytes)
            .unwrap_or_default(),
        protocol_fee_token0: values
            .get("protocol_fees/token0")
            .map(parse_big_int_bytes)
            .unwrap_or_default(),
        protocol_fee_token1: values
            .get("protocol_fees/token1")
            .map(parse_big_int_bytes)
            .unwrap_or_default(),
        tick_liquidity_net,
        balances: HashMap::from([
            (
                token0.clone(),
                balances
                    .get(&token0)
                    .map(|balance| parse_big_int_bytes(&balance.balance))
                    .unwrap_or_default(),
            ),
            (
                token1.clone(),
                balances
                    .get(&token1)
                    .map(|balance| parse_big_int_bytes(&balance.balance))
                    .unwrap_or_default(),
            ),
        ]),
    })
}

fn new_uniswap_v3_pool_runtime_state(
    component_id: String,
    token0: Address,
    token1: Address,
) -> UniswapV3PoolRuntimeState {
    UniswapV3PoolRuntimeState {
        component_id,
        token0: token0.clone(),
        token1: token1.clone(),
        liquidity: BigInt::default(),
        tick: 0,
        sqrt_price_x96: BigInt::default(),
        protocol_fee_token0: BigInt::default(),
        protocol_fee_token1: BigInt::default(),
        tick_liquidity_net: HashMap::new(),
        balances: HashMap::from([(token0, BigInt::default()), (token1, BigInt::default())]),
    }
}

fn build_uniswap_v3_protocol_component(
    component_id: &str,
    protocol_system: &str,
    chain: Chain,
    token0: Address,
    token1: Address,
    fee: BigInt,
    tick_spacing: BigInt,
    tx_hash: &Bytes,
    created_at: NaiveDateTime,
) -> ProtocolComponent {
    ProtocolComponent::new(
        component_id,
        protocol_system,
        "uniswap_v3_pool",
        chain,
        vec![token0.clone(), token1.clone()],
        vec![],
        HashMap::from([
            ("fee".to_string(), encode_big_int(&fee)),
            ("tick_spacing".to_string(), encode_big_int(&tick_spacing)),
            (
                "pool_address".to_string(),
                parse_address(component_id).expect("validated component id"),
            ),
        ]),
        ChangeType::Creation,
        tx_hash.clone(),
        created_at,
    )
}

fn created_state_delta_from_runtime(
    state: &UniswapV3PoolRuntimeState,
) -> ProtocolComponentStateDelta {
    let updated_attributes = runtime_dynamic_attributes(state);
    let created_attributes = updated_attributes
        .keys()
        .cloned()
        .collect();
    ProtocolComponentStateDelta {
        component_id: state.component_id.clone(),
        updated_attributes,
        deleted_attributes: HashSet::new(),
        created_attributes,
    }
}

async fn hydrate_uniswap_v3_pool_states_if_needed(
    context: &dyn AuxiliaryProtocolMessageContext,
    current_states: &mut HashMap<String, UniswapV3PoolRuntimeState>,
    acc: &UniswapV3TxAccumulator,
    existing_components: &HashMap<ComponentId, ProtocolComponent>,
    block_number: u64,
) -> Result<(), ExtractionError> {
    let mut touched_component_ids = acc
        .created_components
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    touched_component_ids.extend(acc.touched_attributes.keys().cloned());
    touched_component_ids.extend(acc.touched_balances.keys().cloned());

    let components_to_hydrate = touched_component_ids
        .iter()
        .filter_map(|component_id| {
            let state = current_states.get(component_id)?;
            (state.liquidity != BigInt::default() && state.tick_liquidity_net.is_empty()).then(
                || {
                    acc.protocol_components
                        .get(component_id)
                        .or_else(|| existing_components.get(component_id))
                        .cloned()
                },
            )?
        })
        .collect::<Vec<_>>();

    if components_to_hydrate.is_empty() {
        return Ok(());
    }

    let hydrated = context
        .hydrate_protocol_components_from_chain(&components_to_hydrate, block_number)
        .await?;

    for component in &components_to_hydrate {
        let Some(state) = current_states.get_mut(&component.id) else {
            continue;
        };
        let Some(hydrated_state) = hydrated.get(&component.id) else {
            continue;
        };
        apply_hydrated_chain_state(state, hydrated_state)?;
    }

    Ok(())
}

fn finalize_uniswap_v3_tx(
    tx: Transaction,
    acc: UniswapV3TxAccumulator,
    current_states: &HashMap<String, UniswapV3PoolRuntimeState>,
) -> Option<TxWithChanges> {
    let mut state_updates = HashMap::new();
    let mut balance_changes = HashMap::new();
    let mut touched_components = acc
        .touched_attributes
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    touched_components.extend(acc.touched_balances.keys().cloned());
    touched_components.extend(acc.created_components.iter().cloned());

    for component_id in touched_components {
        let Some(state) = current_states.get(&component_id) else {
            continue;
        };

        if acc
            .created_components
            .contains(&component_id)
        {
            if state.liquidity != BigInt::default() && state.tick_liquidity_net.is_empty() {
                warn!(
                    component_id = %state.component_id,
                    liquidity = %state.liquidity,
                    tick = state.tick,
                    "created uniswap_v3 pool state has non-zero liquidity but no tick net-liquidity map"
                );
            }
            state_updates.insert(component_id.clone(), created_state_delta_from_runtime(state));
            balance_changes
                .insert(component_id.clone(), all_balance_changes_from_runtime(state, &tx.hash));
            continue;
        }

        if let Some(initial_attrs) = acc
            .touched_attributes
            .get(&component_id)
        {
            let mut updated_attributes = HashMap::new();
            let mut deleted_attributes = HashSet::new();
            let mut created_attributes = HashSet::new();

            for (attr, initial_value) in initial_attrs {
                let final_value = runtime_attr_value(state, attr);
                match (initial_value, final_value) {
                    (Some(initial), Some(final_value)) if *initial != final_value => {
                        updated_attributes.insert(attr.to_string(), final_value);
                    }
                    (None, Some(final_value)) => {
                        updated_attributes.insert(attr.to_string(), final_value);
                        created_attributes.insert(attr.to_string());
                    }
                    (Some(_), None) => {
                        deleted_attributes.insert(attr.to_string());
                    }
                    _ => {}
                }
            }

            if !updated_attributes.is_empty() || !deleted_attributes.is_empty() {
                state_updates.insert(
                    component_id.clone(),
                    ProtocolComponentStateDelta {
                        component_id: component_id.clone(),
                        updated_attributes,
                        deleted_attributes,
                        created_attributes,
                    },
                );
            }
        }

        if let Some(initial_balances) = acc.touched_balances.get(&component_id) {
            let mut component_balance_changes = HashMap::new();
            for (token, initial_balance) in initial_balances {
                let final_balance = runtime_balance_value(state, token);
                if initial_balance.as_ref() != Some(&final_balance) {
                    component_balance_changes.insert(
                        token.clone(),
                        ComponentBalance::new(
                            token.clone(),
                            final_balance.clone(),
                            bytes_to_f64(final_balance.as_ref()).unwrap_or(f64::NAN),
                            tx.hash.clone(),
                            &component_id,
                        ),
                    );
                }
            }
            if !component_balance_changes.is_empty() {
                balance_changes.insert(component_id.clone(), component_balance_changes);
            }
        }
    }

    if acc.protocol_components.is_empty() && state_updates.is_empty() && balance_changes.is_empty()
    {
        return None;
    }

    Some(TxWithChanges::new(
        tx,
        acc.protocol_components,
        HashMap::new(),
        state_updates,
        balance_changes,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    ))
}

fn all_balance_changes_from_runtime(
    state: &UniswapV3PoolRuntimeState,
    tx_hash: &Bytes,
) -> HashMap<Address, ComponentBalance> {
    [state.token0.clone(), state.token1.clone()]
        .into_iter()
        .map(|token| {
            let balance = runtime_balance_value(state, &token);
            (
                token.clone(),
                ComponentBalance::new(
                    token,
                    balance.clone(),
                    bytes_to_f64(balance.as_ref()).unwrap_or(f64::NAN),
                    tx_hash.clone(),
                    &state.component_id,
                ),
            )
        })
        .collect()
}

fn runtime_dynamic_attributes(state: &UniswapV3PoolRuntimeState) -> HashMap<String, Bytes> {
    let mut attrs = HashMap::from([
        ("liquidity".to_string(), encode_big_int(&state.liquidity)),
        ("tick".to_string(), encode_big_int(&BigInt::from(state.tick))),
        ("sqrt_price_x96".to_string(), encode_big_int(&state.sqrt_price_x96)),
        ("protocol_fees/token0".to_string(), encode_big_int(&state.protocol_fee_token0)),
        ("protocol_fees/token1".to_string(), encode_big_int(&state.protocol_fee_token1)),
    ]);

    for (tick, liquidity) in &state.tick_liquidity_net {
        if !liquidity.eq(&BigInt::default()) {
            attrs.insert(format!("ticks/{tick}/net-liquidity"), encode_big_int(liquidity));
        }
    }
    attrs
}

fn apply_hydrated_chain_state(
    state: &mut UniswapV3PoolRuntimeState,
    hydrated: &ChainHydratedComponentState,
) -> Result<(), ExtractionError> {
    state.liquidity = hydrated
        .attributes
        .get("liquidity")
        .map(parse_big_int_bytes)
        .unwrap_or_default();
    state.tick = hydrated
        .attributes
        .get("tick")
        .map(parse_i32_bytes)
        .transpose()?
        .unwrap_or_default();
    state.sqrt_price_x96 = hydrated
        .attributes
        .get("sqrt_price_x96")
        .map(parse_big_int_bytes)
        .unwrap_or_default();
    state.protocol_fee_token0 = hydrated
        .attributes
        .get("protocol_fees/token0")
        .map(parse_big_int_bytes)
        .unwrap_or_default();
    state.protocol_fee_token1 = hydrated
        .attributes
        .get("protocol_fees/token1")
        .map(parse_big_int_bytes)
        .unwrap_or_default();
    state.tick_liquidity_net = parse_tick_liquidity_net(&state.component_id, &hydrated.attributes)?;

    if let Some(balance) = hydrated.balances.get(&state.token0) {
        state
            .balances
            .insert(state.token0.clone(), parse_big_int_bytes(balance));
    }
    if let Some(balance) = hydrated.balances.get(&state.token1) {
        state
            .balances
            .insert(state.token1.clone(), parse_big_int_bytes(balance));
    }

    Ok(())
}

fn runtime_attr_value(state: &UniswapV3PoolRuntimeState, attr: &str) -> Option<Bytes> {
    match attr {
        "liquidity" => Some(encode_big_int(&state.liquidity)),
        "tick" => Some(encode_big_int(&BigInt::from(state.tick))),
        "sqrt_price_x96" => Some(encode_big_int(&state.sqrt_price_x96)),
        "protocol_fees/token0" => Some(encode_big_int(&state.protocol_fee_token0)),
        "protocol_fees/token1" => Some(encode_big_int(&state.protocol_fee_token1)),
        _ => attr
            .strip_prefix("ticks/")
            .and_then(|suffix| suffix.strip_suffix("/net-liquidity"))
            .and_then(|tick| tick.parse::<i32>().ok())
            .and_then(|tick| state.tick_liquidity_net.get(&tick))
            .filter(|value| **value != BigInt::default())
            .map(encode_big_int),
    }
}

fn parse_tick_liquidity_net(
    component_id: &str,
    values: &HashMap<String, Bytes>,
) -> Result<HashMap<i32, BigInt>, ExtractionError> {
    values
        .iter()
        .filter_map(|(attr, value)| {
            attr.strip_prefix("ticks/")
                .and_then(|suffix| suffix.strip_suffix("/net-liquidity"))
                .map(|tick| {
                    let tick_index = tick.parse::<i32>().map_err(|err| {
                        ExtractionError::DecodeError(format!(
                            "failed to parse tick index `{tick}` for component `{component_id}`: {err}",
                        ))
                    })?;
                    Ok((tick_index, parse_big_int_bytes(value)))
                })
        })
        .collect::<Result<HashMap<_, _>, ExtractionError>>()
}

fn runtime_balance_value(state: &UniswapV3PoolRuntimeState, token: &Address) -> Bytes {
    state
        .balances
        .get(token)
        .map(encode_big_int)
        .unwrap_or_default()
}

fn capture_state_attr_before(
    acc: &mut UniswapV3TxAccumulator,
    state: &UniswapV3PoolRuntimeState,
    attr: &str,
) {
    acc.touched_attributes
        .entry(state.component_id.clone())
        .or_default()
        .entry(attr.to_string())
        .or_insert_with(|| runtime_attr_value(state, attr));
}

fn capture_tick_attr_before(
    acc: &mut UniswapV3TxAccumulator,
    state: &UniswapV3PoolRuntimeState,
    tick: i32,
) {
    let attr = format!("ticks/{tick}/net-liquidity");
    capture_state_attr_before(acc, state, &attr);
}

fn capture_balance_before(
    acc: &mut UniswapV3TxAccumulator,
    state: &UniswapV3PoolRuntimeState,
    token: &Address,
) {
    acc.touched_balances
        .entry(state.component_id.clone())
        .or_default()
        .entry(token.clone())
        .or_insert_with(|| Some(runtime_balance_value(state, token)));
}

fn adjust_tick_liquidity(state: &mut UniswapV3PoolRuntimeState, tick: i32, delta: BigInt) {
    let entry = state
        .tick_liquidity_net
        .entry(tick)
        .or_default();
    *entry += delta;
    if *entry == BigInt::default() {
        state.tick_liquidity_net.remove(&tick);
    }
}

fn adjust_balance(state: &mut UniswapV3PoolRuntimeState, token: &Address, delta: BigInt) {
    *state
        .balances
        .entry(token.clone())
        .or_default() += delta;
}

fn normalize_hex_address(value: &str) -> Result<String, ExtractionError> {
    let prefixed = if value.starts_with("0x") {
        value.to_lowercase()
    } else {
        format!("0x{}", value.to_lowercase())
    };
    parse_address(&prefixed)?;
    Ok(prefixed)
}

fn parse_address(value: &str) -> Result<Bytes, ExtractionError> {
    let address = Bytes::from_str(value)
        .map_err(|err| ExtractionError::DecodeError(format!("parse address `{value}`: {err}")))?;
    if address.len() != 20 {
        return Err(ExtractionError::DecodeError(format!("address `{value}` is not 20 bytes")));
    }
    Ok(address)
}

fn parse_big_int_str(value: &str) -> Result<BigInt, ExtractionError> {
    BigInt::from_str(value)
        .map_err(|err| ExtractionError::DecodeError(format!("parse big integer `{value}`: {err}")))
}

fn parse_big_int_bytes(value: &Bytes) -> BigInt {
    BigInt::from_signed_bytes_be(value.as_ref())
}

fn parse_i32_bytes(value: &Bytes) -> Result<i32, ExtractionError> {
    let parsed = parse_big_int_bytes(value);
    parsed
        .to_string()
        .parse::<i32>()
        .map_err(|err| {
            ExtractionError::DecodeError(format!("parse i32 from state value `{parsed}`: {err}"))
        })
}

fn encode_big_int(value: &BigInt) -> Bytes {
    value.to_signed_bytes_be().into()
}

pub(crate) fn materialize_uniswap_v2_branch<'a>(
    rpc: &'a EthereumRpcClient,
    branch: &'a BootstrapBranchDescriptor,
) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
    Box::pin(async move {
        uniswap_v2_bootstrap::build_uniswap_v2_bootstrap_block(
            rpc,
            &branch.extractor_name,
            branch.chain,
            &branch.protocol_system,
            branch.params.bootstrap_block,
            &branch.params.pools,
        )
        .await
    })
}

pub(crate) fn materialize_uniswap_v3_branch<'a>(
    rpc: &'a EthereumRpcClient,
    branch: &'a BootstrapBranchDescriptor,
) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
    Box::pin(async move {
        uniswap_v3_bootstrap::build_uniswap_v3_bootstrap_block(
            rpc,
            &branch.extractor_name,
            branch.chain,
            &branch.protocol_system,
            branch.params.bootstrap_block,
            &branch.params.pools,
        )
        .await
    })
}
