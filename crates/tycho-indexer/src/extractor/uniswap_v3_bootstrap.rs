use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use alloy::{
    primitives::{Address as AlloyAddress, U256},
    rpc::types::{BlockId, BlockNumberOrTag, TransactionInput, TransactionRequest},
    sol,
    sol_types::SolCall,
};
use chrono::DateTime;
use num_bigint::{BigInt, Sign};
use tracing::info;
use tycho_common::{
    models::{
        blockchain::{Block, Transaction, TxWithChanges},
        protocol::{ComponentBalance, ProtocolComponent, ProtocolComponentStateDelta},
        Address, Chain, ChangeType,
    },
    Bytes,
};
use tycho_ethereum::{rpc::EthereumRpcClient, BytesCodec};

use crate::extractor::{models::BlockChanges, u256_num::bytes_to_f64, ExtractionError};

sol! {
    struct Multicall3Call {
        address target;
        bool allowFailure;
        bytes callData;
    }

    struct Multicall3Result {
        bool success;
        bytes returnData;
    }

    function aggregate3(Multicall3Call[] calldata calls)
        external
        payable
        returns (Multicall3Result[] memory returnData);

    function token0() external view returns (address);
    function token1() external view returns (address);
    function fee() external view returns (uint24);
    function tickSpacing() external view returns (int24);
    function liquidity() external view returns (uint128);
    function slot0() external view returns (
        uint160 sqrtPriceX96,
        int24 tick,
        uint16 observationIndex,
        uint16 observationCardinality,
        uint16 observationCardinalityNext,
        uint8 feeProtocol,
        bool unlocked
    );
    function balanceOf(address _owner) external view returns (uint256 balance);
    function wordBounds(address pool) external view returns (int24 tickSpacing, int16 minWord, int16 maxWord);
    function scanWordsPage(
        address pool,
        int16 startWord,
        uint16 maxWordsToScan,
        uint16 maxNonEmptyWords
    ) external view returns (int24 tickSpacing, int16[] words, int16 nextWord, bool done);
    function getTicksForWords(address pool, int16[] words) external view returns (bytes packedTicks, uint32[] counts);
}

const MULTICALL3_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";
const UNISWAP_V3_TICK_SNAPSHOT_LENS_ADDRESS: &str = "0x2743d5CCa9B6B3C0a4E01de9e5ba8837FC60F839";
const STATIC_RPC_BATCH_SIZE: usize = 1000;
const BALANCE_RPC_BATCH_SIZE: usize = 1000;
const POOL_STATIC_CALLS_PER_POOL: usize = 6;
const POOL_BALANCE_CALLS_PER_POOL: usize = 2;
const LENS_WORDS_PER_PAGE: u16 = 1_024;
const LENS_NON_EMPTY_WORDS_PER_PAGE: u16 = 256;
const PACKED_TICK_SIZE: usize = 19;
const TICK_PAGE_PROGRESS_INTERVAL: usize = 10;
const TICK_POOL_BATCH_SIZE: usize = 300;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapParams {
    pub bootstrap_block: u64,
    pub pools: Vec<Bytes>,
}

#[derive(Clone, Debug)]
struct PoolSnapshotSeed {
    pool: AlloyAddress,
    component_id: String,
    token0: Address,
    token1: Address,
    fee: BigInt,
    tick_spacing: BigInt,
    liquidity: BigInt,
    sqrt_price_x96: BigInt,
    tick: BigInt,
    protocol_fee_token0: u8,
    protocol_fee_token1: u8,
}

#[derive(Default)]
struct TickSnapshotState {
    start_word: i16,
    done: bool,
    attributes: HashMap<String, Bytes>,
    pages_scanned: usize,
    non_empty_pages: usize,
    logical_calls: usize,
}

pub fn parse_bootstrap_params(params: &str) -> Result<BootstrapParams, ExtractionError> {
    let mut bootstrap_block = None;
    let mut pools = Vec::new();

    for pair in params
        .split('&')
        .filter(|part| !part.is_empty())
    {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(ExtractionError::Setup(format!("invalid bootstrap param `{pair}`")));
        };

        match key {
            "bootstrap_block" => {
                bootstrap_block = Some(value.parse::<u64>().map_err(|err| {
                    ExtractionError::Setup(format!("parse bootstrap_block: {err}"))
                })?);
            }
            "pool" => pools.push(parse_address(value)?),
            "pools" => {
                for pool in value
                    .split(',')
                    .filter(|pool| !pool.is_empty())
                {
                    pools.push(parse_address(pool)?);
                }
            }
            _ => return Err(ExtractionError::Setup(format!("unknown bootstrap param `{key}`"))),
        }
    }

    let bootstrap_block = bootstrap_block
        .ok_or_else(|| ExtractionError::Setup("missing `bootstrap_block`".to_string()))?;
    if pools.is_empty() {
        return Err(ExtractionError::Setup("missing `pool` or `pools`".to_string()));
    }

    Ok(BootstrapParams { bootstrap_block, pools })
}

pub async fn build_uniswap_v3_bootstrap_block(
    rpc: &EthereumRpcClient,
    extractor_name: &str,
    chain: Chain,
    protocol_system: &str,
    bootstrap_block: u64,
    pools: &[Bytes],
) -> Result<BlockChanges, ExtractionError> {
    let block = fetch_block(rpc, chain, bootstrap_block).await?;
    let block_tag = BlockNumberOrTag::Number(bootstrap_block);
    let pool_addresses = pools
        .iter()
        .map(to_alloy_address)
        .collect::<Result<Vec<_>, _>>()?;

    let seeds = fetch_pool_snapshot_seeds(rpc, block_tag, &pool_addresses).await?;
    let balances = fetch_pool_balances_batched(rpc, block_tag, &seeds).await?;

    let tick_attributes = fetch_tick_attributes_batched(rpc, block_tag, &seeds).await?;

    let tx = synthetic_bootstrap_transaction(&block);
    let mut protocol_components = HashMap::with_capacity(seeds.len());
    let mut state_updates = HashMap::with_capacity(seeds.len());
    let mut balance_changes = HashMap::with_capacity(seeds.len());

    for ((seed, (balance0, balance1)), tick_attributes) in seeds
        .into_iter()
        .zip(balances)
        .zip(tick_attributes)
    {
        let component_id = seed.component_id.clone();

        protocol_components.insert(
            component_id.clone(),
            build_protocol_component(&seed, protocol_system, chain, &tx, block.ts),
        );
        state_updates.insert(
            component_id.clone(),
            build_state_update(&component_id, &seed, tick_attributes),
        );
        balance_changes.insert(
            component_id.clone(),
            HashMap::from([
                (
                    seed.token0.clone(),
                    ComponentBalance::new(
                        seed.token0.clone(),
                        balance0.clone(),
                        bytes_to_f64(balance0.as_ref()).unwrap_or(f64::NAN),
                        tx.hash.clone(),
                        &component_id,
                    ),
                ),
                (
                    seed.token1.clone(),
                    ComponentBalance::new(
                        seed.token1.clone(),
                        balance1.clone(),
                        bytes_to_f64(balance1.as_ref()).unwrap_or(f64::NAN),
                        tx.hash.clone(),
                        &component_id,
                    ),
                ),
            ]),
        );
    }

    Ok(BlockChanges::new(
        extractor_name.to_owned(),
        chain,
        block,
        bootstrap_block,
        false,
        vec![TxWithChanges::new(
            tx,
            protocol_components,
            HashMap::new(),
            state_updates,
            balance_changes,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )],
        vec![],
    ))
}

async fn fetch_block(
    rpc: &EthereumRpcClient,
    chain: Chain,
    block_number: u64,
) -> Result<Block, ExtractionError> {
    let block = rpc
        .get_block_by_number(BlockId::Number(BlockNumberOrTag::Number(block_number)))
        .await
        .map_err(|err| {
            ExtractionError::Setup(format!("failed to fetch block {block_number}: {err}"))
        })?;

    Ok(Block {
        number: block.header.number,
        chain,
        hash: block.header.hash.to_bytes(),
        parent_hash: block.header.parent_hash.to_bytes(),
        ts: DateTime::from_timestamp(block.header.timestamp as i64, 0)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "failed to convert timestamp {} for block {block_number}",
                    block.header.timestamp
                ))
            })?
            .naive_utc(),
    })
}

async fn fetch_pool_snapshot_seeds(
    rpc: &EthereumRpcClient,
    block_tag: BlockNumberOrTag,
    pools: &[AlloyAddress],
) -> Result<Vec<PoolSnapshotSeed>, ExtractionError> {
    let pools_per_chunk =
        max_items_per_rpc_batch(STATIC_RPC_BATCH_SIZE, POOL_STATIC_CALLS_PER_POOL);
    let total_chunks = pools.len().div_ceil(pools_per_chunk);
    let mut seeds = Vec::with_capacity(pools.len());

    for (chunk_index, pools_chunk) in pools
        .chunks(pools_per_chunk)
        .enumerate()
    {
        info!(
            chunk = chunk_index + 1,
            total_chunks,
            pools = pools_chunk.len(),
            logical_calls = pools_chunk.len() * POOL_STATIC_CALLS_PER_POOL,
            rpc_requests = 1,
            transport = "multicall3",
            "BootstrapStaticChunk"
        );

        let mut calls = Vec::with_capacity(pools_chunk.len() * POOL_STATIC_CALLS_PER_POOL);
        for pool in pools_chunk {
            calls.push(multicall_read(*pool, token0Call {}.abi_encode()));
            calls.push(multicall_read(*pool, token1Call {}.abi_encode()));
            calls.push(multicall_read(*pool, feeCall {}.abi_encode()));
            calls.push(multicall_read(*pool, tickSpacingCall {}.abi_encode()));
            calls.push(multicall_read(*pool, liquidityCall {}.abi_encode()));
            calls.push(multicall_read(*pool, slot0Call {}.abi_encode()));
        }

        let responses = multicall_many(rpc, block_tag, calls).await?;

        for (pool_index, pool) in pools_chunk.iter().enumerate() {
            let offset = pool_index * POOL_STATIC_CALLS_PER_POOL;
            let token0 =
                decode_address_response::<token0Call>(&responses[offset], pool, "token0()")?;
            let token1 =
                decode_address_response::<token1Call>(&responses[offset + 1], pool, "token1()")?;
            let fee = feeCall::abi_decode_returns_validate(&responses[offset + 2])
                .map_err(|err| decode_error(pool, "fee()", err))?;
            let tick_spacing = tickSpacingCall::abi_decode_returns_validate(&responses[offset + 3])
                .map_err(|err| decode_error(pool, "tickSpacing()", err))?;
            let liquidity = liquidityCall::abi_decode_returns_validate(&responses[offset + 4])
                .map_err(|err| decode_error(pool, "liquidity()", err))?;
            let slot0 = slot0Call::abi_decode_returns_validate(&responses[offset + 5])
                .map_err(|err| decode_error(pool, "slot0()", err))?;
            let (protocol_fee_token0, protocol_fee_token1) = decode_fee_protocol(slot0.feeProtocol);

            seeds.push(PoolSnapshotSeed {
                pool: *pool,
                component_id: format!("{pool:#x}"),
                token0: token0.to_bytes(),
                token1: token1.to_bytes(),
                fee: BigInt::from(fee.to::<u32>()),
                tick_spacing: BigInt::from(tick_spacing.as_i32()),
                liquidity: uint_to_bigint(U256::from(liquidity)),
                sqrt_price_x96: uint_bytes_to_bigint(&slot0.sqrtPriceX96.to_be_bytes::<20>()),
                tick: BigInt::from(slot0.tick.as_i32()),
                protocol_fee_token0,
                protocol_fee_token1,
            });
        }
    }

    Ok(seeds)
}

async fn fetch_pool_balances_batched(
    rpc: &EthereumRpcClient,
    block_tag: BlockNumberOrTag,
    seeds: &[PoolSnapshotSeed],
) -> Result<Vec<(Bytes, Bytes)>, ExtractionError> {
    let pools_per_chunk =
        max_items_per_rpc_batch(BALANCE_RPC_BATCH_SIZE, POOL_BALANCE_CALLS_PER_POOL);
    let total_chunks = seeds.len().div_ceil(pools_per_chunk);
    let mut balances = Vec::with_capacity(seeds.len());

    for (chunk_index, seeds_chunk) in seeds
        .chunks(pools_per_chunk)
        .enumerate()
    {
        info!(
            chunk = chunk_index + 1,
            total_chunks,
            pools = seeds_chunk.len(),
            logical_calls = seeds_chunk.len() * POOL_BALANCE_CALLS_PER_POOL,
            rpc_requests = 1,
            transport = "multicall3",
            "BootstrapBalanceChunk"
        );

        let mut calls = Vec::with_capacity(seeds_chunk.len() * POOL_BALANCE_CALLS_PER_POOL);
        for seed in seeds_chunk {
            calls.push(multicall_read(
                AlloyAddress::from_bytes(&seed.token0),
                balanceOfCall { _owner: seed.pool }.abi_encode(),
            ));
            calls.push(multicall_read(
                AlloyAddress::from_bytes(&seed.token1),
                balanceOfCall { _owner: seed.pool }.abi_encode(),
            ));
        }

        let responses = multicall_many(rpc, block_tag, calls).await?;

        for (pool_index, seed) in seeds_chunk.iter().enumerate() {
            let offset = pool_index * POOL_BALANCE_CALLS_PER_POOL;
            let balance0 = balanceOfCall::abi_decode_returns_validate(&responses[offset])
                .map_err(|err| decode_error(&seed.pool, "balanceOf(token0)", err))?;
            let balance1 = balanceOfCall::abi_decode_returns_validate(&responses[offset + 1])
                .map_err(|err| decode_error(&seed.pool, "balanceOf(token1)", err))?;
            balances.push((uint_to_bytes(balance0), uint_to_bytes(balance1)));
        }
    }

    Ok(balances)
}

async fn fetch_tick_attributes_batched(
    rpc: &EthereumRpcClient,
    block_tag: BlockNumberOrTag,
    seeds: &[PoolSnapshotSeed],
) -> Result<Vec<HashMap<String, Bytes>>, ExtractionError> {
    let total_chunks = seeds
        .len()
        .div_ceil(TICK_POOL_BATCH_SIZE);
    let mut all_attributes = Vec::with_capacity(seeds.len());

    for (chunk_index, seeds_chunk) in seeds
        .chunks(TICK_POOL_BATCH_SIZE)
        .enumerate()
    {
        info!(
            chunk = chunk_index + 1,
            total_chunks,
            pools = seeds_chunk.len(),
            transport = "multicall3",
            "BootstrapTickChunkStart"
        );

        let states = snapshot_tick_chunk_via_lens(
            rpc,
            block_tag,
            seeds_chunk,
            chunk_index + 1,
            total_chunks,
        )
        .await?;
        let total_pages_scanned: usize = states
            .iter()
            .map(|state| state.pages_scanned)
            .sum();
        let total_non_empty_pages: usize = states
            .iter()
            .map(|state| state.non_empty_pages)
            .sum();
        let total_logical_calls: usize = states
            .iter()
            .map(|state| state.logical_calls)
            .sum();
        let total_tick_attributes: usize = states
            .iter()
            .map(|state| state.attributes.len())
            .sum();

        info!(
            chunk = chunk_index + 1,
            total_chunks,
            pools = seeds_chunk.len(),
            total_pages_scanned,
            total_non_empty_pages,
            logical_calls = total_logical_calls,
            rpc_requests = "multiple",
            tick_attributes = total_tick_attributes,
            transport = "multicall3",
            "BootstrapTickChunkDone"
        );

        all_attributes.extend(
            states
                .into_iter()
                .map(|state| state.attributes),
        );
    }

    Ok(all_attributes)
}

async fn snapshot_tick_chunk_via_lens(
    rpc: &EthereumRpcClient,
    block_tag: BlockNumberOrTag,
    seeds: &[PoolSnapshotSeed],
    chunk: usize,
    total_chunks: usize,
) -> Result<Vec<TickSnapshotState>, ExtractionError> {
    let lens = lens_address()?;
    let mut states = Vec::with_capacity(seeds.len());
    let word_bound_calls = seeds
        .iter()
        .map(|seed| multicall_read(lens, wordBoundsCall { pool: seed.pool }.abi_encode()))
        .collect();
    let word_bound_results = multicall_many(rpc, block_tag, word_bound_calls).await?;

    for (seed, response) in seeds
        .iter()
        .zip(word_bound_results.into_iter())
    {
        let word_bounds = wordBoundsCall::abi_decode_returns_validate(&response)
            .map_err(|err| decode_error(&seed.pool, "wordBounds()", err))?;
        states.push(TickSnapshotState {
            start_word: word_bounds.minWord,
            done: false,
            attributes: HashMap::new(),
            pages_scanned: 0,
            non_empty_pages: 0,
            logical_calls: 1,
        });
    }

    let mut round = 0usize;
    loop {
        let active_indices = states
            .iter()
            .enumerate()
            .filter_map(|(index, state)| (!state.done).then_some(index))
            .collect::<Vec<_>>();
        if active_indices.is_empty() {
            break;
        }

        round += 1;

        let scan_calls = active_indices
            .iter()
            .map(|&index| {
                let state = &states[index];
                let seed = &seeds[index];
                multicall_read(
                    lens,
                    scanWordsPageCall {
                        pool: seed.pool,
                        startWord: state.start_word,
                        maxWordsToScan: LENS_WORDS_PER_PAGE,
                        maxNonEmptyWords: LENS_NON_EMPTY_WORDS_PER_PAGE,
                    }
                    .abi_encode(),
                )
            })
            .collect();
        let scan_results = multicall_many(rpc, block_tag, scan_calls).await?;

        let mut decoded_pages = Vec::with_capacity(active_indices.len());
        for (&index, response) in active_indices
            .iter()
            .zip(scan_results.into_iter())
        {
            let page = scanWordsPageCall::abi_decode_returns_validate(&response)
                .map_err(|err| decode_error(&seeds[index].pool, "scanWordsPage()", err))?;
            states[index].pages_scanned += 1;
            states[index].logical_calls += 1;
            decoded_pages.push((index, page));
        }

        let tick_call_indices = decoded_pages
            .iter()
            .filter_map(|(index, page)| (!page.words.is_empty()).then_some(*index))
            .collect::<Vec<_>>();

        if !tick_call_indices.is_empty() {
            let tick_calls = decoded_pages
                .iter()
                .filter(|(_, page)| !page.words.is_empty())
                .map(|(index, page)| {
                    multicall_read(
                        lens,
                        getTicksForWordsCall {
                            pool: seeds[*index].pool,
                            words: page.words.clone(),
                        }
                        .abi_encode(),
                    )
                })
                .collect();
            let tick_results = multicall_many(rpc, block_tag, tick_calls).await?;

            for (&index, response) in tick_call_indices
                .iter()
                .zip(tick_results.into_iter())
            {
                let tick_page = getTicksForWordsCall::abi_decode_returns_validate(&response)
                    .map_err(|err| decode_error(&seeds[index].pool, "getTicksForWords()", err))?;
                states[index].non_empty_pages += 1;
                states[index].logical_calls += 1;
                states[index]
                    .attributes
                    .extend(decode_packed_tick_attributes(
                        &tick_page.packedTicks,
                        &tick_page.counts,
                    )?);
            }
        }

        for (index, page) in decoded_pages {
            if page.done {
                states[index].done = true;
            } else {
                states[index].start_word = page.nextWord;
            }
        }

        if round == 1 || round % TICK_PAGE_PROGRESS_INTERVAL == 0 {
            let completed_pools = states
                .iter()
                .filter(|state| state.done)
                .count();
            let remaining_active_pools = seeds.len() - completed_pools;
            let cumulative_ticks = states
                .iter()
                .map(|state| state.attributes.len())
                .sum::<usize>();

            info!(
                chunk,
                total_chunks,
                round,
                pools = seeds.len(),
                completed_pools,
                remaining_active_pools,
                cumulative_ticks,
                transport = "multicall3",
                "BootstrapTickChunkProgress"
            );
        }
    }

    Ok(states)
}

fn build_protocol_component(
    seed: &PoolSnapshotSeed,
    protocol_system: &str,
    chain: Chain,
    tx: &Transaction,
    created_at: chrono::NaiveDateTime,
) -> ProtocolComponent {
    ProtocolComponent::new(
        &seed.component_id,
        protocol_system,
        "uniswap_v3_pool",
        chain,
        vec![seed.token0.clone(), seed.token1.clone()],
        vec![],
        HashMap::from([
            ("fee".to_string(), seed.fee.to_signed_bytes_be().into()),
            (
                "tick_spacing".to_string(),
                seed.tick_spacing
                    .to_signed_bytes_be()
                    .into(),
            ),
            ("pool_address".to_string(), seed.pool.to_bytes().into()),
        ]),
        ChangeType::Creation,
        tx.hash.clone(),
        created_at,
    )
}

fn build_state_update(
    component_id: &str,
    seed: &PoolSnapshotSeed,
    tick_attributes: HashMap<String, Bytes>,
) -> ProtocolComponentStateDelta {
    let mut updated_attributes = HashMap::from([
        (
            "liquidity".to_string(),
            seed.liquidity
                .to_signed_bytes_be()
                .into(),
        ),
        ("tick".to_string(), seed.tick.to_signed_bytes_be().into()),
        (
            "sqrt_price_x96".to_string(),
            seed.sqrt_price_x96
                .to_signed_bytes_be()
                .into(),
        ),
        (
            "protocol_fees/token0".to_string(),
            BigInt::from(seed.protocol_fee_token0)
                .to_signed_bytes_be()
                .into(),
        ),
        (
            "protocol_fees/token1".to_string(),
            BigInt::from(seed.protocol_fee_token1)
                .to_signed_bytes_be()
                .into(),
        ),
    ]);
    updated_attributes.extend(tick_attributes);
    let created_attributes = updated_attributes
        .keys()
        .cloned()
        .collect::<HashSet<_>>();

    ProtocolComponentStateDelta {
        component_id: component_id.to_string(),
        updated_attributes,
        deleted_attributes: HashSet::new(),
        created_attributes,
    }
}

fn synthetic_bootstrap_transaction(block: &Block) -> Transaction {
    Transaction::new(block.hash.clone(), block.hash.clone(), Bytes::zero(20), None, 0)
}

fn read_call_request(to: AlloyAddress, calldata: Vec<u8>) -> TransactionRequest {
    TransactionRequest::default()
        .to(to)
        .input(TransactionInput::both(calldata.into()))
}

fn parse_address(value: &str) -> Result<Bytes, ExtractionError> {
    let address = Bytes::from_str(value)
        .map_err(|err| ExtractionError::Setup(format!("parse address `{value}`: {err}")))?;
    if address.len() != 20 {
        return Err(ExtractionError::Setup(format!("address `{value}` is not 20 bytes")));
    }
    Ok(address)
}

fn to_alloy_address(address: &Bytes) -> Result<AlloyAddress, ExtractionError> {
    if address.len() != 20 {
        return Err(ExtractionError::Setup(format!(
            "pool address {} is not 20 bytes",
            hex::encode(address)
        )));
    }
    Ok(AlloyAddress::from_bytes(address))
}

fn lens_address() -> Result<AlloyAddress, ExtractionError> {
    AlloyAddress::from_str(UNISWAP_V3_TICK_SNAPSHOT_LENS_ADDRESS)
        .map_err(|err| ExtractionError::Setup(format!("invalid lens address: {err}")))
}

fn multicall_address() -> Result<AlloyAddress, ExtractionError> {
    AlloyAddress::from_str(MULTICALL3_ADDRESS)
        .map_err(|err| ExtractionError::Setup(format!("invalid multicall address: {err}")))
}

fn multicall_read(target: AlloyAddress, call_data: Vec<u8>) -> Multicall3Call {
    Multicall3Call { target, allowFailure: true, callData: call_data.into() }
}

async fn multicall_many(
    rpc: &EthereumRpcClient,
    block_tag: BlockNumberOrTag,
    calls: Vec<Multicall3Call>,
) -> Result<Vec<Bytes>, ExtractionError> {
    let multicall = multicall_address()?;
    let response = rpc
        .eth_call(read_call_request(multicall, aggregate3Call { calls }.abi_encode()), block_tag)
        .await
        .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?;
    let results = aggregate3Call::abi_decode_returns_validate(&response)
        .map_err(|err| ExtractionError::Setup(format!("decode multicall aggregate3(): {err}")))?;

    let mut return_data = Vec::with_capacity(results.len());
    for (index, result) in results.into_iter().enumerate() {
        if !result.success {
            return Err(ExtractionError::Setup(format!(
                "multicall aggregate3() inner call {index} failed"
            )));
        }
        return_data.push(result.returnData.to_vec().into());
    }

    Ok(return_data)
}

fn decode_address_response<C>(
    response: &Bytes,
    pool: &AlloyAddress,
    call_name: &str,
) -> Result<AlloyAddress, ExtractionError>
where
    C: SolCall<Return = AlloyAddress>,
{
    C::abi_decode_returns_validate(response).map_err(|err| decode_error(pool, call_name, err))
}

fn decode_packed_tick_attributes(
    packed_ticks: &[u8],
    counts: &[u32],
) -> Result<HashMap<String, Bytes>, ExtractionError> {
    let total_ticks = counts
        .iter()
        .try_fold(0usize, |acc, count| {
            acc.checked_add(*count as usize)
                .ok_or_else(|| ExtractionError::Setup("tick count overflow".to_string()))
        })?;
    let expected_len = total_ticks
        .checked_mul(PACKED_TICK_SIZE)
        .ok_or_else(|| ExtractionError::Setup("packed tick payload length overflow".to_string()))?;

    if packed_ticks.len() != expected_len {
        return Err(ExtractionError::Setup(format!(
            "packed tick payload size mismatch: expected {expected_len} bytes for {total_ticks} ticks, got {} bytes",
            packed_ticks.len()
        )));
    }

    let mut attributes = HashMap::with_capacity(total_ticks);
    for chunk in packed_ticks.chunks_exact(PACKED_TICK_SIZE) {
        let tick = decode_signed_bigint(&chunk[..3], 3);
        let liquidity_net = decode_signed_bigint(&chunk[3..], 16);
        if liquidity_net == BigInt::from(0) {
            continue;
        }
        attributes.insert(
            format!("ticks/{tick}/net-liquidity"),
            liquidity_net
                .to_signed_bytes_be()
                .into(),
        );
    }

    Ok(attributes)
}

fn decode_signed_bigint(bytes: &[u8], width: usize) -> BigInt {
    debug_assert_eq!(bytes.len(), width);
    let sign_byte = if bytes
        .first()
        .is_some_and(|byte| byte & 0x80 != 0)
    {
        0xff
    } else {
        0x00
    };
    let mut padded = vec![sign_byte; 32 - width];
    padded.extend_from_slice(bytes);
    BigInt::from_signed_bytes_be(&padded)
}

fn decode_fee_protocol(fee_protocol: u8) -> (u8, u8) {
    (fee_protocol & 0x0f, fee_protocol >> 4)
}

fn decode_error(
    pool: &AlloyAddress,
    call_name: &str,
    err: impl std::fmt::Display,
) -> ExtractionError {
    ExtractionError::Setup(format!("{call_name} failed for {pool:#x}: {err}"))
}

fn uint_to_bigint(value: U256) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, &value.to_be_bytes::<32>())
}

fn uint_to_bytes(value: U256) -> Bytes {
    uint_to_bigint(value)
        .to_signed_bytes_be()
        .into()
}

fn uint_bytes_to_bigint(bytes: &[u8]) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, bytes)
}

fn max_items_per_rpc_batch(batch_size: usize, calls_per_item: usize) -> usize {
    std::cmp::max(1, batch_size / calls_per_item)
}

#[cfg(test)]
mod tests {
    use super::parse_bootstrap_params;

    #[test]
    fn parses_repeated_pool_params() {
        let config = parse_bootstrap_params(
            "bootstrap_block=123&pool=0x0000000000000000000000000000000000000001&pool=0x0000000000000000000000000000000000000002",
        )
        .expect("valid config");

        assert_eq!(config.bootstrap_block, 123);
        assert_eq!(config.pools.len(), 2);
    }

    #[test]
    fn parses_comma_separated_pools() {
        let config = parse_bootstrap_params(
            "bootstrap_block=123&pools=0x0000000000000000000000000000000000000001,0x0000000000000000000000000000000000000002",
        )
        .expect("valid config");

        assert_eq!(config.bootstrap_block, 123);
        assert_eq!(config.pools.len(), 2);
    }
}
