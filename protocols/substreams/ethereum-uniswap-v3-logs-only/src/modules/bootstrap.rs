use anyhow::{anyhow, Context, Result};
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::{
    rpc::{RPCDecodable, RpcBatch},
    Function,
};
use substreams_helper::hex::Hexable;
use tycho_substreams::abi::erc20::functions::BalanceOf;
use tycho_substreams::prelude::*;

use crate::abi::{
    pool::functions::{Fee, Liquidity, Slot0, TickSpacing, Token0, Token1},
    tick_snapshot_lens::functions::{GetTicksForWords, ScanWordsPage, WordBounds},
};

const UNISWAP_V3_TICK_SNAPSHOT_LENS_ADDRESS: [u8; 20] = [
    0x27, 0x43, 0xd5, 0xcc, 0xa9, 0xb6, 0xb3, 0xc0, 0xa4, 0xe0, 0x1d, 0xe9, 0xe5, 0xba,
    0x88, 0x37, 0xfc, 0x60, 0xf8, 0x39,
];
// StreamingFast's rpc::eth_call extension times out on large multicall-style batches for
// bootstrap. Keep the seed fetch small so we fail less often on provider-side deadlines.
const STATIC_RPC_BATCH_SIZE: usize = 24;
// Balance batches hit a much wider set of token contracts and are timing out even when the
// static pool metadata fetch succeeds. Keep these chunks aligned with the static phase.
const BALANCE_RPC_BATCH_SIZE: usize = 8;
const POOL_STATIC_CALLS_PER_POOL: usize = 6;
const POOL_BALANCE_CALLS_PER_POOL: usize = 2;
const LENS_WORDS_PER_PAGE: i64 = 1_024;
const LENS_NON_EMPTY_WORDS_PER_PAGE: i64 = 256;
const PACKED_TICK_SIZE: usize = 19;
const LOG_EVERY_N_BATCHES: usize = 25;

type Slot0Value = (BigInt, BigInt, BigInt, BigInt, BigInt, BigInt, bool);
type WordBoundsValue = (BigInt, BigInt, BigInt);
type ScanWordsPageValue = (BigInt, Vec<BigInt>, BigInt, bool);
type GetTicksForWordsValue = (Vec<u8>, Vec<BigInt>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub bootstrap_block: u64,
    pub pools: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct PoolSnapshotSeed {
    pool: Vec<u8>,
    component_id: String,
    token0: Vec<u8>,
    token1: Vec<u8>,
    fee: BigInt,
    tick_spacing: BigInt,
    liquidity: BigInt,
    sqrt_price_x96: BigInt,
    tick: BigInt,
    protocol_fee_token0: u8,
    protocol_fee_token1: u8,
}

#[derive(Clone, Debug)]
struct PoolBootstrapData {
    seed: PoolSnapshotSeed,
    tick_attributes: Vec<Attribute>,
    balance0: BigInt,
    balance1: BigInt,
}

pub fn parse_config(params: &str) -> Result<Config> {
    let mut bootstrap_block = None;
    let mut pools = Vec::new();

    for pair in params
        .split('&')
        .filter(|part| !part.is_empty())
    {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(anyhow!("invalid bootstrap param `{pair}`"));
        };

        match key {
            "bootstrap_block" => {
                bootstrap_block = Some(
                    value
                        .parse()
                        .context("parse bootstrap_block")?,
                )
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
            _ => return Err(anyhow!("unknown bootstrap param `{key}`")),
        }
    }

    let bootstrap_block = bootstrap_block.ok_or_else(|| anyhow!("missing `bootstrap_block`"))?;
    if pools.is_empty() {
        return Err(anyhow!("missing `pool` or `pools`"));
    }

    Ok(Config { bootstrap_block, pools })
}

pub fn build_bootstrap_changes(params: &str, block: &eth::Block) -> Result<BlockEntityChanges> {
    let config = parse_config(params)?;
    if block.number != config.bootstrap_block {
        return Ok(BlockEntityChanges::default());
    }

    substreams::log::info!(
        "bootstrap triggered at block {} for {} pools",
        block.number,
        config.pools.len()
    );

    let tx = bootstrap_transaction(block)?;
    let pool_seeds = fetch_pool_snapshot_seeds(&config.pools)?;
    let balances = fetch_pool_balances_batched(&pool_seeds)?;
    let pool_snapshots = pool_seeds
        .into_iter()
        .zip(balances)
        .map(|(seed, (balance0, balance1))| {
            let tick_attributes = snapshot_tick_attributes_via_lens(&seed)?;
            Ok(PoolBootstrapData { seed, tick_attributes, balance0, balance1 })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut changes = Vec::with_capacity(pool_snapshots.len());
    for snapshot in pool_snapshots {
        changes.push(build_pool_change(
            snapshot.seed,
            snapshot.tick_attributes,
            snapshot.balance0,
            snapshot.balance1,
            &tx,
        ));
    }

    Ok(BlockEntityChanges { block: None, changes })
}

fn fetch_pool_snapshot_seeds(pools: &[Vec<u8>]) -> Result<Vec<PoolSnapshotSeed>> {
    let pools_per_chunk = max_items_per_rpc_batch(STATIC_RPC_BATCH_SIZE, POOL_STATIC_CALLS_PER_POOL);
    let mut seeds = Vec::with_capacity(pools.len());
    let total_chunks = pools.len().div_ceil(pools_per_chunk);

    for (chunk_index, pools_chunk) in pools.chunks(pools_per_chunk).enumerate() {
        substreams::log::info!(
            "bootstrap static chunk {}/{}: fetching seeds for {} pools ({} rpc calls)",
            chunk_index + 1,
            total_chunks,
            pools_chunk.len(),
            pools_chunk.len() * POOL_STATIC_CALLS_PER_POOL
        );
        let mut batch = RpcBatch::new();
        for pool in pools_chunk {
            batch = batch
                .add(Token0 {}, pool.clone())
                .add(Token1 {}, pool.clone())
                .add(Fee {}, pool.clone())
                .add(TickSpacing {}, pool.clone())
                .add(Liquidity {}, pool.clone())
                .add(Slot0 {}, pool.clone());
        }

        let responses = batch.execute().map_err(|err| {
            anyhow!(
                "bootstrap static rpc batch failed for chunk {} ({} pools): {err}",
                chunk_index + 1,
                pools_chunk.len()
            )
        })?;

        substreams::log::info!(
            "bootstrap static chunk {}/{} completed: decoded {} pools",
            chunk_index + 1,
            total_chunks,
            pools_chunk.len()
        );

        for (pool_index, pool) in pools_chunk.iter().enumerate() {
            let response_offset = pool_index * POOL_STATIC_CALLS_PER_POOL;
            let token0: Vec<u8> =
                decode_response::<Token0, _>(&responses.responses, response_offset, pool, "token0()")?;
            let token1: Vec<u8> = decode_response::<Token1, _>(
                &responses.responses,
                response_offset + 1,
                pool,
                "token1()",
            )?;
            let fee: BigInt =
                decode_response::<Fee, _>(&responses.responses, response_offset + 2, pool, "fee()")?;
            let tick_spacing: BigInt = decode_response::<TickSpacing, _>(
                &responses.responses,
                response_offset + 3,
                pool,
                "tickSpacing()",
            )?;
            let liquidity: BigInt = decode_response::<Liquidity, _>(
                &responses.responses,
                response_offset + 4,
                pool,
                "liquidity()",
            )?;
            let (sqrt_price_x96, tick, _, _, _, fee_protocol, _): Slot0Value =
                decode_response::<Slot0, _>(&responses.responses, response_offset + 5, pool, "slot0()")?;
            let (protocol_fee_token0, protocol_fee_token1) = decode_fee_protocol(&fee_protocol)?;

            seeds.push(PoolSnapshotSeed {
                pool: pool.clone(),
                component_id: pool.to_hex(),
                token0,
                token1,
                fee,
                tick_spacing,
                liquidity,
                sqrt_price_x96,
                tick,
                protocol_fee_token0,
                protocol_fee_token1,
            });
        }
    }

    Ok(seeds)
}

fn fetch_pool_balances_batched(pool_seeds: &[PoolSnapshotSeed]) -> Result<Vec<(BigInt, BigInt)>> {
    let pools_per_chunk =
        max_items_per_rpc_batch(BALANCE_RPC_BATCH_SIZE, POOL_BALANCE_CALLS_PER_POOL);
    let mut balances = Vec::with_capacity(pool_seeds.len());
    let total_chunks = pool_seeds.len().div_ceil(pools_per_chunk);

    for (chunk_index, pools_chunk) in pool_seeds.chunks(pools_per_chunk).enumerate() {
        substreams::log::info!(
            "bootstrap balance chunk {}/{}: fetching balances for {} pools ({} rpc calls)",
            chunk_index + 1,
            total_chunks,
            pools_chunk.len(),
            pools_chunk.len() * POOL_BALANCE_CALLS_PER_POOL
        );
        let mut batch = RpcBatch::new();
        for seed in pools_chunk {
            batch = batch
                .add(
                    BalanceOf {
                        owner: seed.pool.clone(),
                    },
                    seed.token0.clone(),
                )
                .add(
                    BalanceOf {
                        owner: seed.pool.clone(),
                    },
                    seed.token1.clone(),
                );
        }

        let responses = batch.execute().map_err(|err| {
            anyhow!(
                "balance rpc batch failed for chunk {} ({} pools): {err}",
                chunk_index + 1,
                pools_chunk.len()
            )
        })?;

        substreams::log::info!(
            "bootstrap balance chunk {}/{} completed: decoded {} pools",
            chunk_index + 1,
            total_chunks,
            pools_chunk.len()
        );

        for (pool_index, seed) in pools_chunk.iter().enumerate() {
            let response_offset = pool_index * POOL_BALANCE_CALLS_PER_POOL;
            let balance0: BigInt = decode_response::<BalanceOf, _>(
                &responses.responses,
                response_offset,
                &seed.pool,
                "balanceOf(token0)",
            )?;
            let balance1: BigInt = decode_response::<BalanceOf, _>(
                &responses.responses,
                response_offset + 1,
                &seed.pool,
                "balanceOf(token1)",
            )?;
            balances.push((balance0, balance1));
        }
    }

    Ok(balances)
}

fn snapshot_tick_attributes_via_lens(seed: &PoolSnapshotSeed) -> Result<Vec<Attribute>> {
    substreams::log::info!("bootstrap lens scan started for pool={}", seed.component_id);
    let lens_address = tick_snapshot_lens_address();
    let (_, min_word, _): WordBoundsValue = call_lens::<WordBounds, _>(
        WordBounds { pool: seed.pool.clone() },
        &lens_address,
        &seed.pool,
        "wordBounds()",
    )?;

    let mut start_word = min_word;
    let mut page = 0usize;
    let mut attributes = Vec::new();

    loop {
        let (_tick_spacing, non_empty_words, next_word, done): ScanWordsPageValue = call_lens::<ScanWordsPage, _>(
            ScanWordsPage {
                pool: seed.pool.clone(),
                start_word: start_word.clone(),
                max_words_to_scan: BigInt::from(LENS_WORDS_PER_PAGE),
                max_non_empty_words: BigInt::from(LENS_NON_EMPTY_WORDS_PER_PAGE),
            },
            &lens_address,
            &seed.pool,
            "scanWordsPage()",
        )?;

        page += 1;
        if page % LOG_EVERY_N_BATCHES == 0 || done {
            substreams::log::info!(
                "lens scan page={} pool={} non_empty_words={} done={}",
                page,
                seed.component_id,
                non_empty_words.len(),
                done
            );
        }

        if !non_empty_words.is_empty() {
            let (packed_ticks, counts): GetTicksForWordsValue = call_lens::<GetTicksForWords, _>(
                GetTicksForWords {
                    pool: seed.pool.clone(),
                    words: non_empty_words,
                },
                &lens_address,
                &seed.pool,
                "getTicksForWords()",
            )?;
            attributes.extend(decode_packed_tick_attributes(&packed_ticks, &counts)?);
        }

        if done {
            break;
        }
        start_word = next_word;
    }

    substreams::log::info!(
        "bootstrap lens scan completed for pool={} tick_attributes={}",
        seed.component_id,
        attributes.len()
    );

    Ok(attributes)
}

fn build_pool_change(
    seed: PoolSnapshotSeed,
    tick_attributes: Vec<Attribute>,
    balance0: BigInt,
    balance1: BigInt,
    tx: &tycho_substreams::prelude::Transaction,
) -> TransactionEntityChanges {
    substreams::log::info!(
        "bootstrapped pool {} with {} tick attributes",
        seed.component_id,
        tick_attributes.len()
    );

    TransactionEntityChanges {
        tx: Some(tx.clone()),
        entity_changes: vec![EntityChanges {
            component_id: seed.component_id.clone(),
            attributes: {
                let mut attrs = vec![
                    attribute("liquidity", seed.liquidity.to_signed_bytes_be()),
                    attribute("tick", seed.tick.to_signed_bytes_be()),
                    attribute("sqrt_price_x96", seed.sqrt_price_x96.to_signed_bytes_be()),
                    attribute(
                        "protocol_fees/token0",
                        BigInt::from(seed.protocol_fee_token0 as u64).to_signed_bytes_be(),
                    ),
                    attribute(
                        "protocol_fees/token1",
                        BigInt::from(seed.protocol_fee_token1 as u64).to_signed_bytes_be(),
                    ),
                ];
                attrs.extend(tick_attributes);
                attrs
            },
        }],
        component_changes: vec![ProtocolComponent {
            id: seed.component_id.clone(),
            tokens: vec![seed.token0.clone(), seed.token1.clone()],
            contracts: vec![],
            static_att: vec![
                attribute("fee", seed.fee.to_signed_bytes_be()),
                attribute("tick_spacing", seed.tick_spacing.to_signed_bytes_be()),
                attribute("pool_address", seed.pool.clone()),
            ],
            change: i32::from(ChangeType::Creation),
            protocol_type: Some(ProtocolType {
                name: "uniswap_v3_pool".to_string(),
                financial_type: FinancialType::Swap.into(),
                attribute_schema: vec![],
                implementation_type: ImplementationType::Custom.into(),
            }),
        }],
        balance_changes: vec![
            BalanceChange {
                token: seed.token0,
                balance: balance0.to_signed_bytes_be(),
                component_id: seed.component_id.as_bytes().to_vec(),
            },
            BalanceChange {
                token: seed.token1,
                balance: balance1.to_signed_bytes_be(),
                component_id: seed.component_id.as_bytes().to_vec(),
            },
        ],
    }
}

fn decode_packed_tick_attributes(packed_ticks: &[u8], counts: &[BigInt]) -> Result<Vec<Attribute>> {
    let total_ticks = counts
        .iter()
        .try_fold(0usize, |acc, count| {
            let parsed = parse_usize(count)?;
            acc.checked_add(parsed).ok_or_else(|| anyhow!("tick count overflow"))
        })?;
    let expected_len = total_ticks
        .checked_mul(PACKED_TICK_SIZE)
        .ok_or_else(|| anyhow!("packed tick payload length overflow"))?;

    if packed_ticks.len() != expected_len {
        return Err(anyhow!(
            "packed tick payload size mismatch: expected {} bytes for {} ticks, got {} bytes",
            expected_len,
            total_ticks,
            packed_ticks.len()
        ));
    }

    let mut attributes = Vec::with_capacity(total_ticks);
    for chunk in packed_ticks.chunks_exact(PACKED_TICK_SIZE) {
        let tick = decode_signed_bigint(&chunk[..3], 3);
        let liquidity_net = decode_signed_bigint(&chunk[3..], 16);
        if liquidity_net == BigInt::from(0) {
            continue;
        }
        attributes.push(attribute(
            &format!("ticks/{tick}/net-liquidity"),
            liquidity_net.to_signed_bytes_be(),
        ));
    }

    Ok(attributes)
}

fn bootstrap_transaction(block: &eth::Block) -> Result<tycho_substreams::prelude::Transaction> {
    block
        .transaction_traces
        .iter()
        .find(|tx| tx.status == 1)
        .or_else(|| block.transaction_traces.first())
        .map(Into::into)
        .ok_or_else(|| anyhow!("bootstrap block {} has no transactions", block.number))
}

fn parse_address(value: &str) -> Result<Vec<u8>> {
    let trimmed = value
        .strip_prefix("0x")
        .unwrap_or(value);
    let decoded = hex::decode(trimmed)?;
    if decoded.len() != 20 {
        return Err(anyhow!("address `{value}` is not 20 bytes"));
    }
    Ok(decoded)
}

fn attribute(name: &str, value: Vec<u8>) -> Attribute {
    Attribute { name: name.to_string(), value, change: ChangeType::Creation.into() }
}

fn max_items_per_rpc_batch(batch_size: usize, calls_per_item: usize) -> usize {
    std::cmp::max(1, batch_size / calls_per_item)
}

fn decode_fee_protocol(value: &BigInt) -> Result<(u8, u8)> {
    let fee_protocol = value
        .to_string()
        .parse::<u8>()
        .context("parse feeProtocol from bigint")?;
    Ok((fee_protocol & 0x0f, fee_protocol >> 4))
}

fn decode_response<T, R>(
    responses: &[substreams_ethereum::pb::eth::rpc::RpcResponse],
    index: usize,
    pool: &[u8],
    call_name: &str,
) -> Result<R>
where
    T: Function + RPCDecodable<R>,
{
    let response = responses.get(index).ok_or_else(|| {
        anyhow!("missing rpc response {index} for {call_name} on {}", hex::encode(pool))
    })?;
    RpcBatch::decode::<R, T>(response)
        .ok_or_else(|| anyhow!("{call_name} failed for {}", hex::encode(pool)))
}

fn call_lens<T, R>(function: T, lens_address: &[u8], pool: &[u8], call_name: &str) -> Result<R>
where
    T: Function + RPCDecodable<R>,
{
    let mut batch = RpcBatch::new();
    batch = batch.add(function, lens_address.to_vec());
    let responses = batch
        .execute()
        .map_err(|err| anyhow!("{} failed for {}: {err}", call_name, hex::encode(pool)))?;
    decode_response::<T, R>(&responses.responses, 0, pool, call_name)
}

fn tick_snapshot_lens_address() -> Vec<u8> {
    UNISWAP_V3_TICK_SNAPSHOT_LENS_ADDRESS.to_vec()
}

fn parse_usize(value: &BigInt) -> Result<usize> {
    value
        .to_string()
        .parse::<usize>()
        .context("parse usize from bigint")
}

fn decode_signed_bigint(bytes: &[u8], width: usize) -> BigInt {
    debug_assert_eq!(bytes.len(), width);
    let sign_byte = if bytes.first().is_some_and(|byte| byte & 0x80 != 0) {
        0xff
    } else {
        0x00
    };
    let mut padded = vec![sign_byte; 32 - width];
    padded.extend_from_slice(bytes);
    BigInt::from_signed_bytes_be(&padded)
}

#[cfg(test)]
mod tests {
    use substreams::scalar::BigInt;

    use super::{decode_fee_protocol, decode_packed_tick_attributes, parse_config};

    #[test]
    fn parses_repeated_pool_params() {
        let config = parse_config(
            "bootstrap_block=123&pool=0x0000000000000000000000000000000000000001&pool=0x0000000000000000000000000000000000000002",
        )
        .expect("valid config");

        assert_eq!(config.bootstrap_block, 123);
        assert_eq!(config.pools.len(), 2);
    }

    #[test]
    fn parses_comma_separated_pools() {
        let config = parse_config(
            "bootstrap_block=123&pools=0x0000000000000000000000000000000000000001,0x0000000000000000000000000000000000000002",
        )
        .expect("valid config");

        assert_eq!(config.bootstrap_block, 123);
        assert_eq!(config.pools.len(), 2);
    }

    #[test]
    fn decodes_fee_protocol_nibbles() {
        let (token0, token1) = decode_fee_protocol(&BigInt::from(0x44_u64)).expect("valid value");

        assert_eq!(token0, 4);
        assert_eq!(token1, 4);
    }

    #[test]
    fn decodes_packed_tick_attributes() {
        let packed_ticks = vec![
            0xff, 0xff, 0x88, // -120
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // 50
            0x00, 0x00, 0x3c, // 60
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xe7, // -25
        ];
        let attributes = decode_packed_tick_attributes(&packed_ticks, &[BigInt::from(2_u64)])
            .expect("decode should succeed");

        assert_eq!(attributes.len(), 2);
        assert_eq!(attributes[0].name, "ticks/-120/net-liquidity");
        assert_eq!(attributes[1].name, "ticks/60/net-liquidity");
    }
}
