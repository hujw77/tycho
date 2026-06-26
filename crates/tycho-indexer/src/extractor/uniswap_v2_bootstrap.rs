use std::{collections::HashMap, str::FromStr};

use alloy::{
    primitives::{Address as AlloyAddress, U256},
    rpc::types::{BlockId, BlockNumberOrTag, TransactionInput, TransactionRequest},
    sol,
    sol_types::SolCall,
};
use chrono::DateTime;
use num_bigint::BigInt;
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
    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
}

const MULTICALL3_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";
const STATIC_RPC_BATCH_SIZE: usize = 1_000;
const POOL_STATIC_CALLS_PER_POOL: usize = 3;
const TRADING_FEE_BPS: i32 = 30;

#[derive(Debug, Clone)]
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
    reserve0: Bytes,
    reserve1: Bytes,
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

pub async fn build_uniswap_v2_bootstrap_block(
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

    let tx = synthetic_bootstrap_transaction(&block);
    let mut protocol_components = HashMap::with_capacity(seeds.len());
    let mut state_updates = HashMap::with_capacity(seeds.len());
    let mut balance_changes = HashMap::with_capacity(seeds.len());

    for seed in seeds {
        let component_id = seed.component_id.clone();

        protocol_components.insert(
            component_id.clone(),
            build_protocol_component(&seed, protocol_system, chain, &tx, block.ts),
        );
        state_updates.insert(component_id.clone(), build_state_update(&component_id, &seed));
        balance_changes.insert(
            component_id.clone(),
            HashMap::from([
                (
                    seed.token0.clone(),
                    ComponentBalance::new(
                        seed.token0.clone(),
                        seed.reserve0.clone(),
                        bytes_to_f64(seed.reserve0.as_ref()).unwrap_or(f64::NAN),
                        tx.hash.clone(),
                        &component_id,
                    ),
                ),
                (
                    seed.token1.clone(),
                    ComponentBalance::new(
                        seed.token1.clone(),
                        seed.reserve1.clone(),
                        bytes_to_f64(seed.reserve1.as_ref()).unwrap_or(f64::NAN),
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
    let pools_per_chunk = STATIC_RPC_BATCH_SIZE / POOL_STATIC_CALLS_PER_POOL;
    let mut seeds = Vec::with_capacity(pools.len());

    for pools_chunk in pools.chunks(pools_per_chunk.max(1)) {
        let mut calls = Vec::with_capacity(pools_chunk.len() * POOL_STATIC_CALLS_PER_POOL);
        for pool in pools_chunk {
            calls.push(multicall_read(*pool, token0Call {}.abi_encode()));
            calls.push(multicall_read(*pool, token1Call {}.abi_encode()));
            calls.push(multicall_read(*pool, getReservesCall {}.abi_encode()));
        }

        let responses = multicall_many(rpc, block_tag, calls).await?;

        for (pool_index, pool) in pools_chunk.iter().enumerate() {
            let offset = pool_index * POOL_STATIC_CALLS_PER_POOL;
            let token0 =
                decode_address_response::<token0Call>(&responses[offset], pool, "token0()")?;
            let token1 =
                decode_address_response::<token1Call>(&responses[offset + 1], pool, "token1()")?;
            let reserves = getReservesCall::abi_decode_returns_validate(&responses[offset + 2])
                .map_err(|err| decode_error(pool, "getReserves()", err))?;

            seeds.push(PoolSnapshotSeed {
                pool: *pool,
                component_id: format!("{pool:#x}"),
                token0: token0.to_bytes(),
                token1: token1.to_bytes(),
                reserve0: uint_to_bytes(U256::from(reserves.reserve0)),
                reserve1: uint_to_bytes(U256::from(reserves.reserve1)),
            });
        }
    }

    Ok(seeds)
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
        "uniswap_v2_pool",
        chain,
        vec![seed.token0.clone(), seed.token1.clone()],
        vec![],
        HashMap::from([
            (
                "fee".to_string(),
                BigInt::from(TRADING_FEE_BPS)
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

fn build_state_update(component_id: &str, seed: &PoolSnapshotSeed) -> ProtocolComponentStateDelta {
    let updated_attributes = HashMap::from([
        ("reserve0".to_string(), seed.reserve0.clone()),
        ("reserve1".to_string(), seed.reserve1.clone()),
    ]);
    let created_attributes = updated_attributes
        .keys()
        .cloned()
        .collect();

    ProtocolComponentStateDelta {
        component_id: component_id.to_string(),
        updated_attributes,
        deleted_attributes: Default::default(),
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

fn decode_error(
    pool: &AlloyAddress,
    call_name: &str,
    err: impl std::fmt::Display,
) -> ExtractionError {
    ExtractionError::Setup(format!("decode {call_name} for pool {pool:#x}: {err}"))
}

fn uint_to_bytes(value: U256) -> Bytes {
    let bytes = value.to_be_bytes::<32>();
    let first_non_zero = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len() - 1);
    bytes[first_non_zero..].to_vec().into()
}

#[cfg(test)]
mod tests {
    use super::parse_bootstrap_params;

    #[test]
    fn parses_multi_pool_bootstrap_params() {
        let config = parse_bootstrap_params(
            "bootstrap_block=123&pools=0x0000000000000000000000000000000000000001,0x0000000000000000000000000000000000000002",
        )
        .expect("config should parse");

        assert_eq!(config.bootstrap_block, 123);
        assert_eq!(config.pools.len(), 2);
    }
}
