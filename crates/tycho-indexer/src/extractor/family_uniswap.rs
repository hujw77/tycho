use std::{future::Future, pin::Pin};

use prost::Message;
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    protocol_message_registry::{AuxiliaryProtocolMessage, AuxiliaryProtocolMessageDecoder},
    family_runtime::ResolvedSharedBootstrapBranchRuntime,
    models::BlockChanges,
    shared_bootstrap::{
        materialize_plan_by_branch_runtimes, BootstrapBranchDescriptor, SharedBootstrapPlan,
    },
    uniswap_v2_bootstrap, uniswap_v3_bootstrap, uniswap_v3_stream, ExtractionError,
};

fn decode_uniswap_v3_events(
    value: &[u8],
) -> Result<AuxiliaryProtocolMessage, ExtractionError> {
    Ok(AuxiliaryProtocolMessage::UniswapV3Events(
        uniswap_v3_stream::Events::decode(value)?,
    ))
}

pub(crate) const AUXILIARY_PROTOCOL_MESSAGE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
    &[AuxiliaryProtocolMessageDecoder {
        protocol_system: "uniswap_v3",
        type_url_suffix: "Events",
        decode: decode_uniswap_v3_events,
    }];

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

pub(crate) fn materialize_uniswap_family_plan<'a>(
    rpc: &'a EthereumRpcClient,
    plan: &'a SharedBootstrapPlan,
    branch_runtimes: &'a [ResolvedSharedBootstrapBranchRuntime],
) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
    Box::pin(async move { materialize_plan_by_branch_runtimes(rpc, plan, branch_runtimes).await })
}
