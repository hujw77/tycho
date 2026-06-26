use std::{future::Future, pin::Pin};

use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    family_runtime::FamilyRuntimeRegistry,
    models::BlockChanges,
    shared_bootstrap::{
        materialize_plan_by_registered_branches, BootstrapBranchDescriptor, SharedBootstrapPlan,
    },
    uniswap_v2_bootstrap, uniswap_v3_bootstrap, ExtractionError,
};

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
    registry: FamilyRuntimeRegistry<'a>,
) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
    Box::pin(async move { materialize_plan_by_registered_branches(rpc, plan, registry).await })
}
