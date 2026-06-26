use crate::extractor::{
    family_runtime::{
        FamilyMemberSpec, FamilyRuntimeSpec, SharedBootstrapMemberRuntime,
        SharedBootstrapParamsParser, SharedFamilyBootstrapRuntime,
    },
    family_uniswap::{
        materialize_uniswap_family_plan, materialize_uniswap_v2_branch,
        materialize_uniswap_v3_branch,
    },
    runner::BootstrapStrategy,
};

pub const fn pool_list_bootstrap_member_runtime(
    strategy: BootstrapStrategy,
    materialize_branch: crate::extractor::family_runtime::MaterializeBootstrapBranchFn,
) -> SharedBootstrapMemberRuntime {
    shared_bootstrap_member_runtime(
        strategy,
        SharedBootstrapParamsParser::PoolList,
        materialize_branch,
    )
}

pub const fn shared_bootstrap_member_runtime(
    strategy: BootstrapStrategy,
    params_parser: SharedBootstrapParamsParser,
    materialize_branch: crate::extractor::family_runtime::MaterializeBootstrapBranchFn,
) -> SharedBootstrapMemberRuntime {
    SharedBootstrapMemberRuntime {
        strategy,
        params_parser,
        materialize_branch,
    }
}

pub const fn shared_family_member_spec(
    protocol_system: &'static str,
    shared_route_protocols: &'static [&'static str],
    shared_bootstrap: Option<SharedBootstrapMemberRuntime>,
) -> FamilyMemberSpec {
    FamilyMemberSpec {
        protocol_system,
        shared_route_protocols,
        shared_bootstrap,
    }
}

pub const fn shared_family_bootstrap_runtime(
    materialize_plan: crate::extractor::family_runtime::MaterializeBootstrapPlanFn,
) -> SharedFamilyBootstrapRuntime {
    SharedFamilyBootstrapRuntime { materialize_plan }
}

pub const fn shared_family_runtime_spec(
    family_name: &'static str,
    members: &'static [FamilyMemberSpec],
    output_module: &'static str,
    shared_bootstrap_runtime: Option<SharedFamilyBootstrapRuntime>,
) -> FamilyRuntimeSpec {
    FamilyRuntimeSpec {
        family_name,
        members,
        output_module,
        shared_bootstrap_runtime,
    }
}

const UNISWAP_V2_MEMBER: FamilyMemberSpec = shared_family_member_spec(
    "uniswap_v2",
    &["uniswapv2"],
    Some(pool_list_bootstrap_member_runtime(
        BootstrapStrategy::UniswapV2Rpc,
        materialize_uniswap_v2_branch,
    )),
);

const UNISWAP_V3_MEMBER: FamilyMemberSpec = shared_family_member_spec(
    "uniswap_v3",
    &["uniswapv3"],
    Some(pool_list_bootstrap_member_runtime(
        BootstrapStrategy::UniswapV3Rpc,
        materialize_uniswap_v3_branch,
    )),
);

const UNISWAP_V2_V3_MEMBERS: &[FamilyMemberSpec] = &[UNISWAP_V2_MEMBER, UNISWAP_V3_MEMBER];

const UNISWAP_V2_V3_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
    "uniswap",
    UNISWAP_V2_V3_MEMBERS,
    "map_uniswap_family_protocol_changes",
    Some(shared_family_bootstrap_runtime(
        materialize_uniswap_family_plan,
    )),
);

const DEFAULT_FAMILY_RUNTIME_SPECS: &[FamilyRuntimeSpec] = &[UNISWAP_V2_V3_FAMILY];

pub const fn default_family_runtime_specs() -> &'static [FamilyRuntimeSpec] {
    DEFAULT_FAMILY_RUNTIME_SPECS
}
