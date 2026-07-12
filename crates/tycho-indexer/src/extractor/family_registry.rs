use crate::extractor::{
    family_runtime::{
        FamilyMemberSpec, FamilyRuntimeSpec, SharedBootstrapMemberRuntime,
        SharedBootstrapParamsParser, SharedFamilyBootstrapRuntime,
    },
    family_uniswap::{
        materialize_uniswap_family_plan, materialize_uniswap_v2_branch,
        materialize_uniswap_v3_branch, AUXILIARY_PROTOCOL_MESSAGE_DECODERS as UNISWAP_AUXILIARY_PROTOCOL_MESSAGE_DECODERS,
    },
    protocol_message_registry::AuxiliaryProtocolMessageDecoder,
    runner::BootstrapStrategy,
};

#[cfg(test)]
macro_rules! canonical_shared_family_runtime_spec {
    (
        $family_name:literal,
        $members:expr,
        $shared_bootstrap_runtime:expr $(,)?
    ) => {
        shared_family_runtime_spec(
            $family_name,
            $members,
            concat!("map_", $family_name, "_family_protocol_changes"),
            concat!($family_name, "_family"),
            concat!("family::", $family_name),
            $shared_bootstrap_runtime,
        )
    };
}

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use canonical_shared_family_runtime_spec;

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
    SharedBootstrapMemberRuntime { strategy, params_parser, materialize_branch }
}

pub const fn shared_family_member_spec(
    protocol_system: &'static str,
    shared_route_protocols: &'static [&'static str],
    shared_bootstrap: Option<SharedBootstrapMemberRuntime>,
) -> FamilyMemberSpec {
    FamilyMemberSpec { protocol_system, shared_route_protocols, shared_bootstrap }
}

pub const fn canonical_shared_family_member_spec(
    protocol_system: &'static str,
    shared_bootstrap: Option<SharedBootstrapMemberRuntime>,
) -> FamilyMemberSpec {
    shared_family_member_spec(protocol_system, &[], shared_bootstrap)
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
    shared_stream_name: &'static str,
    durability_scope: &'static str,
    shared_bootstrap_runtime: Option<SharedFamilyBootstrapRuntime>,
) -> FamilyRuntimeSpec {
    shared_family_runtime_spec_with_auxiliary_decoders(
        family_name,
        members,
        output_module,
        shared_stream_name,
        durability_scope,
        shared_bootstrap_runtime,
        &[],
    )
}

pub(crate) const fn shared_family_runtime_spec_with_auxiliary_decoders(
    family_name: &'static str,
    members: &'static [FamilyMemberSpec],
    output_module: &'static str,
    shared_stream_name: &'static str,
    durability_scope: &'static str,
    shared_bootstrap_runtime: Option<SharedFamilyBootstrapRuntime>,
    auxiliary_protocol_message_decoders: &'static [AuxiliaryProtocolMessageDecoder],
) -> FamilyRuntimeSpec {
    FamilyRuntimeSpec {
        family_name,
        members,
        output_module,
        shared_stream_name,
        durability_scope,
        shared_bootstrap_runtime,
        auxiliary_protocol_message_decoders,
    }
}

const UNISWAP_V2_MEMBER: FamilyMemberSpec = canonical_shared_family_member_spec(
    "uniswap_v2",
    Some(pool_list_bootstrap_member_runtime(
        BootstrapStrategy::UniswapV2Rpc,
        materialize_uniswap_v2_branch,
    )),
);

const UNISWAP_V3_MEMBER: FamilyMemberSpec = canonical_shared_family_member_spec(
    "uniswap_v3",
    Some(pool_list_bootstrap_member_runtime(
        BootstrapStrategy::UniswapV3Rpc,
        materialize_uniswap_v3_branch,
    )),
);

const UNISWAP_V2_V3_MEMBERS: &[FamilyMemberSpec] = &[UNISWAP_V2_MEMBER, UNISWAP_V3_MEMBER];

const UNISWAP_V2_V3_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec_with_auxiliary_decoders(
    "uniswap",
    UNISWAP_V2_V3_MEMBERS,
    "map_uniswap_family_protocol_changes",
    "uniswap_family",
    "family::uniswap",
    Some(shared_family_bootstrap_runtime(materialize_uniswap_family_plan)),
    UNISWAP_AUXILIARY_PROTOCOL_MESSAGE_DECODERS,
);

const DEFAULT_FAMILY_RUNTIME_SPECS: &[FamilyRuntimeSpec] = &[UNISWAP_V2_V3_FAMILY];

pub const fn default_family_runtime_specs() -> &'static [FamilyRuntimeSpec] {
    DEFAULT_FAMILY_RUNTIME_SPECS
}

#[cfg(test)]
mod tests {
    use super::*;

    const FUTURE_MEMBER: FamilyMemberSpec =
        canonical_shared_family_member_spec("future_swap_v1", None);
    const FUTURE_MEMBERS: &[FamilyMemberSpec] = &[FUTURE_MEMBER];
    const FUTURE_FAMILY: FamilyRuntimeSpec =
        canonical_shared_family_runtime_spec!("future_swap", FUTURE_MEMBERS, None);

    #[test]
    fn canonical_shared_family_runtime_spec_derives_identity_fields() {
        assert_eq!(FUTURE_FAMILY.family_name, "future_swap");
        assert_eq!(FUTURE_FAMILY.output_module, "map_future_swap_family_protocol_changes");
        assert_eq!(FUTURE_FAMILY.shared_stream_name, "future_swap_family");
        assert_eq!(FUTURE_FAMILY.durability_scope, "family::future_swap");
    }

    #[test]
    fn default_auxiliary_decoder_groups_are_sourced_from_family_registry() {
        let families = default_family_runtime_specs();
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].auxiliary_protocol_message_decoders.len(), 1);
        assert_eq!(
            families[0].auxiliary_protocol_message_decoders[0].protocol_system,
            "uniswap_v3"
        );
        assert_eq!(
            families[0].auxiliary_protocol_message_decoders[0].type_url_suffix,
            "Events"
        );
    }
}
