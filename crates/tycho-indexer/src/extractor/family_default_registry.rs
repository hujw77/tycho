use crate::extractor::{
    extractor_config::BootstrapStrategy,
    family_registry::{
        canonical_pool_list_shared_family_member_spec, shared_family_bootstrap_runtime,
        shared_family_runtime_spec_with_auxiliary_decoders, FamilyMemberSpec,
        FamilyRuntimeRegistry, FamilyRuntimeSpec,
    },
    family_uniswap::{
        materialize_uniswap_family_plan, materialize_uniswap_v2_branch,
        materialize_uniswap_v3_branch,
        AUXILIARY_PROTOCOL_MESSAGE_DECODERS as UNISWAP_AUXILIARY_PROTOCOL_MESSAGE_DECODERS,
    },
};

const UNISWAP_V2_MEMBER: FamilyMemberSpec = canonical_pool_list_shared_family_member_spec(
    "uniswap_v2",
    BootstrapStrategy::UniswapV2Rpc,
    materialize_uniswap_v2_branch,
);

const UNISWAP_V3_MEMBER: FamilyMemberSpec = canonical_pool_list_shared_family_member_spec(
    "uniswap_v3",
    BootstrapStrategy::UniswapV3Rpc,
    materialize_uniswap_v3_branch,
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

pub const fn default_family_runtime_registry() -> FamilyRuntimeRegistry<'static> {
    FamilyRuntimeRegistry::new(default_family_runtime_specs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_auxiliary_decoder_groups_are_sourced_from_default_family_registry() {
        let families = default_family_runtime_specs();
        assert_eq!(families.len(), 1);
        assert_eq!(
            families[0].auxiliary_protocol_message_decoders().len(),
            1
        );
        assert_eq!(
            families[0].auxiliary_protocol_message_decoders()[0].protocol_system,
            "uniswap_v3"
        );
        assert_eq!(
            families[0].auxiliary_protocol_message_decoders()[0].type_url_suffix,
            "Events"
        );
        assert_eq!(families[0].output_module(), "map_uniswap_family_protocol_changes");
        assert_eq!(families[0].shared_stream_name(), "uniswap_family");
        assert_eq!(families[0].durability_scope(), "family::uniswap");
    }
}
