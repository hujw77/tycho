use crate::extractor::{
    extractor_config::BootstrapStrategy,
    family_bootstrap_registry::parallel_shared_bootstrap_plan_materializer,
    family_registry::{
        canonical_shared_family_runtime_spec,
        canonical_pool_list_shared_family_member_spec,
        canonical_pool_list_shared_family_member_with_auxiliary_runtime_hooks,
        shared_family_bootstrap_runtime,
        FamilyMemberSpec, FamilyRuntimeRegistry,
        FamilyRuntimeSpec,
    },
    family_uniswap::{
        materialize_uniswap_v2_branch, materialize_uniswap_v3_branch,
        AUXILIARY_PROTOCOL_MESSAGE_DECODERS, AUXILIARY_PROTOCOL_STATE_HYDRATORS,
    },
};

const UNISWAP_V2_MEMBER: FamilyMemberSpec = canonical_pool_list_shared_family_member_spec(
    "uniswap_v2",
    BootstrapStrategy::UniswapV2Rpc,
    materialize_uniswap_v2_branch,
);

const UNISWAP_V3_MEMBER_WITH_AUXILIARY_RUNTIME_HOOKS: FamilyMemberSpec =
    canonical_pool_list_shared_family_member_with_auxiliary_runtime_hooks(
        "uniswap_v3",
        BootstrapStrategy::UniswapV3Rpc,
        materialize_uniswap_v3_branch,
        AUXILIARY_PROTOCOL_MESSAGE_DECODERS,
        AUXILIARY_PROTOCOL_STATE_HYDRATORS,
    );

const UNISWAP_V2_V3_MEMBERS: &[FamilyMemberSpec] =
    &[UNISWAP_V2_MEMBER, UNISWAP_V3_MEMBER_WITH_AUXILIARY_RUNTIME_HOOKS];

const UNISWAP_V2_V3_FAMILY: FamilyRuntimeSpec =
    canonical_shared_family_runtime_spec!(
        "uniswap",
        UNISWAP_V2_V3_MEMBERS,
        Some(shared_family_bootstrap_runtime(parallel_shared_bootstrap_plan_materializer)),
        shared_progress_owner_protocol_system: "uniswap_v2",
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
    use crate::extractor::family_bootstrap_registry::parallel_shared_bootstrap_plan_materializer;

    #[test]
    fn default_auxiliary_decoder_groups_are_sourced_from_default_family_registry() {
        let families = default_family_runtime_specs();
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].output_module(), "map_uniswap_family_protocol_changes");
        assert_eq!(families[0].shared_stream_name(), "uniswap_family");
        assert_eq!(families[0].durability_scope(), "family::uniswap");
        assert_eq!(
            families[0].shared_progress_owner_protocol_system(),
            "uniswap_v2"
        );

        let decoders = default_family_runtime_registry()
            .auxiliary_protocol_message_decoders_for_protocol_system("uniswap_v3")
            .expect("uniswap_v3 decoders should be registered");
        assert_eq!(decoders.len(), 1);
        assert_eq!(decoders[0].protocol_system, "uniswap_v3");
        assert_eq!(decoders[0].type_url_suffix, "Events");
    }

    #[test]
    fn default_auxiliary_hydrator_groups_are_sourced_from_default_family_registry() {
        let families = default_family_runtime_specs();
        assert_eq!(families.len(), 1);

        let hydrators = default_family_runtime_registry()
            .auxiliary_protocol_state_hydrators_for_protocol_system("uniswap_v3")
            .expect("uniswap_v3 hydrators should be registered");
        assert_eq!(hydrators.len(), 1);
        assert_eq!(hydrators[0].protocol_system, "uniswap_v3");
    }

    #[test]
    fn default_uniswap_family_uses_explicit_shared_bootstrap_plan_materializer() {
        let families = default_family_runtime_specs();
        assert_eq!(families.len(), 1);
        assert!(families[0]
            .shared_bootstrap_runtime()
            .is_some());

        let materializer = default_family_runtime_registry()
            .resolve_shared_bootstrap_execution("uniswap")
            .expect("resolve uniswap bootstrap execution")
            .plan_materializer;

        assert_eq!(
            materializer as *const () as usize,
            parallel_shared_bootstrap_plan_materializer as *const () as usize
        );
    }
}
