use crate::extractor::{
    family_registry::{
        shared_family_member_spec, shared_family_member_spec_with_auxiliary_runtime_hooks,
        shared_family_runtime_spec, shared_family_runtime_spec_with_auxiliary_decoders,
        FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec,
    },
    family_runtime_metadata::FamilyRuntimeConfig,
    protocol_message_registry::AuxiliaryProtocolMessageDecoder,
};

pub(crate) fn future_family_runtime_config_for_tests(
    shared_spkg: impl Into<String>,
    durability_scope: impl Into<String>,
) -> FamilyRuntimeConfig {
    FamilyRuntimeConfig {
        family: "future_swap".to_string(),
        shared_spkg: Some(shared_spkg.into()),
        shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
        durability_scope: Some(durability_scope.into()),
    }
}

pub(crate) fn future_family_runtime_registry_with_auxiliary_decoders_for_tests(
    member_protocol_systems: &[&'static str],
    durability_scope: impl Into<String>,
    auxiliary_decoders: &'static [AuxiliaryProtocolMessageDecoder],
) -> FamilyRuntimeRegistry<'static> {
    fn leak_str(value: String) -> &'static str {
        Box::leak(value.into_boxed_str())
    }

    let leaked_scope = leak_str(durability_scope.into());
    let members: &'static [FamilyMemberSpec] = Box::leak(
        member_protocol_systems
            .iter()
            .map(|protocol_system| {
                let normalized_alias = leak_str(protocol_system.replace('_', ""));
                let aliases: &'static [&'static str] =
                    Box::leak(vec![normalized_alias].into_boxed_slice());
                shared_family_member_spec(protocol_system, aliases, None)
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let specs: &'static [FamilyRuntimeSpec] = Box::leak(Box::new([
        shared_family_runtime_spec_with_auxiliary_decoders(
            "future_swap",
            members,
            "map_future_swap_family_protocol_changes",
            "future_swap_family",
            leaked_scope,
            None,
            auxiliary_decoders,
        ),
    ]));

    FamilyRuntimeRegistry::new(specs)
}

pub(crate) fn future_family_runtime_registry_for_tests(
    member_protocol_systems: &[&'static str],
    durability_scope: impl Into<String>,
) -> FamilyRuntimeRegistry<'static> {
    future_family_runtime_registry_with_auxiliary_decoders_for_tests(
        member_protocol_systems,
        durability_scope,
        &[],
    )
}

pub(crate) fn future_family_runtime_registry_with_member_auxiliary_decoders_for_tests(
    member_protocol_systems: &[&'static str],
    protocol_system_with_decoders: &'static str,
    durability_scope: impl Into<String>,
    auxiliary_decoders: &'static [AuxiliaryProtocolMessageDecoder],
) -> FamilyRuntimeRegistry<'static> {
    fn leak_str(value: String) -> &'static str {
        Box::leak(value.into_boxed_str())
    }

    let leaked_scope = leak_str(durability_scope.into());
    let members: &'static [FamilyMemberSpec] = Box::leak(
        member_protocol_systems
            .iter()
            .map(|protocol_system| {
                let normalized_alias = leak_str(protocol_system.replace('_', ""));
                let aliases: &'static [&'static str] =
                    Box::leak(vec![normalized_alias].into_boxed_slice());
                if *protocol_system == protocol_system_with_decoders {
                    shared_family_member_spec_with_auxiliary_runtime_hooks(
                        protocol_system,
                        aliases,
                        None,
                        auxiliary_decoders,
                        &[],
                    )
                } else {
                    shared_family_member_spec(protocol_system, aliases, None)
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let specs: &'static [FamilyRuntimeSpec] = Box::leak(Box::new([shared_family_runtime_spec(
        "future_swap",
        members,
        "map_future_swap_family_protocol_changes",
        "future_swap_family",
        leaked_scope,
        None,
    )]));

    FamilyRuntimeRegistry::new(specs)
}
