use tycho_indexer::canonical_shared_family_runtime_spec_with_explicit_owner;
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    extractor_config::BootstrapStrategy,
    family_bootstrap_registry::MaterializeBootstrapBranchFn,
    family_registry::{
        canonical_pool_list_shared_family_member_spec,
        canonical_pool_list_shared_family_member_with_auxiliary_runtime_hooks, FamilyMemberSpec,
        FamilyRuntimeRegistry, FamilyRuntimeSpec,
    },
    models::BlockChanges,
    family_runtime_metadata::FamilyRuntimeConfig,
    protocol_message_registry::AuxiliaryProtocolMessageDecoder,
    shared_bootstrap::BootstrapBranchDescriptor,
    ExtractionError,
};

fn future_family_noop_materialize_branch<'a>(
    _rpc: &'a EthereumRpcClient,
    branch: &'a BootstrapBranchDescriptor,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>,
> {
    Box::pin(async move {
        Ok(BlockChanges::new(
            branch.extractor_name.clone(),
            branch.chain,
            Default::default(),
            branch.params.bootstrap_block,
            false,
            Vec::new(),
            Vec::new(),
        ))
    })
}

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

pub(crate) fn future_family_runtime_registry_with_auxiliary_decoders_and_explicit_progress_owner_for_tests(
    member_protocol_systems: &[&'static str],
    shared_progress_owner_protocol_system: &'static str,
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
                canonical_pool_list_shared_family_member_spec(
                    protocol_system,
                    BootstrapStrategy::UniswapV2Rpc,
                    future_family_noop_materialize_branch as MaterializeBootstrapBranchFn,
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let specs: &'static [FamilyRuntimeSpec] = Box::leak(Box::new([
        canonical_shared_family_runtime_spec_with_explicit_owner!(
            "future_swap",
            members,
            None,
            shared_progress_owner_protocol_system,
            durability_scope: leaked_scope,
            auxiliary_protocol_message_decoders: auxiliary_decoders,
        ),
    ]));

    FamilyRuntimeRegistry::new(specs)
}

pub(crate) fn future_family_runtime_registry_with_explicit_progress_owner_for_tests(
    member_protocol_systems: &[&'static str],
    shared_progress_owner_protocol_system: &'static str,
    durability_scope: impl Into<String>,
) -> FamilyRuntimeRegistry<'static> {
    future_family_runtime_registry_with_auxiliary_decoders_and_explicit_progress_owner_for_tests(
        member_protocol_systems,
        shared_progress_owner_protocol_system,
        durability_scope,
        &[],
    )
}

pub(crate) fn future_family_runtime_registry_with_member_auxiliary_decoders_and_explicit_progress_owner_for_tests(
    member_protocol_systems: &[&'static str],
    shared_progress_owner_protocol_system: &'static str,
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
                if *protocol_system == protocol_system_with_decoders {
                    canonical_pool_list_shared_family_member_with_auxiliary_runtime_hooks(
                        protocol_system,
                        BootstrapStrategy::UniswapV2Rpc,
                        future_family_noop_materialize_branch as MaterializeBootstrapBranchFn,
                        auxiliary_decoders,
                        &[],
                    )
                } else {
                    canonical_pool_list_shared_family_member_spec(
                        protocol_system,
                        BootstrapStrategy::UniswapV2Rpc,
                        future_family_noop_materialize_branch as MaterializeBootstrapBranchFn,
                    )
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let specs: &'static [FamilyRuntimeSpec] = Box::leak(Box::new([
        canonical_shared_family_runtime_spec_with_explicit_owner!(
            "future_swap",
            members,
            None,
            shared_progress_owner_protocol_system,
            durability_scope: leaked_scope,
            auxiliary_protocol_message_decoders: &[],
            auxiliary_protocol_state_hydrators: &[],
        ),
    ]));

    FamilyRuntimeRegistry::new(specs)
}
