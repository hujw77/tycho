use crate::extractor::{
    extractor_config::BootstrapStrategy,
    family_bootstrap_registry::{
        MaterializeBootstrapBranchFn, MaterializeBootstrapPlanFn, SharedBootstrapMemberRuntime,
        SharedBootstrapParamsParser, SharedFamilyBootstrapRuntime,
    },
    protocol_message_registry::AuxiliaryProtocolMessageDecoder,
};

pub use crate::extractor::family_default_registry::{
    default_family_runtime_registry, default_family_runtime_specs,
};

#[derive(Clone, Copy, Debug)]
pub struct FamilyMemberSpec {
    pub protocol_system: &'static str,
    pub shared_route_protocols: &'static [&'static str],
    pub shared_bootstrap: Option<SharedBootstrapMemberRuntime>,
}

#[derive(Clone, Debug)]
pub struct FamilyRuntimeSpec {
    family_name: &'static str,
    members: &'static [FamilyMemberSpec],
    output_module: &'static str,
    shared_stream_name: &'static str,
    durability_scope: &'static str,
    shared_bootstrap_runtime: Option<SharedFamilyBootstrapRuntime>,
    auxiliary_protocol_message_decoders: &'static [AuxiliaryProtocolMessageDecoder],
}

#[derive(Clone, Copy, Debug)]
pub struct FamilyRuntimeRegistry<'a> {
    specs: &'a [FamilyRuntimeSpec],
}

impl FamilyRuntimeSpec {
    pub(crate) const fn new(
        family_name: &'static str,
        members: &'static [FamilyMemberSpec],
        output_module: &'static str,
        shared_stream_name: &'static str,
        durability_scope: &'static str,
        shared_bootstrap_runtime: Option<SharedFamilyBootstrapRuntime>,
        auxiliary_protocol_message_decoders: &'static [AuxiliaryProtocolMessageDecoder],
    ) -> Self {
        Self {
            family_name,
            members,
            output_module,
            shared_stream_name,
            durability_scope,
            shared_bootstrap_runtime,
            auxiliary_protocol_message_decoders,
        }
    }

    pub const fn family_name(&self) -> &'static str {
        self.family_name
    }

    pub const fn members(&self) -> &'static [FamilyMemberSpec] {
        self.members
    }

    pub const fn output_module(&self) -> &'static str {
        self.output_module
    }

    pub const fn shared_stream_name(&self) -> &'static str {
        self.shared_stream_name
    }

    pub const fn durability_scope(&self) -> &'static str {
        self.durability_scope
    }

    pub const fn shared_bootstrap_runtime(&self) -> Option<SharedFamilyBootstrapRuntime> {
        self.shared_bootstrap_runtime
    }

    pub(crate) const fn auxiliary_protocol_message_decoders(
        &self,
    ) -> &'static [AuxiliaryProtocolMessageDecoder] {
        self.auxiliary_protocol_message_decoders
    }
}

impl<'a> FamilyRuntimeRegistry<'a> {
    pub const fn new(specs: &'a [FamilyRuntimeSpec]) -> Self {
        Self { specs }
    }

    pub fn specs(&self) -> &'a [FamilyRuntimeSpec] {
        self.specs
    }
}

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
    (
        $family_name:literal,
        $members:expr,
        $shared_bootstrap_runtime:expr,
        auxiliary_protocol_message_decoders: $auxiliary_protocol_message_decoders:expr $(,)?
    ) => {
        shared_family_runtime_spec_with_auxiliary_decoders(
            $family_name,
            $members,
            concat!("map_", $family_name, "_family_protocol_changes"),
            concat!($family_name, "_family"),
            concat!("family::", $family_name),
            $shared_bootstrap_runtime,
            $auxiliary_protocol_message_decoders,
        )
    };
}

#[cfg(test)]
pub(crate) use canonical_shared_family_runtime_spec;

pub const fn pool_list_bootstrap_member_runtime(
    strategy: BootstrapStrategy,
    materialize_branch: MaterializeBootstrapBranchFn,
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
    materialize_branch: MaterializeBootstrapBranchFn,
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

pub const fn shared_family_member_with_bootstrap(
    protocol_system: &'static str,
    shared_route_protocols: &'static [&'static str],
    strategy: BootstrapStrategy,
    params_parser: SharedBootstrapParamsParser,
    materialize_branch: MaterializeBootstrapBranchFn,
) -> FamilyMemberSpec {
    shared_family_member_spec(
        protocol_system,
        shared_route_protocols,
        Some(shared_bootstrap_member_runtime(strategy, params_parser, materialize_branch)),
    )
}

pub const fn canonical_shared_family_member_with_bootstrap(
    protocol_system: &'static str,
    strategy: BootstrapStrategy,
    params_parser: SharedBootstrapParamsParser,
    materialize_branch: MaterializeBootstrapBranchFn,
) -> FamilyMemberSpec {
    shared_family_member_with_bootstrap(
        protocol_system,
        &[],
        strategy,
        params_parser,
        materialize_branch,
    )
}

pub const fn canonical_pool_list_shared_family_member_spec(
    protocol_system: &'static str,
    strategy: BootstrapStrategy,
    materialize_branch: MaterializeBootstrapBranchFn,
) -> FamilyMemberSpec {
    canonical_shared_family_member_with_bootstrap(
        protocol_system,
        strategy,
        SharedBootstrapParamsParser::PoolList,
        materialize_branch,
    )
}

pub const fn shared_family_bootstrap_runtime(
    materialize_plan: MaterializeBootstrapPlanFn,
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
    FamilyRuntimeSpec::new(
        family_name,
        members,
        output_module,
        shared_stream_name,
        durability_scope,
        shared_bootstrap_runtime,
        auxiliary_protocol_message_decoders,
    )
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use tycho_ethereum::rpc::EthereumRpcClient;

    use super::*;
    use crate::extractor::{
        models::BlockChanges, shared_bootstrap::BootstrapBranchDescriptor, ExtractionError,
    };

    fn noop_materialize_branch<'a>(
        _rpc: &'a EthereumRpcClient,
        branch: &'a BootstrapBranchDescriptor,
    ) -> Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>> {
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

    const FUTURE_MEMBER: FamilyMemberSpec =
        canonical_shared_family_member_spec("future_swap_v1", None);
    const FUTURE_MEMBERS: &[FamilyMemberSpec] = &[FUTURE_MEMBER];
    const FUTURE_FAMILY: FamilyRuntimeSpec =
        canonical_shared_family_runtime_spec!("future_swap", FUTURE_MEMBERS, None);

    #[test]
    fn canonical_shared_family_runtime_spec_derives_identity_fields() {
        assert_eq!(FUTURE_FAMILY.family_name(), "future_swap");
        assert_eq!(FUTURE_FAMILY.output_module(), "map_future_swap_family_protocol_changes");
        assert_eq!(FUTURE_FAMILY.shared_stream_name(), "future_swap_family");
        assert_eq!(FUTURE_FAMILY.durability_scope(), "family::future_swap");
    }

    #[test]
    fn canonical_pool_list_shared_family_member_spec_builds_bootstrap_member() {
        let member = canonical_pool_list_shared_family_member_spec(
            "future_swap_v1",
            BootstrapStrategy::UniswapV2Rpc,
            noop_materialize_branch,
        );

        assert_eq!(member.protocol_system, "future_swap_v1");
        assert_eq!(member.shared_route_protocols, &[] as &[&'static str]);
        assert_eq!(
            member
                .shared_bootstrap
                .expect("pool-list helper should declare shared bootstrap runtime")
                .strategy,
            BootstrapStrategy::UniswapV2Rpc
        );
    }
}
