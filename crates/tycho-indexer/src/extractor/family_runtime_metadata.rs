use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tycho_common::models::Chain;

use crate::extractor::{
    extractor_config::ExtractorConfig,
    family_bootstrap_registry::SharedBootstrapMemberRuntime,
    family_registry::{FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec},
    protocol_message_registry::{AuxiliaryProtocolMessageDecoder, AuxiliaryProtocolStateHydrator},
    ExtractionError,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedSharedFamilyStream {
    pub spkg: String,
    pub module: String,
    pub extractor_id: String,
    pub durability_scope: String,
}

pub struct FamilySharedStreamIdentity<'a> {
    pub output_module: &'a str,
    pub shared_stream_name: &'a str,
    pub extractor_id: String,
    pub durability_scope: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FamilySharedRuntimeMetadata<'a> {
    pub output_module: &'a str,
    pub shared_stream_name: &'a str,
    pub durability_scope: &'a str,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct FamilyRuntimeConfig {
    pub family: String,
    #[serde(default)]
    pub shared_spkg: Option<String>,
    #[serde(default)]
    pub shared_module: Option<String>,
    #[serde(default)]
    pub durability_scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedStreamTarget {
    pub spkg: String,
    pub module: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFamilyRuntimeMetadata {
    pub family: String,
    pub shared_stream: SharedStreamTarget,
    pub durability_scope: String,
}

impl FamilyRuntimeConfig {
    pub fn shared_spkg(&self) -> Option<&str> {
        self.shared_spkg.as_deref()
    }

    pub fn shared_module(&self) -> Option<&str> {
        self.shared_module.as_deref()
    }

    pub fn durability_scope(&self) -> Option<&str> {
        self.durability_scope.as_deref()
    }
}

pub fn canonicalize_shared_route_protocol(protocol: &str) -> String {
    protocol
        .chars()
        .filter(|char| char.is_ascii_alphanumeric())
        .flat_map(|char| char.to_lowercase())
        .collect()
}

pub(crate) fn normalized_shared_route_protocols_for_member(
    member: &FamilyMemberSpec,
) -> HashSet<String> {
    if member.shared_route_protocols.is_empty() {
        HashSet::from([canonicalize_shared_route_protocol(member.protocol_system)])
    } else {
        member
            .shared_route_protocols
            .iter()
            .map(|protocol| canonicalize_shared_route_protocol(protocol))
            .collect()
    }
}

impl ExtractorConfig {
    pub fn family_runtime(&self) -> Option<&FamilyRuntimeConfig> {
        self.family_runtime.as_ref()
    }

    pub fn resolve_family_runtime_metadata(
        &self,
        registry: Option<FamilyRuntimeRegistry<'_>>,
    ) -> Result<Option<ResolvedFamilyRuntimeMetadata>, ExtractionError> {
        match self.family_runtime() {
            Some(runtime) => {
                let spkg = runtime.shared_spkg().ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "extractor `{}` uses family runtime `{}` without a resolved shared_spkg",
                        self.name(),
                        runtime.family
                    ))
                })?;

                let registry_metadata = registry.and_then(|registry| {
                    registry.shared_runtime_metadata_for_family(&runtime.family)
                });

                let module = runtime
                    .shared_module()
                    .or_else(|| registry_metadata.map(|metadata| metadata.output_module))
                    .ok_or_else(|| {
                        ExtractionError::Setup(format!(
                            "extractor `{}` uses family runtime `{}` without a resolved shared_module",
                            self.name(),
                            runtime.family
                        ))
                    })?;

                let durability_scope = runtime
                    .durability_scope()
                    .or_else(|| registry_metadata.map(|metadata| metadata.durability_scope))
                    .ok_or_else(|| {
                        ExtractionError::Setup(format!(
                            "extractor `{}` uses family runtime `{}` without a resolved durability_scope",
                            self.name(),
                            runtime.family
                        ))
                    })?;

                Ok(Some(ResolvedFamilyRuntimeMetadata {
                    family: runtime.family.clone(),
                    shared_stream: SharedStreamTarget {
                        spkg: spkg.to_string(),
                        module: module.to_string(),
                    },
                    durability_scope: durability_scope.to_string(),
                }))
            }
            None => Ok(None),
        }
    }

    pub fn require_resolved_family_runtime_metadata(
        &self,
    ) -> Result<Option<ResolvedFamilyRuntimeMetadata>, ExtractionError> {
        self.resolve_family_runtime_metadata(None)
    }

    pub fn with_family_runtime(mut self, family_runtime: Option<FamilyRuntimeConfig>) -> Self {
        self.family_runtime = family_runtime;
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RegisteredProtocolSystemDefaults<'a> {
    family_name: &'a str,
    member_spec: &'a FamilyMemberSpec,
    auxiliary_protocol_message_decoders: Vec<AuxiliaryProtocolMessageDecoder>,
    auxiliary_protocol_state_hydrators: Vec<AuxiliaryProtocolStateHydrator>,
}

impl<'a> RegisteredProtocolSystemDefaults<'a> {
    pub(crate) fn family_name(&self) -> &'a str {
        self.family_name
    }

    pub(crate) fn member_spec(&self) -> &'a FamilyMemberSpec {
        self.member_spec
    }

    pub(crate) fn shared_route_protocols(&self) -> &'a [&'static str] {
        self.member_spec.shared_route_protocols
    }

    pub(crate) fn normalized_shared_route_protocols(&self) -> HashSet<String> {
        normalized_shared_route_protocols_for_member(self.member_spec)
    }

    pub(crate) fn auxiliary_protocol_message_decoders(&self) -> &[AuxiliaryProtocolMessageDecoder] {
        &self.auxiliary_protocol_message_decoders
    }

    pub(crate) fn auxiliary_protocol_state_hydrators(&self) -> &[AuxiliaryProtocolStateHydrator] {
        &self.auxiliary_protocol_state_hydrators
    }

    pub(crate) fn shared_bootstrap(&self) -> Option<SharedBootstrapMemberRuntime> {
        self.member_spec.shared_bootstrap
    }
}

impl<'a> FamilyRuntimeRegistry<'a> {
    pub fn family_spec_by_name(&self, family_name: &str) -> Option<&'a FamilyRuntimeSpec> {
        self.specs()
            .iter()
            .find(|spec| spec.family_name() == family_name)
    }

    pub fn shared_runtime_metadata_for_family(
        &self,
        family_name: &str,
    ) -> Option<FamilySharedRuntimeMetadata<'a>> {
        let spec = self.family_spec_by_name(family_name)?;
        Some(FamilySharedRuntimeMetadata {
            output_module: spec.output_module(),
            shared_stream_name: spec.shared_stream_name(),
            durability_scope: spec.durability_scope(),
        })
    }

    pub fn shared_runtime_metadata_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<FamilySharedRuntimeMetadata<'a>> {
        self.family_name_for_protocol_system(protocol_system)
            .and_then(|family_name| self.shared_runtime_metadata_for_family(family_name))
    }

    pub fn output_module_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_family(family_name)
            .map(|metadata| metadata.output_module)
    }

    pub fn shared_stream_name_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_family(family_name)
            .map(|metadata| metadata.shared_stream_name)
    }

    pub fn member_protocol_systems_for_family(&self, family_name: &str) -> Option<Vec<&'a str>> {
        self.family_spec_by_name(family_name)
            .map(|spec| {
                spec.members()
                    .iter()
                    .map(|member| member.protocol_system)
                    .collect()
            })
    }

    pub fn durability_scope_for_family(&self, family_name: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_family(family_name)
            .map(|metadata| metadata.durability_scope)
    }

    pub fn require_shared_runtime_metadata_for_family(
        &self,
        family_name: &str,
        context: &str,
    ) -> Result<FamilySharedRuntimeMetadata<'a>, ExtractionError> {
        self.shared_runtime_metadata_for_family(family_name)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "{context} `{family_name}` does not match any registered family runtime"
                ))
            })
    }

    pub fn shared_stream_identity_for_family(
        &self,
        chain: Chain,
        family_name: &str,
    ) -> Option<FamilySharedStreamIdentity<'a>> {
        let metadata = self.shared_runtime_metadata_for_family(family_name)?;
        Some(FamilySharedStreamIdentity {
            output_module: metadata.output_module,
            shared_stream_name: metadata.shared_stream_name,
            extractor_id: format!("{chain}:{}", metadata.shared_stream_name),
            durability_scope: metadata.durability_scope,
        })
    }

    pub fn resolved_shared_stream_for_family(
        &self,
        chain: Chain,
        family_name: &str,
        shared_spkg: impl Into<String>,
    ) -> Result<ResolvedSharedFamilyStream, ExtractionError> {
        self.require_family_spec(family_name, "family runtime")?;
        let identity = self
            .shared_stream_identity_for_family(chain, family_name)
            .expect("required family spec must expose shared stream identity");
        Ok(ResolvedSharedFamilyStream {
            spkg: shared_spkg.into(),
            module: identity.output_module.to_string(),
            extractor_id: identity.extractor_id,
            durability_scope: identity.durability_scope.to_string(),
        })
    }

    pub fn output_module_for_protocol_system(&self, protocol_system: &str) -> Option<&'a str> {
        self.shared_runtime_metadata_for_protocol_system(protocol_system)
            .map(|metadata| metadata.output_module)
    }

    pub fn require_family_spec(
        &self,
        family_name: &str,
        context: &str,
    ) -> Result<&'a FamilyRuntimeSpec, ExtractionError> {
        self.family_spec_by_name(family_name)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "{context} `{family_name}` does not match any registered family runtime"
                ))
            })
    }

    pub(crate) fn registered_protocol_system_defaults(
        &self,
        protocol_system: &str,
    ) -> Option<RegisteredProtocolSystemDefaults<'a>> {
        self.specs().iter().find_map(|spec| {
            spec.members()
                .iter()
                .find(|member| member.protocol_system == protocol_system)
                .map(|member| RegisteredProtocolSystemDefaults {
                    family_name: spec.family_name(),
                    member_spec: member,
                    auxiliary_protocol_message_decoders: if member
                        .auxiliary_protocol_message_decoders()
                        .is_empty()
                    {
                        spec.auxiliary_protocol_message_decoders()
                            .iter()
                            .copied()
                            .filter(|decoder| decoder.protocol_system == protocol_system)
                            .collect()
                    } else {
                        member
                            .auxiliary_protocol_message_decoders()
                            .to_vec()
                    },
                    auxiliary_protocol_state_hydrators: if member
                        .auxiliary_protocol_state_hydrators()
                        .is_empty()
                    {
                        spec.auxiliary_protocol_state_hydrators()
                            .iter()
                            .copied()
                            .filter(|hydrator| hydrator.protocol_system == protocol_system)
                            .collect()
                    } else {
                        member
                            .auxiliary_protocol_state_hydrators()
                            .to_vec()
                    },
                })
        })
    }

    pub(crate) fn require_registered_protocol_system_defaults(
        &self,
        protocol_system: &str,
        context: &str,
    ) -> Result<RegisteredProtocolSystemDefaults<'a>, ExtractionError> {
        self.registered_protocol_system_defaults(protocol_system)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "{context} could not resolve registered protocol defaults for `{protocol_system}`"
                ))
            })
    }

    pub fn member_spec_by_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<&'a FamilyMemberSpec> {
        self.registered_protocol_system_defaults(protocol_system)
            .map(|defaults| defaults.member_spec())
    }

    pub fn member_spec_for_family(
        &self,
        family_name: &str,
        protocol_system: &str,
    ) -> Option<&'a FamilyMemberSpec> {
        self.family_spec_by_name(family_name)
            .into_iter()
            .flat_map(|spec| spec.members().iter())
            .find(|member| member.protocol_system == protocol_system)
    }

    pub fn require_member_spec_for_family(
        &self,
        family_name: &str,
        protocol_system: &str,
        context: &str,
    ) -> Result<&'a FamilyMemberSpec, ExtractionError> {
        self.require_family_spec(family_name, context)?;
        self.member_spec_for_family(family_name, protocol_system)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "{context} `{family_name}` cannot be applied to protocol system `{protocol_system}` because that protocol is not a declared member of the family"
                ))
            })
    }

    pub fn family_name_for_protocol_system(&self, protocol_system: &str) -> Option<&'a str> {
        self.registered_protocol_system_defaults(protocol_system)
            .map(|defaults| defaults.family_name())
    }

    #[cfg(test)]
    pub(crate) fn auxiliary_protocol_message_decoders_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<Vec<AuxiliaryProtocolMessageDecoder>> {
        self.registered_protocol_system_defaults(protocol_system)
            .map(|defaults| {
                defaults
                    .auxiliary_protocol_message_decoders()
                    .to_vec()
            })
    }

    #[cfg(test)]
    pub(crate) fn auxiliary_protocol_state_hydrators_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<Vec<AuxiliaryProtocolStateHydrator>> {
        self.registered_protocol_system_defaults(protocol_system)
            .map(|defaults| {
                defaults
                    .auxiliary_protocol_state_hydrators()
                    .to_vec()
            })
    }

    pub fn require_family_name_for_protocol_systems<'b>(
        &self,
        protocol_systems: impl IntoIterator<Item = &'b str>,
        context: &str,
    ) -> Result<&'a str, ExtractionError> {
        let mut family_name = None;

        for protocol_system in protocol_systems {
            let candidate = self
                .family_name_for_protocol_system(protocol_system)
                .ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "{context} could not resolve a registered family for protocol system `{protocol_system}`"
                    ))
                })?;

            if let Some(existing) = family_name {
                if existing != candidate {
                    return Err(ExtractionError::Setup(format!(
                        "{context} requires one registered family, found `{existing}` and `{candidate}`"
                    )));
                }
            } else {
                family_name = Some(candidate);
            }
        }

        family_name.ok_or_else(|| {
            ExtractionError::Setup(format!("{context} requires at least one protocol system"))
        })
    }

    pub fn shared_route_protocols_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<&'a [&'static str]> {
        self.registered_protocol_system_defaults(protocol_system)
            .map(|defaults| defaults.shared_route_protocols())
    }

    pub fn normalized_shared_route_protocol_filter_for_protocol_system(
        &self,
        protocol_system: &str,
    ) -> Option<HashSet<String>> {
        self.registered_protocol_system_defaults(protocol_system)
            .map(|defaults| defaults.normalized_shared_route_protocols())
    }

    pub fn validate_family_runtime_config(
        &self,
        protocol_system: &str,
        family_runtime: &FamilyRuntimeConfig,
    ) -> Result<&'a FamilyMemberSpec, ExtractionError> {
        self.require_member_spec_for_family(
            &family_runtime.family,
            protocol_system,
            "family_runtime",
        )
    }

    pub fn resolve_family_runtime_config(
        &self,
        protocol_system: &str,
        mut family_runtime: FamilyRuntimeConfig,
        shared_spkg: Option<String>,
        shared_module: Option<String>,
        durability_scope: Option<String>,
    ) -> Result<FamilyRuntimeConfig, ExtractionError> {
        self.validate_family_runtime_config(protocol_system, &family_runtime)?;
        let shared_metadata = self
            .require_shared_runtime_metadata_for_family(&family_runtime.family, "family_runtime")?;

        if family_runtime.shared_spkg.is_none() {
            family_runtime.shared_spkg = shared_spkg;
        }
        if family_runtime.shared_module.is_none() {
            family_runtime.shared_module = shared_module.or_else(|| {
                Some(
                    shared_metadata
                        .output_module
                        .to_string(),
                )
            });
        }
        if family_runtime
            .durability_scope
            .is_none()
        {
            family_runtime.durability_scope = durability_scope.or_else(|| {
                Some(
                    shared_metadata
                        .durability_scope
                        .to_string(),
                )
            });
        }

        if family_runtime.shared_spkg.is_none() {
            return Err(ExtractionError::Setup(format!(
                "family_runtime `{}` must resolve `shared_spkg` either inline or via top-level family_runtimes",
                family_runtime.family
            )));
        }

        Ok(family_runtime)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, future::Future, pin::Pin};

    use tycho_common::models::Chain;
    use tycho_ethereum::rpc::EthereumRpcClient;

    use super::*;
    use crate::extractor::{
        family_bootstrap_registry::SharedBootstrapParamsParser,
        family_registry::{
            canonical_shared_family_runtime_spec, default_family_runtime_registry,
            shared_family_member_spec, shared_family_member_with_bootstrap,
            shared_family_runtime_spec, FamilyRuntimeRegistry, FamilyRuntimeSpec,
        },
        models::BlockChanges,
        protocol_message_registry::{
            AuxiliaryProtocolMessageBuildFuture, AuxiliaryProtocolMessageContext,
        },
        shared_bootstrap::BootstrapBranchDescriptor,
        ExtractionError,
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

    fn uniswap_shared_stream(shared_spkg: &str) -> ResolvedSharedFamilyStream {
        default_family_runtime_registry()
            .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", shared_spkg)
            .expect("registered uniswap shared stream")
    }

    #[test]
    fn registry_resolves_family_name_for_protocol_system() {
        let registry = default_family_runtime_registry();

        assert_eq!(registry.family_name_for_protocol_system("uniswap_v2"), Some("uniswap"));
        assert_eq!(registry.family_name_for_protocol_system("uniswap_v3"), Some("uniswap"));
        assert_eq!(registry.family_name_for_protocol_system("curve"), None);
    }

    #[test]
    fn registry_exposes_registered_protocol_system_defaults() {
        let registry = default_family_runtime_registry();
        let defaults = registry
            .registered_protocol_system_defaults("uniswap_v3")
            .expect("uniswap_v3 registered defaults");

        assert_eq!(defaults.family_name(), "uniswap");
        assert_eq!(defaults.shared_route_protocols(), &[] as &[&'static str]);
        assert_eq!(
            defaults.normalized_shared_route_protocols(),
            HashSet::from([String::from("uniswapv3")])
        );
        assert_eq!(
            defaults
                .shared_bootstrap()
                .expect("uniswap_v3 shared bootstrap runtime")
                .strategy,
            crate::extractor::extractor_config::BootstrapStrategy::UniswapV3Rpc
        );
        assert_eq!(
            defaults
                .auxiliary_protocol_message_decoders()
                .len(),
            1
        );
        assert_eq!(defaults.auxiliary_protocol_message_decoders()[0].protocol_system, "uniswap_v3");

        assert!(registry
            .registered_protocol_system_defaults("curve")
            .is_none());
    }

    #[test]
    fn registry_requires_single_registered_family_for_protocol_systems() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry
                .require_family_name_for_protocol_systems(
                    ["uniswap_v2", "uniswap_v3"],
                    "family resolution test",
                )
                .expect("uniswap members should resolve one family"),
            "uniswap"
        );
    }

    #[test]
    fn registry_rejects_unknown_protocol_system_when_requiring_family_name() {
        let registry = default_family_runtime_registry();

        let err = registry
            .require_family_name_for_protocol_systems(["curve"], "family resolution test")
            .expect_err("unknown protocol system should fail family resolution");

        assert!(err
            .to_string()
            .contains("could not resolve a registered family for protocol system `curve`"));
    }

    #[test]
    fn registry_rejects_mixed_registered_families_when_requiring_family_name() {
        const OTHER_MEMBER: FamilyMemberSpec = shared_family_member_spec("other_v1", &[], None);
        const OTHER_MEMBERS: &[FamilyMemberSpec] = &[OTHER_MEMBER];
        const OTHER_FAMILY: FamilyRuntimeSpec =
            canonical_shared_family_runtime_spec!("other_swap", OTHER_MEMBERS, None);
        let mut specs = default_family_runtime_registry()
            .specs()
            .to_vec();
        specs.push(OTHER_FAMILY);

        let registry = FamilyRuntimeRegistry::new(&specs);
        let err = registry
            .require_family_name_for_protocol_systems(
                ["uniswap_v2", "other_v1"],
                "family resolution test",
            )
            .expect_err("mixed protocol systems should fail family resolution");

        assert!(err
            .to_string()
            .contains("requires one registered family, found `uniswap` and `other_swap`"));
    }

    #[test]
    fn registry_exposes_output_module_for_family_and_protocol_system() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-registry-output-module.spkg");

        assert_eq!(
            registry.output_module_for_family("uniswap"),
            Some(expected_shared_stream.module.as_str())
        );
        assert_eq!(
            registry.output_module_for_protocol_system("uniswap_v2"),
            Some(expected_shared_stream.module.as_str())
        );
        assert_eq!(
            registry.output_module_for_protocol_system("uniswap_v3"),
            Some(expected_shared_stream.module.as_str())
        );
        assert_eq!(registry.output_module_for_family("curve"), None);
        assert_eq!(registry.output_module_for_protocol_system("curve"), None);
    }

    #[test]
    fn registry_exposes_shared_runtime_metadata_for_family() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-shared-runtime-metadata.spkg");
        let metadata = registry
            .shared_runtime_metadata_for_family("uniswap")
            .expect("uniswap shared runtime metadata");

        assert_eq!(metadata.output_module, expected_shared_stream.module);
        assert_eq!(metadata.shared_stream_name, "uniswap_family");
        assert_eq!(metadata.durability_scope, expected_shared_stream.durability_scope);
        assert_eq!(registry.shared_runtime_metadata_for_family("curve"), None);
    }

    #[test]
    fn registry_exposes_shared_runtime_metadata_for_protocol_system() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-shared-runtime-metadata-by-protocol.spkg");
        let metadata = registry
            .shared_runtime_metadata_for_protocol_system("uniswap_v2")
            .expect("uniswap_v2 shared runtime metadata");

        assert_eq!(metadata.output_module, expected_shared_stream.module);
        assert_eq!(metadata.shared_stream_name, "uniswap_family");
        assert_eq!(metadata.durability_scope, expected_shared_stream.durability_scope);
        assert_eq!(registry.shared_runtime_metadata_for_protocol_system("curve"), None);
    }

    #[test]
    fn registry_exposes_member_protocol_systems_for_family() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry.member_protocol_systems_for_family("uniswap"),
            Some(vec!["uniswap_v2", "uniswap_v3"])
        );
        assert_eq!(registry.member_protocol_systems_for_family("curve"), None);
    }

    #[test]
    fn registry_exposes_shared_stream_identity_for_family() {
        let registry = default_family_runtime_registry();
        let expected_shared_stream =
            uniswap_shared_stream("/tmp/uniswap-shared-stream-identity.spkg");
        let identity = registry
            .shared_stream_identity_for_family(Chain::Ethereum, "uniswap")
            .expect("uniswap family stream identity");

        assert_eq!(
            registry.shared_stream_name_for_family("uniswap"),
            Some(identity.shared_stream_name)
        );
        assert_eq!(
            registry.durability_scope_for_family("uniswap"),
            Some(
                expected_shared_stream
                    .durability_scope
                    .as_str()
            )
        );
        assert_eq!(registry.shared_stream_name_for_family("curve"), None);
        assert_eq!(registry.durability_scope_for_family("curve"), None);
        assert_eq!(identity.output_module, expected_shared_stream.module);
        assert_eq!(identity.extractor_id, expected_shared_stream.extractor_id);
        assert_eq!(identity.durability_scope, expected_shared_stream.durability_scope);
        assert!(registry
            .shared_stream_identity_for_family(Chain::Ethereum, "curve")
            .is_none());
    }

    #[test]
    fn registry_validates_family_runtime_membership_with_shared_error_surface() {
        let registry = default_family_runtime_registry();

        let member = registry
            .validate_family_runtime_config(
                "uniswap_v2",
                &FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
            )
            .expect("uniswap v2 belongs to uniswap family");

        assert_eq!(member.protocol_system, "uniswap_v2");

        let err = registry
            .validate_family_runtime_config(
                "curve",
                &FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
            )
            .expect_err("curve should not belong to uniswap family");
        assert!(err
            .to_string()
            .contains("family_runtime `uniswap` cannot be applied to protocol system `curve`"));
    }

    #[test]
    fn registry_resolves_family_runtime_config_with_top_level_defaults() {
        let registry = default_family_runtime_registry();
        let shared_stream = registry
            .resolved_shared_stream_for_family(
                Chain::Ethereum,
                "uniswap",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            )
            .expect("registered uniswap shared stream");

        let resolved = registry
            .resolve_family_runtime_config(
                "uniswap_v2",
                FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
                Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string()),
                Some(shared_stream.module.clone()),
                None,
            )
            .expect("family runtime defaults should resolve");

        assert_eq!(resolved.family, "uniswap");
        assert_eq!(
            resolved.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(resolved.shared_module.as_deref(), Some(shared_stream.module.as_str()));
        assert_eq!(
            resolved.durability_scope.as_deref(),
            Some(shared_stream.durability_scope.as_str())
        );
    }

    #[test]
    fn registry_resolves_family_runtime_config_with_registry_output_module_default() {
        let registry = default_family_runtime_registry();
        let shared_stream = registry
            .resolved_shared_stream_for_family(
                Chain::Ethereum,
                "uniswap",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            )
            .expect("registered uniswap shared stream");

        let resolved = registry
            .resolve_family_runtime_config(
                "uniswap_v2",
                FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: None,
                    shared_module: None,
                    durability_scope: None,
                },
                Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string()),
                None,
                None,
            )
            .expect("family runtime should inherit output module from registry");

        assert_eq!(resolved.family, "uniswap");
        assert_eq!(
            resolved.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(resolved.shared_module.as_deref(), Some(shared_stream.module.as_str()));
        assert_eq!(
            resolved.durability_scope.as_deref(),
            Some(shared_stream.durability_scope.as_str())
        );
    }

    #[test]
    fn registry_resolves_family_runtime_config_with_registry_durability_scope_default() {
        let registry = crate::extractor::test_support::future_family_runtime_registry_for_tests(
            &["future_v1"],
            "family::future_swap_runtime",
        );

        let resolved = registry
            .resolve_family_runtime_config(
                "future_v1",
                FamilyRuntimeConfig {
                    family: "future_swap".to_string(),
                    shared_spkg: Some(
                        "protocols/substreams/future-swap-combined/test.spkg".to_string(),
                    ),
                    shared_module: None,
                    durability_scope: None,
                },
                None,
                None,
                None,
            )
            .expect("family runtime should inherit durability scope from registry");

        assert_eq!(resolved.durability_scope.as_deref(), Some("family::future_swap_runtime"));
    }

    #[test]
    fn registry_exposes_normalized_shared_route_protocol_filter() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry.normalized_shared_route_protocol_filter_for_protocol_system("uniswap_v2"),
            Some(HashSet::from(["uniswapv2".to_string()]))
        );
        assert_eq!(
            registry.normalized_shared_route_protocol_filter_for_protocol_system("uniswap_v3"),
            Some(HashSet::from(["uniswapv3".to_string()]))
        );
        assert_eq!(
            registry.normalized_shared_route_protocol_filter_for_protocol_system("curve"),
            None
        );
        assert_eq!(canonicalize_shared_route_protocol("Uniswap-V3"), "uniswapv3".to_string());
    }

    #[test]
    fn registry_defaults_bootstrap_member_route_aliases_from_protocol_system() {
        const BROKEN_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "broken_family",
            &[shared_family_member_with_bootstrap(
                "broken_protocol_v2",
                &[],
                crate::extractor::extractor_config::BootstrapStrategy::UniswapV2Rpc,
                SharedBootstrapParamsParser::PoolList,
                noop_materialize_branch,
            )],
            "map_broken_family",
            "broken_family_stream",
            "family::broken_family",
            None,
        );
        let registry = FamilyRuntimeRegistry::new(&[BROKEN_FAMILY]);

        registry
            .validate()
            .expect("bootstrap-capable member without explicit route aliases should default from protocol system");
        assert_eq!(
            registry
                .normalized_shared_route_protocol_filter_for_protocol_system("broken_protocol_v2"),
            Some(HashSet::from(["brokenprotocolv2".to_string()]))
        );
    }

    #[test]
    fn registry_resolves_member_within_specific_family() {
        let registry = default_family_runtime_registry();

        let member = registry
            .member_spec_for_family("uniswap", "uniswap_v2")
            .expect("member in family");

        assert_eq!(member.protocol_system, "uniswap_v2");
        assert!(registry
            .member_spec_for_family("future_swap", "uniswap_v2")
            .is_none());
        assert!(registry
            .member_spec_for_family("uniswap", "future_v1")
            .is_none());
    }

    #[test]
    fn auxiliary_decoder_lookup_can_use_custom_runtime_registry() {
        fn build_future_events<'a>(
            _context: &'a dyn AuxiliaryProtocolMessageContext,
            _value: &'a [u8],
            _finalized_block_height: u64,
            _partial_block_index: Option<u32>,
        ) -> AuxiliaryProtocolMessageBuildFuture<'a> {
            Box::pin(async {
                Err(ExtractionError::Unknown("test-only decoder should not run".to_string()))
            })
        }

        const FUTURE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
            &[AuxiliaryProtocolMessageDecoder {
                protocol_system: "future_v1",
                type_url_suffix: "FutureEvents",
                build_block_changes: build_future_events,
            }];
        let registry = crate::extractor::test_support::future_family_runtime_registry_with_member_auxiliary_decoders_for_tests(
            &["future_v1"],
            "future_v1",
            "family::future_swap_runtime",
            FUTURE_DECODERS,
        );

        let decoders = registry
            .auxiliary_protocol_message_decoders_for_protocol_system("future_v1")
            .expect("future_v1 decoders");
        assert_eq!(decoders.len(), 1);
        assert_eq!(decoders[0].protocol_system, "future_v1");
        assert_eq!(decoders[0].type_url_suffix, "FutureEvents");
        assert!(registry
            .auxiliary_protocol_message_decoders_for_protocol_system("uniswap_v3")
            .is_none());
    }

    #[test]
    fn builtin_member_defaults_only_expose_protocol_scoped_auxiliary_hooks() {
        let registry = default_family_runtime_registry();

        assert_eq!(
            registry
                .auxiliary_protocol_message_decoders_for_protocol_system("uniswap_v2")
                .map(|decoders| decoders.len()),
            Some(0)
        );
        assert_eq!(
            registry
                .auxiliary_protocol_state_hydrators_for_protocol_system("uniswap_v2")
                .map(|hydrators| hydrators.len()),
            Some(0)
        );
        assert_eq!(
            registry
                .auxiliary_protocol_message_decoders_for_protocol_system("uniswap_v3")
                .map(|decoders| decoders.len()),
            Some(1)
        );
        assert_eq!(
            registry
                .auxiliary_protocol_state_hydrators_for_protocol_system("uniswap_v3")
                .map(|hydrators| hydrators.len()),
            Some(1)
        );
    }
}
