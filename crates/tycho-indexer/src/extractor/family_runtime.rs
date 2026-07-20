use std::collections::HashMap;

#[cfg(test)]
use crate::extractor::family_registry::default_family_runtime_registry;
use crate::extractor::{
    extractor_config::ExtractorConfig,
    family_runtime_types::{FamilyRuntimeBuildPlan, ResolvedFamilyRuntimePlan},
    runtime_target_planning::{
        ResolvedRuntimeTargets as PlannedResolvedRuntimeTargets,
    },
    ExtractionError,
};

pub use crate::extractor::family_bootstrap_registry::{
    MaterializeBootstrapBranchFn, MaterializeBootstrapPlanFn, ParseBootstrapParamsFn,
    ResolvedSharedBootstrapExecution, SharedBootstrapMemberRuntime,
    SharedBootstrapParamsParser, SharedFamilyBootstrapRuntime,
};
pub use crate::extractor::family_registry::{
    FamilyMemberSpec, FamilyRuntimeRegistry, FamilyRuntimeSpec,
};
pub use crate::extractor::family_runtime_metadata::{
    canonicalize_shared_route_protocol, FamilyRuntimeConfig, FamilySharedRuntimeMetadata,
    FamilySharedStreamIdentity, ResolvedFamilyRuntimeMetadata, ResolvedSharedFamilyStream,
    SharedStreamTarget,
};
pub use crate::extractor::family_runtime_types::{
    DetectedFamilyRuntime, FamilyRuntimeMembership, FamilyRuntimeMembershipView,
    ResolvedFamilyRuntime,
};

pub use crate::extractor::managed_substreams_request::PreparedSubstreamsRequest;
pub use crate::extractor::runtime_target_planning::{
    ResolvedInitializedAccountsRequest, ResolvedRuntimeTarget, ResolvedRuntimeTargetSelector,
    ResolvedRuntimeTargets, ResolvedStandaloneRuntime, ResolvedSubstreamsExecutionRequest,
};

pub fn resolve_runtime_targets_with_registry<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<PlannedResolvedRuntimeTargets<'a>, ExtractionError> {
    registry.resolve_runtime_targets(extractors)
}

#[cfg(test)]
pub fn resolve_runtime_targets<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<PlannedResolvedRuntimeTargets<'a>, ExtractionError> {
    resolve_runtime_targets_with_registry(extractors, default_family_runtime_registry())
}

pub fn build_family_runtime_plan_via_registry(
    extractors: &HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<FamilyRuntimeBuildPlan, ExtractionError> {
    registry.build_family_runtime_plan(extractors)
}

pub fn build_resolved_family_runtime_plan_via_registry<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilyRuntimePlan<'a>, ExtractionError> {
    registry.build_resolved_family_runtime_plan(extractors)
}

#[cfg(test)]
pub(crate) fn build_family_runtime_plan(
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<FamilyRuntimeBuildPlan, ExtractionError> {
    build_family_runtime_plan_via_registry(extractors, default_family_runtime_registry())
}

#[cfg(test)]
pub(crate) fn build_resolved_family_runtime_plan<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<ResolvedFamilyRuntimePlan<'a>, ExtractionError> {
    build_resolved_family_runtime_plan_via_registry(extractors, default_family_runtime_registry())
}

#[cfg(test)]
pub(crate) fn resolved_family_runtime_from_extractor_configs_for_tests<'a>(
    extractor_configs: &[&'a ExtractorConfig],
    shared_spkg: &str,
) -> Result<ResolvedFamilyRuntime<'a>, ExtractionError> {
    let registry = default_family_runtime_registry();
    let family_name = registry
        .require_family_name_for_protocol_systems(
            extractor_configs.iter().map(|config| config.protocol_system()),
            "family runtime test helper",
        )?
        .to_string();
    let extractors = extractor_configs
        .iter()
        .map(|config| {
            let mut cloned = (*config).clone();
            cloned.family_runtime =
                Some(crate::testing::family_runtime_config_for_tests(&family_name, shared_spkg));
            (cloned.protocol_system().to_string(), cloned)
        })
        .collect::<HashMap<_, _>>();
    let mut detected =
        crate::extractor::family_runtime_detection::detect_family_runtimes_with_registry(
            &extractors, registry,
        )?
        .into_iter()
        .filter(|family| family.family_name() == family_name)
        .collect::<Vec<_>>();
    let family = match detected.len() {
        1 => detected.remove(0),
        0 => {
            return Err(ExtractionError::Setup(format!(
                "family runtime test helper could not detect family runtime `{family_name}`"
            )))
        }
        many => {
            return Err(ExtractionError::Setup(format!(
                "family runtime test helper expected exactly one detected family runtime `{family_name}`, found {many}"
            )))
        }
    };
    let execution = crate::extractor::family_runtime_resolution::resolve_resolved_family_execution_config(
        &family,
        extractor_configs,
        registry,
    )?;
    let shared_runtime = crate::extractor::family_runtime_resolution::resolve_resolved_family_shared_runtime(
        &family,
        &execution.runtime_contract,
        extractor_configs,
        registry,
    )?;

    Ok(ResolvedFamilyRuntime {
        family: family.runtime_membership(),
        extractor_configs: extractor_configs.to_vec(),
        shared_runtime,
        execution,
    })
}
