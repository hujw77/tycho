use crate::extractor::{
    extractor_config::{
        configured_stream_start_block, extractor_config_by_protocol_system, ExtractorConfig,
    },
    family_bootstrap_registry::ResolvedSharedBootstrapRuntime,
    family_dispatch::FamilyBranchSpec,
    family_registry::FamilyRuntimeRegistry,
    family_runtime_types::{
        DetectedFamilyRuntime, FamilyRuntimeBuildPlan, FamilyRuntimeMembershipView,
        ResolvedFamilyRuntime, ResolvedFamilyRuntimePlan,
    },
    protocol_message_registry::ProtocolSystemAuxiliaryRuntimeHooks,
    runtime_target_planning::PlannedSubstreamsRequestTemplate,
    family_runtime_metadata::ResolvedSharedFamilyStream,
    shared_bootstrap::validate_shared_bootstrap_plan_against_runtime_contract,
    ExtractionError,
};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FamilySharedSubstreamsParams {
    params: HashMap<String, String>,
}

impl FamilySharedSubstreamsParams {
    fn merge_from_extractor(
        merged: &mut HashMap<String, String>,
        config: &ExtractorConfig,
    ) -> Result<(), ExtractionError> {
        for (key, value) in &config.substreams_params {
            if let Some(existing) = merged.get(key) {
                if existing != value {
                    return Err(ExtractionError::Setup(format!(
                        "conflicting substreams param `{key}` while building family runner for `{}`",
                        config.name()
                    )));
                }
            } else {
                merged.insert(key.clone(), value.clone());
            }
        }
        Ok(())
    }

    pub(crate) fn from_extractor_configs(
        extractor_configs: &[&ExtractorConfig],
    ) -> Result<Self, ExtractionError> {
        let mut merged = HashMap::new();

        for config in extractor_configs {
            Self::merge_from_extractor(&mut merged, config)?;
        }

        Ok(Self { params: merged })
    }

    pub(crate) fn as_map(&self) -> &HashMap<String, String> {
        &self.params
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FamilySharedStreamSettings {
    merged_substreams_params: FamilySharedSubstreamsParams,
    stop_block: u64,
    configured_start_block: i64,
}

impl FamilySharedStreamSettings {
    pub(crate) fn from_extractor_configs(
        family_name: &str,
        extractor_configs: &[&ExtractorConfig],
    ) -> Result<Self, ExtractionError> {
        let merged_substreams_params =
            FamilySharedSubstreamsParams::from_extractor_configs(extractor_configs).map_err(
                |err| match err {
                    ExtractionError::Setup(message) => ExtractionError::Setup(format!(
                        "family `{}` has incompatible shared substreams params: {message}",
                        family_name
                    )),
                    other => other,
                },
            )?;

        let first_config = extractor_configs.first().ok_or_else(|| {
            ExtractionError::Setup(format!(
                "family `{}` has no extractor configs to resolve stream settings",
                family_name
            ))
        })?;

        let stop_block =
            u64::try_from(first_config.stop_block().unwrap_or(0)).map_err(|_| {
                ExtractionError::Setup(format!(
                    "family `{}` resolved stop_block exceeds u64",
                    family_name
                ))
            })?;
        let configured_start_block = configured_stream_start_block(first_config)?;

        Ok(Self { merged_substreams_params, stop_block, configured_start_block })
    }

    pub(crate) fn stop_block(&self) -> u64 {
        self.stop_block
    }

    pub(crate) fn merged_substreams_params(&self) -> &FamilySharedSubstreamsParams {
        &self.merged_substreams_params
    }

    pub(crate) fn configured_start_block(&self) -> i64 {
        self.configured_start_block
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedFamilyExecutionConfig {
    pub runtime_contract: ResolvedFamilyRuntimeContract,
    pub(crate) auxiliary_runtime_hooks_by_protocol_system:
        HashMap<String, ProtocolSystemAuxiliaryRuntimeHooks>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedFamilySharedRuntime {
    settings: FamilySharedStreamSettings,
    pub configured_start_block: i64,
    pub shared_bootstrap_runtime: Option<ResolvedSharedBootstrapRuntime>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedFamilyRuntimeContract {
    pub shared_stream: ResolvedSharedFamilyStream,
    pub branch_specs: Vec<FamilyBranchSpec>,
    pub shared_progress_owner_protocol_system: String,
}

impl ResolvedFamilySharedRuntime {
    pub(crate) fn new(
        settings: FamilySharedStreamSettings,
        configured_start_block: i64,
        shared_bootstrap_runtime: Option<ResolvedSharedBootstrapRuntime>,
    ) -> Self {
        Self { settings, configured_start_block, shared_bootstrap_runtime }
    }

    pub(crate) fn request_template(
        &self,
        shared_stream: &ResolvedSharedFamilyStream,
    ) -> PlannedSubstreamsRequestTemplate {
        PlannedSubstreamsRequestTemplate::new(
            &shared_stream.spkg,
            &shared_stream.module,
            self.stop_block(),
            self.merged_substreams_params().as_map().clone(),
            &shared_stream.extractor_id,
        )
    }

    pub(crate) fn stop_block(&self) -> u64 {
        self.settings.stop_block()
    }

    pub(crate) fn merged_substreams_params(&self) -> &FamilySharedSubstreamsParams {
        self.settings.merged_substreams_params()
    }
}

impl ResolvedFamilyRuntimeContract {
    pub fn new(
        shared_stream: ResolvedSharedFamilyStream,
        branch_specs: Vec<FamilyBranchSpec>,
        shared_progress_owner_protocol_system: impl Into<String>,
    ) -> Self {
        Self {
            shared_stream,
            branch_specs,
            shared_progress_owner_protocol_system: shared_progress_owner_protocol_system.into(),
        }
    }

    pub fn shared_extractor_id(&self) -> &str {
        &self.shared_stream.extractor_id
    }

    pub fn shared_stream_name(&self) -> &str {
        &self.shared_stream.shared_stream_name
    }

    pub fn durability_scope(&self) -> &str {
        &self.shared_stream.durability_scope
    }

    pub fn resolved_shared_stream(&self) -> &ResolvedSharedFamilyStream {
        &self.shared_stream
    }

    pub fn branch_specs(&self) -> &[FamilyBranchSpec] {
        &self.branch_specs
    }

    pub fn branch_protocol_systems(&self) -> impl Iterator<Item = &str> {
        self.branch_specs
            .iter()
            .map(|branch| branch.protocol_system.as_str())
    }

    pub fn shared_progress_owner_protocol_system(&self) -> &str {
        &self
            .shared_progress_owner_protocol_system
    }
}

pub fn family_extractor_configs<'a>(
    family: &DetectedFamilyRuntime,
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<Vec<&'a ExtractorConfig>, ExtractionError> {
    let extractor_configs = family
        .member_protocol_systems()
        .iter()
        .map(|name| {
            extractor_config_by_protocol_system(extractors, name)?.ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "family `{}` is missing extractor config for `{name}`",
                    family.family_name()
                ))
            })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;

    validate_family_runtime_membership(family, &extractor_configs)?;
    validate_resolved_family_stream_config(family, &extractor_configs)?;

    Ok(extractor_configs)
}

pub(crate) fn validate_family_runtime_membership(
    family: &impl FamilyRuntimeMembershipView,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    for config in extractor_configs {
        if config.chain() != family.chain() {
            return Err(ExtractionError::Setup(format!(
                "family runner for `{}` requires chain `{}`, but extractor `{}` uses `{}`",
                family.family_name(),
                family.chain(),
                config.name(),
                config.chain()
            )));
        }

        if let Some(runtime) = config.family_runtime() {
            if runtime.family != family.family_name() {
                return Err(ExtractionError::Setup(format!(
                    "family runner for `{}` cannot include extractor `{}` declared for family `{}`",
                    family.family_name(),
                    config.name(),
                    runtime.family
                )));
            }
        }

        if config.protocol_types().is_empty() {
            return Err(ExtractionError::Setup(format!(
                "family runner for `{}` requires extractor `{}` to declare at least one protocol type for branch routing",
                family.family_name(),
                config.name()
            )));
        }
    }

    let actual = extractor_configs
        .iter()
        .map(|config| config.protocol_system().to_string())
        .collect::<HashSet<_>>();
    let expected = family
        .member_protocol_systems()
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    if actual != expected {
        return Err(ExtractionError::Setup(format!(
            "family runner for `{}` requires exact member protocol systems {:?}, got {:?}",
            family.family_name(),
            family.member_protocol_systems(),
            actual
        )));
    }

    Ok(())
}

pub(crate) fn validate_resolved_family_stream_config(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    validate_family_shared_bootstrap_config(family, extractor_configs)?;
    validate_family_shared_start_block(family, extractor_configs)?;
    validate_family_shared_stop_block(family, extractor_configs)?;
    validate_family_shared_substreams_params(family, extractor_configs)?;
    Ok(())
}

pub(crate) fn resolve_resolved_family_execution_config(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilyExecutionConfig, ExtractionError> {
    validate_resolved_family_stream_config(family, extractor_configs)?;

    let branch_specs = FamilyBranchSpec::from_extractor_configs(extractor_configs)?;
    let family_spec = registry.require_family_spec(family.family_name(), "family execution")?;
    let runtime_contract = ResolvedFamilyRuntimeContract::new(
        family.resolved_shared_stream_with_registry(registry)?,
        branch_specs.clone(),
        family_spec
            .shared_progress_owner_protocol_system()
            .to_string(),
    );
    let auxiliary_runtime_hooks_by_protocol_system = extractor_configs
        .iter()
        .map(|config| {
            let protocol_system = config.protocol_system();
            let member_spec = family_spec
                .members()
                .iter()
                .find(|member| member.protocol_system == protocol_system)
                .expect("validated family member should exist in runtime registry");
            (
                protocol_system.to_string(),
                ProtocolSystemAuxiliaryRuntimeHooks {
                    message_decoders: if member_spec
                        .auxiliary_protocol_message_decoders()
                        .is_empty()
                    {
                        family_spec
                            .auxiliary_protocol_message_decoders()
                            .to_vec()
                    } else {
                        member_spec
                            .auxiliary_protocol_message_decoders()
                            .to_vec()
                    },
                    state_hydrators: if member_spec
                        .auxiliary_protocol_state_hydrators()
                        .is_empty()
                    {
                        family_spec
                            .auxiliary_protocol_state_hydrators()
                            .to_vec()
                    } else {
                        member_spec
                            .auxiliary_protocol_state_hydrators()
                            .to_vec()
                    },
                },
            )
        })
        .collect::<HashMap<_, _>>();

    Ok(ResolvedFamilyExecutionConfig {
        runtime_contract,
        auxiliary_runtime_hooks_by_protocol_system,
    })
}

pub(crate) fn resolve_resolved_family_shared_runtime(
    family: &DetectedFamilyRuntime,
    runtime_contract: &ResolvedFamilyRuntimeContract,
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilySharedRuntime, ExtractionError> {
    validate_resolved_family_stream_config(family, extractor_configs)?;

    let (settings, configured_start_block) =
        resolve_resolved_family_shared_settings(family.family_name(), extractor_configs)?;
    let shared_bootstrap_runtime =
        registry.resolve_optional_shared_bootstrap_runtime(extractor_configs.iter().copied())?;

    if let Some(runtime) = shared_bootstrap_runtime.as_ref() {
        validate_shared_bootstrap_plan_against_runtime_contract(&runtime.plan, runtime_contract)?;
    }

    Ok(ResolvedFamilySharedRuntime::new(
        settings,
        configured_start_block,
        shared_bootstrap_runtime,
    ))
}

pub(crate) fn resolve_detected_family_runtime<'a>(
    family: &DetectedFamilyRuntime,
    extractors: &'a HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilyRuntime<'a>, ExtractionError> {
    let extractor_configs = family_extractor_configs(family, extractors)?;
    let execution = resolve_resolved_family_execution_config(family, &extractor_configs, registry)?;
    let shared_runtime = resolve_resolved_family_shared_runtime(
        family,
        &execution.runtime_contract,
        &extractor_configs,
        registry,
    )?;

    Ok(ResolvedFamilyRuntime {
        family: family.runtime_membership(),
        extractor_configs,
        shared_runtime,
        execution,
    })
}

pub(crate) fn resolve_family_runtime_plan_with_registry<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    runtime_plan: FamilyRuntimeBuildPlan,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilyRuntimePlan<'a>, ExtractionError> {
    let families = runtime_plan
        .families
        .into_iter()
        .map(|family| resolve_detected_family_runtime(&family, extractors, registry))
        .collect::<Result<Vec<_>, ExtractionError>>()?;
    let standalone_extractors = runtime_plan
        .standalone_protocol_systems
        .into_iter()
        .map(|protocol_system| {
            extractor_config_by_protocol_system(extractors, &protocol_system)?
                .map(|config| {
                    crate::extractor::runtime_target_planning::ResolvedStandaloneRuntime::from_extractor_config_with_registry(
                        config,
                        registry,
                    )
                })
                .transpose()?
                .ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "standalone extractor config `{protocol_system}` disappeared during resolution"
                    ))
                })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;

    Ok(ResolvedFamilyRuntimePlan { families, standalone_extractors })
}

fn resolve_resolved_family_shared_settings(
    family_name: &str,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(FamilySharedStreamSettings, i64), ExtractionError> {
    let settings = FamilySharedStreamSettings::from_extractor_configs(family_name, extractor_configs)?;
    let configured_start_block = settings.configured_start_block();

    Ok((settings, configured_start_block))
}

fn validate_family_shared_bootstrap_config(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    let bootstrapped = extractor_configs
        .iter()
        .filter(|config| config.bootstrap.is_some())
        .map(|config| config.protocol_system().to_string())
        .collect::<Vec<_>>();
    let missing = extractor_configs
        .iter()
        .filter(|config| config.bootstrap.is_none())
        .map(|config| config.protocol_system().to_string())
        .collect::<Vec<_>>();

    if !bootstrapped.is_empty() && !missing.is_empty() {
        return Err(ExtractionError::Setup(format!(
            "family `{}` requires shared bootstrap configuration consistency across members; bootstrapped members: {:?}, missing bootstrap members: {:?}",
            family.family_name(), bootstrapped, missing
        )));
    }

    Ok(())
}

fn validate_family_shared_start_block(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    let mut starts = Vec::new();

    for config in extractor_configs {
        starts.push((config.protocol_system().to_string(), configured_stream_start_block(config)?));
    }

    if let Some((_, first_start)) = starts.first() {
        if starts
            .iter()
            .any(|(_, start_block)| start_block != first_start)
        {
            return Err(ExtractionError::Setup(format!(
                "family `{}` requires aligned branch start blocks, found {:?}",
                family.family_name(), starts
            )));
        }
    }

    Ok(())
}

fn validate_family_shared_stop_block(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    let mut stop_blocks = Vec::new();

    for config in extractor_configs {
        stop_blocks.push((config.protocol_system().to_string(), config.stop_block()));
    }

    if let Some((_, first_stop_block)) = stop_blocks.first() {
        if stop_blocks
            .iter()
            .any(|(_, stop_block)| stop_block != first_stop_block)
        {
            return Err(ExtractionError::Setup(format!(
                "family `{}` requires one shared stop_block, found {:?}",
                family.family_name(), stop_blocks
            )));
        }
    }

    Ok(())
}

fn validate_family_shared_substreams_params(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    FamilySharedStreamSettings::from_extractor_configs(family.family_name(), extractor_configs)?;

    Ok(())
}
