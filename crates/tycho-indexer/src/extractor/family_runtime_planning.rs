use std::collections::{HashMap, HashSet};

use tycho_common::models::Chain;

use crate::extractor::{
    extractor_config::{configured_stream_start_block, ExtractorConfig},
    family_dispatch::FamilyBranchSpec,
    family_bootstrap_registry::ResolvedSharedBootstrapExecution,
    family_registry::{default_family_runtime_registry, FamilyRuntimeRegistry, FamilyRuntimeSpec},
    family_runtime_metadata::ResolvedSharedFamilyStream,
    protocol_message_registry::AuxiliaryProtocolMessageDecoder,
    runtime_target_planning::{ResolvedRuntimeTarget, ResolvedStandaloneRuntime},
    shared_bootstrap::SharedBootstrapPlan,
    ExtractionError,
};

pub(crate) fn merge_substreams_params(
    merged: &mut HashMap<String, String>,
    incoming: &HashMap<String, String>,
    extractor_name: &str,
) -> Result<(), ExtractionError> {
    for (key, value) in incoming {
        if let Some(existing) = merged.get(key) {
            if existing != value {
                return Err(ExtractionError::Setup(format!(
                    "conflicting substreams param `{key}` while building family runner for `{extractor_name}`"
                )));
            }
        } else {
            merged.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}

pub(crate) fn merged_family_substreams_params(
    extractor_configs: &[&ExtractorConfig],
) -> Result<HashMap<String, String>, ExtractionError> {
    let mut merged_substreams_params = HashMap::new();

    for config in extractor_configs {
        merge_substreams_params(
            &mut merged_substreams_params,
            &config.substreams_params,
            config.name(),
        )?;
    }

    Ok(merged_substreams_params)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetectedFamilyRuntime {
    pub family_name: String,
    pub chain: Chain,
    pub member_protocol_systems: Vec<String>,
    pub shared_stream_name: String,
    pub(crate) shared_stream: ResolvedSharedFamilyStream,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FamilyRuntimeBuildPlan {
    pub families: Vec<DetectedFamilyRuntime>,
    pub standalone_protocol_systems: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyRuntime<'a> {
    pub family: DetectedFamilyRuntime,
    pub extractor_configs: Vec<&'a ExtractorConfig>,
    pub execution: ResolvedFamilyExecutionConfig,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyExecutionConfig {
    pub branch_specs: Vec<FamilyBranchSpec>,
    pub shared_stream: ResolvedSharedFamilyStream,
    pub shared_bootstrap_execution: ResolvedSharedBootstrapExecution,
    pub(crate) auxiliary_protocol_message_decoders_by_protocol_system:
        HashMap<String, Vec<AuxiliaryProtocolMessageDecoder>>,
    pub(crate) auxiliary_protocol_state_hydrators_by_protocol_system:
        HashMap<String, Vec<crate::extractor::protocol_message_registry::AuxiliaryProtocolStateHydrator>>,
    pub merged_substreams_params: HashMap<String, String>,
    pub stop_block: u64,
    pub configured_start_block: i64,
    pub bootstrap_plan: Option<SharedBootstrapPlan>,
}

#[derive(Clone, Debug)]
pub struct ResolvedFamilyRuntimePlan<'a> {
    pub families: Vec<ResolvedFamilyRuntime<'a>>,
    pub standalone_extractors: Vec<ResolvedStandaloneRuntime<'a>>,
}

impl<'a> FamilyRuntimeRegistry<'a> {
    pub fn detected_family_runtime(
        &self,
        family_name: &str,
        chain: Chain,
        shared_spkg: impl Into<String>,
    ) -> Result<DetectedFamilyRuntime, ExtractionError> {
        let spec = self.require_family_spec(family_name, "family runtime")?;
        let shared_metadata =
            self.require_shared_runtime_metadata_for_family(family_name, "family runtime")?;
        Ok(DetectedFamilyRuntime {
            family_name: spec.family_name().to_string(),
            chain,
            member_protocol_systems: spec
                .members()
                .iter()
                .map(|member| member.protocol_system.to_string())
                .collect(),
            shared_stream_name: shared_metadata.shared_stream_name.to_string(),
            shared_stream: self.resolved_shared_stream_for_family(chain, family_name, shared_spkg)?,
        })
    }
}

impl DetectedFamilyRuntime {
    pub fn stream_extractor_id(&self) -> String {
        self.shared_stream.extractor_id.clone()
    }

    pub fn resolved_shared_stream(&self) -> ResolvedSharedFamilyStream {
        self.shared_stream.clone()
    }

    pub fn durability_scope(&self) -> String {
        self.shared_stream.durability_scope.clone()
    }

    pub fn shared_spkg(&self) -> &str {
        &self.shared_stream.spkg
    }

    pub fn output_module(&self) -> &str {
        &self.shared_stream.module
    }
}

pub fn detect_family_runtimes(
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<Vec<DetectedFamilyRuntime>, ExtractionError> {
    detect_family_runtimes_with_registry(extractors, default_family_runtime_registry())
}

pub fn detect_family_runtimes_with_registry(
    extractors: &HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Vec<DetectedFamilyRuntime>, ExtractionError> {
    registry.validate()?;
    let mut detected = Vec::new();
    let mut claimed_members = HashMap::new();

    for spec in registry.specs() {
        let Some((shared_spkg, output_module)) = detect_shared_runtime(spec, extractors, registry)?
        else {
            continue;
        };
        let chain = detect_shared_chain(spec, extractors)?;

        for member in spec.members() {
            if let Some(existing_family) =
                claimed_members.insert(member.protocol_system, spec.family_name())
            {
                return Err(ExtractionError::Setup(format!(
                    "protocol system `{}` is assigned to multiple family runtimes: `{existing_family}` and `{}`",
                    member.protocol_system,
                    spec.family_name()
                )));
            }
        }

        let detected_family =
            registry.detected_family_runtime(spec.family_name(), chain, shared_spkg)?;
        debug_assert_eq!(detected_family.output_module(), output_module);
        detected.push(detected_family);
    }

    Ok(detected)
}

fn detect_shared_chain(
    spec: &FamilyRuntimeSpec,
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<Chain, ExtractionError> {
    let mut shared_chain = None;

    for member in spec.members() {
        let protocol_system = member.protocol_system;
        let config = extractor_config_by_protocol_system(extractors, protocol_system)?
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "family `{}` is missing extractor config for `{protocol_system}` while resolving chain",
                    spec.family_name()
                ))
            })?;

        if let Some(existing) = shared_chain {
            if existing != config.chain() {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires all members to share one chain, but `{}` uses `{}` while another member uses `{}`",
                    spec.family_name(),
                    protocol_system,
                    config.chain(),
                    existing,
                )));
            }
        } else {
            shared_chain = Some(config.chain());
        }
    }

    shared_chain.ok_or_else(|| {
        ExtractionError::Setup(format!(
            "family `{}` has no members to resolve chain from",
            spec.family_name()
        ))
    })
}

fn detect_shared_runtime(
    spec: &FamilyRuntimeSpec,
    extractors: &HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Option<(String, String)>, ExtractionError> {
    detect_explicit_shared_runtime(spec, extractors, registry)
}

fn detect_explicit_shared_runtime(
    spec: &FamilyRuntimeSpec,
    extractors: &HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Option<(String, String)>, ExtractionError> {
    let mut family_members: Vec<(&str, &ExtractorConfig)> = Vec::new();
    let explicitly_enabled_protocols = extractors
        .values()
        .filter_map(|config| {
            config
                .family_runtime()
                .filter(|runtime| runtime.family == spec.family_name())
                .map(|_| config.protocol_system().to_string())
        })
        .collect::<Vec<_>>();
    let any_explicit_opt_in = !explicitly_enabled_protocols.is_empty();

    for member in spec.members() {
        let protocol_system = member.protocol_system;
        let Some(config) = extractor_config_by_protocol_system(extractors, protocol_system)? else {
            if any_explicit_opt_in {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires every declared member extractor to be present once any member opts into the shared runtime; configured members: {:?}, missing member: `{}`",
                    spec.family_name(),
                    explicitly_enabled_protocols,
                    protocol_system,
                )));
            }
            return Ok(None);
        };
        family_members.push((protocol_system, config));
    }

    let explicitly_enabled = family_members
        .iter()
        .filter(|(_, config)| {
            config
                .family_runtime()
                .is_some_and(|runtime| runtime.family == spec.family_name())
        })
        .count();

    if explicitly_enabled == 0 {
        return Ok(None);
    }

    if explicitly_enabled != family_members.len() {
        let configured_members = family_members
            .iter()
            .filter_map(|(protocol_system, config)| {
                config
                    .family_runtime()
                    .filter(|runtime| runtime.family == spec.family_name())
                    .map(|_| (*protocol_system).to_string())
            })
            .collect::<Vec<_>>();
        return Err(ExtractionError::Setup(format!(
            "family `{}` requires every member to opt into the shared runtime; configured members: {:?}, expected members: {:?}",
            spec.family_name(),
            configured_members,
            spec.members()
                .iter()
                .map(|member| member.protocol_system)
                .collect::<Vec<_>>(),
        )));
    }

    let mut shared_spkg: Option<String> = None;
    let mut output_module: Option<String> = None;

    for (protocol_system, config) in family_members {
        let target = config
            .resolve_family_runtime_metadata(Some(registry))?
            .expect("explicitly enabled members must resolve one shared stream target");
        let candidate_spkg = target.shared_stream.spkg;
        let candidate_module = target.shared_stream.module;

        if let Some(existing) = &shared_spkg {
            if existing != &candidate_spkg {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires all members to share one spkg, but `{}` resolves `{}` while another member resolves `{existing}`",
                    spec.family_name(),
                    protocol_system,
                    candidate_spkg,
                )));
            }
        } else {
            shared_spkg = Some(candidate_spkg.to_string());
        }

        if let Some(existing) = &output_module {
            if existing != &candidate_module {
                return Err(ExtractionError::Setup(format!(
                    "family `{}` requires all members to share one output module, but `{}` resolves `{}` while another member resolves `{existing}`",
                    spec.family_name(),
                    protocol_system,
                    candidate_module,
                )));
            }
        } else {
            output_module = Some(candidate_module.to_string());
        }
    }

    Ok(Some((
        shared_spkg.expect("shared spkg resolved for explicit family"),
        output_module.expect("shared output module resolved for explicit family"),
    )))
}

pub(crate) fn extractor_config_by_protocol_system<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    protocol_system: &str,
) -> Result<Option<&'a ExtractorConfig>, ExtractionError> {
    let mut matches = extractors
        .values()
        .filter(|config| config.protocol_system() == protocol_system);
    let first = matches.next();
    if matches.next().is_some() {
        return Err(ExtractionError::Setup(format!(
            "multiple extractor configs declare protocol_system `{protocol_system}`"
        )));
    }
    Ok(first)
}

pub fn family_member_set(detected: &[DetectedFamilyRuntime]) -> HashSet<String> {
    detected
        .iter()
        .flat_map(|family| {
            family
                .member_protocol_systems
                .iter()
                .cloned()
        })
        .collect()
}

pub fn standalone_protocol_systems(
    extractors: &HashMap<String, ExtractorConfig>,
    detected: &[DetectedFamilyRuntime],
) -> Vec<String> {
    let handled = family_member_set(detected);
    let mut standalone = extractors
        .values()
        .map(|config| config.protocol_system().to_string())
        .filter(|name| !handled.contains(name))
        .collect::<Vec<_>>();
    standalone.sort();
    standalone.dedup();
    standalone
}

pub fn build_family_runtime_plan(
    extractors: &HashMap<String, ExtractorConfig>,
) -> Result<FamilyRuntimeBuildPlan, ExtractionError> {
    build_family_runtime_plan_with_registry(extractors, default_family_runtime_registry())
}

pub fn build_family_runtime_plan_with_registry(
    extractors: &HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<FamilyRuntimeBuildPlan, ExtractionError> {
    let families = detect_family_runtimes_with_registry(extractors, registry)?;
    let standalone_protocol_systems = standalone_protocol_systems(extractors, &families);

    Ok(FamilyRuntimeBuildPlan { families, standalone_protocol_systems })
}

pub fn family_extractor_configs<'a>(
    family: &DetectedFamilyRuntime,
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<Vec<&'a ExtractorConfig>, ExtractionError> {
    let extractor_configs = family
        .member_protocol_systems
        .iter()
        .map(|name| {
            extractor_config_by_protocol_system(extractors, name)?.ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "family `{}` is missing extractor config for `{name}`",
                    family.family_name
                ))
            })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;

    validate_family_runtime_membership(family, &extractor_configs)?;
    validate_resolved_family_stream_config(family, &extractor_configs)?;

    Ok(extractor_configs)
}

pub(crate) fn validate_family_runtime_membership(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    for config in extractor_configs {
        if config.chain() != family.chain {
            return Err(ExtractionError::Setup(format!(
                "family runner for `{}` requires chain `{}`, but extractor `{}` uses `{}`",
                family.family_name,
                family.chain,
                config.name(),
                config.chain()
            )));
        }

        if let Some(runtime) = config.family_runtime() {
            if runtime.family != family.family_name {
                return Err(ExtractionError::Setup(format!(
                    "family runner for `{}` cannot include extractor `{}` declared for family `{}`",
                    family.family_name,
                    config.name(),
                    runtime.family
                )));
            }
        }

        if config.protocol_types().is_empty() {
            return Err(ExtractionError::Setup(format!(
                "family runner for `{}` requires extractor `{}` to declare at least one protocol type for branch routing",
                family.family_name,
                config.name()
            )));
        }
    }

    let actual = extractor_configs
        .iter()
        .map(|config| config.protocol_system().to_string())
        .collect::<HashSet<_>>();
    let expected = family
        .member_protocol_systems
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    if actual != expected {
        return Err(ExtractionError::Setup(format!(
            "family runner for `{}` requires exact member protocol systems {:?}, got {:?}",
            family.family_name, family.member_protocol_systems, actual
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
    let merged_substreams_params =
        merged_family_substreams_params(extractor_configs).map_err(|err| match err {
            ExtractionError::Setup(message) => ExtractionError::Setup(format!(
                "family `{}` has incompatible shared substreams params: {message}",
                family.family_name
            )),
            other => other,
        })?;

    let first_config = extractor_configs
        .first()
        .ok_or_else(|| {
            ExtractionError::Setup(format!(
                "family `{}` has no extractor configs to resolve execution settings",
                family.family_name
            ))
        })?;

    let stop_block = u64::try_from(first_config.stop_block().unwrap_or(0)).map_err(|_| {
        ExtractionError::Setup(format!(
            "family `{}` resolved stop_block exceeds u64",
            family.family_name
        ))
    })?;
    let configured_start_block = configured_stream_start_block(first_config)?;
    let bootstrap_plan = resolve_family_bootstrap_plan(extractor_configs, registry)?;
    let shared_bootstrap_execution: ResolvedSharedBootstrapExecution =
        registry.resolve_shared_bootstrap_execution(&family.family_name)?;
    let auxiliary_protocol_message_decoders_by_protocol_system = extractor_configs
        .iter()
        .map(|config| {
            (
                config.protocol_system().to_string(),
                registry
                    .auxiliary_protocol_message_decoders_for_protocol_system(
                        config.protocol_system(),
                    )
                    .unwrap_or_default(),
            )
        })
        .collect::<HashMap<_, _>>();
    let auxiliary_protocol_state_hydrators_by_protocol_system = extractor_configs
        .iter()
        .map(|config| {
            (
                config.protocol_system().to_string(),
                registry
                    .auxiliary_protocol_state_hydrators_for_protocol_system(
                        config.protocol_system(),
                    )
                    .unwrap_or_default(),
            )
        })
        .collect::<HashMap<_, _>>();

    Ok(ResolvedFamilyExecutionConfig {
        branch_specs,
        shared_stream: family.resolved_shared_stream(),
        shared_bootstrap_execution,
        auxiliary_protocol_message_decoders_by_protocol_system,
        auxiliary_protocol_state_hydrators_by_protocol_system,
        merged_substreams_params,
        stop_block,
        configured_start_block,
        bootstrap_plan,
    })
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
            family.family_name, bootstrapped, missing
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
                family.family_name, starts
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
                family.family_name, stop_blocks
            )));
        }
    }

    Ok(())
}

fn validate_family_shared_substreams_params(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    merged_family_substreams_params(extractor_configs).map_err(|err| match err {
        ExtractionError::Setup(message) => ExtractionError::Setup(format!(
            "family `{}` has incompatible shared substreams params: {message}",
            family.family_name
        )),
        other => other,
    })?;

    Ok(())
}

fn resolve_family_bootstrap_plan(
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Option<SharedBootstrapPlan>, ExtractionError> {
    let plan_inputs = extractor_configs
        .iter()
        .filter_map(|config| {
            config
                .bootstrap
                .as_ref()
                .map(|bootstrap| (*config, bootstrap))
        })
        .collect::<Vec<_>>();

    if plan_inputs.is_empty() {
        Ok(None)
    } else {
        registry
            .build_shared_bootstrap_plan(plan_inputs)
            .map(Some)
    }
}

pub fn build_resolved_family_runtime_plan<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<ResolvedFamilyRuntimePlan<'a>, ExtractionError> {
    build_resolved_family_runtime_plan_with_registry(extractors, default_family_runtime_registry())
}

pub fn build_resolved_runtime_targets<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
) -> Result<Vec<ResolvedRuntimeTarget<'a>>, ExtractionError> {
    build_resolved_runtime_targets_with_registry(extractors, default_family_runtime_registry())
}

pub fn build_resolved_family_runtime_plan_with_registry<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedFamilyRuntimePlan<'a>, ExtractionError> {
    let runtime_plan = build_family_runtime_plan_with_registry(extractors, registry)?;
    let families = runtime_plan
        .families
        .into_iter()
        .map(|family| {
            let extractor_configs = family_extractor_configs(&family, extractors)?;
            let execution =
                resolve_resolved_family_execution_config(&family, &extractor_configs, registry)?;
            Ok(ResolvedFamilyRuntime { family, extractor_configs, execution })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;
    let standalone_extractors = runtime_plan
        .standalone_protocol_systems
        .into_iter()
        .map(|protocol_system| {
            extractor_config_by_protocol_system(extractors, &protocol_system)?
                .map(|extractor_config| ResolvedStandaloneRuntime {
                    protocol_system: extractor_config.protocol_system(),
                    extractor_config,
                })
                .ok_or_else(|| {
                    ExtractionError::Setup(format!(
                        "standalone extractor config `{protocol_system}` disappeared during resolution"
                    ))
                })
        })
        .collect::<Result<Vec<_>, ExtractionError>>()?;

    Ok(ResolvedFamilyRuntimePlan { families, standalone_extractors })
}

#[cfg(test)]
pub(crate) fn resolved_family_execution_config_from_extractor_configs_for_tests(
    extractor_configs: &[&ExtractorConfig],
) -> Result<ResolvedFamilyExecutionConfig, ExtractionError> {
    let registry = default_family_runtime_registry();
    let detected_family = detect_single_test_family_runtime(extractor_configs, registry)?;

    resolve_resolved_family_execution_config(&detected_family, extractor_configs, registry)
}

#[cfg(test)]
fn infer_single_test_family_name(
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<String, ExtractionError> {
    registry
        .require_family_name_for_protocol_systems(
            extractor_configs
                .iter()
                .map(|config| config.protocol_system()),
            "family execution test helper",
        )
        .map(str::to_string)
}

#[cfg(test)]
fn detect_single_test_family_runtime(
    extractor_configs: &[&ExtractorConfig],
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<DetectedFamilyRuntime, ExtractionError> {
    let family_name = infer_single_test_family_name(extractor_configs, registry)?;
    let extractors = extractor_configs
        .iter()
        .map(|config| (config.protocol_system().to_string(), (*config).clone()))
        .collect::<HashMap<_, _>>();
    let detected = detect_family_runtimes_with_registry(&extractors, registry)?;
    let detected = if detected.is_empty() {
        let family_spec = registry.require_family_spec(&family_name, "test family runtime")?;
        let synthetic_shared_spkg = format!("/tmp/{family_name}-family-test.spkg");
        let enriched = extractor_configs
            .iter()
            .map(|config| {
                let mut cloned = (*config).clone();
                cloned.family_runtime = Some(crate::extractor::family_runtime_metadata::FamilyRuntimeConfig {
                    family: family_name.clone(),
                    shared_spkg: Some(synthetic_shared_spkg.clone()),
                    shared_module: Some(family_spec.output_module().to_string()),
                    durability_scope: Some(family_spec.durability_scope().to_string()),
                });
                (cloned.protocol_system().to_string(), cloned)
            })
            .collect::<HashMap<_, _>>();
        detect_family_runtimes_with_registry(&enriched, registry)?
    } else {
        detected
    };
    let mut matches = detected
        .into_iter()
        .filter(|family| family.family_name == family_name)
        .collect::<Vec<_>>();

    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(ExtractionError::Setup(format!(
            "family execution test helper could not detect family runtime `{family_name}` from provided extractor configs"
        ))),
        _ => Err(ExtractionError::Setup(format!(
            "family execution test helper expected exactly one detected family runtime `{family_name}`, found {}",
            matches.len()
        ))),
    }
}

pub fn build_resolved_runtime_targets_with_registry<'a>(
    extractors: &'a HashMap<String, ExtractorConfig>,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<Vec<ResolvedRuntimeTarget<'a>>, ExtractionError> {
    let resolved = build_resolved_family_runtime_plan_with_registry(extractors, registry)?;
    let mut targets = resolved
        .families
        .into_iter()
        .map(ResolvedRuntimeTarget::Family)
        .collect::<Vec<_>>();
    targets.extend(
        resolved
            .standalone_extractors
            .into_iter()
            .map(ResolvedRuntimeTarget::Standalone),
    );
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use crate::extractor::{
        extractor_config::{
            BootstrapConfig, BootstrapStrategy, ExtractorConfig, ProtocolTypeConfig,
        },
        family_registry::{
            default_family_runtime_registry, FamilyRuntimeRegistry, FamilyRuntimeSpec,
        },
        family_runtime_metadata::{FamilyRuntimeConfig, ResolvedSharedFamilyStream},
        protocol_message_registry::{
            AuxiliaryProtocolMessageBuildFuture, AuxiliaryProtocolMessageContext,
            AuxiliaryProtocolMessageDecoder,
        },
        runtime_target_planning::{ResolvedRuntimeTarget, ResolvedStandaloneRuntime},
        ExtractionError,
    };

    use super::{
        build_family_runtime_plan, build_family_runtime_plan_with_registry,
        build_resolved_family_runtime_plan, build_resolved_family_runtime_plan_with_registry,
        build_resolved_runtime_targets, build_resolved_runtime_targets_with_registry,
        detect_family_runtimes, detect_family_runtimes_with_registry, family_extractor_configs,
        standalone_protocol_systems,
    };

    fn family_shared_stream(
        chain: Chain,
        family_name: &str,
        shared_spkg: &str,
    ) -> ResolvedSharedFamilyStream {
        default_family_runtime_registry()
            .resolved_shared_stream_for_family(chain, family_name, shared_spkg)
            .expect("registered shared stream")
    }

    fn uniswap_shared_stream(shared_spkg: &str) -> ResolvedSharedFamilyStream {
        family_shared_stream(Chain::Ethereum, "uniswap", shared_spkg)
    }

    fn base_uniswap_shared_stream(shared_spkg: &str) -> ResolvedSharedFamilyStream {
        default_family_runtime_registry()
            .resolved_shared_stream_for_family(Chain::Base, "uniswap", shared_spkg)
            .expect("registered base uniswap shared stream")
    }

    fn with_resolved_family_runtime(
        config: ExtractorConfig,
        chain: Chain,
        family_name: &str,
        shared_spkg: &str,
    ) -> ExtractorConfig {
        let shared_stream = family_shared_stream(chain, family_name, shared_spkg);
        config.with_family_runtime(Some(FamilyRuntimeConfig {
            family: family_name.to_string(),
            shared_spkg: Some(shared_spkg.to_string()),
            shared_module: Some(shared_stream.module),
            durability_scope: Some(shared_stream.durability_scope),
        }))
    }

    fn make_config(name: &str, spkg: &str) -> ExtractorConfig {
        ExtractorConfig::new(
            name.to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            42,
            None,
            vec![ProtocolTypeConfig::new(format!("{name}_pool"), FinancialType::Swap)],
            spkg.to_string(),
            "map_protocol_changes".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::new(),
            None,
        )
    }

    fn with_resolved_uniswap_family_runtime(
        config: ExtractorConfig,
        shared_spkg: &str,
    ) -> ExtractorConfig {
        with_resolved_family_runtime(config, Chain::Ethereum, "uniswap", shared_spkg)
    }

    fn build_future_family_events<'a>(
        _context: &'a dyn AuxiliaryProtocolMessageContext,
        _value: &'a [u8],
        _finalized_block_height: u64,
        _partial_block_index: Option<u32>,
    ) -> AuxiliaryProtocolMessageBuildFuture<'a> {
        Box::pin(async {
            Err(ExtractionError::Unknown(
                "test-only decoder should not run".to_string(),
            ))
        })
    }

    #[test]
    fn custom_registry_detects_future_family_without_runner_changes() {
        const FUTURE_AUXILIARY_PROTOCOL_MESSAGE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
            &[AuxiliaryProtocolMessageDecoder {
                protocol_system: "future_v1",
                type_url_suffix: "FutureEvents",
                build_block_changes: build_future_family_events,
            }];
        const FUTURE_FAMILY: FamilyRuntimeSpec =
            crate::extractor::family_registry::shared_family_runtime_spec_with_auxiliary_decoders(
                "future_swap",
                &[
                    crate::extractor::family_registry::shared_family_member_spec(
                        "future_v1",
                        &["futurev1"],
                        None,
                    ),
                    crate::extractor::family_registry::shared_family_member_spec(
                        "future_v2",
                        &["futurev2"],
                        None,
                    ),
                ],
                "map_future_swap_family_protocol_changes",
                "future_swap_family",
                "family::future_swap_runtime",
                None,
                FUTURE_AUXILIARY_PROTOCOL_MESSAGE_DECODERS,
            );
        const SPECS: &[FamilyRuntimeSpec] = &[FUTURE_FAMILY];
        let registry = FamilyRuntimeRegistry::new(SPECS);
        let extractors = HashMap::from([
            (
                "future_v1".to_string(),
                make_config("future_v1", "/tmp/future-v1-only.spkg").with_family_runtime(Some(
                    FamilyRuntimeConfig {
                        family: "future_swap".to_string(),
                        shared_spkg: Some(
                            "protocols/substreams/future-swap-combined/test.spkg".to_string(),
                        ),
                        shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                        durability_scope: Some("family::future_swap_runtime".to_string()),
                    },
                )),
            ),
            (
                "future_v2".to_string(),
                make_config("future_v2", "/tmp/future-v2-only.spkg").with_family_runtime(Some(
                    FamilyRuntimeConfig {
                        family: "future_swap".to_string(),
                        shared_spkg: Some(
                            "protocols/substreams/future-swap-combined/test.spkg".to_string(),
                        ),
                        shared_module: Some("map_future_swap_family_protocol_changes".to_string()),
                        durability_scope: Some("family::future_swap_runtime".to_string()),
                    },
                )),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let detected = detect_family_runtimes_with_registry(&extractors, registry)
            .expect("custom family detection succeeds");
        let plan = build_family_runtime_plan_with_registry(&extractors, registry)
            .expect("custom family plan builds");
        let resolved = build_resolved_family_runtime_plan_with_registry(&extractors, registry)
            .expect("custom resolved plan builds");

        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].family_name, "future_swap");
        assert_eq!(
            detected[0].member_protocol_systems,
            vec!["future_v1".to_string(), "future_v2".to_string()]
        );
        assert_eq!(detected[0].output_module(), "map_future_swap_family_protocol_changes");
        assert_eq!(detected[0].shared_stream_name, "future_swap_family");
        assert_eq!(detected[0].durability_scope(), "family::future_swap_runtime");
        assert_eq!(plan.standalone_protocol_systems, vec!["curve".to_string()]);
        assert_eq!(resolved.families.len(), 1);
        assert_eq!(resolved.families[0].extractor_configs.len(), 2);
        assert_eq!(resolved.standalone_extractors[0].protocol_system, "curve");

        let targets = build_resolved_runtime_targets_with_registry(&extractors, registry)
            .expect("custom resolved targets build");
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Family(family)
                if family.family.family_name == "future_swap" && family.extractor_configs.len() == 2
        )));
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Standalone(ResolvedStandaloneRuntime { extractor_config, .. })
                if extractor_config.name() == "curve"
        )));
        assert_eq!(
            registry
                .auxiliary_protocol_message_decoders_for_protocol_system("future_v1")
                .map(|decoders| decoders.len()),
            Some(1)
        );
        assert_eq!(
            resolved.families[0]
                .execution
                .auxiliary_protocol_message_decoders_by_protocol_system
                .get("future_v1")
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn test_family_execution_helper_reuses_production_family_execution_resolution() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let registry = default_family_runtime_registry();
        let family = detect_family_runtimes(&extractors)
            .expect("family detection succeeds")
            .into_iter()
            .next()
            .expect("uniswap family should be detected");
        let extractor_configs =
            family_extractor_configs(&family, &extractors).expect("family configs resolve");

        let from_production =
            super::resolve_resolved_family_execution_config(&family, &extractor_configs, registry)
                .expect("production family execution config resolves");
        let from_test_helper =
            super::resolved_family_execution_config_from_extractor_configs_for_tests(
                &extractor_configs,
            )
            .expect("test helper family execution config resolves");

        assert_eq!(
            from_test_helper.branch_specs, from_production.branch_specs,
            "test helper should reuse production branch routing"
        );
        assert_eq!(
            from_test_helper.shared_stream, from_production.shared_stream,
            "test helper should reuse production shared stream identity"
        );
        assert_eq!(
            from_test_helper.merged_substreams_params, from_production.merged_substreams_params,
            "test helper should reuse production shared substreams params"
        );
        assert_eq!(
            from_test_helper.stop_block, from_production.stop_block,
            "test helper should reuse production stop block resolution"
        );
        assert_eq!(
            from_test_helper.configured_start_block, from_production.configured_start_block,
            "test helper should reuse production start block resolution"
        );
        assert_eq!(
            from_test_helper.bootstrap_plan, from_production.bootstrap_plan,
            "test helper should reuse production shared bootstrap planning"
        );
    }

    #[test]
    fn resolved_runtime_plan_precomputes_family_execution_settings() {
        let v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(120),
                vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::from([("map_pool_events".to_string(), "factory=0x01".to_string())]),
                Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params: "bootstrap_block=42&pools=0x01".to_string(),
                }),
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let v3 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v3".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(120),
                vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::from([("map_events".to_string(), "factory=0x02".to_string())]),
                Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 42,
                    params: "bootstrap_block=42&pools=0x02".to_string(),
                }),
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let resolved = build_resolved_family_runtime_plan(&extractors)
            .expect("resolved family runtime plan should build");

        let family = resolved
            .families
            .first()
            .expect("one uniswap family should be resolved");
        let expected_shared_stream =
            uniswap_shared_stream("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg");

        assert_eq!(
            family.execution.shared_stream.spkg,
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );
        assert_eq!(family.execution.shared_stream.module, expected_shared_stream.module);
        assert_eq!(
            family.execution.shared_stream.extractor_id,
            expected_shared_stream.extractor_id
        );
        assert_eq!(
            family.execution.shared_bootstrap_execution.branch_runtimes.len(),
            2
        );
        assert_eq!(family.execution.stop_block, 120);
        assert_eq!(family.execution.configured_start_block, 43);
        assert_eq!(
            family.execution.merged_substreams_params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
        let bootstrap_plan = family
            .execution
            .bootstrap_plan
            .as_ref()
            .expect("family execution should precompute shared bootstrap plan");
        assert_eq!(bootstrap_plan.bootstrap_block, 42);
        assert_eq!(bootstrap_plan.branches.len(), 2);
    }

    #[test]
    fn resolved_runtime_plan_rejects_misaligned_effective_start_blocks() {
        let mut v2 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v2.bootstrap = Some(BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pools=0x01".to_string(),
        });
        let v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("misaligned effective start blocks should fail");

        assert!(err
            .to_string()
            .contains("family `uniswap` requires aligned branch start blocks"));
    }

    #[test]
    fn resolved_runtime_plan_rejects_partial_shared_bootstrap_config() {
        let mut v2 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v2.bootstrap = Some(BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV2Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pools=0x01".to_string(),
        });
        let v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("partial shared bootstrap config should fail during planning");

        assert!(err.to_string().contains(
            "family `uniswap` requires shared bootstrap configuration consistency across members"
        ));
    }

    #[test]
    fn resolved_runtime_plan_rejects_misaligned_stop_blocks() {
        let v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(100),
                vec![ProtocolTypeConfig::new("uniswap_v2_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let v3 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v3".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                Some(200),
                vec![ProtocolTypeConfig::new("uniswap_v3_pool".to_string(), FinancialType::Swap)],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("misaligned stop blocks should fail");

        assert!(err
            .to_string()
            .contains("family `uniswap` requires one shared stop_block"));
    }

    #[test]
    fn resolved_runtime_plan_rejects_conflicting_substreams_params() {
        let mut v2 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v2.substreams_params =
            HashMap::from([("map_pool_events".to_string(), "factory=0x01".to_string())]);

        let mut v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        v3.substreams_params =
            HashMap::from([("map_pool_events".to_string(), "factory=0x02".to_string())]);

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("conflicting substreams params should fail");

        assert!(err
            .to_string()
            .contains("family `uniswap` has incompatible shared substreams params"));
    }

    #[test]
    fn resolved_runtime_plan_rejects_missing_protocol_types() {
        let v2 = with_resolved_uniswap_family_runtime(
            ExtractorConfig::new(
                "uniswap_v2".to_string(),
                Chain::Ethereum,
                ImplementationType::Custom,
                1000,
                42,
                None,
                vec![],
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg".to_string(),
                "map_protocol_changes".to_string(),
                vec![],
                0,
                None,
                None,
                HashMap::new(),
                None,
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );
        let v3 = with_resolved_uniswap_family_runtime(
            make_config(
                "uniswap_v3",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
        );

        let extractors =
            HashMap::from([("uniswap_v2".to_string(), v2), ("uniswap_v3".to_string(), v3)]);

        let err = build_resolved_family_runtime_plan(&extractors)
            .expect_err("missing protocol types should fail");

        assert!(err
            .to_string()
            .contains("requires extractor `uniswap_v2` to declare at least one protocol type"));
    }

    #[test]
    fn does_not_detect_uniswap_family_runtime_without_explicit_opt_in() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                make_config(
                    "uniswap_v2",
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                make_config(
                    "uniswap_v3",
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");

        assert!(detected.is_empty());
    }

    #[test]
    fn does_not_detect_family_when_one_member_missing() {
        let extractors = HashMap::from([(
            "uniswap_v2".to_string(),
            make_config(
                "uniswap_v2",
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
            ),
        )]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");

        assert!(detected.is_empty());
    }

    #[test]
    fn explicit_family_runtime_rejects_mismatched_family_spkgs() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    "/tmp/a.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v3", "/tmp/v3-only.spkg"),
                    "/tmp/b.spkg",
                ),
            ),
        ]);

        let err = detect_family_runtimes(&extractors).expect_err("mismatched spkgs should fail");

        assert!(err
            .to_string()
            .contains("requires all members to share one spkg"));
    }

    #[test]
    fn explicit_family_runtime_rejects_mismatched_family_chains() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    "/tmp/a.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                ExtractorConfig::new(
                    "uniswap_v3".to_string(),
                    Chain::Base,
                    ImplementationType::Custom,
                    1000,
                    42,
                    None,
                    vec![ProtocolTypeConfig::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                    )],
                    "/tmp/v3-only.spkg".to_string(),
                    "map_protocol_changes".to_string(),
                    vec![],
                    0,
                    None,
                    None,
                    HashMap::new(),
                    None,
                )
                .with_family_runtime(Some(FamilyRuntimeConfig {
                    family: "uniswap".to_string(),
                    shared_spkg: Some("/tmp/a.spkg".to_string()),
                    shared_module: Some(uniswap_shared_stream("/tmp/a.spkg").module),
                    durability_scope: None,
                })),
            ),
        ]);

        let err = detect_family_runtimes(&extractors).expect_err("mismatched chains should fail");

        assert!(err
            .to_string()
            .contains("requires all members to share one chain"));
    }

    #[test]
    fn preserves_standalone_extractors_outside_detected_families() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let standalone = standalone_protocol_systems(&extractors, &detected);

        assert_eq!(standalone, vec!["curve".to_string()]);
    }

    #[test]
    fn builds_runtime_plan_with_family_and_standalone_extractors() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let plan = build_family_runtime_plan(&extractors).expect("build plan succeeds");

        assert_eq!(plan.families.len(), 1);
        assert_eq!(plan.families[0].family_name, "uniswap");
        assert_eq!(plan.families[0].chain, Chain::Ethereum);
        assert_eq!(plan.standalone_protocol_systems, vec!["curve".to_string()]);
    }

    #[test]
    fn resolves_family_member_configs_from_detected_runtime() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                make_config(
                    "uniswap_v2",
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                make_config(
                    "uniswap_v3",
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let resolved =
            family_extractor_configs(&detected[0], &extractors).expect("family configs resolve");

        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].name(), "uniswap_v2");
        assert_eq!(resolved[1].name(), "uniswap_v3");
    }

    #[test]
    fn builds_resolved_runtime_plan() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let resolved = build_resolved_family_runtime_plan(&extractors).expect("resolved plan");

        assert_eq!(resolved.families.len(), 1);
        assert_eq!(resolved.families[0].family.family_name, "uniswap");
        assert_eq!(resolved.families[0].family.chain, Chain::Ethereum);
        assert_eq!(resolved.families[0].extractor_configs.len(), 2);
        assert_eq!(resolved.standalone_extractors.len(), 1);
        assert_eq!(resolved.standalone_extractors[0].protocol_system, "curve");
        assert_eq!(resolved.standalone_extractors[0].extractor_config.name(), "curve");
    }

    #[test]
    fn builds_resolved_runtime_targets() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    ),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            ("curve".to_string(), make_config("curve", "protocols/substreams/curve/curve.spkg")),
        ]);

        let targets = build_resolved_runtime_targets(&extractors).expect("resolved targets");

        assert_eq!(targets.len(), 2);
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Family(family)
                if family.family.family_name == "uniswap" && family.extractor_configs.len() == 2
        )));
        assert!(targets.iter().any(|target| matches!(
            target,
            ResolvedRuntimeTarget::Standalone(ResolvedStandaloneRuntime { extractor_config, .. })
                if extractor_config.name() == "curve"
        )));

        let standalone_target = targets
            .iter()
            .find(|target| matches!(target, ResolvedRuntimeTarget::Standalone(_)))
            .expect("standalone target present");
        assert_eq!(standalone_target.chain(), Chain::Ethereum);
        assert_eq!(standalone_target.protocol_systems(), vec!["curve"]);
        assert_eq!(
            standalone_target
                .extractor_configs()
                .into_iter()
                .map(|config| config.name())
                .collect::<Vec<_>>(),
            vec!["curve"]
        );
    }

    #[test]
    fn stream_extractor_id_uses_detected_chain() {
        let spkg = "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg";
        let family = default_family_runtime_registry()
            .detected_family_runtime("uniswap", Chain::Base, spkg)
            .expect("registered uniswap family runtime");
        let expected_shared_stream = base_uniswap_shared_stream(spkg);

        assert_eq!(family.stream_extractor_id(), expected_shared_stream.extractor_id);
    }

    #[test]
    fn durability_scope_uses_detected_family_name() {
        let spkg = "protocols/substreams/base-uniswap-v2-v3-combined/test.spkg";
        let family = default_family_runtime_registry()
            .detected_family_runtime("uniswap", Chain::Base, spkg)
            .expect("registered uniswap family runtime");
        let expected_shared_stream = base_uniswap_shared_stream(spkg);

        assert_eq!(family.durability_scope(), expected_shared_stream.durability_scope);
    }

    #[test]
    fn registry_builds_detected_family_runtime_from_registered_metadata() {
        let registry = default_family_runtime_registry();
        let family = registry
            .detected_family_runtime("uniswap", Chain::Ethereum, "/tmp/test.spkg")
            .expect("registered uniswap family runtime");
        let shared_stream = registry
            .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", "/tmp/test.spkg")
            .expect("registered uniswap shared stream");

        assert_eq!(family.family_name, "uniswap");
        assert_eq!(
            family.member_protocol_systems,
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert_eq!(family.shared_spkg(), "/tmp/test.spkg");
        assert_eq!(family.output_module(), shared_stream.module);
        assert_eq!(
            family.shared_stream_name,
            registry
                .shared_stream_name_for_family("uniswap")
                .expect("uniswap shared stream name")
        );
        assert_eq!(family.durability_scope(), shared_stream.durability_scope);
    }

    #[test]
    fn detects_explicit_family_runtime_without_spkg_hint() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    shared_spkg,
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v3", "/tmp/v3-only.spkg"),
                    shared_spkg,
                ),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let expected_shared_stream = uniswap_shared_stream(shared_spkg);

        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].shared_spkg(), shared_spkg);
        assert_eq!(detected[0].output_module(), expected_shared_stream.module);
    }

    #[test]
    fn rejects_partially_configured_explicit_family_runtime() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                    shared_spkg,
                ),
            ),
            ("uniswap_v3".to_string(), make_config("uniswap_v3", "/tmp/v3-only.spkg")),
        ]);

        let err = detect_family_runtimes(&extractors)
            .expect_err("partially configured explicit family should fail");

        assert!(err
            .to_string()
            .contains("requires every member to opt into the shared runtime"));
    }

    #[test]
    fn rejects_explicit_family_runtime_when_declared_member_extractor_is_missing() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([(
            "uniswap_v2".to_string(),
            with_resolved_uniswap_family_runtime(
                make_config("uniswap_v2", "/tmp/v2-only.spkg"),
                shared_spkg,
            ),
        )]);

        let err = detect_family_runtimes(&extractors)
            .expect_err("missing family member should fail once explicit runtime is enabled");

        assert!(err
            .to_string()
            .contains("requires every declared member extractor to be present once any member opts into the shared runtime"));
    }

    #[test]
    fn detects_family_by_explicit_protocol_system_not_config_key() {
        let shared_spkg = "/tmp/custom-runtime.spkg";
        let extractors = HashMap::from([
            (
                "uniswap_v2_primary".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v2_indexer", "/tmp/v2-only.spkg")
                        .with_protocol_system("uniswap_v2"),
                    shared_spkg,
                ),
            ),
            (
                "uniswap_v3_primary".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config("uniswap_v3_indexer", "/tmp/v3-only.spkg")
                        .with_protocol_system("uniswap_v3"),
                    shared_spkg,
                ),
            ),
        ]);

        let detected = detect_family_runtimes(&extractors).expect("family detection succeeds");
        let resolved = build_resolved_family_runtime_plan(&extractors).expect("resolved plan");

        assert_eq!(detected.len(), 1);
        assert_eq!(resolved.families.len(), 1);
        assert_eq!(
            resolved.families[0]
                .extractor_configs
                .iter()
                .map(|cfg| cfg.protocol_system().to_string())
                .collect::<Vec<_>>(),
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
    }

    #[test]
    fn rejects_duplicate_protocol_system_declarations() {
        let extractors = HashMap::from([
            (
                "first_v2".to_string(),
                make_config("first_v2", "/tmp/a.spkg").with_protocol_system("uniswap_v2"),
            ),
            (
                "second_v2".to_string(),
                make_config("second_v2", "/tmp/b.spkg").with_protocol_system("uniswap_v2"),
            ),
            (
                "v3".to_string(),
                make_config("v3", "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
                    .with_protocol_system("uniswap_v3"),
            ),
        ]);

        let err = detect_family_runtimes(&extractors)
            .expect_err("duplicate protocol_system declarations should fail");

        assert!(err
            .to_string()
            .contains("multiple extractor configs declare protocol_system `uniswap_v2`"));
    }
}
