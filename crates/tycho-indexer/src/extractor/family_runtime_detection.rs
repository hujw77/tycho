use std::collections::HashMap;

use tycho_common::models::Chain;

#[cfg(test)]
use crate::extractor::family_registry::default_family_runtime_registry;
use crate::extractor::{
    extractor_config::{extractor_config_by_protocol_system, ExtractorConfig},
    family_registry::{FamilyRuntimeRegistry, FamilyRuntimeSpec},
    family_runtime_types::DetectedFamilyRuntime,
    ExtractionError,
};

#[cfg(test)]
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
            .require_resolved_family_runtime_metadata_with_registry(registry)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use crate::extractor::{
        extractor_config::{ExtractorConfig, ProtocolTypeConfig},
        family_registry::default_family_runtime_registry,
        family_runtime_metadata::{FamilyRuntimeConfig, ResolvedSharedFamilyStream},
    };

    use super::detect_family_runtimes;

    fn uniswap_shared_stream(shared_spkg: &str) -> ResolvedSharedFamilyStream {
        default_family_runtime_registry()
            .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", shared_spkg)
            .expect("registered uniswap shared stream")
    }

    fn with_resolved_uniswap_family_runtime(
        config: ExtractorConfig,
        shared_spkg: &str,
    ) -> ExtractorConfig {
        config.with_family_runtime(Some(FamilyRuntimeConfig::from_resolved_shared_stream(
            "uniswap",
            uniswap_shared_stream(shared_spkg),
        )))
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
                .with_family_runtime(Some({
                    let mut runtime = FamilyRuntimeConfig::from_resolved_shared_stream(
                        "uniswap",
                        uniswap_shared_stream("/tmp/a.spkg"),
                    );
                    runtime.durability_scope = None;
                    runtime
                })),
            ),
        ]);

        let err = detect_family_runtimes(&extractors).expect_err("mismatched chains should fail");
        assert!(err
            .to_string()
            .contains("requires all members to share one chain"));
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
        assert_eq!(detected.len(), 1);
        assert_eq!(
            detected[0].member_protocol_systems(),
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
