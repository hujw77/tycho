use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use tycho_common::models::{Chain, ImplementationType};
use tycho_indexer::extractor::runner::{
    BootstrapConfig, BootstrapStrategy, DCIType, ExtractorConfig, FamilyRuntimeConfig,
    ProtocolTypeConfig,
};

#[derive(Debug, Deserialize)]
pub(crate) struct ExtractorConfigs {
    pub(crate) extractors: HashMap<String, ExtractorConfig>,
}

#[derive(Debug, Deserialize)]
struct RawExtractorConfigs {
    #[serde(default)]
    includes: Vec<String>,
    #[serde(default)]
    family_runtimes: HashMap<String, RawFamilyRuntimeDefaults>,
    extractors: HashMap<String, RawExtractorConfig>,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct RawFamilyRuntimeDefaults {
    #[serde(default)]
    shared_spkg: Option<String>,
    #[serde(default)]
    shared_module: Option<String>,
    #[serde(default)]
    stop_block: Option<i64>,
    #[serde(default)]
    bootstrap: Option<RawFamilyBootstrapDefaults>,
    #[serde(default)]
    members: HashMap<String, RawFamilyMemberDefaults>,
}

#[derive(Debug, Deserialize, Clone)]
struct RawFamilyBootstrapDefaults {
    params: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct RawFamilyMemberDefaults {
    #[serde(default)]
    substreams_params: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RawExtractorConfig {
    name: String,
    #[serde(default)]
    protocol_system: Option<String>,
    chain: Chain,
    implementation_type: ImplementationType,
    sync_batch_size: usize,
    start_block: Option<i64>,
    stop_block: Option<i64>,
    protocol_types: Vec<ProtocolTypeConfig>,
    #[serde(default)]
    spkg: Option<String>,
    module_name: String,
    #[serde(default)]
    initialized_accounts: Vec<tycho_common::Bytes>,
    #[serde(default)]
    initialized_accounts_block: u64,
    #[serde(default)]
    post_processor: Option<String>,
    #[serde(default)]
    dci_plugin: Option<DCIType>,
    #[serde(default)]
    substreams_params: HashMap<String, String>,
    #[serde(default)]
    bootstrap: Option<RawBootstrapConfig>,
    #[serde(default)]
    family_runtime: Option<FamilyRuntimeConfig>,
}

#[derive(Debug, Deserialize)]
struct RawBootstrapConfig {
    strategy: BootstrapStrategy,
    params: String,
    #[serde(skip)]
    start_block: Option<i64>,
}

impl ExtractorConfigs {
    pub(crate) fn new(extractors: HashMap<String, ExtractorConfig>) -> Self {
        Self { extractors }
    }

    pub(crate) fn from_yaml(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = load_raw_extractor_configs(Path::new(path), &mut HashSet::new())?;
        config.validate_family_runtime_defaults()?;
        let base_dir = Path::new(path)
            .parent()
            .unwrap_or_else(|| Path::new("."));
        config.resolve_substreams_params(base_dir)?;
        let config: Self = config.try_into()?;
        tycho_indexer::extractor::family_runtime::build_resolved_runtime_targets(
            &config.extractors,
        )
        .map_err(|err| -> Box<dyn std::error::Error> { err.to_string().into() })?;
        Ok(config)
    }
}

impl RawExtractorConfigs {
    fn validate_family_runtime_defaults(&self) -> Result<(), Box<dyn std::error::Error>> {
        let registry = tycho_indexer::extractor::family_runtime::default_family_runtime_registry();
        registry.validate()?;

        for (family_name, defaults) in &self.family_runtimes {
            registry.require_family_spec(family_name, "family_runtime")?;
            if defaults.bootstrap.is_some() {
                registry.validate_shared_bootstrap_support_for_family(family_name)?;
            }
            registry.validate_family_member_defaults_for_family(
                family_name,
                defaults.members.keys().map(String::as_str),
            )?;
        }

        Ok(())
    }

    fn resolve_substreams_params(
        &mut self,
        base_dir: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let family_runtime_defaults = self.family_runtimes.clone();

        for (extractor_name, extractor) in &mut self.extractors {
            let protocol_system = extractor
                .protocol_system
                .as_deref()
                .unwrap_or(&extractor.name);
            let route_protocol_filter = extractor
                .family_runtime
                .as_ref()
                .map(|_| protocol_system);
            let mut resolved_start_block = extractor.start_block;
            merge_family_member_substreams_params_defaults(
                protocol_system,
                extractor.family_runtime.as_ref(),
                &family_runtime_defaults,
                &mut extractor.substreams_params,
            );
            resolve_substreams_params_map(
                route_protocol_filter,
                &mut resolved_start_block,
                &mut extractor.substreams_params,
                base_dir,
            )?;

            merge_family_bootstrap_defaults(
                protocol_system,
                extractor.family_runtime.as_ref(),
                &family_runtime_defaults,
                &mut extractor.bootstrap,
            )?;
            merge_family_stop_block_defaults(
                extractor.family_runtime.as_ref(),
                &family_runtime_defaults,
                &mut extractor.stop_block,
            );

            if let Some(bootstrap) = &mut extractor.bootstrap {
                bootstrap.start_block = Some(resolve_bootstrap_params(
                    route_protocol_filter,
                    &mut bootstrap.params,
                    base_dir,
                )?);

                if let Some(start_block) = bootstrap.start_block {
                    if let Some(existing_start_block) = resolved_start_block {
                        if existing_start_block != start_block {
                            return Err(format!(
                                "conflicting start_block values for extractor `{extractor_name}`: \
                                 {existing_start_block} vs {start_block} from bootstrap config"
                            )
                            .into());
                        }
                    } else {
                        resolved_start_block = Some(start_block);
                    }
                }
            }
            extractor.start_block = resolved_start_block;
        }
        Ok(())
    }
}

fn merge_family_stop_block_defaults(
    family_runtime: Option<&FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    stop_block: &mut Option<i64>,
) {
    if stop_block.is_some() {
        return;
    }

    let Some(family_runtime) = family_runtime else {
        return;
    };
    let Some(defaults) = family_defaults.get(&family_runtime.family) else {
        return;
    };

    *stop_block = defaults.stop_block;
}

fn merge_family_bootstrap_defaults(
    protocol_system: &str,
    family_runtime: Option<&FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    bootstrap: &mut Option<RawBootstrapConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    if bootstrap.is_some() {
        return Ok(());
    }

    let Some(family_runtime) = family_runtime else {
        return Ok(());
    };
    let Some(defaults) = family_defaults.get(&family_runtime.family) else {
        return Ok(());
    };
    let Some(family_bootstrap) = defaults.bootstrap.as_ref() else {
        return Ok(());
    };
    let registry = tycho_indexer::extractor::family_runtime::default_family_runtime_registry();

    let strategy = registry.shared_bootstrap_strategy_for_family_member(
        &family_runtime.family,
        protocol_system,
        "family bootstrap defaults for",
    )?;

    *bootstrap = Some(RawBootstrapConfig {
        strategy,
        params: family_bootstrap.params.clone(),
        start_block: None,
    });
    Ok(())
}

fn merge_family_member_substreams_params_defaults(
    protocol_system: &str,
    family_runtime: Option<&FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
    substreams_params: &mut HashMap<String, String>,
) {
    let Some(family_runtime) = family_runtime else {
        return;
    };
    let Some(defaults) = family_defaults.get(&family_runtime.family) else {
        return;
    };
    let Some(member_defaults) = defaults.members.get(protocol_system) else {
        return;
    };

    for (module_name, params) in &member_defaults.substreams_params {
        substreams_params
            .entry(module_name.clone())
            .or_insert_with(|| params.clone());
    }
}

impl TryFrom<RawExtractorConfigs> for ExtractorConfigs {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: RawExtractorConfigs) -> Result<Self, Self::Error> {
        let mut extractors = HashMap::with_capacity(value.extractors.len());

        for (extractor_id, extractor) in value.extractors {
            let protocol_system = extractor
                .protocol_system
                .unwrap_or_else(|| extractor.name.clone());
            let family_runtime = merge_family_runtime_config(
                &protocol_system,
                extractor.family_runtime,
                &value.family_runtimes,
            )?;
            let spkg = resolve_extractor_spkg(
                &protocol_system,
                extractor.spkg,
                family_runtime.as_ref(),
            )?;
            let start_block = extractor
                .start_block
                .ok_or_else(|| format!("extractor `{extractor_id}` is missing `start_block`"))?;

            extractors.insert(
                extractor_id,
                ExtractorConfig::new(
                    extractor.name,
                    extractor.chain,
                    extractor.implementation_type,
                    extractor.sync_batch_size,
                    start_block,
                    extractor.stop_block,
                    extractor.protocol_types,
                    spkg,
                    extractor.module_name,
                    extractor.initialized_accounts,
                    extractor.initialized_accounts_block,
                    extractor.post_processor,
                    extractor.dci_plugin,
                    extractor.substreams_params,
                    extractor
                        .bootstrap
                        .map(|bootstrap| BootstrapConfig {
                            strategy: bootstrap.strategy,
                            start_block: bootstrap.start_block.expect(
                                "bootstrap config start_block must be resolved before conversion",
                            ),
                            params: bootstrap.params,
                        }),
                )
                .with_protocol_system(protocol_system)
                .with_family_runtime(family_runtime),
            );
        }

        Ok(ExtractorConfigs::new(extractors))
    }
}

fn resolve_extractor_spkg(
    protocol_system: &str,
    spkg: Option<String>,
    family_runtime: Option<&FamilyRuntimeConfig>,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(spkg) = spkg {
        return Ok(spkg);
    }

    if let Some(shared_spkg) = family_runtime.and_then(|runtime| runtime.shared_spkg.clone()) {
        return Ok(shared_spkg);
    }

    Err(format!(
        "extractor for protocol system `{protocol_system}` must declare `spkg` unless its family_runtime resolves `shared_spkg`"
    )
    .into())
}

fn merge_family_runtime_config(
    protocol_system: &str,
    family_runtime: Option<FamilyRuntimeConfig>,
    family_defaults: &HashMap<String, RawFamilyRuntimeDefaults>,
) -> Result<Option<FamilyRuntimeConfig>, Box<dyn std::error::Error>> {
    let Some(family_runtime) = family_runtime else {
        return Ok(None);
    };
    let registry = tycho_indexer::extractor::family_runtime::default_family_runtime_registry();
    let defaults = family_defaults.get(&family_runtime.family);

    Ok(Some(
        registry.resolve_family_runtime_config(
            protocol_system,
            family_runtime,
            defaults.and_then(|defaults| defaults.shared_spkg.clone()),
            defaults.and_then(|defaults| defaults.shared_module.clone()),
        )?,
    ))
}

#[derive(Debug, Deserialize)]
struct SubstreamsParamsFile {
    #[serde(default)]
    start_block: Option<i64>,
    #[serde(default)]
    includes: Vec<String>,
    #[serde(default)]
    params: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct BootstrapParamsFile {
    #[serde(default)]
    start_block: Option<i64>,
    #[serde(default)]
    includes: Vec<String>,
    #[serde(default)]
    params: BootstrapParamsYaml,
}

#[derive(Debug, Default, Deserialize)]
struct BootstrapParamsYaml {
    #[serde(default)]
    bootstrap_block: Option<i64>,
    #[serde(default)]
    pools: Vec<String>,
    #[serde(default)]
    routes: Vec<BootstrapRouteYaml>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BootstrapRouteYaml {
    token0: String,
    token1: String,
    #[serde(default)]
    routers: Vec<BootstrapRouterYaml>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BootstrapRouterYaml {
    pool: String,
    protocol: String,
}

#[cfg(test)]
pub(crate) fn parse_substreams_params_yaml(
    extractor_name: &str,
    contents: &str,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    let parsed: SubstreamsParamsFile = serde_yaml::from_str(contents)?;
    let (start_block, params) = normalize_substreams_params(Some(extractor_name), parsed)?;
    let mut substreams_params = Vec::with_capacity(params.len());

    for (key, value) in params {
        let rendered_value = render_substreams_param_value(&value)?;
        substreams_params.push(format!("{key}={rendered_value}"));
    }

    Ok((start_block, substreams_params.join("&")))
}

fn resolve_substreams_params_map(
    route_protocol_filter: Option<&str>,
    resolved_start_block: &mut Option<i64>,
    substreams_params: &mut HashMap<String, String>,
    base_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for (module_name, value) in substreams_params {
        let Some(path) = value.strip_prefix('@') else {
            continue;
        };

        let params_path = base_dir.join(path);
        let (start_block, resolved_params) = parse_substreams_params_file(
            route_protocol_filter,
            &params_path,
        )
        .map_err(|err| {
                format!(
                    "failed to parse substreams config file for extractor `{}` \
                     module `{module_name}` at `{}`: {err}",
                    route_protocol_filter.unwrap_or("<unfiltered>"),
                    params_path.display()
                )
            })?;

        if let Some(start_block) = start_block {
            if let Some(existing_start_block) = resolved_start_block {
                if *existing_start_block != start_block {
                    return Err(format!(
                        "conflicting start_block values for extractor `{}`: \
                         {existing_start_block} vs {start_block} from module `{module_name}`"
                        ,
                        route_protocol_filter.unwrap_or("<unfiltered>")
                    )
                    .into());
                }
            } else {
                *resolved_start_block = Some(start_block);
            }
        }

        *value = resolved_params;
    }

    Ok(())
}

fn resolve_bootstrap_params(
    route_protocol_filter: Option<&str>,
    params_value: &mut String,
    base_dir: &Path,
) -> Result<i64, Box<dyn std::error::Error>> {
    let Some(path) = params_value.strip_prefix('@') else {
        return extract_bootstrap_block_from_query(params_value).map_err(Into::into);
    };

    let params_path = base_dir.join(path);
    let (start_block, resolved_params) =
        parse_bootstrap_params_file(route_protocol_filter, &params_path)
        .map_err(|err| {
            format!(
                "failed to parse bootstrap config file for extractor `{}` at `{}`: \
                 {err}",
                route_protocol_filter.unwrap_or("<unfiltered>"),
                params_path.display()
            )
        })?;

    let start_block = start_block.ok_or_else(|| {
        format!(
            "bootstrap config file for extractor `{}` at `{}` is missing \
             `start_block` or `params.bootstrap_block`",
            route_protocol_filter.unwrap_or("<unfiltered>"),
            params_path.display()
        )
    })?;

    *params_value = resolved_params;
    Ok(start_block)
}

#[cfg(test)]
pub(crate) fn parse_bootstrap_params_yaml(
    extractor_name: &str,
    contents: &str,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    parse_bootstrap_params_yaml_with_filter(Some(extractor_name), contents)
}

#[cfg(test)]
fn parse_bootstrap_params_yaml_with_filter(
    route_protocol_filter: Option<&str>,
    contents: &str,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    let parsed: BootstrapParamsFile = serde_yaml::from_str(contents)?;
    let bootstrap_block = match (parsed.start_block, parsed.params.bootstrap_block) {
        (Some(start_block), Some(bootstrap_block)) => {
            if start_block != bootstrap_block {
                return Err(format!(
                    "`start_block` ({start_block}) must match `params.bootstrap_block` \
                     ({bootstrap_block})"
                )
                .into());
            }
            start_block
        }
        (Some(start_block), None) => start_block,
        (None, Some(bootstrap_block)) => bootstrap_block,
        (None, None) => {
            return Err(
                "bootstrap config is missing `start_block` or `params.bootstrap_block`".into()
            )
        }
    };

    let protocol_filter = route_protocol_filter.and_then(protocol_filter_for_protocol_system);
    let all_pools = collect_bootstrap_pools(&parsed.params, protocol_filter.as_ref())?;

    if all_pools.is_empty() {
        return Err("bootstrap config is missing `params.pools` or `params.routes`".into());
    }

    Ok((
        Some(bootstrap_block),
        format!("bootstrap_block={bootstrap_block}&pools={}", all_pools.join(",")),
    ))
}

fn normalize_substreams_params(
    route_protocol_filter: Option<&str>,
    mut parsed: SubstreamsParamsFile,
) -> Result<(Option<i64>, BTreeMap<String, Value>), Box<dyn std::error::Error>> {
    if parsed.params.contains_key("routes") {
        let protocol_filter = route_protocol_filter.and_then(protocol_filter_for_protocol_system);
        let (pools, pool_tokens) =
            collect_bootstrap_pool_metadata(&parsed.params, protocol_filter.as_ref())?;
        if !pools.is_empty() {
            parsed.params.insert(
                "pools".to_string(),
                Value::Sequence(
                    pools
                        .into_iter()
                        .map(Value::String)
                        .collect(),
                ),
            );
        }
        if !pool_tokens.is_empty() {
            parsed.params.insert(
                "pool_tokens".to_string(),
                Value::Sequence(
                    pool_tokens
                        .into_iter()
                        .map(Value::String)
                        .collect(),
                ),
            );
        }
        parsed.params.remove("routes");
    }

    let bootstrap_block = parsed
        .params
        .get("bootstrap_block")
        .map(parse_i64_yaml_value)
        .transpose()?;

    let start_block = match (parsed.start_block, bootstrap_block) {
        (Some(start_block), Some(bootstrap_block)) => {
            if start_block != bootstrap_block {
                return Err(format!(
                    "`start_block` ({start_block}) must match `params.bootstrap_block` \
                     ({bootstrap_block})"
                )
                .into());
            }
            start_block
        }
        (Some(start_block), None) => {
            parsed.params.insert(
                "bootstrap_block".to_string(),
                Value::Number(serde_yaml::Number::from(start_block)),
            );
            start_block
        }
        (None, Some(bootstrap_block)) => bootstrap_block,
        (None, None) => return Ok((None, parsed.params)),
    };

    Ok((Some(start_block), parsed.params))
}

fn parse_substreams_params_file(
    route_protocol_filter: Option<&str>,
    path: &Path,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    let parsed = load_substreams_params_file(path, &mut HashSet::new())?;
    let (start_block, params) = normalize_substreams_params(route_protocol_filter, parsed)?;
    let mut substreams_params = Vec::with_capacity(params.len());

    for (key, value) in params {
        let rendered_value = render_substreams_param_value(&value)?;
        substreams_params.push(format!("{key}={rendered_value}"));
    }

    Ok((start_block, substreams_params.join("&")))
}

fn parse_bootstrap_params_file(
    route_protocol_filter: Option<&str>,
    path: &Path,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    let parsed = load_bootstrap_params_file(path, &mut HashSet::new())?;
    let bootstrap_block = match (parsed.start_block, parsed.params.bootstrap_block) {
        (Some(start_block), Some(bootstrap_block)) => {
            if start_block != bootstrap_block {
                return Err(format!(
                    "`start_block` ({start_block}) must match `params.bootstrap_block` \
                     ({bootstrap_block})"
                )
                .into());
            }
            start_block
        }
        (Some(start_block), None) => start_block,
        (None, Some(bootstrap_block)) => bootstrap_block,
        (None, None) => {
            return Err(
                "bootstrap config is missing `start_block` or `params.bootstrap_block`".into()
            )
        }
    };

    let protocol_filter = route_protocol_filter.and_then(protocol_filter_for_protocol_system);
    let all_pools = collect_bootstrap_pools(&parsed.params, protocol_filter.as_ref())?;

    if all_pools.is_empty() {
        return Err("bootstrap config is missing `params.pools` or `params.routes`".into());
    }

    Ok((
        Some(bootstrap_block),
        format!("bootstrap_block={bootstrap_block}&pools={}", all_pools.join(",")),
    ))
}

fn load_substreams_params_file(
    path: &Path,
    visited: &mut HashSet<PathBuf>,
) -> Result<SubstreamsParamsFile, Box<dyn std::error::Error>> {
    let resolved_path = canonicalize_for_include_tracking(path)?;
    if !visited.insert(resolved_path.clone()) {
        return Err(format!(
            "cyclic substreams config include detected at `{}`",
            resolved_path.display()
        )
        .into());
    }

    let contents = fs::read_to_string(&resolved_path)?;
    let parsed: SubstreamsParamsFile = serde_yaml::from_str(&contents)?;
    let base_dir = resolved_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let mut merged =
        SubstreamsParamsFile { start_block: None, includes: vec![], params: BTreeMap::new() };

    for include in &parsed.includes {
        let included =
            load_substreams_params_file(&base_dir.join(normalize_include_path(include)), visited)?;
        merge_substreams_params_file(&mut merged, included)?;
    }

    merge_substreams_params_file(
        &mut merged,
        SubstreamsParamsFile {
            start_block: parsed.start_block,
            includes: vec![],
            params: parsed.params,
        },
    )?;

    visited.remove(&resolved_path);
    Ok(merged)
}

fn load_bootstrap_params_file(
    path: &Path,
    visited: &mut HashSet<PathBuf>,
) -> Result<BootstrapParamsFile, Box<dyn std::error::Error>> {
    let resolved_path = canonicalize_for_include_tracking(path)?;
    if !visited.insert(resolved_path.clone()) {
        return Err(format!(
            "cyclic bootstrap config include detected at `{}`",
            resolved_path.display()
        )
        .into());
    }

    let contents = fs::read_to_string(&resolved_path)?;
    let parsed: BootstrapParamsFile = serde_yaml::from_str(&contents)?;
    let base_dir = resolved_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let mut merged = BootstrapParamsFile {
        start_block: None,
        includes: vec![],
        params: BootstrapParamsYaml::default(),
    };

    for include in &parsed.includes {
        let included =
            load_bootstrap_params_file(&base_dir.join(normalize_include_path(include)), visited)?;
        merge_bootstrap_params_file(&mut merged, included)?;
    }

    merge_bootstrap_params_file(
        &mut merged,
        BootstrapParamsFile {
            start_block: parsed.start_block,
            includes: vec![],
            params: parsed.params,
        },
    )?;

    visited.remove(&resolved_path);
    Ok(merged)
}

fn merge_substreams_params_file(
    target: &mut SubstreamsParamsFile,
    incoming: SubstreamsParamsFile,
) -> Result<(), Box<dyn std::error::Error>> {
    target.start_block = merge_optional_i64(
        target.start_block,
        incoming.start_block,
        "substreams config start_block",
    )?;

    for (key, incoming_value) in incoming.params {
        if let Some(existing_value) = target.params.get_mut(&key) {
            merge_substreams_param_value(existing_value, incoming_value, &key)?;
        } else {
            target
                .params
                .insert(key, incoming_value);
        }
    }

    Ok(())
}

fn merge_bootstrap_params_file(
    target: &mut BootstrapParamsFile,
    incoming: BootstrapParamsFile,
) -> Result<(), Box<dyn std::error::Error>> {
    target.start_block = merge_optional_i64(
        target.start_block,
        incoming.start_block,
        "bootstrap config start_block",
    )?;
    target.params.bootstrap_block = merge_optional_i64(
        target.params.bootstrap_block,
        incoming.params.bootstrap_block,
        "bootstrap config params.bootstrap_block",
    )?;
    target
        .params
        .pools
        .extend(incoming.params.pools);
    target
        .params
        .routes
        .extend(incoming.params.routes);
    Ok(())
}

fn merge_substreams_param_value(
    existing: &mut Value,
    incoming: Value,
    key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match (&mut *existing, incoming) {
        (Value::Sequence(existing_items), Value::Sequence(incoming_items)) => {
            existing_items.extend(incoming_items);
            Ok(())
        }
        (existing_value, incoming_value) if *existing_value == incoming_value => Ok(()),
        _ => Err(format!("conflicting substreams param values for key `{key}`").into()),
    }
}

fn merge_optional_i64(
    existing: Option<i64>,
    incoming: Option<i64>,
    context: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    match (existing, incoming) {
        (Some(existing), Some(incoming)) if existing != incoming => {
            Err(format!("conflicting values for {context}: {existing} vs {incoming}").into())
        }
        (Some(existing), _) => Ok(Some(existing)),
        (None, Some(incoming)) => Ok(Some(incoming)),
        (None, None) => Ok(None),
    }
}

fn normalize_include_path(include: &str) -> &str {
    include
        .strip_prefix('@')
        .unwrap_or(include)
}

fn canonicalize_for_include_tracking(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    path.canonicalize()
        .map_err(|err| format!("failed to resolve config path `{}`: {err}", path.display()).into())
}

fn load_raw_extractor_configs(
    path: &Path,
    visited: &mut HashSet<PathBuf>,
) -> Result<RawExtractorConfigs, Box<dyn std::error::Error>> {
    let resolved_path = canonicalize_for_include_tracking(path)?;
    if !visited.insert(resolved_path.clone()) {
        return Err(format!(
            "cyclic extractor config include detected at `{}`",
            resolved_path.display()
        )
        .into());
    }

    let mut file = File::open(&resolved_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let parsed: RawExtractorConfigs = serde_yaml::from_str(&contents)?;
    let base_dir = resolved_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let mut merged = RawExtractorConfigs {
        includes: vec![],
        family_runtimes: HashMap::new(),
        extractors: HashMap::new(),
    };

    for include in &parsed.includes {
        let included =
            load_raw_extractor_configs(&base_dir.join(normalize_include_path(include)), visited)?;
        merge_raw_extractor_configs(&mut merged, included)?;
    }

    merge_raw_extractor_configs(
        &mut merged,
        RawExtractorConfigs {
            includes: vec![],
            family_runtimes: parsed.family_runtimes,
            extractors: parsed.extractors,
        },
    )?;

    visited.remove(&resolved_path);
    Ok(merged)
}

fn merge_raw_extractor_configs(
    target: &mut RawExtractorConfigs,
    incoming: RawExtractorConfigs,
) -> Result<(), Box<dyn std::error::Error>> {
    for (family_name, defaults) in incoming.family_runtimes {
        if target
            .family_runtimes
            .insert(family_name.clone(), defaults)
            .is_some()
        {
            return Err(format!("duplicate family_runtime definition for `{family_name}`").into());
        }
    }

    for (extractor_id, extractor) in incoming.extractors {
        if target
            .extractors
            .insert(extractor_id.clone(), extractor)
            .is_some()
        {
            return Err(format!("duplicate extractor definition for `{extractor_id}`").into());
        }
    }
    Ok(())
}

fn render_substreams_param_value(value: &Value) -> Result<String, Box<dyn std::error::Error>> {
    match value {
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(value.clone()),
        Value::Sequence(values) => values
            .iter()
            .map(render_substreams_scalar_value)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| values.join(",")),
        Value::Null => Err("null is not a supported substreams param value".into()),
        Value::Mapping(_) | Value::Tagged(_) => {
            Err("nested YAML objects are not supported in substreams params".into())
        }
    }
}

fn render_substreams_scalar_value(value: &Value) -> Result<String, Box<dyn std::error::Error>> {
    match value {
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(value.clone()),
        Value::Null => Err("null is not a supported substreams param list item".into()),
        Value::Sequence(_) | Value::Mapping(_) | Value::Tagged(_) => {
            Err("substreams param lists may only contain scalar values".into())
        }
    }
}

fn collect_bootstrap_pool_metadata(
    params: &BTreeMap<String, Value>,
    allowed_protocols: Option<&HashSet<String>>,
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
    let all_pools = collect_bootstrap_pools_from_parts(
        params
            .get("pools")
            .map(parse_string_sequence_yaml_value)
            .transpose()?
            .unwrap_or_default(),
        params
            .get("routes")
            .cloned()
            .unwrap_or(Value::Sequence(vec![])),
        allowed_protocols,
    )?;

    let routes = params
        .get("routes")
        .cloned()
        .unwrap_or(Value::Sequence(vec![]));
    let routes: Vec<BootstrapRouteYaml> = serde_yaml::from_value(routes)?;

    let mut pool_tokens = Vec::new();
    let mut seen_pool_tokens = HashSet::new();

    for route in routes {
        for router in route.routers {
            if !router_matches_allowed_protocols(router.protocol.as_str(), allowed_protocols) {
                continue;
            }

            let pool_token = format!("{}:{}:{}", router.pool, route.token0, route.token1);
            if seen_pool_tokens.insert(pool_token.clone()) {
                pool_tokens.push(pool_token);
            }
        }
    }

    Ok((all_pools, pool_tokens))
}

fn collect_bootstrap_pools(
    params: &BootstrapParamsYaml,
    allowed_protocols: Option<&HashSet<String>>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    collect_bootstrap_pools_from_parts(
        params.pools.clone(),
        serde_yaml::to_value(&params.routes)?,
        allowed_protocols,
    )
}

fn collect_bootstrap_pools_from_parts(
    pools: Vec<String>,
    routes: Value,
    allowed_protocols: Option<&HashSet<String>>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let routes: Vec<BootstrapRouteYaml> = serde_yaml::from_value(routes)?;

    let mut seen_pools = HashSet::new();
    let mut all_pools = Vec::new();

    for pool in pools {
        if seen_pools.insert(pool.clone()) {
            all_pools.push(pool);
        }
    }

    for route in routes {
        for router in route.routers {
            let BootstrapRouterYaml { pool, protocol } = router;
            if !router_matches_allowed_protocols(protocol.as_str(), allowed_protocols) {
                continue;
            }
            if seen_pools.insert(pool.clone()) {
                all_pools.push(pool);
            }
        }
    }

    Ok(all_pools)
}

fn protocol_filter_for_protocol_system(protocol_system: &str) -> Option<HashSet<String>> {
    tycho_indexer::extractor::family_runtime::default_family_runtime_registry()
        .normalized_shared_route_protocol_filter_for_protocol_system(protocol_system)
}

fn router_matches_allowed_protocols(
    router_protocol: &str,
    allowed_protocols: Option<&HashSet<String>>,
) -> bool {
    let Some(allowed_protocols) = allowed_protocols else {
        return true;
    };

    allowed_protocols.contains(
        &tycho_indexer::extractor::family_runtime::canonicalize_shared_route_protocol(
            router_protocol,
        ),
    )
}

fn parse_i64_yaml_value(value: &Value) -> Result<i64, Box<dyn std::error::Error>> {
    match value {
        Value::Number(value) => value
            .as_i64()
            .ok_or_else(|| "numeric YAML value does not fit into i64".into()),
        Value::String(value) => Ok(value.parse()?),
        Value::Bool(_)
        | Value::Null
        | Value::Sequence(_)
        | Value::Mapping(_)
        | Value::Tagged(_) => Err("block parameters must be scalar integers".into()),
    }
}

fn parse_string_sequence_yaml_value(
    value: &Value,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    match value {
        Value::Sequence(values) => values
            .iter()
            .map(|value| match value {
                Value::String(value) => Ok(value.clone()),
                _ => Err("pool parameters must be string values".into()),
            })
            .collect(),
        _ => Err("pool parameters must be a list of strings".into()),
    }
}

fn extract_bootstrap_block_from_query(params: &str) -> Result<i64, Box<dyn std::error::Error>> {
    for pair in params
        .split('&')
        .filter(|part| !part.is_empty())
    {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(format!("invalid bootstrap param `{pair}`").into());
        };

        if key == "bootstrap_block" {
            return Ok(value.parse()?);
        }
    }

    Err("bootstrap params must include `bootstrap_block`".into())
}

#[cfg(test)]
mod tests {
    use std::{fs, process};

    use super::*;

    #[test]
    fn extractor_configs_load_substreams_params_from_file() {
        let temp_root =
            std::env::temp_dir().join(format!("tycho-indexer-substreams-params-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/uniswap_v3_bootstrap.yaml"),
            r#"
start_block: 1
params:
  pools:
    - "0xabc"
"#,
        )
        .expect("write config file");
        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
extractors:
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_protocol_changes"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/uniswap_v3_bootstrap.yaml"
"#,
        )
        .expect("write extractor config");

        let config = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load extractor configs");

        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .map(ExtractorConfig::start_block),
            Some(1)
        );
        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .and_then(|extractor| extractor.bootstrap.as_ref())
                .map(|bootstrap| bootstrap.strategy.clone()),
            Some(BootstrapStrategy::UniswapV3Rpc)
        );
        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .and_then(|extractor| extractor.bootstrap.as_ref())
                .map(|bootstrap| bootstrap.params.as_str()),
            Some("bootstrap_block=1&pools=0xabc")
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_support_recursive_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-extractor-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("fragments")).expect("create temp fragment dir");

        fs::write(
            temp_root.join("fragments/uniswap_v2.yaml"),
            r#"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
"#,
        )
        .expect("write v2 fragment");
        fs::write(
            temp_root.join("fragments/uniswap_v3.yaml"),
            r#"
extractors:
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 43
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
"#,
        )
        .expect("write v3 fragment");
        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
includes:
  - "fragments/uniswap_v2.yaml"
  - "fragments/uniswap_v3.yaml"
extractors: {}
"#,
        )
        .expect("write extractor root");

        let config = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load included extractor configs");

        assert_eq!(
            config
                .extractors
                .get("uniswap_v2")
                .map(ExtractorConfig::start_block),
            Some(42)
        );
        assert_eq!(
            config
                .extractors
                .get("uniswap_v3")
                .map(ExtractorConfig::start_block),
            Some(43)
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn bootstrap_config_supports_recursive_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-bootstrap-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/v2.yaml"),
            r#"
start_block: 42
params:
  routes:
    - token0: "0xaaaa"
      token1: "0xbbbb"
      routers:
        - pool: "0x1111"
          protocol: uniswap_v2
"#,
        )
        .expect("write v2 include");
        fs::write(
            temp_root.join("config/v3.yaml"),
            r#"
start_block: 42
params:
  routes:
    - token0: "0xcccc"
      token1: "0xdddd"
      routers:
        - pool: "0x2222"
          protocol: uniswap_v3
"#,
        )
        .expect("write v3 include");
        fs::write(
            temp_root.join("config/shared.yaml"),
            r#"
includes:
  - "v2.yaml"
  - "v3.yaml"
"#,
        )
        .expect("write shared include");

        let (v2_start_block, v2_params) =
            parse_bootstrap_params_file(Some("uniswap_v2"), &temp_root.join("config/shared.yaml"))
                .expect("parse v2 shared include");
        let (v3_start_block, v3_params) =
            parse_bootstrap_params_file(Some("uniswap_v3"), &temp_root.join("config/shared.yaml"))
                .expect("parse v3 shared include");

        assert_eq!(v2_start_block, Some(42));
        assert_eq!(v3_start_block, Some(42));
        assert_eq!(v2_params, "bootstrap_block=42&pools=0x1111");
        assert_eq!(v3_params, "bootstrap_block=42&pools=0x2222");

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn substreams_config_supports_recursive_includes() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-substreams-includes-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/shared.yaml"),
            r#"
start_block: 42
params:
  routes:
    - token0: "0xaaaa"
      token1: "0xbbbb"
      routers:
        - pool: "0x1111"
          protocol: uniswap_v2
        - pool: "0x2222"
          protocol: uniswap_v3
"#,
        )
        .expect("write shared include");
        fs::write(
            temp_root.join("config/v2-substreams.yaml"),
            r#"
includes:
  - "shared.yaml"
params:
  extra_flag: "enabled"
"#,
        )
        .expect("write v2 overlay");

        let (start_block, params) = parse_substreams_params_file(
            Some("uniswap_v2"),
            &temp_root.join("config/v2-substreams.yaml"),
        )
        .expect("parse v2 substreams include");

        assert_eq!(start_block, Some(42));
        assert_eq!(
            params,
            "bootstrap_block=42&extra_flag=enabled&pool_tokens=0x1111:0xaaaa:0xbbbb&pools=0x1111"
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn bootstrap_config_supports_route_format() {
        let (start_block, params) = parse_bootstrap_params_yaml(
            "test_protocol",
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
          protocol: bebop
        - pool: "0x8710039d5de6840ede452a85672b32270a709ae2"
          protocol: fluid
        - pool: "0xc1cd3d0913f4633b43fcddbcd7342bc9b71c676f"
          protocol: uniswapv3
"#,
        )
        .expect("route-format bootstrap should parse");

        assert_eq!(start_block, Some(25377208));
        assert_eq!(
            params,
            "bootstrap_block=25377208&pools=0x6f40d4a6237c257fff2db00fa0510deeecd303eb,0x8710039d5de6840ede452a85672b32270a709ae2,0xc1cd3d0913f4633b43fcddbcd7342bc9b71c676f"
        );
    }

    #[test]
    fn extractor_configs_reject_mismatched_start_and_bootstrap_blocks() {
        let err = parse_substreams_params_yaml(
            "test_protocol",
            r#"
start_block: 1
params:
  bootstrap_block: 2
  pools:
    - "0xabc"
"#,
        )
        .expect_err("mismatched config should fail");

        assert!(err.to_string().contains("must match"));
    }

    #[test]
    fn substreams_params_support_route_format_with_pool_token_metadata() {
        let (start_block, params) = parse_substreams_params_yaml(
            "uniswap_v2",
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
          protocol: uniswap_v2
        - pool: "0x8710039d5de6840ede452a85672b32270a709ae2"
          protocol: uniswap_v2
"#,
        )
        .expect("route-format substreams params should parse");

        assert_eq!(start_block, Some(25377208));
        assert_eq!(
            params,
            "bootstrap_block=25377208&pool_tokens=0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0x8710039d5de6840ede452a85672b32270a709ae2:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2&pools=0x6f40d4a6237c257fff2db00fa0510deeecd303eb,0x8710039d5de6840ede452a85672b32270a709ae2"
        );
    }

    #[test]
    fn bootstrap_route_format_filters_by_extractor_protocol() {
        let contents = r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#;

        let (_, v2_params) =
            parse_bootstrap_params_yaml("uniswap_v2", contents).expect("v2 bootstrap should parse");
        let (_, v3_params) =
            parse_bootstrap_params_yaml("uniswap_v3", contents).expect("v3 bootstrap should parse");

        assert_eq!(
            v2_params,
            "bootstrap_block=25377208&pools=0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            v3_params,
            "bootstrap_block=25377208&pools=0x2222222222222222222222222222222222222222"
        );
    }

    #[test]
    fn substreams_route_format_filters_by_extractor_protocol() {
        let contents = r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#;

        let (_, v2_params) = parse_substreams_params_yaml("uniswap_v2", contents)
            .expect("v2 substreams params should parse");
        let (_, v3_params) = parse_substreams_params_yaml("uniswap_v3", contents)
            .expect("v3 substreams params should parse");

        assert_eq!(
            v2_params,
            "bootstrap_block=25377208&pool_tokens=0x1111111111111111111111111111111111111111:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2&pools=0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            v3_params,
            "bootstrap_block=25377208&pool_tokens=0x2222222222222222222222222222222222222222:0x6f40d4a6237c257fff2db00fa0510deeecd303eb:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2&pools=0x2222222222222222222222222222222222222222"
        );
    }

    #[test]
    fn extractor_configs_keep_v2_params_consistent_between_v2_only_and_v2_v3() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-uniswap-shared-bootstrap-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/shared_uniswap_bootstrap.yaml"),
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#,
        )
        .expect("write shared bootstrap config");

        fs::write(
            temp_root.join("extractors.uniswap_v2.yaml"),
            r#"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
    substreams_params:
      map_pool_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
        )
        .expect("write v2 extractor config");

        fs::write(
            temp_root.join("extractors.uniswap_v2_v3.yaml"),
            r#"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
    substreams_params:
      map_pool_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
    substreams_params:
      map_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
        )
        .expect("write v2+v3 extractor config");

        let v2_only = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.uniswap_v2.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v2-only extractor config");
        let v2_v3 = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v2+v3 extractor config");

        let v2_only_extractor = v2_only
            .extractors
            .get("uniswap_v2")
            .expect("v2-only extractor present");
        let v2_v3_extractor = v2_v3
            .extractors
            .get("uniswap_v2")
            .expect("v2 extractor present in combined config");

        assert_eq!(v2_only_extractor.start_block(), v2_v3_extractor.start_block());
        assert_eq!(
            v2_only_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            v2_v3_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            v2_only_extractor
                .substreams_params
                .get("map_pool_events"),
            v2_v3_extractor
                .substreams_params
                .get("map_pool_events")
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_keep_v2_params_consistent_between_default_and_combined() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-uniswap-default-parity-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/shared_uniswap_bootstrap.yaml"),
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#,
        )
        .expect("write shared bootstrap config");

        fs::write(
            temp_root.join("extractors.yaml"),
            r#"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
    substreams_params:
      map_pool_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
        )
        .expect("write default extractor config");

        fs::write(
            temp_root.join("extractors.uniswap_v2_v3.yaml"),
            r#"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
    substreams_params:
      map_pool_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
        )
        .expect("write combined extractor config");

        let default_config = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load default extractor config");
        let combined_config = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load combined extractor config");

        let default_v2 = default_config
            .extractors
            .get("uniswap_v2")
            .expect("default v2 extractor present");
        let combined_v2 = combined_config
            .extractors
            .get("uniswap_v2")
            .expect("combined v2 extractor present");

        assert_eq!(default_v2.start_block(), combined_v2.start_block());

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn extractor_configs_keep_v3_params_consistent_between_v3_only_and_v2_v3() {
        let temp_root = std::env::temp_dir()
            .join(format!("tycho-indexer-uniswap-v3-shared-bootstrap-{}", process::id()));
        let _ = fs::remove_dir_all(&temp_root);
        fs::create_dir_all(temp_root.join("config")).expect("create temp config dir");

        fs::write(
            temp_root.join("config/shared_uniswap_bootstrap.yaml"),
            r#"
start_block: 25377208
params:
  routes:
    - token0: "0x6f40d4a6237c257fff2db00fa0510deeecd303eb"
      token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      routers:
        - pool: "0x1111111111111111111111111111111111111111"
          protocol: uniswap_v2
        - pool: "0x2222222222222222222222222222222222222222"
          protocol: uniswap_v3
"#,
        )
        .expect("write shared bootstrap config");

        fs::write(
            temp_root.join("extractors.uniswap_v3.yaml"),
            r#"
extractors:
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
    substreams_params:
      map_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
        )
        .expect("write v3 extractor config");

        fs::write(
            temp_root.join("extractors.uniswap_v2_v3.yaml"),
            r#"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_pool_events"
    substreams_params:
      map_pool_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    spkg: "stream.spkg"
    module_name: "map_events"
    substreams_params:
      map_events: "@config/shared_uniswap_bootstrap.yaml"
    bootstrap:
      strategy: "uniswap_v3_rpc"
      params: "@config/shared_uniswap_bootstrap.yaml"
"#,
        )
        .expect("write v2+v3 extractor config");

        let v3_only = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.uniswap_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v3-only extractor config");
        let v2_v3 = ExtractorConfigs::from_yaml(
            temp_root
                .join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("load v2+v3 extractor config");

        let v3_only_extractor = v3_only
            .extractors
            .get("uniswap_v3")
            .expect("v3-only extractor present");
        let v2_v3_extractor = v2_v3
            .extractors
            .get("uniswap_v3")
            .expect("v3 extractor present in combined config");

        assert_eq!(v3_only_extractor.start_block(), v2_v3_extractor.start_block());
        assert_eq!(
            v3_only_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            v2_v3_extractor
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            v3_only_extractor
                .substreams_params
                .get("map_events"),
            v2_v3_extractor
                .substreams_params
                .get("map_events")
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn repo_uniswap_v2_configs_stay_consistent_across_entrypoints() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let default_config = ExtractorConfigs::from_yaml(
            root.join("extractors.yaml")
                .to_str()
                .expect("utf8 default config path"),
        )
        .expect("load default extractors config");
        let v2_only_config = ExtractorConfigs::from_yaml(
            root.join("extractors.uniswap_v2.yaml")
                .to_str()
                .expect("utf8 v2-only config path"),
        )
        .expect("load v2-only extractors config");
        let combined_config = ExtractorConfigs::from_yaml(
            root.join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 combined config path"),
        )
        .expect("load combined extractors config");
        let combined_substream_config = ExtractorConfigs::from_yaml(
            root.join("extractors.uniswap_v2_v3.combined.yaml")
                .to_str()
                .expect("utf8 combined-substream config path"),
        )
        .expect("load combined-substream extractors config");

        let default_v2 = default_config
            .extractors
            .get("uniswap_v2")
            .expect("default v2 extractor present");
        let v2_only = v2_only_config
            .extractors
            .get("uniswap_v2")
            .expect("v2-only extractor present");
        let combined_v2 = combined_config
            .extractors
            .get("uniswap_v2")
            .expect("combined v2 extractor present");
        let combined_substream_v2 = combined_substream_config
            .extractors
            .get("uniswap_v2")
            .expect("combined-substream v2 extractor present");

        assert_eq!(default_v2.start_block(), v2_only.start_block());
        assert_eq!(default_v2.start_block(), combined_v2.start_block());
        assert_eq!(default_v2.start_block(), combined_substream_v2.start_block());
        assert_eq!(
            default_v2
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            v2_only
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            default_v2
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            combined_v2
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            default_v2
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            combined_v2
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            default_v2
                .substreams_params
                .get("map_pool_events"),
            v2_only
                .substreams_params
                .get("map_pool_events")
        );
        assert_eq!(
            default_v2
                .substreams_params
                .get("map_pool_events"),
            combined_v2
                .substreams_params
                .get("map_pool_events")
        );
        assert_eq!(
            default_v2
                .substreams_params
                .get("map_pool_events"),
            combined_v2
                .substreams_params
                .get("map_pool_events")
        );
        let combined_substream_v2_bootstrap = combined_substream_v2
            .bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.params.clone())
            .expect("combined-substream v2 bootstrap params present");
        assert!(
            combined_substream_v2_bootstrap.contains("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"),
            "combined-substream v2 bootstrap should keep v2 pools"
        );
        assert!(
            !combined_substream_v2_bootstrap.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "combined-substream v2 bootstrap should exclude v3 pools"
        );
        let combined_substream_v2_params = combined_substream_v2
            .substreams_params
            .get("v2_map_pool_events")
            .expect("combined-substream v2 params present");
        assert!(
            combined_substream_v2_params.contains("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"),
            "combined-substream v2 params should keep v2 pools"
        );
        assert!(
            !combined_substream_v2_params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "combined-substream v2 params should exclude v3 pools"
        );

        let combined_substream_yaml =
            fs::read_to_string(root.join("extractors.uniswap_v2_v3.combined.yaml"))
                .expect("read combined-substream config");
        let combined_v2_fragment =
            fs::read_to_string(root.join("extractors.fragments/uniswap_v2_combined.yaml"))
                .expect("read combined-substream v2 fragment");

        assert!(
            combined_substream_yaml.contains("extractors.fragments/uniswap_v2_combined.yaml"),
            "combined-substream config should include the v2 combined fragment"
        );
        assert!(
            combined_v2_fragment.contains("module_name: \"v2_map_pool_events\""),
            "combined-substream v2 fragment should point at the combined package module"
        );
        assert!(
            combined_substream_yaml.contains("members:"),
            "combined-substream config should declare top-level family member defaults"
        );
        assert!(
            combined_substream_yaml.contains("v2_map_pool_events: \"@config/uniswap_v2_substreams.yaml\""),
            "combined-substream config should centralize v2 substreams params at the family level"
        );
        assert!(
            combined_substream_yaml.contains("v3_map_events: \"@config/uniswap_v3_substreams.yaml\""),
            "combined-substream config should centralize v3 substreams params at the family level"
        );
        assert!(combined_substream_yaml.contains("family_runtimes:"));
    }

    #[test]
    fn repo_uniswap_bootstrap_files_share_start_block() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let v2_bootstrap = fs::read_to_string(root.join("config/uniswap_v2_bootstrap.yaml"))
            .expect("read v2 bootstrap config");
        let v3_bootstrap = fs::read_to_string(root.join("config/uniswap_v3_bootstrap.yaml"))
            .expect("read v3 bootstrap config");

        let v2_start_block = parse_substreams_params_yaml("uniswap_v2", &v2_bootstrap)
            .expect("parse v2 bootstrap config")
            .0
            .expect("v2 bootstrap start_block present");
        let v3_start_block = parse_bootstrap_params_yaml("uniswap_v3", &v3_bootstrap)
            .expect("parse v3 bootstrap config")
            .0
            .expect("v3 bootstrap start_block present");

        assert_eq!(v2_start_block, v3_start_block);
    }

    #[test]
    fn repo_combined_uniswap_config_builds_one_family_runtime_plan() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let combined_config = ExtractorConfigs::from_yaml(
            root.join("extractors.uniswap_v2_v3.combined.yaml")
                .to_str()
                .expect("utf8 combined-substream config path"),
        )
        .expect("load combined-substream extractors config");
        let standard_config = ExtractorConfigs::from_yaml(
            root.join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 standard combined config path"),
        )
        .expect("load standard combined extractors config");

        let combined_plan = tycho_indexer::extractor::family_runtime::build_family_runtime_plan(
            &combined_config.extractors,
        )
        .expect("combined config should build a family runtime plan");
        let standard_plan = tycho_indexer::extractor::family_runtime::build_family_runtime_plan(
            &standard_config.extractors,
        )
        .expect("standard config should build a runtime plan");

        assert_eq!(combined_plan.families.len(), 1);
        assert_eq!(combined_plan.families[0].family_name, "uniswap");
        assert_eq!(
            combined_plan.families[0].member_protocol_systems,
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
        assert!(combined_plan.families[0]
            .shared_spkg
            .contains("ethereum-uniswap-v2-v3-combined"));
        assert_eq!(combined_plan.families[0].output_module, "map_uniswap_family_protocol_changes");
        assert!(combined_plan
            .standalone_protocol_systems
            .is_empty());

        assert!(
            standard_plan.families.is_empty(),
            "non-combined V2+V3 config should keep independent stream sessions"
        );
        assert_eq!(
            standard_plan.standalone_protocol_systems,
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );
    }

    #[test]
    fn extractor_config_defaults_protocol_system_to_name() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-extractor-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  sample:
    name: "sample"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config.extractors.get("sample").expect("sample extractor present");

        assert_eq!(extractor.name(), "sample");
        assert_eq!(extractor.protocol_system(), "sample");

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_family_runtime_defaults_from_top_level() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    spkg: "sample-v3.spkg"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");
        let family_runtime = extractor
            .family_runtime()
            .expect("family runtime present");

        assert_eq!(family_runtime.family, "uniswap");
        assert_eq!(
            family_runtime.shared_spkg.as_deref(),
            Some("protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg")
        );
        assert_eq!(
            family_runtime.shared_module.as_deref(),
            Some("map_uniswap_family_protocol_changes")
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn top_level_family_runtime_defaults_do_not_enable_shared_runtime_without_member_opt_in() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-no-opt-in-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample-v2.spkg"
    module_name: "map_sample_v2"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample-v3.spkg"
    module_name: "map_sample_v3"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let plan = tycho_indexer::extractor::family_runtime::build_family_runtime_plan(
            &config.extractors,
        )
        .expect("build runtime plan");

        assert!(
            plan.families.is_empty(),
            "top-level family defaults alone must not opt members into the shared runtime"
        );
        assert_eq!(
            plan.standalone_protocol_systems,
            vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()]
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn config_load_rejects_explicit_family_runtime_with_missing_member_extractor() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-missing-member-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample_v2"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write config");

        let err = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect_err("missing family member should fail during config load");

        assert!(err
            .to_string()
            .contains("requires every declared member extractor to be present once any member opts into the shared runtime"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn config_load_rejects_partial_shared_bootstrap_family_config() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-partial-bootstrap-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample_v2"
    family_runtime:
      family: "uniswap"
    bootstrap:
      strategy: "uniswap_v2_rpc"
      params: "bootstrap_block=42&pools=0x0000000000000000000000000000000000000001"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write config");

        let err = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect_err("partial shared bootstrap config should fail during config load");

        assert!(err
            .to_string()
            .contains("requires shared bootstrap configuration consistency across members"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_spkg_from_family_runtime_defaults() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-spkg-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");

        assert_eq!(
            extractor.spkg(),
            "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_stop_block_from_family_runtime_defaults() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-stop-block-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
    stop_block: 1234
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");

        assert_eq!(extractor.stop_block(), Some(1234));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_explicit_stop_block_overrides_family_runtime_default() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-stop-block-override-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
    stop_block: 1234
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    stop_block: 5678
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");

        assert_eq!(extractor.stop_block(), Some(5678));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_family_member_substreams_params_from_top_level() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let config_path = root.join(format!(
            "tycho-indexer-family-member-substreams-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
    members:
      uniswap_v2:
        substreams_params:
          v2_map_pool_events: "@config/uniswap_v2_substreams.yaml"
extractors:
  alias_v2:
    name: "alias_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config
            .extractors
            .get("alias_v2")
            .expect("aliased v2 extractor present");
        let params = extractor
            .substreams_params
            .get("v2_map_pool_events")
            .expect("resolved params present");

        assert!(
            params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "inherited family member defaults should resolve v2 pool filters"
        );
        assert!(
            !params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "v2 inherited params should exclude v3 pools"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_rejects_unknown_family_runtime() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-unknown-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "nonexistent_family"
"#,
        )
        .expect("write config");

        let err = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect_err("unknown family runtime should fail");

        assert!(err
            .to_string()
            .contains("family_runtime `nonexistent_family` does not match any registered family runtime"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn top_level_family_runtime_defaults_reject_unknown_family() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-defaults-unknown-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
family_runtimes:
  nonexistent_family:
    shared_spkg: "protocols/substreams/nonexistent/test.spkg"
    shared_module: "map_nonexistent_family_protocol_changes"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
"#,
        )
        .expect("write config");

        let err = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect_err("unknown top-level family runtime should fail");

        assert!(err
            .to_string()
            .contains("family_runtime `nonexistent_family` does not match any registered family runtime"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_rejects_protocol_system_not_in_declared_family() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-membership-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  sample_curve:
    name: "sample_curve"
    protocol_system: "curve"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
      shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
      shared_module: "map_uniswap_family_protocol_changes"
"#,
        )
        .expect("write config");

        let err = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect_err("mismatched protocol family should fail");

        assert!(err
            .to_string()
            .contains("cannot be applied to protocol system `curve` because that protocol is not a declared member of the family"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_requires_spkg_without_family_shared_spkg() {
        let config_path = std::env::temp_dir().join(format!(
            "tycho-indexer-family-runtime-spkg-missing-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    start_block: 42
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    module_name: "map_sample"
"#,
        )
        .expect("write config");

        let err = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect_err("missing spkg should fail");

        assert!(err
            .to_string()
            .contains("must declare `spkg` unless its family_runtime resolves `shared_spkg`"));

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn extractor_config_inherits_family_bootstrap_defaults_from_top_level() {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        );
        let config_path =
            std::env::temp_dir().join(format!("tycho-indexer-family-bootstrap-config-{unique}.yaml"));
        let bootstrap_path = std::env::temp_dir()
            .join(format!("tycho-indexer-family-bootstrap-params-{unique}.yaml"));

        fs::write(
            &bootstrap_path,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0x0000000000000000000000000000000000000003"
          protocol: "uniswapv2"
        - pool: "0x0000000000000000000000000000000000000004"
          protocol: "uniswapv3"
"#,
        )
        .expect("write family bootstrap params");
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
    bootstrap:
      params: "@{}"
extractors:
  sample_v2:
    name: "sample_v2"
    protocol_system: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
  sample_v3:
    name: "sample_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool_v3"
        financial_type: "Swap"
    spkg: "sample-v3.spkg"
    module_name: "map_sample_v3"
    family_runtime:
      family: "uniswap"
"#,
                bootstrap_path.display()
            ),
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config
            .extractors
            .get("sample_v2")
            .expect("sample extractor present");
        let bootstrap = extractor.bootstrap.as_ref().expect("bootstrap present");
        let v3_extractor = config
            .extractors
            .get("sample_v3")
            .expect("sample v3 extractor present");
        let v3_bootstrap = v3_extractor
            .bootstrap
            .as_ref()
            .expect("sample v3 bootstrap present");

        assert_eq!(bootstrap.strategy, BootstrapStrategy::UniswapV2Rpc);
        assert_eq!(bootstrap.start_block, 42);
        assert_eq!(
            bootstrap.params,
            "bootstrap_block=42&pools=0x0000000000000000000000000000000000000003"
        );
        assert_eq!(v3_bootstrap.strategy, BootstrapStrategy::UniswapV3Rpc);
        assert_eq!(v3_bootstrap.start_block, 42);
        assert_eq!(
            v3_bootstrap.params,
            "bootstrap_block=42&pools=0x0000000000000000000000000000000000000004"
        );

        let _ = fs::remove_file(config_path);
        let _ = fs::remove_file(bootstrap_path);
    }

    #[test]
    fn family_bootstrap_defaults_reject_protocol_not_declared_in_family() {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        );
        let config_path = std::env::temp_dir()
            .join(format!("tycho-indexer-family-bootstrap-mismatch-{unique}.yaml"));
        let bootstrap_path = std::env::temp_dir()
            .join(format!("tycho-indexer-family-bootstrap-mismatch-params-{unique}.yaml"));

        fs::write(
            &bootstrap_path,
            r#"
start_block: 42
params:
  bootstrap_block: 42
  routes:
    - token0: "0x0000000000000000000000000000000000000001"
      token1: "0x0000000000000000000000000000000000000002"
      routers:
        - pool: "0x0000000000000000000000000000000000000003"
          protocol: "uniswapv2"
"#,
        )
        .expect("write family bootstrap params");
        fs::write(
            &config_path,
            format!(
                r#"
family_runtimes:
  uniswap:
    shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
    shared_module: "map_uniswap_family_protocol_changes"
    bootstrap:
      params: "@{}"
extractors:
  sample_curve:
    name: "sample_curve"
    protocol_system: "curve"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
"#,
                bootstrap_path.display()
            ),
        )
        .expect("write config");

        let err = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect_err("protocol outside family should fail");

        assert!(err
            .to_string()
            .contains("protocol system `curve` is not a declared member of that family"));

        let _ = fs::remove_file(config_path);
        let _ = fs::remove_file(bootstrap_path);
    }

    #[test]
    fn shared_route_filter_uses_protocol_system_not_extractor_key() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let config_path = root.join(format!(
            "tycho-indexer-protocol-filter-config-{}-{}.yaml",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        fs::write(
            &config_path,
            r#"
extractors:
  alias_v3:
    name: "alias_v3"
    protocol_system: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "sample_pool"
        financial_type: "Swap"
    spkg: "sample.spkg"
    module_name: "map_sample"
    family_runtime:
      family: "uniswap"
      shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
      shared_module: "map_uniswap_family_protocol_changes"
    substreams_params:
      v3_map_events: "@config/shared_uniswap_bootstrap.yaml"
"#,
        )
        .expect("write config");

        let config = ExtractorConfigs::from_yaml(config_path.to_str().expect("utf8 config path"))
            .expect("load config");
        let extractor = config
            .extractors
            .get("alias_v3")
            .expect("aliased v3 extractor present");
        let params = extractor
            .substreams_params
            .get("v3_map_events")
            .expect("resolved params present");

        assert!(
            params.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "v3 params should keep v3 pools from shared bootstrap routes"
        );
        assert!(
            !params.contains("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"),
            "v3 params should exclude v2 pools even when extractor key is aliased"
        );

        let _ = fs::remove_file(config_path);
    }

    #[test]
    fn protocol_filter_for_protocol_system_comes_from_family_registry() {
        assert_eq!(
            protocol_filter_for_protocol_system("uniswap_v2"),
            Some(HashSet::from(["uniswapv2".to_string()]))
        );
        assert_eq!(
            protocol_filter_for_protocol_system("uniswap_v3"),
            Some(HashSet::from(["uniswapv3".to_string()]))
        );
        assert_eq!(protocol_filter_for_protocol_system("curve"), None);
    }

    #[test]
    fn repo_uniswap_v3_configs_stay_consistent_across_entrypoints() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let default_config = ExtractorConfigs::from_yaml(
            root.join("extractors.yaml")
                .to_str()
                .expect("utf8 default config path"),
        )
        .expect("load default extractors config");
        let combined_config = ExtractorConfigs::from_yaml(
            root.join("extractors.uniswap_v2_v3.yaml")
                .to_str()
                .expect("utf8 combined config path"),
        )
        .expect("load combined extractors config");
        let combined_substream_config = ExtractorConfigs::from_yaml(
            root.join("extractors.uniswap_v2_v3.combined.yaml")
                .to_str()
                .expect("utf8 combined-substream config path"),
        )
        .expect("load combined-substream extractors config");

        let default_v3 = default_config
            .extractors
            .get("uniswap_v3")
            .expect("default v3 extractor present");
        let combined_v3 = combined_config
            .extractors
            .get("uniswap_v3")
            .expect("combined v3 extractor present");
        let combined_substream_v3 = combined_substream_config
            .extractors
            .get("uniswap_v3")
            .expect("combined-substream v3 extractor present");

        assert_eq!(default_v3.start_block(), combined_v3.start_block());
        assert_eq!(default_v3.start_block(), combined_substream_v3.start_block());
        assert_eq!(
            default_v3
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone()),
            combined_v3
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.params.clone())
        );
        assert_eq!(
            default_v3
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.start_block),
            combined_v3
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.start_block)
        );
        assert_eq!(
            default_v3
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.start_block),
            combined_substream_v3
                .bootstrap
                .as_ref()
                .map(|bootstrap| bootstrap.start_block)
        );
        let combined_substream_v3_bootstrap = combined_substream_v3
            .bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.params.clone())
            .expect("combined-substream v3 bootstrap params present");
        assert!(
            combined_substream_v3_bootstrap.contains("0xe0554a476a092703abdb3ef35c80e0d76d32939f"),
            "combined-substream v3 bootstrap should keep v3 pools"
        );
        assert!(
            !combined_substream_v3_bootstrap.contains("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"),
            "combined-substream v3 bootstrap should exclude v2 pools"
        );
        let combined_v3_events_params = combined_substream_v3
            .substreams_params
            .get("v3_map_events")
            .expect("combined-substream v3 map_events params present");
        assert!(
            combined_v3_events_params
                .contains("factory=0x1F98431c8aD98523631AE4a59f267346ea31F984"),
            "combined-substream v3 map_events params should preserve the factory filter"
        );
        assert!(
            combined_v3_events_params.contains("&pools="),
            "combined-substream v3 map_events params should add an explicit pool allowlist"
        );

        let combined_substream_yaml =
            fs::read_to_string(root.join("extractors.uniswap_v2_v3.combined.yaml"))
                .expect("read combined-substream config");
        let combined_v3_fragment = fs::read_to_string(
            root.join("extractors.fragments/uniswap_v3_combined_protocol_changes.yaml"),
        )
        .expect("read combined-substream v3 fragment");
        let combined_v3_substreams =
            fs::read_to_string(root.join("config/uniswap_v3_substreams.yaml"))
                .expect("read combined-substream v3 substreams config");

        assert!(
            combined_substream_yaml
                .contains("extractors.fragments/uniswap_v3_combined_protocol_changes.yaml"),
            "combined-substream config should include the v3 combined fragment"
        );
        assert!(
            combined_v3_fragment.contains("v3_map_events"),
            "combined-substream v3 fragment should pass params to the v3_map_events module"
        );
        assert!(
            combined_v3_fragment.contains("module_name: \"v3_map_protocol_changes\""),
            "combined-substream v3 fragment should point at the combined package module"
        );
        assert!(
            combined_v3_fragment.contains("ethereum-uniswap-v2-v3-combined"),
            "combined-substream v3 fragment should point at the combined package"
        );
        assert!(
            combined_v3_substreams.contains("shared_uniswap_bootstrap.yaml"),
            "combined-substream v3 substreams config should derive its pool filter from the shared bootstrap"
        );
    }
}
