use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use crate::extractor::{
    family_registry::{default_family_runtime_registry, FamilyRuntimeRegistry},
    family_runtime_metadata::canonicalize_shared_route_protocol,
};

#[derive(Debug, Default, Deserialize)]
struct SubstreamsParamsFile {
    #[serde(default)]
    start_block: Option<i64>,
    #[serde(default)]
    includes: Vec<String>,
    #[serde(default)]
    params: BTreeMap<String, Value>,
}

#[derive(Debug, Default, Deserialize)]
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

pub fn protocol_filter_for_protocol_system(
    protocol_system: &str,
    registry: FamilyRuntimeRegistry<'_>,
) -> Option<HashSet<String>> {
    registry.normalized_shared_route_protocol_filter_for_protocol_system(protocol_system)
}

pub fn parse_substreams_params_yaml_with_registry(
    protocol_system: &str,
    contents: &str,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    let parsed: SubstreamsParamsFile = serde_yaml::from_str(contents)?;
    let allowed_protocols = protocol_filter_for_protocol_system(protocol_system, registry);
    let (start_block, params) = normalize_substreams_params(allowed_protocols.as_ref(), parsed)?;
    let mut substreams_params = Vec::with_capacity(params.len());

    for (key, value) in params {
        let rendered_value = render_substreams_param_value(&value)?;
        substreams_params.push(format!("{key}={rendered_value}"));
    }

    Ok((start_block, substreams_params.join("&")))
}

pub fn parse_substreams_params_yaml(
    protocol_system: &str,
    contents: &str,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    parse_substreams_params_yaml_with_registry(
        protocol_system,
        contents,
        default_family_runtime_registry(),
    )
}

pub fn parse_bootstrap_params_yaml_with_filter_and_registry(
    route_protocol_filter: Option<&str>,
    contents: &str,
    registry: FamilyRuntimeRegistry<'_>,
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

    let protocol_filter = route_protocol_filter
        .and_then(|protocol| protocol_filter_for_protocol_system(protocol, registry));
    let all_pools = collect_bootstrap_pools(&parsed.params, protocol_filter.as_ref())?;

    if all_pools.is_empty() {
        return Err("bootstrap config is missing `params.pools` or `params.routes`".into());
    }

    Ok((
        Some(bootstrap_block),
        format!("bootstrap_block={bootstrap_block}&pools={}", all_pools.join(",")),
    ))
}

pub fn parse_bootstrap_params_yaml(
    route_protocol_filter: Option<&str>,
    contents: &str,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    parse_bootstrap_params_yaml_with_filter_and_registry(
        route_protocol_filter,
        contents,
        default_family_runtime_registry(),
    )
}

pub fn resolve_substreams_params_map(
    allowed_protocols: Option<&HashSet<String>>,
    resolved_start_block: &mut Option<i64>,
    substreams_params: &mut HashMap<String, String>,
    base_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for (module_name, value) in substreams_params {
        let Some(path) = value.strip_prefix('@') else {
            continue;
        };

        let params_path = base_dir.join(path);
        let filter_name = render_allowed_protocols_label(allowed_protocols);
        let (start_block, resolved_params) =
            parse_substreams_params_file(allowed_protocols, &params_path).map_err(|err| {
                format!(
                    "failed to parse substreams config file for extractor `{}` \
                     module `{module_name}` at `{}`: {err}",
                    filter_name,
                    params_path.display()
                )
            })?;

        if let Some(start_block) = start_block {
            if let Some(existing_start_block) = resolved_start_block {
                if *existing_start_block != start_block {
                    return Err(format!(
                        "conflicting start_block values for extractor `{}`: \
                         {existing_start_block} vs {start_block} from module `{module_name}`",
                        filter_name
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

pub fn resolve_bootstrap_params(
    allowed_protocols: Option<&HashSet<String>>,
    params_value: &mut String,
    base_dir: &Path,
) -> Result<i64, Box<dyn std::error::Error>> {
    let Some(path) = params_value.strip_prefix('@') else {
        return extract_bootstrap_block_from_query(params_value).map_err(Into::into);
    };

    let params_path = base_dir.join(path);
    let filter_name = render_allowed_protocols_label(allowed_protocols);
    let (start_block, resolved_params) =
        parse_bootstrap_params_file(allowed_protocols, &params_path).map_err(|err| {
            format!(
                "failed to parse bootstrap config file for extractor `{}` at `{}`: \
                 {err}",
                filter_name,
                params_path.display()
            )
        })?;

    let start_block = start_block.ok_or_else(|| {
        format!(
            "bootstrap config file for extractor `{}` at `{}` is missing \
             `start_block` or `params.bootstrap_block`",
            filter_name,
            params_path.display()
        )
    })?;

    *params_value = resolved_params;
    Ok(start_block)
}

fn normalize_substreams_params(
    allowed_protocols: Option<&HashSet<String>>,
    mut parsed: SubstreamsParamsFile,
) -> Result<(Option<i64>, BTreeMap<String, Value>), Box<dyn std::error::Error>> {
    if parsed.params.contains_key("routes") {
        let (pools, pool_tokens) =
            collect_bootstrap_pool_metadata(&parsed.params, allowed_protocols)?;
        if !pools.is_empty() {
            parsed.params.insert(
                "pools".to_string(),
                Value::Sequence(pools.into_iter().map(Value::String).collect()),
            );
        }
        if !pool_tokens.is_empty() {
            parsed.params.insert(
                "pool_tokens".to_string(),
                Value::Sequence(pool_tokens.into_iter().map(Value::String).collect()),
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

pub fn parse_substreams_params_file(
    allowed_protocols: Option<&HashSet<String>>,
    path: &Path,
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    let parsed = load_substreams_params_file(path, &mut HashSet::new())?;
    let (start_block, params) = normalize_substreams_params(allowed_protocols, parsed)?;
    let mut substreams_params = Vec::with_capacity(params.len());

    for (key, value) in params {
        let rendered_value = render_substreams_param_value(&value)?;
        substreams_params.push(format!("{key}={rendered_value}"));
    }

    Ok((start_block, substreams_params.join("&")))
}

pub fn parse_bootstrap_params_file(
    allowed_protocols: Option<&HashSet<String>>,
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

    let all_pools = collect_bootstrap_pools(&parsed.params, allowed_protocols)?;

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
    let base_dir = resolved_path.parent().unwrap_or_else(|| Path::new("."));
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
    let base_dir = resolved_path.parent().unwrap_or_else(|| Path::new("."));
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
            target.params.insert(key, incoming_value);
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
    target.params.pools.extend(incoming.params.pools);
    target.params.routes.extend(incoming.params.routes);
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
    include.strip_prefix('@').unwrap_or(include)
}

fn canonicalize_for_include_tracking(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    path.canonicalize()
        .map_err(|err| format!("failed to resolve config path `{}`: {err}", path.display()).into())
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

fn render_allowed_protocols_label(allowed_protocols: Option<&HashSet<String>>) -> String {
    let Some(allowed_protocols) = allowed_protocols else {
        return "<unfiltered>".to_string();
    };

    let mut labels = allowed_protocols.iter().cloned().collect::<Vec<_>>();
    labels.sort();
    labels.join(",")
}

fn router_matches_allowed_protocols(
    router_protocol: &str,
    allowed_protocols: Option<&HashSet<String>>,
) -> bool {
    let Some(allowed_protocols) = allowed_protocols else {
        return true;
    };

    allowed_protocols.contains(&canonicalize_shared_route_protocol(router_protocol))
}

fn parse_i64_yaml_value(value: &Value) -> Result<i64, Box<dyn std::error::Error>> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| "expected integer value".into()),
        Value::String(value) => value.parse::<i64>().map_err(Into::into),
        _ => Err("expected integer value".into()),
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
                _ => Err("expected string value in sequence".into()),
            })
            .collect(),
        _ => Err("expected sequence value".into()),
    }
}

fn extract_bootstrap_block_from_query(params: &str) -> Result<i64, Box<dyn std::error::Error>> {
    for pair in params.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key == "bootstrap_block" {
            return value.parse::<i64>().map_err(Into::into);
        }
    }

    Err("bootstrap params are missing `bootstrap_block`".into())
}
