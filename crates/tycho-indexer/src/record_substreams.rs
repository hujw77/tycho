use std::collections::HashMap;

use tracing::info;
use tycho_indexer::{
    cli::{GlobalArgs, RecordSubstreamsArgs},
    extractor::{
        family_runtime::{
            default_family_runtime_registry, select_resolved_runtime_target, FamilyRuntimeRegistry,
            ResolvedRuntimeTargetSelector, ResolvedSubstreamsExecutionRequest,
        },
        runner::load_substreams_package,
        ExtractionError,
    },
    substreams::{mock::write_mock_substreams_fixture, stream::build_substreams_request},
};

use crate::config::ExtractorConfigs;

fn parse_substreams_params(params: &[String]) -> Result<HashMap<String, String>, ExtractionError> {
    let mut parsed = HashMap::new();
    for raw in params {
        let Some((key, value)) = raw.split_once('=') else {
            return Err(ExtractionError::Setup(format!(
                "Invalid substreams param `{raw}`; expected key=value"
            )));
        };
        if key.is_empty() {
            return Err(ExtractionError::Setup(format!(
                "Invalid substreams param `{raw}`; key cannot be empty"
            )));
        }
        parsed.insert(key.to_string(), value.to_string());
    }
    Ok(parsed)
}

pub(crate) fn derived_record_target_selector_from_args(
    record_args: &RecordSubstreamsArgs,
) -> Result<ResolvedRuntimeTargetSelector<'_>, ExtractionError> {
    match (
        record_args.family.as_deref(),
        record_args.protocol_system.as_deref(),
    ) {
        (Some(family_name), None) => Ok(ResolvedRuntimeTargetSelector::Family(family_name)),
        (None, Some(protocol_system)) => Ok(
            ResolvedRuntimeTargetSelector::StandaloneProtocolSystem(protocol_system),
        ),
        _ => Err(ExtractionError::Setup(
            "record-substreams derived mode requires exactly one of `--family` or `--protocol-system`"
                .to_string(),
        )),
    }
}

pub(crate) fn render_record_substreams_request_json(
    request: &ResolvedSubstreamsExecutionRequest,
) -> Result<String, ExtractionError> {
    serde_json::to_string_pretty(request)
        .map_err(|err| ExtractionError::Setup(format!("Failed to serialize request: {err}")))
}

fn merge_record_substreams_params(
    base: &mut HashMap<String, String>,
    overrides: &HashMap<String, String>,
) {
    for (key, value) in overrides {
        base.insert(key.clone(), value.clone());
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn resolve_record_substreams_request(
    record_args: &RecordSubstreamsArgs,
) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
    resolve_record_substreams_request_with_registry(record_args, default_family_runtime_registry())
}

pub(crate) fn resolve_record_substreams_request_with_registry(
    record_args: &RecordSubstreamsArgs,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
    let override_params = parse_substreams_params(&record_args.params)?;

    if let Some(extractors_config_path) = &record_args.extractors_config {
        let selector = derived_record_target_selector_from_args(record_args)?;

        let extractors_config =
            ExtractorConfigs::from_yaml_with_registry(extractors_config_path, registry).map_err(
                |err| ExtractionError::Setup(format!("Failed to load extractors config. {err}")),
            )?;
        let targets = extractors_config.resolved_runtime_targets_with_registry(registry)?;
        let target = select_resolved_runtime_target(targets, selector)
            .ok_or_else(|| selector.not_found_error(extractors_config_path))?;
        let default_resolved = target.substreams_execution_request()?;
        let effective_start_block = record_args
            .start_block
            .unwrap_or(default_resolved.start_block);
        let mut resolved =
            target.substreams_execution_request_with_start_block(effective_start_block)?;
        resolved.stop_block = record_args
            .stop_block(resolved.start_block)
            .unwrap_or(resolved.stop_block as i64) as u64;
        merge_record_substreams_params(&mut resolved.params, &override_params);
        return Ok(resolved);
    }

    let spkg = record_args
        .spkg
        .clone()
        .ok_or_else(|| {
            ExtractionError::Setup(
            "record-substreams manual mode requires `--spkg` unless `--extractors-config` is used"
                .to_string(),
        )
        })?;
    let module = record_args
        .module
        .clone()
        .ok_or_else(|| {
            ExtractionError::Setup(
            "record-substreams manual mode requires `--module` unless `--extractors-config` is used"
                .to_string(),
        )
        })?;
    let start_block = record_args.start_block.ok_or_else(|| {
        ExtractionError::Setup(
            "record-substreams manual mode requires `--start-block` unless `--extractors-config` is used"
                .to_string(),
        )
    })?;

    Ok(ResolvedSubstreamsExecutionRequest {
        spkg,
        module,
        start_block,
        stop_block: record_args
            .stop_block(start_block)
            .unwrap_or_default() as u64,
        params: override_params,
        extractor_id: String::new(),
    })
}

pub(crate) async fn record_substreams_fixture(
    global_args: &GlobalArgs,
    record_args: &RecordSubstreamsArgs,
) -> Result<(), ExtractionError> {
    record_substreams_fixture_with_registry(
        global_args,
        record_args,
        default_family_runtime_registry(),
    )
    .await
}

pub(crate) async fn record_substreams_fixture_with_registry(
    global_args: &GlobalArgs,
    record_args: &RecordSubstreamsArgs,
    registry: FamilyRuntimeRegistry<'_>,
) -> Result<(), ExtractionError> {
    let resolved_request = resolve_record_substreams_request_with_registry(record_args, registry)?;
    if record_args.print_request {
        println!("{}", render_record_substreams_request_json(&resolved_request)?);
        return Ok(());
    }

    let loaded = load_substreams_package(
        global_args.s3_bucket.as_deref(),
        &resolved_request.spkg,
        &global_args.endpoint_url,
        Some(
            record_args
                .substreams_args
                .substreams_api_token
                .clone(),
        ),
    )
    .await?;

    let request = build_substreams_request(
        None,
        Some(loaded.spkg),
        resolved_request.module,
        resolved_request.start_block,
        resolved_request.stop_block,
        record_args.final_blocks_only,
        record_args
            .substreams_args
            .enable_partial_blocks,
        resolved_request.params,
    );

    let script = loaded
        .endpoint
        .record(request, record_args.max_responses)
        .await
        .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?;

    let output_path = std::path::Path::new(&record_args.output);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            ExtractionError::Setup(format!("Failed to create output directory: {err}"))
        })?;
    }
    write_mock_substreams_fixture(output_path, &[script])
        .map_err(|err| ExtractionError::Setup(format!("Failed to write fixture: {err}")))?;

    info!(output = %record_args.output, "Recorded Substreams fixture");
    Ok(())
}
