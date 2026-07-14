use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use tracing::info;
use tycho_indexer::{
    cli::{GlobalArgs, RecordSubstreamsArgs},
    extractor::{
        family_registry::{default_family_runtime_registry, FamilyRuntimeRegistry},
        runtime_target_planning::{
            ResolvedRuntimeTargetSelector, ResolvedSubstreamsExecutionRequest,
        },
        substreams_package_loader::load_substreams_package,
        ExtractionError,
    },
    pb::sf::substreams::{rpc::v3::Request, v1::Package},
    substreams::{
        mock::{write_mock_substreams_fixture, MockSubstreamsScript},
        stream::build_substreams_request,
        SubstreamsEndpoint,
    },
};

use crate::config::LoadedIndexerRuntimePlan;

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

fn resolve_loaded_record_substreams_request(
    extractors_config_path: &str,
    record_args: &RecordSubstreamsArgs,
    override_params: &HashMap<String, String>,
    resolve_request: impl FnOnce(
        Option<ResolvedRuntimeTargetSelector<'_>>,
        &str,
        &str,
        Option<i64>,
        Option<i64>,
        &HashMap<String, String>,
    ) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError>,
) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
    let selector = match (record_args.family.as_deref(), record_args.protocol_system.as_deref()) {
        (None, None) => None,
        _ => Some(derived_record_target_selector_from_args(record_args)?),
    };
    let unique_context = format!(
        "record-substreams derived mode requires exactly one of `--family` or `--protocol-system` unless `{extractors_config_path}` resolves exactly one runtime target"
    );
    let mut resolved = resolve_request(
        selector,
        &unique_context,
        extractors_config_path,
        record_args.start_block,
        None,
        override_params,
    )?;
    if let Some(stop_block) = record_args.stop_block(resolved.start_block) {
        resolved.stop_block = stop_block as u64;
    }
    Ok(resolved)
}

pub(crate) fn resolve_record_substreams_request(
    record_args: &RecordSubstreamsArgs,
) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
    let override_params = parse_substreams_params(&record_args.params)?;

    if let Some(extractors_config_path) = &record_args.extractors_config {
        let loaded_runtime_plan = LoadedIndexerRuntimePlan::from_yaml(extractors_config_path)?;
        return resolve_loaded_record_substreams_request(
            extractors_config_path,
            record_args,
            &override_params,
            |selector, unique_context, selector_context, start_block, stop_block, params| {
                loaded_runtime_plan.resolve_substreams_execution_request(
                    selector,
                    unique_context,
                    selector_context,
                    start_block,
                    stop_block,
                    params,
                )
            },
        );
    }

    resolve_record_substreams_request_with_registry(record_args, default_family_runtime_registry())
}

pub(crate) fn resolve_record_substreams_request_with_registry(
    record_args: &RecordSubstreamsArgs,
    registry: FamilyRuntimeRegistry<'static>,
) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
    let override_params = parse_substreams_params(&record_args.params)?;

    if let Some(extractors_config_path) = &record_args.extractors_config {
        let loaded_runtime_plan =
            LoadedIndexerRuntimePlan::from_yaml_with_registry(extractors_config_path, registry)?;
        return resolve_loaded_record_substreams_request(
            extractors_config_path,
            record_args,
            &override_params,
            |selector, unique_context, selector_context, start_block, stop_block, params| {
                loaded_runtime_plan.resolve_substreams_execution_request(
                    selector,
                    unique_context,
                    selector_context,
                    start_block,
                    stop_block,
                    params,
                )
            },
        );
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

pub(crate) trait SubstreamsFixtureRecorder: Send + Sync {
    fn record<'a>(
        &'a self,
        request: Request,
        max_responses: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<MockSubstreamsScript, anyhow::Error>> + Send + 'a>>;
}

impl SubstreamsFixtureRecorder for SubstreamsEndpoint {
    fn record<'a>(
        &'a self,
        request: Request,
        max_responses: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<MockSubstreamsScript, anyhow::Error>> + Send + 'a>>
    {
        let endpoint = Arc::new(self.clone());
        Box::pin(async move {
            endpoint
                .record(request, max_responses)
                .await
        })
    }
}

pub(crate) async fn record_substreams_fixture_from_package_and_recorder<R>(
    loaded_spkg: Package,
    recorder: Arc<R>,
    resolved_request: ResolvedSubstreamsExecutionRequest,
    record_args: &RecordSubstreamsArgs,
) -> Result<(), ExtractionError>
where
    R: SubstreamsFixtureRecorder + ?Sized,
{
    let request = build_substreams_request(
        None,
        Some(loaded_spkg),
        resolved_request.module,
        resolved_request.start_block,
        resolved_request.stop_block,
        record_args.final_blocks_only,
        record_args
            .substreams_args
            .enable_partial_blocks,
        resolved_request.params,
    );

    let script = recorder
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

pub(crate) async fn record_substreams_fixture(
    global_args: &GlobalArgs,
    record_args: &RecordSubstreamsArgs,
) -> Result<(), ExtractionError> {
    let resolved_request = resolve_record_substreams_request(record_args)?;
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

    record_substreams_fixture_from_package_and_recorder(
        loaded.spkg,
        loaded.endpoint,
        resolved_request,
        record_args,
    )
    .await
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn record_substreams_fixture_with_registry(
    global_args: &GlobalArgs,
    record_args: &RecordSubstreamsArgs,
    registry: FamilyRuntimeRegistry<'static>,
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

    record_substreams_fixture_from_package_and_recorder(
        loaded.spkg,
        loaded.endpoint,
        resolved_request,
        record_args,
    )
    .await
}
