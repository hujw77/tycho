use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use tracing::info;
use tycho_indexer::{
    cli::{GlobalArgs, RecordSubstreamsArgs},
    extractor::{
        family_registry::FamilyRuntimeRegistry,
        runtime_target_planning::{
            ResolvedRuntimeTargetSelector, ResolvedRuntimeTargets,
            ResolvedSubstreamsExecutionRequest,
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
    runtime_targets: &ResolvedRuntimeTargets<'_>,
    extractors_config_path: &str,
    record_args: &RecordSubstreamsArgs,
    override_params: &HashMap<String, String>,
) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
    let selector = match (record_args.family.as_deref(), record_args.protocol_system.as_deref()) {
        (None, None) => None,
        _ => Some(derived_record_target_selector_from_args(record_args)?),
    };
    let unique_context = format!(
        "record-substreams derived mode requires exactly one of `--family` or `--protocol-system` unless `{extractors_config_path}` resolves exactly one runtime target"
    );
    let selected_target = runtime_targets.resolve_target(
        selector,
        &unique_context,
        extractors_config_path,
    )?;
    let effective_start_block = selected_target.effective_substreams_start_block(
        record_args.start_block,
    )?;
    runtime_targets.resolve_substreams_execution_request(
        selector,
        &unique_context,
        extractors_config_path,
        record_args.start_block,
        record_args.stop_block(effective_start_block),
        override_params,
    )
}

pub(crate) fn resolve_record_substreams_request_with_registry(
    record_args: &RecordSubstreamsArgs,
    registry: FamilyRuntimeRegistry<'static>,
) -> Result<ResolvedSubstreamsExecutionRequest, ExtractionError> {
    let override_params = parse_substreams_params(&record_args.params)?;

    if let Some(extractors_config_path) = &record_args.extractors_config {
        let loaded_runtime_plan =
            LoadedIndexerRuntimePlan::from_yaml_with_registry(extractors_config_path, registry)?;
        let runtime_targets = loaded_runtime_plan.resolved_runtime_targets()?;
        return resolve_loaded_record_substreams_request(
            &runtime_targets,
            extractors_config_path,
            record_args,
            &override_params,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_indexer::{
        cli::{Cli, Command, RecordSubstreamsArgs},
        extractor::{
            family_runtime::resolve_runtime_targets_with_registry,
            family_registry::default_family_runtime_registry,
        },
    };
    use clap::Parser;
    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use super::*;

    fn record_args_with_config() -> RecordSubstreamsArgs {
        let cli = Cli::try_parse_from([
            "tycho-indexer",
            "--database-url",
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0",
            "--endpoint",
            "http://localhost:9000",
            "--rpc-url",
            "http://localhost:8545",
            "record-substreams",
            "--substreams-api-token",
            "token",
            "--extractors-config",
            "/tmp/extractors.yaml",
            "--output",
            "/tmp/out.json",
        ])
        .expect("parse record-substreams test args");
        match cli.command() {
            Command::RecordSubstreams(args) => args,
            other => panic!("expected record-substreams command, got {other:?}"),
        }
    }

    fn make_config(name: &str, spkg: &str) -> tycho_indexer::extractor::extractor_config::ExtractorConfig {
        tycho_indexer::extractor::extractor_config::ExtractorConfig::new(
            name.to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            42,
            None,
            vec![tycho_indexer::extractor::extractor_config::ProtocolTypeConfig::new(
                format!("{name}_pool"),
                FinancialType::Swap,
            )],
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
        config: tycho_indexer::extractor::extractor_config::ExtractorConfig,
        shared_spkg: &str,
    ) -> tycho_indexer::extractor::extractor_config::ExtractorConfig {
        let shared_stream = default_family_runtime_registry()
            .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", shared_spkg)
            .expect("registered uniswap shared stream");
        config.with_family_runtime(Some(
            tycho_indexer::extractor::family_runtime_metadata::FamilyRuntimeConfig::from_resolved_shared_stream(
                "uniswap",
                shared_stream,
            ),
        ))
    }

    #[test]
    fn record_substreams_rejects_protocol_system_selector_for_family_member() {
        let extractors = HashMap::from([
            (
                "uniswap_v2".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v2",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    )
                    .with_protocol_system("uniswap_v2"),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
            (
                "uniswap_v3".to_string(),
                with_resolved_uniswap_family_runtime(
                    make_config(
                        "uniswap_v3",
                        "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                    )
                    .with_protocol_system("uniswap_v3"),
                    "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg",
                ),
            ),
        ]);
        let runtime_targets = resolve_runtime_targets_with_registry(
            &extractors,
            default_family_runtime_registry(),
        )
        .expect("resolved runtime targets");
        let mut record_args = record_args_with_config();
        record_args.protocol_system = Some("uniswap_v2".to_string());

        let err = resolve_loaded_record_substreams_request(
            &runtime_targets,
            "/tmp/extractors.yaml",
            &record_args,
            &HashMap::new(),
        )
        .expect_err("family member protocol system should be rejected");

        assert!(
            err.to_string().contains("belongs to shared family runtime `uniswap`"),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("use the family selector"),
            "unexpected error: {err}"
        );
    }
}
