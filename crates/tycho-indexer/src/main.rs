#![doc = include_str!("../README.md")]

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// TODO: We need to use `use pretty_assertions::{assert_eq, assert_ne}` per test module.
#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

use std::{
    collections::HashMap,
    env, process,
    str::FromStr,
    sync::{mpsc, Arc},
};

use actix_web::{dev::ServerHandle, web, App, HttpResponse, HttpServer, Responder};
use anyhow::anyhow;
use chrono::{NaiveDateTime, Utc};
use clap::Parser;
use futures03::future::select_all;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::{
    runtime::Handle,
    select,
    signal::unix::{signal, SignalKind},
    task::JoinHandle,
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
#[cfg(test)]
use tycho_common::dto;
use tycho_common::models::{Chain, ImplementationType};
#[cfg(test)]
use tycho_common::storage::ProtocolGateway;
#[cfg(test)]
use tycho_common::storage::{ChainGateway, ContractStateGateway, ExtractionStateGateway};
#[cfg(test)]
use tycho_common::{
    models::{blockchain::Block, Address, ExtractionState},
    Bytes,
};
use tycho_ethereum::{
    rpc::EthereumRpcClient, services::token_pre_processor::EthereumTokenPreProcessor,
};
#[cfg(test)]
use tycho_indexer::extractor::runner::FamilyRuntimeConfig;
use tycho_indexer::{
    cli::{
        AnalyzeTokenArgs, Cli, Command, GlobalArgs, IndexArgs, RecordSubstreamsArgs, RunSpkgArgs,
        SubstreamsArgs,
    },
    extractor::{
        chain_state::ChainState,
        family_runtime::ResolvedRuntimeTarget,
        protocol_cache::ProtocolMemoryCache,
        runner::{
            build_runners_for_runtime_targets, DCIType, ExtractorConfig, ExtractorHandle,
            ManagedRunner, ProtocolTypeConfig,
        },
        startup::initialize_resolved_runtime_target_accounts,
        token_analysis_cron::analyze_tokens,
        ExtractionError,
    },
    services::{PlansConfig, ServicesBuilder},
};
use tycho_storage::postgres::{builder::GatewayBuilder, cache::CachedGateway};

#[cfg(test)]
use tycho_indexer::extractor::startup::initialize_accounts;

mod config;
mod ot;
#[cfg(test)]
mod testing;
#[cfg(test)]
pub use tycho_indexer::{extractor, pb};

use config::ExtractorConfigs;
use record_substreams::record_substreams_fixture;
#[cfg(test)]
use record_substreams::{
    record_substreams_fixture_with_registry, render_record_substreams_request_json,
    resolve_record_substreams_request, resolve_record_substreams_request_with_registry,
};

type ExtractionTasks = Vec<JoinHandle<Result<(), ExtractionError>>>;
type ServerTasks = Vec<JoinHandle<Result<(), ExtractionError>>>; //TODO: introduce an error type for it

mod record_substreams;

fn main() -> Result<(), anyhow::Error> {
    let cli: Cli = Cli::parse();
    let global_args = cli.args();
    match cli.command() {
        Command::Index(indexer_args) => {
            run_indexer(global_args, indexer_args).map_err(|e| anyhow!(e))?;
        }
        Command::Run(run_args) => {
            run_spkg(global_args, run_args).map_err(|e| anyhow!(e))?;
        }
        Command::RecordSubstreams(record_args) => {
            run_record_substreams(global_args, record_args).map_err(|e| anyhow!(e))?;
        }
        Command::AnalyzeTokens(analyze_args) => {
            run_analyze_tokens(global_args, analyze_args).map_err(|e| anyhow!(e))?;
        }
        Command::Rpc => {
            run_rpc(global_args).map_err(|e| anyhow!(e))?;
        }
    };
    Ok(())
}

fn create_tracing_subscriber() {
    // Set up the subscriber
    let console_flag = std::env::var("ENABLE_CONSOLE").unwrap_or_else(|_| "false".to_string());
    if console_flag == "true" {
        console_subscriber::init();
    } else {
        // OTLP endpoint is set, construct OTLP pipeline
        if let Ok(otlp_exporter_endpoint) = std::env::var("OTLP_EXPORTER_ENDPOINT") {
            let config = ot::TracingConfig { otlp_exporter_endpoint };
            ot::init_tracing(config).unwrap();
        } else {
            warn!("OTLP_EXPORTER_ENDPOINT not set defaulting to stdout subscriber!");
            let format = tracing_subscriber::fmt::format()
                .with_level(true)
                .with_target(false)
                .compact();
            tracing_subscriber::fmt()
                .event_format(format)
                .with_env_filter(EnvFilter::from_default_env())
                .init();
        }
    }
}

/// Creates and runs the Prometheus metrics exporter using Actix Web.
pub fn create_metrics_exporter() -> tokio::task::JoinHandle<()> {
    let exporter_builder = PrometheusBuilder::new();
    let handle = exporter_builder
        .install_recorder()
        .expect("Failed to install Prometheus recorder");

    tokio::spawn(async move {
        if let Err(e) = HttpServer::new(move || {
            App::new().route(
                "/metrics",
                web::get().to({
                    let handle = handle.clone();
                    move || metrics_handler(handle.clone())
                }),
            )
        })
        .bind(("0.0.0.0", 9898))
        .expect("Failed to bind metrics server")
        .run()
        .await
        {
            error!("Metrics server failed: {}", e);
        }
    })
}

/// Handles requests to the /metrics endpoint, rendering Prometheus metrics.
async fn metrics_handler(handle: PrometheusHandle) -> impl Responder {
    let metrics = handle.render();
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(metrics)
}

/// Spawns a background task that emits jemalloc allocator stats as Prometheus gauges every 60s.
///
/// Emits `jemalloc_allocated_bytes` (live allocations) and `jemalloc_resident_bytes` (RSS as seen
/// by jemalloc).
#[cfg(feature = "jemalloc")]
fn spawn_jemalloc_stats_reporter() {
    use metrics::gauge;
    use tikv_jemalloc_ctl::{epoch, stats};

    tokio::spawn(async {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            // Advance the epoch to refresh stats.
            if epoch::advance().is_err() {
                continue;
            }
            if let Ok(allocated) = stats::allocated::read() {
                gauge!("jemalloc_allocated_bytes").set(allocated as f64);
            }
            if let Ok(resident) = stats::resident::read() {
                gauge!("jemalloc_resident_bytes").set(resident as f64);
            }
        }
    });
}

/// Executes all extractors configured in the extractor configuration file and starts the server.
///
/// Note: This function utilizes two distinct runtimes: one for extraction tasks and another
/// for others operations such as server and gateway.
///
/// By using separate runtimes, extraction processes in Tycho can run independently, ensuring
/// that server-related tasks do not interfere with the extraction workflow, and overall
/// system performance is maintained.
fn run_indexer(global_args: GlobalArgs, index_args: IndexArgs) -> Result<(), ExtractionError> {
    let extraction_threads = std::env::var("EXTRACTION_WORKER_THREADS")
        .unwrap_or_else(|_| "2".to_string())
        .parse()
        .expect("EXTRACTION_WORKER_THREADS must be a number");
    let main_threads = std::env::var("MAIN_WORKER_THREADS")
        .unwrap_or_else(|_| "3".to_string())
        .parse()
        .expect("MAIN_WORKER_THREADS must be a number");
    // We spawn a dedicated runtime for extraction
    let extraction_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(extraction_threads)
        .enable_all()
        .build()
        .unwrap();

    let main_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(main_threads)
        .enable_all()
        .build()
        .unwrap();

    let (control_tx, control_rx) = mpsc::channel();

    let (extraction_tasks, other_tasks) = main_runtime.block_on(async {
        create_tracing_subscriber();
        let _metrics_task = create_metrics_exporter();
        #[cfg(feature = "jemalloc")]
        spawn_jemalloc_stats_reporter();

        info!("Starting Tycho");
        debug!("{} CPUs detected", num_cpus::get());
        let extractors_config = ExtractorConfigs::from_yaml(&index_args.extractors_config)
            .map_err(|e| ExtractionError::Setup(format!("Failed to load extractors.yaml. {e}")))?;

        let retention_horizon: NaiveDateTime = index_args
            .retention_horizon
            .parse()
            .expect("Failed to parse retention horizon");

        let (extraction_tasks, other_tasks) = create_indexing_tasks(
            &global_args,
            &index_args.substreams_args,
            &index_args
                .chains
                .iter()
                .map(|chain_str| {
                    Chain::from_str(chain_str)
                        .unwrap_or_else(|_| panic!("Unknown chain {chain_str}"))
                })
                .collect::<Vec<_>>(),
            retention_horizon,
            extractors_config,
            Some(extraction_runtime.handle()),
            index_args.settlement_contract,
        )
        .await?;

        Ok::<_, ExtractionError>((extraction_tasks, other_tasks))
    })?;

    let extractor_ctrl_tx = control_tx.clone();
    extraction_runtime.spawn(async move {
        let (res, _, _) = select_all(extraction_tasks).await;

        if extractor_ctrl_tx.send(res).is_err() {
            error!(
                "Fatal execution task exited and failed trying to communicate with main thread. Exiting the process..."
            );
            process::exit(1);
        }
    });

    let services_ctrl_tx = control_tx.clone();
    main_runtime.spawn(async move {
        let (res, _, _) = select_all(other_tasks).await;

        if services_ctrl_tx.send(res).is_err() {
            error!("Fatal service task exited and failed trying to communicate with main thread. Exiting the process...");
            process::exit(1);
        }
    });

    let res = control_rx
        .recv()
        .expect("Control channel unexpectedly closed");

    res.expect("A thread panicked. Shutting down Tycho.")
}

#[tokio::main]
async fn run_spkg(global_args: GlobalArgs, run_args: RunSpkgArgs) -> Result<(), ExtractionError> {
    create_tracing_subscriber();
    info!("Starting Tycho");

    let dci_plugin = run_args
        .dci_plugin
        .clone()
        .map_or(Ok(None), |s| match s.as_str() {
            "rpc" => Ok(Some(DCIType::RPC)),
            _ => Err(ExtractionError::Setup(format!("Unknown DCI plugin: {s}"))),
        })?;

    let config = ExtractorConfigs::new(HashMap::from([(
        run_args.protocol_system.clone(),
        ExtractorConfig::new(
            run_args.protocol_system.clone(),
            Chain::from_str(&run_args.chain).unwrap(),
            ImplementationType::Vm,
            1, /* TODO: if we want to increase this, we need to commit the cache when we reached
                * `end_block` */
            run_args.start_block,
            run_args.stop_block(),
            run_args
                .protocol_type_names
                .into_iter()
                .map(|name| {
                    ProtocolTypeConfig::new(name, tycho_common::models::FinancialType::Swap)
                })
                .collect::<Vec<_>>(),
            run_args.spkg,
            run_args.module,
            run_args.initialized_accounts,
            run_args.initialization_block,
            None,
            dci_plugin,
            HashMap::new(),
            None,
        ),
    )]));

    let (extraction_tasks, mut other_tasks) = create_indexing_tasks(
        &global_args,
        &run_args.substreams_args,
        &[Chain::from_str(&run_args.chain).unwrap()],
        Utc::now().naive_utc(),
        config,
        None,
        run_args.settlement_contract,
    )
    .await?;

    let mut all_tasks = extraction_tasks;
    all_tasks.append(&mut other_tasks);

    let (res, _, _) = select_all(all_tasks).await;
    res.expect("Extractor- nor ServiceTasks should panic!")
}

#[tokio::main]
async fn run_record_substreams(
    global_args: GlobalArgs,
    record_args: RecordSubstreamsArgs,
) -> Result<(), ExtractionError> {
    create_tracing_subscriber();
    info!("Recording Substreams responses");

    record_substreams_fixture(&global_args, &record_args).await
}

#[cfg(test)]
fn repo_combined_family_extractors_config_path(file_name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(file_name)
}

#[cfg(test)]
fn repo_combined_family_output_module(family_name: &str) -> String {
    crate::testing::family_output_module_for_tests(family_name)
}

#[tokio::main]
async fn run_rpc(global_args: GlobalArgs) -> Result<(), ExtractionError> {
    create_tracing_subscriber();

    let rpc_client = global_args.rpc.build_client()?;

    let direct_gw = GatewayBuilder::new(&global_args.database_url)
        .set_chains(&[Chain::Ethereum]) // TODO: handle multichain
        .build_direct_gw()
        .await?;

    info!("Starting Tycho RPC");
    let server_url = format!("http://{}:{}", global_args.server_ip, global_args.server_port);
    let api_key = env::var("AUTH_API_KEY").map_err(|_| {
        ExtractionError::Setup("AUTH_API_KEY environment variable is not set".to_string())
    })?;

    let plans_config = PlansConfig::from_yaml("./plans.yaml").map_err(ExtractionError::Setup)?;

    let (server_handle, server_task) =
        ServicesBuilder::new(direct_gw.clone(), rpc_client.clone(), api_key)
            .prefix(&global_args.server_version_prefix)
            .bind(&global_args.server_ip)
            .port(global_args.server_port)
            .plans_config(plans_config)
            .run()?;
    info!(server_url, "Http and Ws server started");
    let shutdown_task = tokio::spawn(shutdown_handler(server_handle, vec![], None));
    let (res, _, _) = select_all([server_task, shutdown_task]).await;
    res.expect("ServiceTasks shouldn't panic!")
}

/// Creates extraction and server tasks.
async fn create_indexing_tasks(
    global_args: &GlobalArgs,
    substreams_args: &SubstreamsArgs,
    chains: &[Chain],
    retention_horizon: NaiveDateTime,
    extractors_config: ExtractorConfigs,
    extraction_runtime: Option<&Handle>,
    settlement_contract: alloy::primitives::Address,
) -> Result<(ExtractionTasks, ServerTasks), ExtractionError> {
    let rpc_client = global_args.rpc.build_client()?;

    let block_number = rpc_client
        .get_block_number()
        .await
        .expect("Error getting block number");

    let chain_state = ChainState::new(chrono::Local::now().naive_utc(), block_number, 12); //TODO: remove hardcoded blocktime

    let resolved_indexer_runtime = extractors_config
        .resolved_indexer_runtime()
        .map_err(|e| ExtractionError::Setup(format!("Failed to resolve runtime targets: {e}")))?;
    let protocol_systems = resolved_indexer_runtime
        .protocol_systems
        .clone();
    let dci_protocols = resolved_indexer_runtime
        .dci_protocol_systems
        .clone();

    let (cached_gw, gw_writer_handle) = GatewayBuilder::new(&global_args.database_url)
        .set_chains(chains)
        .set_protocol_systems(&protocol_systems)
        .set_retention_horizon(retention_horizon)
        .build()
        .await?;
    let token_processor = EthereumTokenPreProcessor::new(
        &rpc_client,
        *chains
            .first()
            .expect("No chain provided"), //TODO: handle multichain?
        settlement_contract,
    );

    let (runners, extractor_handles) =
        // TODO: accept substreams configuration from cli.
        build_all_extractors_for_runtime_targets(resolved_indexer_runtime.runtime_targets, chain_state, chains, &global_args.endpoint_url, global_args.s3_bucket.as_deref(), &substreams_args.substreams_api_token, &cached_gw, global_args.database_insert_batch_size, &token_processor, &rpc_client, extraction_runtime, substreams_args.enable_partial_blocks)
            .await
            .map_err(|e| ExtractionError::Setup(format!("Failed to create extractors: {e}")))?;

    let server_url = format!("http://{}:{}", global_args.server_ip, global_args.server_port);
    let api_key = env::var("AUTH_API_KEY").map_err(|_| {
        ExtractionError::Setup("AUTH_API_KEY environment variable is not set".to_string())
    })?;
    let plans_config = PlansConfig::from_yaml("./plans.yaml").map_err(ExtractionError::Setup)?;

    let (server_handle, server_task) =
        ServicesBuilder::new(cached_gw.clone(), rpc_client.clone(), api_key)
            .prefix(&global_args.server_version_prefix)
            .bind(&global_args.server_ip)
            .port(global_args.server_port)
            .plans_config(plans_config)
            .dci_protocols(dci_protocols)
            .protocol_systems(protocol_systems)
            .register_extractors(extractor_handles.clone())
            .run()?;
    info!(server_url, "Http and Ws server started");

    let shutdown_task =
        tokio::spawn(shutdown_handler(server_handle, extractor_handles, Some(gw_writer_handle)));

    let extractor_tasks = runners
        .into_iter()
        .map(|runner| runner.run())
        .collect::<Vec<_>>();

    Ok((extractor_tasks, vec![server_task, shutdown_task]))
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(test), allow(dead_code))]
async fn build_all_extractors(
    config: &ExtractorConfigs,
    chain_state: ChainState,
    chains: &[Chain],
    endpoint_url: &str,
    s3_bucket: Option<&str>,
    substreams_api_token: &str,
    cached_gw: &CachedGateway,
    database_insert_batch_size: usize,
    token_pre_processor: &EthereumTokenPreProcessor,
    rpc_client: &EthereumRpcClient,
    runtime: Option<&tokio::runtime::Handle>,
    partial_blocks: bool,
) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
    let runtime_targets = config.resolved_runtime_targets()?;

    build_all_extractors_for_runtime_targets(
        runtime_targets,
        chain_state,
        chains,
        endpoint_url,
        s3_bucket,
        substreams_api_token,
        cached_gw,
        database_insert_batch_size,
        token_pre_processor,
        rpc_client,
        runtime,
        partial_blocks,
    )
    .await
}

async fn build_all_extractors_for_runtime_targets<'a>(
    runtime_targets: Vec<ResolvedRuntimeTarget<'a>>,
    chain_state: ChainState,
    chains: &[Chain],
    endpoint_url: &str,
    s3_bucket: Option<&str>,
    substreams_api_token: &str,
    cached_gw: &CachedGateway,
    database_insert_batch_size: usize,
    token_pre_processor: &EthereumTokenPreProcessor,
    rpc_client: &EthereumRpcClient,
    runtime: Option<&tokio::runtime::Handle>,
    partial_blocks: bool,
) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
    let chain = *chains
        .first()
        .expect("No chain provided");

    info!("Building protocol cache");
    let protocol_cache = ProtocolMemoryCache::new(
        chain,
        chrono::Duration::seconds(900),
        Arc::new(cached_gw.clone()),
    );
    protocol_cache.populate().await?;

    initialize_resolved_runtime_target_accounts(runtime_targets.iter(), rpc_client, cached_gw)
        .await;

    build_runners_for_runtime_targets(
        runtime_targets,
        chain_state,
        endpoint_url,
        s3_bucket,
        substreams_api_token,
        cached_gw,
        database_insert_batch_size,
        token_pre_processor,
        &protocol_cache,
        rpc_client,
        runtime.cloned(),
        partial_blocks,
    )
    .await
}
async fn shutdown_handler(
    server_handle: ServerHandle,
    extractors: Vec<ExtractorHandle>,
    db_write_executor_handle: Option<JoinHandle<()>>,
) -> Result<(), ExtractionError> {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| ExtractionError::Unknown(e.to_string()))?;

    tokio::select! {
        _ = ctrl_c => {
            info!("SIGINT (Ctrl+C) received. Cleaning up...");
        },
        _ = sigterm.recv() => {
            info!("SIGTERM received. Cleaning up...");
        },
    }

    for e in extractors.iter() {
        if let Err(err) = e.stop().await {
            warn!(extractor_id = %e.get_id(), error = %err, "Failed to stop extractor cleanly");
        }
    }
    server_handle.stop(true).await;
    if let Some(handle) = db_write_executor_handle {
        handle.abort();
    }
    Ok(())
}

#[tokio::main]
async fn run_analyze_tokens(
    global_args: GlobalArgs,
    analyzer_args: AnalyzeTokenArgs,
) -> Result<(), anyhow::Error> {
    let rpc_client = global_args.rpc.build_client()?;

    create_tracing_subscriber();
    let (cached_gw, gw_writer_thread) = GatewayBuilder::new(&global_args.database_url)
        .set_chains(&[analyzer_args.chain])
        .build()
        .await?;
    let cached_gw = Arc::new(cached_gw);
    let analyze_thread = analyze_tokens(analyzer_args, &rpc_client, cached_gw.clone());
    select! {
         res = analyze_thread => {
            res?;
         },
         res = gw_writer_thread => {
            res?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test_serial_db {
    use std::collections::HashMap;

    use crate::testing::{
        family_block_response, family_block_response_from_block_changes,
        family_member_protocol_systems_for_tests, family_runtime_config_for_tests,
        family_shared_module_for_tests, scripted_session_response, scripted_undo_response,
        v2_pair_created_block, v3_pool_created_block,
        write_uniswap_family_defaults_config,
        write_uniswap_family_defaults_config_with_member_names,
        write_uniswap_family_defaults_config_with_shared_bootstrap,
    };
    use alloy::primitives::Address as AlloyAddress;
    use once_cell::sync::Lazy;
    use prost::Message;
    use substreams::store::StoreGet;
    use tycho_storage::postgres::testing::run_against_db;

    use super::*;

    const TEST_FAMILY_NAME: &str = "uniswap";

    fn test_family_shared_module() -> String {
        family_shared_module_for_tests(TEST_FAMILY_NAME)
    }

    fn test_family_runtime_config(shared_spkg: impl Into<String>) -> FamilyRuntimeConfig {
        family_runtime_config_for_tests(TEST_FAMILY_NAME, shared_spkg)
    }

    fn test_family_protocol_systems() -> Vec<String> {
        family_member_protocol_systems_for_tests(TEST_FAMILY_NAME)
    }

    fn test_family_defaults_config(
        file_prefix: &str,
        unique: &str,
        shared_spkg_path: &str,
        start_block: i64,
        stop_block: Option<i64>,
    ) -> std::path::PathBuf {
        write_uniswap_family_defaults_config(
            file_prefix,
            unique,
            shared_spkg_path,
            start_block,
            stop_block,
        )
    }

    fn test_family_defaults_config_with_member_names(
        file_prefix: &str,
        unique: &str,
        shared_spkg_path: &str,
        start_block: i64,
        stop_block: Option<i64>,
        v2_name: &str,
        v3_name: &str,
    ) -> std::path::PathBuf {
        write_uniswap_family_defaults_config_with_member_names(
            file_prefix,
            unique,
            shared_spkg_path,
            start_block,
            stop_block,
            v2_name,
            v3_name,
        )
    }

    fn test_family_shared_spkg_path(label: &str) -> String {
        let shared_spkg_path = std::env::temp_dir().join(format!(
            "tycho-indexer-{label}-{}-{}.spkg",
            process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        std::fs::write(
            &shared_spkg_path,
            tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
        )
        .expect("write temp spkg");
        shared_spkg_path
            .to_str()
            .expect("utf8 spkg path")
            .to_string()
    }

    fn test_unique_suffix() -> String {
        format!(
            "{}-{}",
            process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        )
    }

    static RPC: Lazy<EthereumRpcClient> = Lazy::new(|| {
        let rpc_url = std::env::var("RPC_URL").expect("RPC URL must be set for testing");
        EthereumRpcClient::new(&rpc_url).expect("Failed to create RPC client")
    });

    #[derive(Clone, Debug, Default)]
    struct MockProtoStore<T> {
        values: HashMap<String, T>,
    }

    impl<T> MockProtoStore<T> {
        fn with_last<K: Into<String>>(mut self, key: K, value: T) -> Self {
            self.values.insert(key.into(), value);
            self
        }
    }

    impl<T> StoreGet<T> for MockProtoStore<T>
    where
        T: Default + prost::Message + Clone,
    {
        fn new(_idx: u32) -> Self {
            Self::default()
        }

        fn get_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> Option<T> {
            self.get_last(key)
        }

        fn get_last<K: AsRef<str>>(&self, key: K) -> Option<T> {
            self.values.get(key.as_ref()).cloned()
        }

        fn get_first<K: AsRef<str>>(&self, key: K) -> Option<T> {
            self.get_last(key)
        }

        fn has_at<K: AsRef<str>>(&self, _ord: u64, key: K) -> bool {
            self.has_last(key)
        }

        fn has_last<K: AsRef<str>>(&self, key: K) -> bool {
            self.values.contains_key(key.as_ref())
        }

        fn has_first<K: AsRef<str>>(&self, key: K) -> bool {
            self.has_last(key)
        }
    }

    #[tokio::test]
    #[ignore = "require archive node (RPC)"]
    async fn initialize_account_saves_correct_state() {
        run_against_db(move |_| async move {
            let accounts =
                vec![Address::from_str("0xba12222222228d8ba445958a75a0704d566bf2c8").unwrap()];
            let block_id = 20378314;
            let db_url =
                std::env::var("DATABASE_URL").expect("Database URL must be set for testing");

            let chain = Chain::Ethereum;

            let (cached_gw, _) = GatewayBuilder::new(&db_url.to_string())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");
            initialize_accounts(accounts, block_id, &RPC, chain, &cached_gw).await;

            let contracts = cached_gw
                .get_contracts(&chain, None, None, true, None)
                .await
                .unwrap()
                .entity;

            assert_eq!(contracts.len(), 1);
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "require archive node (RPC)"]
    async fn initialize_multiple_accounts_saves_correct_state() {
        run_against_db(move |_| async move {
            let accounts = vec![
                Address::from_str("0xba12222222228d8ba445958a75a0704d566bf2c8").unwrap(),
                Address::from_str("0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC").unwrap(),
            ];
            let block_id = 20378314;
            let db_url =
                std::env::var("DATABASE_URL").expect("Database URL must be set for testing");
            let chain = Chain::Ethereum;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            initialize_accounts(accounts, block_id, &RPC, chain, &cached_gw).await;

            let contracts = cached_gw
                .get_contracts(&chain, None, None, true, None)
                .await
                .unwrap()
                .entity;

            assert_eq!(contracts.len(), 2);
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "require archive node (RPC)"]
    async fn initialize_multiple_accounts_different_blocks() {
        run_against_db(|_| async move {
            let accounts =
                vec![Address::from_str("0xba12222222228d8ba445958a75a0704d566bf2c8").unwrap()];
            let block_id = 20378314;
            let db_url =
                std::env::var("DATABASE_URL").expect("Database URL must be set for testing");
            let chain = Chain::Ethereum;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            initialize_accounts(accounts, block_id, &RPC, chain, &cached_gw).await;
            let accounts =
                vec![Address::from_str("0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC").unwrap()];
            initialize_accounts(accounts, 20378315, &RPC, chain, &cached_gw).await;

            let contracts = cached_gw
                .get_contracts(&chain, None, None, true, None)
                .await
                .unwrap()
                .entity;

            assert_eq!(contracts.len(), 2);
        })
        .await;
    }

    #[tokio::test]
    async fn initialize_accounts_handles_empty_accounts() {
        run_against_db(|_| async move {
            let accounts = vec![];
            let block_id = 20378314;
            let rpc_url = "http://localhost:0000";
            let db_url =
                std::env::var("DATABASE_URL").expect("Database URL must be set for testing");
            let chain = Chain::Ethereum;

            // RPC client won't be used since an account list is empty, so we can create a stub one
            let rpc = EthereumRpcClient::new(rpc_url).expect("Failed to create RPC client");

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            initialize_accounts(accounts, block_id, &rpc, chain, &cached_gw).await;
        })
        .await;
    }

    #[tokio::test]
    async fn combined_config_builds_one_family_runner() {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        // Reuse the local dev Postgres when the test env does not inject DATABASE_URL.
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family");
            let missing_member_v2_spkg = std::env::temp_dir().join(format!(
                "tycho-indexer-missing-v2-{}-{}.spkg",
                process::id(),
                "member"
            ));
            let missing_member_v3_spkg = std::env::temp_dir().join(format!(
                "tycho-indexer-missing-v3-{}-{}.spkg",
                process::id(),
                "member"
            ));
            let missing_member_v2_spkg = missing_member_v2_spkg
                .to_str()
                .expect("utf8 missing member v2 spkg path")
                .to_string();
            let missing_member_v3_spkg = missing_member_v3_spkg
                .to_str()
                .expect("utf8 missing member v3 spkg path")
                .to_string();

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        missing_member_v2_spkg,
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: TEST_FAMILY_NAME.to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some(test_family_shared_module()),
                        durability_scope: None,
                    })),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        missing_member_v3_spkg,
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: TEST_FAMILY_NAME.to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some(test_family_shared_module()),
                        durability_scope: None,
                    })),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                "https://mainnet.eth.streamingfast.io",
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert!(matches!(runners[0], ManagedRunner::Family(_)));
            assert_eq!(handles.len(), 2);
            assert_eq!(handles[0].get_id().chain, chain);
            assert_eq!(handles[1].get_id().chain, chain);

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_config_rejects_conflicting_family_stop_blocks_before_runner_build() {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("conflicting-family-stop-block");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1,
                        42,
                        Some(100),
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1,
                        42,
                        Some(200),
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let err = match build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                "https://mainnet.eth.streamingfast.io",
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            {
                Ok(_) => panic!("conflicting family stop blocks should fail before runner build"),
                Err(err) => err,
            };

            assert!(err
                .to_string()
                .contains("family `uniswap` requires one shared stop_block"));

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_config_builds_one_family_runner_from_top_level_family_defaults() {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let unique = test_unique_suffix();
            let shared_spkg_path =
                test_family_shared_spkg_path(&format!("family-defaults-{unique}"));
            let config_path = test_family_defaults_config(
                "tycho-indexer-family-defaults",
                &unique,
                &shared_spkg_path,
                42,
                Some(123),
            );

            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 config path"),
            )
            .expect("load family-default config");

            let (runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                "https://mainnet.eth.streamingfast.io",
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors from family defaults");

            assert_eq!(runners.len(), 1);
            assert!(matches!(runners[0], ManagedRunner::Family(_)));
            assert_eq!(handles.len(), 2);
            assert_eq!(handles[0].get_id().chain, chain);
            assert_eq!(handles[1].get_id().chain, chain);

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_config_builds_one_family_runner_from_top_level_family_defaults_with_shared_bootstrap_and_member_params(
    ) {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let persisted_block = Block {
                number: 123,
                chain,
                hash: Bytes::from(vec![0x66; 32]),
                parent_hash: Bytes::from(vec![0x55; 32]),
                ts: chrono::NaiveDateTime::default(),
            };
            cached_gw
                .start_transaction(&persisted_block, Some("seed-family-default-bootstrap-progress"))
                .await;
            cached_gw
                .upsert_block(std::slice::from_ref(&persisted_block))
                .await
                .expect("persist block");
            cached_gw
                .save_state(&ExtractionState::new(
                    "uniswap_v2".to_string(),
                    chain,
                    None,
                    b"cursor@123-shared",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist v2 extraction state");
            cached_gw
                .save_state(&ExtractionState::new(
                    "uniswap_v3".to_string(),
                    chain,
                    None,
                    b"cursor@123-shared",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist v3 extraction state");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit seeded extraction state");

            let unique = test_unique_suffix();
            let temp_root = std::env::temp_dir()
                .join(format!("tycho-indexer-family-default-bootstrap-{unique}"));
            let _ = std::fs::remove_dir_all(&temp_root);
            std::fs::create_dir_all(&temp_root).expect("create temp config dir");

            let shared_spkg_path = temp_root.join("combined.spkg");
            std::fs::write(
                &shared_spkg_path,
                tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
            )
            .expect("write temp spkg");
            let bootstrap_path = temp_root.join("shared_bootstrap.yaml");
            std::fs::write(
                &bootstrap_path,
                r#"
start_block: 42
params:
  routes:
    - token0: "0x00000000000000000000000000000000000000a1"
      token1: "0x00000000000000000000000000000000000000b1"
      routers:
        - pool: "0x0000000000000000000000000000000000000011"
          protocol: uniswap_v2
        - pool: "0x0000000000000000000000000000000000000022"
          protocol: uniswap_v3
"#,
            )
            .expect("write bootstrap defaults");
            let config_path = write_uniswap_family_defaults_config_with_shared_bootstrap(
                "tycho-indexer-family-default-bootstrap",
                &unique,
                shared_spkg_path
                    .to_str()
                    .expect("utf8 shared spkg path"),
                bootstrap_path.to_str().expect("utf8 bootstrap path"),
                42,
                Some(123),
                Some("factory=0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"),
                Some("factory=0x1F98431c8aD98523631AE4a59f267346ea31F984"),
            );

            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 config path"),
            )
            .expect("load family-default bootstrap config");

            let v2 = config
                .extractors
                .get("uniswap_v2")
                .expect("v2 extractor present");
            let v3 = config
                .extractors
                .get("uniswap_v3")
                .expect("v3 extractor present");
            assert_eq!(v2.stop_block(), Some(123));
            assert_eq!(v3.stop_block(), Some(123));
            assert_eq!(
                v2.bootstrap
                    .as_ref()
                    .map(|bootstrap| bootstrap.start_block),
                Some(42)
            );
            assert_eq!(
                v3.bootstrap
                    .as_ref()
                    .map(|bootstrap| bootstrap.start_block),
                Some(42)
            );
            assert_eq!(
                v2.substreams_params
                    .get("v2_map_pool_events"),
                Some(&"factory=0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".to_string())
            );
            assert_eq!(
                v3.substreams_params
                    .get("v3_map_events"),
                Some(&"factory=0x1F98431c8aD98523631AE4a59f267346ea31F984".to_string())
            );

            let (runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                "https://mainnet.eth.streamingfast.io",
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors from top-level family defaults");

            assert_eq!(runners.len(), 1);
            assert!(matches!(runners[0], ManagedRunner::Family(_)));
            assert_eq!(handles.len(), 2);
            assert_eq!(handles[0].get_id().chain, chain);
            assert_eq!(handles[1].get_id().chain, chain);

            let _ = std::fs::remove_dir_all(&temp_root);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_resumes_from_persisted_branch_progress() {
        use tycho_indexer::substreams::mock::start_mock_substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;

            let (captured, addr) = start_mock_substreams().await;
            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let persisted_block = Block {
                number: 123,
                chain,
                hash: Bytes::from(vec![0x55; 32]),
                parent_hash: Bytes::from(vec![0x44; 32]),
                ts: chrono::NaiveDateTime::default(),
            };
            cached_gw
                .start_transaction(&persisted_block, Some("seed-family-progress"))
                .await;
            cached_gw
                .upsert_block(std::slice::from_ref(&persisted_block))
                .await
                .expect("persist block");
            cached_gw
                .save_state(&ExtractionState::new(
                    "uniswap_v2".to_string(),
                    chain,
                    None,
                    b"cursor@123-shared",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist v2 extraction state");
            cached_gw
                .save_state(&ExtractionState::new(
                    "uniswap_v3".to_string(),
                    chain,
                    None,
                    b"cursor@123-shared",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist v3 extraction state");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit seeded extraction state");
            let saved_v2 = cached_gw
                .get_state("uniswap_v2", &chain)
                .await
                .expect("read back v2 extraction state");
            let saved_v3 = cached_gw
                .get_state("uniswap_v3", &chain)
                .await
                .expect("read back v3 extraction state");
            assert_eq!(saved_v2.block_hash, persisted_block.hash);
            assert_eq!(saved_v3.block_hash, persisted_block.hash);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-resume");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("family runner present");
            runner.run().await.unwrap().unwrap();

            let requests = captured.lock().unwrap();
            assert_eq!(requests.len(), 1, "expected a single substreams request");
            assert_eq!(
                requests[0].start_block_num, 124,
                "family runner should resume from last persisted block + 1"
            );
            assert_eq!(requests[0].start_cursor, "cursor@123-shared");

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_alias_members_resume_from_shared_family_cursor() {
        use tycho_indexer::substreams::mock::start_mock_substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;

            let (captured, addr) = start_mock_substreams().await;
            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let persisted_block = Block {
                number: 123,
                chain,
                hash: Bytes::from(vec![0x66; 32]),
                parent_hash: Bytes::from(vec![0x55; 32]),
                ts: chrono::NaiveDateTime::default(),
            };
            cached_gw
                .start_transaction(&persisted_block, Some("seed-family-shared-progress"))
                .await;
            cached_gw
                .upsert_block(std::slice::from_ref(&persisted_block))
                .await
                .expect("persist block");
            cached_gw
                .save_state(&ExtractionState::new(
                    "family::uniswap".to_string(),
                    chain,
                    None,
                    b"cursor@123-family-shared",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist shared family extraction state");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit seeded shared family extraction state");
            let shared_state = cached_gw
                .get_state("family::uniswap", &chain)
                .await
                .expect("read back shared family extraction state");
            assert_eq!(shared_state.block_hash, persisted_block.hash);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-alias-resume");
            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config_with_member_names(
                "tycho-indexer-family-defaults-aliased-resume",
                &unique,
                &shared_spkg_path,
                42,
                None,
                "uniswap_v2_alias",
                "uniswap_v3_alias",
            );
            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 aliased family-default config path"),
            )
            .expect("load aliased family-default config");

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build aliased combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners.pop().expect("family runner present");
            runner.run().await.unwrap().unwrap();

            let requests = captured.lock().unwrap();
            assert_eq!(requests.len(), 1, "expected a single substreams request");
            assert_eq!(
                requests[0].start_block_num, 124,
                "shared family cursor should resume from last committed block + 1 even when extractor names are aliases"
            );
            assert_eq!(requests[0].start_cursor, "cursor@123-family-shared");

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_persists_dynamically_admitted_component() {
        use prost::Message;
        use tycho_common::models::token::Token;
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::Response,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    scripted_session_response("trace", 42),
                    crate::testing::family_block_response(
                        "cursor",
                        42,
                        1_718_000_000,
                        vec![substreams::TransactionChanges {
                            tx: Some(substreams::Transaction {
                                hash: vec![0xaa; 32],
                                from: vec![0x01; 20],
                                to: vec![0x02; 20],
                                index: 0,
                            }),
                            contract_changes: vec![substreams::ContractChange {
                                address: vec![0x44; 20],
                                slots: vec![],
                                token_balances: vec![],
                                balance: vec![],
                                code: vec![],
                                change: substreams::ChangeType::Creation as i32,
                            }],
                            entity_changes: vec![substreams::EntityChanges {
                                component_id: "v2-dynamic-pool".to_string(),
                                attributes: vec![substreams::Attribute {
                                    name: "reserve0".to_string(),
                                    value: Bytes::from(1_000u64)
                                        .lpad(32, 0)
                                        .to_vec(),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                            }],
                            component_changes: vec![substreams::ProtocolComponent {
                                id: "v2-dynamic-pool".to_string(),
                                tokens: vec![token0.to_vec(), token1.to_vec()],
                                contracts: vec![vec![0x44; 20]],
                                static_att: vec![],
                                protocol_type: Some(substreams::ProtocolType {
                                    name: "uniswap_v2_pool".to_string(),
                                    financial_type: substreams::FinancialType::Swap as i32,
                                    attribute_schema: vec![],
                                    implementation_type: substreams::ImplementationType::Custom
                                        as i32,
                                }),
                                change: substreams::ChangeType::Creation as i32,
                            }],
                            balance_changes: vec![],
                            entrypoints: vec![],
                            entrypoint_params: vec![],
                        }],
                    ),
                    crate::testing::family_block_response("cursor", 43, 1_718_000_000, vec![]),
                ],
                grpc_status: "0",
                grpc_message: None,
            }])
            .await;
            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");
            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed tokens for dynamic component");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-dynamic");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let all_components = cached_gw
                .get_protocol_components(&chain, None, None, None, None)
                .await
                .expect("read persisted protocol components");
            let persisted = all_components
                .entity
                .iter()
                .find(|component| component.id == "v2-dynamic-pool")
                .unwrap_or_else(|| {
                    panic!(
                        "dynamic component not persisted; saw component ids {:?}",
                        all_components
                            .entity
                            .iter()
                            .map(|component| component.id.clone())
                            .collect::<Vec<_>>()
                    )
                });

            assert_eq!(persisted.protocol_system, "uniswap_v2");
            assert_eq!(persisted.contract_addresses, vec![Bytes::from(vec![0x44; 20])]);

            let rpc_port = {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("bind temp rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) =
                ServicesBuilder::new(direct_gw.clone(), rpc.clone(), "test-api-key".to_string())
                    .bind("127.0.0.1")
                    .port(rpc_port)
                    .protocol_systems(protocol_systems.clone())
                    .run()
                    .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let mut rpc_body = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                    .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                        "uniswap_v2",
                        vec!["v2-dynamic-pool".to_string()],
                        dto::Chain::Ethereum,
                    ))
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_components rpc should succeed, got {}",
                    response.status()
                );
                let body: dto::ProtocolComponentRequestResponse = response
                    .json()
                    .await
                    .expect("decode protocol components rpc response");
                if body.protocol_components.len() == 1 {
                    rpc_body = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let rpc_body = rpc_body
                .unwrap_or_else(|| panic!("protocol component never became queryable through rpc"));
            assert_eq!(rpc_body.protocol_components.len(), 1);
            assert_eq!(rpc_body.protocol_components[0].id, "v2-dynamic-pool");
            assert_eq!(rpc_body.protocol_components[0].protocol_system, "uniswap_v2");

            let mut state_body = None;
            for _ in 0..100 {
                let state_response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                    .json(&dto::ProtocolStateRequestBody {
                        protocol_ids: Some(vec!["v2-dynamic-pool".to_string()]),
                        protocol_system: "uniswap_v2".to_string(),
                        chain: dto::Chain::Ethereum,
                        include_balances: false,
                        version: dto::VersionParam::default(),
                        pagination: dto::PaginationParams::default(),
                    })
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    state_response.status().is_success(),
                    "protocol_state rpc should succeed, got {}",
                    state_response.status()
                );
                let body: dto::ProtocolStateRequestResponse = state_response
                    .json()
                    .await
                    .expect("decode protocol state rpc response");
                if body.states.len() == 1 {
                    state_body = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let state_body = state_body
                .unwrap_or_else(|| panic!("protocol state never became queryable through rpc"));
            assert_eq!(state_body.states.len(), 1);
            assert_eq!(state_body.states[0].component_id, "v2-dynamic-pool");
            assert_eq!(
                state_body.states[0]
                    .attributes
                    .get("reserve0"),
                Some(&Bytes::from(1_000u64).lpad(32, 0))
            );
            assert!(
                state_body.states[0].balances.is_empty(),
                "include_balances=false should omit component balances"
            );

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_persists_follow_up_state_for_dynamically_admitted_component() {
        use prost::Message;
        use tycho_common::models::token::Token;
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::Response,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    scripted_session_response("trace-follow-up", 42),
                    crate::testing::family_block_response(
                        "cursor-follow-up",
                        42,
                        1_718_000_000 + 42,
                        vec![substreams::TransactionChanges {
                            tx: Some(substreams::Transaction {
                                hash: vec![0xaa; 32],
                                from: vec![0x01; 20],
                                to: vec![0x02; 20],
                                index: 0,
                            }),
                            contract_changes: vec![substreams::ContractChange {
                                address: vec![0x44; 20],
                                slots: vec![],
                                token_balances: vec![],
                                balance: vec![],
                                code: vec![],
                                change: substreams::ChangeType::Creation as i32,
                            }],
                            entity_changes: vec![substreams::EntityChanges {
                                component_id: "v2-dynamic-pool".to_string(),
                                attributes: vec![substreams::Attribute {
                                    name: "reserve0".to_string(),
                                    value: Bytes::from(1_000u64).lpad(32, 0).to_vec(),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                            }],
                            component_changes: vec![substreams::ProtocolComponent {
                                id: "v2-dynamic-pool".to_string(),
                                tokens: vec![token0.to_vec(), token1.to_vec()],
                                contracts: vec![vec![0x44; 20]],
                                static_att: vec![],
                                protocol_type: Some(substreams::ProtocolType {
                                    name: "uniswap_v2_pool".to_string(),
                                    financial_type: substreams::FinancialType::Swap as i32,
                                    attribute_schema: vec![],
                                    implementation_type:
                                        substreams::ImplementationType::Custom as i32,
                                }),
                                change: substreams::ChangeType::Creation as i32,
                            }],
                            balance_changes: vec![],
                            entrypoints: vec![],
                            entrypoint_params: vec![],
                        }],
                    ),
                    crate::testing::family_block_response(
                        "cursor-follow-up",
                        43,
                        1_718_000_000 + 43,
                        vec![substreams::TransactionChanges {
                            tx: Some(substreams::Transaction {
                                hash: vec![0xbb; 32],
                                from: vec![0x01; 20],
                                to: vec![0x02; 20],
                                index: 0,
                            }),
                            contract_changes: vec![],
                            entity_changes: vec![substreams::EntityChanges {
                                component_id: "v2-dynamic-pool".to_string(),
                                attributes: vec![substreams::Attribute {
                                    name: "reserve0".to_string(),
                                    value: Bytes::from(2_000u64).lpad(32, 0).to_vec(),
                                    change: substreams::ChangeType::Update as i32,
                                }],
                            }],
                            component_changes: vec![],
                            balance_changes: vec![],
                            entrypoints: vec![],
                            entrypoint_params: vec![],
                        }],
                    ),
                    crate::testing::family_block_response(
                        "cursor-follow-up",
                        44,
                        1_718_000_000 + 44,
                        vec![],
                    ),
                ],
                grpc_status: "0",
                grpc_message: None,
            }])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");
            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed tokens for dynamic component");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-follow-up");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(shared_spkg_path.clone()))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(shared_spkg_path.clone()))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners.pop().expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&["v2-dynamic-pool"]),
                    false,
                    None,
                )
                .await
                .expect("read direct protocol state after follow-up update");

            assert_eq!(
                state.entity.len(),
                1,
                "expected direct gateway to expose one dynamic pool state, got {:?}",
                state
                    .entity
                    .iter()
                    .map(|entry| (entry.component_id.clone(), entry.attributes.clone()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                state.entity[0].attributes.get("reserve0"),
                Some(&Bytes::from(2_000u64).lpad(32, 0))
            );

            let explicit_version_state = direct_gw
                .get_protocol_states(
                    &chain,
                    Some(tycho_common::storage::Version(
                        tycho_common::storage::BlockOrTimestamp::Timestamp(
                            chrono::Utc::now().naive_utc(),
                        ),
                        tycho_common::storage::VersionKind::Last,
                    )),
                    Some("uniswap_v2".to_string()),
                    Some(&["v2-dynamic-pool"]),
                    false,
                    None,
                )
                .await
                .expect("read direct protocol state at explicit timestamp");
            assert_eq!(
                explicit_version_state.entity.len(),
                1,
                "expected explicit-version direct gateway query to expose one dynamic pool state, got {:?}",
                explicit_version_state
                    .entity
                    .iter()
                    .map(|entry| (entry.component_id.clone(), entry.attributes.clone()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                explicit_version_state.entity[0].attributes.get("reserve0"),
                Some(&Bytes::from(2_000u64).lpad(32, 0))
            );

            let rpc_port = {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")
                    .expect("bind temp rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) = ServicesBuilder::new(
                direct_gw.clone(),
                rpc.clone(),
                "test-api-key".to_string(),
            )
            .bind("127.0.0.1")
            .port(rpc_port)
            .protocol_systems(protocol_systems.clone())
            .run()
            .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let mut state_body = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                    .json(&dto::ProtocolStateRequestBody {
                        protocol_ids: Some(vec!["v2-dynamic-pool".to_string()]),
                        protocol_system: "uniswap_v2".to_string(),
                        chain: dto::Chain::Ethereum,
                        include_balances: false,
                        version: dto::VersionParam::default(),
                        pagination: dto::PaginationParams::default(),
                    })
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_state rpc should succeed, got {}",
                    response.status()
                );
                let body: dto::ProtocolStateRequestResponse = response
                    .json()
                    .await
                    .expect("decode protocol state rpc response");
                if body.states.len() == 1 {
                    state_body = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let state_body = state_body.unwrap_or_else(|| {
                panic!("protocol follow-up state never became queryable through rpc")
            });
            assert_eq!(state_body.states.len(), 1);
            assert_eq!(state_body.states[0].component_id, "v2-dynamic-pool");
            assert_eq!(
                state_body.states[0].attributes.get("reserve0"),
                Some(&Bytes::from(2_000u64).lpad(32, 0))
            );

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_reverts_dynamically_admitted_components_across_branches() {
        use prost::Message;
        use tycho_common::models::token::Token;
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{BlockScopedData, MapModuleOutput, Response},
            pb::sf::substreams::v1::Clock,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let v2_component_id = "v2-reorg-pool";
            let v3_component_id = "v3-reorg-pool";
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    scripted_session_response("trace-revert", 42),
                    crate::testing::family_block_response("cursor", 42, 1_718_900_000 + 42, vec![]),
                    crate::testing::family_block_response(
                        "cursor",
                        43,
                        1_718_900_000 + 43,
                        vec![
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xaa; 32],
                                    from: vec![0x01; 20],
                                    to: vec![0x02; 20],
                                    index: 0,
                                }),
                                contract_changes: vec![substreams::ContractChange {
                                    address: vec![0x44; 20],
                                    slots: vec![],
                                    token_balances: vec![],
                                    balance: vec![],
                                    code: vec![],
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v2_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "reserve0".to_string(),
                                        value: Bytes::from(1_000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: substreams::ChangeType::Creation as i32,
                                    }],
                                }],
                                component_changes: vec![substreams::ProtocolComponent {
                                    id: v2_component_id.to_string(),
                                    tokens: vec![token0.to_vec(), token1.to_vec()],
                                    contracts: vec![vec![0x44; 20]],
                                    static_att: vec![],
                                    protocol_type: Some(substreams::ProtocolType {
                                        name: "uniswap_v2_pool".to_string(),
                                        financial_type: substreams::FinancialType::Swap as i32,
                                        attribute_schema: vec![],
                                        implementation_type: substreams::ImplementationType::Custom
                                            as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xbb; 32],
                                    from: vec![0x03; 20],
                                    to: vec![0x04; 20],
                                    index: 1,
                                }),
                                contract_changes: vec![substreams::ContractChange {
                                    address: vec![0x55; 20],
                                    slots: vec![],
                                    token_balances: vec![],
                                    balance: vec![],
                                    code: vec![],
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v3_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "tick".to_string(),
                                        value: Bytes::from(7u64).lpad(32, 0).to_vec(),
                                        change: substreams::ChangeType::Creation as i32,
                                    }],
                                }],
                                component_changes: vec![substreams::ProtocolComponent {
                                    id: v3_component_id.to_string(),
                                    tokens: vec![token0.to_vec(), token1.to_vec()],
                                    contracts: vec![vec![0x55; 20]],
                                    static_att: vec![],
                                    protocol_type: Some(substreams::ProtocolType {
                                        name: "uniswap_v3_pool".to_string(),
                                        financial_type: substreams::FinancialType::Swap as i32,
                                        attribute_schema: vec![],
                                        implementation_type: substreams::ImplementationType::Custom
                                            as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                        ],
                    ),
                    crate::testing::family_block_response("cursor", 44, 1_718_900_000 + 44, vec![]),
                    scripted_undo_response("cursor", 42),
                ],
                grpc_status: "0",
                grpc_message: None,
            }])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");
            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed tokens for shared-family revert test");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-revert");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let v2_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read reverted V2 component universe");
            assert!(
                v2_components
                    .entity
                    .iter()
                    .all(|component| component.id != v2_component_id),
                "V2 component should be absent after shared-family revert"
            );
            let v3_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read reverted V3 component universe");
            assert!(
                v3_components
                    .entity
                    .iter()
                    .all(|component| component.id != v3_component_id),
                "V3 component should be absent after shared-family revert"
            );

            let v2_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[v2_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read reverted V2 protocol state");
            assert!(v2_state.entity.is_empty());
            let v3_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[v3_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read reverted V3 protocol state");
            assert!(v3_state.entity.is_empty());

            let rpc_port = {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("bind temp rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) =
                ServicesBuilder::new(direct_gw.clone(), rpc.clone(), "test-api-key".to_string())
                    .bind("127.0.0.1")
                    .port(rpc_port)
                    .protocol_systems(protocol_systems.clone())
                    .run()
                    .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let v2_rpc_components: dto::ProtocolComponentRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                    "uniswap_v2",
                    vec![v2_component_id.to_string()],
                    dto::Chain::Ethereum,
                ))
                .send()
                .await
                .expect("call reverted V2 protocol_components rpc")
                .json()
                .await
                .expect("decode reverted V2 protocol_components response");
            assert!(v2_rpc_components
                .protocol_components
                .is_empty());

            let v3_rpc_components: dto::ProtocolComponentRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                    "uniswap_v3",
                    vec![v3_component_id.to_string()],
                    dto::Chain::Ethereum,
                ))
                .send()
                .await
                .expect("call reverted V3 protocol_components rpc")
                .json()
                .await
                .expect("decode reverted V3 protocol_components response");
            assert!(v3_rpc_components
                .protocol_components
                .is_empty());

            let v2_rpc_state: dto::ProtocolStateRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                .json(&dto::ProtocolStateRequestBody {
                    protocol_ids: Some(vec![v2_component_id.to_string()]),
                    protocol_system: "uniswap_v2".to_string(),
                    chain: dto::Chain::Ethereum,
                    include_balances: false,
                    version: dto::VersionParam::default(),
                    pagination: dto::PaginationParams::default(),
                })
                .send()
                .await
                .expect("call reverted V2 protocol_state rpc")
                .json()
                .await
                .expect("decode reverted V2 protocol_state response");
            assert!(v2_rpc_state.states.is_empty());

            let v3_rpc_state: dto::ProtocolStateRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                .json(&dto::ProtocolStateRequestBody {
                    protocol_ids: Some(vec![v3_component_id.to_string()]),
                    protocol_system: "uniswap_v3".to_string(),
                    chain: dto::Chain::Ethereum,
                    include_balances: false,
                    version: dto::VersionParam::default(),
                    pagination: dto::PaginationParams::default(),
                })
                .send()
                .await
                .expect("call reverted V3 protocol_state rpc")
                .json()
                .await
                .expect("decode reverted V3 protocol_state response");
            assert!(v3_rpc_state.states.is_empty());

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_recovers_after_revert_and_reapplies_multi_branch_state() {
        use prost::Message;
        use tycho_common::models::token::Token;
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{BlockScopedData, MapModuleOutput, Response},
            pb::sf::substreams::v1::Clock,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let v2_component_id = "v2-recover-pool";
            let v3_component_id = "v3-recover-pool";
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    scripted_session_response("trace-recover", 42),
                    crate::testing::family_block_response(
                        "cursor-recover",
                        42,
                        1_718_910_000 + 42,
                        vec![],
                    ),
                    crate::testing::family_block_response(
                        "cursor-recover",
                        43,
                        1_718_910_000 + 43,
                        vec![
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xaa; 32],
                                    from: vec![0x01; 20],
                                    to: vec![0x02; 20],
                                    index: 0,
                                }),
                                contract_changes: vec![substreams::ContractChange {
                                    address: vec![0x44; 20],
                                    slots: vec![],
                                    token_balances: vec![],
                                    balance: vec![],
                                    code: vec![],
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v2_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "reserve0".to_string(),
                                        value: Bytes::from(1_000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: substreams::ChangeType::Creation as i32,
                                    }],
                                }],
                                component_changes: vec![substreams::ProtocolComponent {
                                    id: v2_component_id.to_string(),
                                    tokens: vec![token0.to_vec(), token1.to_vec()],
                                    contracts: vec![vec![0x44; 20]],
                                    static_att: vec![],
                                    protocol_type: Some(substreams::ProtocolType {
                                        name: "uniswap_v2_pool".to_string(),
                                        financial_type: substreams::FinancialType::Swap as i32,
                                        attribute_schema: vec![],
                                        implementation_type: substreams::ImplementationType::Custom
                                            as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xbb; 32],
                                    from: vec![0x03; 20],
                                    to: vec![0x04; 20],
                                    index: 1,
                                }),
                                contract_changes: vec![substreams::ContractChange {
                                    address: vec![0x55; 20],
                                    slots: vec![],
                                    token_balances: vec![],
                                    balance: vec![],
                                    code: vec![],
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v3_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "tick".to_string(),
                                        value: Bytes::from(7u64).lpad(32, 0).to_vec(),
                                        change: substreams::ChangeType::Creation as i32,
                                    }],
                                }],
                                component_changes: vec![substreams::ProtocolComponent {
                                    id: v3_component_id.to_string(),
                                    tokens: vec![token0.to_vec(), token1.to_vec()],
                                    contracts: vec![vec![0x55; 20]],
                                    static_att: vec![],
                                    protocol_type: Some(substreams::ProtocolType {
                                        name: "uniswap_v3_pool".to_string(),
                                        financial_type: substreams::FinancialType::Swap as i32,
                                        attribute_schema: vec![],
                                        implementation_type: substreams::ImplementationType::Custom
                                            as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                        ],
                    ),
                    crate::testing::family_block_response(
                        "cursor-recover",
                        44,
                        1_718_910_000 + 44,
                        vec![],
                    ),
                    scripted_undo_response("cursor-recover", 42),
                    crate::testing::family_block_response(
                        "cursor-recover",
                        43,
                        1_718_910_000 + 43,
                        vec![
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xca; 32],
                                    from: vec![0x01; 20],
                                    to: vec![0x02; 20],
                                    index: 0,
                                }),
                                contract_changes: vec![substreams::ContractChange {
                                    address: vec![0x44; 20],
                                    slots: vec![],
                                    token_balances: vec![],
                                    balance: vec![],
                                    code: vec![],
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v2_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "reserve0".to_string(),
                                        value: Bytes::from(2_500u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: substreams::ChangeType::Creation as i32,
                                    }],
                                }],
                                component_changes: vec![substreams::ProtocolComponent {
                                    id: v2_component_id.to_string(),
                                    tokens: vec![token0.to_vec(), token1.to_vec()],
                                    contracts: vec![vec![0x44; 20]],
                                    static_att: vec![],
                                    protocol_type: Some(substreams::ProtocolType {
                                        name: "uniswap_v2_pool".to_string(),
                                        financial_type: substreams::FinancialType::Swap as i32,
                                        attribute_schema: vec![],
                                        implementation_type: substreams::ImplementationType::Custom
                                            as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xcb; 32],
                                    from: vec![0x03; 20],
                                    to: vec![0x04; 20],
                                    index: 1,
                                }),
                                contract_changes: vec![substreams::ContractChange {
                                    address: vec![0x55; 20],
                                    slots: vec![],
                                    token_balances: vec![],
                                    balance: vec![],
                                    code: vec![],
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v3_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "tick".to_string(),
                                        value: Bytes::from(11u64).lpad(32, 0).to_vec(),
                                        change: substreams::ChangeType::Creation as i32,
                                    }],
                                }],
                                component_changes: vec![substreams::ProtocolComponent {
                                    id: v3_component_id.to_string(),
                                    tokens: vec![token0.to_vec(), token1.to_vec()],
                                    contracts: vec![vec![0x55; 20]],
                                    static_att: vec![],
                                    protocol_type: Some(substreams::ProtocolType {
                                        name: "uniswap_v3_pool".to_string(),
                                        financial_type: substreams::FinancialType::Swap as i32,
                                        attribute_schema: vec![],
                                        implementation_type: substreams::ImplementationType::Custom
                                            as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                        ],
                    ),
                    crate::testing::family_block_response(
                        "cursor-recover",
                        44,
                        1_718_910_000 + 44,
                        vec![
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xda; 32],
                                    from: vec![0x01; 20],
                                    to: vec![0x02; 20],
                                    index: 0,
                                }),
                                contract_changes: vec![],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v2_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "reserve0".to_string(),
                                        value: Bytes::from(3_000u64)
                                            .lpad(32, 0)
                                            .to_vec(),
                                        change: substreams::ChangeType::Update as i32,
                                    }],
                                }],
                                component_changes: vec![],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                            substreams::TransactionChanges {
                                tx: Some(substreams::Transaction {
                                    hash: vec![0xdb; 32],
                                    from: vec![0x03; 20],
                                    to: vec![0x04; 20],
                                    index: 1,
                                }),
                                contract_changes: vec![],
                                entity_changes: vec![substreams::EntityChanges {
                                    component_id: v3_component_id.to_string(),
                                    attributes: vec![substreams::Attribute {
                                        name: "tick".to_string(),
                                        value: Bytes::from(13u64).lpad(32, 0).to_vec(),
                                        change: substreams::ChangeType::Update as i32,
                                    }],
                                }],
                                component_changes: vec![],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                        ],
                    ),
                    crate::testing::family_block_response(
                        "cursor-recover",
                        45,
                        1_718_910_000 + 45,
                        vec![],
                    ),
                ],
                grpc_status: "0",
                grpc_message: None,
            }])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");
            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed tokens for shared-family recover test");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-recover");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        42,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let v2_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read recovered V2 component universe");
            assert!(
                v2_components
                    .entity
                    .iter()
                    .any(|component| component.id == v2_component_id),
                "V2 component should be present after shared-family recovery"
            );
            let v3_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read recovered V3 component universe");
            assert!(
                v3_components
                    .entity
                    .iter()
                    .any(|component| component.id == v3_component_id),
                "V3 component should be present after shared-family recovery"
            );

            let v2_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[v2_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read recovered V2 protocol state");
            assert_eq!(v2_state.entity.len(), 1);
            assert_eq!(
                v2_state.entity[0]
                    .attributes
                    .get("reserve0"),
                Some(&Bytes::from(3_000u64).lpad(32, 0))
            );
            let v3_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[v3_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read recovered V3 protocol state");
            assert_eq!(v3_state.entity.len(), 1);
            assert_eq!(
                v3_state.entity[0]
                    .attributes
                    .get("tick"),
                Some(&Bytes::from(13u64).lpad(32, 0))
            );

            let rpc_port = {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("bind temp rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) =
                ServicesBuilder::new(direct_gw.clone(), rpc.clone(), "test-api-key".to_string())
                    .bind("127.0.0.1")
                    .port(rpc_port)
                    .protocol_systems(protocol_systems.clone())
                    .run()
                    .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let v2_rpc_components: dto::ProtocolComponentRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                    "uniswap_v2",
                    vec![v2_component_id.to_string()],
                    dto::Chain::Ethereum,
                ))
                .send()
                .await
                .expect("call recovered V2 protocol_components rpc")
                .json()
                .await
                .expect("decode recovered V2 protocol_components response");
            assert_eq!(
                v2_rpc_components
                    .protocol_components
                    .len(),
                1
            );

            let v3_rpc_components: dto::ProtocolComponentRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                    "uniswap_v3",
                    vec![v3_component_id.to_string()],
                    dto::Chain::Ethereum,
                ))
                .send()
                .await
                .expect("call recovered V3 protocol_components rpc")
                .json()
                .await
                .expect("decode recovered V3 protocol_components response");
            assert_eq!(
                v3_rpc_components
                    .protocol_components
                    .len(),
                1
            );

            let v2_rpc_state: dto::ProtocolStateRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                .json(&dto::ProtocolStateRequestBody {
                    protocol_ids: Some(vec![v2_component_id.to_string()]),
                    protocol_system: "uniswap_v2".to_string(),
                    chain: dto::Chain::Ethereum,
                    include_balances: false,
                    version: dto::VersionParam::default(),
                    pagination: dto::PaginationParams::default(),
                })
                .send()
                .await
                .expect("call recovered V2 protocol_state rpc")
                .json()
                .await
                .expect("decode recovered V2 protocol_state response");
            assert_eq!(v2_rpc_state.states.len(), 1);
            assert_eq!(
                v2_rpc_state.states[0]
                    .attributes
                    .get("reserve0"),
                Some(&Bytes::from(3_000u64).lpad(32, 0))
            );

            let v3_rpc_state: dto::ProtocolStateRequestResponse = client
                .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                .json(&dto::ProtocolStateRequestBody {
                    protocol_ids: Some(vec![v3_component_id.to_string()]),
                    protocol_system: "uniswap_v3".to_string(),
                    chain: dto::Chain::Ethereum,
                    include_balances: false,
                    version: dto::VersionParam::default(),
                    pagination: dto::PaginationParams::default(),
                })
                .send()
                .await
                .expect("call recovered V3 protocol_state rpc")
                .json()
                .await
                .expect("decode recovered V3 protocol_state response");
            assert_eq!(v3_rpc_state.states.len(), 1);
            assert_eq!(
                v3_rpc_state.states[0]
                    .attributes
                    .get("tick"),
                Some(&Bytes::from(13u64).lpad(32, 0))
            );

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_factory_style_dynamic_component_joins_seeded_universe_and_receives_follow_up_state(
    ) {
        use ::substreams::store::StoreGet;
        use ethabi::{ethereum_types::U256, Token as AbiToken};
        use ethereum_uniswap_v2_v3_combined::{
            run_map_uniswap_family_protocol_changes, run_v2_map_pool_events,
            run_v2_map_pools_created,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_common::models::{
            blockchain::{Block, Transaction},
            contract::Account,
            contract::AccountDelta,
            token::Token,
            ChangeType, FinancialType, ProtocolType,
        };
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::pb::tycho::evm::v1::{
            Block as V2ProtoBlock, BlockChanges as V2BlockChanges,
        };

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn v2_sync_block(
            number: u64,
            timestamp_secs: i64,
            pool: u8,
            reserve0: u64,
            reserve1: u64,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![vec![
                    28, 65, 30, 154, 150, 224, 113, 36, 28, 47, 33, 247, 114, 107, 23, 174, 137,
                    227, 202, 180, 199, 139, 229, 14, 6, 43, 3, 169, 255, 251, 186, 209,
                ]],
                data: ethabi::encode(&[
                    AbiToken::Uint(U256::from(reserve0)),
                    AbiToken::Uint(U256::from(reserve1)),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp { seconds: timestamp_secs, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xbb; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let seed_component_id = "v2-seeded-pool";
            let dynamic_component_id = "0x4545454545454545454545454545454545454545";
            let v2_creation_block = v2_pair_created_block(43, 1_718_100_043, 0xf1, 0xa0, 0xc0, 0x45);
            let v2_creation_changes = run_v2_map_pools_created(
                "factory_address=0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1&protocol_type_name=uniswap_v2_pool"
                    .to_string(),
                v2_creation_block.clone(),
            )
            .expect("combined V2 handler should emit pair-created changes");
            let created_pool = v2_creation_changes.changes[0].component_changes[0].clone();
            assert_eq!(v2_creation_changes.changes.len(), 1);
            assert_eq!(
                v2_creation_changes.changes[0].component_changes[0].contracts,
                vec![vec![0x45; 20]],
                "real pair-created path should carry the pool contract for downstream routing"
            );
            let family_creation_changes = substreams::BlockChanges::decode(
                run_map_uniswap_family_protocol_changes(
                    v2_creation_changes.clone(),
                    V2BlockChanges {
                        block: v2_creation_changes.block.clone(),
                        changes: vec![],
                        storage_changes: vec![],
                    },
                )
                .expect("combined family handler should merge V2 created-pool output")
                    .encode_to_vec()
                    .as_slice(),
            )
            .expect("bridge family block changes into indexer protobuf type");
            let v2_follow_up_block = v2_sync_block(44, 1_718_100_044, 0x45, 2_000, 3_000);
            let pools_store = MockProtoStore::new(0).with_last(
                format!("Pool:{dynamic_component_id}"),
                created_pool,
            );
            let v2_follow_up_changes = run_v2_map_pool_events(
                format!("pools={dynamic_component_id}"),
                v2_follow_up_block.clone(),
                V2BlockChanges {
                    block: Some(V2ProtoBlock {
                        hash: v2_follow_up_block.hash.clone(),
                        parent_hash: v2_follow_up_block
                            .header
                            .as_ref()
                            .map(|header| header.parent_hash.clone())
                            .unwrap_or_default(),
                        number: v2_follow_up_block.number,
                        ts: v2_follow_up_block
                            .header
                            .as_ref()
                            .and_then(|header| header.timestamp.as_ref())
                            .map(|timestamp| timestamp.seconds as u64)
                            .unwrap_or_default(),
                    }),
                    changes: vec![],
                    storage_changes: vec![],
                },
                &pools_store,
            )
            .expect("combined V2 handler should emit sync follow-up changes");
            assert_eq!(v2_follow_up_changes.changes.len(), 1);
            assert_eq!(
                v2_follow_up_changes.changes[0].entity_changes[0].component_id,
                dynamic_component_id
            );
            let family_follow_up_changes = substreams::BlockChanges::decode(
                run_map_uniswap_family_protocol_changes(
                    v2_follow_up_changes.clone(),
                    V2BlockChanges {
                        block: v2_follow_up_changes.block.clone(),
                        changes: vec![],
                        storage_changes: vec![],
                    },
                )
                .expect("combined family handler should merge V2 follow-up output")
                    .encode_to_vec()
                    .as_slice(),
            )
            .expect("bridge v2 sync follow-up into indexer protobuf type");

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    scripted_session_response("trace-factory", 43),
                    family_block_response_from_block_changes(
                        "cursor-factory",
                        family_creation_changes,
                    ),
                    family_block_response_from_block_changes(
                        "cursor-factory",
                        family_follow_up_changes,
                    ),
                    family_block_response("cursor-factory", 45, 1_718_100_045, vec![]),
                ],
                grpc_status: "0",
                grpc_message: None,
            }])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed tokens");
            direct_gw
                .add_protocol_types(&[ProtocolType::new(
                    "uniswap_v2_pool".to_string(),
                    FinancialType::Swap,
                    None,
                    ImplementationType::Custom,
                )])
                .await
                .expect("seed protocol type for existing universe component");
            let seed_block = Block::new(
                42,
                chain,
                Bytes::from(vec![0x77; 32]),
                Bytes::from(vec![0x66; 32]),
                chrono::Utc::now().naive_utc(),
            );
            direct_gw
                .upsert_block(std::slice::from_ref(&seed_block))
                .await
                .expect("seed block for existing universe component");
            let seed_tx = Transaction::new(
                Bytes::from(vec![0x99; 32]),
                seed_block.hash.clone(),
                Bytes::from(vec![0x10; 20]),
                Some(Bytes::from(vec![0x20; 20])),
                0,
            );
            direct_gw
                .upsert_tx(std::slice::from_ref(&seed_tx))
                .await
                .expect("seed tx for existing universe component");
            let seeded_contract = Account::new(
                chain,
                Bytes::from(vec![0x33; 20]),
                "SeededPoolContract".to_string(),
                HashMap::new(),
                Bytes::new(),
                HashMap::new(),
                Bytes::new(),
                Bytes::new(),
                seed_tx.hash.clone(),
                seed_tx.hash.clone(),
                Some(seed_tx.hash.clone()),
            );
            direct_gw
                .insert_contract(&seeded_contract)
                .await
                .expect("seed contract for existing universe component");
            let seeded_contract_delta: AccountDelta = seeded_contract.clone().into();
            direct_gw
                .update_contracts(&[(seed_tx.hash.clone(), seeded_contract_delta)])
                .await
                .expect("seed contract code/state for existing universe component");

            let seeded_component = tycho_common::models::protocol::ProtocolComponent::new(
                seed_component_id,
                "uniswap_v2",
                "uniswap_v2_pool",
                chain,
                vec![token0.clone(), token1.clone()],
                vec![Bytes::from(vec![0x33; 20])],
                HashMap::from([("factory_address".to_string(), Bytes::from(vec![0xf1; 20]))]),
                ChangeType::Creation,
                seed_tx.hash.clone(),
                seed_block.ts,
            );
            direct_gw
                .add_protocol_components(std::slice::from_ref(&seeded_component))
                .await
                .expect("seed existing universe component");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-factory");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        43,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        43,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners.pop().expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read combined component universe after factory-style onboarding");
            let component_ids = components
                .entity
                .iter()
                .map(|component| component.id.clone())
                .collect::<Vec<_>>();
            assert!(
                component_ids.iter().any(|id| id == seed_component_id),
                "expected seeded universe component to remain visible, saw {:?}",
                component_ids
            );
            assert!(
                component_ids.iter().any(|id| id == dynamic_component_id),
                "expected factory-style dynamic component to join seeded universe, saw {:?}",
                component_ids
            );

            let dynamic_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[dynamic_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic factory-style component state");
            assert_eq!(dynamic_state.entity.len(), 1);
            assert_eq!(
                dynamic_state.entity[0].attributes.get("reserve0"),
                Some(&Bytes::from(vec![0x07, 0xd0]))
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_v3_dynamic_component_joins_seeded_universe_and_receives_follow_up_state(
    ) {
        use ::substreams::{
            pb::substreams::StoreDeltas,
            store::{StoreGet, StoreGetProto},
        };
        use ethabi::{
            ethereum_types::{Address, U256},
            Token as AbiToken,
        };
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v3_pool_events, build_family_v3_protocol_changes,
            build_uniswap_family_protocol_changes_from_v3_created_pools,
            build_uniswap_family_protocol_changes_from_v3_protocol_changes,
            FamilyV3LiquidityChanges, FamilyV3Pool, FamilyV3TickDeltas,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_common::models::{
            blockchain::{Block, Transaction},
            contract::Account,
            contract::AccountDelta,
            token::Token,
            ChangeType, FinancialType, ProtocolType,
        };
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::models::{BlockBalanceDeltas, BlockEntityChanges};

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn topic_address(byte: u8) -> Vec<u8> {
            ethabi::encode(&[AbiToken::Address(Address::from_slice(&address(byte)))])
        }

        fn v3_swap_block(
            number: u64,
            timestamp_secs: i64,
            pool: u8,
            sender: u8,
            recipient: u8,
            amount0: u64,
            amount1: u64,
            sqrt_price_x96: u64,
            liquidity: u64,
            tick: i32,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![
                    vec![
                        196, 32, 121, 249, 74, 99, 80, 215, 230, 35, 95, 41, 23, 73, 36, 249, 40,
                        204, 42, 200, 24, 235, 100, 254, 216, 0, 78, 17, 95, 188, 202, 103,
                    ],
                    topic_address(sender),
                    topic_address(recipient),
                ],
                data: ethabi::encode(&[
                    AbiToken::Int(amount0.into()),
                    AbiToken::Int(amount1.into()),
                    AbiToken::Uint(U256::from(sqrt_price_x96)),
                    AbiToken::Uint(U256::from(liquidity)),
                    AbiToken::Int(tick.into()),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp { seconds: timestamp_secs, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xde; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let seed_component_id = "v3-seeded-pool";
            let dynamic_component_id = "0x5656565656565656565656565656565656565656";
            let v3_creation_block =
                v3_pool_created_block(63, 1_718_500_063, 0xf1, 0xa0, 0xc0, 500, 10, 0x56);
            let family_creation_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_created_pools(
                    "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
                    v3_creation_block,
                )
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge seeded-universe V3 family block changes into indexer protobuf type");
            assert_eq!(
                family_creation_changes.changes[0].component_changes[0].contracts,
                vec![vec![0x56; 20]]
            );
            let v3_follow_up_block =
                v3_swap_block(64, 1_718_500_064, 0x56, 0x01, 0x02, 10, 20, 123_456, 654_321, 7);
            let empty_pools_store: StoreGetProto<FamilyV3Pool> = StoreGet::new(0);
            let v3_events = build_family_v3_pool_events(
                &format!("factory=0x{}&pool={dynamic_component_id}", hex::encode(address(0xf1)),),
                v3_follow_up_block.clone(),
                &empty_pools_store,
            );
            let v3_follow_up_changes = build_family_v3_protocol_changes(
                v3_follow_up_block.clone(),
                BlockEntityChanges { block: None, changes: vec![] },
                v3_events,
                BlockBalanceDeltas { balance_deltas: vec![] },
                StoreDeltas { deltas: vec![] },
                FamilyV3TickDeltas { deltas: vec![] },
                StoreDeltas { deltas: vec![] },
                FamilyV3LiquidityChanges { changes: vec![] },
                StoreDeltas { deltas: vec![] },
            );
            let family_follow_up_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_protocol_changes(
                    v3_follow_up_changes,
                )
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge seeded-universe V3 swap follow-up into indexer protobuf type");

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    scripted_session_response("trace-v3-seeded-universe", 63),
                    family_block_response_from_block_changes(
                        "cursor-v3-seeded-universe",
                        family_creation_changes,
                    ),
                    family_block_response_from_block_changes(
                        "cursor-v3-seeded-universe",
                        family_follow_up_changes,
                    ),
                    family_block_response("cursor-v3-seeded-universe", 65, 1_718_500_065, vec![]),
                ],
                grpc_status: "0",
                grpc_message: None,
            }])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed V3 seeded-universe test tokens");
            direct_gw
                .add_protocol_types(&[ProtocolType::new(
                    "uniswap_v3_pool".to_string(),
                    FinancialType::Swap,
                    None,
                    ImplementationType::Custom,
                )])
                .await
                .expect("seed protocol type for existing V3 universe component");
            let seed_block = Block::new(
                62,
                chain,
                Bytes::from(vec![0x87; 32]),
                Bytes::from(vec![0x76; 32]),
                chrono::Utc::now().naive_utc(),
            );
            direct_gw
                .upsert_block(std::slice::from_ref(&seed_block))
                .await
                .expect("seed block for existing V3 universe component");
            let seed_tx = Transaction::new(
                Bytes::from(vec![0xa9; 32]),
                seed_block.hash.clone(),
                Bytes::from(vec![0x10; 20]),
                Some(Bytes::from(vec![0x20; 20])),
                0,
            );
            direct_gw
                .upsert_tx(std::slice::from_ref(&seed_tx))
                .await
                .expect("seed tx for existing V3 universe component");
            let seeded_contract = Account::new(
                chain,
                Bytes::from(vec![0x43; 20]),
                "SeededV3PoolContract".to_string(),
                HashMap::new(),
                Bytes::new(),
                HashMap::new(),
                Bytes::new(),
                Bytes::new(),
                seed_tx.hash.clone(),
                seed_tx.hash.clone(),
                Some(seed_tx.hash.clone()),
            );
            direct_gw
                .insert_contract(&seeded_contract)
                .await
                .expect("seed contract for existing V3 universe component");
            let seeded_contract_delta: AccountDelta = seeded_contract.clone().into();
            direct_gw
                .update_contracts(&[(seed_tx.hash.clone(), seeded_contract_delta)])
                .await
                .expect("seed contract code/state for existing V3 universe component");

            let seeded_component = tycho_common::models::protocol::ProtocolComponent::new(
                seed_component_id,
                "uniswap_v3",
                "uniswap_v3_pool",
                chain,
                vec![token0.clone(), token1.clone()],
                vec![Bytes::from(vec![0x43; 20])],
                HashMap::from([("factory_address".to_string(), Bytes::from(vec![0xf1; 20]))]),
                ChangeType::Creation,
                seed_tx.hash.clone(),
                seed_block.ts,
            );
            direct_gw
                .add_protocol_components(std::slice::from_ref(&seeded_component))
                .await
                .expect("seed existing V3 universe component");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path =
                test_family_shared_spkg_path("combined-family-v3-seeded-universe");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        63,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        63,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined V3 seeded-universe extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read combined V3 seeded universe");
            let component_ids = components
                .entity
                .iter()
                .map(|component| component.id.clone())
                .collect::<Vec<_>>();
            assert!(
                component_ids
                    .iter()
                    .any(|id| id == seed_component_id),
                "expected seeded V3 universe component to remain visible, saw {:?}",
                component_ids
            );
            assert!(
                component_ids
                    .iter()
                    .any(|id| id == dynamic_component_id),
                "expected V3 dynamic component to join seeded universe, saw {:?}",
                component_ids
            );

            let dynamic_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[dynamic_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic V3 seeded-universe component state");
            assert_eq!(dynamic_state.entity.len(), 1);
            assert_eq!(
                dynamic_state.entity[0]
                    .attributes
                    .get("tick"),
                Some(&Bytes::from(vec![0x07]))
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_v3_dynamic_component_from_real_pool_created_block_receives_follow_up_state(
    ) {
        use ::substreams::{
            pb::substreams::StoreDeltas,
            store::{StoreGet, StoreGetProto},
        };
        use ethabi::{
            ethereum_types::{Address, U256},
            Token as AbiToken,
        };
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v3_pool_events, build_family_v3_protocol_changes,
            build_uniswap_family_protocol_changes_from_v3_created_pools,
            build_uniswap_family_protocol_changes_from_v3_protocol_changes,
            FamilyV3LiquidityChanges, FamilyV3Pool, FamilyV3TickDeltas,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_common::models::token::Token;
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::models::{BlockBalanceDeltas, BlockEntityChanges};

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn topic_address(byte: u8) -> Vec<u8> {
            ethabi::encode(&[AbiToken::Address(Address::from_slice(&address(byte)))])
        }

        fn v3_swap_block(
            number: u64,
            timestamp_secs: i64,
            pool: u8,
            sender: u8,
            recipient: u8,
            amount0: u64,
            amount1: u64,
            sqrt_price_x96: u64,
            liquidity: u64,
            tick: i32,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![
                    vec![
                        196, 32, 121, 249, 74, 99, 80, 215, 230, 35, 95, 41, 23, 73, 36, 249, 40,
                        204, 42, 200, 24, 235, 100, 254, 216, 0, 78, 17, 95, 188, 202, 103,
                    ],
                    topic_address(sender),
                    topic_address(recipient),
                ],
                data: ethabi::encode(&[
                    AbiToken::Int(amount0.into()),
                    AbiToken::Int(amount1.into()),
                    AbiToken::Uint(U256::from(sqrt_price_x96)),
                    AbiToken::Uint(U256::from(liquidity)),
                    AbiToken::Int(tick.into()),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp { seconds: timestamp_secs, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xde; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let dynamic_component_id = "0x4646464646464646464646464646464646464646";
            let v3_creation_block =
                v3_pool_created_block(53, 1_718_300_053, 0xf1, 0xa0, 0xc0, 500, 10, 0x46);
            let family_creation_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_created_pools(
                    "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
                    v3_creation_block,
                )
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge V3 family block changes into indexer protobuf type");
            assert_eq!(
                family_creation_changes.changes[0].component_changes[0].contracts,
                vec![vec![0x46; 20]]
            );
            assert_eq!(
                family_creation_changes.changes[0].contract_changes[0].address,
                vec![0x46; 20]
            );
            let v3_follow_up_block =
                v3_swap_block(54, 1_718_300_054, 0x46, 0x01, 0x02, 10, 20, 123_456, 654_321, 7);
            let empty_pools_store: StoreGetProto<FamilyV3Pool> = StoreGet::new(0);
            let v3_events = build_family_v3_pool_events(
                &format!("factory=0x{}&pool={dynamic_component_id}", hex::encode(address(0xf1)),),
                v3_follow_up_block.clone(),
                &empty_pools_store,
            );
            let v3_follow_up_changes = build_family_v3_protocol_changes(
                v3_follow_up_block.clone(),
                BlockEntityChanges { block: None, changes: vec![] },
                v3_events,
                BlockBalanceDeltas { balance_deltas: vec![] },
                StoreDeltas { deltas: vec![] },
                FamilyV3TickDeltas { deltas: vec![] },
                StoreDeltas { deltas: vec![] },
                FamilyV3LiquidityChanges { changes: vec![] },
                StoreDeltas { deltas: vec![] },
            );
            let family_follow_up_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_protocol_changes(
                    v3_follow_up_changes,
                )
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge V3 swap follow-up into indexer protobuf type");

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    scripted_session_response("trace-v3-factory", 53),
                    family_block_response_from_block_changes(
                        "cursor-v3-factory",
                        family_creation_changes,
                    ),
                    family_block_response_from_block_changes(
                        "cursor-v3-factory",
                        family_follow_up_changes,
                    ),
                    family_block_response("cursor-v3-factory", 55, 1_718_300_055, vec![]),
                ],
                grpc_status: "0",
                grpc_message: None,
            }])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed V3 dynamic test tokens");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-v3-dynamic");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        53,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        53,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read combined V3 component universe");
            let component_ids = components
                .entity
                .iter()
                .map(|component| component.id.clone())
                .collect::<Vec<_>>();
            assert!(
                component_ids
                    .iter()
                    .any(|id| id == dynamic_component_id),
                "expected V3 dynamic component to be visible, saw {:?}",
                component_ids
            );

            let dynamic_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[dynamic_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic V3 component state");
            assert_eq!(dynamic_state.entity.len(), 1);
            assert_eq!(
                dynamic_state.entity[0]
                    .attributes
                    .get("tick"),
                Some(&Bytes::from(vec![0x07]))
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    fn combined_family_real_history_slice_fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("combined_family_real_history_slice.json")
    }

    fn combined_family_real_history_slice_scripts(
    ) -> Vec<tycho_indexer::substreams::mock::MockSubstreamsScript> {
        use ::substreams::{
            pb::substreams::StoreDeltas,
            store::{StoreGet, StoreGetProto},
        };
        use ethabi::{
            ethereum_types::{Address, U256},
            Token as AbiToken,
        };
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v3_pool_events, build_family_v3_protocol_changes,
            build_uniswap_family_protocol_changes_from_v3_created_pools,
            build_uniswap_family_protocol_changes_from_v3_protocol_changes,
            run_map_uniswap_family_protocol_changes, run_v2_map_pool_events,
            run_v2_map_pools_created, FamilyV3LiquidityChanges, FamilyV3Pool, FamilyV3TickDeltas,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_indexer::substreams::mock::MockSubstreamsScript;
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::{
            models::{BlockBalanceDeltas, BlockEntityChanges},
            pb::tycho::evm::v1::{Block as V2ProtoBlock, BlockChanges as V2BlockChanges},
        };

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn topic_address(byte: u8) -> Vec<u8> {
            ethabi::encode(&[AbiToken::Address(Address::from_slice(&address(byte)))])
        }

        fn v2_sync_block(
            number: u64,
            timestamp_secs: i64,
            pool: u8,
            reserve0: u64,
            reserve1: u64,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![vec![
                    28, 65, 30, 154, 150, 224, 113, 36, 28, 47, 33, 247, 114, 107, 23, 174, 137,
                    227, 202, 180, 199, 139, 229, 14, 6, 43, 3, 169, 255, 251, 186, 209,
                ]],
                data: ethabi::encode(&[
                    AbiToken::Uint(U256::from(reserve0)),
                    AbiToken::Uint(U256::from(reserve1)),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp { seconds: timestamp_secs, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xbb; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        fn v3_swap_block(
            number: u64,
            timestamp_secs: i64,
            pool: u8,
            sender: u8,
            recipient: u8,
            amount0: u64,
            amount1: u64,
            sqrt_price_x96: u64,
            liquidity: u64,
            tick: i32,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![
                    vec![
                        196, 32, 121, 249, 74, 99, 80, 215, 230, 35, 95, 41, 23, 73, 36, 249, 40,
                        204, 42, 200, 24, 235, 100, 254, 216, 0, 78, 17, 95, 188, 202, 103,
                    ],
                    topic_address(sender),
                    topic_address(recipient),
                ],
                data: ethabi::encode(&[
                    AbiToken::Int(amount0.into()),
                    AbiToken::Int(amount1.into()),
                    AbiToken::Uint(U256::from(sqrt_price_x96)),
                    AbiToken::Uint(U256::from(liquidity)),
                    AbiToken::Int(tick.into()),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp { seconds: timestamp_secs, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xde; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let v2_component_id = "0x4545454545454545454545454545454545454545";
        let v3_component_id = "0x4646464646464646464646464646464646464646";

        let v2_creation_block = v2_pair_created_block(63, 1_718_500_063, 0xf1, 0xa0, 0xc0, 0x45);
        let v2_creation_changes = run_v2_map_pools_created(
            "factory_address=0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1&protocol_type_name=uniswap_v2_pool"
                .to_string(),
            v2_creation_block.clone(),
        )
        .expect("combined V2 handler should emit pair-created changes");
        let v2_created_pool = v2_creation_changes.changes[0].component_changes[0].clone();
        let family_v2_creation_changes = substreams::BlockChanges::decode(
            run_map_uniswap_family_protocol_changes(
                v2_creation_changes.clone(),
                V2BlockChanges {
                    block: v2_creation_changes.block.clone(),
                    changes: vec![],
                    storage_changes: vec![],
                },
            )
            .expect("combined family handler should merge V2 created-pool output")
            .encode_to_vec()
            .as_slice(),
        )
        .expect("bridge V2 family block changes into indexer protobuf type");

        let v2_follow_up_block = v2_sync_block(64, 1_718_500_064, 0x45, 2_000, 3_000);
        let v2_pools_store =
            MockProtoStore::new(0).with_last(format!("Pool:{v2_component_id}"), v2_created_pool);
        let v2_follow_up_changes = run_v2_map_pool_events(
            format!("pools={v2_component_id}"),
            v2_follow_up_block.clone(),
            V2BlockChanges {
                block: Some(V2ProtoBlock {
                    hash: v2_follow_up_block.hash.clone(),
                    parent_hash: v2_follow_up_block
                        .header
                        .as_ref()
                        .map(|header| header.parent_hash.clone())
                        .unwrap_or_default(),
                    number: v2_follow_up_block.number,
                    ts: v2_follow_up_block
                        .header
                        .as_ref()
                        .and_then(|header| header.timestamp.as_ref())
                        .map(|timestamp| timestamp.seconds as u64)
                        .unwrap_or_default(),
                }),
                changes: vec![],
                storage_changes: vec![],
            },
            &v2_pools_store,
        )
        .expect("combined V2 handler should emit sync follow-up changes");
        let family_v2_follow_up_changes = substreams::BlockChanges::decode(
            run_map_uniswap_family_protocol_changes(
                v2_follow_up_changes.clone(),
                V2BlockChanges {
                    block: v2_follow_up_changes.block.clone(),
                    changes: vec![],
                    storage_changes: vec![],
                },
            )
            .expect("combined family handler should merge V2 follow-up output")
            .encode_to_vec()
            .as_slice(),
        )
        .expect("bridge V2 sync follow-up into indexer protobuf type");

        let v3_creation_block =
            v3_pool_created_block(65, 1_718_500_065, 0xf1, 0xa0, 0xc0, 500, 10, 0x46);
        let family_v3_creation_changes = substreams::BlockChanges::decode(
            build_uniswap_family_protocol_changes_from_v3_created_pools(
                "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
                v3_creation_block,
            )
            .encode_to_vec()
            .as_slice(),
        )
        .expect("bridge V3 family block changes into indexer protobuf type");

        let v3_follow_up_block =
            v3_swap_block(66, 1_718_500_066, 0x46, 0x01, 0x02, 10, 20, 123_456, 654_321, 7);
        let empty_v3_pools_store: StoreGetProto<FamilyV3Pool> = StoreGet::new(0);
        let v3_events = build_family_v3_pool_events(
            &format!("factory=0x{}&pool={v3_component_id}", hex::encode(address(0xf1)),),
            v3_follow_up_block.clone(),
            &empty_v3_pools_store,
        );
        let v3_follow_up_changes = build_family_v3_protocol_changes(
            v3_follow_up_block,
            BlockEntityChanges { block: None, changes: vec![] },
            v3_events,
            BlockBalanceDeltas { balance_deltas: vec![] },
            StoreDeltas { deltas: vec![] },
            FamilyV3TickDeltas { deltas: vec![] },
            StoreDeltas { deltas: vec![] },
            FamilyV3LiquidityChanges { changes: vec![] },
            StoreDeltas { deltas: vec![] },
        );
        let family_v3_follow_up_changes = substreams::BlockChanges::decode(
            build_uniswap_family_protocol_changes_from_v3_protocol_changes(v3_follow_up_changes)
                .encode_to_vec()
                .as_slice(),
        )
        .expect("bridge V3 swap follow-up into indexer protobuf type");

        vec![MockSubstreamsScript {
            responses: vec![
                scripted_session_response("trace-real-history-slice", 63),
                family_block_response_from_block_changes(
                    "cursor-real-history-slice",
                    family_v2_creation_changes,
                ),
                family_block_response_from_block_changes(
                    "cursor-real-history-slice",
                    family_v2_follow_up_changes,
                ),
                family_block_response_from_block_changes(
                    "cursor-real-history-slice",
                    family_v3_creation_changes,
                ),
                family_block_response_from_block_changes(
                    "cursor-real-history-slice",
                    family_v3_follow_up_changes,
                ),
                family_block_response("cursor-real-history-slice", 67, 1_718_500_067, vec![]),
            ],
            grpc_status: "0",
            grpc_message: None,
        }]
    }

    fn split_combined_family_real_history_slice_scripts_for_restart(
        use_fixture: bool,
    ) -> (
        tycho_indexer::substreams::mock::MockSubstreamsScript,
        tycho_indexer::substreams::mock::MockSubstreamsScript,
    ) {
        let mut split = if use_fixture {
            let fixture_path = combined_family_real_history_slice_fixture_path();
            assert!(fixture_path.exists(), "expected fixture at {}", fixture_path.display());
            tycho_indexer::substreams::mock::read_and_split_mock_substreams_fixture(
                &fixture_path,
                0,
                &[0..4, 4..6],
            )
            .expect("read and split committed history-slice fixture for restart")
        } else {
            let mut scripts = combined_family_real_history_slice_scripts();
            let script = scripts
                .pop()
                .expect("real history slice should produce one scripted session");
            tycho_indexer::substreams::mock::split_mock_substreams_script(&script, &[0..4, 4..6])
                .expect("split generated history-slice script for restart")
        };
        let first = split.remove(0);
        let mut second = split.remove(0);
        second
            .responses
            .insert(0, scripted_session_response("trace-real-history-slice-restart", 66));

        (first, second)
    }

    async fn assert_combined_family_real_history_slice_replay(use_fixture: bool) {
        use tycho_common::models::token::Token;
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, start_scripted_mock_substreams_from_fixture,
        };

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(move |_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let v2_component_id = "0x4545454545454545454545454545454545454545";
            let v3_component_id = "0x4646464646464646464646464646464646464646";

            let (captured, addr) = if use_fixture {
                let fixture_path = combined_family_real_history_slice_fixture_path();
                assert!(fixture_path.exists(), "expected fixture at {}", fixture_path.display());
                start_scripted_mock_substreams_from_fixture(&fixture_path)
                    .await
                    .expect("start fixture-backed mock substreams")
            } else {
                start_scripted_mock_substreams(combined_family_real_history_slice_scripts()).await
            };

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed V2/V3 history-slice tokens");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path =
                test_family_shared_spkg_path("combined-family-real-history-slice");

            let extractors = HashMap::from([
                (
                    "uniswap_v2".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v2".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        63,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v2-member.spkg".to_string(),
                        "v2_map_pool_events".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
                (
                    "uniswap_v3".to_string(),
                    ExtractorConfig::new(
                        "uniswap_v3".to_string(),
                        chain,
                        ImplementationType::Custom,
                        1000,
                        63,
                        None,
                        vec![ProtocolTypeConfig::new(
                            "uniswap_v3_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                        )],
                        "/tmp/missing-v3-member.spkg".to_string(),
                        "v3_map_protocol_changes".to_string(),
                        vec![],
                        0,
                        None,
                        None,
                        HashMap::new(),
                        None,
                    )
                    .with_family_runtime(Some(test_family_runtime_config(
                        shared_spkg_path.clone(),
                    ))),
                ),
            ]);
            let config = ExtractorConfigs::new(extractors);

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("family runner present");
            runner.run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared substreams request");
            }

            let v2_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read combined V2 component universe after history slice");
            assert!(
                v2_components
                    .entity
                    .iter()
                    .any(|component| component.id == v2_component_id),
                "expected V2 dynamic component from real history slice to be visible"
            );

            let v3_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read combined V3 component universe after history slice");
            assert!(
                v3_components
                    .entity
                    .iter()
                    .any(|component| component.id == v3_component_id),
                "expected V3 dynamic component from real history slice to be visible"
            );

            let v2_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[v2_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic V2 state after history slice");
            assert_eq!(v2_state.entity.len(), 1);
            assert_eq!(
                v2_state.entity[0]
                    .attributes
                    .get("reserve0"),
                Some(&Bytes::from(vec![0x07, 0xd0]))
            );
            assert_eq!(
                v2_state.entity[0]
                    .attributes
                    .get("reserve1"),
                Some(&Bytes::from(vec![0x0b, 0xb8]))
            );

            let v3_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[v3_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic V3 state after history slice");
            assert_eq!(v3_state.entity.len(), 1);
            assert_eq!(
                v3_state.entity[0]
                    .attributes
                    .get("tick"),
                Some(&Bytes::from(vec![0x07]))
            );

            let rpc_port = {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("bind temp rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) =
                ServicesBuilder::new(direct_gw.clone(), rpc.clone(), "test-api-key".to_string())
                    .bind("127.0.0.1")
                    .port(rpc_port)
                    .protocol_systems(protocol_systems.clone())
                    .run()
                    .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();

            let assert_component_visible_through_rpc =
                |protocol_system: &'static str, component_id: &'static str| {
                    let client = client.clone();
                    async move {
                        let mut rpc_body = None;
                        for _ in 0..100 {
                            let response = match client
                            .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                            .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                                protocol_system,
                                vec![component_id.to_string()],
                                dto::Chain::Ethereum,
                            ))
                            .send()
                            .await
                        {
                            Ok(response) => response,
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                        };
                        assert!(
                            response.status().is_success(),
                            "protocol_components rpc should succeed for {protocol_system}, got {}",
                            response.status()
                        );
                        let body: dto::ProtocolComponentRequestResponse = response
                            .json()
                            .await
                            .expect("decode protocol components rpc response");
                        if body.protocol_components.len() == 1 {
                            rpc_body = Some(body);
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }

                    let rpc_body = rpc_body.unwrap_or_else(|| {
                        panic!(
                            "{protocol_system} component `{component_id}` never became queryable through rpc"
                        )
                    });
                    assert_eq!(rpc_body.protocol_components.len(), 1);
                    assert_eq!(rpc_body.protocol_components[0].id, component_id);
                    assert_eq!(rpc_body.protocol_components[0].protocol_system, protocol_system);
                    }
                };

            let assert_state_visible_through_rpc =
                |protocol_system: &'static str,
                 component_id: &'static str,
                 expected_attribute: &'static str,
                 expected_value: Bytes| {
                    let client = client.clone();
                    async move {
                        let mut state_body = None;
                        for _ in 0..100 {
                            let response = match client
                            .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                            .json(&dto::ProtocolStateRequestBody {
                                protocol_ids: Some(vec![component_id.to_string()]),
                                protocol_system: protocol_system.to_string(),
                                chain: dto::Chain::Ethereum,
                                include_balances: false,
                                version: dto::VersionParam::default(),
                                pagination: dto::PaginationParams::default(),
                            })
                            .send()
                            .await
                        {
                            Ok(response) => response,
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                        };
                        assert!(
                            response.status().is_success(),
                            "protocol_state rpc should succeed for {protocol_system}, got {}",
                            response.status()
                        );
                        let body: dto::ProtocolStateRequestResponse = response
                            .json()
                            .await
                            .expect("decode protocol state rpc response");
                        if body.states.len() == 1 {
                            state_body = Some(body);
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }

                    let state_body = state_body.unwrap_or_else(|| {
                        panic!(
                            "{protocol_system} state for `{component_id}` never became queryable through rpc"
                        )
                    });
                    assert_eq!(state_body.states.len(), 1);
                    assert_eq!(state_body.states[0].component_id, component_id);
                    assert_eq!(
                        state_body.states[0]
                            .attributes
                            .get(expected_attribute),
                        Some(&expected_value)
                    );
                    }
                };

            assert_component_visible_through_rpc("uniswap_v2", v2_component_id).await;
            assert_component_visible_through_rpc("uniswap_v3", v3_component_id).await;
            assert_state_visible_through_rpc(
                "uniswap_v2",
                v2_component_id,
                "reserve0",
                Bytes::from(vec![0x07, 0xd0]),
            )
            .await;
            assert_state_visible_through_rpc(
                "uniswap_v3",
                v3_component_id,
                "tick",
                Bytes::from(vec![0x07]),
            )
            .await;

            server_handle.stop(true).await;
            let _ = server_task.await;

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[test]
    fn combined_family_real_history_slice_fixture_matches_generated_script() {
        use prost::Message;
        use tycho_indexer::pb::sf::substreams::rpc::v2::{
            response::Message as ResponseMessage, Response,
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        fn normalize_attributes(attributes: &mut [substreams::Attribute]) {
            attributes.sort_by(|left, right| {
                left.name
                    .cmp(&right.name)
                    .then_with(|| left.value.cmp(&right.value))
                    .then_with(|| left.change.cmp(&right.change))
            });
        }

        fn normalize_block_changes(changes: &mut substreams::BlockChanges) {
            for tx_changes in &mut changes.changes {
                tx_changes
                    .contract_changes
                    .sort_by(|left, right| left.address.cmp(&right.address));
                for contract_change in &mut tx_changes.contract_changes {
                    contract_change
                        .slots
                        .sort_by(|left, right| left.slot.cmp(&right.slot));
                    contract_change
                        .token_balances
                        .sort_by(|left, right| left.token.cmp(&right.token));
                }

                tx_changes
                    .entity_changes
                    .sort_by(|left, right| {
                        left.component_id
                            .cmp(&right.component_id)
                    });
                for entity_change in &mut tx_changes.entity_changes {
                    normalize_attributes(&mut entity_change.attributes);
                }

                tx_changes
                    .component_changes
                    .sort_by(|left, right| left.id.cmp(&right.id));
                for component_change in &mut tx_changes.component_changes {
                    component_change.tokens.sort();
                    component_change.contracts.sort();
                    normalize_attributes(&mut component_change.static_att);
                }

                tx_changes
                    .balance_changes
                    .sort_by(|left, right| {
                        left.component_id
                            .cmp(&right.component_id)
                            .then_with(|| left.token.cmp(&right.token))
                            .then_with(|| left.balance.cmp(&right.balance))
                    });

                tx_changes
                    .entrypoints
                    .sort_by(|left, right| {
                        left.id
                            .cmp(&right.id)
                            .then_with(|| {
                                left.component_id
                                    .cmp(&right.component_id)
                            })
                            .then_with(|| left.signature.cmp(&right.signature))
                            .then_with(|| left.target.cmp(&right.target))
                    });
                tx_changes
                    .entrypoint_params
                    .sort_by(|left, right| {
                        left.entrypoint_id
                            .cmp(&right.entrypoint_id)
                            .then_with(|| {
                                left.component_id
                                    .cmp(&right.component_id)
                            })
                    });
            }

            changes.changes.sort_by(|left, right| {
                left.tx
                    .as_ref()
                    .map(|tx| (&tx.hash, tx.index))
                    .cmp(
                        &right
                            .tx
                            .as_ref()
                            .map(|tx| (&tx.hash, tx.index)),
                    )
            });

            for tx_storage_changes in &mut changes.storage_changes {
                tx_storage_changes
                    .storage_changes
                    .sort_by(|left, right| left.address.cmp(&right.address));
                for storage_change in &mut tx_storage_changes.storage_changes {
                    storage_change
                        .slots
                        .sort_by(|left, right| left.slot.cmp(&right.slot));
                }
            }
            changes
                .storage_changes
                .sort_by(|left, right| {
                    left.tx
                        .as_ref()
                        .map(|tx| (&tx.hash, tx.index))
                        .cmp(
                            &right
                                .tx
                                .as_ref()
                                .map(|tx| (&tx.hash, tx.index)),
                        )
                });
        }

        let fixture_path = combined_family_real_history_slice_fixture_path();
        let fixture = tycho_indexer::substreams::mock::read_mock_substreams_fixture(&fixture_path)
            .expect("read committed history-slice fixture");
        let generated = combined_family_real_history_slice_scripts();
        let start_block = 25_384_601_u64;

        assert_eq!(fixture.len(), 1, "expected one recorded shared-family session");
        assert_eq!(fixture[0].grpc_status, "0");
        assert_eq!(fixture[0].grpc_message, None);
        assert!(
            fixture[0].responses.len() > generated[0].responses.len(),
            "live captured fixture should remain richer than the in-repo synthetic smoke script"
        );

        let first = fixture[0]
            .responses
            .first()
            .and_then(|response| response.message.as_ref())
            .expect("fixture session response present");
        match first {
            ResponseMessage::Session(session) => {
                assert_eq!(session.resolved_start_block, start_block);
            }
            other => panic!("expected session response first, got {other:?}"),
        }

        let block_heights = fixture[0]
            .responses
            .iter()
            .skip(1)
            .filter_map(|response| match response.message.as_ref() {
                Some(ResponseMessage::BlockScopedData(block)) => {
                    assert!(
                        block.final_block_height >= start_block,
                        "fixture block should not rewind before configured start block"
                    );
                    Some(block.final_block_height)
                }
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            !block_heights.is_empty(),
            "fixture should include block-scoped responses after the session init"
        );
        assert!(
            block_heights.contains(&start_block),
            "fixture should include the configured start block"
        );
        assert!(
            block_heights.iter().any(|height| *height > start_block),
            "fixture should include follow-up live blocks beyond the start block"
        );
    }

    #[test]
    #[ignore = "utility for refreshing the committed combined-family history-slice fixture"]
    fn regenerate_combined_family_real_history_slice_fixture() {
        let fixture_path = combined_family_real_history_slice_fixture_path();
        std::fs::create_dir_all(
            fixture_path
                .parent()
                .expect("fixture parent directory"),
        )
        .expect("create fixture directory");
        tycho_indexer::substreams::mock::write_mock_substreams_fixture(
            &fixture_path,
            &combined_family_real_history_slice_scripts(),
        )
        .expect("write history-slice fixture");
    }

    #[tokio::test]
    async fn combined_family_runner_replays_real_v2_and_v3_history_slice_in_one_shared_session() {
        assert_combined_family_real_history_slice_replay(false).await;
    }

    #[tokio::test]
    async fn combined_family_runner_replays_fixture_backed_v2_and_v3_history_slice_in_one_shared_session(
    ) {
        assert_combined_family_real_history_slice_replay(true).await;
    }

    async fn assert_combined_family_real_history_slice_restart(use_fixture: bool) {
        use tycho_common::models::{token::Token, FinancialType, ProtocolType};
        use tycho_indexer::substreams::mock::start_scripted_mock_substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(move |_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let v2_component_id = "0x4545454545454545454545454545454545454545";
            let v3_component_id = "0x4646464646464646464646464646464646464646";
            let (first_script, second_script) =
                split_combined_family_real_history_slice_scripts_for_restart(use_fixture);

            let (captured_first, addr_first) =
                start_scripted_mock_substreams(vec![first_script]).await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed restart history-slice tokens");
            direct_gw
                .add_protocol_types(&[
                    ProtocolType::new(
                        "uniswap_v2_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                    ProtocolType::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                ])
                .await
                .expect("seed restart history-slice protocol types");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path =
                test_family_shared_spkg_path("combined-family-real-history-slice-restart");

            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config(
                "tycho-indexer-real-history-slice-restart-family-defaults",
                &unique,
                &shared_spkg_path,
                63,
                None,
            );
            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 restart history-slice config path"),
            )
            .expect("load restart history-slice family-default config");

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr_first}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build first restart history-slice extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);
            runners.pop().expect("first family runner").run().await.unwrap().unwrap();

            {
                let requests = captured_first.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected one shared request on first run");
                assert_eq!(requests[0].start_block_num, 63);
                assert!(requests[0].start_cursor.is_empty());
            }

            let after_first_v2_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[v2_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read V2 state before restart");
            assert_eq!(after_first_v2_state.entity.len(), 1);
            assert_eq!(
                after_first_v2_state.entity[0].attributes.get("reserve0"),
                Some(&Bytes::from(vec![0x07, 0xd0]))
            );

            let after_first_v3_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read V3 component universe before restart");
            assert!(
                after_first_v3_components
                    .entity
                    .iter()
                    .any(|component| component.id == v3_component_id),
                "V3 component should already exist before restart"
            );

            let (captured_second, addr_second) =
                start_scripted_mock_substreams(vec![second_script]).await;
            let (resumed_cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create resumed Gateway");
            let resumed_config_path = test_family_defaults_config(
                "tycho-indexer-real-history-slice-restart-family-defaults",
                &format!("{unique}-resumed"),
                &shared_spkg_path,
                63,
                None,
            );
            let resumed_config = ExtractorConfigs::from_yaml(
                resumed_config_path
                    .to_str()
                    .expect("utf8 resumed restart history-slice config path"),
            )
            .expect("load resumed restart history-slice family-default config");
            let (mut resumed_runners, resumed_handles) = build_all_extractors(
                &resumed_config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr_second}"),
                None,
                "",
                &resumed_cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build resumed restart history-slice extractors");

            assert_eq!(resumed_runners.len(), 1);
            assert_eq!(resumed_handles.len(), 2);
            resumed_runners
                .pop()
                .expect("resumed family runner")
                .run()
                .await
                .unwrap()
                .unwrap();

            {
                let requests = captured_second.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected one shared request after restart");
                assert_eq!(requests[0].start_block_num, 66);
                assert_eq!(
                    requests[0].start_cursor, "cursor-real-history-slice@65",
                    "restart should resume from the persisted history-slice cursor"
                );
            }

            let after_restart_v2_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read V2 components after restart");
            assert!(
                after_restart_v2_components
                    .entity
                    .iter()
                    .any(|component| component.id == v2_component_id),
                "V2 component should remain queryable after restart"
            );
            let after_restart_v3_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[v3_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read V3 state after restart follow-up");
            assert_eq!(after_restart_v3_state.entity.len(), 1);
            assert_eq!(
                after_restart_v3_state.entity[0].attributes.get("tick"),
                Some(&Bytes::from(vec![0x07]))
            );

            let rpc_port = {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("bind temp rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) =
                ServicesBuilder::new(direct_gw.clone(), rpc.clone(), "test-api-key".to_string())
                    .bind("127.0.0.1")
                    .port(rpc_port)
                    .protocol_systems(protocol_systems.clone())
                    .run()
                    .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            for (protocol_system, component_id) in
                [("uniswap_v2", v2_component_id), ("uniswap_v3", v3_component_id)]
            {
                let mut rpc_components = None;
                for _ in 0..100 {
                    let response = match client
                        .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                        .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                            protocol_system,
                            vec![component_id.to_string()],
                            dto::Chain::Ethereum,
                        ))
                        .send()
                        .await
                    {
                        Ok(response) => response,
                        Err(_) => {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    assert!(
                        response.status().is_success(),
                        "protocol_components rpc should succeed for {protocol_system}, got {}",
                        response.status()
                    );
                    let body: dto::ProtocolComponentRequestResponse = response
                        .json()
                        .await
                        .expect("decode protocol components rpc response");
                    if body.protocol_components.len() == 1 {
                        rpc_components = Some(body);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                let rpc_components = rpc_components.unwrap_or_else(|| {
                    panic!(
                        "{protocol_system} component `{component_id}` never became queryable through rpc after restart"
                    )
                });
                assert_eq!(rpc_components.protocol_components[0].id, component_id);
            }

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
            let _ = std::fs::remove_file(&resumed_config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_restart_replays_real_history_slice_from_persisted_cursor() {
        assert_combined_family_real_history_slice_restart(false).await;
    }

    #[tokio::test]
    async fn combined_family_runner_restart_replays_fixture_backed_real_history_slice_from_persisted_cursor(
    ) {
        assert_combined_family_real_history_slice_restart(true).await;
    }

    #[tokio::test]
    async fn combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission(
    ) {
        use ::substreams::{
            pb::substreams::StoreDeltas,
            store::{StoreGet, StoreGetProto},
        };
        use ethabi::{
            ethereum_types::{Address, U256},
            Token as AbiToken,
        };
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v3_pool_events, build_family_v3_protocol_changes,
            build_uniswap_family_protocol_changes_from_v3_created_pools,
            build_uniswap_family_protocol_changes_from_v3_protocol_changes,
            FamilyV3LiquidityChanges, FamilyV3Pool, FamilyV3TickDeltas,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_common::models::{token::Token, FinancialType, ProtocolType};
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::Response,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::models::{BlockBalanceDeltas, BlockEntityChanges};

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn topic_address(byte: u8) -> Vec<u8> {
            ethabi::encode(&[AbiToken::Address(Address::from_slice(&address(byte)))])
        }

        fn topic_uint24(value: u32) -> Vec<u8> {
            ethabi::encode(&[AbiToken::Uint(U256::from(value))])
        }

        fn v3_pool_created_block(
            number: u64,
            factory: u8,
            token0: u8,
            token1: u8,
            fee: u32,
            tick_spacing: i32,
            pool: u8,
        ) -> EthBlock {
            let data = ethabi::encode(&[
                AbiToken::Int(tick_spacing.into()),
                AbiToken::Address(Address::from_slice(&address(pool))),
            ]);
            let log = EthLog {
                address: address(factory),
                topics: vec![
                    vec![
                        120, 60, 202, 28, 4, 18, 221, 13, 105, 94, 120, 69, 104, 201, 109, 162,
                        233, 194, 47, 249, 137, 53, 122, 46, 139, 29, 155, 43, 78, 107, 113, 24,
                    ],
                    topic_address(token0),
                    topic_address(token1),
                    topic_uint24(fee),
                ],
                data,
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp {
                        seconds: (1_718_700_000 + number) as i64,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 0,
                    hash: vec![0xef; 32],
                    from: vec![0x01; 20],
                    to: address(factory),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        fn v3_swap_block(
            number: u64,
            pool: u8,
            sender: u8,
            recipient: u8,
            amount0: u64,
            amount1: u64,
            sqrt_price_x96: u64,
            liquidity: u64,
            tick: i32,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![
                    vec![
                        196, 32, 121, 249, 74, 99, 80, 215, 230, 35, 95, 41, 23, 73, 36, 249, 40,
                        204, 42, 200, 24, 235, 100, 254, 216, 0, 78, 17, 95, 188, 202, 103,
                    ],
                    topic_address(sender),
                    topic_address(recipient),
                ],
                data: ethabi::encode(&[
                    AbiToken::Int(amount0.into()),
                    AbiToken::Int(amount1.into()),
                    AbiToken::Uint(U256::from(sqrt_price_x96)),
                    AbiToken::Uint(U256::from(liquidity)),
                    AbiToken::Int(tick.into()),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp {
                        seconds: (1_718_700_000 + number) as i64,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xf0; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let dynamic_component_id = "0x4848484848484848484848484848484848484848";

            let v3_creation_block = v3_pool_created_block(63, 0xf1, 0xa0, 0xc0, 500, 10, 0x48);
            let family_creation_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_created_pools(
                    "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
                    v3_creation_block,
                )
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge V3 restart family block changes into indexer protobuf type");

            let (captured_first, addr_first) =
                start_scripted_mock_substreams(vec![MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-v3-restart", 63),
                        crate::testing::family_block_response_from_block_changes(
                            "cursor-v3-restart",
                            family_creation_changes,
                        ),
                    ],
                    grpc_status: "0",
                    grpc_message: None,
                }])
                .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed V3 restart tokens");
            direct_gw
                .add_protocol_types(&[
                    ProtocolType::new(
                        "uniswap_v2_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                    ProtocolType::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                ])
                .await
                .expect("seed V3 restart protocol types");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-v3-restart");

            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config(
                "tycho-indexer-v3-restart-family-defaults",
                &unique,
                &shared_spkg_path,
                63,
                None,
            );
            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 v3 restart family-default config path"),
            )
            .expect("load first v3 restart family-default config");

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr_first}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build first combined V3 restart extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);
            runners.pop().expect("first family runner").run().await.unwrap().unwrap();

            {
                let requests = captured_first.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared request on first V3 run");
                assert_eq!(requests[0].start_block_num, 63);
                assert!(requests[0].start_cursor.is_empty());
            }

            let after_first_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read V3 component universe after first run");
            assert!(
                after_first_components
                    .entity
                    .iter()
                    .any(|component| component.id == dynamic_component_id),
                "V3 dynamic component should persist before restart"
            );
            let v3_follow_up_block =
                v3_swap_block(64, 0x48, 0x01, 0x02, 15, 25, 456_789, 777_777, 9);
            let empty_pools_store: StoreGetProto<FamilyV3Pool> = StoreGet::new(0);
            let v3_events = build_family_v3_pool_events(
                &format!(
                    "factory=0x{}&pool={dynamic_component_id}",
                    hex::encode(address(0xf1)),
                ),
                v3_follow_up_block.clone(),
                &empty_pools_store,
            );
            let v3_follow_up_changes = build_family_v3_protocol_changes(
                v3_follow_up_block.clone(),
                BlockEntityChanges { block: None, changes: vec![] },
                v3_events,
                BlockBalanceDeltas {
                    balance_deltas: vec![],
                },
                StoreDeltas { deltas: vec![] },
                FamilyV3TickDeltas { deltas: vec![] },
                StoreDeltas { deltas: vec![] },
                FamilyV3LiquidityChanges { changes: vec![] },
                StoreDeltas { deltas: vec![] },
            );
            let family_follow_up_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_protocol_changes(v3_follow_up_changes)
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge V3 restart swap follow-up into indexer protobuf type");

            let (captured_second, addr_second) =
                start_scripted_mock_substreams(vec![MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-v3-restart", 64),
                        crate::testing::family_block_response_from_block_changes(
                            "cursor-v3-restart",
                            family_follow_up_changes,
                        ),
                    ],
                    grpc_status: "0",
                    grpc_message: None,
                }])
                .await;

            let resumed_cached_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create resumed Gateway")
                .0;

            let resumed_config_path = test_family_defaults_config(
                "tycho-indexer-v3-restart-family-defaults",
                &format!("{unique}-resumed"),
                &shared_spkg_path,
                63,
                None,
            );
            let resumed_config = ExtractorConfigs::from_yaml(
                resumed_config_path
                    .to_str()
                    .expect("utf8 resumed v3 restart family-default config path"),
            )
            .expect("load resumed v3 restart family-default config");
            let (mut resumed_runners, resumed_handles) = build_all_extractors(
                &resumed_config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr_second}"),
                None,
                "",
                &resumed_cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build resumed combined V3 restart extractors");

            assert_eq!(resumed_runners.len(), 1);
            assert_eq!(resumed_handles.len(), 2);
            resumed_runners
                .pop()
                .expect("resumed family runner")
                .run()
                .await
                .unwrap()
                .unwrap();

            {
                let requests = captured_second.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected one shared request after V3 restart");
                assert_eq!(
                    requests[0].start_block_num, 64,
                    "fresh restart should resume from block after last committed V3 dynamic-admission block"
                );
                assert_eq!(requests[0].start_cursor, "cursor-v3-restart@63");
            }

            let after_restart_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read V3 component universe after restart");
            assert!(
                after_restart_components
                    .entity
                    .iter()
                    .any(|component| component.id == dynamic_component_id),
                "V3 dynamic component should remain queryable after restart resume"
            );
            let dynamic_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[dynamic_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read V3 dynamic component state after restart follow-up");
            assert_eq!(dynamic_state.entity.len(), 1);
            assert_eq!(
                dynamic_state.entity[0].attributes.get("tick"),
                Some(&Bytes::from(vec![0x09]))
            );

            let rpc_port = {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")
                    .expect("bind temp rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) = ServicesBuilder::new(
                direct_gw.clone(),
                rpc.clone(),
                "test-api-key".to_string(),
            )
            .bind("127.0.0.1")
            .port(rpc_port)
            .protocol_systems(protocol_systems.clone())
            .run()
            .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let mut rpc_components = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                    .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                        "uniswap_v3",
                        vec![dynamic_component_id.to_string()],
                        dto::Chain::Ethereum,
                    ))
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_components rpc should succeed, got {}",
                    response.status()
                );
                let body: dto::ProtocolComponentRequestResponse = response
                    .json()
                    .await
                    .expect("decode protocol components rpc response");
                if body.protocol_components.len() == 1 {
                    rpc_components = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let rpc_components = rpc_components.unwrap_or_else(|| {
                panic!("V3 dynamic component never became queryable through rpc after restart")
            });
            assert_eq!(rpc_components.protocol_components.len(), 1);
            assert_eq!(rpc_components.protocol_components[0].id, dynamic_component_id);
            assert_eq!(
                rpc_components.protocol_components[0].protocol_system,
                "uniswap_v3"
            );

            let mut rpc_state = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                    .json(&dto::ProtocolStateRequestBody {
                        protocol_ids: Some(vec![dynamic_component_id.to_string()]),
                        protocol_system: "uniswap_v3".to_string(),
                        chain: dto::Chain::Ethereum,
                        include_balances: false,
                        version: dto::VersionParam::default(),
                        pagination: dto::PaginationParams::default(),
                    })
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_state rpc should succeed, got {}",
                    response.status()
                );
                let body: dto::ProtocolStateRequestResponse = response
                    .json()
                    .await
                    .expect("decode protocol state rpc response");
                if body.states.len() == 1 {
                    rpc_state = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let rpc_state = rpc_state.unwrap_or_else(|| {
                panic!("V3 dynamic state never became queryable through rpc after restart")
            });
            assert_eq!(rpc_state.states.len(), 1);
            assert_eq!(rpc_state.states[0].component_id, dynamic_component_id);
            assert_eq!(
                rpc_state.states[0].attributes.get("tick"),
                Some(&Bytes::from(vec![0x09]))
            );

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
            let _ = std::fs::remove_file(&resumed_config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_reconnect_applies_v3_follow_up_state_after_dynamic_component_admission(
    ) {
        use ::substreams::{
            pb::substreams::StoreDeltas,
            store::{StoreGet, StoreGetProto},
        };
        use ethabi::{
            ethereum_types::{Address, U256},
            Token as AbiToken,
        };
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v3_pool_events, build_family_v3_protocol_changes,
            build_uniswap_family_protocol_changes_from_v3_created_pools,
            build_uniswap_family_protocol_changes_from_v3_protocol_changes,
            FamilyV3LiquidityChanges, FamilyV3Pool, FamilyV3TickDeltas,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_common::models::{token::Token, FinancialType, ProtocolType};
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{BlockScopedData, MapModuleOutput, Response},
            pb::sf::substreams::v1::Clock,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::models::{BlockBalanceDeltas, BlockEntityChanges};

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn topic_address(byte: u8) -> Vec<u8> {
            ethabi::encode(&[AbiToken::Address(Address::from_slice(&address(byte)))])
        }

        fn topic_uint24(value: u32) -> Vec<u8> {
            ethabi::encode(&[AbiToken::Uint(U256::from(value))])
        }

        fn v3_pool_created_block(
            number: u64,
            factory: u8,
            token0: u8,
            token1: u8,
            fee: u32,
            tick_spacing: i32,
            pool: u8,
        ) -> EthBlock {
            let data = ethabi::encode(&[
                AbiToken::Int(tick_spacing.into()),
                AbiToken::Address(Address::from_slice(&address(pool))),
            ]);
            let log = EthLog {
                address: address(factory),
                topics: vec![
                    vec![
                        120, 60, 202, 28, 4, 18, 221, 13, 105, 94, 120, 69, 104, 201, 109, 162,
                        233, 194, 47, 249, 137, 53, 122, 46, 139, 29, 155, 43, 78, 107, 113, 24,
                    ],
                    topic_address(token0),
                    topic_address(token1),
                    topic_uint24(fee),
                ],
                data,
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp {
                        seconds: (1_718_800_000 + number) as i64,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 0,
                    hash: vec![0xef; 32],
                    from: vec![0x01; 20],
                    to: address(factory),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        fn v3_swap_block(
            number: u64,
            pool: u8,
            sender: u8,
            recipient: u8,
            amount0: u64,
            amount1: u64,
            sqrt_price_x96: u64,
            liquidity: u64,
            tick: i32,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![
                    vec![
                        196, 32, 121, 249, 74, 99, 80, 215, 230, 35, 95, 41, 23, 73, 36, 249, 40,
                        204, 42, 200, 24, 235, 100, 254, 216, 0, 78, 17, 95, 188, 202, 103,
                    ],
                    topic_address(sender),
                    topic_address(recipient),
                ],
                data: ethabi::encode(&[
                    AbiToken::Int(amount0.into()),
                    AbiToken::Int(amount1.into()),
                    AbiToken::Uint(U256::from(sqrt_price_x96)),
                    AbiToken::Uint(U256::from(liquidity)),
                    AbiToken::Int(tick.into()),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp {
                        seconds: (1_718_800_000 + number) as i64,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xf0; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let dynamic_component_id = "0x4949494949494949494949494949494949494949";

            let v3_creation_block = v3_pool_created_block(73, 0xf1, 0xa0, 0xc0, 500, 10, 0x49);
            let family_creation_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_created_pools(
                    "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
                    v3_creation_block,
                )
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge reconnect V3 family block changes into indexer protobuf type");

            let v3_follow_up_block = v3_swap_block(74, 0x49, 0x01, 0x02, 21, 34, 654_321, 888_888, 11);
            let empty_pools_store: StoreGetProto<FamilyV3Pool> = StoreGet::new(0);
            let v3_events = build_family_v3_pool_events(
                &format!(
                    "factory=0x{}&pool={dynamic_component_id}",
                    hex::encode(address(0xf1)),
                ),
                v3_follow_up_block.clone(),
                &empty_pools_store,
            );
            let v3_follow_up_changes = build_family_v3_protocol_changes(
                v3_follow_up_block.clone(),
                BlockEntityChanges { block: None, changes: vec![] },
                v3_events,
                BlockBalanceDeltas {
                    balance_deltas: vec![],
                },
                StoreDeltas { deltas: vec![] },
                FamilyV3TickDeltas { deltas: vec![] },
                StoreDeltas { deltas: vec![] },
                FamilyV3LiquidityChanges { changes: vec![] },
                StoreDeltas { deltas: vec![] },
            );
            let family_follow_up_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v3_protocol_changes(v3_follow_up_changes)
                    .encode_to_vec()
                    .as_slice(),
            )
            .expect("bridge reconnect V3 swap follow-up into indexer protobuf type");

            let (captured, addr) = start_scripted_mock_substreams(vec![
                MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-v3-reconnect", 73),
                        crate::testing::family_block_response_from_block_changes(
                            "cursor-v3-reconnect",
                            family_creation_changes,
                        ),
                    ],
                    grpc_status: "13",
                    grpc_message: Some("forced-reconnect"),
                },
                MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-v3-reconnect", 74),
                        crate::testing::family_block_response_from_block_changes(
                            "cursor-v3-reconnect",
                            family_follow_up_changes,
                        ),
                    ],
                    grpc_status: "0",
                    grpc_message: None,
                },
            ])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed reconnect V3 tokens");
            direct_gw
                .add_protocol_types(&[
                    ProtocolType::new(
                        "uniswap_v2_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                    ProtocolType::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                ])
                .await
                .expect("seed reconnect V3 protocol types");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-v3-reconnect");

            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config(
                "tycho-indexer-v3-reconnect-family-defaults",
                &unique,
                &shared_spkg_path,
                73,
                None,
            );
            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 reconnect family-default config path"),
            )
            .expect("load reconnect family-default config");

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build reconnect combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);
            runners.pop().expect("reconnect family runner").run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 2, "expected initial request and hot reconnect");
                assert_eq!(requests[0].start_block_num, 73);
                assert!(requests[0].start_cursor.is_empty());
                assert_eq!(
                    requests[1].start_cursor,
                    "cursor-v3-reconnect@73",
                    "hot reconnect should resume from the cursor emitted after the dynamic-admission block"
                );
            }

            let components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read V3 component universe after reconnect");
            assert!(
                components
                    .entity
                    .iter()
                    .any(|component| component.id == dynamic_component_id),
                "V3 dynamic component should remain queryable after reconnect"
            );

            let dynamic_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[dynamic_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read V3 dynamic component state after reconnect follow-up");
            assert_eq!(dynamic_state.entity.len(), 1);
            assert_eq!(
                dynamic_state.entity[0].attributes.get("tick"),
                Some(&Bytes::from(vec![0x0b]))
            );

            let rpc_port = {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")
                    .expect("bind temp reconnect rpc port");
                let port = listener
                    .local_addr()
                    .expect("temp reconnect rpc local addr")
                    .port();
                drop(listener);
                port
            };
            let (server_handle, server_task) = ServicesBuilder::new(
                direct_gw.clone(),
                rpc.clone(),
                "test-api-key".to_string(),
            )
            .bind("127.0.0.1")
            .port(rpc_port)
            .protocol_systems(protocol_systems.clone())
            .run()
            .expect("start reconnect rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let mut rpc_components = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                    .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                        "uniswap_v3",
                        vec![dynamic_component_id.to_string()],
                        dto::Chain::Ethereum,
                    ))
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_components rpc should succeed after reconnect, got {}",
                    response.status()
                );
                let body: dto::ProtocolComponentRequestResponse = response
                    .json()
                    .await
                    .expect("decode reconnect protocol components rpc response");
                if body.protocol_components.len() == 1 {
                    rpc_components = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let rpc_components = rpc_components.unwrap_or_else(|| {
                panic!("V3 dynamic component never became queryable through rpc after reconnect")
            });
            assert_eq!(rpc_components.protocol_components.len(), 1);
            assert_eq!(rpc_components.protocol_components[0].id, dynamic_component_id);
            assert_eq!(
                rpc_components.protocol_components[0].protocol_system,
                "uniswap_v3"
            );

            let mut rpc_state = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                    .json(&dto::ProtocolStateRequestBody {
                        protocol_ids: Some(vec![dynamic_component_id.to_string()]),
                        protocol_system: "uniswap_v3".to_string(),
                        chain: dto::Chain::Ethereum,
                        include_balances: false,
                        version: dto::VersionParam::default(),
                        pagination: dto::PaginationParams::default(),
                    })
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_state rpc should succeed after reconnect, got {}",
                    response.status()
                );
                let body: dto::ProtocolStateRequestResponse = response
                    .json()
                    .await
                    .expect("decode reconnect protocol state rpc response");
                if body.states.len() == 1 {
                    rpc_state = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let rpc_state = rpc_state.unwrap_or_else(|| {
                panic!("V3 dynamic state never became queryable through rpc after reconnect")
            });
            assert_eq!(rpc_state.states.len(), 1);
            assert_eq!(rpc_state.states[0].component_id, dynamic_component_id);
            assert_eq!(
                rpc_state.states[0].attributes.get("tick"),
                Some(&Bytes::from(vec![0x0b]))
            );

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_reconnect_applies_v2_follow_up_state_after_dynamic_component_admission(
    ) {
        use ::substreams::store::StoreGet;
        use ethabi::{ethereum_types::U256, Token as AbiToken};
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v2_pool_created_block_changes, build_family_v2_pool_event_block_changes,
            build_uniswap_family_protocol_changes_from_v2, parse_family_v2_pool_created_params,
        };
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_common::models::{token::Token, FinancialType, ProtocolType};
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::pb::tycho::evm::v1::{
            Block as V2ProtoBlock, BlockChanges as V2BlockChanges,
        };

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn v2_sync_block(
            number: u64,
            timestamp_secs: i64,
            pool: u8,
            reserve0: u64,
            reserve1: u64,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![vec![
                    28, 65, 30, 154, 150, 224, 113, 36, 28, 47, 33, 247, 114, 107, 23, 174, 137,
                    227, 202, 180, 199, 139, 229, 14, 6, 43, 3, 169, 255, 251, 186, 209,
                ]],
                data: ethabi::encode(&[
                    AbiToken::Uint(U256::from(reserve0)),
                    AbiToken::Uint(U256::from(reserve1)),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp { seconds: timestamp_secs, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xbb; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let dynamic_component_id = "0x4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a";

            let v2_creation_block = v2_pair_created_block(83, 1_718_900_083, 0xf1, 0xa0, 0xc0, 0x4a);
            let v2_creation_changes = build_family_v2_pool_created_block_changes(
                &v2_creation_block,
                &parse_family_v2_pool_created_params(
                    "factory_address=0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1&protocol_type_name=uniswap_v2_pool",
                ),
            );
            let created_pool = v2_creation_changes.changes[0].component_changes[0].clone();
            let family_creation_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v2(v2_creation_changes)
                    .encode_to_vec()
                    .as_slice(),
            )
            .expect("bridge reconnect v2 creation changes into indexer protobuf type");

            let v2_follow_up_block = v2_sync_block(84, 1_718_900_084, 0x4a, 5_000, 6_000);
            let pools_store = MockProtoStore::new(0).with_last(
                format!("Pool:{dynamic_component_id}"),
                created_pool,
            );
            let v2_follow_up_changes = build_family_v2_pool_event_block_changes(
                &format!("pools={dynamic_component_id}"),
                &v2_follow_up_block,
                V2BlockChanges {
                    block: Some(V2ProtoBlock {
                        hash: v2_follow_up_block.hash.clone(),
                        parent_hash: v2_follow_up_block
                            .header
                            .as_ref()
                            .map(|header| header.parent_hash.clone())
                            .unwrap_or_default(),
                        number: v2_follow_up_block.number,
                        ts: v2_follow_up_block
                            .header
                            .as_ref()
                            .and_then(|header| header.timestamp.as_ref())
                            .map(|timestamp| timestamp.seconds as u64)
                            .unwrap_or_default(),
                    }),
                    changes: vec![],
                    storage_changes: vec![],
                },
                &pools_store,
            );
            let family_follow_up_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v2(v2_follow_up_changes)
                    .encode_to_vec()
                    .as_slice(),
            )
            .expect("bridge reconnect v2 follow-up changes into indexer protobuf type");

            let (captured, addr) = start_scripted_mock_substreams(vec![
                MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-v2-reconnect", 83),
                        family_block_response_from_block_changes(
                            "cursor-v2-reconnect",
                            family_creation_changes,
                        ),
                    ],
                    grpc_status: "13",
                    grpc_message: Some("forced-reconnect"),
                },
                MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-v2-reconnect", 84),
                        family_block_response_from_block_changes(
                            "cursor-v2-reconnect",
                            family_follow_up_changes,
                        ),
                    ],
                    grpc_status: "0",
                    grpc_message: None,
                },
            ])
            .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed reconnect v2 tokens");
            direct_gw
                .add_protocol_types(&[
                    ProtocolType::new(
                        "uniswap_v2_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                    ProtocolType::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                ])
                .await
                .expect("seed reconnect v2 protocol types");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path = test_family_shared_spkg_path("combined-family-v2-reconnect");

            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config(
                "tycho-indexer-v2-reconnect-family-defaults",
                &unique,
                &shared_spkg_path,
                83,
                None,
            );
            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 v2 reconnect family-default config path"),
            )
            .expect("load reconnect family-default config");

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build reconnect combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);
            runners.pop().expect("reconnect family runner").run().await.unwrap().unwrap();

            {
                let requests = captured.lock().unwrap();
                assert_eq!(requests.len(), 2, "expected initial request and hot reconnect");
                assert_eq!(requests[0].start_block_num, 83);
                assert!(requests[0].start_cursor.is_empty());
                assert_eq!(
                    requests[1].start_cursor,
                    "cursor-v2-reconnect@83",
                    "hot reconnect should resume from the cursor emitted after the dynamic-admission block"
                );
            }

            let components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read V2 component universe after reconnect");
            assert!(
                components
                    .entity
                    .iter()
                    .any(|component| component.id == dynamic_component_id),
                "V2 dynamic component should remain queryable after reconnect"
            );

            let dynamic_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[dynamic_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read V2 dynamic component state after reconnect follow-up");
            assert_eq!(dynamic_state.entity.len(), 1);
            assert_eq!(
                dynamic_state.entity[0].attributes.get("reserve0"),
                Some(&Bytes::from(vec![0x13, 0x88]))
            );

            let rpc_port = {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")
                    .expect("bind temp reconnect rpc port");
                let port = listener.local_addr().expect("temp reconnect rpc local addr").port();
                drop(listener);
                port
            };
            let (server_handle, server_task) = ServicesBuilder::new(
                direct_gw.clone(),
                rpc.clone(),
                "test-api-key".to_string(),
            )
            .bind("127.0.0.1")
            .port(rpc_port)
            .protocol_systems(protocol_systems.clone())
            .run()
            .expect("start reconnect rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let mut rpc_components = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                    .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                        "uniswap_v2",
                        vec![dynamic_component_id.to_string()],
                        dto::Chain::Ethereum,
                    ))
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_components rpc should succeed after reconnect, got {}",
                    response.status()
                );
                let body: dto::ProtocolComponentRequestResponse = response
                    .json()
                    .await
                    .expect("decode reconnect protocol components rpc response");
                if body.protocol_components.len() == 1 {
                    rpc_components = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let rpc_components = rpc_components.unwrap_or_else(|| {
                panic!("V2 dynamic component never became queryable through rpc after reconnect")
            });
            assert_eq!(rpc_components.protocol_components.len(), 1);
            assert_eq!(rpc_components.protocol_components[0].id, dynamic_component_id);
            assert_eq!(
                rpc_components.protocol_components[0].protocol_system,
                "uniswap_v2"
            );

            let mut rpc_state = None;
            for _ in 0..100 {
                let response = match client
                    .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                    .json(&dto::ProtocolStateRequestBody {
                        protocol_ids: Some(vec![dynamic_component_id.to_string()]),
                        protocol_system: "uniswap_v2".to_string(),
                        chain: dto::Chain::Ethereum,
                        include_balances: false,
                        version: dto::VersionParam::default(),
                        pagination: dto::PaginationParams::default(),
                    })
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                assert!(
                    response.status().is_success(),
                    "protocol_state rpc should succeed after reconnect, got {}",
                    response.status()
                );
                let body: dto::ProtocolStateRequestResponse = response
                    .json()
                    .await
                    .expect("decode reconnect protocol state rpc response");
                if body.states.len() == 1 {
                    rpc_state = Some(body);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let rpc_state = rpc_state.unwrap_or_else(|| {
                panic!("V2 dynamic state never became queryable through rpc after reconnect")
            });
            assert_eq!(rpc_state.states.len(), 1);
            assert_eq!(rpc_state.states[0].component_id, dynamic_component_id);
            assert_eq!(
                rpc_state.states[0].attributes.get("reserve0"),
                Some(&Bytes::from(vec![0x13, 0x88]))
            );

            server_handle.stop(true).await;
            server_task.abort();

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission(
    ) {
        use ::substreams::store::StoreGet;
        use ethabi::{ethereum_types::U256, Token as AbiToken};
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v2_pool_created_block_changes, build_family_v2_pool_event_block_changes,
            build_uniswap_family_protocol_changes_from_v2, parse_family_v2_pool_created_params,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, transaction_trace::Type as EthTransactionType, Block as EthBlock,
            BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            TransactionTraceStatus,
        };
        use tycho_common::models::{token::Token, FinancialType, ProtocolType};
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::pb::tycho::evm::v1::{
            Block as V2ProtoBlock, BlockChanges as V2BlockChanges,
        };

        fn address(byte: u8) -> Vec<u8> {
            vec![byte; 20]
        }

        fn v2_sync_block(
            number: u64,
            timestamp_secs: i64,
            pool: u8,
            reserve0: u64,
            reserve1: u64,
        ) -> EthBlock {
            let log = EthLog {
                address: address(pool),
                topics: vec![vec![
                    28, 65, 30, 154, 150, 224, 113, 36, 28, 47, 33, 247, 114, 107, 23, 174, 137,
                    227, 202, 180, 199, 139, 229, 14, 6, 43, 3, 169, 255, 251, 186, 209,
                ]],
                data: ethabi::encode(&[
                    AbiToken::Uint(U256::from(reserve0)),
                    AbiToken::Uint(U256::from(reserve1)),
                ]),
                index: 0,
                block_index: 0,
                ordinal: 1,
            };

            EthBlock {
                hash: vec![number as u8; 32],
                number,
                size: 0,
                header: Some(EthBlockHeader {
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    timestamp: Some(Timestamp { seconds: timestamp_secs, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xbb; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt { logs: vec![log], ..Default::default() }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = test_family_protocol_systems();
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);
            let dynamic_component_id = "0x4747474747474747474747474747474747474747";

            let v2_creation_block =
                v2_pair_created_block(43, 1_718_500_043, 0xf1, 0xa0, 0xc0, 0x47);
            let v2_creation_changes = build_family_v2_pool_created_block_changes(
                &v2_creation_block,
                &parse_family_v2_pool_created_params(
                    "factory_address=0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1&protocol_type_name=uniswap_v2_pool",
                ),
            );
            let created_pool = v2_creation_changes.changes[0].component_changes[0].clone();
            let family_creation_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v2(v2_creation_changes)
                    .encode_to_vec()
                    .as_slice(),
            )
            .expect("bridge family block changes into indexer protobuf type");

            let (captured_first, addr_first) =
                start_scripted_mock_substreams(vec![MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-restart-factory", 43),
                        family_block_response_from_block_changes(
                            "cursor-restart",
                            family_creation_changes,
                        ),
                    ],
                    grpc_status: "0",
                    grpc_message: None,
                }])
                .await;

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create Gateway");
            let direct_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build_direct_gw()
                .await
                .expect("Failed to create DirectGateway");

            direct_gw
                .add_tokens(&[
                    Token::new(&token0, "USDC", 6, 0, &[], chain, 100),
                    Token::new(&token1, "WETH", 18, 0, &[], chain, 100),
                ])
                .await
                .expect("seed tokens");
            direct_gw
                .add_protocol_types(&[
                    ProtocolType::new(
                        "uniswap_v2_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                    ProtocolType::new(
                        "uniswap_v3_pool".to_string(),
                        FinancialType::Swap,
                        None,
                        ImplementationType::Custom,
                    ),
                ])
                .await
                .expect("seed protocol types");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let shared_spkg_path =
                test_family_shared_spkg_path("combined-family-restart-dynamic");

            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config(
                "tycho-indexer-family-defaults",
                &unique,
                &shared_spkg_path,
                43,
                None,
            );
            let config = ExtractorConfigs::from_yaml(
                config_path
                    .to_str()
                    .expect("utf8 family-default config path"),
            )
            .expect("load first family-default config");

            let (mut runners, handles) = build_all_extractors(
                &config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr_first}"),
                None,
                "",
                &cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build first combined extractors");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);
            runners.pop().expect("first family runner").run().await.unwrap().unwrap();

            {
                let requests = captured_first.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected a single shared request on first run");
                assert_eq!(requests[0].start_block_num, 43);
                assert!(requests[0].start_cursor.is_empty());
            }

            let after_first_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read component universe after first run");
            assert!(
                after_first_components
                    .entity
                    .iter()
                    .any(|component| component.id == dynamic_component_id),
                "dynamic component should persist before restart"
            );

            let v2_follow_up_block = v2_sync_block(44, 1_718_500_044, 0x47, 3_000, 4_000);
            let pools_store = MockProtoStore::new(0).with_last(
                format!("Pool:{dynamic_component_id}"),
                created_pool,
            );
            let v2_follow_up_changes = build_family_v2_pool_event_block_changes(
                &format!("pools={dynamic_component_id}"),
                &v2_follow_up_block,
                V2BlockChanges {
                    block: Some(V2ProtoBlock {
                        hash: v2_follow_up_block.hash.clone(),
                        parent_hash: v2_follow_up_block
                            .header
                            .as_ref()
                            .map(|header| header.parent_hash.clone())
                            .unwrap_or_default(),
                        number: v2_follow_up_block.number,
                        ts: v2_follow_up_block
                            .header
                            .as_ref()
                            .and_then(|header| header.timestamp.as_ref())
                            .map(|timestamp| timestamp.seconds as u64)
                            .unwrap_or_default(),
                    }),
                    changes: vec![],
                    storage_changes: vec![],
                },
                &pools_store,
            );
            let family_follow_up_changes = substreams::BlockChanges::decode(
                build_uniswap_family_protocol_changes_from_v2(v2_follow_up_changes)
                    .encode_to_vec()
                    .as_slice(),
            )
            .expect("bridge restart v2 sync follow-up into indexer protobuf type");

            let (captured_second, addr_second) =
                start_scripted_mock_substreams(vec![MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-restart-factory", 44),
                        family_block_response_from_block_changes(
                            "cursor-restart",
                            family_follow_up_changes,
                        ),
                    ],
                    grpc_status: "0",
                    grpc_message: None,
                }])
                .await;

            let resumed_cached_gw = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&protocol_systems)
                .build()
                .await
                .expect("Failed to create resumed Gateway")
                .0;

            let resumed_config_path = test_family_defaults_config(
                "tycho-indexer-family-defaults",
                &format!("{unique}-resumed"),
                &shared_spkg_path,
                43,
                None,
            );
            let resumed_config = ExtractorConfigs::from_yaml(
                resumed_config_path
                    .to_str()
                    .expect("utf8 resumed family-default config path"),
            )
            .expect("load resumed family-default config");
            let (mut resumed_runners, resumed_handles) = build_all_extractors(
                &resumed_config,
                ChainState::default(),
                &[chain],
                &format!("http://{addr_second}"),
                None,
                "",
                &resumed_cached_gw,
                1000,
                &token_processor,
                &rpc,
                None,
                false,
            )
            .await
            .expect("build resumed combined extractors");

            assert_eq!(resumed_runners.len(), 1);
            assert_eq!(resumed_handles.len(), 2);
            resumed_runners
                .pop()
                .expect("resumed family runner")
                .run()
                .await
                .unwrap()
                .unwrap();

            {
                let requests = captured_second.lock().unwrap();
                assert_eq!(requests.len(), 1, "expected one shared request after restart");
                assert_eq!(
                    requests[0].start_block_num, 44,
                    "fresh restart should resume from block after last committed dynamic-admission block"
                );
                assert_eq!(requests[0].start_cursor, "cursor-restart@43");
            }

            let after_restart_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v2".to_string()), None, None, None)
                .await
                .expect("read component universe after restart");
            assert!(
                after_restart_components
                    .entity
                    .iter()
                    .any(|component| component.id == dynamic_component_id),
                "dynamic component should remain queryable after restart resume"
            );
            let dynamic_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[dynamic_component_id]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic component state after restart follow-up");
            assert_eq!(dynamic_state.entity.len(), 1);
            assert_eq!(
                dynamic_state.entity[0].attributes.get("reserve0"),
                Some(&Bytes::from(vec![0x0b, 0xb8]))
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
            let _ = std::fs::remove_file(&resumed_config_path);
        })
        .await;
    }
}

#[cfg(test)]
mod recorder_tests {
    use super::*;
    use crate::pb::sf::substreams::rpc::v2::{
        response::Message as ResponseMessage, BlockScopedData, Response,
    };
    use clap::Parser;
    use prost::Message;
    use tycho_indexer::{
        pb::sf::substreams::v1::Clock,
        substreams::mock::{
            read_mock_substreams_fixture, start_scripted_mock_substreams, MockSubstreamsScript,
        },
    };

    fn block_response(number: u64, cursor: &str) -> Response {
        Response {
            message: Some(ResponseMessage::BlockScopedData(BlockScopedData {
                output: None,
                clock: Some(Clock { id: number.to_string(), number, timestamp: None }),
                cursor: cursor.to_string(),
                final_block_height: number,
                debug_map_outputs: vec![],
                debug_store_outputs: vec![],
                attestation: String::new(),
                is_partial: false,
                partial_index: None,
                is_last_partial: None,
            })),
        }
    }

    fn temp_path(name: &str, ext: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tycho-indexer-{name}-{}-{}.{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default(),
            ext
        ))
    }

    fn combined_family_real_history_slice_fixture_path_for_recorder() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("combined_family_real_history_slice.json")
    }

    fn combined_family_real_history_slice_script_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent")
            .join("scripts")
            .join("combined-family-history-slice-fixture.sh")
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RepoCombinedFamilyFixtureCaptureSpec {
        family_name: String,
        extractors_config_path: std::path::PathBuf,
        output_module: String,
        expected_spkg: String,
        output_path: std::path::PathBuf,
        start_block: i64,
        stop_block: String,
        params: Vec<String>,
    }

    #[derive(Debug, Clone, Copy)]
    struct RecordSubstreamsFixtureMemberSpec<'a> {
        protocol_system: &'a str,
        protocol_type_name: &'a str,
        module_name: &'a str,
        substreams_module_name: &'a str,
        substreams_file_name: &'a str,
        substreams_body: &'a str,
    }

    #[derive(Debug, Clone, Copy)]
    struct RecordSubstreamsFixtureFamilySpec<'a> {
        temp_prefix: &'a str,
        extractors_file_name: &'a str,
        family_name: &'a str,
        shared_module: &'a str,
        shared_bootstrap_body: &'a str,
        members: &'a [RecordSubstreamsFixtureMemberSpec<'a>],
    }

    fn combined_family_real_history_slice_capture_spec() -> RepoCombinedFamilyFixtureCaptureSpec {
        RepoCombinedFamilyFixtureCaptureSpec {
            family_name: "uniswap".to_string(),
            extractors_config_path: repo_combined_family_extractors_config_path(
                "extractors.uniswap_v2_v3.combined.yaml",
            ),
            output_module: repo_combined_family_output_module("uniswap"),
            expected_spkg:
                "protocols/substreams/ethereum-uniswap-v2-v3-combined/ethereum-uniswap-v2-v3-combined-v0.1.0.spkg"
                    .to_string(),
            output_path: combined_family_real_history_slice_fixture_path_for_recorder(),
            start_block: 25_384_601,
            stop_block: "+2".to_string(),
            params: vec![],
        }
    }

    fn repo_combined_family_record_args(
        spec: &RepoCombinedFamilyFixtureCaptureSpec,
        output_path: &std::path::Path,
        start_block: i64,
        stop_block: &str,
        params: &[&str],
    ) -> RecordSubstreamsArgs {
        let cli_args = repo_combined_family_record_cli_args(
            spec,
            output_path,
            start_block,
            stop_block,
            params,
        );

        let cli =
            Cli::try_parse_from(cli_args).expect("parse repo-combined record-substreams command");
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };
        record_args
    }

    fn repo_combined_family_record_cli_args(
        spec: &RepoCombinedFamilyFixtureCaptureSpec,
        output_path: &std::path::Path,
        start_block: i64,
        stop_block: &str,
        params: &[&str],
    ) -> Vec<String> {
        let mut cli_args = vec![
            "tycho-indexer".to_string(),
            "--database-url".to_string(),
            "postgres://unused".to_string(),
            "--endpoint".to_string(),
            "http://localhost:9000".to_string(),
            "--rpc-url".to_string(),
            "http://localhost:8545".to_string(),
            "record-substreams".to_string(),
            "--substreams-api-token".to_string(),
            "token".to_string(),
            "--extractors-config".to_string(),
            spec.extractors_config_path
                .to_string_lossy()
                .to_string(),
            "--family".to_string(),
            spec.family_name.clone(),
            "--start-block".to_string(),
            start_block.to_string(),
            "--stop-block".to_string(),
            stop_block.to_string(),
            "--output".to_string(),
            output_path
                .to_string_lossy()
                .to_string(),
        ];
        for param in params {
            cli_args.push("--params".to_string());
            cli_args.push((*param).to_string());
        }
        cli_args
    }

    fn shell_escape_cli_arg(arg: &str) -> String {
        if arg.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'/' | b'.' | b':' | b'+' | b'=')
        }) {
            return arg.to_string();
        }

        format!("'{}'", arg.replace('\'', "'\"'\"'"))
    }

    fn render_repo_combined_family_record_command(cli_args: &[String]) -> String {
        cli_args
            .iter()
            .map(|arg| shell_escape_cli_arg(arg))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn repo_combined_family_record_args_from_spec(
        spec: &RepoCombinedFamilyFixtureCaptureSpec,
    ) -> RecordSubstreamsArgs {
        let params = spec
            .params
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        repo_combined_family_record_args(
            spec,
            &spec.output_path,
            spec.start_block,
            &spec.stop_block,
            &params,
        )
    }

    fn write_record_substreams_combined_family_fixture_inputs(
        shared_spkg_path: &std::path::Path,
        spec: RecordSubstreamsFixtureFamilySpec<'_>,
    ) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "tycho-indexer-{}-{}-{}",
            spec.temp_prefix,
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).expect("create record-substreams family config dir");

        std::fs::write(
            config_dir.join("shared_bootstrap.yaml"),
            spec.shared_bootstrap_body,
        )
        .expect("write shared bootstrap file");
        for member in spec.members {
            std::fs::write(config_dir.join(member.substreams_file_name), member.substreams_body)
                .expect("write member substreams params file");
        }

        let members_yaml = spec
            .members
            .iter()
            .map(|member| {
                format!(
                    r#"      {protocol_system}:
        substreams_params:
          {substreams_module_name}: "@config/{substreams_file_name}"
"#,
                    protocol_system = member.protocol_system,
                    substreams_module_name = member.substreams_module_name,
                    substreams_file_name = member.substreams_file_name,
                )
            })
            .collect::<String>();
        let extractors_yaml = spec
            .members
            .iter()
            .map(|member| {
                format!(
                    r#"  {protocol_system}:
    name: "{protocol_system}"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1000
    protocol_types:
      - name: "{protocol_type_name}"
        financial_type: "Swap"
    module_name: "{module_name}"
    family_runtime:
      family: "{family_name}"
"#,
                    protocol_system = member.protocol_system,
                    protocol_type_name = member.protocol_type_name,
                    module_name = member.module_name,
                    family_name = spec.family_name,
                )
            })
            .collect::<String>();
        let extractors_config = root.join(spec.extractors_file_name);
        std::fs::write(
            &extractors_config,
            format!(
                r#"family_runtimes:
  {family_name}:
    shared_spkg: "{}"
    shared_module: "{shared_module}"
    bootstrap:
      params: "@config/shared_bootstrap.yaml"
    members:
{members_yaml}extractors:
{extractors_yaml}
"#,
                shared_spkg_path.display(),
                family_name = spec.family_name,
                shared_module = spec.shared_module,
                members_yaml = members_yaml,
                extractors_yaml = extractors_yaml,
            )
        )
        .expect("write combined extractors config");

        extractors_config
    }

    fn write_record_substreams_family_fixture_inputs(
        shared_spkg_path: &std::path::Path,
    ) -> std::path::PathBuf {
        const MEMBERS: &[RecordSubstreamsFixtureMemberSpec<'_>] = &[
            RecordSubstreamsFixtureMemberSpec {
                protocol_system: "uniswap_v2",
                protocol_type_name: "uniswap_v2_pool",
                module_name: "v2_map_pool_events",
                substreams_module_name: "v2_map_pool_events",
                substreams_file_name: "uniswap_v2_substreams.yaml",
                substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x1111111111111111111111111111111111111111"
"#,
            },
            RecordSubstreamsFixtureMemberSpec {
                protocol_system: "uniswap_v3",
                protocol_type_name: "uniswap_v3_pool",
                module_name: "v3_map_protocol_changes",
                substreams_module_name: "v3_map_events",
                substreams_file_name: "uniswap_v3_substreams.yaml",
                substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x2222222222222222222222222222222222222222"
"#,
            },
        ];
        write_record_substreams_combined_family_fixture_inputs(
            shared_spkg_path,
            RecordSubstreamsFixtureFamilySpec {
                temp_prefix: "record-family-config",
                extractors_file_name: "extractors.combined.yaml",
                family_name: "uniswap",
                shared_module: &repo_combined_family_output_module("uniswap"),
                shared_bootstrap_body: r#"start_block: 42
params:
  pools:
    - "0x1111111111111111111111111111111111111111"
    - "0x2222222222222222222222222222222222222222"
"#,
                members: MEMBERS,
            },
        )
    }

    fn write_record_substreams_future_family_fixture_inputs(
        shared_spkg_path: &std::path::Path,
    ) -> std::path::PathBuf {
        const MEMBERS: &[RecordSubstreamsFixtureMemberSpec<'_>] = &[
            RecordSubstreamsFixtureMemberSpec {
                protocol_system: "future_v1",
                protocol_type_name: "future_v1_pool",
                module_name: "future_v1_map_protocol_changes",
                substreams_module_name: "future_v1_map_events",
                substreams_file_name: "future_v1_substreams.yaml",
                substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x00000000000000000000000000000000000000a1"
"#,
            },
            RecordSubstreamsFixtureMemberSpec {
                protocol_system: "future_v2",
                protocol_type_name: "future_v2_pool",
                module_name: "future_v2_map_protocol_changes",
                substreams_module_name: "future_v2_map_events",
                substreams_file_name: "future_v2_substreams.yaml",
                substreams_body: r#"includes:
  - "shared_bootstrap.yaml"
params:
  pools:
    - "0x00000000000000000000000000000000000000b2"
"#,
            },
        ];
        write_record_substreams_combined_family_fixture_inputs(
            shared_spkg_path,
            RecordSubstreamsFixtureFamilySpec {
                temp_prefix: "record-future-family-config",
                extractors_file_name: "extractors.future.combined.yaml",
                family_name: "future_swap",
                shared_module: "map_future_swap_family_protocol_changes",
                shared_bootstrap_body: r#"start_block: 99
params:
  pools:
    - "0x00000000000000000000000000000000000000a1"
    - "0x00000000000000000000000000000000000000b2"
"#,
                members: MEMBERS,
            },
        )
    }

    #[tokio::test]
    async fn record_substreams_fixture_writes_replayable_fixture_via_command_path() {
        let expected_responses = vec![
            crate::testing::scripted_session_response("trace-record", 42),
            block_response(42, "cursor@42"),
            block_response(43, "cursor@43"),
        ];
        let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
            responses: expected_responses.clone(),
            grpc_status: "0",
            grpc_message: None,
        }])
        .await;

        let spkg_path = temp_path("record-command-spkg", "spkg");
        std::fs::write(
            &spkg_path,
            tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
        )
        .expect("write temp spkg");
        let output_path = temp_path("record-command-output", "json");

        let cli = Cli::try_parse_from([
            "tycho-indexer",
            "--database-url",
            "postgres://unused",
            "--endpoint",
            &format!("http://{addr}"),
            "--rpc-url",
            "http://unused",
            "record-substreams",
            "--substreams-api-token",
            "token",
            "--spkg",
            &spkg_path.to_string_lossy(),
            "--module",
            &repo_combined_family_output_module("uniswap"),
            "--start-block",
            "42",
            "--stop-block",
            "44",
            "--output",
            &output_path.to_string_lossy(),
            "--params",
            "factory=0xf1,pool=0x45",
        ])
        .expect("parse record-substreams command");
        let global_args = cli.args();
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };

        record_substreams_fixture(&global_args, &record_args)
            .await
            .expect("record fixture through command path");

        let fixture = read_mock_substreams_fixture(&output_path).expect("read recorded fixture");
        assert_eq!(fixture.len(), 1);
        assert_eq!(fixture[0].grpc_status, "0");
        assert_eq!(fixture[0].grpc_message, None);
        assert_eq!(fixture[0].responses, expected_responses);

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].start_block_num, 42);
        assert_eq!(requests[0].stop_block_num, 44);
        assert_eq!(requests[0].output_module, repo_combined_family_output_module("uniswap"));
        assert_eq!(requests[0].params.get("factory"), Some(&"0xf1".to_string()));
        assert_eq!(requests[0].params.get("pool"), Some(&"0x45".to_string()));
        drop(requests);

        let _ = std::fs::remove_file(spkg_path);
        let _ = std::fs::remove_file(output_path);
    }

    #[tokio::test]
    async fn record_substreams_fixture_derives_shared_family_request_from_combined_config() {
        let expected_responses = vec![
            crate::testing::scripted_session_response("trace-record", 43),
            block_response(43, "cursor@43"),
            block_response(44, "cursor@44"),
        ];
        let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
            responses: expected_responses.clone(),
            grpc_status: "0",
            grpc_message: None,
        }])
        .await;

        let spkg_path = temp_path("record-family-command-spkg", "spkg");
        std::fs::write(
            &spkg_path,
            tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
        )
        .expect("write temp family spkg");
        let extractors_config_path = write_record_substreams_family_fixture_inputs(&spkg_path);
        let output_path = temp_path("record-family-command-output", "json");

        let cli = Cli::try_parse_from([
            "tycho-indexer",
            "--database-url",
            "postgres://unused",
            "--endpoint",
            &format!("http://{addr}"),
            "--rpc-url",
            "http://unused",
            "record-substreams",
            "--substreams-api-token",
            "token",
            "--extractors-config",
            &extractors_config_path.to_string_lossy(),
            "--family",
            "uniswap",
            "--stop-block",
            "+2",
            "--output",
            &output_path.to_string_lossy(),
            "--params",
            "extra_flag=enabled",
        ])
        .expect("parse derived record-substreams command");
        let global_args = cli.args();
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };

        record_substreams_fixture(&global_args, &record_args)
            .await
            .expect("record derived fixture through command path");

        let fixture = read_mock_substreams_fixture(&output_path).expect("read recorded fixture");
        assert_eq!(fixture.len(), 1);
        assert_eq!(fixture[0].grpc_status, "0");
        assert_eq!(fixture[0].grpc_message, None);
        assert_eq!(fixture[0].responses, expected_responses);

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].start_block_num, 43);
        assert_eq!(requests[0].stop_block_num, 45);
        assert_eq!(requests[0].output_module, repo_combined_family_output_module("uniswap"));
        let v2_params = requests[0]
            .params
            .get("v2_map_pool_events")
            .expect("derived v2 params present");
        assert!(
            v2_params.contains("bootstrap_block=42"),
            "expected derived v2 params to preserve bootstrap block, got {v2_params}"
        );
        assert!(
            v2_params.contains("0x1111111111111111111111111111111111111111"),
            "expected derived v2 params to include the V2 pool, got {v2_params}"
        );
        let v3_params = requests[0]
            .params
            .get("v3_map_events")
            .expect("derived v3 params present");
        assert!(
            v3_params.contains("bootstrap_block=42"),
            "expected derived v3 params to preserve bootstrap block, got {v3_params}"
        );
        assert!(
            v3_params.contains("0x2222222222222222222222222222222222222222"),
            "expected derived v3 params to include the V3 pool, got {v3_params}"
        );
        assert_eq!(requests[0].params.get("extra_flag"), Some(&"enabled".to_string()));
        drop(requests);

        let _ = std::fs::remove_file(spkg_path);
        let _ = std::fs::remove_file(output_path);
        let _ = std::fs::remove_file(extractors_config_path);
    }

    #[test]
    fn resolve_record_substreams_request_with_registry_derives_future_family_request() {
        use crate::extractor::family_registry::{
            shared_bootstrap_member_runtime, shared_family_member_spec, shared_family_runtime_spec,
        };
        use crate::extractor::family_runtime::{
            FamilyRuntimeRegistry, FamilyRuntimeSpec, SharedBootstrapParamsParser,
        };

        fn future_branch_materializer<'a>(
            _rpc: &'a tycho_ethereum::rpc::EthereumRpcClient,
            _branch: &'a crate::extractor::shared_bootstrap::BootstrapBranchDescriptor,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::extractor::models::BlockChanges, ExtractionError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async {
                Err(ExtractionError::Setup(
                    "future branch materializer should not run in request-derivation test"
                        .to_string(),
                ))
            })
        }

        const FUTURE_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "future_swap",
            &[
                shared_family_member_spec(
                    "future_v1",
                    &["futurev1"],
                    Some(shared_bootstrap_member_runtime(
                        crate::extractor::runner::BootstrapStrategy::UniswapV2Rpc,
                        SharedBootstrapParamsParser::PoolList,
                        future_branch_materializer,
                    )),
                ),
                shared_family_member_spec(
                    "future_v2",
                    &["futurev2"],
                    Some(shared_bootstrap_member_runtime(
                        crate::extractor::runner::BootstrapStrategy::UniswapV2Rpc,
                        SharedBootstrapParamsParser::PoolList,
                        future_branch_materializer,
                    )),
                ),
            ],
            "map_future_swap_family_protocol_changes",
            "future_swap_family",
            "family::future_swap",
            None,
        );

        let spkg_path = temp_path("record-future-family-command-spkg", "spkg");
        std::fs::write(
            &spkg_path,
            tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
        )
        .expect("write temp future family spkg");
        let extractors_config_path =
            write_record_substreams_future_family_fixture_inputs(&spkg_path);
        let output_path = temp_path("record-future-family-output", "json");
        let cli = Cli::try_parse_from([
            "tycho-indexer",
            "--database-url",
            "postgres://unused",
            "--endpoint",
            "http://localhost:9000",
            "--rpc-url",
            "http://localhost:8545",
            "record-substreams",
            "--substreams-api-token",
            "token",
            "--extractors-config",
            &extractors_config_path.to_string_lossy(),
            "--family",
            "future_swap",
            "--stop-block",
            "+2",
            "--output",
            &output_path.to_string_lossy(),
            "--params",
            "extra_flag=future-enabled",
        ])
        .expect("parse future-family derived record-substreams command");
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };

        let resolved = resolve_record_substreams_request_with_registry(
            &record_args,
            FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]),
        )
        .expect("future family request should resolve through custom registry");

        assert_eq!(resolved.spkg, spkg_path.to_string_lossy());
        assert_eq!(resolved.module, "map_future_swap_family_protocol_changes");
        assert_eq!(resolved.start_block, 100);
        assert_eq!(resolved.stop_block, 102);
        assert_eq!(resolved.extractor_id, "ethereum:future_swap_family");
        assert_eq!(resolved.params.get("extra_flag"), Some(&"future-enabled".to_string()));
        let v1_params = resolved
            .params
            .get("future_v1_map_events")
            .expect("future v1 params should resolve");
        assert!(v1_params.contains("bootstrap_block=99"));
        assert!(v1_params.contains("0x00000000000000000000000000000000000000a1"));
        let v2_params = resolved
            .params
            .get("future_v2_map_events")
            .expect("future v2 params should resolve");
        assert!(v2_params.contains("bootstrap_block=99"));
        assert!(v2_params.contains("0x00000000000000000000000000000000000000b2"));

        let _ = std::fs::remove_file(spkg_path);
        let _ = std::fs::remove_file(output_path);
        let _ = std::fs::remove_file(extractors_config_path);
    }

    #[tokio::test]
    async fn record_substreams_fixture_with_registry_records_future_family_request() {
        use crate::extractor::family_registry::{
            shared_bootstrap_member_runtime, shared_family_member_spec, shared_family_runtime_spec,
        };
        use crate::extractor::family_runtime::{
            FamilyRuntimeRegistry, FamilyRuntimeSpec, SharedBootstrapParamsParser,
        };
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
        };

        fn future_branch_materializer<'a>(
            _rpc: &'a tycho_ethereum::rpc::EthereumRpcClient,
            _branch: &'a crate::extractor::shared_bootstrap::BootstrapBranchDescriptor,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::extractor::models::BlockChanges, ExtractionError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async {
                Err(ExtractionError::Setup(
                    "future branch materializer should not run in recorder test".to_string(),
                ))
            })
        }

        const FUTURE_FAMILY: FamilyRuntimeSpec = shared_family_runtime_spec(
            "future_swap",
            &[
                shared_family_member_spec(
                    "future_v1",
                    &["futurev1"],
                    Some(shared_bootstrap_member_runtime(
                        crate::extractor::runner::BootstrapStrategy::UniswapV2Rpc,
                        SharedBootstrapParamsParser::PoolList,
                        future_branch_materializer,
                    )),
                ),
                shared_family_member_spec(
                    "future_v2",
                    &["futurev2"],
                    Some(shared_bootstrap_member_runtime(
                        crate::extractor::runner::BootstrapStrategy::UniswapV2Rpc,
                        SharedBootstrapParamsParser::PoolList,
                        future_branch_materializer,
                    )),
                ),
            ],
            "map_future_swap_family_protocol_changes",
            "future_swap_family",
            "family::future_swap",
            None,
        );

        let expected_responses = vec![
            crate::testing::scripted_session_response("trace-record", 100),
            block_response(100, "future-cursor@100"),
            block_response(101, "future-cursor@101"),
        ];
        let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
            responses: expected_responses.clone(),
            grpc_status: "0",
            grpc_message: None,
        }])
        .await;

        let spkg_path = temp_path("record-future-family-command-spkg", "spkg");
        std::fs::write(
            &spkg_path,
            tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
        )
        .expect("write temp future family spkg");
        let extractors_config_path =
            write_record_substreams_future_family_fixture_inputs(&spkg_path);
        let output_path = temp_path("record-future-family-command-output", "json");

        let cli = Cli::try_parse_from([
            "tycho-indexer",
            "--database-url",
            "postgres://unused",
            "--endpoint",
            &format!("http://{addr}"),
            "--rpc-url",
            "http://unused",
            "record-substreams",
            "--substreams-api-token",
            "token",
            "--extractors-config",
            &extractors_config_path.to_string_lossy(),
            "--family",
            "future_swap",
            "--stop-block",
            "+2",
            "--output",
            &output_path.to_string_lossy(),
            "--params",
            "extra_flag=future-enabled",
        ])
        .expect("parse future-family record-substreams command");
        let global_args = cli.args();
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };

        record_substreams_fixture_with_registry(
            &global_args,
            &record_args,
            FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]),
        )
        .await
        .expect("record future-family fixture through command path");

        let fixture = read_mock_substreams_fixture(&output_path).expect("read recorded fixture");
        assert_eq!(fixture.len(), 1);
        assert_eq!(fixture[0].grpc_status, "0");
        assert_eq!(fixture[0].grpc_message, None);
        assert_eq!(fixture[0].responses, expected_responses);

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].start_block_num, 100);
        assert_eq!(requests[0].stop_block_num, 102);
        assert_eq!(requests[0].output_module, "map_future_swap_family_protocol_changes");
        assert_eq!(requests[0].params.get("extra_flag"), Some(&"future-enabled".to_string()));
        let v1_params = requests[0]
            .params
            .get("future_v1_map_events")
            .expect("future v1 params present");
        assert!(v1_params.contains("bootstrap_block=99"));
        assert!(v1_params.contains("0x00000000000000000000000000000000000000a1"));
        let v2_params = requests[0]
            .params
            .get("future_v2_map_events")
            .expect("future v2 params present");
        assert!(v2_params.contains("bootstrap_block=99"));
        assert!(v2_params.contains("0x00000000000000000000000000000000000000b2"));
        drop(requests);

        let _ = std::fs::remove_file(spkg_path);
        let _ = std::fs::remove_file(output_path);
        let _ = std::fs::remove_file(extractors_config_path);
    }

    #[test]
    fn render_record_substreams_request_json_includes_resolved_combined_family_fields() {
        let spec = combined_family_real_history_slice_capture_spec();
        let output_path = temp_path("repo-combined-derived-request-json", "json");
        let record_args = repo_combined_family_record_args(
            &spec,
            &output_path,
            25_384_601,
            "+2",
            &["extra_flag=enabled"],
        );
        let resolved = resolve_record_substreams_request(&record_args)
            .expect("repo combined config should derive one shared family request");

        let rendered = render_record_substreams_request_json(&resolved)
            .expect("resolved combined family request should render to json");

        assert!(rendered.contains(&format!("\"module\": \"{}\"", spec.output_module)));
        assert!(rendered.contains("\"start_block\": 25384601"));
        assert!(rendered.contains("\"stop_block\": 25384603"));
        assert!(rendered.contains("\"v2_map_pool_events\""));
        assert!(rendered.contains("\"v3_map_events\""));
        assert!(rendered.contains("\"extra_flag\": \"enabled\""));

        let _ = std::fs::remove_file(output_path);
    }

    #[tokio::test]
    async fn record_substreams_print_request_short_circuits_before_network_recording() {
        let spec = combined_family_real_history_slice_capture_spec();
        let output_path = temp_path("record-family-print-request-output", "json");

        let cli = Cli::try_parse_from([
            "tycho-indexer",
            "--database-url",
            "postgres://unused",
            "--endpoint",
            "http://127.0.0.1:1",
            "--rpc-url",
            "http://unused",
            "record-substreams",
            "--substreams-api-token",
            "token",
            "--extractors-config",
            &spec
                .extractors_config_path
                .to_string_lossy(),
            "--family",
            &spec.family_name,
            "--start-block",
            "25384601",
            "--stop-block",
            "+2",
            "--output",
            &output_path.to_string_lossy(),
            "--print-request",
        ])
        .expect("parse print-request record-substreams command");
        let global_args = cli.args();
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };

        record_substreams_fixture(&global_args, &record_args)
            .await
            .expect("print-request path should not require network access");

        assert!(!output_path.exists(), "print-request mode should not write a fixture file");
    }

    #[test]
    fn resolve_record_substreams_request_derives_shared_family_request_from_repo_combined_config() {
        let spec = combined_family_real_history_slice_capture_spec();
        let output_path = temp_path("repo-combined-derived-request", "json");
        assert!(
            spec.extractors_config_path.exists(),
            "expected checked-in combined config at {}",
            spec.extractors_config_path.display()
        );
        let record_args = repo_combined_family_record_args(
            &spec,
            &output_path,
            25_384_601,
            "+2",
            &["extra_flag=enabled"],
        );

        let resolved = resolve_record_substreams_request(&record_args)
            .expect("repo combined config should derive one shared family request");

        assert_eq!(resolved.spkg, spec.expected_spkg);
        assert_eq!(resolved.module, spec.output_module);
        assert_eq!(resolved.start_block, 25_384_601);
        assert_eq!(resolved.stop_block, 25_384_603);
        assert_eq!(resolved.params.get("extra_flag"), Some(&"enabled".to_string()));

        let v2_params = resolved
            .params
            .get("v2_map_pool_events")
            .expect("repo combined config should resolve v2 shared params");
        assert!(
            v2_params.contains("bootstrap_block=25384600"),
            "expected repo combined v2 params to preserve bootstrap block, got {v2_params}"
        );
        assert!(
            v2_params.contains("0xfaf477185220f1fbf987a43374ca640d670f2c90"),
            "expected repo combined v2 params to include a checked-in V2 pool, got {v2_params}"
        );

        let v3_params = resolved
            .params
            .get("v3_map_events")
            .expect("repo combined config should resolve v3 shared params");
        assert!(
            v3_params.contains("bootstrap_block=25384600"),
            "expected repo combined v3 params to preserve bootstrap block, got {v3_params}"
        );
        assert!(
            v3_params.contains("0x58cf91c080f7052f6da209bf605d6cf1cefd65f3"),
            "expected repo combined v3 params to include a checked-in V3 pool, got {v3_params}"
        );

        let _ = std::fs::remove_file(output_path);
    }

    #[test]
    fn repo_combined_family_record_args_can_anchor_fixture_refresh_workflow() {
        let spec = combined_family_real_history_slice_capture_spec();
        let output_path = combined_family_real_history_slice_fixture_path_for_recorder();
        let record_args = repo_combined_family_record_args(
            &spec,
            &output_path,
            25_384_601,
            "+2",
            &["extra_flag=enabled"],
        );
        let resolved = resolve_record_substreams_request(&record_args)
            .expect("repo combined recorder helper should resolve");

        assert_eq!(record_args.output, output_path.to_string_lossy());
        assert_eq!(
            record_args.extractors_config.as_deref(),
            Some(
                spec.extractors_config_path
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert_eq!(record_args.family.as_deref(), Some(spec.family_name.as_str()));
        assert_eq!(resolved.module, spec.output_module);
        assert_eq!(resolved.spkg, spec.expected_spkg);
    }

    #[test]
    fn combined_family_real_history_slice_capture_spec_anchors_live_fixture_refresh() {
        let spec = combined_family_real_history_slice_capture_spec();
        let record_args = repo_combined_family_record_args_from_spec(&spec);
        let resolved = resolve_record_substreams_request(&record_args).expect(
            "history-slice capture spec should resolve through repo combined recorder path",
        );

        assert_eq!(
            spec.output_path,
            combined_family_real_history_slice_fixture_path_for_recorder()
        );
        assert_eq!(spec.start_block, 25_384_601);
        assert_eq!(spec.stop_block, "+2");
        assert!(spec.params.is_empty());

        assert_eq!(
            record_args.output,
            combined_family_real_history_slice_fixture_path_for_recorder().to_string_lossy()
        );
        assert_eq!(record_args.start_block, Some(25_384_601));
        assert_eq!(record_args.family.as_deref(), Some(spec.family_name.as_str()));
        assert_eq!(resolved.start_block, 25_384_601);
        assert_eq!(resolved.stop_block, 25_384_603);
        assert_eq!(resolved.module, spec.output_module);
    }

    #[test]
    fn combined_family_real_history_slice_capture_spec_builds_stable_repo_cli_args() {
        let spec = combined_family_real_history_slice_capture_spec();
        let cli_args = repo_combined_family_record_cli_args(
            &spec,
            &spec.output_path,
            spec.start_block,
            &spec.stop_block,
            &[],
        );

        let expected = vec![
            "tycho-indexer".to_string(),
            "--database-url".to_string(),
            "postgres://unused".to_string(),
            "--endpoint".to_string(),
            "http://localhost:9000".to_string(),
            "--rpc-url".to_string(),
            "http://localhost:8545".to_string(),
            "record-substreams".to_string(),
            "--substreams-api-token".to_string(),
            "token".to_string(),
            "--extractors-config".to_string(),
            spec.extractors_config_path
                .to_string_lossy()
                .to_string(),
            "--family".to_string(),
            spec.family_name.clone(),
            "--start-block".to_string(),
            "25384601".to_string(),
            "--stop-block".to_string(),
            "+2".to_string(),
            "--output".to_string(),
            spec.output_path
                .to_string_lossy()
                .to_string(),
        ];

        assert_eq!(cli_args, expected);
    }

    #[test]
    fn combined_family_real_history_slice_capture_spec_renders_stable_shell_command() {
        let spec = combined_family_real_history_slice_capture_spec();
        let cli_args = repo_combined_family_record_cli_args(
            &spec,
            &spec.output_path,
            spec.start_block,
            &spec.stop_block,
            &[],
        );

        let rendered = render_repo_combined_family_record_command(&cli_args);

        assert_eq!(
            rendered,
            format!(
                "tycho-indexer --database-url postgres://unused --endpoint http://localhost:9000 --rpc-url http://localhost:8545 record-substreams --substreams-api-token token --extractors-config {} --family {} --start-block 25384601 --stop-block +2 --output {}",
                shell_escape_cli_arg(&spec.extractors_config_path.to_string_lossy()),
                spec.family_name,
                shell_escape_cli_arg(&spec.output_path.to_string_lossy()),
            )
        );
    }

    #[test]
    fn combined_family_real_history_slice_script_command_renders_stable_live_capture_command() {
        let spec = combined_family_real_history_slice_capture_spec();
        let script_path = combined_family_real_history_slice_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected fixture helper script command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "script command mode should succeed, stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("script command mode should emit utf8 shell command");
        let expected = format!(
            "cargo run --bin tycho-indexer -- \\\n  --database-url postgres://unused \\\n  --endpoint '<set TYCHO_RECORD_ENDPOINT>' \\\n  --rpc-url '<set TYCHO_RECORD_RPC_URL>' \\\n  record-substreams \\\n  --substreams-api-token '<set SUBSTREAMS_API_TOKEN>' \\\n  --extractors-config {} \\\n  --family {} \\\n  --start-block {} \\\n  --stop-block {} \\\n  --output {}\n",
            spec.extractors_config_path.to_string_lossy(),
            spec.family_name,
            spec.start_block,
            spec.stop_block,
            spec.output_path.to_string_lossy(),
        );

        assert_eq!(rendered, expected);
    }

    #[test]
    fn combined_family_real_history_slice_script_doctor_reports_missing_external_requirements() {
        let spec = combined_family_real_history_slice_capture_spec();
        let script_path = combined_family_real_history_slice_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env_remove("SUBSTREAMS_API_TOKEN")
            .env_remove("TYCHO_RECORD_ENDPOINT")
            .env_remove("TYCHO_RECORD_RPC_URL")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected fixture helper script doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "script doctor mode should succeed, stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("script doctor mode should emit utf8 readiness output");
        let expected = format!(
            "ready=false\nstart_block={}\nstop_block={}\nextractors_config={}\noutput_path={}\nsubstreams_api_token=missing\nrecord_endpoint=missing\nrecord_rpc_url=missing\n",
            spec.start_block,
            spec.stop_block,
            spec.extractors_config_path.to_string_lossy(),
            spec.output_path.to_string_lossy(),
        );

        assert_eq!(rendered, expected);
    }

    #[test]
    fn combined_family_real_history_slice_script_doctor_strict_fails_when_env_is_incomplete() {
        let script_path = combined_family_real_history_slice_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .arg("--strict")
            .env_remove("SUBSTREAMS_API_TOKEN")
            .env_remove("TYCHO_RECORD_ENDPOINT")
            .env_remove("TYCHO_RECORD_RPC_URL")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected fixture helper script strict doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            !output.status.success(),
            "strict doctor mode should fail when required env is missing"
        );
        assert_eq!(output.status.code(), Some(1));
        let rendered = String::from_utf8(output.stdout)
            .expect("strict doctor mode should still emit utf8 readiness output");
        assert!(
            rendered.contains("ready=false"),
            "strict doctor mode should still explain the readiness failure"
        );
    }

    #[test]
    fn combined_family_real_history_slice_script_stays_aligned_with_capture_spec() {
        let spec = combined_family_real_history_slice_capture_spec();
        let script_path = combined_family_real_history_slice_script_path();
        let extractors_config_relative = spec
            .extractors_config_path
            .strip_prefix(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))
            .expect("combined extractor config should live under CARGO_MANIFEST_DIR")
            .to_string_lossy()
            .to_string();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!("expected to read fixture helper script at {}: {err}", script_path.display())
        });

        assert!(
            script.contains("scripts/combined-family-history-slice-fixture.sh doctor"),
            "script should advertise the doctor mode"
        );
        assert!(
            script.contains("scripts/combined-family-history-slice-fixture.sh preflight"),
            "script should advertise the preflight mode"
        );
        assert!(
            script.contains("scripts/combined-family-history-slice-fixture.sh command"),
            "script should advertise the command mode"
        );
        assert!(
            script.contains("scripts/combined-family-history-slice-fixture.sh record"),
            "script should advertise the record mode"
        );
        assert!(
            script.contains("record-substreams"),
            "script should drive the record-substreams command"
        );
        assert!(
            script.contains("--print-request"),
            "script should expose the no-network preflight path"
        );
        assert!(
            script.contains(&format!("--family {}", spec.family_name)),
            "script should stay anchored to the checked-in combined family"
        );
        assert!(
            script.contains(&format!("TYCHO_COMBINED_FIXTURE_START_BLOCK:-{}", spec.start_block)),
            "script start-block default should stay aligned with the capture spec"
        );
        assert!(
            script.contains(&format!("TYCHO_COMBINED_FIXTURE_STOP_BLOCK:-{}", spec.stop_block)),
            "script stop-block default should stay aligned with the capture spec"
        );
        assert!(
            script.contains(
                "crates/tycho-indexer/tests/fixtures/combined_family_real_history_slice.json"
            ),
            "script output path should stay aligned with the capture spec fixture"
        );
        assert!(
            script.contains(&extractors_config_relative),
            "script extractor config should stay aligned with the repo combined config"
        );
    }
}
