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
    process,
    str::FromStr,
    sync::{mpsc, Arc},
};

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anyhow::anyhow;
use chrono::{NaiveDateTime, Utc};
use clap::Parser;
use futures03::future::select_all;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::{runtime::Handle, select, task::JoinHandle};
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
#[cfg(test)]
use tycho_ethereum::{
    rpc::EthereumRpcClient, services::token_pre_processor::EthereumTokenPreProcessor,
};
#[cfg(test)]
use tycho_indexer::extractor::family_runtime_metadata::FamilyRuntimeConfig;
#[cfg(test)]
use tycho_indexer::extractor::{
    chain_state::ChainState, control::ExtractorHandle, runner::ManagedRunner,
};
#[cfg(test)]
use tycho_indexer::services::ServicesBuilder;
use tycho_indexer::{
    cli::{
        AnalyzeTokenArgs, Cli, Command, GlobalArgs, IndexArgs, RecordSubstreamsArgs, RunSpkgArgs,
        SubstreamsArgs,
    },
    extractor::{
        extractor_config::{DCIType, ExtractorConfig, ProtocolTypeConfig},
        token_analysis_cron::analyze_tokens,
        ExtractionError,
    },
};
use tycho_storage::postgres::builder::GatewayBuilder;

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
    record_substreams_fixture_from_package_and_recorder, render_record_substreams_request_json,
    resolve_record_substreams_request, resolve_record_substreams_request_with_registry,
    SubstreamsFixtureRecorder,
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
fn repo_combined_family_output_module(family_name: &str) -> String {
    crate::testing::family_output_module_for_tests(family_name)
}

#[tokio::main]
async fn run_rpc(global_args: GlobalArgs) -> Result<(), ExtractionError> {
    create_tracing_subscriber();

    let rpc_client = global_args.rpc.build_client()?;
    let launch_config = config::ResolvedServiceLaunchConfig::from_runtime_args(
        &global_args.server_version_prefix,
        &global_args.server_ip,
        global_args.server_port,
    )?;

    let direct_gw = GatewayBuilder::new(&global_args.database_url)
        .set_chains(&[Chain::Ethereum]) // TODO: handle multichain
        .build_direct_gw()
        .await?;

    info!("Starting Tycho RPC");
    let managed_server = launch_config
        .start_managed_server(
            &config::ResolvedIndexerServiceConfig::empty(),
            direct_gw,
            rpc_client,
            vec![],
            None,
        )
        .await?;
    info!(server_url = managed_server.server_url, "Http and Ws server started");
    let (res, _, _) = select_all([managed_server.server_task, managed_server.shutdown_task]).await;
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
    let launch_config = config::ResolvedServiceLaunchConfig::from_runtime_args(
        &global_args.server_version_prefix,
        &global_args.server_ip,
        global_args.server_port,
    )?;

    let runtime_plan = extractors_config
        .resolved_indexer_runtime_plan()
        .map_err(|e| ExtractionError::Setup(format!("Failed to resolve runtime targets: {e}")))?;
    let family_runtime_registry = runtime_plan.family_runtime_registry();

    let managed_indexer = launch_config
        .start_indexing_runtime_plan(
            runtime_plan,
            rpc_client,
            config::ResolvedIndexerTaskContext {
                database_url: &global_args.database_url,
                chains,
                retention_horizon,
                endpoint_url: &global_args.endpoint_url,
                s3_bucket: global_args.s3_bucket.as_deref(),
                substreams_api_token: &substreams_args.substreams_api_token,
                database_insert_batch_size: global_args.database_insert_batch_size,
                settlement_contract,
                extraction_runtime: extraction_runtime.cloned(),
                partial_blocks: substreams_args.enable_partial_blocks,
                family_runtime_registry,
            },
        )
        .await
        .map_err(|e| ExtractionError::Setup(format!("Failed to create extractors: {e}")))?;

    Ok((managed_indexer.extraction_tasks, managed_indexer.service_tasks))
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
        build_all_extractors_for_tests, family_block_response,
        family_block_response_from_block_changes,
        future_family_runtime_registry_for_record_substreams_tests,
        future_family_runtime_registry_for_record_substreams_tests_with_durability_scope,
        repo_combined_family_bootstrap_pool_seeds_for_tests, scripted_session_response,
        scripted_undo_response, seed_repo_runtime_target_shared_bootstrap_universe_for_tests,
        shared_bootstrap_seed_universe_spec_from_config_path_with_registry_for_tests,
        swap_extractor_config_for_tests, unique_test_suffix as test_unique_suffix,
        uniswap_family_durability_scope_for_tests as test_family_durability_scope,
        uniswap_family_protocol_systems_for_tests as test_family_protocol_systems,
        uniswap_family_runtime_config_for_tests as test_family_runtime_config,
        uniswap_family_shared_module_for_tests as test_family_shared_module, v2_pair_created_block,
        v3_pool_created_block, write_record_substreams_future_family_fixture_inputs,
        write_record_substreams_future_family_fixture_inputs_with_registry,
        write_temp_substreams_package_for_tests as test_family_shared_spkg_path,
        write_uniswap_family_defaults_config_for_tests as test_family_defaults_config,
        write_uniswap_family_defaults_config_with_member_names_for_tests as test_family_defaults_config_with_member_names,
        write_uniswap_family_defaults_config_with_shared_bootstrap,
    };
    use alloy::primitives::Address as AlloyAddress;
    use once_cell::sync::Lazy;
    use prost::Message;
    use substreams::store::StoreGet;
    use tycho_storage::postgres::cache::CachedGateway;
    use tycho_storage::postgres::testing::run_against_db;

    use super::*;
    static RPC: Lazy<EthereumRpcClient> = Lazy::new(|| {
        let rpc_url = std::env::var("RPC_URL").expect("RPC URL must be set for testing");
        EthereumRpcClient::new(&rpc_url).expect("Failed to create RPC client")
    });

    #[allow(clippy::too_many_arguments)]
    async fn build_all_extractors(
        config: &ExtractorConfigs,
        chain_state: ChainState,
        _chains: &[Chain],
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
        build_all_extractors_for_tests(
            config,
            crate::testing::BuildExtractorsTestContext {
                chain_state,
                endpoint_url,
                s3_bucket,
                substreams_api_token,
                cached_gw,
                database_insert_batch_size,
                token_pre_processor,
                rpc_client,
                runtime,
                partial_blocks,
                family_runtime_registry:
                    tycho_indexer::extractor::family_registry::default_family_runtime_registry(),
            },
        )
        .await
    }

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
                    swap_extractor_config_for_tests(
                        "uniswap_v2",
                        "uniswap_v2",
                        chain,
                        ImplementationType::Custom,
                        42,
                        "uniswap_v2_pool",
                        missing_member_v2_spkg,
                        "v2_map_pool_events",
                        Some(FamilyRuntimeConfig {
                            family: "uniswap".to_string(),
                            shared_spkg: Some(shared_spkg_path.clone()),
                            shared_module: Some(test_family_shared_module()),
                            durability_scope: Some(test_family_durability_scope()),
                        }),
                    ),
                ),
                (
                    "uniswap_v3".to_string(),
                    swap_extractor_config_for_tests(
                        "uniswap_v3",
                        "uniswap_v3",
                        chain,
                        ImplementationType::Custom,
                        42,
                        "uniswap_v3_pool",
                        missing_member_v3_spkg,
                        "v3_map_protocol_changes",
                        Some(FamilyRuntimeConfig {
                            family: "uniswap".to_string(),
                            shared_spkg: Some(shared_spkg_path.clone()),
                            shared_module: Some(test_family_shared_module()),
                            durability_scope: Some(test_family_durability_scope()),
                        }),
                    ),
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
            assert_eq!(
                runners[0].kind(),
                tycho_indexer::extractor::runner::ManagedRunnerKind::Family
            );
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
            assert_eq!(
                runners[0].kind(),
                tycho_indexer::extractor::runner::ManagedRunnerKind::Family
            );
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
                bootstrap_path
                    .to_str()
                    .expect("utf8 bootstrap path"),
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
            assert_eq!(
                runners[0].kind(),
                tycho_indexer::extractor::runner::ManagedRunnerKind::Family
            );
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
                    test_family_durability_scope(),
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
                .get_state(&test_family_durability_scope(), &chain)
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
    async fn combined_family_runner_alias_members_fresh_start_from_completed_shared_bootstrap() {
        use serde_json::json;
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
                number: 42,
                chain,
                hash: Bytes::from(vec![0x77; 32]),
                parent_hash: Bytes::from(vec![0x66; 32]),
                ts: chrono::NaiveDateTime::default(),
            };
            cached_gw
                .start_transaction(&persisted_block, Some("seed-family-shared-bootstrap-complete"))
                .await;
            cached_gw
                .upsert_block(std::slice::from_ref(&persisted_block))
                .await
                .expect("persist bootstrap marker block");
            cached_gw
                .save_state(&ExtractionState::new(
                    test_family_durability_scope(),
                    chain,
                    None,
                    b"bootstrap@42",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist shared family bootstrap marker state");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit shared family bootstrap marker state");

            cached_gw
                .start_transaction(&persisted_block, Some("seed-family-shared-bootstrap-state"))
                .await;
            cached_gw
                .save_state(&ExtractionState::new(
                    format!("{}::bootstrap", test_family_durability_scope()),
                    chain,
                    Some(json!({
                        "bootstrap_block": 42,
                        "completed": true,
                    })),
                    b"bootstrap_completed",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist shared family bootstrap completion state");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit shared family bootstrap completion state");

            let bootstrap_state = cached_gw
                .get_state(&format!("{}::bootstrap", test_family_durability_scope()), &chain)
                .await
                .expect("read back shared family bootstrap completion state");
            assert_eq!(bootstrap_state.block_hash, persisted_block.hash);

            let shared_spkg_path =
                test_family_shared_spkg_path("combined-family-alias-bootstrap-complete");
            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config_with_member_names(
                "tycho-indexer-family-defaults-aliased-bootstrap-complete",
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
                    .expect("utf8 aliased family-default bootstrap config path"),
            )
            .expect("load aliased family-default bootstrap config");

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
            .expect("build aliased combined extractors from completed shared bootstrap");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners.pop().expect("family runner present");
            runner.run().await.unwrap().unwrap();

            let requests = captured.lock().unwrap();
            assert_eq!(requests.len(), 1, "expected a single substreams request");
            assert_eq!(
                requests[0].start_block_num, 43,
                "completed shared bootstrap should fresh-start the shared family at bootstrap block + 1"
            );
            assert!(
                requests[0].start_cursor.is_empty(),
                "bootstrap marker fresh start should not send a stream cursor even when extractor names are aliases"
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_rejects_legacy_extractor_scoped_resume_state_under_shared_durability(
    ) {
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
                hash: Bytes::from(vec![0x23; 32]),
                parent_hash: Bytes::from(vec![0x22; 32]),
                ts: chrono::NaiveDateTime::default(),
            };
            cached_gw
                .start_transaction(&persisted_block, Some("seed-legacy-alias-resume-state"))
                .await;
            cached_gw
                .upsert_block(std::slice::from_ref(&persisted_block))
                .await
                .expect("persist legacy resume block");
            for extractor_name in ["uniswap_v2_alias", "uniswap_v3_alias"] {
                cached_gw
                    .save_state(&ExtractionState::new(
                        extractor_name.to_string(),
                        chain,
                        None,
                        b"cursor@123-legacy-extractor",
                        persisted_block.hash.clone(),
                    ))
                    .await
                    .expect("persist legacy extractor-scoped resume state");
            }
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit seeded legacy extractor-scoped resume state");

            let shared_spkg_path =
                test_family_shared_spkg_path("combined-family-legacy-fallback-reject");
            let unique = test_unique_suffix();
            let config_path = test_family_defaults_config_with_member_names(
                "tycho-indexer-family-defaults-legacy-fallback-reject",
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

            let err = match build_all_extractors(
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
            .await {
                Ok(_) => panic!(
                    "shared-family startup should reject legacy extractor-scoped resume state"
                ),
                Err(err) => err,
            };
            let err_text = err.to_string();
            assert!(
                err_text.contains("legacy extractor-scoped fallback cursor state"),
                "unexpected error: {err_text}"
            );

            let requests = captured.lock().unwrap();
            assert!(
                requests.is_empty(),
                "shared-family startup should reject legacy fallback before opening a shared stream"
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_persists_dynamically_admitted_component() {
        use tycho_common::models::token::Token;
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
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
                    .await
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
        use tycho_common::models::token::Token;
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
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
            .await
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
        use tycho_common::models::token::Token;
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
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
                    .await
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
        use tycho_common::models::token::Token;
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
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
                    .await
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

    #[derive(Clone, Debug)]
    struct RealHistorySliceExpectation {
        v2_component_id: String,
        v2_reserve0: Bytes,
        v2_reserve1: Bytes,
        v3_component_id: String,
        v3_tick: Bytes,
    }

    #[derive(Clone, Debug, Default)]
    struct PartialRealHistorySliceExpectation {
        v2_component_id: Option<String>,
        v2_reserve0: Option<Bytes>,
        v2_reserve1: Option<Bytes>,
        v3_component_id: Option<String>,
        v3_tick: Option<Bytes>,
    }

    #[test]
    fn repo_combined_family_bootstrap_pool_seeds_are_derived_from_repo_config() {
        let seeds = repo_combined_family_bootstrap_pool_seeds_for_tests(
            "extractors.uniswap_v2_v3.combined.yaml",
        );

        assert!(
            !seeds.is_empty(),
            "repo combined family bootstrap seed extraction should produce at least one pool"
        );
        assert!(
            seeds
                .iter()
                .any(|seed| seed.protocol_system == "uniswap_v2"
                    && seed.protocol_type_name == "uniswap_v2_pool"),
            "repo combined family bootstrap seed extraction should include uniswap_v2 pools"
        );
        assert!(
            seeds
                .iter()
                .any(|seed| seed.protocol_system == "uniswap_v3"
                    && seed.protocol_type_name == "uniswap_v3_pool"),
            "repo combined family bootstrap seed extraction should include uniswap_v3 pools"
        );
    }

    #[test]
    fn shared_bootstrap_seed_universe_spec_supports_non_uniswap_family_registry() {
        let spkg_path = std::env::temp_dir()
            .join(format!("future-family-bootstrap-seed-spec-{}.spkg", process::id()));
        std::fs::write(&spkg_path, b"future-family-bootstrap-seed-spec")
            .expect("write temp future family spkg");
        let config_path = write_record_substreams_future_family_fixture_inputs(&spkg_path);

        let seed_spec =
            shared_bootstrap_seed_universe_spec_from_config_path_with_registry_for_tests(
                &config_path,
                future_family_runtime_registry_for_record_substreams_tests(),
            );

        assert_eq!(seed_spec.chain, Chain::Ethereum);
        assert_eq!(seed_spec.protocol_types.len(), 2);
        assert!(
            seed_spec
                .protocol_types
                .iter()
                .any(|protocol_type| protocol_type.name == "future_v1_pool"),
            "future family seed extraction should keep future_v1 protocol type metadata"
        );
        assert!(
            seed_spec
                .protocol_types
                .iter()
                .any(|protocol_type| protocol_type.name == "future_v2_pool"),
            "future family seed extraction should keep future_v2 protocol type metadata"
        );
        assert!(
            seed_spec
                .pools
                .iter()
                .any(|seed| seed.protocol_system == "future_v1"
                    && seed.protocol_type_name == "future_v1_pool"
                    && seed.component_id == "0x00000000000000000000000000000000000000a1"),
            "future family seed extraction should include future_v1 pool ownership"
        );
        assert!(
            seed_spec
                .pools
                .iter()
                .any(|seed| seed.protocol_system == "future_v2"
                    && seed.protocol_type_name == "future_v2_pool"
                    && seed.component_id == "0x00000000000000000000000000000000000000b2"),
            "future family seed extraction should include future_v2 pool ownership"
        );

        let _ = std::fs::remove_file(&spkg_path);
    }

    #[tokio::test]
    async fn custom_registry_builds_future_family_managed_runner_and_starts_one_shared_stream() {
        use tycho_common::{
            models::ExtractionState,
            storage::{ChainGateway, ExtractionStateGateway},
        };
        use tycho_indexer::extractor::protocol_extractor::ExtractorGateway;
        use tycho_indexer::substreams::mock::start_mock_substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let family_scope = format!(
                "{}::{}",
                crate::testing::family_durability_scope_for_tests("future_swap"),
                test_unique_suffix()
            );
            let registry =
                future_family_runtime_registry_for_record_substreams_tests_with_durability_scope(
                    family_scope.clone(),
                );
            let (captured, addr) = start_mock_substreams().await;
            let spkg_path = std::env::temp_dir()
                .join(format!("future-family-managed-runner-{}.spkg", process::id()));
            std::fs::write(
                &spkg_path,
                tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
            )
            .expect("write temp future family spkg");
            let config_path = write_record_substreams_future_family_fixture_inputs_with_registry(
                &spkg_path, registry,
            );
            let config = ExtractorConfigs::from_yaml_with_registry(
                config_path
                    .to_str()
                    .expect("utf8 config path"),
                registry,
            )
            .expect("load future family config through custom registry");

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&["future_v1".to_string(), "future_v2".to_string()])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let persisted_block = Block {
                number: 99,
                chain,
                hash: Bytes::from(vec![0x63; 32]),
                parent_hash: Bytes::from(vec![0x62; 32]),
                ts: chrono::NaiveDateTime::default(),
            };
            let bootstrap_gateway =
                tycho_indexer::extractor::protocol_extractor::ExtractorPgGateway::new(
                    "future_v1",
                    chain,
                    1000,
                    cached_gw.clone(),
                    Some(family_scope.clone()),
                );
            cached_gw
                .start_transaction(&persisted_block, Some("seed-future-family-bootstrap-progress"))
                .await;
            cached_gw
                .upsert_block(std::slice::from_ref(&persisted_block))
                .await
                .expect("persist future family bootstrap marker block");
            cached_gw
                .save_state(&ExtractionState::new(
                    family_scope.clone(),
                    chain,
                    None,
                    b"bootstrap@99",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist future family bootstrap marker cursor");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit future family bootstrap marker cursor state");
            bootstrap_gateway
                .save_bootstrap_state(99, persisted_block.hash.clone())
                .await
                .expect("persist future family bootstrap completion state");

            let (mut runners, handles) = build_all_extractors_for_tests(
                &config,
                crate::testing::BuildExtractorsTestContext {
                    chain_state: ChainState::default(),
                    endpoint_url: &format!("http://{addr}"),
                    s3_bucket: None,
                    substreams_api_token: "",
                    cached_gw: &cached_gw,
                    database_insert_batch_size: 1000,
                    token_pre_processor: &token_processor,
                    rpc_client: &rpc,
                    runtime: None,
                    partial_blocks: false,
                    family_runtime_registry: registry,
                },
            )
            .await
            .expect("build future family runtime through custom registry");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("future family runner present");
            runner.run().await.unwrap().unwrap();

            let requests = captured.lock().unwrap();
            assert_eq!(requests.len(), 1, "expected a single substreams request");
            assert_eq!(requests[0].start_block_num, 100);
            assert!(
                requests[0].start_cursor.is_empty(),
                "bootstrap marker resume should not send a stream cursor on fresh startup"
            );
            assert_eq!(requests[0].output_module, "map_future_swap_family_protocol_changes");

            let _ = std::fs::remove_file(&spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    #[tokio::test]
    async fn custom_registry_resumes_future_family_from_persisted_shared_cursor() {
        use tycho_common::storage::{ChainGateway, ExtractionStateGateway};
        use tycho_indexer::substreams::mock::start_mock_substreams;

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let family_scope = format!(
                "{}::{}",
                crate::testing::family_durability_scope_for_tests("future_swap"),
                test_unique_suffix()
            );
            let registry =
                future_family_runtime_registry_for_record_substreams_tests_with_durability_scope(
                    family_scope.clone(),
                );
            let (captured, addr) = start_mock_substreams().await;
            let spkg_path = std::env::temp_dir()
                .join(format!("future-family-managed-resume-{}.spkg", process::id()));
            std::fs::write(
                &spkg_path,
                tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
            )
            .expect("write temp future family spkg");
            let config_path = write_record_substreams_future_family_fixture_inputs_with_registry(
                &spkg_path, registry,
            );
            let config = ExtractorConfigs::from_yaml_with_registry(
                config_path
                    .to_str()
                    .expect("utf8 config path"),
                registry,
            )
            .expect("load future family config through custom registry");

            let (cached_gw, _) = GatewayBuilder::new(db_url.as_str())
                .set_chains(&[chain])
                .set_protocol_systems(&["future_v1".to_string(), "future_v2".to_string()])
                .build()
                .await
                .expect("Failed to create Gateway");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);

            let persisted_block = Block {
                number: 123,
                chain,
                hash: Bytes::from(vec![0x7b; 32]),
                parent_hash: Bytes::from(vec![0x7a; 32]),
                ts: chrono::NaiveDateTime::default(),
            };
            cached_gw
                .start_transaction(&persisted_block, Some("seed-future-family-resume-progress"))
                .await;
            cached_gw
                .upsert_block(std::slice::from_ref(&persisted_block))
                .await
                .expect("persist future family resumed block");
            cached_gw
                .save_state(&ExtractionState::new(
                    family_scope.clone(),
                    chain,
                    None,
                    b"cursor@123-future-shared",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist future family shared extraction state");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("commit future family resumed extraction state");

            let (mut runners, handles) = build_all_extractors_for_tests(
                &config,
                crate::testing::BuildExtractorsTestContext {
                    chain_state: ChainState::default(),
                    endpoint_url: &format!("http://{addr}"),
                    s3_bucket: None,
                    substreams_api_token: "",
                    cached_gw: &cached_gw,
                    database_insert_batch_size: 1000,
                    token_pre_processor: &token_processor,
                    rpc_client: &rpc,
                    runtime: None,
                    partial_blocks: false,
                    family_runtime_registry: registry,
                },
            )
            .await
            .expect("build future family runtime from persisted shared cursor");

            assert_eq!(runners.len(), 1);
            assert_eq!(handles.len(), 2);

            let runner = runners
                .pop()
                .expect("future family runner present");
            runner.run().await.unwrap().unwrap();

            let requests = captured.lock().unwrap();
            assert_eq!(requests.len(), 1, "expected a single substreams request");
            assert_eq!(requests[0].start_block_num, 124);
            assert_eq!(requests[0].start_cursor, "cursor@123-future-shared");
            assert_eq!(requests[0].output_module, "map_future_swap_family_protocol_changes");

            let _ = std::fs::remove_file(&spkg_path);
            let _ = std::fs::remove_file(&config_path);
        })
        .await;
    }

    fn update_partial_real_history_slice_expectation(
        response: &tycho_indexer::pb::sf::substreams::rpc::v2::Response,
        known_component_systems: &mut HashMap<String, std::collections::HashSet<String>>,
        partial: &mut PartialRealHistorySliceExpectation,
    ) {
        use prost::Message;
        use tycho_indexer::pb::sf::substreams::rpc::v2::response::Message as ResponseMessage;
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        let Some(ResponseMessage::BlockScopedData(block_scoped_data)) = response.message.as_ref()
        else {
            return;
        };
        let output = block_scoped_data
            .output
            .as_ref()
            .and_then(|output| output.map_output.as_ref())
            .expect("fixture block-scoped data should include map output");
        let block_changes = substreams::BlockChanges::decode(output.value.as_slice())
            .expect("decode fixture block changes");

        for tx_changes in block_changes.changes {
            for component_change in &tx_changes.component_changes {
                let protocol_system = match component_change
                    .protocol_type
                    .as_ref()
                    .map(|protocol_type| protocol_type.name.as_str())
                {
                    Some("uniswap_v2_pool") => "uniswap_v2",
                    Some("uniswap_v3_pool") => "uniswap_v3",
                    _ => continue,
                };
                known_component_systems
                    .entry(protocol_system.to_string())
                    .or_default()
                    .insert(component_change.id.clone());
            }

            for entity_change in &tx_changes.entity_changes {
                let attrs = entity_change
                    .attributes
                    .iter()
                    .map(|attribute| {
                        (attribute.name.as_str(), Bytes::from(attribute.value.clone()))
                    })
                    .collect::<HashMap<_, _>>();

                if known_component_systems
                    .get("uniswap_v2")
                    .is_some_and(|components| components.contains(&entity_change.component_id))
                {
                    if let (Some(reserve0), Some(reserve1)) =
                        (attrs.get("reserve0"), attrs.get("reserve1"))
                    {
                        partial.v2_component_id = Some(entity_change.component_id.clone());
                        partial.v2_reserve0 = Some(reserve0.clone());
                        partial.v2_reserve1 = Some(reserve1.clone());
                    }
                }

                if known_component_systems
                    .get("uniswap_v3")
                    .is_some_and(|components| components.contains(&entity_change.component_id))
                {
                    if partial.v3_component_id.is_none() {
                        partial.v3_component_id = Some(entity_change.component_id.clone());
                    }
                    if let Some(tick) = attrs.get("tick") {
                        partial.v3_component_id = Some(entity_change.component_id.clone());
                        partial.v3_tick = Some(tick.clone());
                    }
                }
            }
        }
    }

    fn partial_combined_family_real_history_slice_expectation_from_scripts(
        scripts: &[tycho_indexer::substreams::mock::MockSubstreamsScript],
        seeded_component_ids: &HashMap<String, std::collections::HashSet<String>>,
    ) -> PartialRealHistorySliceExpectation {
        let mut known_component_systems = seeded_component_ids.clone();
        let mut partial = PartialRealHistorySliceExpectation::default();

        for script in scripts {
            for response in &script.responses {
                update_partial_real_history_slice_expectation(
                    response,
                    &mut known_component_systems,
                    &mut partial,
                );
            }
        }

        partial
    }

    fn combined_family_real_history_slice_expectation_from_scripts(
        scripts: &[tycho_indexer::substreams::mock::MockSubstreamsScript],
        seeded_component_ids: &HashMap<String, std::collections::HashSet<String>>,
    ) -> RealHistorySliceExpectation {
        let partial = partial_combined_family_real_history_slice_expectation_from_scripts(
            scripts,
            seeded_component_ids,
        );

        RealHistorySliceExpectation {
            v2_component_id: partial
                .v2_component_id
                .expect("fixture should update one seeded V2 component"),
            v2_reserve0: partial
                .v2_reserve0
                .expect("fixture should update V2 reserve0"),
            v2_reserve1: partial
                .v2_reserve1
                .expect("fixture should update V2 reserve1"),
            v3_component_id: partial
                .v3_component_id
                .expect("fixture should update one seeded V3 component"),
            v3_tick: partial
                .v3_tick
                .expect("fixture should update V3 tick"),
        }
    }

    fn combined_family_real_history_slice_expectation(
        seeded_component_ids: &HashMap<String, std::collections::HashSet<String>>,
    ) -> RealHistorySliceExpectation {
        let fixture_path = combined_family_real_history_slice_fixture_path();
        let fixture_scripts =
            tycho_indexer::substreams::mock::read_mock_substreams_fixture(&fixture_path)
                .expect("read committed history-slice fixture");
        combined_family_real_history_slice_expectation_from_scripts(
            &fixture_scripts,
            seeded_component_ids,
        )
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
        seeded_component_ids: &HashMap<String, std::collections::HashSet<String>>,
    ) -> (
        tycho_indexer::substreams::mock::MockSubstreamsScript,
        tycho_indexer::substreams::mock::MockSubstreamsScript,
    ) {
        fn split_script_at_first_resumed_block(
            script: tycho_indexer::substreams::mock::MockSubstreamsScript,
            seeded_component_ids: &HashMap<String, std::collections::HashSet<String>>,
        ) -> (
            tycho_indexer::substreams::mock::MockSubstreamsScript,
            tycho_indexer::substreams::mock::MockSubstreamsScript,
        ) {
            use tycho_indexer::pb::sf::substreams::rpc::v2::response::Message as ResponseMessage;

            let mut known_component_systems = seeded_component_ids.clone();
            let mut partial = PartialRealHistorySliceExpectation::default();
            let mut resume_response_index = None;
            for (idx, response) in script.responses.iter().enumerate() {
                update_partial_real_history_slice_expectation(
                    response,
                    &mut known_component_systems,
                    &mut partial,
                );
                let reached_first_run_milestone = partial.v2_component_id.is_some()
                    && partial.v2_reserve0.is_some()
                    && partial.v2_reserve1.is_some()
                    && partial.v3_component_id.is_some();
                if reached_first_run_milestone {
                    resume_response_index = script
                        .responses
                        .iter()
                        .enumerate()
                        .skip(idx + 1)
                        .find_map(|(next_idx, next_response)| {
                            matches!(
                                next_response.message.as_ref(),
                                Some(ResponseMessage::BlockScopedData(_))
                            )
                            .then_some(next_idx)
                        });
                    break;
                }
            }
            let resume_response_index = resume_response_index.expect(
                "history-slice restart split should find a resumed block after the first-run milestone",
            );
            let mut split = tycho_indexer::substreams::mock::split_mock_substreams_script(
                &script,
                &[0..resume_response_index, resume_response_index..script.responses.len()],
            )
            .expect("split history-slice script at resumed block boundary");
            assert_eq!(split.len(), 2, "expected exactly two restart script segments");
            let first = split.remove(0);
            let second = split.remove(0);
            (first, second)
        }

        let (first, mut second) = if use_fixture {
            let fixture_path = combined_family_real_history_slice_fixture_path();
            assert!(fixture_path.exists(), "expected fixture at {}", fixture_path.display());
            let mut scripts =
                tycho_indexer::substreams::mock::read_mock_substreams_fixture(&fixture_path)
                    .expect("read committed history-slice fixture for restart");
            let script = scripts
                .pop()
                .expect("fixture-backed history slice should contain one scripted session");
            split_script_at_first_resumed_block(script, seeded_component_ids)
        } else {
            let mut scripts = combined_family_real_history_slice_scripts();
            let script = scripts
                .pop()
                .expect("real history slice should produce one scripted session");
            split_script_at_first_resumed_block(script, seeded_component_ids)
        };
        let resumed_start_block = second
            .responses
            .iter()
            .find_map(|response| match response.message.as_ref() {
                Some(
                    tycho_indexer::pb::sf::substreams::rpc::v2::response::Message::BlockScopedData(
                        block,
                    ),
                ) => Some(block.final_block_height),
                _ => None,
            })
            .expect("restart second script should include a resumed block");
        second.responses.insert(
            0,
            scripted_session_response("trace-real-history-slice-restart", resumed_start_block),
        );

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
            let seeded_component_ids = if use_fixture {
                seed_repo_runtime_target_shared_bootstrap_universe_for_tests(
                    &direct_gw,
                    "extractors.uniswap_v2_v3.combined.yaml",
                )
                .await
            } else {
                HashMap::new()
            };
            let expectation = if use_fixture {
                Some(combined_family_real_history_slice_expectation(&seeded_component_ids))
            } else {
                None
            };

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
            let expected_v2_component_id = expectation
                .as_ref()
                .map(|expectation| expectation.v2_component_id.clone())
                .unwrap_or_else(|| "0x4545454545454545454545454545454545454545".to_string());
            assert!(
                v2_components
                    .entity
                    .iter()
                    .any(|component| component.id == expected_v2_component_id),
                "expected V2 dynamic component from real history slice to be visible"
            );

            let v3_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read combined V3 component universe after history slice");
            let expected_v3_component_id = expectation
                .as_ref()
                .map(|expectation| expectation.v3_component_id.clone())
                .unwrap_or_else(|| "0x4646464646464646464646464646464646464646".to_string());
            assert!(
                v3_components
                    .entity
                    .iter()
                    .any(|component| component.id == expected_v3_component_id),
                "expected V3 dynamic component from real history slice to be visible"
            );

            let v2_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v2".to_string()),
                    Some(&[expected_v2_component_id.as_str()]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic V2 state after history slice");
            assert_eq!(v2_state.entity.len(), 1);
            let expected_v2_reserve0 = expectation
                .as_ref()
                .map(|expectation| expectation.v2_reserve0.clone())
                .unwrap_or_else(|| Bytes::from(vec![0x07, 0xd0]));
            assert_eq!(
                v2_state.entity[0]
                    .attributes
                    .get("reserve0"),
                Some(&expected_v2_reserve0)
            );
            let expected_v2_reserve1 = expectation
                .as_ref()
                .map(|expectation| expectation.v2_reserve1.clone())
                .unwrap_or_else(|| Bytes::from(vec![0x0b, 0xb8]));
            assert_eq!(
                v2_state.entity[0]
                    .attributes
                    .get("reserve1"),
                Some(&expected_v2_reserve1)
            );

            let v3_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[expected_v3_component_id.as_str()]),
                    false,
                    None,
                )
                .await
                .expect("read dynamic V3 state after history slice");
            assert_eq!(v3_state.entity.len(), 1);
            let expected_v3_tick = expectation
                .as_ref()
                .map(|expectation| expectation.v3_tick.clone())
                .unwrap_or_else(|| Bytes::from(vec![0x07]));
            assert_eq!(
                v3_state.entity[0]
                    .attributes
                    .get("tick"),
                Some(&expected_v3_tick)
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
                    .await
                    .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();

            let assert_component_visible_through_rpc =
                |protocol_system: &'static str, component_id: String| {
                    let client = client.clone();
                    async move {
                        let mut rpc_body = None;
                        for _ in 0..100 {
                            let response = match client
                            .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                            .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                                protocol_system,
                                vec![component_id.clone()],
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
                 component_id: String,
                 expected_attribute: &'static str,
                 expected_value: Bytes| {
                    let client = client.clone();
                    async move {
                        let mut state_body = None;
                        for _ in 0..100 {
                            let response = match client
                            .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_state"))
                            .json(&dto::ProtocolStateRequestBody {
                                protocol_ids: Some(vec![component_id.clone()]),
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

            assert_component_visible_through_rpc("uniswap_v2", expected_v2_component_id.clone())
                .await;
            assert_component_visible_through_rpc("uniswap_v3", expected_v3_component_id.clone())
                .await;
            assert_state_visible_through_rpc(
                "uniswap_v2",
                expected_v2_component_id.clone(),
                "reserve0",
                expected_v2_reserve0,
            )
            .await;
            assert_state_visible_through_rpc(
                "uniswap_v3",
                expected_v3_component_id.clone(),
                "tick",
                expected_v3_tick,
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
        use tycho_indexer::pb::sf::substreams::rpc::v2::response::Message as ResponseMessage;

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
            block_heights
                .iter()
                .any(|height| *height > start_block),
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
            let seeded_component_ids = if use_fixture {
                seed_repo_runtime_target_shared_bootstrap_universe_for_tests(
                    &direct_gw,
                    "extractors.uniswap_v2_v3.combined.yaml",
                )
                .await
            } else {
                HashMap::new()
            };
            let (first_script, second_script) =
                split_combined_family_real_history_slice_scripts_for_restart(
                    use_fixture,
                    &seeded_component_ids,
                );
            let first_run_scripts = vec![first_script.clone()];
            let full_run_scripts = vec![first_script.clone(), second_script.clone()];
            let resumed_start_block = second_script
                .responses
                .iter()
                .find_map(|response| match response.message.as_ref() {
                    Some(tycho_indexer::pb::sf::substreams::rpc::v2::response::Message::BlockScopedData(
                        block,
                    )) => Some(block.final_block_height),
                    _ => None,
                })
                .expect("restart second script should include a resumed block");
            let resumed_cursor = first_script
                .responses
                .iter()
                .rev()
                .find_map(|response| match response.message.as_ref() {
                    Some(tycho_indexer::pb::sf::substreams::rpc::v2::response::Message::BlockScopedData(
                        block,
                    )) => Some(block.cursor.clone()),
                    _ => None,
                })
                .expect("restart first script should include a persisted cursor");

            let (captured_first, addr_first) =
                start_scripted_mock_substreams(vec![first_script]).await;
            let first_run_expectation = if use_fixture {
                Some(partial_combined_family_real_history_slice_expectation_from_scripts(
                    &first_run_scripts,
                    &seeded_component_ids,
                ))
            } else {
                None
            };
            let final_expectation = if use_fixture {
                Some(combined_family_real_history_slice_expectation_from_scripts(
                    &full_run_scripts,
                    &seeded_component_ids,
                ))
            } else {
                None
            };
            let expected_v2_component_id = first_run_expectation
                .as_ref()
                .and_then(|expectation| expectation.v2_component_id.clone())
                .unwrap_or_else(|| "0x4545454545454545454545454545454545454545".to_string());
            let expected_v3_component_id = first_run_expectation
                .as_ref()
                .and_then(|expectation| expectation.v3_component_id.clone())
                .unwrap_or_else(|| "0x4646464646464646464646464646464646464646".to_string());
            let expected_final_v3_component_id = final_expectation
                .as_ref()
                .map(|expectation| expectation.v3_component_id.clone())
                .unwrap_or_else(|| expected_v3_component_id.clone());

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
                    Some(&[expected_v2_component_id.as_str()]),
                    false,
                    None,
                )
                .await
                .expect("read V2 state before restart");
            if let Some(expected_v2_reserve0) = first_run_expectation
                .as_ref()
                .and_then(|expectation| expectation.v2_reserve0.clone())
            {
                assert_eq!(after_first_v2_state.entity.len(), 1);
                assert_eq!(
                    after_first_v2_state.entity[0].attributes.get("reserve0"),
                    Some(&expected_v2_reserve0)
                );
            }

            let after_first_v3_components = direct_gw
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read V3 component universe before restart");
            assert!(
                after_first_v3_components
                    .entity
                    .iter()
                    .any(|component| component.id == expected_v3_component_id),
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
                assert_eq!(requests[0].start_block_num, resumed_start_block as i64);
                assert_eq!(
                    requests[0].start_cursor, resumed_cursor,
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
                    .any(|component| component.id == expected_v2_component_id),
                "V2 component should remain queryable after restart"
            );
            let after_restart_v3_state = direct_gw
                .get_protocol_states(
                    &chain,
                    None,
                    Some("uniswap_v3".to_string()),
                    Some(&[expected_final_v3_component_id.as_str()]),
                    false,
                    None,
                )
                .await
                .expect("read V3 state after restart follow-up");
            assert_eq!(after_restart_v3_state.entity.len(), 1);
            let expected_v3_tick = final_expectation
                .as_ref()
                .map(|expectation| expectation.v3_tick.clone())
                .unwrap_or_else(|| Bytes::from(vec![0x07]));
            assert_eq!(
                after_restart_v3_state.entity[0].attributes.get("tick"),
                Some(&expected_v3_tick)
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
                    .await
                    .expect("start standalone rpc server");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            for (protocol_system, component_id) in
                [
                    ("uniswap_v2", expected_v2_component_id.clone()),
                    ("uniswap_v3", expected_final_v3_component_id.clone()),
                ]
            {
                let mut rpc_components = None;
                for _ in 0..100 {
                    let response = match client
                        .post(format!("http://127.0.0.1:{rpc_port}/v1/protocol_components"))
                        .json(&dto::ProtocolComponentsRequestBody::id_filtered(
                            protocol_system,
                            vec![component_id.clone()],
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
            .await
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
            .await
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
            .await
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

    #[tokio::test]
    async fn combined_family_runner_restart_keeps_dynamic_component_queryable_after_contract_and_storage_only_follow_up(
    ) {
        use prost::Message;
        use tycho_common::models::{token::Token, FinancialType, ProtocolType};
        use tycho_indexer::substreams::mock::{
            start_scripted_mock_substreams, MockSubstreamsScript,
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
            let dynamic_component_id = "0x4747474747474747474747474747474747474747";

            let v2_creation_block =
                v2_pair_created_block(43, 1_718_500_043, 0xf1, 0xa0, 0xc0, 0x47);
            let v2_creation_changes = ethereum_uniswap_v2_v3_combined::build_family_v2_pool_created_block_changes(
                &v2_creation_block,
                &ethereum_uniswap_v2_v3_combined::parse_family_v2_pool_created_params(
                    "factory_address=0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1&protocol_type_name=uniswap_v2_pool",
                ),
            );
            let family_creation_changes = substreams::BlockChanges::decode(
                ethereum_uniswap_v2_v3_combined::build_uniswap_family_protocol_changes_from_v2(
                    v2_creation_changes,
                )
                .encode_to_vec()
                .as_slice(),
            )
            .expect("bridge family creation block changes into indexer protobuf type");

            let (captured_first, addr_first) =
                start_scripted_mock_substreams(vec![MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-restart-contract-storage", 43),
                        family_block_response_from_block_changes(
                            "cursor-restart-contract-storage",
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
                test_family_shared_spkg_path("combined-family-restart-contract-storage");

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

            let family_follow_up_changes = substreams::BlockChanges {
                block: Some(substreams::Block {
                    number: 44,
                    hash: vec![0x44; 32],
                    parent_hash: vec![0x43; 32],
                    ts: 1_718_500_044,
                }),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(substreams::Transaction {
                        hash: vec![0xcc; 32],
                        from: vec![0x11; 20],
                        to: vec![0x22; 20],
                        index: 9,
                    }),
                    contract_changes: vec![substreams::ContractChange {
                        address: vec![0x47; 20],
                        balance: vec![],
                        code: vec![],
                        change: substreams::ChangeType::Update as i32,
                        slots: vec![],
                        token_balances: vec![],
                    }],
                    entity_changes: vec![],
                    component_changes: vec![],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![substreams::TransactionStorageChanges {
                    tx: Some(substreams::Transaction {
                        hash: vec![0xdd; 32],
                        from: vec![0x11; 20],
                        to: vec![0x22; 20],
                        index: 10,
                    }),
                    storage_changes: vec![substreams::StorageChanges {
                        address: vec![0x47; 20],
                        slots: vec![substreams::ContractSlot {
                            slot: vec![0x01],
                            value: vec![0x02],
                            previous_value: vec![],
                        }],
                        native_balance: None,
                    }],
                }],
            };

            let (captured_second, addr_second) =
                start_scripted_mock_substreams(vec![MockSubstreamsScript {
                    responses: vec![
                        scripted_session_response("trace-restart-contract-storage", 44),
                        family_block_response_from_block_changes(
                            "cursor-restart-contract-storage",
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
                assert_eq!(requests[0].start_block_num, 44);
                assert_eq!(
                    requests[0].start_cursor,
                    "cursor-restart-contract-storage@43"
                );
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
                .expect("read dynamic component state after contract/storage-only restart follow-up");
            assert_eq!(dynamic_state.entity.len(), 1);

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
    use crate::testing::{
        combined_family_real_history_slice_capture_spec_for_tests,
        future_family_runtime_registry_for_record_substreams_tests,
        render_repo_combined_family_record_command_for_tests,
        repo_combined_family_expected_spkg_for_tests,
        repo_combined_family_record_cli_args_for_tests,
        write_record_substreams_ambiguous_fixture_inputs,
        write_record_substreams_family_fixture_inputs,
        write_record_substreams_future_family_fixture_inputs,
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

    fn repo_combined_family_record_args_for_tests(
        spec: &crate::testing::RepoCombinedFamilyFixtureCaptureSpec,
        output_path: &std::path::Path,
        start_block: i64,
        stop_block: &str,
        params: &[&str],
    ) -> RecordSubstreamsArgs {
        let cli_args = repo_combined_family_record_cli_args_for_tests(
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

    fn repo_combined_family_record_args_from_spec_for_tests(
        spec: &crate::testing::RepoCombinedFamilyFixtureCaptureSpec,
    ) -> RecordSubstreamsArgs {
        let params = spec
            .params
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        repo_combined_family_record_args_for_tests(
            spec,
            &spec.output_path,
            spec.start_block,
            &spec.stop_block,
            &params,
        )
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

    fn combined_family_db_gate_manifest_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("combined_family_db_gate.tests")
    }

    fn combined_family_live_gate_manifest_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("combined_family_live_gate.tests")
    }

    fn combined_family_extensibility_contract_manifest_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("combined_family_extensibility_contract.tests")
    }

    fn combined_family_db_gate_script_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent")
            .join("scripts")
            .join("check-combined-family-db.sh")
    }

    fn combined_family_fynd_live_e2e_script_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent")
            .join("scripts")
            .join("check-combined-family-fynd-live-e2e.sh")
    }

    fn combined_family_extensibility_gate_script_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent")
            .join("scripts")
            .join("check-combined-family-extensibility.sh")
    }

    fn combined_family_validation_script_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent")
            .join("scripts")
            .join("check-combined-family.sh")
    }

    fn combined_family_indexer_run_script_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent")
            .join("scripts")
            .join("run-combined-family-indexer.sh")
    }

    fn combined_family_fynd_repo_root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent")
            .parent()
            .expect("repo root should have sibling workspace directory")
            .join("fynd")
    }

    fn read_nonempty_manifest_lines(path: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap_or_else(|err| panic!("read manifest at {}: {err}", path.display()))
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(ToOwned::to_owned)
            .collect()
    }

    fn combined_family_live_gate_tests() -> std::collections::BTreeMap<String, String> {
        let mut tests = std::collections::BTreeMap::new();
        for line in read_nonempty_manifest_lines(&combined_family_live_gate_manifest_path()) {
            let mut parts = line.split_whitespace();
            let selection = parts
                .next()
                .unwrap_or_else(|| panic!("live gate manifest entry missing selection: {line}"));
            let test_name = parts
                .next()
                .unwrap_or_else(|| panic!("live gate manifest entry missing test name: {line}"));
            let extra = parts.next();
            assert!(
                extra.is_none(),
                "live gate manifest entry should contain exactly two fields: {line}"
            );
            tests.insert(selection.to_string(), test_name.to_string());
        }
        tests
    }

    fn combined_family_extensibility_contract_entries() -> Vec<(String, String)> {
        read_nonempty_manifest_lines(&combined_family_extensibility_contract_manifest_path())
            .into_iter()
            .map(|line| {
                let mut parts = line.split_whitespace();
                let file_path = parts.next().unwrap_or_else(|| {
                    panic!("extensibility contract entry missing file path: {line}")
                });
                let function_name = parts.next().unwrap_or_else(|| {
                    panic!("extensibility contract entry missing function name: {line}")
                });
                let extra = parts.next();
                assert!(
                    extra.is_none(),
                    "extensibility contract entry should contain exactly two fields: {line}"
                );
                (file_path.to_string(), function_name.to_string())
            })
            .collect()
    }

    fn combined_family_fynd_route_test_name() -> String {
        combined_family_live_gate_tests()
            .remove("route")
            .expect("live gate manifest should define route test")
    }

    fn combined_family_fynd_settlement_test_name() -> String {
        combined_family_live_gate_tests()
            .remove("settlement")
            .expect("live gate manifest should define settlement test")
    }

    fn combined_family_fynd_default_route_test() -> &'static str {
        "quote_returns_route_for_combined_uniswap_family"
    }

    fn combined_family_fynd_default_settlement_test() -> &'static str {
        "quote_settles_within_encoded_bounds_at_quote_block_for_combined_uniswap_family"
    }

    fn combined_family_db_gate_tests() -> Vec<String> {
        read_nonempty_manifest_lines(&combined_family_db_gate_manifest_path())
    }

    fn combined_family_db_gate_history_slice_rpc_semantics_test() -> &'static str {
        "test_serial_db::combined_family_runner_replays_fixture_backed_v2_and_v3_history_slice_in_one_shared_session"
    }

    fn combined_family_db_gate_bootstrap_complete_fresh_start_test() -> &'static str {
        "test_serial_db::combined_family_runner_alias_members_fresh_start_from_completed_shared_bootstrap"
    }

    fn combined_family_db_gate_rejects_legacy_fallback_resume_test() -> &'static str {
        "test_serial_db::combined_family_runner_rejects_legacy_extractor_scoped_resume_state_under_shared_durability"
    }

    fn combined_family_db_gate_v2_dynamic_admission_reconnect_test() -> &'static str {
        "test_serial_db::combined_family_runner_reconnect_applies_v2_follow_up_state_after_dynamic_component_admission"
    }

    fn combined_family_db_gate_v3_dynamic_admission_reconnect_test() -> &'static str {
        "test_serial_db::combined_family_runner_reconnect_applies_v3_follow_up_state_after_dynamic_component_admission"
    }

    fn combined_family_db_gate_v2_dynamic_admission_restart_test() -> &'static str {
        "test_serial_db::combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission"
    }

    fn combined_family_db_gate_v3_dynamic_admission_restart_test() -> &'static str {
        "test_serial_db::combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission"
    }

    fn combined_family_db_gate_contract_and_storage_only_restart_test() -> &'static str {
        "test_serial_db::combined_family_runner_restart_keeps_dynamic_component_queryable_after_contract_and_storage_only_follow_up"
    }

    fn write_temp_combined_family_db_gate_manifest_for_tests(
        file_name: &str,
        tests: &[&str],
    ) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "{file_name}-{}-{}.tests",
            process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos()
        ));
        let body = tests
            .iter()
            .map(|test_name| format!("{test_name}\n"))
            .collect::<String>();
        std::fs::write(&path, body).expect("write temp combined-family DB gate manifest");
        path
    }

    fn write_temp_combined_family_live_gate_manifest_for_tests(
        file_name: &str,
        route_test: &str,
        settlement_test: &str,
    ) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "{file_name}-{}-{}.tests",
            process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos()
        ));
        let body = format!("route {route_test}\nsettlement {settlement_test}\n");
        std::fs::write(&path, body).expect("write temp combined-family live gate manifest");
        path
    }

    fn write_temp_combined_family_extensibility_manifest_for_tests(
        file_name: &str,
        entries: &[(&str, &str)],
    ) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "{file_name}-{}-{}.tests",
            process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos()
        ));
        let body = entries
            .iter()
            .map(|(file_path, function_name)| format!("{file_path} {function_name}\n"))
            .collect::<String>();
        std::fs::write(&path, body).expect("write temp combined-family extensibility manifest");
        path
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
            future_family_runtime_registry_for_record_substreams_tests(),
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

    #[test]
    fn resolve_record_substreams_request_with_registry_auto_selects_unique_future_family_target() {
        let spkg_path = temp_path("record-future-family-auto-select-spkg", "spkg");
        std::fs::write(
            &spkg_path,
            tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
        )
        .expect("write temp future family spkg");
        let extractors_config_path =
            write_record_substreams_future_family_fixture_inputs(&spkg_path);
        let output_path = temp_path("record-future-family-auto-select-output", "json");
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
        .expect("parse future-family auto-select record-substreams command");
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };

        let resolved = resolve_record_substreams_request_with_registry(
            &record_args,
            future_family_runtime_registry_for_record_substreams_tests(),
        )
        .expect("unique future-family runtime should auto-select");

        assert_eq!(resolved.module, "map_future_swap_family_protocol_changes");
        assert_eq!(resolved.start_block, 100);
        assert_eq!(resolved.stop_block, 102);
        assert_eq!(resolved.extractor_id, "ethereum:future_swap_family");
        assert_eq!(resolved.params.get("extra_flag"), Some(&"future-enabled".to_string()));

        let _ = std::fs::remove_file(spkg_path);
        let _ = std::fs::remove_file(output_path);
        let _ = std::fs::remove_file(extractors_config_path);
    }

    #[test]
    fn resolve_record_substreams_request_requires_selector_when_config_has_multiple_targets() {
        let spkg_path = temp_path("record-ambiguous-command-spkg", "spkg");
        std::fs::write(
            &spkg_path,
            tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
        )
        .expect("write temp ambiguous spkg");
        let extractors_config_path = write_record_substreams_ambiguous_fixture_inputs(&spkg_path);
        let output_path = temp_path("record-ambiguous-output", "json");

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
            "--output",
            &output_path.to_string_lossy(),
        ])
        .expect("parse ambiguous derived record-substreams command");
        let Command::RecordSubstreams(record_args) = cli.command() else {
            panic!("expected record-substreams command");
        };

        let err = resolve_record_substreams_request(&record_args)
            .expect_err("ambiguous config should require an explicit selector");
        let err_text = err.to_string();
        assert!(err_text.contains("requires exactly one of `--family` or `--protocol-system`"));
        assert!(err_text.contains("family:uniswap"));
        assert!(err_text.contains("protocol_system:curve"));

        let _ = std::fs::remove_file(spkg_path);
        let _ = std::fs::remove_file(output_path);
        let _ = std::fs::remove_file(extractors_config_path);
    }

    #[tokio::test]
    async fn record_substreams_fixture_with_registry_records_future_family_request() {
        use std::{future::Future, pin::Pin, sync::Mutex};

        use tycho_indexer::substreams::mock::MockSubstreamsScript;

        let expected_responses = vec![
            crate::testing::scripted_session_response("trace-record", 100),
            block_response(100, "future-cursor@100"),
            block_response(101, "future-cursor@101"),
        ];
        #[derive(Default)]
        struct FakeRecorder {
            captured: Mutex<Vec<tycho_indexer::pb::sf::substreams::rpc::v3::Request>>,
            script: Mutex<Option<MockSubstreamsScript>>,
        }

        impl SubstreamsFixtureRecorder for FakeRecorder {
            fn record<'a>(
                &'a self,
                request: tycho_indexer::pb::sf::substreams::rpc::v3::Request,
                _max_responses: Option<usize>,
            ) -> Pin<
                Box<dyn Future<Output = Result<MockSubstreamsScript, anyhow::Error>> + Send + 'a>,
            > {
                Box::pin(async move {
                    self.captured
                        .lock()
                        .unwrap()
                        .push(request);
                    self.script
                        .lock()
                        .unwrap()
                        .clone()
                        .ok_or_else(|| anyhow!("fake recorder missing scripted response"))
                })
            }
        }

        let recorder = Arc::new(FakeRecorder {
            captured: Mutex::new(Vec::new()),
            script: Mutex::new(Some(MockSubstreamsScript {
                responses: expected_responses.clone(),
                grpc_status: "0",
                grpc_message: None,
            })),
        });

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
            "http://unused",
            "--rpc-url",
            "http://unused",
            "record-substreams",
            "--substreams-api-token",
            "token",
            "--extractors-config",
            &extractors_config_path.to_string_lossy(),
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

        let resolved_request = resolve_record_substreams_request_with_registry(
            &record_args,
            future_family_runtime_registry_for_record_substreams_tests(),
        )
        .expect("resolve future-family record-substreams request");
        assert!(resolved_request
            .extractor_id
            .contains("future_swap"));

        let loaded_package = tycho_indexer::pb::sf::substreams::v1::Package::default();
        let _ = global_args;
        record_substreams_fixture_from_package_and_recorder(
            loaded_package,
            recorder.clone(),
            resolved_request,
            &record_args,
        )
        .await
        .expect("record future-family fixture through command path");

        let fixture = read_mock_substreams_fixture(&output_path).expect("read recorded fixture");
        assert_eq!(fixture.len(), 1);
        assert_eq!(fixture[0].grpc_status, "0");
        assert_eq!(fixture[0].grpc_message, None);
        assert_eq!(fixture[0].responses, expected_responses);

        let requests = recorder.captured.lock().unwrap();
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
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
        let output_path = temp_path("repo-combined-derived-request-json", "json");
        let record_args = repo_combined_family_record_args_for_tests(
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
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
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
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
        let output_path = temp_path("repo-combined-derived-request", "json");
        assert!(
            spec.extractors_config_path.exists(),
            "expected checked-in combined config at {}",
            spec.extractors_config_path.display()
        );
        let record_args = repo_combined_family_record_args_for_tests(
            &spec,
            &output_path,
            25_384_601,
            "+2",
            &["extra_flag=enabled"],
        );

        let resolved = resolve_record_substreams_request(&record_args)
            .expect("repo combined config should derive one shared family request");

        assert_eq!(resolved.spkg, repo_combined_family_expected_spkg_for_tests());
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
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
        let output_path = combined_family_real_history_slice_fixture_path_for_recorder();
        let record_args = repo_combined_family_record_args_for_tests(
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
        assert_eq!(record_args.family.as_deref(), None);
        assert_eq!(resolved.module, spec.output_module);
        assert_eq!(resolved.spkg, repo_combined_family_expected_spkg_for_tests());
    }

    #[test]
    fn combined_family_real_history_slice_capture_spec_anchors_live_fixture_refresh() {
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
        let record_args = repo_combined_family_record_args_from_spec_for_tests(&spec);
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
        assert_eq!(record_args.family.as_deref(), None);
        assert_eq!(resolved.start_block, 25_384_601);
        assert_eq!(resolved.stop_block, 25_384_603);
        assert_eq!(resolved.module, spec.output_module);
    }

    #[test]
    fn combined_family_real_history_slice_capture_spec_builds_stable_repo_cli_args() {
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
        let cli_args = repo_combined_family_record_cli_args_for_tests(
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
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
        let cli_args = repo_combined_family_record_cli_args_for_tests(
            &spec,
            &spec.output_path,
            spec.start_block,
            &spec.stop_block,
            &[],
        );

        let rendered = render_repo_combined_family_record_command_for_tests(&cli_args);

        assert_eq!(
            rendered,
            format!(
                "tycho-indexer --database-url postgres://unused --endpoint http://localhost:9000 --rpc-url http://localhost:8545 record-substreams --substreams-api-token token --extractors-config {} --start-block 25384601 --stop-block +2 --output {}",
                spec.extractors_config_path.to_string_lossy(),
                spec.output_path.to_string_lossy(),
            )
        );
    }

    #[test]
    fn combined_family_real_history_slice_script_command_renders_stable_live_capture_command() {
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
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
            "cargo run --bin tycho-indexer -- \\\n  --database-url postgres://unused \\\n  --endpoint '<set TYCHO_RECORD_ENDPOINT>' \\\n  --rpc-url '<set TYCHO_RECORD_RPC_URL>' \\\n  record-substreams \\\n  --substreams-api-token '<set SUBSTREAMS_API_TOKEN>' \\\n  --extractors-config {} \\\n  --start-block {} \\\n  --stop-block {} \\\n  --output {}\n",
            spec.extractors_config_path.to_string_lossy(),
            spec.start_block,
            spec.stop_block,
            spec.output_path.to_string_lossy(),
        );

        assert_eq!(rendered, expected);
    }

    #[test]
    fn combined_family_real_history_slice_script_preflight_renders_resolved_request_json() {
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
        let record_args = repo_combined_family_record_args_from_spec_for_tests(&spec);
        let expected = format!(
            "{}\n",
            render_record_substreams_request_json(
                &resolve_record_substreams_request(&record_args)
                    .expect("history-slice preflight request should resolve"),
            )
            .expect("history-slice request json should render")
        );

        let script_path = combined_family_real_history_slice_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("preflight")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected fixture helper script preflight mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "script preflight mode should succeed, stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("script preflight mode should emit utf8 request json");
        assert_eq!(rendered, expected);
    }

    #[test]
    fn combined_family_real_history_slice_script_doctor_reports_missing_external_requirements() {
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
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
            "ready=false\nfamily_name=auto\nstart_block={}\nstop_block={}\nextractors_config={}\noutput_path={}\nsubstreams_api_token=missing\nrecord_endpoint=missing\nrecord_rpc_url=missing\n",
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
        let spec = combined_family_real_history_slice_capture_spec_for_tests();
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
            script.contains("TYCHO_COMBINED_FIXTURE_FAMILY:-}"),
            "script should expose an optional family override with auto-resolution by default"
        );
        assert!(
            script.contains("RECORD_CMD+=(--family \"${FAMILY_NAME}\")"),
            "script should route the family argument through the shared override"
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

    #[test]
    fn combined_family_db_gate_manifest_stays_aligned_with_main_rs_tests() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            !manifest_tests.is_empty(),
            "combined-family DB gate manifest should enumerate at least one test"
        );

        let main_rs = std::fs::read_to_string(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join("main.rs"),
        )
        .expect("read main.rs for combined-family DB gate alignment");

        for test_name in manifest_tests {
            let function_name = test_name
                .rsplit("::")
                .next()
                .expect("manifest test name should have a final segment");
            assert!(
                main_rs.contains(&format!("async fn {function_name}(")),
                "combined-family DB gate manifest references missing test `{test_name}`"
            );
        }
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_shared_session_rpc_semantics_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_history_slice_rpc_semantics_test().to_string()
            ),
            "combined-family DB gate manifest must keep the fixture-backed shared-session history-slice test that proves external RPC component/state semantics"
        );
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_completed_bootstrap_fresh_start_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_bootstrap_complete_fresh_start_test().to_string()
            ),
            "combined-family DB gate manifest must keep the completed-shared-bootstrap fresh-start test that proves top-level shared-family startup resumes at bootstrap block + 1 without a stream cursor"
        );
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_shared_durability_resume_rejection_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_rejects_legacy_fallback_resume_test().to_string()
            ),
            "combined-family DB gate manifest must keep the legacy-fallback rejection test that proves shared-family startup does not silently inherit extractor-local resume state under a family durability scope"
        );
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_v2_dynamic_admission_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_v2_dynamic_admission_reconnect_test().to_string()
            ),
            "combined-family DB gate manifest must keep the V2 dynamic-admission reconnect test that proves a shared-bootstrap-seeded V2 branch can admit a new pool, persist follow-up state, and keep that state externally queryable after reconnect"
        );
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_v3_dynamic_admission_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_v3_dynamic_admission_reconnect_test().to_string()
            ),
            "combined-family DB gate manifest must keep the V3 dynamic-admission reconnect test that proves a shared-bootstrap-seeded V3 branch can admit a new pool, persist follow-up state, and keep that state externally queryable after reconnect"
        );
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_v2_dynamic_admission_restart_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_v2_dynamic_admission_restart_test().to_string()
            ),
            "combined-family DB gate manifest must keep the V2 dynamic-admission restart test that proves a shared-bootstrap-seeded V2 branch can admit a new pool, persist follow-up state across process restart, and keep that state externally queryable after restart"
        );
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_v3_dynamic_admission_restart_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_v3_dynamic_admission_restart_test().to_string()
            ),
            "combined-family DB gate manifest must keep the V3 dynamic-admission restart test that proves a shared-bootstrap-seeded V3 branch can admit a new pool, persist follow-up state across process restart, and keep that state externally queryable after restart"
        );
    }

    #[test]
    fn combined_family_db_gate_manifest_keeps_contract_and_storage_only_restart_coverage() {
        let manifest_tests = combined_family_db_gate_tests();
        assert!(
            manifest_tests.contains(
                &combined_family_db_gate_contract_and_storage_only_restart_test().to_string()
            ),
            "combined-family DB gate manifest must keep the restart-time contract/storage-only follow-up test that proves a dynamically admitted family component stays externally queryable after restart even when subsequent ownership arrives only through contract/storage changes"
        );
    }

    #[test]
    fn combined_family_extensibility_contract_manifest_stays_aligned_with_source_tests() {
        let manifest_entries = combined_family_extensibility_contract_entries();
        assert!(
            !manifest_entries.is_empty(),
            "combined-family extensibility contract manifest should enumerate at least one test"
        );

        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let crate_root = manifest_dir
            .parent()
            .and_then(std::path::Path::parent)
            .expect("tycho-indexer crate should have repo grandparent");

        for (file_path, function_name) in manifest_entries {
            let absolute_path = crate_root.join(&file_path);
            let source = std::fs::read_to_string(&absolute_path).unwrap_or_else(|err| {
                panic!("read extensibility contract source file {}: {err}", absolute_path.display())
            });
            let test_patterns =
                [format!("fn {function_name}("), format!("async fn {function_name}(")];
            assert!(
                test_patterns.iter().any(|pattern| source.contains(pattern)),
                "combined-family extensibility contract manifest references missing test `{function_name}` in `{file_path}`"
            );
        }
    }

    #[test]
    fn combined_family_db_gate_script_stays_aligned_with_manifest() {
        let script_path = combined_family_db_gate_script_path();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!("expected to read DB gate script at {}: {err}", script_path.display())
        });
        assert!(
            script.contains("combined_family_db_gate.tests"),
            "DB gate script should resolve its test list from the checked-in manifest"
        );

        let output = std::process::Command::new(&script_path)
            .arg("list")
            .output()
            .unwrap_or_else(|err| {
                panic!("expected DB gate script list mode at {}: {err}", script_path.display())
            });
        assert!(
            output.status.success(),
            "DB gate script list mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("DB gate script list mode should emit utf8 test names");
        let rendered_tests = rendered
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        assert_eq!(rendered_tests, combined_family_db_gate_tests());
    }

    #[test]
    fn combined_family_db_gate_script_supports_manifest_override() {
        let script_path = combined_family_db_gate_script_path();
        let custom_manifest = write_temp_combined_family_db_gate_manifest_for_tests(
            "combined-family-db-gate-override",
            &[
                "test_serial_db::shared_bootstrap_seed_universe_spec_supports_non_uniswap_family_registry",
                "test_serial_db::repo_combined_family_bootstrap_pool_seeds_are_derived_from_repo_config",
            ],
        );

        let output = std::process::Command::new(&script_path)
            .arg("list")
            .env("TYCHO_COMBINED_FAMILY_DB_TEST_MANIFEST", &custom_manifest)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected DB gate script list mode with manifest override at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "DB gate script list mode with manifest override should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("DB gate script list mode with manifest override should emit utf8");
        let rendered_tests = rendered
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        assert_eq!(
            rendered_tests,
            vec![
                "test_serial_db::shared_bootstrap_seed_universe_spec_supports_non_uniswap_family_registry"
                    .to_string(),
                "test_serial_db::repo_combined_family_bootstrap_pool_seeds_are_derived_from_repo_config"
                    .to_string(),
            ]
        );

        let _ = std::fs::remove_file(custom_manifest);
    }

    #[test]
    fn combined_family_db_gate_doctor_reports_expected_diagnostics() {
        let script_path = combined_family_db_gate_script_path();
        let custom_database_url = "postgres://example:secret@127.0.0.1:5999/custom_db";
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env("DATABASE_URL", custom_database_url)
            .output()
            .unwrap_or_else(|err| {
                panic!("expected DB gate script doctor mode at {}: {err}", script_path.display())
            });
        assert!(
            output.status.success(),
            "DB gate script doctor mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("DB gate script doctor mode should emit utf8 diagnostics");
        assert!(rendered.contains("ready="), "doctor output should include readiness");
        assert!(
            rendered.contains(&format!("base_database_url={custom_database_url}")),
            "doctor output should echo the base DATABASE_URL override"
        );
        assert!(
            rendered.contains("database_state="),
            "doctor output should include database reachability"
        );
        assert!(
            rendered.contains("run_database_name="),
            "doctor output should include the isolated run database name"
        );
        assert!(
            rendered.contains("run_database_url="),
            "doctor output should include the isolated run database URL"
        );
        assert!(
            rendered.contains("maintenance_database_url="),
            "doctor output should include the maintenance database URL"
        );
        assert!(
            rendered.contains("docker_cli="),
            "doctor output should include docker CLI availability"
        );
        assert!(
            rendered.contains("docker_daemon="),
            "doctor output should include docker daemon availability"
        );
        assert!(
            rendered.contains("docker_compose_file="),
            "doctor output should include compose file path"
        );
        assert!(
            rendered.contains("database_start_command="),
            "doctor output should include the DB start command"
        );
        assert!(
            rendered.contains("test_count="),
            "doctor output should include focused test count"
        );
        assert!(rendered.contains("test_manifest="), "doctor output should include manifest path");
    }

    #[test]
    fn combined_family_db_gate_db_command_stays_aligned_with_repo_layout() {
        let script_path = combined_family_db_gate_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("db-command")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected DB gate script db-command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "DB gate script db-command mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("DB gate script db-command mode should emit utf8 command");
        let expected = format!(
            "cd {}\nTYCHO_IMAGE=alpine docker compose -f {} up -d db\n",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(std::path::Path::parent)
                .expect("tycho-indexer crate should have repo grandparent")
                .display(),
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(std::path::Path::parent)
                .expect("tycho-indexer crate should have repo grandparent")
                .join("docker")
                .join("docker-compose.yaml")
                .display(),
        );
        assert_eq!(rendered, expected);
    }

    #[test]
    fn combined_family_db_gate_run_fails_fast_when_database_is_unreachable() {
        let script_path = combined_family_db_gate_script_path();
        let unreachable_database_url = "postgres://example:secret@127.0.0.1:1/unreachable_db";
        let output = std::process::Command::new(&script_path)
            .arg("run")
            .env("DATABASE_URL", unreachable_database_url)
            .output()
            .unwrap_or_else(|err| {
                panic!("expected DB gate script run mode at {}: {err}", script_path.display())
            });
        assert!(
            !output.status.success(),
            "DB gate script run mode should fail when DATABASE_URL is unreachable"
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("DB gate script run mode should emit utf8 diagnostics");
        assert!(
            rendered.contains(&format!("base_database_url={unreachable_database_url}")),
            "run preflight should echo the base DATABASE_URL override before failing"
        );
        assert!(
            rendered.contains("database_state=unreachable"),
            "run preflight should report unreachable database before aborting"
        );
        assert!(
            rendered.contains("database_start_command="),
            "run preflight should include the DB start command"
        );
        assert!(
            !rendered.contains("cargo test -p tycho-indexer"),
            "run should fail at preflight before emitting or executing cargo test commands"
        );
    }

    #[test]
    fn combined_family_db_gate_command_renders_manifest_tests_and_database_override() {
        let script_path = combined_family_db_gate_script_path();
        let custom_database_url = "postgres://example:secret@127.0.0.1:5888/command_db";
        let custom_run_db_name = "combined_family_command_gate";
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .env("DATABASE_URL", custom_database_url)
            .env("TYCHO_COMBINED_FAMILY_DB_NAME", custom_run_db_name)
            .output()
            .unwrap_or_else(|err| {
                panic!("expected DB gate script command mode at {}: {err}", script_path.display())
            });
        assert!(
            output.status.success(),
            "DB gate script command mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("DB gate script command mode should emit utf8 shell command");
        assert!(
            rendered.contains(&format!(
                "export DATABASE_URL='postgres://example:secret@127.0.0.1:5888/{custom_run_db_name}'"
            )),
            "command output should export the isolated run database URL"
        );
        assert!(
            rendered.contains("export TYCHO_REQUIRE_TEST_DB=1"),
            "command output should force strict DB mode"
        );
        assert!(
            rendered.contains("CREATE DATABASE"),
            "command output should create the isolated run database"
        );
        assert!(
            rendered.contains("DROP DATABASE IF EXISTS"),
            "command output should drop the isolated run database before and after the gate"
        );
        assert!(
            rendered.contains(
                "cargo test -p tycho-indexer --bin tycho-indexer --no-run 2>&1"
            ),
            "command output should build the tycho-indexer test binary once before running manifest tests"
        );
        assert!(
            rendered.contains("sed -n 's/^  Executable .* (\\(.*\\))$/\\1/p'"),
            "command output should extract the current test binary path from the latest cargo --no-run executable report"
        );

        for test_name in combined_family_db_gate_tests() {
            assert!(
                rendered.contains(&format!("  {test_name} ")),
                "command output should include manifest test `{test_name}`"
            );
        }
        assert!(
            rendered.contains("\"${TEST_BINARY}\" \"${test_name}\" --exact --nocapture"),
            "command output should reuse the resolved test binary for each manifest test"
        );
        assert!(
            rendered.contains(
                "TEST_BINARY=\"$(cargo test -p tycho-indexer --bin tycho-indexer --no-run 2>&1"
            ),
            "command output should resolve the test binary path once"
        );
        assert!(
            rendered.contains("failed to resolve tycho-indexer test binary path"),
            "command output should fail explicitly when the test binary path cannot be resolved"
        );
    }

    #[test]
    fn combined_family_db_gate_strict_doctor_fails_when_database_is_unreachable() {
        let script_path = combined_family_db_gate_script_path();
        let unreachable_database_url = "postgres://example:secret@127.0.0.1:1/strict_doctor_db";
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .arg("--strict")
            .env("DATABASE_URL", unreachable_database_url)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected DB gate script strict doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            !output.status.success(),
            "DB gate script strict doctor mode should fail when DATABASE_URL is unreachable"
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("DB gate script strict doctor mode should emit utf8 diagnostics");
        assert!(
            rendered.contains(&format!("base_database_url={unreachable_database_url}")),
            "strict doctor output should echo the base DATABASE_URL override"
        );
        assert!(
            rendered.contains("database_state=unreachable"),
            "strict doctor output should report unreachable database"
        );
        assert!(
            rendered.contains("database_start_command="),
            "strict doctor output should include the DB start command"
        );
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_doctor_reports_expected_diagnostics() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let fynd_repo_root = combined_family_fynd_repo_root();
        let test_manifest = combined_family_live_gate_manifest_path();
        let tycho_url = "127.0.0.1:1";
        let rpc_url = "https://rpc.example.invalid";
        let rust_log = "warn,tycho_client=debug,fynd=trace";
        let route_test = combined_family_fynd_route_test_name();
        let settlement_test = combined_family_fynd_settlement_test_name();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env("FYND_REPO_ROOT", &fynd_repo_root)
            .env("FYND_E2E_TYCHO_URL", tycho_url)
            .env("FYND_E2E_RPC_URL", rpc_url)
            .env("FYND_E2E_RUST_LOG", rust_log)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected Fynd live E2E script doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "Fynd live E2E script doctor mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("Fynd live E2E script doctor mode should emit utf8 diagnostics");
        let expected = format!(
            "ready=false\nfynd_repo_root={}\nfynd_repo_exists=true\nfynd_test_exists=true\ntycho_url={}\ntycho_health=unreachable\ntycho_protocols_ready=unreachable\nprotocol_v2_ready=unknown\nprotocol_v3_ready=unknown\nchain=ethereum\nrpc_url={}\nrust_log={}\nhealth_timeout_secs=300\ntraded_n_days_ago=3\nclient_timeout_secs=5\nclient_retry_max_attempts=1\nmin_token_quality=100\nhealth_mode_override=default\nroute_health_mode=quote_ready\nsettlement_health_mode=strict\nquote_timeout_secs=420\nconnector_tokens=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,0xdac17f958d2ee523a2206206994597c13d831ec7,0x6b175474e89094c44da98b954eedeac495271d0f,0x2260fac5e5542a773aa44fbcfedf7c193bc2c599\nroute_test={}\nsettlement_test={}\ncurl_available=true\ntycho_stream_ws_buffer_size=default\ntycho_stream_subscription_buffer_size=default\ntest_manifest={}\n",
            fynd_repo_root.display(),
            tycho_url,
            rpc_url,
            rust_log,
            route_test,
            settlement_test,
            test_manifest.display(),
        );
        assert_eq!(rendered, expected);
    }

    #[test]
    fn combined_family_live_gate_manifest_keeps_route_test_mapping() {
        let manifest_tests = combined_family_live_gate_tests();
        assert_eq!(
            manifest_tests.get("route").map(String::as_str),
            Some(combined_family_fynd_default_route_test()),
            "combined-family live gate manifest must keep the canonical route-return Fynd E2E test mapping"
        );
    }

    #[test]
    fn combined_family_live_gate_manifest_keeps_settlement_test_mapping() {
        let manifest_tests = combined_family_live_gate_tests();
        assert_eq!(
            manifest_tests.get("settlement").map(String::as_str),
            Some(combined_family_fynd_default_settlement_test()),
            "combined-family live gate manifest must keep the canonical settlement Fynd E2E test mapping"
        );
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_list_reports_manifest_backed_mapping() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let route_test = combined_family_fynd_route_test_name();
        let settlement_test = combined_family_fynd_settlement_test_name();
        let output = std::process::Command::new(&script_path)
            .arg("list")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected Fynd live E2E script list mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "Fynd live E2E script list mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("Fynd live E2E script list mode should emit utf8");
        assert_eq!(
            rendered,
            format!("route={route_test}\nsettlement={settlement_test}\n"),
            "list mode should report the manifest-backed live test mapping"
        );
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_command_all_renders_stable_commands() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let fynd_repo_root = combined_family_fynd_repo_root();
        let tycho_url = "127.0.0.1:4242";
        let rpc_url = "https://rpc.mevblocker.io";
        let rust_log = "info,tycho_client=info,tycho_simulation=info,fynd=info";
        let route_test = combined_family_fynd_route_test_name();
        let settlement_test = combined_family_fynd_settlement_test_name();
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("all")
            .env("FYND_REPO_ROOT", &fynd_repo_root)
            .env("FYND_E2E_TYCHO_URL", tycho_url)
            .env("FYND_E2E_RPC_URL", rpc_url)
            .env("FYND_E2E_RUST_LOG", rust_log)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected Fynd live E2E script command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "Fynd live E2E script command mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("Fynd live E2E script command mode should emit utf8 commands");
        let expected = format!(
            "cd {} && \\\nRUST_LOG={} \\\nFYND_E2E_TYCHO_URL={} \\\nFYND_E2E_RPC_URL={} \\\nFYND_E2E_HEALTH_TIMEOUT_SECS=300 \\\nFYND_E2E_TRADED_N_DAYS_AGO=3 \\\nFYND_E2E_CLIENT_TIMEOUT_SECS=5 \\\nFYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS=1 \\\nFYND_E2E_MIN_TOKEN_QUALITY=100 \\\nFYND_E2E_HEALTH_MODE=quote_ready \\\nFYND_E2E_QUOTE_TIMEOUT_SECS=420 \\\nFYND_E2E_CONNECTOR_TOKENS=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,0xdac17f958d2ee523a2206206994597c13d831ec7,0x6b175474e89094c44da98b954eedeac495271d0f,0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 \\\ncargo test --test e2e_quote {} -- --ignored --nocapture && \\\nRUST_LOG={} \\\nFYND_E2E_TYCHO_URL={} \\\nFYND_E2E_RPC_URL={} \\\nFYND_E2E_HEALTH_TIMEOUT_SECS=300 \\\nFYND_E2E_TRADED_N_DAYS_AGO=3 \\\nFYND_E2E_CLIENT_TIMEOUT_SECS=5 \\\nFYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS=1 \\\nFYND_E2E_MIN_TOKEN_QUALITY=100 \\\nFYND_E2E_HEALTH_MODE=strict \\\nFYND_E2E_QUOTE_TIMEOUT_SECS=420 \\\nFYND_E2E_CONNECTOR_TOKENS=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,0xdac17f958d2ee523a2206206994597c13d831ec7,0x6b175474e89094c44da98b954eedeac495271d0f,0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 \\\ncargo test --test e2e_quote {} -- --ignored --nocapture\n",
            fynd_repo_root.display(),
            rust_log,
            tycho_url,
            rpc_url,
            route_test,
            rust_log,
            tycho_url,
            rpc_url,
            settlement_test,
        );
        assert_eq!(rendered, expected);
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_supports_test_name_overrides() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let route_test = "quote_returns_route_for_future_family";
        let settlement_test = "quote_settles_for_future_family";
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("all")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_ROUTE_TEST", route_test)
            .env("FYND_E2E_SETTLEMENT_TEST", settlement_test)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected Fynd live E2E script command mode with test overrides at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "Fynd live E2E script command mode with test overrides should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("Fynd live E2E script command mode should emit utf8 commands");
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                route_test
            )),
            "command output should honor the route test override"
        );
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                settlement_test
            )),
            "command output should honor the settlement test override"
        );
        assert!(
            !rendered.contains(&combined_family_fynd_route_test_name()),
            "command output should not hard-code the default route test when overridden"
        );
        assert!(
            !rendered.contains(&combined_family_fynd_settlement_test_name()),
            "command output should not hard-code the default settlement test when overridden"
        );
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_health_mode_override_beats_selection_defaults() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let override_mode = "strict";
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("all")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_HEALTH_MODE", override_mode)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected Fynd live E2E script command mode with health-mode override at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "Fynd live E2E script command mode with health-mode override should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("Fynd live E2E script command mode with health-mode override should emit utf8");
        let override_occurrences = rendered.matches("FYND_E2E_HEALTH_MODE=strict").count();
        assert_eq!(
            override_occurrences, 2,
            "health-mode override should apply to both route and settlement commands"
        );
        assert!(
            !rendered.contains("FYND_E2E_HEALTH_MODE=quote_ready"),
            "health-mode override should suppress the route default"
        );
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_supports_manifest_override() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let route_test = "quote_returns_route_for_manifest_override_family";
        let settlement_test = "quote_settles_for_manifest_override_family";
        let custom_manifest = write_temp_combined_family_live_gate_manifest_for_tests(
            "combined-family-live-gate-override",
            route_test,
            settlement_test,
        );
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("all")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("TYCHO_COMBINED_FAMILY_LIVE_TEST_MANIFEST", &custom_manifest)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected Fynd live E2E script command mode with manifest override at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "Fynd live E2E script command mode with manifest override should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("Fynd live E2E script manifest override should emit utf8 commands");
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                route_test
            )),
            "command output should honor the manifest-provided route test"
        );
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                settlement_test
            )),
            "command output should honor the manifest-provided settlement test"
        );
        assert!(
            !rendered.contains(&combined_family_fynd_route_test_name()),
            "command output should stop using the default route test when manifest is overridden"
        );
        assert!(
            !rendered.contains(&combined_family_fynd_settlement_test_name()),
            "command output should stop using the default settlement test when manifest is overridden"
        );

        let _ = std::fs::remove_file(custom_manifest);
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_strict_doctor_fails_when_tycho_is_unreachable() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .arg("--strict")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:1")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected Fynd live E2E script strict doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            !output.status.success(),
            "Fynd live E2E strict doctor mode should fail when Tycho is unreachable"
        );
        assert_eq!(output.status.code(), Some(1));

        let rendered = String::from_utf8(output.stdout)
            .expect("Fynd live E2E strict doctor mode should emit utf8 diagnostics");
        assert!(
            rendered.contains("ready=false"),
            "strict doctor output should explain the readiness failure"
        );
        assert!(
            rendered.contains("tycho_health=unreachable"),
            "strict doctor output should report unreachable Tycho"
        );
    }

    #[test]
    fn combined_family_fynd_live_e2e_script_stays_aligned_with_repo_workflow() {
        let script_path = combined_family_fynd_live_e2e_script_path();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!("expected to read Fynd live E2E script at {}: {err}", script_path.display())
        });

        assert!(script.contains("doctor [--strict]"), "script should advertise the doctor mode");
        assert!(
            script.contains("command [route|settlement|all]"),
            "script should advertise the command mode"
        );
        assert!(script.contains("list"), "script should advertise the list mode");
        assert!(script.contains("run-route"), "script should advertise the route execution mode");
        assert!(
            script.contains("run-settlement"),
            "script should advertise the settlement execution mode"
        );
        assert!(script.contains("run-all"), "script should advertise the combined execution mode");
        assert!(script.contains("../fynd"), "script should default to the sibling Fynd repository");
        assert!(
            script.contains("FYND_E2E_ROUTE_TEST"),
            "script should expose the route test override surface"
        );
        assert!(
            script.contains("FYND_E2E_SETTLEMENT_TEST"),
            "script should expose the settlement test override surface"
        );
        assert!(
            script.contains("FYND_E2E_HEALTH_TIMEOUT_SECS"),
            "script should expose the health-timeout override surface"
        );
        assert!(
            script.contains("FYND_E2E_MIN_TOKEN_QUALITY"),
            "script should expose the minimum-token-quality override surface"
        );
        assert!(
            script.contains("FYND_E2E_TRADED_N_DAYS_AGO"),
            "script should expose the traded-n-days-ago override surface"
        );
        assert!(
            script.contains("FYND_E2E_CONNECTOR_TOKENS"),
            "script should expose the connector-token allowlist override surface"
        );
        assert!(
            script.contains("FYND_E2E_HEALTH_MODE"),
            "script should expose the health-mode override surface"
        );
        assert!(
            script.contains("quote_ready"),
            "script should document the quote-ready health mode used by the route gate"
        );
        assert!(
            script.contains("strict"),
            "script should document the strict health mode used by the settlement gate"
        );
        assert!(
            script.matches("quote_ready").count() >= 1,
            "script should keep documenting quote-ready health mode for the route gate"
        );
        assert!(
            script.contains("Default: 3"),
            "script should document the combined-family traded-n-days-ago default"
        );
        assert!(script.contains("Default: 300"), "script should document the live quote timeout");
        assert!(
            script.contains("Default: 100"),
            "script should document the combined-family live minimum token quality default"
        );
        assert!(
            script.contains("Default: WETH,USDC,USDT,DAI,WBTC"),
            "script should document the default combined-family connector-token allowlist"
        );
        assert!(
            script.contains("TYCHO_COMBINED_FAMILY_LIVE_TEST_MANIFEST"),
            "script should expose the manifest override surface"
        );
        assert!(
            script.contains("cargo test --test e2e_quote"),
            "script should drive the Fynd ignored e2e_quote tests"
        );
    }

    #[test]
    fn combined_family_extensibility_gate_script_list_stays_aligned_with_manifest() {
        let script_path = combined_family_extensibility_gate_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("list")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected extensibility gate script list mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "extensibility gate script list mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("extensibility gate script list mode should emit utf8 entries");
        let rendered_entries = rendered
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let manifest_entries = combined_family_extensibility_contract_entries()
            .into_iter()
            .map(|(file_path, function_name)| format!("{file_path} {function_name}"))
            .collect::<Vec<_>>();
        assert_eq!(rendered_entries, manifest_entries);
    }

    #[test]
    fn combined_family_extensibility_gate_script_supports_manifest_override() {
        let script_path = combined_family_extensibility_gate_script_path();
        let custom_manifest = write_temp_combined_family_extensibility_manifest_for_tests(
            "combined-family-extensibility-override",
            &[(
                "crates/tycho-indexer/src/main.rs",
                "combined_family_extensibility_contract_manifest_stays_aligned_with_source_tests",
            )],
        );
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .env("TYCHO_COMBINED_FAMILY_EXTENSIBILITY_TEST_MANIFEST", &custom_manifest)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected extensibility gate script command mode with manifest override at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "extensibility gate script command mode with manifest override should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("extensibility gate script override should emit utf8 commands");
        assert!(
            rendered.contains(
                "combined_family_extensibility_contract_manifest_stays_aligned_with_source_tests"
            ),
            "extensibility gate command should honor the override manifest test name"
        );
        assert!(
            rendered.contains("cargo test -p tycho-indexer --no-run 2>/dev/null |"),
            "extensibility gate command should resolve the current tycho-indexer test executables from the latest cargo test --no-run output"
        );
        assert!(
            rendered.contains("sed -n 's/^  Executable .* (\\(.*\\))$/\\1/p'"),
            "extensibility gate command should extract only the current executable paths from cargo output instead of scanning stale target/debug/deps artifacts"
        );
        assert!(
            rendered.contains("\"${TEST_BINARY_BY_ENTRY[${test_name}]}\" \"${TEST_FULL_NAME_BY_ENTRY[${test_name}]}\" --exact --nocapture"),
            "extensibility gate command should execute the override through the manifest-backed executable-index loop"
        );
        assert!(
            rendered.contains("LIST_OUTPUT_FILE=\"$(mktemp)\""),
            "extensibility gate command should allocate a temporary list-output file while probing the resolved test executables"
        );
        assert!(
            rendered.contains("if ! \"${test_binary}\" --list >\"${LIST_OUTPUT_FILE}\" 2>/dev/null; then"),
            "extensibility gate command should skip resolved executables that are not libtest harnesses"
        );
        assert!(
            rendered.contains("done < \"${LIST_OUTPUT_FILE}\""),
            "extensibility gate command should read manifest-backed ownership from the successful harness list output"
        );
        assert!(
            rendered.contains("trap 'rm -f \"${LIST_OUTPUT_FILE}\"' EXIT"),
            "extensibility gate command should clean up the temporary harness list file"
        );
        assert!(
            !rendered.contains("custom_registry_detects_future_family_without_runner_changes"),
            "extensibility gate command should stop using the default manifest entries when overridden"
        );

        let _ = std::fs::remove_file(custom_manifest);
    }

    #[test]
    fn combined_family_extensibility_gate_script_stays_aligned_with_repo_workflow() {
        let script_path = combined_family_extensibility_gate_script_path();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!("expected to read extensibility gate script at {}: {err}", script_path.display())
        });

        assert!(script.contains("doctor [--strict]"), "script should advertise the doctor mode");
        assert!(script.contains("list"), "script should advertise the list mode");
        assert!(script.contains("command"), "script should advertise the command mode");
        assert!(script.contains("run"), "script should advertise the run mode");
        assert!(
            script.contains("TYCHO_COMBINED_FAMILY_EXTENSIBILITY_TEST_MANIFEST"),
            "script should expose the extensibility manifest override surface"
        );
        assert!(
            script.contains("cargo test -p tycho-indexer"),
            "script should drive the tycho-indexer test target directly"
        );
    }

    #[test]
    fn combined_family_validation_script_doctor_reports_expected_diagnostics() {
        let script_path = combined_family_validation_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env(
                "DATABASE_URL",
                "postgres://example:secret@127.0.0.1:1/combined_family_validation_db",
            )
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:1")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script doctor mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family validation script doctor mode should emit utf8 diagnostics");
        assert!(
            rendered.contains("ready=false"),
            "doctor output should report aggregate readiness"
        );
        assert!(
            rendered.contains("acceptance_ready=false"),
            "doctor output should report repo-local acceptance readiness"
        );
        assert!(
            rendered.contains("extensibility_ready=true"),
            "doctor output should report extensibility gate readiness separately"
        );
        assert!(
            rendered.contains("full_ready=false"),
            "doctor output should report full validation readiness"
        );
        assert!(
            rendered.contains("repo_ready=false"),
            "doctor output should report repo gate readiness"
        );
        assert!(
            rendered.contains("live_ready=false"),
            "doctor output should report live gate readiness"
        );
        assert!(
            rendered.contains("operator_ready=false"),
            "doctor output should report canonical indexer operator readiness separately"
        );
        assert!(
            rendered.contains("managed_live_ready=false"),
            "doctor output should report managed live readiness separately"
        );
        assert!(
            rendered.contains("managed_full_ready=false"),
            "doctor output should report managed full readiness separately"
        );
        assert!(
            rendered.contains("extensibility_gate_script="),
            "doctor output should include the extensibility gate script path"
        );
        assert!(
            rendered.contains("db_gate_script="),
            "doctor output should include the DB gate script path"
        );
        assert!(
            rendered.contains("extensibility_doctor_command="),
            "doctor output should include the extensibility doctor command"
        );
        assert!(
            rendered.contains("live_gate_script="),
            "doctor output should include the live gate script path"
        );
        assert!(
            rendered.contains("indexer_run_script="),
            "doctor output should include the canonical indexer run script path"
        );
        assert!(
            rendered.contains("repo_doctor_command="),
            "doctor output should include the repo doctor command"
        );
        assert!(
            rendered.contains("live_doctor_command="),
            "doctor output should include the live doctor command"
        );
        assert!(
            rendered.contains("operator_doctor_command="),
            "doctor output should include the canonical indexer doctor command"
        );
        assert!(
            rendered.contains("acceptance_run_command=cd "),
            "doctor output should include the acceptance run command"
        );
        assert!(
            rendered.contains("repo_run_command=cd "),
            "doctor output should include the repo run command"
        );
        assert!(
            rendered.contains("live_run_command=cd "),
            "doctor output should include the live run command"
        );
        assert!(
            rendered.contains("managed_live_run_command="),
            "doctor output should include the managed live run command"
        );
        assert!(
            rendered.contains("operator_run_command=cd "),
            "doctor output should include the canonical indexer run command"
        );
        assert!(
            rendered.contains("full_run_command=cd "),
            "doctor output should include the full run command"
        );
        assert!(
            rendered.contains("managed_full_run_command="),
            "doctor output should include the managed full run command"
        );
    }

    #[test]
    fn combined_family_validation_script_command_acceptance_renders_extensibility_then_repo_gate() {
        let script_path = combined_family_validation_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("acceptance")
            .env(
                "DATABASE_URL",
                "postgres://example:secret@127.0.0.1:5888/combined_family_validation_command_db",
            )
            .env("TYCHO_COMBINED_FAMILY_DB_NAME", "combined_family_validation_command_gate")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:4242")
            .env("FYND_E2E_RPC_URL", "https://rpc.mevblocker.io")
            .env("FYND_E2E_RUST_LOG", "info,tycho_client=info,tycho_simulation=info,fynd=info")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script acceptance command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script acceptance command mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family validation script acceptance command should emit utf8");
        assert!(
            rendered.contains("custom_registry_loads_future_family_from_yaml_entrypoint"),
            "acceptance command should include the repo-local extensibility contract gate"
        );
        assert!(
            rendered.contains("TEST_BINARY_BY_ENTRY"),
            "acceptance command should execute the extensibility gate through the manifest-backed executable index"
        );
        assert!(
            rendered.contains("\"${TEST_BINARY_BY_ENTRY[${test_name}]}\" \"${TEST_FULL_NAME_BY_ENTRY[${test_name}]}\" --exact --nocapture"),
            "acceptance command should execute the extensibility gate through the manifest-backed executable-index loop"
        );
        assert!(
            rendered.contains("export DATABASE_URL='postgres://example:secret@127.0.0.1:5888/combined_family_validation_command_gate'"),
            "acceptance command should include the isolated DB gate database URL"
        );
        assert!(
            rendered.contains("export TYCHO_REQUIRE_TEST_DB=1"),
            "acceptance command should keep the strict DB gate contract"
        );
        assert!(
            rendered.contains("TEST_BINARY=\"$(cargo test -p tycho-indexer --bin tycho-indexer --no-run 2>&1"),
            "acceptance command should resolve the tycho-indexer test binary before executing the gate"
        );
        assert!(
            rendered.contains("\"${TEST_BINARY}\" \"${test_name}\" --exact --nocapture"),
            "acceptance command should include the repo-local DB-backed gate"
        );
        assert!(
            !rendered.contains("cargo test --test e2e_quote quote_returns_route_for_combined_uniswap_family -- --ignored --nocapture"),
            "acceptance command should not include the live route-return test"
        );
        assert!(
            !rendered.contains("cargo test --test e2e_quote quote_settles_within_encoded_bounds_at_quote_block_for_combined_uniswap_family -- --ignored --nocapture"),
            "acceptance command should not include the live settlement test"
        );
    }

    #[test]
    fn combined_family_validation_script_acceptance_supports_manifest_override() {
        let script_path = combined_family_validation_script_path();
        let custom_manifest = write_temp_combined_family_db_gate_manifest_for_tests(
            "combined-family-validation-override",
            &[
                "test_serial_db::shared_bootstrap_seed_universe_spec_supports_non_uniswap_family_registry",
                "test_serial_db::repo_combined_family_bootstrap_pool_seeds_are_derived_from_repo_config",
            ],
        );
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("acceptance")
            .env(
                "DATABASE_URL",
                "postgres://example:secret@127.0.0.1:5888/combined_family_validation_override_db",
            )
            .env("TYCHO_COMBINED_FAMILY_DB_NAME", "combined_family_validation_override_gate")
            .env("TYCHO_COMBINED_FAMILY_DB_TEST_MANIFEST", &custom_manifest)
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:4242")
            .env("FYND_E2E_RPC_URL", "https://rpc.mevblocker.io")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script acceptance command mode with manifest override at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script acceptance command mode with manifest override should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout).expect(
            "combined-family validation script acceptance manifest override should emit utf8",
        );
        assert!(
            rendered.contains("custom_registry_loads_future_family_from_yaml_entrypoint"),
            "acceptance command should still include the extensibility contract gate"
        );
        assert!(
            rendered.contains(
                "shared_bootstrap_seed_universe_spec_supports_non_uniswap_family_registry"
            ),
            "acceptance command should include the overridden DB gate tests"
        );
        assert!(
            rendered
                .contains("repo_combined_family_bootstrap_pool_seeds_are_derived_from_repo_config"),
            "acceptance command should include every overridden DB gate test"
        );
        assert!(
            !rendered.contains("combined_family_runner_replays_fixture_backed_v2_and_v3_history_slice_in_one_shared_session"),
            "acceptance command should no longer be forced to the default DB gate manifest when override is set"
        );

        let _ = std::fs::remove_file(custom_manifest);
    }

    #[test]
    fn combined_family_validation_script_command_all_renders_stable_commands() {
        let script_path = combined_family_validation_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("all")
            .env(
                "DATABASE_URL",
                "postgres://example:secret@127.0.0.1:5888/combined_family_validation_command_db",
            )
            .env("TYCHO_COMBINED_FAMILY_DB_NAME", "combined_family_validation_command_gate")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:4242")
            .env("FYND_E2E_RPC_URL", "https://rpc.mevblocker.io")
            .env("FYND_E2E_RUST_LOG", "info,tycho_client=info,tycho_simulation=info,fynd=info")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script command mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family validation script command mode should emit utf8 commands");
        assert!(
            rendered.contains("export DATABASE_URL='postgres://example:secret@127.0.0.1:5888/combined_family_validation_command_gate'"),
            "command output should include the isolated DB gate database URL"
        );
        assert!(
            !rendered.contains("custom_registry_loads_future_family_from_yaml_entrypoint"),
            "all mode should remain the repo DB gate plus live gate surface rather than the acceptance surface"
        );
        assert!(
            rendered.contains("export TYCHO_REQUIRE_TEST_DB=1"),
            "command output should keep the strict DB gate contract"
        );
        assert!(
            rendered.contains("CREATE DATABASE"),
            "command output should create the isolated validation database"
        );
        assert!(
            rendered.contains(
                "TEST_BINARY=\"$(cargo test -p tycho-indexer --bin tycho-indexer --no-run 2>&1"
            ),
            "command output should resolve the tycho-indexer test binary before executing the gate"
        );
        assert!(
            rendered.contains("\"${TEST_BINARY}\" \"${test_name}\" --exact --nocapture"),
            "command output should include the repo-local DB-backed gate"
        );
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                combined_family_fynd_route_test_name()
            )),
            "command output should include the live route-return test"
        );
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                combined_family_fynd_settlement_test_name()
            )),
            "command output should include the live settlement test"
        );
    }

    #[test]
    fn combined_family_validation_script_command_full_renders_acceptance_then_live() {
        let script_path = combined_family_validation_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("full")
            .env(
                "DATABASE_URL",
                "postgres://example:secret@127.0.0.1:5888/combined_family_validation_full_db",
            )
            .env("TYCHO_COMBINED_FAMILY_DB_NAME", "combined_family_validation_full_gate")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:4242")
            .env("FYND_E2E_RPC_URL", "https://rpc.mevblocker.io")
            .env("FYND_E2E_RUST_LOG", "info,tycho_client=info,tycho_simulation=info,fynd=info")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script full command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script full command mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family validation script full command mode should emit utf8");
        assert!(
            rendered.contains("custom_registry_loads_future_family_from_yaml_entrypoint"),
            "full mode should include the extensibility contract gate before live validation"
        );
        assert!(
            rendered.contains("TEST_BINARY_BY_ENTRY"),
            "full mode should execute the extensibility gate through the manifest-backed executable index"
        );
        assert!(
            rendered.contains("export DATABASE_URL='postgres://example:secret@127.0.0.1:5888/combined_family_validation_full_gate'"),
            "full mode should include the isolated DB gate database URL"
        );
        assert!(
            rendered.contains("export TYCHO_REQUIRE_TEST_DB=1"),
            "full mode should keep the strict DB gate contract"
        );
        assert!(
            rendered.contains("\"${TEST_BINARY}\" \"${test_name}\" --exact --nocapture"),
            "full mode should include the repo-local DB-backed gate"
        );
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                combined_family_fynd_route_test_name()
            )),
            "full mode should include the live route-return test"
        );
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                combined_family_fynd_settlement_test_name()
            )),
            "full mode should include the live settlement test"
        );
    }

    #[test]
    fn combined_family_validation_script_supports_live_selection_override() {
        let script_path = combined_family_validation_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("live")
            .env("TYCHO_COMBINED_FAMILY_LIVE_SELECTION", "route")
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:4242")
            .env("FYND_E2E_RPC_URL", "https://rpc.mevblocker.io")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script live command mode with selection override at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script live command mode with selection override should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family validation script live command override should emit utf8");
        assert!(
            rendered.contains(&format!(
                "cargo test --test e2e_quote {} -- --ignored --nocapture",
                combined_family_fynd_route_test_name()
            )),
            "live command should include the selected route test"
        );
        assert!(
            !rendered.contains(&combined_family_fynd_settlement_test_name()),
            "live command should not include the settlement test when route-only selection is requested"
        );
    }

    #[test]
    fn combined_family_validation_script_supports_managed_live_command_modes() {
        let script_path = combined_family_validation_script_path();
        let live_output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("live-managed")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script managed live command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            live_output.status.success(),
            "combined-family validation script managed live command mode should succeed, got {:?}",
            live_output.status.code()
        );
        let live_rendered = String::from_utf8(live_output.stdout)
            .expect("combined-family validation script managed live command should emit utf8");
        assert_eq!(
            live_rendered.trim(),
            format!("{} run-live-managed", script_path.display()),
            "managed live command should render the script's canonical managed entrypoint"
        );

        let full_output = std::process::Command::new(&script_path)
            .arg("command")
            .arg("full-managed")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script managed full command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            full_output.status.success(),
            "combined-family validation script managed full command mode should succeed, got {:?}",
            full_output.status.code()
        );
        let full_rendered = String::from_utf8(full_output.stdout)
            .expect("combined-family validation script managed full command should emit utf8");
        assert_eq!(
            full_rendered.trim(),
            format!("{} run-full-managed", script_path.display()),
            "managed full command should render the script's canonical managed entrypoint"
        );
    }

    #[test]
    fn combined_family_validation_script_managed_readiness_requires_live_prerequisites() {
        use std::os::unix::fs::PermissionsExt;

        let script_path = combined_family_validation_script_path();
        let temp_root = std::env::temp_dir().join(format!(
            "combined-family-managed-readiness-{}-{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        let fake_bin = temp_root.join("bin");
        let fake_fynd = temp_root.join("fynd");
        std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
        std::fs::create_dir_all(&fake_fynd).expect("create fake fynd dir");
        std::fs::write(fake_fynd.join("Cargo.toml"), "[package]\nname='fynd'\nversion='0.0.0'\n")
            .expect("write fake fynd Cargo.toml");

        let write_fake_exec = |name: &str, body: &str| {
            let path = fake_bin.join(name);
            std::fs::write(&path, body)
                .unwrap_or_else(|err| panic!("write fake executable {}: {err}", path.display()));
            let mut perms = std::fs::metadata(&path)
                .expect("read fake executable metadata")
                .permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).expect("chmod fake executable");
        };

        write_fake_exec(
            "docker",
            "#!/usr/bin/env bash\nif [[ \"${1:-}\" == \"info\" ]]; then exit 0; fi\nexit 0\n",
        );
        write_fake_exec("psql", "#!/usr/bin/env bash\nexit 0\n");
        write_fake_exec("cargo", "#!/usr/bin/env bash\nexit 0\n");
        write_fake_exec("curl", "#!/usr/bin/env bash\nexit 1\n");

        let path_env = format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", fake_bin.to_string_lossy());
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env("PATH", path_env)
            .env("FYND_REPO_ROOT", &fake_fynd)
            .env("DATABASE_URL", "postgres://example:secret@127.0.0.1:5999/managed_ready_db")
            .env("SUBSTREAMS_API_TOKEN", "token")
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:1")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script managed-readiness doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script managed-readiness doctor mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("managed-readiness doctor mode should emit utf8 diagnostics");
        assert!(
            rendered.contains("repo_ready=true"),
            "fake docker/psql environment should make the repo gate look ready"
        );
        assert!(
            rendered.contains("extensibility_ready=true"),
            "fake cargo environment should make the extensibility gate look ready"
        );
        assert!(
            rendered.contains("acceptance_ready=true"),
            "repo-local acceptance readiness should become true once extensibility and DB gates are ready, even if live validation is still unavailable"
        );
        assert!(
            rendered.contains("operator_ready=true"),
            "fake cargo/psql plus token should make the operator gate look ready"
        );
        assert!(
            rendered.contains("full_ready=false"),
            "full readiness should remain false until the live gate is ready as well"
        );
        assert!(
            rendered.contains("live_ready=false"),
            "missing live test prerequisites should keep the unmanaged live gate unready"
        );
        assert!(
            rendered.contains("managed_live_ready=false"),
            "managed live readiness must still require the Fynd live prerequisites, not just operator readiness"
        );
        assert!(
            rendered.contains("managed_full_ready=false"),
            "managed full readiness must stay false when managed live prerequisites are missing"
        );

        let _ = std::fs::remove_dir_all(&temp_root);
    }

    #[test]
    fn combined_family_validation_script_managed_readiness_turns_true_when_operator_and_live_inputs_are_ready(
    ) {
        use std::os::unix::fs::PermissionsExt;

        let script_path = combined_family_validation_script_path();
        let temp_root = std::env::temp_dir().join(format!(
            "combined-family-managed-ready-positive-{}-{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ));
        let fake_bin = temp_root.join("bin");
        let fake_fynd = temp_root.join("fynd");
        std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
        std::fs::create_dir_all(&fake_fynd).expect("create fake fynd dir");
        std::fs::write(fake_fynd.join("Cargo.toml"), "[package]\nname='fynd'\nversion='0.0.0'\n")
            .expect("write fake fynd Cargo.toml");
        std::fs::create_dir_all(fake_fynd.join("tests")).expect("create fake fynd tests dir");
        std::fs::write(fake_fynd.join("tests").join("e2e_quote.rs"), "// fake live e2e\n")
            .expect("write fake fynd e2e_quote.rs");

        let write_fake_exec = |name: &str, body: &str| {
            let path = fake_bin.join(name);
            std::fs::write(&path, body)
                .unwrap_or_else(|err| panic!("write fake executable {}: {err}", path.display()));
            let mut perms = std::fs::metadata(&path)
                .expect("read fake executable metadata")
                .permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).expect("chmod fake executable");
        };

        write_fake_exec(
            "docker",
            "#!/usr/bin/env bash\nif [[ \"${1:-}\" == \"info\" ]]; then exit 0; fi\nexit 0\n",
        );
        write_fake_exec("psql", "#!/usr/bin/env bash\nexit 0\n");
        write_fake_exec("cargo", "#!/usr/bin/env bash\nexit 0\n");
        write_fake_exec("curl", "#!/usr/bin/env bash\nexit 0\n");

        let path_env = format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", fake_bin.to_string_lossy());
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env("PATH", path_env)
            .env("FYND_REPO_ROOT", &fake_fynd)
            .env("DATABASE_URL", "postgres://example:secret@127.0.0.1:5999/managed_full_ready_db")
            .env("SUBSTREAMS_API_TOKEN", "token")
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:4242")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script managed-ready-positive doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family validation script managed-ready-positive doctor mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("managed-ready-positive doctor mode should emit utf8 diagnostics");
        assert!(
            rendered.contains("extensibility_ready=true"),
            "fake cargo environment should make the extensibility gate ready"
        );
        assert!(
            rendered.contains("repo_ready=true"),
            "fake docker/psql environment should make the repo DB gate ready"
        );
        assert!(
            rendered.contains("live_ready=true"),
            "fake curl plus fake Fynd repo should make the live gate ready"
        );
        assert!(
            rendered.contains("acceptance_ready=true"),
            "acceptance readiness should be true once extensibility and repo gates are ready"
        );
        assert!(
            rendered.contains("operator_ready=true"),
            "fake cargo/psql plus token should make the managed indexer operator gate ready"
        );
        assert!(
            rendered.contains("managed_live_ready=true"),
            "managed live readiness should become true once operator and live prerequisites are both ready"
        );
        assert!(
            rendered.contains("managed_full_ready=true"),
            "managed full readiness should become true once acceptance and managed live readiness are both true"
        );
        assert!(
            rendered.contains("full_ready=true"),
            "full readiness should become true once acceptance and live gates are both ready"
        );
        assert!(
            rendered.contains("ready=true"),
            "aggregate readiness should become true once every validation surface is ready"
        );

        let _ = std::fs::remove_dir_all(&temp_root);
    }

    #[test]
    fn combined_family_validation_script_strict_doctor_fails_when_any_gate_is_unready() {
        let script_path = combined_family_validation_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .arg("--strict")
            .env(
                "DATABASE_URL",
                "postgres://example:secret@127.0.0.1:1/combined_family_validation_strict_db",
            )
            .env("FYND_REPO_ROOT", combined_family_fynd_repo_root())
            .env("FYND_E2E_TYCHO_URL", "127.0.0.1:1")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family validation script strict doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            !output.status.success(),
            "combined-family validation script strict doctor mode should fail when any gate is unready"
        );
        assert_eq!(output.status.code(), Some(1));

        let rendered = String::from_utf8(output.stdout).expect(
            "combined-family validation script strict doctor mode should emit utf8 diagnostics",
        );
        assert!(
            rendered.contains("ready=false"),
            "strict doctor output should report aggregate readiness failure"
        );
        assert!(
            rendered.contains("acceptance_ready=false"),
            "strict doctor output should report repo-local acceptance failure"
        );
        assert!(
            rendered.contains("extensibility_ready=true"),
            "strict doctor output should still report extensibility gate readiness"
        );
        assert!(
            rendered.contains("full_ready=false"),
            "strict doctor output should report full validation readiness failure"
        );
        assert!(
            rendered.contains("repo_ready=false"),
            "strict doctor output should report repo gate failure"
        );
        assert!(
            rendered.contains("live_ready=false"),
            "strict doctor output should report live gate failure"
        );
        assert!(
            rendered.contains("operator_ready=false"),
            "strict doctor output should still surface canonical indexer operator readiness"
        );
        assert!(
            rendered.contains("managed_live_ready=false"),
            "strict doctor output should still report managed live readiness"
        );
        assert!(
            rendered.contains("managed_full_ready=false"),
            "strict doctor output should still report managed full readiness"
        );
    }

    #[test]
    fn combined_family_validation_script_stays_aligned_with_repo_workflow() {
        let script_path = combined_family_validation_script_path();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!(
                "expected to read combined-family validation script at {}: {err}",
                script_path.display()
            )
        });

        assert!(script.contains("doctor [--strict]"), "script should advertise the doctor mode");
        assert!(
            script.contains("command [acceptance|repo|live|live-managed|full|full-managed|all]"),
            "script should advertise the command mode"
        );
        assert!(script.contains("run-acceptance"), "script should advertise acceptance execution");
        assert!(script.contains("run-repo"), "script should advertise repo execution");
        assert!(script.contains("run-live"), "script should advertise live execution");
        assert!(
            script.contains("run-live-managed"),
            "script should advertise managed live execution"
        );
        assert!(script.contains("run-full"), "script should advertise full execution");
        assert!(
            script.contains("run-full-managed"),
            "script should advertise managed full execution"
        );
        assert!(script.contains("run-all"), "script should advertise combined execution");
        assert!(
            script.contains("check-combined-family-extensibility.sh"),
            "script should compose the extensibility contract gate"
        );
        assert!(
            script.contains("check-combined-family-db.sh"),
            "script should compose the repo-local DB gate"
        );
        assert!(
            script.contains("check-combined-family-fynd-live-e2e.sh"),
            "script should compose the live Fynd gate"
        );
        assert!(
            script.contains("TYCHO_COMBINED_FAMILY_LIVE_SELECTION"),
            "script should expose the top-level live selection override"
        );
        assert!(
            script.contains("TYCHO_COMBINED_FAMILY_EXTENSIBILITY_TEST_MANIFEST"),
            "script should document the forwarded extensibility manifest override"
        );
        assert!(
            script.contains("FYND_E2E_ROUTE_TEST"),
            "script should document the forwarded live route test override"
        );
        assert!(
            script.contains("FYND_E2E_SETTLEMENT_TEST"),
            "script should document the forwarded live settlement test override"
        );
        assert!(
            script.contains("FYND_E2E_HEALTH_TIMEOUT_SECS"),
            "script should document the forwarded live health-timeout override"
        );
        assert!(
            script.contains("FYND_E2E_TRADED_N_DAYS_AGO"),
            "script should document the forwarded live traded-n-days-ago override"
        );
        assert!(
            script.contains("FYND_E2E_MIN_TOKEN_QUALITY"),
            "script should document the forwarded live minimum-token-quality override"
        );
        assert!(
            script.contains("FYND_E2E_CONNECTOR_TOKENS"),
            "script should document the forwarded live connector-token override"
        );
        assert!(
            script.contains("TYCHO_COMBINED_FAMILY_LIVE_TEST_MANIFEST"),
            "script should document the forwarded live manifest override"
        );
        assert!(
            script.contains("run-combined-family-indexer.sh"),
            "script should surface the canonical combined-family indexer operator entrypoint"
        );
        assert!(
            script.contains("TYCHO_COMBINED_FAMILY_MANAGED_HEALTH_TIMEOUT_SECS"),
            "script should expose the managed health-timeout override"
        );
        assert!(
            script.contains("mktemp \"${TMPDIR:-/tmp}/tycho-combined-family-indexer.XXXXXX\""),
            "managed live mode should allocate its default indexer log path with a portable mktemp template"
        );
        assert!(
            script.contains(
                "trap 'cleanup_managed_indexer \"${TYCHO_COMBINED_FAMILY_MANAGED_PID:-}\"' EXIT"
            ),
            "managed live mode should capture the spawned indexer pid through the exported managed-pid fallback so cleanup remains safe under set -u"
        );
        assert!(
            script.contains("TYCHO_COMBINED_FAMILY_MANAGED_INDEXER_LOG"),
            "script should expose the managed indexer log override"
        );
    }

    #[test]
    fn combined_family_validation_script_usage_keeps_full_vs_all_distinction() {
        let script_path = combined_family_validation_script_path();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!(
                "expected to read combined-family validation script at {}: {err}",
                script_path.display()
            )
        });

        assert!(
            script.contains(
                "run-full  Execute the repo-local extensibility contract gate, then the DB-backed"
            ),
            "usage text should describe run-full as acceptance plus live, not as the narrower repo-plus-live flow"
        );
        assert!(
            script.contains(
                "run-full-managed Execute the repo-local extensibility contract gate, then the DB-backed"
            ),
            "usage text should describe run-full-managed as acceptance plus managed live"
        );
        assert!(
            script
                .contains("run-all   Execute the repo-local DB gate, then the live Fynd E2E gate."),
            "usage text should keep run-all as the narrower repo-plus-live operator flow"
        );
    }

    #[test]
    fn combined_family_validation_script_run_all_uses_repo_then_live_instead_of_full() {
        let script_path = combined_family_validation_script_path();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!(
                "expected to read combined-family validation script at {}: {err}",
                script_path.display()
            )
        });

        assert!(
            script.contains("run_full() {\n  run_acceptance\n  run_live\n}"),
            "script should keep run-full wired to acceptance plus live"
        );
        assert!(
            script.contains("run_all() {\n  run_repo\n  run_live\n}"),
            "script should keep run-all as the narrower repo-plus-live flow instead of aliasing run-full"
        );
        assert!(
            !script.contains("run_all() {\n  run_full\n}"),
            "script should not let run-all silently widen to the full acceptance-plus-live surface"
        );
    }

    #[test]
    fn combined_family_indexer_run_script_doctor_reports_expected_diagnostics() {
        let script_path = combined_family_indexer_run_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env_remove("SUBSTREAMS_API_TOKEN")
            .env(
                "TYCHO_INDEXER_DATABASE_URL",
                "postgres://example:secret@127.0.0.1:1/combined_family_indexer_doctor_db",
            )
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family indexer run script doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family indexer run script doctor mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family indexer run script doctor mode should emit utf8 diagnostics");
        assert!(
            rendered.contains("ready=false"),
            "doctor output should report missing readiness prerequisites"
        );
        assert!(
            rendered.contains("entrypoint_label=combined-family"),
            "doctor output should report the default operator entrypoint label"
        );
        assert!(
            rendered.contains(
                "extractors_config=crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml"
            ),
            "doctor output should pin the canonical combined-family config"
        );
        assert!(
            rendered.contains("extractors_config_state=present"),
            "doctor output should confirm the canonical combined-family config is present"
        );
        assert!(
            rendered.contains("database_url=postgres://example:secret@127.0.0.1:1/combined_family_indexer_doctor_db"),
            "doctor output should report the effective database url"
        );
        assert!(
            rendered.contains("database_state=unreachable"),
            "doctor output should report unreachable database state when readiness fails"
        );
        assert!(
            rendered.contains("endpoint=https://mainnet.eth.streamingfast.io"),
            "doctor output should report the default endpoint"
        );
        assert!(
            rendered.contains("rpc_url=https://rpc.mevblocker.io"),
            "doctor output should report the default rpc url"
        );
        assert!(
            rendered.contains("auth_api_key_state=set"),
            "doctor output should report the default auth api key contract"
        );
        assert!(
            rendered.contains("substreams_api_token_state=missing"),
            "doctor output should report missing substreams api token state"
        );
        assert!(
            rendered.contains("rust_log=info"),
            "doctor output should report the default rust log"
        );
    }

    #[test]
    fn combined_family_indexer_run_script_command_renders_canonical_combined_entrypoint() {
        let script_path = combined_family_indexer_run_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("command")
            .env("AUTH_API_KEY", "operator-key")
            .env("SUBSTREAMS_API_TOKEN", "test-substreams-token")
            .env("TYCHO_INDEXER_ENDPOINT", "https://streamingfast.example")
            .env(
                "TYCHO_INDEXER_DATABASE_URL",
                "postgres://example:secret@127.0.0.1:5888/combined_family_indexer_command_db",
            )
            .env("TYCHO_INDEXER_RPC_URL", "https://rpc.example")
            .env(
                "TYCHO_INDEXER_EXTRACTORS_CONFIG",
                "crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml",
            )
            .env("TYCHO_INDEXER_RUST_LOG", "debug,tycho_indexer=info")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family indexer run script command mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family indexer run script command mode should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family indexer run script command mode should emit utf8 commands");
        assert!(rendered.contains("cd "), "command output should switch to the repo root");
        assert!(
            rendered.contains("export AUTH_API_KEY=operator-key"),
            "command output should export the auth api key explicitly"
        );
        assert!(
            rendered.contains("export SUBSTREAMS_API_TOKEN=test-substreams-token"),
            "command output should export the substreams api token before cargo run"
        );
        assert!(
            rendered.contains("export RUST_LOG=debug,tycho_indexer=info"),
            "command output should export rust log explicitly"
        );
        assert!(
            rendered.contains("cargo run --bin tycho-indexer --"),
            "command output should use cargo run for the canonical operator entrypoint"
        );
        assert!(
            rendered.contains("--endpoint https://streamingfast.example"),
            "command output should include the effective endpoint"
        );
        assert!(
            rendered.contains("--database-url 'postgres://example:secret@127.0.0.1:5888/combined_family_indexer_command_db'"),
            "command output should include the effective database url"
        );
        assert!(
            rendered.contains("--rpc-url https://rpc.example"),
            "command output should include the effective rpc url"
        );
        assert!(
            rendered.contains(
                "--extractors-config crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml"
            ),
            "command output should pin the canonical combined-family extractor config"
        );
        assert!(
            rendered.contains("--api_token \"$SUBSTREAMS_API_TOKEN\""),
            "command output should avoid the inline token expansion bug"
        );
    }

    #[test]
    fn combined_family_indexer_run_script_supports_entrypoint_label_override() {
        let script_path = combined_family_indexer_run_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .env_remove("SUBSTREAMS_API_TOKEN")
            .env("TYCHO_INDEXER_ENTRYPOINT_LABEL", "future-family")
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family indexer run script doctor mode with label override at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            output.status.success(),
            "combined-family indexer run script doctor mode with label override should succeed, got {:?}",
            output.status.code()
        );

        let rendered = String::from_utf8(output.stdout)
            .expect("combined-family indexer run script doctor mode should emit utf8 diagnostics");
        assert!(
            rendered.contains("entrypoint_label=future-family"),
            "doctor output should honor the operator entrypoint label override"
        );
        assert!(
            !rendered.contains("entrypoint_label=combined-family"),
            "doctor output should not pin the default label when overridden"
        );
    }

    #[test]
    fn combined_family_indexer_run_script_strict_doctor_fails_when_token_is_missing() {
        let script_path = combined_family_indexer_run_script_path();
        let output = std::process::Command::new(&script_path)
            .arg("doctor")
            .arg("--strict")
            .env_remove("SUBSTREAMS_API_TOKEN")
            .env(
                "TYCHO_INDEXER_DATABASE_URL",
                "postgres://example:secret@127.0.0.1:1/combined_family_indexer_strict_db",
            )
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "expected combined-family indexer run script strict doctor mode at {}: {err}",
                    script_path.display()
                )
            });
        assert!(
            !output.status.success(),
            "combined-family indexer run script strict doctor mode should fail when readiness is false"
        );
        assert_eq!(output.status.code(), Some(1));

        let rendered = String::from_utf8(output.stdout).expect(
            "combined-family indexer run script strict doctor mode should emit utf8 diagnostics",
        );
        assert!(
            rendered.contains("ready=false"),
            "strict doctor output should report the readiness failure"
        );
        assert!(
            rendered.contains("substreams_api_token_state=missing"),
            "strict doctor output should explain the missing token state"
        );
    }

    #[test]
    fn combined_family_indexer_run_script_stays_aligned_with_combined_repo_workflow() {
        let script_path = combined_family_indexer_run_script_path();
        let script = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
            panic!(
                "expected to read combined-family indexer run script at {}: {err}",
                script_path.display()
            )
        });

        assert!(script.contains("doctor [--strict]"), "script should advertise the doctor mode");
        assert!(script.contains("command"), "script should advertise the command mode");
        assert!(script.contains("run"), "script should advertise the run mode");
        assert!(
            script.contains("AUTH_API_KEY                  Default: dummy"),
            "script should document the auth api key default"
        );
        assert!(
            script.contains("SUBSTREAMS_API_TOKEN          Required"),
            "script should document the required substreams api token"
        );
        assert!(
            script.contains("TYCHO_INDEXER_ENTRYPOINT_LABEL"),
            "script should expose the operator entrypoint label override"
        );
        assert!(
            script.contains("extractors.uniswap_v2_v3.combined.yaml"),
            "script should pin the canonical combined-family extractor config"
        );
        assert!(
            script.contains("cargo run --bin tycho-indexer --"),
            "script should drive the tycho-indexer binary directly"
        );
        assert!(
            script.contains("--api_token \"$SUBSTREAMS_API_TOKEN\""),
            "script should preserve the safe token expansion pattern"
        );
    }
}
