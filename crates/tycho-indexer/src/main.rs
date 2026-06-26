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
    env, process, slice,
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
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber::EnvFilter;
use tycho_common::{
    models::{
        blockchain::{Block, Transaction},
        contract::AccountDelta,
        Address, Chain, ExtractionState, ImplementationType,
    },
    storage::{ChainGateway, ContractStateGateway, ExtractionStateGateway},
    traits::{AccountExtractor, StorageSnapshotRequest},
    Bytes,
};
#[cfg(test)]
use tycho_common::dto;
#[cfg(test)]
use tycho_common::storage::ProtocolGateway;
use tycho_ethereum::{
    rpc::EthereumRpcClient,
    services::{
        account_extractor::EVMAccountExtractor, token_pre_processor::EthereumTokenPreProcessor,
    },
};
use tycho_indexer::{
    cli::{AnalyzeTokenArgs, Cli, Command, GlobalArgs, IndexArgs, RunSpkgArgs, SubstreamsArgs},
    extractor::{
        chain_state::ChainState,
        family_runtime::{build_resolved_runtime_targets, ResolvedRuntimeTarget},
        protocol_cache::ProtocolMemoryCache,
        runner::{
            build_family_runner, DCIType, ExtractorBuilder, ExtractorConfig, ExtractorHandle,
            ManagedRunner, ProtocolTypeConfig,
        },
        token_analysis_cron::analyze_tokens,
        ExtractionError,
    },
    services::{PlansConfig, ServicesBuilder},
};
#[cfg(test)]
use tycho_indexer::extractor::runner::FamilyRuntimeConfig;
use tycho_storage::postgres::{builder::GatewayBuilder, cache::CachedGateway};

mod config;
mod ot;
#[cfg(test)]
mod testing;
#[cfg(test)]
pub use tycho_indexer::{extractor, pb};

use config::ExtractorConfigs;

type ExtractionTasks = Vec<JoinHandle<Result<(), ExtractionError>>>;
type ServerTasks = Vec<JoinHandle<Result<(), ExtractionError>>>; //TODO: introduce an error type for it

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

    let protocol_systems: Vec<String> = extractors_config
        .extractors
        .keys()
        .cloned()
        .collect();

    let dci_protocols: Vec<String> = extractors_config
        .extractors
        .iter()
        .filter(|(_, cfg)| cfg.dci_plugin.is_some())
        .map(|(name, _)| name.clone())
        .collect();

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
        build_all_extractors(&extractors_config, chain_state, chains, &global_args.endpoint_url, global_args.s3_bucket.as_deref(), &substreams_args.substreams_api_token, &cached_gw, global_args.database_insert_batch_size, &token_processor, &rpc_client, extraction_runtime, substreams_args.enable_partial_blocks)
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
    let mut runners = Vec::new();
    let mut extractor_handles = Vec::new();

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

    let runtime_targets = build_resolved_runtime_targets(&config.extractors)?;

    for target in runtime_targets {
        initialize_accounts_for_runtime_target(&target, rpc_client, cached_gw).await;

        match target {
            ResolvedRuntimeTarget::Family(family) => {
                let runtime = runtime
                    .cloned()
                    .unwrap_or_else(|| tokio::runtime::Handle::current());
                let (family_runner, handles) = build_family_runner(
                    &family,
                    chain_state,
                    endpoint_url,
                    s3_bucket,
                    substreams_api_token,
                    cached_gw,
                    database_insert_batch_size,
                    token_pre_processor,
                    &protocol_cache,
                    rpc_client,
                    Some(runtime),
                    partial_blocks,
                )
                .await?;
                runners.push(family_runner);
                extractor_handles.extend(handles);
            }
            ResolvedRuntimeTarget::Standalone(standalone) => {
                let runtime = runtime
                    .cloned()
                    .unwrap_or_else(|| tokio::runtime::Handle::current());

                let (runner, handle) = ExtractorBuilder::new(
                    standalone.extractor_config,
                    endpoint_url,
                    s3_bucket,
                    substreams_api_token,
                )
                .database_insert_batch_size(database_insert_batch_size)
                .partial_blocks(partial_blocks)
                .build(chain_state, cached_gw, token_pre_processor, &protocol_cache, rpc_client)
                .await?
                .set_runtime(runtime)
                .into_runner()
                .await?;

                runners.push(ManagedRunner::Single(runner));
                extractor_handles.push(handle);
            }
        }
    }

    Ok((runners, extractor_handles))
}

async fn initialize_accounts_for_runtime_target(
    target: &ResolvedRuntimeTarget<'_>,
    rpc_client: &EthereumRpcClient,
    cached_gw: &CachedGateway,
) {
    let chain = target.chain();
    for extractor_config in target.extractor_configs() {
        initialize_accounts(
            extractor_config
                .initialized_accounts
                .clone(),
            extractor_config.initialized_accounts_block,
            rpc_client,
            chain,
            cached_gw,
        )
        .await;
    }
}

async fn with_transaction<F, Fut, R>(gw: &CachedGateway, block: &Block, f: F) -> R
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = R>,
{
    gw.start_transaction(block, Some("accountExtractor"))
        .await;
    let result = f().await;
    gw.commit_transaction(0)
        .await
        .expect("Failed to commit transaction");
    result
}

#[instrument(skip_all, fields(n_accounts = %accounts.len(), block_id = block_id))]
async fn initialize_accounts(
    accounts: Vec<Address>,
    block_id: u64,
    rpc: &EthereumRpcClient,
    chain: Chain,
    cached_gw: &CachedGateway,
) {
    if accounts.is_empty() {
        return;
    }
    let (block, extracted_accounts) = get_accounts_data(accounts, block_id, rpc, chain).await;

    info!(block_number = block.number, "Initializing accounts");

    let tx = Transaction {
        hash: Bytes::random(32), //TODO: remove Bytes length assumption
        block_hash: block.hash.clone(),
        from: Bytes::from([0u8; 20]),
        to: None,
        index: 0,
    };

    // First transaction
    with_transaction(cached_gw, &block, || async {
        cached_gw
            .upsert_block(slice::from_ref(&block))
            .await
            .expect("Failed to insert block");

        cached_gw
            .upsert_tx(slice::from_ref(&tx))
            .await
            .expect("Failed to insert tx");
    })
    .await;

    // Process account updates
    for account_update in extracted_accounts.into_values() {
        with_transaction(cached_gw, &block, || async {
            let new_account = account_update.ref_into_account(&tx);
            info!(block_number = block.number, contract_address = ?new_account.address, "NewContract");

            // Insert new accounts
            cached_gw
                .insert_contract(&new_account)
                .await
                .expect("Failed to insert contract");
            cached_gw
                .update_contracts(&[(tx.hash.clone(), account_update)])
                .await
                .expect("Failed to update contract");
        })
        .await;
    }

    with_transaction(cached_gw, &block, || async {
        let state = ExtractionState::new(
            "accountExtractor".to_string(),
            chain,
            None,
            "account_cursor".as_bytes(),
            block.hash.clone(),
        );

        cached_gw
            .save_state(&state)
            .await
            .expect("Failed to save cursor");
    })
    .await;
}

async fn get_accounts_data(
    accounts: Vec<Address>,
    block_id: u64,
    rpc: &EthereumRpcClient,
    chain: Chain,
) -> (Block, HashMap<Bytes, AccountDelta>) {
    let account_extractor = EVMAccountExtractor::new(rpc, chain);

    let block = account_extractor
        .get_block_data(block_id)
        .await
        .expect("Failed to get block data");

    let requests = accounts
        .iter()
        .map(|address| StorageSnapshotRequest { address: address.clone(), slots: None })
        .collect::<Vec<_>>();

    let extracted_accounts: HashMap<Bytes, AccountDelta> = account_extractor
        .get_accounts_at_block(&block, &requests)
        .await
        .expect("Failed to extract accounts");
    (block, extracted_accounts)
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

    use alloy::primitives::Address as AlloyAddress;
    use once_cell::sync::Lazy;
    use prost::Message;
    use substreams::store::StoreGet;
    use crate::testing::{
        family_block_response, family_block_response_from_block_changes, scripted_session_response,
        v2_pair_created_block, v3_pool_created_block,
    };
    use tycho_storage::postgres::testing::run_against_db;

    use super::*;

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
        run_against_db(|_| async move {
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
        run_against_db(|_| async move {
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();
            let missing_member_v2_spkg = std::env::temp_dir()
                .join(format!("tycho-indexer-missing-v2-{}-{}.spkg", process::id(), "member"));
            let missing_member_v3_spkg = std::env::temp_dir()
                .join(format!("tycho-indexer-missing-v3-{}-{}.spkg", process::id(), "member"));
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
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
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
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-conflicting-family-stop-block-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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

            let unique = format!(
                "{}-{}",
                process::id(),
                chrono::Utc::now()
                    .timestamp_nanos_opt()
                    .unwrap_or_default()
            );
            let shared_spkg_path = std::env::temp_dir()
                .join(format!("tycho-indexer-family-defaults-{unique}.spkg"));
            std::fs::write(
                &shared_spkg_path,
                tycho_indexer::pb::sf::substreams::v1::Package::default().encode_to_vec(),
            )
            .expect("write temp spkg");
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

            let config_path = std::env::temp_dir()
                .join(format!("tycho-indexer-family-defaults-{unique}.yaml"));
            std::fs::write(
                &config_path,
                format!(
                    r#"
family_runtimes:
  uniswap:
    shared_spkg: "{shared_spkg_path}"
    shared_module: "map_uniswap_family_protocol_changes"
    stop_block: 123
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1
    start_block: 42
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    module_name: "v2_map_pool_events"
    family_runtime:
      family: "uniswap"
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1
    start_block: 42
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    module_name: "v3_map_protocol_changes"
    family_runtime:
      family: "uniswap"
"#
                ),
            )
            .expect("write temp config");

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
                    b"cursor@123-v2",
                    persisted_block.hash.clone(),
                ))
                .await
                .expect("persist v2 extraction state");
            cached_gw
                .save_state(&ExtractionState::new(
                    "uniswap_v3".to_string(),
                    chain,
                    None,
                    b"cursor@123-v3",
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-resume-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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

            let requests = captured.lock().unwrap();
            assert_eq!(requests.len(), 1, "expected a single substreams request");
            assert_eq!(
                requests[0].start_block_num, 124,
                "family runner should resume from last persisted block + 1"
            );
            assert!(
                requests[0].start_cursor.is_empty(),
                "fresh process restart should still start from block number, not hot cursor"
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_persists_dynamically_admitted_component() {
        use prost::Message;
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{
                response::Message as ResponseMessage, BlockScopedData, MapModuleOutput, Response,
                SessionInit,
            },
            pb::sf::substreams::v1::Clock,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::token::Token;
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        fn session_response(start_block: u64) -> Response {
            Response {
                message: Some(ResponseMessage::Session(SessionInit {
                    trace_id: format!("trace-{start_block}"),
                    resolved_start_block: start_block,
                    linear_handoff_block: start_block,
                    max_parallel_workers: 1,
                    attestation_public_key: String::new(),
                    chain_head: start_block,
                    blocks_to_process_before_start_block: 0,
                    effective_blocks_to_process_before_start_block: 0,
                    blocks_to_process_after_start_block: 0,
                    effective_blocks_to_process_after_start_block: 0,
                })),
            }
        }

        fn family_block_response(number: u64, changes: Vec<substreams::TransactionChanges>) -> Response {
            let family_changes = substreams::BlockChanges {
                block: Some(substreams::Block {
                    number,
                    hash: vec![number as u8; 32],
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    ts: 1_718_000_000,
                }),
                changes,
                storage_changes: vec![],
            };

            Response {
                message: Some(ResponseMessage::BlockScopedData(BlockScopedData {
                    output: Some(MapModuleOutput {
                        name: "map_uniswap_family_protocol_changes".to_string(),
                        map_output: Some(prost_types::Any {
                            type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                            value: family_changes.encode_to_vec(),
                        }),
                        debug_info: None,
                    }),
                    clock: Some(Clock {
                        id: number.to_string(),
                        number,
                        timestamp: None,
                    }),
                    cursor: format!("cursor@{number}"),
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

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    session_response(42),
                    family_block_response(
                        42,
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
                    family_block_response(43, vec![]),
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-dynamic-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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

            let all_components = cached_gw
                .get_protocol_components(
                    &chain,
                    None,
                    None,
                    None,
                    None,
                )
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
            let rpc_body = rpc_body.unwrap_or_else(|| {
                panic!("protocol component never became queryable through rpc")
            });
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
            let state_body = state_body.unwrap_or_else(|| {
                panic!("protocol state never became queryable through rpc")
            });
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
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{
                response::Message as ResponseMessage, BlockScopedData, MapModuleOutput, Response,
                SessionInit,
            },
            pb::sf::substreams::v1::Clock,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::token::Token;
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        fn session_response(start_block: u64) -> Response {
            Response {
                message: Some(ResponseMessage::Session(SessionInit {
                    trace_id: format!("trace-follow-up-{start_block}"),
                    resolved_start_block: start_block,
                    linear_handoff_block: start_block,
                    max_parallel_workers: 1,
                    attestation_public_key: String::new(),
                    chain_head: start_block,
                    blocks_to_process_before_start_block: 0,
                    effective_blocks_to_process_before_start_block: 0,
                    blocks_to_process_after_start_block: 0,
                    effective_blocks_to_process_after_start_block: 0,
                })),
            }
        }

        fn family_block_response(number: u64, changes: Vec<substreams::TransactionChanges>) -> Response {
            let family_changes = substreams::BlockChanges {
                block: Some(substreams::Block {
                    number,
                    hash: vec![number as u8; 32],
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    ts: 1_718_000_000 + number,
                }),
                changes,
                storage_changes: vec![],
            };

            Response {
                message: Some(ResponseMessage::BlockScopedData(BlockScopedData {
                    output: Some(MapModuleOutput {
                        name: "map_uniswap_family_protocol_changes".to_string(),
                        map_output: Some(prost_types::Any {
                            type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                            value: family_changes.encode_to_vec(),
                        }),
                        debug_info: None,
                    }),
                    clock: Some(Clock {
                        id: number.to_string(),
                        number,
                        timestamp: None,
                    }),
                    cursor: format!("cursor-follow-up@{number}"),
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

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    session_response(42),
                    family_block_response(
                        42,
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
                    family_block_response(
                        43,
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
                    family_block_response(44, vec![]),
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-follow-up-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{
                response::Message as ResponseMessage, BlockScopedData, BlockUndoSignal,
                MapModuleOutput, Response, SessionInit,
            },
            pb::sf::substreams::v1::{BlockRef, Clock},
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::token::Token;
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        fn session_response(start_block: u64) -> Response {
            Response {
                message: Some(ResponseMessage::Session(SessionInit {
                    trace_id: format!("trace-revert-{start_block}"),
                    resolved_start_block: start_block,
                    linear_handoff_block: start_block,
                    max_parallel_workers: 1,
                    attestation_public_key: String::new(),
                    chain_head: start_block,
                    blocks_to_process_before_start_block: 0,
                    effective_blocks_to_process_before_start_block: 0,
                    blocks_to_process_after_start_block: 0,
                    effective_blocks_to_process_after_start_block: 0,
                })),
            }
        }

        fn family_block_response(number: u64, changes: Vec<substreams::TransactionChanges>) -> Response {
            let family_changes = substreams::BlockChanges {
                block: Some(substreams::Block {
                    number,
                    hash: vec![number as u8; 32],
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    ts: 1_718_900_000 + number,
                }),
                changes,
                storage_changes: vec![],
            };

            Response {
                message: Some(ResponseMessage::BlockScopedData(BlockScopedData {
                    output: Some(MapModuleOutput {
                        name: "map_uniswap_family_protocol_changes".to_string(),
                        map_output: Some(prost_types::Any {
                            type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                            value: family_changes.encode_to_vec(),
                        }),
                        debug_info: None,
                    }),
                    clock: Some(Clock {
                        id: number.to_string(),
                        number,
                        timestamp: None,
                    }),
                    cursor: format!("cursor@{number}"),
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

        fn undo_response(last_valid_block: u64) -> Response {
            let block_id = format!(
                "0x{}",
                std::iter::repeat(format!("{:02x}", last_valid_block as u8))
                    .take(32)
                    .collect::<String>()
            );
            Response {
                message: Some(ResponseMessage::BlockUndoSignal(BlockUndoSignal {
                    last_valid_block: Some(BlockRef {
                        id: block_id,
                        number: last_valid_block,
                    }),
                    last_valid_cursor: format!("cursor@{last_valid_block}"),
                })),
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
            let v2_component_id = "v2-reorg-pool";
            let v3_component_id = "v3-reorg-pool";
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    session_response(42),
                    family_block_response(42, vec![]),
                    family_block_response(
                        43,
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
                                        value: Bytes::from(1_000u64).lpad(32, 0).to_vec(),
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
                                        implementation_type:
                                            substreams::ImplementationType::Custom as i32,
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
                                        implementation_type:
                                            substreams::ImplementationType::Custom as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                        ],
                    ),
                    family_block_response(44, vec![]),
                    undo_response(42),
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-revert-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
            assert!(v2_rpc_components.protocol_components.is_empty());

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
            assert!(v3_rpc_components.protocol_components.is_empty());

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
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{
                response::Message as ResponseMessage, BlockScopedData, BlockUndoSignal,
                MapModuleOutput, Response, SessionInit,
            },
            pb::sf::substreams::v1::{BlockRef, Clock},
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::token::Token;
        use tycho_substreams::pb::tycho::evm::v1 as substreams;

        fn session_response(start_block: u64) -> Response {
            Response {
                message: Some(ResponseMessage::Session(SessionInit {
                    trace_id: format!("trace-recover-{start_block}"),
                    resolved_start_block: start_block,
                    linear_handoff_block: start_block,
                    max_parallel_workers: 1,
                    attestation_public_key: String::new(),
                    chain_head: start_block,
                    blocks_to_process_before_start_block: 0,
                    effective_blocks_to_process_before_start_block: 0,
                    blocks_to_process_after_start_block: 0,
                    effective_blocks_to_process_after_start_block: 0,
                })),
            }
        }

        fn family_block_response(number: u64, changes: Vec<substreams::TransactionChanges>) -> Response {
            let family_changes = substreams::BlockChanges {
                block: Some(substreams::Block {
                    number,
                    hash: vec![number as u8; 32],
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    ts: 1_718_910_000 + number,
                }),
                changes,
                storage_changes: vec![],
            };

            Response {
                message: Some(ResponseMessage::BlockScopedData(BlockScopedData {
                    output: Some(MapModuleOutput {
                        name: "map_uniswap_family_protocol_changes".to_string(),
                        map_output: Some(prost_types::Any {
                            type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                            value: family_changes.encode_to_vec(),
                        }),
                        debug_info: None,
                    }),
                    clock: Some(Clock {
                        id: number.to_string(),
                        number,
                        timestamp: None,
                    }),
                    cursor: format!("cursor-recover@{number}"),
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

        fn undo_response(last_valid_block: u64) -> Response {
            let block_id = format!(
                "0x{}",
                std::iter::repeat(format!("{:02x}", last_valid_block as u8))
                    .take(32)
                    .collect::<String>()
            );
            Response {
                message: Some(ResponseMessage::BlockUndoSignal(BlockUndoSignal {
                    last_valid_block: Some(BlockRef {
                        id: block_id,
                        number: last_valid_block,
                    }),
                    last_valid_cursor: format!("cursor-recover@{last_valid_block}"),
                })),
            }
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
            let v2_component_id = "v2-recover-pool";
            let v3_component_id = "v3-recover-pool";
            let token0 = Bytes::from(vec![0xa0; 20]);
            let token1 = Bytes::from(vec![0xc0; 20]);

            let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
                responses: vec![
                    session_response(42),
                    family_block_response(42, vec![]),
                    family_block_response(
                        43,
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
                                        value: Bytes::from(1_000u64).lpad(32, 0).to_vec(),
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
                                        implementation_type:
                                            substreams::ImplementationType::Custom as i32,
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
                                        implementation_type:
                                            substreams::ImplementationType::Custom as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                        ],
                    ),
                    family_block_response(44, vec![]),
                    undo_response(42),
                    family_block_response(
                        43,
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
                                        value: Bytes::from(2_500u64).lpad(32, 0).to_vec(),
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
                                        implementation_type:
                                            substreams::ImplementationType::Custom as i32,
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
                                        implementation_type:
                                            substreams::ImplementationType::Custom as i32,
                                    }),
                                    change: substreams::ChangeType::Creation as i32,
                                }],
                                balance_changes: vec![],
                                entrypoints: vec![],
                                entrypoint_params: vec![],
                            },
                        ],
                    ),
                    family_block_response(
                        44,
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
                                        value: Bytes::from(3_000u64).lpad(32, 0).to_vec(),
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
                    family_block_response(45, vec![]),
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-recover-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                v2_state.entity[0].attributes.get("reserve0"),
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
                v3_state.entity[0].attributes.get("tick"),
                Some(&Bytes::from(13u64).lpad(32, 0))
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
            assert_eq!(v2_rpc_components.protocol_components.len(), 1);

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
            assert_eq!(v3_rpc_components.protocol_components.len(), 1);

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
                v2_rpc_state.states[0].attributes.get("reserve0"),
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
                v3_rpc_state.states[0].attributes.get("tick"),
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
        use ethabi::{ethereum_types::U256, Token as AbiToken};
        use ethereum_uniswap_v2_v3_combined::{
            run_map_uniswap_family_protocol_changes, run_v2_map_pool_events,
            run_v2_map_pools_created,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use ::substreams::store::StoreGet;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, Block as EthBlock, BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            transaction_trace::Type as EthTransactionType, TransactionTraceStatus,
        };
        use tycho_indexer::{
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::{
            contract::AccountDelta,
            contract::Account,
            blockchain::{Block, Transaction},
            token::Token,
            ChangeType, FinancialType, ProtocolType,
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
                    28, 65, 30, 154, 150, 224, 113, 36, 28, 47, 33, 247, 114, 107, 23, 174,
                    137, 227, 202, 180, 199, 139, 229, 14, 6, 43, 3, 169, 255, 251, 186, 209,
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
                    timestamp: Some(Timestamp {
                        seconds: timestamp_secs,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xbb; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt {
                        logs: vec![log],
                        ..Default::default()
                    }),
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
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-factory-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
    async fn combined_family_runner_v3_dynamic_component_from_real_pool_created_block_receives_follow_up_state(
    ) {
        use ethabi::{ethereum_types::{Address, U256}, Token as AbiToken};
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v3_pool_events, build_family_v3_protocol_changes,
            build_uniswap_family_protocol_changes_from_v3_created_pools,
            build_uniswap_family_protocol_changes_from_v3_protocol_changes,
            FamilyV3LiquidityChanges, FamilyV3Pool, FamilyV3TickDeltas,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use ::substreams::{
            pb::substreams::StoreDeltas,
            store::{StoreGet, StoreGetProto},
        };
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, Block as EthBlock, BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            transaction_trace::Type as EthTransactionType, TransactionTraceStatus,
        };
        use tycho_indexer::{
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::token::Token;
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
                        196, 32, 121, 249, 74, 99, 80, 215, 230, 35, 95, 41, 23, 73, 36, 249,
                        40, 204, 42, 200, 24, 235, 100, 254, 216, 0, 78, 17, 95, 188, 202, 103,
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
                        seconds: timestamp_secs,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xde; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt {
                        logs: vec![log],
                        ..Default::default()
                    }),
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
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-v3-dynamic-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                    .with_family_runtime(Some(FamilyRuntimeConfig {
                        family: "uniswap".to_string(),
                        shared_spkg: Some(shared_spkg_path.clone()),
                        shared_module: Some("map_uniswap_family_protocol_changes".to_string()),
                    })),
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
                .get_protocol_components(&chain, Some("uniswap_v3".to_string()), None, None, None)
                .await
                .expect("read combined V3 component universe");
            let component_ids = components
                .entity
                .iter()
                .map(|component| component.id.clone())
                .collect::<Vec<_>>();
            assert!(
                component_ids.iter().any(|id| id == dynamic_component_id),
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
                dynamic_state.entity[0].attributes.get("tick"),
                Some(&Bytes::from(vec![0x07]))
            );

            let _ = std::fs::remove_file(&shared_spkg_path);
        })
        .await;
    }

    #[tokio::test]
    async fn combined_family_runner_restart_applies_v3_follow_up_state_after_dynamic_component_admission(
    ) {
        use ethabi::{ethereum_types::{Address, U256}, Token as AbiToken};
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v3_pool_events, build_family_v3_protocol_changes,
            build_uniswap_family_protocol_changes_from_v3_created_pools,
            build_uniswap_family_protocol_changes_from_v3_protocol_changes,
            FamilyV3LiquidityChanges, FamilyV3Pool, FamilyV3TickDeltas,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use ::substreams::{
            pb::substreams::StoreDeltas,
            store::{StoreGet, StoreGetProto},
        };
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, Block as EthBlock, BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            transaction_trace::Type as EthTransactionType, TransactionTraceStatus,
        };
        use tycho_indexer::{
            pb::sf::substreams::rpc::v2::{
                response::Message as ResponseMessage, BlockScopedData, MapModuleOutput, Response,
                SessionInit,
            },
            pb::sf::substreams::v1::Clock,
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::{
            token::Token,
            FinancialType, ProtocolType,
        };
        use tycho_substreams::pb::tycho::evm::v1 as substreams;
        use tycho_substreams_local::models::{BlockBalanceDeltas, BlockEntityChanges};

        fn session_response(start_block: u64) -> Response {
            Response {
                message: Some(ResponseMessage::Session(SessionInit {
                    trace_id: format!("trace-v3-restart-{start_block}"),
                    resolved_start_block: start_block,
                    linear_handoff_block: start_block,
                    max_parallel_workers: 1,
                    attestation_public_key: String::new(),
                    chain_head: start_block,
                    blocks_to_process_before_start_block: 0,
                    effective_blocks_to_process_before_start_block: 0,
                    blocks_to_process_after_start_block: 0,
                    effective_blocks_to_process_after_start_block: 0,
                })),
            }
        }

        fn family_block_response_from_block_changes(
            family_changes: substreams::BlockChanges,
            cursor_label: &str,
        ) -> Response {
            let number = family_changes
                .block
                .as_ref()
                .expect("family block present")
                .number;
            Response {
                message: Some(ResponseMessage::BlockScopedData(BlockScopedData {
                    output: Some(MapModuleOutput {
                        name: "map_uniswap_family_protocol_changes".to_string(),
                        map_output: Some(prost_types::Any {
                            type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                            value: family_changes.encode_to_vec(),
                        }),
                        debug_info: None,
                    }),
                    clock: Some(Clock {
                        id: number.to_string(),
                        number,
                        timestamp: None,
                    }),
                    cursor: format!("{cursor_label}@{number}"),
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
                    receipt: Some(EthTransactionReceipt {
                        logs: vec![log],
                        ..Default::default()
                    }),
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
                        196, 32, 121, 249, 74, 99, 80, 215, 230, 35, 95, 41, 23, 73, 36, 249,
                        40, 204, 42, 200, 24, 235, 100, 254, 216, 0, 78, 17, 95, 188, 202, 103,
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
                    receipt: Some(EthTransactionReceipt {
                        logs: vec![log],
                        ..Default::default()
                    }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        fn write_family_defaults_config(
            unique: &str,
            shared_spkg_path: &str,
            start_block: i64,
        ) -> std::path::PathBuf {
            let config_path = std::env::temp_dir()
                .join(format!("tycho-indexer-v3-restart-family-defaults-{unique}.yaml"));
            std::fs::write(
                &config_path,
                format!(
                    r#"
family_runtimes:
  uniswap:
    shared_spkg: "{shared_spkg_path}"
    shared_module: "map_uniswap_family_protocol_changes"
extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1
    start_block: {start_block}
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    module_name: "v2_map_pool_events"
    family_runtime:
      family: "uniswap"
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1
    start_block: {start_block}
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    module_name: "v3_map_protocol_changes"
    family_runtime:
      family: "uniswap"
"#
                ),
            )
            .expect("write temp v3 restart family-default config");
            config_path
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
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
                        session_response(63),
                        family_block_response_from_block_changes(
                            family_creation_changes,
                            "cursor-v3-restart",
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-v3-restart-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

            let unique = format!(
                "{}-{}",
                process::id(),
                chrono::Utc::now()
                    .timestamp_nanos_opt()
                    .unwrap_or_default()
            );
            let config_path = write_family_defaults_config(&unique, &shared_spkg_path, 63);
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
                        session_response(64),
                        family_block_response_from_block_changes(
                            family_follow_up_changes,
                            "cursor-v3-restart",
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

            let resumed_config_path =
                write_family_defaults_config(&format!("{unique}-resumed"), &shared_spkg_path, 63);
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
                assert!(
                    requests[0].start_cursor.is_empty(),
                    "fresh restart should resume from persisted branch progress, not a hot cursor"
                );
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
    async fn combined_family_runner_restart_applies_follow_up_state_after_dynamic_component_admission(
    ) {
        use ethabi::{ethereum_types::U256, Token as AbiToken};
        use ethereum_uniswap_v2_v3_combined::{
            build_family_v2_pool_created_block_changes,
            build_family_v2_pool_event_block_changes,
            build_uniswap_family_protocol_changes_from_v2,
            parse_family_v2_pool_created_params,
        };
        use prost::Message;
        use prost_types::Timestamp;
        use ::substreams::store::StoreGet;
        use substreams_ethereum::pb::eth::v2::{
            block::DetailLevel, Block as EthBlock, BlockHeader as EthBlockHeader, Log as EthLog,
            TransactionReceipt as EthTransactionReceipt, TransactionTrace as EthTransactionTrace,
            transaction_trace::Type as EthTransactionType, TransactionTraceStatus,
        };
        use tycho_indexer::{
            substreams::mock::{start_scripted_mock_substreams, MockSubstreamsScript},
        };
        use tycho_common::models::{
            token::Token,
            FinancialType, ProtocolType,
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
                    28, 65, 30, 154, 150, 224, 113, 36, 28, 47, 33, 247, 114, 107, 23, 174,
                    137, 227, 202, 180, 199, 139, 229, 14, 6, 43, 3, 169, 255, 251, 186, 209,
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
                    timestamp: Some(Timestamp {
                        seconds: timestamp_secs,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                transaction_traces: vec![EthTransactionTrace {
                    index: 1,
                    hash: vec![0xbb; 32],
                    from: vec![0x01; 20],
                    to: address(pool),
                    status: TransactionTraceStatus::Succeeded as i32,
                    receipt: Some(EthTransactionReceipt {
                        logs: vec![log],
                        ..Default::default()
                    }),
                    r#type: EthTransactionType::TrxTypeLegacy as i32,
                    ..Default::default()
                }],
                detail_level: DetailLevel::DetaillevelBase as i32,
                ..Default::default()
            }
        }

        fn write_family_defaults_config(
            unique: &str,
            shared_spkg_path: &str,
            start_block: i64,
            stop_block: Option<i64>,
        ) -> std::path::PathBuf {
            let stop_block_yaml = stop_block
                .map(|value| format!("    stop_block: {value}\n"))
                .unwrap_or_default();
            let config_path = std::env::temp_dir()
                .join(format!("tycho-indexer-family-defaults-{unique}.yaml"));
            std::fs::write(
                &config_path,
                format!(
                    r#"
family_runtimes:
  uniswap:
    shared_spkg: "{shared_spkg_path}"
    shared_module: "map_uniswap_family_protocol_changes"
{stop_block_yaml}extractors:
  uniswap_v2:
    name: "uniswap_v2"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1
    start_block: {start_block}
    protocol_types:
      - name: "uniswap_v2_pool"
        financial_type: "Swap"
    module_name: "v2_map_pool_events"
    family_runtime:
      family: "uniswap"
  uniswap_v3:
    name: "uniswap_v3"
    chain: "ethereum"
    implementation_type: "Custom"
    sync_batch_size: 1
    start_block: {start_block}
    protocol_types:
      - name: "uniswap_v3_pool"
        financial_type: "Swap"
    module_name: "v3_map_protocol_changes"
    family_runtime:
      family: "uniswap"
"#
                ),
            )
            .expect("write temp family-default config");
            config_path
        }

        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:mypassword@localhost:5431/tycho_indexer_0".to_string()
        });
        std::env::set_var("DATABASE_URL", &db_url);

        run_against_db(|_| async move {
            let chain = Chain::Ethereum;
            let protocol_systems = vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()];
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

            let shared_spkg_path = std::env::temp_dir().join(format!(
                "tycho-indexer-combined-family-restart-dynamic-{}-{}.spkg",
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
            let shared_spkg_path = shared_spkg_path
                .to_str()
                .expect("utf8 spkg path")
                .to_string();

            let unique = format!(
                "{}-{}",
                process::id(),
                chrono::Utc::now()
                    .timestamp_nanos_opt()
                    .unwrap_or_default()
            );
            let config_path =
                write_family_defaults_config(&unique, &shared_spkg_path, 43, None);
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

            let resumed_config_path =
                write_family_defaults_config(&format!("{unique}-resumed"), &shared_spkg_path, 43, None);
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
                assert!(
                    requests[0].start_cursor.is_empty(),
                    "fresh restart should resume from persisted branch progress, not a hot cursor"
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
