use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::{format_err, Context, Result};
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use metrics::gauge;
use prost::Message;
use serde::Deserialize;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{self, error::SendError, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument};
use tycho_common::{
    models::{
        blockchain::BlockAggregatedChanges, Address, Chain, ExtractorIdentity, FinancialType,
        ImplementationType, ProtocolType,
    },
    traits::AccountExtractor,
    Bytes,
};
use tycho_ethereum::{
    rpc::EthereumRpcClient,
    services::{
        account_extractor::EVMAccountExtractor, entrypoint_tracer::tracer::EVMEntrypointService,
        token_pre_processor::EthereumTokenPreProcessor,
    },
};
use tycho_storage::postgres::cache::CachedGateway;

use crate::{
    extractor::{
        chain_state::ChainState,
        dynamic_contract_indexer::{
            dci::DynamicContractIndexer,
            hooks::{hook_dci::UniswapV4HookDCI, hooks_dci_builder::UniswapV4HookDCIBuilder},
        },
        extractor_lifecycle::{
            decide_standalone_bootstrap_action, load_standalone_progress_snapshot,
            resolve_standalone_stream_start_block, StandaloneBootstrapAction,
        },
        family_managed_startup::{
            build_family_managed_runner, FamilyRuntimeBuildContext,
        },
        family_runtime::{
            default_family_runtime_registry, FamilyRuntimeConfig, PreparedSubstreamsRequest,
            ResolvedRuntimeTarget, ResolvedRuntimeTargets, ResolvedStandaloneRuntime,
        },
        post_processors::POST_PROCESSOR_REGISTRY,
        protocol_cache::ProtocolMemoryCache,
        protocol_extractor::{ExtractorPgGateway, ProtocolExtractor},
        protocol_message_registry::AuxiliaryProtocolMessageDecoder,
        shared_bootstrap::{commit_materialized_bootstrap, SharedBootstrapPlan},
        standalone_managed_startup::{
            build_standalone_managed_runner_from_startup, prepare_standalone_managed_startup,
            StandaloneRuntimeBuildContext,
        },
        ExtractionError, Extractor, ExtractorExtension, ExtractorMsg,
    },
    pb::sf::substreams::{rpc::v2::BlockScopedData, v1::Package},
    substreams::{
        stream::{BlockResponse, SubstreamsStream},
        SubstreamsEndpoint,
    },
};

#[cfg(test)]
use crate::extractor::family_dispatch::FamilyBranchSpec;
#[cfg(test)]
use crate::extractor::family_dispatch::FamilyBlockChangesDispatcher;
#[cfg(test)]
use crate::extractor::family_runtime_execution::FamilyRuntimeState;
#[cfg(test)]
use crate::extractor::family_runner_wiring::{
    extractors_by_protocol_system, FamilyBranchRuntimeWiring, FamilyBranchSubscriptionIndex,
};
#[cfg(test)]
use crate::extractor::family_lifecycle::{
    apply_family_bootstrap_plan, family_bootstrap_already_completed, resolve_family_stream_cursor,
    resolve_family_stream_position, resolve_family_stream_start, run_family_bootstrap_if_needed,
    validate_family_progress_consistency,
};
#[cfg(test)]
use crate::extractor::family_runtime::{
    resolved_family_execution_config_from_extractor_configs_for_tests,
    validate_family_runtime_membership, DetectedFamilyRuntime,
    ResolvedSharedBootstrapBranchRuntime,
};
#[cfg(test)]
use crate::extractor::family_managed_startup::PreparedFamilyRunnerStartup;
#[cfg(test)]
use crate::extractor::family_managed_startup::build_family_managed_runner_from_startup;
#[cfg(test)]
use crate::testing::{
    family_detected_runtime_from_configs_for_tests, family_detected_runtime_with_members_for_tests,
    family_output_module_for_tests, family_shared_extractor_id_for_tests,
    family_shared_stream_name_for_tests,
};
#[cfg(test)]
use crate::testing::MockGateway;

#[cfg(test)]
fn uniswap_shared_stream_for_tests(
    shared_spkg: &str,
) -> crate::extractor::family_runtime::ResolvedSharedFamilyStream {
    default_family_runtime_registry()
        .resolved_shared_stream_for_family(Chain::Ethereum, "uniswap", shared_spkg)
        .expect("registered uniswap shared stream")
}

#[cfg(test)]
fn family_runtime_state_for_tests(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    dispatcher: FamilyBlockChangesDispatcher,
) -> FamilyRuntimeState {
    let protocol_cache = ProtocolMemoryCache::new(
        Chain::Ethereum,
        chrono::Duration::seconds(60),
        Arc::new(MockGateway::new()),
    );
    FamilyRuntimeState::new(extractors, dispatcher, protocol_cache)
}

#[cfg(test)]
fn family_runner_for_tests(
    extractors: HashMap<String, Arc<dyn Extractor>>,
    substreams: SubstreamsStream,
    subscriptions: BranchSubscriptionsMap,
    dispatcher: FamilyBlockChangesDispatcher,
) -> FamilyExtractorRunner {
    let runtime_state = family_runtime_state_for_tests(&extractors, dispatcher);
    FamilyExtractorRunner::new(
        extractors,
        substreams,
        subscriptions,
        mpsc::channel(4).1,
        None,
        false,
        runtime_state,
    )
}

/// Enum to handle both standard DCI and UniswapV4 Hook DCI
#[allow(clippy::large_enum_variant)]
pub(crate) enum DCIPlugin<AE: AccountExtractor + Send + Sync> {
    Standard(DynamicContractIndexer<AE, EVMEntrypointService, CachedGateway>),
    UniswapV4Hooks(Box<UniswapV4HookDCI<AE, EVMEntrypointService, CachedGateway>>),
}

#[async_trait]
impl<AE: AccountExtractor + Send + Sync> ExtractorExtension for DCIPlugin<AE> {
    async fn process_block_update(
        &mut self,
        block_changes: &mut crate::extractor::models::BlockChanges,
    ) -> Result<(), ExtractionError> {
        match self {
            DCIPlugin::Standard(dci) => {
                dci.process_block_update(block_changes)
                    .await
            }
            DCIPlugin::UniswapV4Hooks(hooks_dci) => {
                hooks_dci
                    .process_block_update(block_changes)
                    .await
            }
        }
    }

    async fn process_revert(
        &mut self,
        target_block: &tycho_common::models::BlockHash,
    ) -> Result<(), ExtractionError> {
        match self {
            DCIPlugin::Standard(dci) => dci.process_revert(target_block).await,
            DCIPlugin::UniswapV4Hooks(hooks_dci) => {
                hooks_dci
                    .process_revert(target_block)
                    .await
            }
        }
    }

    fn cache_size(&self) -> usize {
        match self {
            DCIPlugin::Standard(dci) => dci.cache_size(),
            DCIPlugin::UniswapV4Hooks(hooks_dci) => hooks_dci.cache_size(),
        }
    }

    fn emit_cache_metrics(&self, chain: &str, extractor: &str) {
        match self {
            DCIPlugin::Standard(dci) => dci.emit_cache_metrics(chain, extractor),
            DCIPlugin::UniswapV4Hooks(hooks_dci) => hooks_dci.emit_cache_metrics(chain, extractor),
        }
    }
}
pub enum ControlMessage {
    Stop,
    Subscribe { extractor_id: ExtractorIdentity, sender: Sender<ExtractorMsg> },
}

/// A trait for a message sender that can be used to subscribe to messages
///
/// Extracted out of the [ExtractorHandle] to allow for easier testing
#[async_trait]
pub trait MessageSender: Send + Sync {
    async fn subscribe(&self) -> Result<Receiver<ExtractorMsg>, SendError<ControlMessage>>;
}

#[derive(Clone)]
pub struct ExtractorHandle {
    id: ExtractorIdentity,
    control_tx: Sender<ControlMessage>,
}

impl ExtractorHandle {
    pub(crate) fn new(id: ExtractorIdentity, control_tx: Sender<ControlMessage>) -> Self {
        Self { id, control_tx }
    }

    pub fn get_id(&self) -> ExtractorIdentity {
        self.id.clone()
    }

    #[instrument(skip(self))]
    pub async fn stop(&self) -> Result<(), ExtractionError> {
        // TODO: send a oneshot along here and wait for it
        self.control_tx
            .send(ControlMessage::Stop)
            .await
            .map_err(|err| ExtractionError::Unknown(err.to_string()))
    }
}

#[async_trait]
impl MessageSender for ExtractorHandle {
    #[instrument(skip(self))]
    async fn subscribe(&self) -> Result<Receiver<ExtractorMsg>, SendError<ControlMessage>> {
        let (tx, rx) = mpsc::channel(16);
        // Define a timeout duration
        let timeout_duration = std::time::Duration::from_secs(5); // 5 seconds timeout

        // Wrap the send operation with a timeout
        let send_result = tokio::time::timeout(
            timeout_duration,
            self.control_tx
                .send(ControlMessage::Subscribe { extractor_id: self.id.clone(), sender: tx }),
        )
        .await;

        match send_result {
            Ok(Ok(())) => Ok(rx),
            Ok(Err(e)) => Err(e),
            // TODO: use a better error type that let's us return this as an error.
            Err(_) => panic!("Subscription timed out!"),
        }
    }
}

// Define the SubscriptionsMap type alias
pub(crate) type SubscriptionsMap = HashMap<u64, Sender<ExtractorMsg>>;
pub(crate) type BranchSubscriptionsMap = HashMap<String, Arc<Mutex<SubscriptionsMap>>>;

pub struct ExtractorRunner {
    extractor: Arc<dyn Extractor>,
    substreams: SubstreamsStream,
    subscriptions: Arc<Mutex<SubscriptionsMap>>,
    next_subscriber_id: u64,
    control_rx: Receiver<ControlMessage>,
    /// Handle of the tokio runtime on which the extraction tasks will be run.
    /// If 'None' the default runtime will be used.
    runtime_handle: Option<Handle>,
    partial_blocks: bool,
}

pub use crate::extractor::family_runtime_execution::FamilyExtractorRunner;

pub enum ManagedRunner {
    Single(ExtractorRunner),
    Family(FamilyExtractorRunner),
}

impl ManagedRunner {
    pub fn run(self) -> JoinHandle<Result<(), ExtractionError>> {
        match self {
            ManagedRunner::Single(runner) => runner.run(),
            ManagedRunner::Family(runner) => runner.run(),
        }
    }
}

impl<'a> ResolvedRuntimeTargets<'a> {
    #[allow(clippy::too_many_arguments)]
    pub async fn build_managed_runners(
        self,
        chain_state: ChainState,
        endpoint_url: &str,
        s3_bucket: Option<&str>,
        substreams_api_token: &str,
        cached_gw: &CachedGateway,
        database_insert_batch_size: usize,
        token_pre_processor: &EthereumTokenPreProcessor,
        rpc_client: &EthereumRpcClient,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(Vec<ManagedRunner>, Vec<ExtractorHandle>), ExtractionError> {
        let chain = self
            .as_slice()
            .first()
            .map(ResolvedRuntimeTarget::chain)
            .expect("resolved runtime targets should not be empty");

        info!("Building protocol cache");
        let protocol_cache = ProtocolMemoryCache::new(
            chain,
            chrono::Duration::seconds(900),
            Arc::new(cached_gw.clone()),
        );
        protocol_cache.populate().await?;

        self.initialize_accounts(rpc_client, cached_gw).await;

        let mut runners = Vec::new();
        let mut extractor_handles = Vec::new();

        for target in self.into_inner() {
            let (runner, handles) = target
                .build_managed_runner(
                    chain_state,
                    endpoint_url,
                    s3_bucket,
                    substreams_api_token,
                    cached_gw,
                    database_insert_batch_size,
                    token_pre_processor,
                    &protocol_cache,
                    rpc_client,
                    runtime.clone(),
                    partial_blocks,
                )
                .await?;
            runners.push(runner);
            extractor_handles.extend(handles);
        }

        Ok((runners, extractor_handles))
    }
}

impl<'a> ResolvedRuntimeTarget<'a> {
    #[allow(clippy::too_many_arguments)]
    pub async fn build_managed_runner(
        self,
        chain_state: ChainState,
        endpoint_url: &str,
        s3_bucket: Option<&str>,
        substreams_api_token: &str,
        cached_gw: &CachedGateway,
        database_insert_batch_size: usize,
        token_pre_processor: &EthereumTokenPreProcessor,
        protocol_cache: &ProtocolMemoryCache,
        rpc_client: &EthereumRpcClient,
        runtime: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<(ManagedRunner, Vec<ExtractorHandle>), ExtractionError> {
        match self {
            ResolvedRuntimeTarget::Family(family) => build_family_managed_runner(
                    family,
                    FamilyRuntimeBuildContext {
                        chain_state,
                        endpoint_url,
                        s3_bucket,
                        substreams_api_token,
                        cached_gw,
                        database_insert_batch_size,
                        token_pre_processor,
                        protocol_cache,
                        rpc_client,
                        partial_blocks,
                    },
                    runtime,
                    partial_blocks,
                    false,
                )
                .await,
            ResolvedRuntimeTarget::Standalone(standalone) => {
                let prepared_startup = prepare_standalone_managed_startup(
                    standalone.extractor_config,
                    StandaloneRuntimeBuildContext {
                        chain_state,
                        endpoint_url,
                        s3_bucket,
                        substreams_api_token,
                        cached_gw,
                        database_insert_batch_size,
                        token_pre_processor,
                        protocol_cache,
                        rpc_client,
                        partial_blocks,
                        final_block_only: false,
                    },
                )
                .await?;
                Ok(build_standalone_managed_runner_from_startup(
                    prepared_startup,
                    runtime,
                    partial_blocks,
                ))
            }
        }
    }
}

pub(crate) struct PreparedSingleRunnerStartup {
    pub(crate) extractor: Arc<dyn Extractor>,
    pub(crate) extractor_id: ExtractorIdentity,
    pub(crate) stream: SubstreamsStream,
}

impl ExtractorRunner {
    pub fn new(
        extractor: Arc<dyn Extractor>,
        substreams: SubstreamsStream,
        subscriptions: Arc<Mutex<SubscriptionsMap>>,
        control_rx: Receiver<ControlMessage>,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
    ) -> Self {
        ExtractorRunner {
            extractor,
            substreams,
            subscriptions,
            next_subscriber_id: 0,
            control_rx,
            runtime_handle,
            partial_blocks,
        }
    }

    pub fn run(mut self) -> JoinHandle<Result<(), ExtractionError>> {
        info!("Extractor {} started!", self.extractor.get_id());

        let runtime = self
            .runtime_handle
            .clone()
            .unwrap_or_else(|| Handle::current());

        runtime.spawn(async move {
            let id = self.extractor.get_id();
            // Track the number of partials received for the current block != partial_index.
            let mut partials_in_block: u32 = 0;
            loop {
                // this is the main info span of an extractor
                let loop_span = info_span!(
                    parent: None,  // don't attach this to the parent (builder) span to keep spans short
                    "extractor",
                    extractor_id = %id,
                    sf_trace_id = tracing::field::Empty,
                    block_number = tracing::field::Empty,
                    otel.status_code = tracing::field::Empty,
                );

                let should_continue = async {
                    tokio::select! {
                        Some(ctrl) = self.control_rx.recv() => {
                            match ctrl {
                                ControlMessage::Stop => {
                                    warn!("Stop signal received; exiting!");
                                    return Ok(false);
                                },
                                ControlMessage::Subscribe { sender, .. } => {
                                    self.subscribe(sender).await;
                                },
                            }
                        }
                        val = self.substreams.next().instrument(info_span!("substreams_waiting")) => {
                            match val {
                                None => {
                                    error!("stream ended");
                                    tracing::Span::current().record("otel.status_code", "error");
                                    return Err(ExtractionError::SubstreamsError(format!("{id}: stream ended")));
                                }
                                Some(Ok(BlockResponse::New(data))) => {
                                    let block_number = data.clock.as_ref().map(|v| v.number).unwrap_or(0);
                                    tracing::Span::current().record("block_number", block_number);
                                    gauge!(
                                        "extractor_current_block_number",
                                        "chain" => id.chain.to_string(),
                                        "extractor" => id.name.to_string()
                                    ).set(block_number as f64);

                                    if data.is_partial {
                                        partials_in_block += 1;
                                    }

                                    if data.is_last_partial == Some(true) || data.partial_index.is_none() {
                                        gauge!(
                                            "extractor_partials_per_block",
                                            "chain" => id.chain.to_string(),
                                            "extractor" => id.name.to_string()
                                        )
                                        .set(partials_in_block as f64);
                                        partials_in_block = 0;
                                    }

                                    // Start measuring block processing time
                                    let start_time = std::time::Instant::now();

                                    let msgs = Self::process_block_data(
                                        self.extractor.as_ref(),
                                        &data,
                                        self.partial_blocks,
                                    )
                                    .await
                                    .map_err(|err| {
                                        error!(error = %err, "Error while processing block data");
                                        tracing::Span::current().record("otel.status_code", "error");
                                        err
                                    })?;
                                    for msg in msgs {
                                        trace!("Propagating block data message.");
                                        Self::propagate_msg(&self.subscriptions, msg).await
                                    }

                                    let duration_ms = start_time.elapsed().as_millis() as f64;
                                    let block_type = match (data.is_partial, data.is_last_partial) {
                                        (false, _) => "full",
                                        (true, Some(true)) => "final_partial",
                                        (true, _) => "partial",
                                    };

                                    gauge!(
                                        "block_processing_time_ms",
                                        "chain" => id.chain.to_string(),
                                        "extractor" => id.name.to_string(),
                                        "block_type" => block_type
                                    ).set(duration_ms);
                                }
                                Some(Ok(BlockResponse::Undo(undo_signal))) => {
                                    partials_in_block = 0;
                                    info!(block=?&undo_signal.last_valid_block,  "Revert requested!");
                                    match self.extractor.handle_revert(undo_signal.clone()).await {
                                        Ok(Some(msg)) => {
                                            trace!("Propagating block undo message.");
                                            Self::propagate_msg(&self.subscriptions, msg).await
                                        }
                                        Ok(None) => {
                                            trace!("No message to propagate.");
                                        }
                                        Err(err) => {
                                            error!(error = %err, "Error while processing revert!");
                                            tracing::Span::current().record("otel.status_code", "error");
                                            return Err(err);
                                        }
                                    }
                                }
                                Some(Ok(BlockResponse::Ended)) => {
                                    self.extractor.flush().await?;
                                    tracing::Span::current().record("otel.status_code", "ok");
                                    return Ok(false);
                                }
                                Some(Err(err)) => {
                                    error!(error = %err, "Stream terminated with error.");
                                    tracing::Span::current().record("otel.status_code", "error");
                                    return Err(ExtractionError::SubstreamsError(err.to_string()));
                                }
                            };
                        }
                    }

                    tracing::Span::current().record("otel.status_code", "ok");
                    Ok(true) // Continue the loop
                }
                    .instrument(loop_span)
                    .await?;

                if !should_continue {
                    break Ok(());
                }
            }
        })
    }

    #[instrument(skip_all)]
    async fn subscribe(&mut self, sender: Sender<ExtractorMsg>) {
        let subscriber_id = self.next_subscriber_id;
        self.next_subscriber_id += 1;
        tracing::Span::current().record("subscriber_id", subscriber_id);
        info!(?subscriber_id, "New subscription");
        self.subscriptions
            .lock()
            .await
            .insert(subscriber_id, sender);
    }

    /// Processes block-scoped data from the stream: always sends the input to the extractor,
    /// then optionally adds a partial copy of the message (for full blocks with partials enabled)
    /// and/or the result of collect_and_process_full_block (for final partials).
    #[instrument(skip_all, fields(partial_blocks_enabled, is_partial = data.is_partial))]
    pub(crate) async fn process_block_data(
        extractor: &dyn Extractor,
        data: &BlockScopedData,
        partial_blocks_enabled: bool,
    ) -> Result<Vec<ExtractorMsg>, ExtractionError> {
        let mut msgs = Vec::new();

        match extractor
            .handle_tick_scoped_data(data.clone())
            .await
        {
            Ok(Some(msg)) => {
                if partial_blocks_enabled && !data.is_partial {
                    // Full block and partial blocks enabled: add a partial copy of the message
                    msgs.push(Self::as_partial_message(&msg));
                }
                msgs.push(msg);
            }
            Ok(None) => {
                trace!("No message to propagate.");
            }
            Err(e) => return Err(e),
        }

        let is_final_partial = data.is_partial && data.is_last_partial == Some(true);
        if partial_blocks_enabled && is_final_partial {
            // Final partial: Create full block message from cached partials
            match extractor
                .collect_and_process_full_block(
                    data.cursor.clone(),
                    data.final_block_height,
                    data.clock.clone(),
                )
                .await
            {
                Ok(Some(msg)) => msgs.push(msg),
                Ok(None) => {
                    trace!("No message to propagate.");
                }
                Err(e) => return Err(e),
            }
        }

        Ok(msgs)
    }

    /// Returns a copy of the message with partial_block_index set to Some(0).
    fn as_partial_message(msg: &ExtractorMsg) -> ExtractorMsg {
        let mut copy: BlockAggregatedChanges = (**msg).clone();
        copy.partial_block_index = Some(0);
        Arc::new(copy)
    }

    // TODO: add message tracing_id to the log
    #[instrument(skip_all, fields(subscriber_count))]
    pub(crate) async fn propagate_msg(
        subscribers: &Arc<Mutex<SubscriptionsMap>>,
        message: ExtractorMsg,
    ) {
        trace!(msg = %message, "Propagating message to subscribers.");
        // TODO: rename variable here instead
        let arced_message = message;

        let mut to_remove = Vec::new();

        // Lock the subscribers HashMap for exclusive access
        let mut subscribers = subscribers.lock().await;
        tracing::Span::current().record("subscriber_count", subscribers.len());

        for (counter, sender) in subscribers.iter_mut() {
            match sender.send(arced_message.clone()).await {
                Ok(_) => {
                    // Message sent successfully
                    trace!(subscriber_id = %counter, "Message sent successfully.");
                }
                Err(err) => {
                    // Receiver has been dropped, mark for removal
                    to_remove.push(*counter);
                    error!(error = %err, counter, "Error while sending message to subscriber");
                }
            }
        }

        // Remove inactive subscribers
        for counter in to_remove {
            subscribers.remove(&counter);
            debug!("Subscriber {} has been dropped", counter);
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProtocolTypeConfig {
    name: String,
    financial_type: FinancialType,
}

impl ProtocolTypeConfig {
    pub fn new(name: String, financial_type: FinancialType) -> Self {
        Self { name, financial_type }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn financial_type(&self) -> FinancialType {
        self.financial_type.clone()
    }
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapStrategy {
    #[default]
    UniswapV3Rpc,
    UniswapV2Rpc,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct BootstrapConfig {
    pub strategy: BootstrapStrategy,
    pub start_block: i64,
    pub params: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ExtractorConfig {
    name: String,
    protocol_system: String,
    chain: Chain,
    implementation_type: ImplementationType,
    sync_batch_size: usize,
    start_block: i64,
    stop_block: Option<i64>,
    protocol_types: Vec<ProtocolTypeConfig>,
    spkg: String,
    module_name: String,
    #[serde(default)]
    pub initialized_accounts: Vec<Bytes>,
    #[serde(default)]
    pub initialized_accounts_block: u64,
    #[serde(default)]
    pub post_processor: Option<String>,
    #[serde(default)]
    pub dci_plugin: Option<DCIType>,
    #[serde(default)]
    pub substreams_params: HashMap<String, String>,
    #[serde(default)]
    pub bootstrap: Option<BootstrapConfig>,
    #[serde(default)]
    pub family_runtime: Option<FamilyRuntimeConfig>,
}

impl ExtractorConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        chain: Chain,
        implementation_type: ImplementationType,
        sync_batch_size: usize,
        start_block: i64,
        stop_block: Option<i64>,
        protocol_types: Vec<ProtocolTypeConfig>,
        spkg: String,
        module_name: String,
        initialized_accounts: Vec<Bytes>,
        initialized_accounts_block: u64,
        post_processor: Option<String>,
        dci_plugin: Option<DCIType>,
        substreams_params: HashMap<String, String>,
        bootstrap: Option<BootstrapConfig>,
    ) -> Self {
        Self {
            protocol_system: name.clone(),
            name,
            chain,
            implementation_type,
            sync_batch_size,
            start_block,
            stop_block,
            protocol_types,
            spkg,
            module_name,
            initialized_accounts,
            initialized_accounts_block,
            post_processor,
            dci_plugin,
            substreams_params,
            bootstrap,
            family_runtime: None,
        }
    }

    pub fn start_block(&self) -> i64 {
        self.start_block
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn protocol_system(&self) -> &str {
        &self.protocol_system
    }

    pub fn chain(&self) -> Chain {
        self.chain
    }

    pub fn stop_block(&self) -> Option<i64> {
        self.stop_block
    }

    pub fn protocol_types(&self) -> &[ProtocolTypeConfig] {
        &self.protocol_types
    }

    pub fn spkg(&self) -> &str {
        &self.spkg
    }

    pub fn module_name(&self) -> &str {
        &self.module_name
    }

    pub fn with_protocol_system(mut self, protocol_system: impl Into<String>) -> Self {
        self.protocol_system = protocol_system.into();
        self
    }
}

pub fn configured_stream_start_block(config: &ExtractorConfig) -> Result<i64, ExtractionError> {
    if config.bootstrap.is_some() {
        config
            .start_block
            .checked_add(1)
            .ok_or_else(|| ExtractionError::Setup("stream start block overflow".to_string()))
    } else {
        Ok(config.start_block)
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DCIType {
    /// RPC DCI plugin - uses the RPC endpoint to fetch the account data
    #[serde(rename = "rpc")]
    RPC,
    /// UniswapV4Hooks DCI plugin - wrapper for the RPC DCI plugin that generates hook entrypoints
    /// for tracing
    UniswapV4Hooks { pool_manager_address: String },
}

pub struct ExtractorBuilder {
    config: ExtractorConfig,
    endpoint_url: String,
    s3_bucket: Option<String>,
    token: String,
    extractor: Option<Arc<dyn Extractor>>,
    rpc_client: Option<EthereumRpcClient>,
    database_insert_batch_size: Option<usize>,
    auxiliary_protocol_message_decoders: Vec<AuxiliaryProtocolMessageDecoder>,
    final_block_only: bool,
    partial_blocks: bool,
    /// Handle of the tokio runtime on which the extraction tasks will be run.
    /// If 'None' the default runtime will be used.
    runtime_handle: Option<Handle>,
}

impl ExtractorBuilder {
    pub(crate) fn build_from_startup(
        prepared_startup: PreparedSingleRunnerStartup,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
    ) -> (ExtractorRunner, ExtractorHandle) {
        let (ctrl_tx, ctrl_rx) = mpsc::channel(128);
        let runner = ExtractorRunner::new(
            prepared_startup.extractor,
            prepared_startup.stream,
            Arc::new(Mutex::new(HashMap::new())),
            ctrl_rx,
            runtime_handle,
            partial_blocks,
        );

        (runner, ExtractorHandle::new(prepared_startup.extractor_id, ctrl_tx))
    }

    pub fn new(
        config: &ExtractorConfig,
        endpoint_url: &str,
        s3_bucket: Option<&str>,
        substreams_api_token: &str,
    ) -> Self {
        Self {
            config: config.clone(),
            endpoint_url: endpoint_url.to_owned(),
            s3_bucket: s3_bucket.map(ToString::to_string),
            token: substreams_api_token.to_string(),
            extractor: None,
            rpc_client: None,
            database_insert_batch_size: None,
            auxiliary_protocol_message_decoders: Vec::new(),
            final_block_only: false,
            partial_blocks: false,
            runtime_handle: None,
        }
    }

    /// Set the substreams endpoint url
    pub fn endpoint_url(mut self, val: &str) -> Self {
        val.clone_into(&mut self.endpoint_url);
        self
    }

    pub fn module_name(mut self, val: &str) -> Self {
        val.clone_into(&mut self.config.module_name);
        self
    }

    pub fn start_block(mut self, val: i64) -> Self {
        self.config.start_block = val;
        self
    }

    pub fn token(mut self, val: &str) -> Self {
        val.clone_into(&mut self.token);
        self
    }

    pub fn only_final_blocks(mut self) -> Self {
        self.final_block_only = true;
        self
    }

    pub fn set_runtime(mut self, runtime: Handle) -> Self {
        self.runtime_handle = Some(runtime);
        self
    }

    pub fn partial_blocks(mut self, val: bool) -> Self {
        self.partial_blocks = val;
        self
    }

    /// Set the global database insert batch size
    pub fn database_insert_batch_size(mut self, database_insert_batch_size: usize) -> Self {
        self.database_insert_batch_size = Some(database_insert_batch_size);
        self
    }

    pub(crate) fn auxiliary_protocol_message_decoders(
        mut self,
        decoders: Vec<AuxiliaryProtocolMessageDecoder>,
    ) -> Self {
        self.auxiliary_protocol_message_decoders = decoders;
        self
    }

    pub(crate) fn into_protocol_system_and_extractor(self) -> (String, Arc<dyn Extractor>) {
        (
            self.config.protocol_system().to_string(),
            self.extractor
                .clone()
                .expect("extractor initialized in build()"),
        )
    }

    pub(crate) fn initialized_extractor(&self) -> Arc<dyn Extractor> {
        self.extractor
            .clone()
            .expect("extractor initialized in build()")
    }

    #[cfg(test)]
    pub fn set_extractor(mut self, val: Arc<dyn Extractor>) -> Self {
        self.extractor = Some(val);
        self
    }

    /// Creates a rpc DynamicContractIndexer with account extractor and tracer configured
    async fn create_rpc_dci(
        rpc_client: &EthereumRpcClient,
        chain: Chain,
        extractor_name: String,
        cached_gw: &CachedGateway,
    ) -> Result<
        DynamicContractIndexer<EVMAccountExtractor, EVMEntrypointService, CachedGateway>,
        ExtractionError,
    > {
        let account_extractor = EVMAccountExtractor::new(rpc_client, chain);

        // Tracer uses dedicated TRACE_RPC_URL if available, and falls back to the main
        // rpc client otherwise.
        let tracer_rpc_client = if let Ok(tracer_rpc_url) = std::env::var("TRACE_RPC_URL") {
            EthereumRpcClient::new(&tracer_rpc_url).map_err(|err| {
                ExtractionError::Setup(format!(
                    "Failed to create RPC client for {tracer_rpc_url}: {err}"
                ))
            })?
        } else {
            rpc_client.clone()
        };

        let max_retries = std::env::var("TRACE_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);

        let retry_delay_ms = std::env::var("TRACE_RETRY_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);

        let tracer =
            EVMEntrypointService::new_with_config(&tracer_rpc_client, max_retries, retry_delay_ms);

        let mut rpc_dci = DynamicContractIndexer::new(
            chain,
            extractor_name,
            cached_gw.clone(),
            account_extractor,
            tracer,
        );
        rpc_dci.initialize().await?;

        Ok(rpc_dci)
    }

    pub async fn build(
        mut self,
        chain_state: ChainState,
        cached_gw: &CachedGateway,
        token_pre_processor: &EthereumTokenPreProcessor,
        protocol_cache: &ProtocolMemoryCache,
        rpc_client: &EthereumRpcClient,
    ) -> Result<Self, ExtractionError> {
        self.rpc_client = Some(rpc_client.clone());

        let protocol_types = self
            .config
            .protocol_types
            .iter()
            .map(|pt| {
                (
                    pt.name.clone(),
                    ProtocolType::new(
                        pt.name.clone(),
                        pt.financial_type.clone(),
                        None,
                        self.config.implementation_type.clone(),
                    ),
                )
            })
            .collect();

        let family_durability_scope = self
            .config
            .require_resolved_family_runtime_metadata()?
            .map(|metadata| metadata.durability_scope.to_string());

        let gw = ExtractorPgGateway::new(
            &self.config.name,
            self.config.chain,
            self.config.sync_batch_size,
            cached_gw.clone(),
            family_durability_scope,
        );

        let post_processor = self
            .config
            .post_processor
            .as_ref()
            .map(|name| {
                POST_PROCESSOR_REGISTRY
                    .get(name)
                    .cloned()
                    .ok_or_else(|| {
                        ExtractionError::Setup(format!(
                            "Post processor '{name}' not found in registry"
                        ))
                    })
            })
            .transpose()?;

        let dci_plugin = if let Some(ref dci_type) = self.config.dci_plugin {
            Some(match dci_type {
                DCIType::RPC => {
                    let rpc_dci = Self::create_rpc_dci(
                        rpc_client,
                        self.config.chain,
                        self.config.name.clone(),
                        cached_gw,
                    )
                    .await?;

                    DCIPlugin::Standard(rpc_dci)
                }
                DCIType::UniswapV4Hooks { pool_manager_address } => {
                    // random address to deploy our mini router to
                    let router_address =
                        Address::from("0x2e234DAe75C793f67A35089C9d99245E1C58470b");
                    let pool_manager = Address::from(pool_manager_address.as_str());

                    let base_dci = Self::create_rpc_dci(
                        rpc_client,
                        self.config.chain,
                        self.config.name.clone(),
                        cached_gw,
                    )
                    .await?;

                    let mut hooks_dci = UniswapV4HookDCIBuilder::new(
                        base_dci,
                        rpc_client,
                        router_address,
                        pool_manager,
                        cached_gw.clone(),
                        self.config.chain,
                    )
                    .pause_after_retries(3)
                    .max_retries(5)
                    .build()?;

                    hooks_dci.initialize().await?;
                    DCIPlugin::UniswapV4Hooks(Box::new(hooks_dci))
                }
            })
        } else {
            None
        };

        let database_insert_batch_size = self
            .database_insert_batch_size
            .unwrap_or_default();

        let extractor =
            ProtocolExtractor::<ExtractorPgGateway, EthereumTokenPreProcessor, DCIPlugin<_>>::new(
                gw,
                database_insert_batch_size,
                &self.config.name,
                self.config.chain,
                chain_state,
                self.config
                    .protocol_system()
                    .to_string(),
                protocol_cache.clone(),
                protocol_types,
                self.auxiliary_protocol_message_decoders.clone(),
                token_pre_processor.clone(),
                post_processor,
                dci_plugin,
            )
            .await?;

        self.extractor = Some(Arc::new(extractor));

        Ok(self)
    }

    async fn run_bootstrap_once(
        &self,
        extractor: Arc<dyn Extractor>,
        bootstrap: &BootstrapConfig,
        extractor_id: &ExtractorIdentity,
    ) -> Result<(), ExtractionError> {
        let rpc_client = self
            .rpc_client
            .as_ref()
            .ok_or_else(|| {
                ExtractionError::Setup("missing RPC client for bootstrap".to_string())
            })?;
        let registry = default_family_runtime_registry();
        let plan =
            SharedBootstrapPlan::for_extractor_config_with_registry(&self.config, bootstrap, registry)?;
        let shared_bootstrap_execution = registry
            .resolve_shared_bootstrap_execution_for_protocol_system(self.config.protocol_system())?;

        info!(
            extractor_id = %extractor_id,
            branches = plan.branches.len(),
            bootstrap_block = plan.bootstrap_block,
            "BootstrapExecutorInit"
        );

        for branch in &plan.branches {
            info!(
                extractor_id = %extractor_id,
                strategy = ?branch.strategy,
                protocol_system = branch.protocol_system,
                pools = branch.params.pools.len(),
                "BootstrapExecutorBranch"
            );
        }

        let changes = shared_bootstrap_execution
            .materialize_plan(rpc_client, &plan)
            .await?;
        let bootstrap_block_hash = changes.block.hash.clone();
        commit_materialized_bootstrap(
            vec![(extractor.clone(), changes)],
            extractor,
            plan.bootstrap_block,
            bootstrap_block_hash,
        )
        .await?;

        info!(
            extractor_id = %extractor_id,
            bootstrap_block = plan.bootstrap_block,
            "BootstrapExecutorCompleted"
        );

        Ok(())
    }

    pub(crate) async fn prepare_substreams_request(
        &self,
        extractor: Arc<dyn Extractor>,
        extractor_id: &ExtractorIdentity,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        let runtime_target = ResolvedRuntimeTarget::Standalone(ResolvedStandaloneRuntime {
            protocol_system: self.config.protocol_system(),
            extractor_config: &self.config,
        });
        let default_request = runtime_target.substreams_execution_request()?;
        let mut progress = load_standalone_progress_snapshot(extractor.as_ref()).await?;
        match decide_standalone_bootstrap_action(
            &progress,
            &self.config.name,
            self.config.bootstrap.as_ref(),
        )? {
            StandaloneBootstrapAction::Skip => {}
            StandaloneBootstrapAction::AlreadyCompleted { .. } => {
                let bootstrap = self
                    .config
                    .bootstrap
                    .as_ref()
                    .expect("completed bootstrap action requires bootstrap config");
                info!(
                    extractor_id = %extractor_id,
                    bootstrap_block = bootstrap.start_block,
                    "Bootstrap already completed in storage; skipping bootstrap run"
                );
            }
            StandaloneBootstrapAction::Run { .. } => {
                let bootstrap = self
                    .config
                    .bootstrap
                    .as_ref()
                    .expect("run bootstrap action requires bootstrap config");
                info!(
                    bootstrap_block = bootstrap.start_block,
                    extractor_id = %extractor_id,
                    "Running bootstrap block before starting event stream"
                );
                tokio::select! {
                    res = self.run_bootstrap_once(
                        extractor.clone(),
                        bootstrap,
                        extractor_id,
                    ) => res?,
                    _ = tokio::signal::ctrl_c() => {
                        warn!(
                            extractor_id = %extractor_id,
                            bootstrap_block = bootstrap.start_block,
                            "Bootstrap interrupted by SIGINT before extractor startup completed"
                        );
                        return Err(ExtractionError::Unknown(format!(
                            "bootstrap interrupted for {extractor_id}"
                        )));
                    }
                }
                progress = load_standalone_progress_snapshot(extractor.as_ref()).await?;
            }
        }

        let start_block = resolve_standalone_stream_start_block(&progress, default_request.start_block)?;

        if let Some(block) = &progress.last_processed_block {
            info!(
                start_block,
                last_committed_block = block.number,
                config_start_block = self.config.start_block,
                "Fresh start: resuming from block after last committed"
            );
        }

        let request = runtime_target.substreams_execution_request_with_start_block(start_block)?;
        Ok(PreparedSubstreamsRequest { request, cursor: None })
    }

    /// Converts this builder into a ready-to-run ExtractorRunner and its associated handle.
    ///
    /// This method completes the extractor setup process by:
    /// - Ensuring the Substreams package (.spkg) file is available, downloading from S3 if
    ///   necessary
    /// - Creating a Substreams endpoint connection with authentication
    /// - Setting up the data stream with the configured module, block range, and cursor
    /// - Initializing control channels for managing the extractor lifecycle
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `ExtractorRunner`: The main component that processes blockchain data from the stream
    /// - `ExtractorHandle`: A control interface for stopping the extractor and subscribing to its
    ///   output
    ///
    /// # Errors
    ///
    /// Returns `ExtractionError` if:
    /// - The extractor was not properly configured
    /// - The Substreams package file cannot be accessed or downloaded
    /// - The Substreams endpoint connection cannot be established
    /// - Package decoding fails due to corrupted or invalid data
    #[instrument(name = "extractor_runner_build", skip(self), fields(extractor_id))]
    pub async fn into_runner(self) -> Result<(ExtractorRunner, ExtractorHandle), ExtractionError> {
        let runtime_handle = self.runtime_handle.clone();
        let partial_blocks = self.partial_blocks;
        let extractor = self
            .extractor
            .clone()
            .expect("Extractor not set");
        let extractor_id = extractor.get_id();
        let prepared_request = self
            .prepare_substreams_request(extractor.clone(), &extractor_id)
            .await?;
        let stream = load_stream_for_prepared_request(
            &prepared_request,
            self.s3_bucket.as_deref(),
            &self.endpoint_url,
            &self.token,
            self.final_block_only,
            self.partial_blocks,
        )
        .await?;
        let prepared_startup = PreparedSingleRunnerStartup { extractor, extractor_id, stream };
        Ok(Self::build_from_startup(prepared_startup, runtime_handle, partial_blocks))
    }
}

fn build_substreams_stream_from_prepared_request(
    prepared_request: &PreparedSubstreamsRequest,
    loaded_substreams: LoadedSubstreamsPackage,
    final_block_only: bool,
    partial_blocks: bool,
) -> SubstreamsStream {
    SubstreamsStream::new(
        loaded_substreams.endpoint,
        prepared_request.cursor.clone(),
        Some(loaded_substreams.spkg),
        prepared_request.request.module.clone(),
        prepared_request.request.start_block,
        prepared_request.request.stop_block,
        final_block_only,
        prepared_request
            .request
            .extractor_id
            .clone(),
        partial_blocks,
        prepared_request.request.params.clone(),
    )
}

async fn load_stream_for_prepared_request(
    prepared_request: &PreparedSubstreamsRequest,
    s3_bucket: Option<&str>,
    endpoint_url: &str,
    substreams_api_token: &str,
    final_block_only: bool,
    partial_blocks: bool,
) -> Result<SubstreamsStream, ExtractionError> {
    let loaded_substreams = load_substreams_package(
        s3_bucket,
        &prepared_request.request.spkg,
        endpoint_url,
        Some(substreams_api_token.to_string()),
    )
    .await?;

    Ok(build_substreams_stream_from_prepared_request(
        prepared_request,
        loaded_substreams,
        final_block_only,
        partial_blocks,
    ))
}

#[cfg(test)]
fn validate_family_runner_membership(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    validate_family_runtime_membership(family, extractor_configs)
}

async fn download_file_from_s3(
    bucket: &str,
    key: &str,
    download_path: &Path,
) -> anyhow::Result<()> {
    info!("Downloading file from s3: {}/{} to {:?}", bucket, key, download_path);

    let region_provider = RegionProviderChain::default_provider().or_else("eu-central-1");

    let config = aws_config::from_env()
        .region(region_provider)
        .load()
        .await;

    let client = Client::new(&config);

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let data = resp.body.collect().await.unwrap();

    // Ensure the directory exists
    if let Some(parent) = download_path.parent() {
        std::fs::create_dir_all(parent)
            .context(format!("Failed to create directories for {parent:?}"))?;
    }

    std::fs::write(download_path, data.into_bytes()).unwrap();

    Ok(())
}

async fn ensure_spkg_path(s3_bucket: Option<&str>, spkg_path: &str) -> Result<(), ExtractionError> {
    if Path::new(spkg_path).exists() {
        return Ok(());
    }

    download_file_from_s3(
        s3_bucket.ok_or_else(|| {
            ExtractionError::Setup(format!("Missing spkg and s3 bucket config for {spkg_path}"))
        })?,
        spkg_path,
        Path::new(spkg_path),
    )
    .await
    .map_err(|e| ExtractionError::Setup(format!("Failed to download {spkg_path} from s3. {e}")))?;

    Ok(())
}

async fn read_spkg(spkg_path: &str) -> Result<Package, ExtractionError> {
    let content = std::fs::read(spkg_path)
        .context(format_err!("read package from file '{spkg_path}'"))
        .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?;
    Package::decode(content.as_ref())
        .context("decode command")
        .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))
}

pub struct LoadedSubstreamsPackage {
    pub spkg: Package,
    pub endpoint: Arc<SubstreamsEndpoint>,
}

pub async fn load_substreams_package(
    s3_bucket: Option<&str>,
    spkg_path: &str,
    endpoint_url: &str,
    token: Option<String>,
) -> Result<LoadedSubstreamsPackage, ExtractionError> {
    ensure_spkg_path(s3_bucket, spkg_path).await?;
    let spkg = read_spkg(spkg_path).await?;
    let endpoint = Arc::new(
        SubstreamsEndpoint::new(endpoint_url, token)
            .await
            .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?,
    );

    Ok(LoadedSubstreamsPackage { spkg, endpoint })
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use chrono::NaiveDateTime;
    use futures03::stream;
    use prost::Message;
    use tycho_common::{
        models::{
            blockchain::Block, blockchain::BlockAggregatedChanges, protocol::ProtocolComponent,
            token::Token, ChangeType,
        },
        storage::WithTotal,
        Bytes,
    };
    use tycho_substreams::pb::tycho::evm::v1 as substreams;

    use super::*;
    use crate::{
        extractor::{
            family_lifecycle::ResolvedFamilyStreamPosition,
            family_runtime::ResolvedFamilyRuntime, protocol_cache::ProtocolDataCache, MockExtractor,
        },
        pb::sf::substreams::v1::Clock,
        testing::MockGateway,
    };

    /// Builds minimal BlockScopedData for runner message-selection tests.
    fn make_block_scoped_data(
        is_partial: bool,
        partial_index: Option<u32>,
        is_last_partial: Option<bool>,
    ) -> BlockScopedData {
        BlockScopedData {
            output: None,
            clock: None,
            cursor: String::new(),
            final_block_height: 0,
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: String::new(),
            is_partial,
            partial_index,
            is_last_partial,
        }
    }

    fn make_family_block_scoped_data() -> BlockScopedData {
        use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

        let family_changes = substreams::BlockChanges {
            block: Some(substreams::Block {
                number: 42,
                hash: vec![0x01; 32],
                parent_hash: vec![0x02; 32],
                ts: 1_718_000_000,
            }),
            changes: vec![substreams::TransactionChanges {
                tx: Some(substreams::Transaction {
                    hash: vec![0xaa; 32],
                    from: vec![0x11; 20],
                    to: vec![0x22; 20],
                    index: 7,
                }),
                contract_changes: vec![],
                entity_changes: vec![],
                component_changes: vec![
                    substreams::ProtocolComponent {
                        id: "v2-pool".to_string(),
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v2_pool".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    substreams::ProtocolComponent {
                        id: "v3-pool".to_string(),
                        protocol_type: Some(substreams::ProtocolType {
                            name: "uniswap_v3_pool".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
                balance_changes: vec![],
                entrypoints: vec![],
                entrypoint_params: vec![],
            }],
            storage_changes: vec![],
        };

        BlockScopedData {
            output: Some(MapModuleOutput {
                name: family_output_module_for_tests("uniswap"),
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: family_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock { id: "42".to_string(), number: 42, timestamp: None }),
            cursor: "cursor-42".to_string(),
            final_block_height: 42,
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: String::new(),
            is_partial: false,
            partial_index: None,
            is_last_partial: None,
        }
    }

    fn make_uniswap_family_bootstrap_test_configs() -> [ExtractorConfig; 2] {
        [
            ExtractorConfig {
                name: "uniswap_v2".to_owned(),
                protocol_system: "uniswap_v2".to_string(),
                start_block: 42,
                protocol_types: vec![ProtocolTypeConfig::new(
                    "uniswap_v2_pool".to_string(),
                    FinancialType::Swap,
                )],
                substreams_params: HashMap::from([(
                    "map_pool_events".to_string(),
                    "factory=0x01".to_string(),
                )]),
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                        .to_owned(),
                }),
                ..Default::default()
            },
            ExtractorConfig {
                name: "uniswap_v3".to_owned(),
                protocol_system: "uniswap_v3".to_string(),
                start_block: 42,
                protocol_types: vec![ProtocolTypeConfig::new(
                    "uniswap_v3_pool".to_string(),
                    FinancialType::Swap,
                )],
                substreams_params: HashMap::from([(
                    "map_events".to_string(),
                    "factory=0x02".to_string(),
                )]),
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 42,
                    params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678"
                        .to_owned(),
                }),
                ..Default::default()
            },
        ]
    }

    fn make_uniswap_family_runtime_test_configs(
        v2_start_block: i64,
        v3_start_block: i64,
    ) -> [ExtractorConfig; 2] {
        [
            ExtractorConfig {
                name: "uniswap_v2".to_owned(),
                protocol_system: "uniswap_v2".to_string(),
                start_block: v2_start_block,
                protocol_types: vec![ProtocolTypeConfig::new(
                    "uniswap_v2_pool".to_string(),
                    FinancialType::Swap,
                )],
                ..Default::default()
            },
            ExtractorConfig {
                name: "uniswap_v3".to_owned(),
                protocol_system: "uniswap_v3".to_string(),
                start_block: v3_start_block,
                protocol_types: vec![ProtocolTypeConfig::new(
                    "uniswap_v3_pool".to_string(),
                    FinancialType::Swap,
                )],
                ..Default::default()
            },
        ]
    }

    fn resolved_family_runtime_from_configs_for_tests<'a>(
        extractor_configs: &[&'a ExtractorConfig],
        shared_spkg: &str,
    ) -> ResolvedFamilyRuntime<'a> {
        ResolvedFamilyRuntime {
            family: family_detected_runtime_from_configs_for_tests(extractor_configs, shared_spkg),
            extractor_configs: extractor_configs.to_vec(),
            execution: resolved_family_execution_config_from_extractor_configs_for_tests(
                extractor_configs,
            )
            .expect("family execution config should derive from test configs"),
        }
    }

    fn make_family_follow_up_block_scoped_data(block_number: u64, cursor: &str) -> BlockScopedData {
        use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

        let family_changes = substreams::BlockChanges {
            block: Some(substreams::Block {
                number: block_number,
                hash: vec![0x04; 32],
                parent_hash: vec![0x01; 32],
                ts: 1_718_000_001,
            }),
            changes: vec![substreams::TransactionChanges {
                tx: Some(substreams::Transaction {
                    hash: vec![0xbb; 32],
                    from: vec![0x11; 20],
                    to: vec![0x22; 20],
                    index: 8,
                }),
                contract_changes: vec![],
                entity_changes: vec![
                    substreams::EntityChanges {
                        component_id: "v2-pool".to_string(),
                        attributes: vec![],
                    },
                    substreams::EntityChanges {
                        component_id: "v3-pool".to_string(),
                        attributes: vec![],
                    },
                ],
                component_changes: vec![],
                balance_changes: vec![],
                entrypoints: vec![],
                entrypoint_params: vec![],
            }],
            storage_changes: vec![],
        };

        BlockScopedData {
            output: Some(MapModuleOutput {
                name: family_output_module_for_tests("uniswap"),
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: family_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock {
                id: block_number.to_string(),
                number: block_number,
                timestamp: None,
            }),
            cursor: cursor.to_string(),
            final_block_height: block_number,
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: String::new(),
            is_partial: false,
            partial_index: None,
            is_last_partial: None,
        }
    }

    fn make_family_contract_and_storage_follow_up_block_scoped_data(
        block_number: u64,
        cursor: &str,
    ) -> BlockScopedData {
        use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

        let family_changes = substreams::BlockChanges {
            block: Some(substreams::Block {
                number: block_number,
                hash: vec![0x05; 32],
                parent_hash: vec![0x04; 32],
                ts: 1_718_000_002,
            }),
            changes: vec![substreams::TransactionChanges {
                tx: Some(substreams::Transaction {
                    hash: vec![0xcc; 32],
                    from: vec![0x11; 20],
                    to: vec![0x22; 20],
                    index: 9,
                }),
                contract_changes: vec![substreams::ContractChange {
                    address: vec![0x44; 20],
                    balance: vec![],
                    code: vec![],
                    change: 0,
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
                    address: vec![0x55; 20],
                    slots: vec![substreams::ContractSlot {
                        slot: vec![0x01],
                        value: vec![0x02],
                        previous_value: vec![],
                    }],
                    native_balance: None,
                }],
            }],
        };

        BlockScopedData {
            output: Some(MapModuleOutput {
                name: family_output_module_for_tests("uniswap"),
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: family_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock {
                id: block_number.to_string(),
                number: block_number,
                timestamp: None,
            }),
            cursor: cursor.to_string(),
            final_block_height: block_number,
            debug_map_outputs: vec![],
            debug_store_outputs: vec![],
            attestation: String::new(),
            is_partial: false,
            partial_index: None,
            is_last_partial: None,
        }
    }

    #[path = "runner_family_tests.rs"]
    mod family_runner_tests;

    #[path = "runner_family_lifecycle_tests.rs"]
    mod family_runner_lifecycle_tests;

    #[path = "runner_family_planning_tests.rs"]
    mod family_runner_planning_tests;

    #[path = "runner_family_bootstrap_tests.rs"]
    mod family_runner_bootstrap_tests;

    #[path = "runner_family_runtime_metadata_tests.rs"]
    mod family_runner_runtime_metadata_tests;

    #[test]
    fn test_extractor_config_without_dci_plugin() {
        let yaml = r#"
name: uniswap_v2
chain: ethereum
implementation_type: Custom
sync_batch_size: 1000
start_block: 10008300
protocol_types:
  - name: uniswap_v2_pool
    financial_type: Swap
spkg: substreams/ethereum-uniswap-v2/ethereum-uniswap-v2-v0.3.0.spkg
module_name: map_pool_events
"#;

        let config: ExtractorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize YAML");

        // Verify basic fields
        assert_eq!(config.name, "uniswap_v2");

        // Verify DCI plugin is None (optional field)
        assert!(config.dci_plugin.is_none());
    }

    #[test]
    fn test_dci_extractor_config() {
        let yaml = r#"
name: uniswap_v3
chain: ethereum
implementation_type: Custom
sync_batch_size: 1000
start_block: 12369621
protocol_types:
  - name: uniswap_v3_pool
    financial_type: Swap
spkg: substreams/ethereum-uniswap-v3/ethereum-uniswap-v3-logs-only-0.1.1.spkg
module_name: map_protocol_changes
dci_plugin:
  type: rpc
"#;

        let config: ExtractorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize YAML");

        // Verify basic fields
        assert_eq!(config.name, "uniswap_v3");

        // Verify DCI plugin is RPC
        assert!(
            matches!(config.dci_plugin, Some(DCIType::RPC)),
            "Expected RPC DCI plugin but got {:?}",
            config.dci_plugin
        );
    }

    #[test]
    fn test_uniswap_v4_hooks_dci_extractor_config() {
        let yaml = r#"
name: uniswap_v4
chain: ethereum
implementation_type: Custom
sync_batch_size: 1000
start_block: 21688329
protocol_types:
  - name: uniswap_v4_pool
    financial_type: Swap
spkg: substreams/ethereum-uniswap-v4/ethereum-uniswap-v4-v0.2.1.spkg
module_name: map_protocol_changes
dci_plugin:
  type: uniswap_v4_hooks
  router_address: "0x2e234DAe75C793f67A35089C9d99245E1C58470b"
  pool_manager_address: "0x000000000004444c5dc75cB358380D2e3dE08A90"
"#;

        let config: ExtractorConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize YAML");

        // Verify basic fields
        assert_eq!(config.name, "uniswap_v4");
        assert_eq!(config.chain, Chain::Ethereum);
        assert_eq!(config.sync_batch_size, 1000);
        assert_eq!(config.start_block, 21688329);

        // Verify protocol types
        assert_eq!(config.protocol_types.len(), 1);
        assert_eq!(config.protocol_types[0].name, "uniswap_v4_pool");

        // Verify DCI plugin configuration
        let dci_plugin = config
            .dci_plugin
            .expect("Expected dci_plugin to be set");
        match dci_plugin {
            DCIType::UniswapV4Hooks { pool_manager_address } => {
                assert_eq!(pool_manager_address, "0x000000000004444c5dc75cB358380D2e3dE08A90");
            }
            _ => {
                panic!("Expected UniswapV4Hooks DCI plugin but got RPC");
            }
        }
    }

    fn one_msg() -> ExtractorMsg {
        Arc::new(BlockAggregatedChanges::default())
    }

    #[tokio::test]
    async fn test_process_block_data_partial_blocks_disabled() {
        // When partial_blocks is false: handle_tick_scoped_data is called with data as-is;
        // collect_and_process_full_block is not called. One message from handle_tick_scoped_data.
        let data = make_block_scoped_data(false, None, None);
        let mut mock = MockExtractor::new();
        mock.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                assert!(!inp.is_partial, "data must be sent as full block");
                Ok(Some(one_msg()))
            });
        let extractor: Arc<dyn Extractor> = Arc::new(mock);

        let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, false)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test]
    async fn test_process_block_data_final_partial() {
        // When partial_blocks is true and is_last_partial == true: handle_tick_scoped_data with
        // data, then collect_and_process_full_block. Two messages (one from each).
        let data = make_block_scoped_data(true, Some(2), Some(true));
        let mut mock = MockExtractor::new();
        mock.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                assert_eq!(inp.partial_index, Some(2));
                assert_eq!(inp.is_last_partial, Some(true));
                Ok(Some(one_msg()))
            });
        mock.expect_collect_and_process_full_block()
            .once()
            .returning(|_cursor: String, _final_block_height: u64, _clock: Option<Clock>| {
                Ok(Some(one_msg()))
            });
        let extractor: Arc<dyn Extractor> = Arc::new(mock);

        let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, true)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 2);
    }

    #[tokio::test]
    async fn test_process_block_data_full_block() {
        // When partial_blocks is true and message is full block: handle_tick_scoped_data
        // receives data as-is; runner adds a partial copy of the returned message.
        let data = make_block_scoped_data(false, None, None);
        let mut mock = MockExtractor::new();
        mock.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                assert!(!inp.is_partial, "data is sent as full block");
                Ok(Some(one_msg()))
            });
        let extractor: Arc<dyn Extractor> = Arc::new(mock);

        let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, true)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].partial_block_index, Some(0));
        assert!(msgs[1].partial_block_index.is_none());
    }

    #[tokio::test]
    async fn test_process_block_data_middle_partial() {
        // When partial_blocks is true and message is a non-final partial: only
        // handle_tick_scoped_data; collect_and_process_full_block is not called. One message.
        let data = make_block_scoped_data(true, Some(1), Some(false));
        let mut mock = MockExtractor::new();
        mock.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                assert_eq!(inp.partial_index, Some(1));
                assert_eq!(inp.is_last_partial, Some(false));
                Ok(Some(one_msg()))
            });
        let extractor: Arc<dyn Extractor> = Arc::new(mock);

        let msgs = ExtractorRunner::process_block_data(extractor.as_ref(), &data, true)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test]
    async fn test_extractor_runner_builder_fresh_start_no_db_state() {
        // No DB state: get_last_processed_block returns None, so the stream
        // starts from the config start_block with no cursor.
        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| None);
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(None));
        mock_extractor
            .expect_flush()
            .returning(|| Ok(()));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        // Build the ExtractorRunnerBuilder
        let extractor = Arc::new(mock_extractor);
        let builder = ExtractorBuilder::new(
            &ExtractorConfig {
                name: "test_module".to_owned(),
                implementation_type: ImplementationType::Vm,
                protocol_types: vec![ProtocolTypeConfig {
                    name: "test_module_pool".to_owned(),
                    financial_type: FinancialType::Swap,
                }],
                spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
                module_name: "test_module".to_owned(),
                ..Default::default()
            },
            "https://mainnet.eth.streamingfast.io",
            None,
            "test_token",
        )
        .token("test_token")
        .set_extractor(extractor);

        // Run the builder
        let (runner, _handle) = builder.into_runner().await.unwrap();

        // Wait for the handle to complete
        match runner.run().await {
            Ok(_) => {
                info!("ExtractorRunnerBuilder completed successfully");
            }
            Err(err) => {
                error!(error = %err, "ExtractorRunnerBuilder failed");
                panic!("ExtractorRunnerBuilder failed");
            }
        }
    }

    #[tokio::test]
    async fn test_start_block_no_db_state() {
        use crate::substreams::mock::start_mock_substreams;

        let (captured, addr) = start_mock_substreams().await;

        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| None);
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(None));
        mock_extractor
            .expect_flush()
            .returning(|| Ok(()));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        let extractor = Arc::new(mock_extractor);
        let builder = ExtractorBuilder::new(
            &ExtractorConfig {
                name: "test_module".to_owned(),
                implementation_type: ImplementationType::Vm,
                protocol_types: vec![ProtocolTypeConfig {
                    name: "test_module_pool".to_owned(),
                    financial_type: FinancialType::Swap,
                }],
                spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
                module_name: "test_module".to_owned(),
                start_block: 42,
                substreams_params: HashMap::from([(
                    "test_module".to_owned(),
                    "bootstrap_block=42&pool=0x1234".to_owned(),
                )]),
                ..Default::default()
            },
            &format!("http://{addr}"),
            None,
            "test_token",
        )
        .token("test_token")
        .set_extractor(extractor);

        let (runner, _handle) = builder.into_runner().await.unwrap();
        let handle = runner.run();
        handle.await.unwrap().unwrap();

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
        assert_eq!(requests[0].start_block_num, 42);
        assert!(requests[0].start_cursor.is_empty(), "fresh start should have no cursor");
        assert_eq!(
            requests[0].params.get("test_module"),
            Some(&"bootstrap_block=42&pool=0x1234".to_owned())
        );
    }

    #[tokio::test]
    async fn test_start_block_with_db_state() {
        use chrono::NaiveDateTime;
        use tycho_common::models::blockchain::Block;

        use crate::substreams::mock::start_mock_substreams;

        let (captured, addr) = start_mock_substreams().await;

        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| {
                Some(Block::new(
                    1000,
                    Chain::Ethereum,
                    vec![0x01].into(),
                    vec![0x00].into(),
                    NaiveDateTime::default(),
                ))
            });
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(None));
        mock_extractor
            .expect_flush()
            .returning(|| Ok(()));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        let extractor = Arc::new(mock_extractor);
        let builder = ExtractorBuilder::new(
            &ExtractorConfig {
                name: "test_module".to_owned(),
                implementation_type: ImplementationType::Vm,
                protocol_types: vec![ProtocolTypeConfig {
                    name: "test_module_pool".to_owned(),
                    financial_type: FinancialType::Swap,
                }],
                spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
                module_name: "test_module".to_owned(),
                start_block: 500,
                ..Default::default()
            },
            &format!("http://{addr}"),
            None,
            "test_token",
        )
        .token("test_token")
        .set_extractor(extractor);

        let (runner, _handle) = builder.into_runner().await.unwrap();
        let handle = runner.run();
        handle.await.unwrap().unwrap();

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
        assert_eq!(
            requests[0].start_block_num, 1001,
            "should use last_committed + 1, not config's start_block"
        );
        assert!(requests[0].start_cursor.is_empty(), "fresh start should have no cursor");
    }

    #[tokio::test]
    async fn test_skip_bootstrap_when_completed_state_exists() {
        use crate::substreams::mock::start_mock_substreams;

        let (captured, addr) = start_mock_substreams().await;

        let mut mock_extractor = MockExtractor::new();
        mock_extractor
            .expect_get_last_processed_block()
            .returning(|| None);
        mock_extractor
            .expect_get_cursor()
            .returning(String::new);
        mock_extractor
            .expect_get_completed_bootstrap_block()
            .returning(|| Ok(Some(42)));
        mock_extractor
            .expect_get_id()
            .returning(ExtractorIdentity::default);

        let extractor = Arc::new(mock_extractor);
        let builder = ExtractorBuilder::new(
            &ExtractorConfig {
                name: "uniswap_v3".to_owned(),
                implementation_type: ImplementationType::Custom,
                protocol_types: vec![ProtocolTypeConfig {
                    name: "uniswap_v3_pool".to_owned(),
                    financial_type: FinancialType::Swap,
                }],
                spkg: "./test/spkg/substreams-ethereum-quickstart-v1.0.0.spkg".to_owned(),
                module_name: "map_protocol_changes".to_owned(),
                start_block: 42,
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 42,
                    params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                        .to_owned(),
                }),
                ..Default::default()
            },
            &format!("http://{addr}"),
            None,
            "test_token",
        )
        .token("test_token")
        .set_extractor(extractor);

        let (runner, _handle) = builder.into_runner().await.unwrap();
        let handle = runner.run();
        handle.await.unwrap().unwrap();

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one gRPC request");
        assert_eq!(
            requests[0].start_block_num, 43,
            "should start from bootstrap block + 1 when bootstrap is already completed"
        );
        assert!(requests[0].start_cursor.is_empty(), "fresh start should have no cursor");
    }

    #[tokio::test]
    async fn test_family_runner_does_not_durably_persist_failing_block_across_branches() {
        use std::sync::Arc;

        use alloy::primitives::Address as AlloyAddress;
        use tycho_common::{
            models::ProtocolType,
            storage::{ExtractionStateGateway, ProtocolGateway, StorageError},
        };
        use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
        use tycho_storage::postgres::{builder::GatewayBuilder, testing::run_against_db};

        use crate::extractor::{
            chain_state::ChainState,
            protocol_cache::ProtocolMemoryCache,
            protocol_extractor::{ExtractorPgGateway, ProtocolExtractor},
            MockExtractorExtension,
        };

        fn family_block_with_branch_ids(
            number: u64,
            v2_component_id: &str,
            v3_component_id: &str,
            reserve0: u64,
            _v2_contract_byte: u8,
            _v3_contract_byte: u8,
            token0: &Bytes,
            token1: &Bytes,
        ) -> BlockScopedData {
            use crate::pb::sf::substreams::rpc::v2::MapModuleOutput;

            let family_changes = substreams::BlockChanges {
                block: Some(substreams::Block {
                    number,
                    hash: vec![number as u8; 32],
                    parent_hash: vec![number.saturating_sub(1) as u8; 32],
                    ts: 1_718_000_000,
                }),
                changes: vec![substreams::TransactionChanges {
                    tx: Some(substreams::Transaction {
                        hash: vec![number as u8; 32],
                        from: vec![0x01; 20],
                        to: vec![0x02; 20],
                        index: 0,
                    }),
                    contract_changes: vec![],
                    entity_changes: vec![substreams::EntityChanges {
                        component_id: v2_component_id.to_string(),
                        attributes: vec![substreams::Attribute {
                            name: "reserve0".to_string(),
                            value: Bytes::from(reserve0)
                                .lpad(32, 0)
                                .to_vec(),
                            change: substreams::ChangeType::Creation as i32,
                        }],
                    }],
                    component_changes: vec![
                        substreams::ProtocolComponent {
                            id: v2_component_id.to_string(),
                            tokens: vec![token0.to_vec(), token1.to_vec()],
                            contracts: vec![],
                            static_att: vec![],
                            protocol_type: Some(substreams::ProtocolType {
                                name: "uniswap_v2_pool".to_string(),
                                financial_type: substreams::FinancialType::Swap as i32,
                                attribute_schema: vec![],
                                implementation_type: substreams::ImplementationType::Custom as i32,
                            }),
                            change: substreams::ChangeType::Creation as i32,
                        },
                        substreams::ProtocolComponent {
                            id: v3_component_id.to_string(),
                            tokens: vec![token0.to_vec(), token1.to_vec()],
                            contracts: vec![],
                            static_att: vec![],
                            protocol_type: Some(substreams::ProtocolType {
                                name: "uniswap_v3_pool".to_string(),
                                financial_type: substreams::FinancialType::Swap as i32,
                                attribute_schema: vec![],
                                implementation_type: substreams::ImplementationType::Custom as i32,
                            }),
                            change: substreams::ChangeType::Creation as i32,
                        },
                    ],
                    balance_changes: vec![],
                    entrypoints: vec![],
                    entrypoint_params: vec![],
                }],
                storage_changes: vec![],
            };

            BlockScopedData {
                output: Some(MapModuleOutput {
                    name: family_output_module_for_tests("uniswap"),
                    map_output: Some(prost_types::Any {
                        type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                        value: family_changes.encode_to_vec(),
                    }),
                    debug_info: None,
                }),
                clock: Some(Clock { id: number.to_string(), number, timestamp: None }),
                cursor: format!("cursor@{number}"),
                final_block_height: number,
                debug_map_outputs: vec![],
                debug_store_outputs: vec![],
                attestation: String::new(),
                is_partial: false,
                partial_index: None,
                is_last_partial: None,
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
                .expect("seed tokens for family persistence isolation");

            let rpc = EthereumRpcClient::new("http://localhost:0000")
                .expect("Failed to create stub RPC client");
            let token_processor = EthereumTokenPreProcessor::new(&rpc, chain, AlloyAddress::ZERO);
            let protocol_cache = ProtocolMemoryCache::new(
                chain,
                chrono::Duration::seconds(900),
                Arc::new(direct_gw.clone()),
            );
            protocol_cache
                .populate()
                .await
                .expect("populate protocol cache");

            let v2_gateway = ExtractorPgGateway::new(
                "uniswap_v2",
                chain,
                1000,
                cached_gw.clone(),
                None,
            );
            let v2_extractor = Arc::new(
                ProtocolExtractor::<
                    ExtractorPgGateway,
                    EthereumTokenPreProcessor,
                    MockExtractorExtension,
                >::new(
                    v2_gateway,
                    1,
                    "uniswap_v2",
                    chain,
                    ChainState::default(),
                    "uniswap_v2".to_string(),
                    protocol_cache,
                    HashMap::from([(
                        "uniswap_v2_pool".to_string(),
                        ProtocolType::new(
                            "uniswap_v2_pool".to_string(),
                            tycho_common::models::FinancialType::Swap,
                            None,
                            ImplementationType::Custom,
                        ),
                    )]),
                    vec![],
                    token_processor,
                    None,
                    None,
                )
                .await
                .expect("build real v2 extractor"),
            );
            v2_extractor
                .ensure_protocol_types()
                .await
                .expect("persist v2 protocol types");

            let v3_call_count = Arc::new(std::sync::Mutex::new(0usize));
            let mut v3 = MockExtractor::new();
            {
                let v3_call_count = Arc::clone(&v3_call_count);
                v3.expect_handle_tick_scoped_data()
                    .times(0..)
                    .returning(move |_| {
                        let mut count = v3_call_count.lock().expect("lock v3 call count");
                        *count += 1;
                        if *count == 1 {
                            Ok(Some(Arc::new(BlockAggregatedChanges::default())))
                        } else {
                            Err(ExtractionError::Unknown(
                                "simulated v3 branch failure".to_string(),
                            ))
                        }
                    });
            }

            let dispatcher = FamilyBlockChangesDispatcher::new([
                FamilyBranchSpec {
                    protocol_system: "uniswap_v2".to_string(),
                    protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
                },
                FamilyBranchSpec {
                    protocol_system: "uniswap_v3".to_string(),
                    protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
                },
            ])
            .expect("dispatcher builds");

            let runner = family_runner_for_tests(
                HashMap::from([
                    ("uniswap_v2".to_string(), v2_extractor.clone() as Arc<dyn Extractor>),
                    ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
                ]),
                SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
                    Ok(BlockResponse::New(family_block_with_branch_ids(
                        100,
                        "v2-block-100-pool",
                        "v3-block-100-pool",
                        1_000,
                        0x44,
                        0x55,
                        &token0,
                        &token1,
                    ))),
                    Ok(BlockResponse::New(family_block_with_branch_ids(
                        101,
                        "v2-block-101-pool",
                        "v3-block-101-pool",
                        2_000,
                        0x46,
                        0x57,
                        &token0,
                        &token1,
                    ))),
                ]))),
                HashMap::from([
                    ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                    ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                ]),
                dispatcher,
            );

            let err = runner.run().await.unwrap().expect_err("family runner should fail");
            assert!(
                matches!(err, ExtractionError::Unknown(ref message) if message == "simulated v3 branch failure"),
                "unexpected error: {err:?}"
            );
            assert_eq!(
                *v3_call_count.lock().expect("lock v3 call count"),
                2,
                "expected both family blocks to reach the v3 branch before the synthetic failure"
            );
            v2_extractor
                .await_pending_commit_for_test()
                .await
                .expect("complete v2 commit task");

            let mut persisted_state = None;
            for _ in 0..20 {
                match cached_gw.get_state("uniswap_v2", &chain).await {
                    Ok(state) => {
                        persisted_state = Some(state);
                        break;
                    }
                    Err(StorageError::NotFound(_, _)) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(err) => panic!("unexpected read error while waiting for v2 state: {err}"),
                }
            }
            let persisted_state = persisted_state.unwrap_or_else(|| {
                panic!("expected block 100 extraction state to become durable within retry window")
            });
            assert_eq!(persisted_state.cursor, b"cursor@100".to_vec());
            assert_eq!(persisted_state.block_hash, Bytes::from(vec![100u8; 32]));

            let components = cached_gw
                .get_protocol_components(&chain, None, None, None, None)
                .await
                .expect("read protocol components after mixed success/failure family run");
            let component_ids = components
                .entity
                .iter()
                .map(|component| component.id.clone())
                .collect::<Vec<_>>();
            assert!(
                component_ids.contains(&"v2-block-100-pool".to_string()),
                "expected first successful shared-family block to persist, saw {component_ids:?}"
            );
            assert!(
                !component_ids.contains(&"v2-block-101-pool".to_string()),
                "failing shared-family block should not leave v2 durable state, saw {component_ids:?}"
            );

            let v2_states = cached_gw
                .get_protocol_states(
                    &chain,
                    None,
                    None,
                    Some(&["v2-block-100-pool", "v2-block-101-pool"]),
                    false,
                    None,
                )
                .await
                .expect("read protocol states after mixed success/failure family run");
            let state_ids = v2_states
                .entity
                .iter()
                .map(|state| state.component_id.clone())
                .collect::<Vec<_>>();
            assert!(
                state_ids.contains(&"v2-block-100-pool".to_string()),
                "expected durable state for first successful block, saw {state_ids:?}"
            );
            assert!(
                !state_ids.contains(&"v2-block-101-pool".to_string()),
                "failing shared-family block should not leave durable v2 protocol state, saw {state_ids:?}"
            );

            assert!(
                matches!(
                    cached_gw.get_state("uniswap_v3", &chain).await,
                    Err(StorageError::NotFound(_, _))
                ),
                "mock v3 branch should not persist extraction state"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_extractor_runner_flushes_on_stream_end() {
        let mut extractor = MockExtractor::new();
        extractor
            .expect_get_id()
            .return_const(ExtractorIdentity::default());
        extractor
            .expect_flush()
            .once()
            .returning(|| Ok(()));

        let runner = ExtractorRunner::new(
            Arc::new(extractor),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![Ok(BlockResponse::Ended)]))),
            Arc::new(Mutex::new(HashMap::new())),
            mpsc::channel(4).1,
            None,
            false,
        );

        runner.run().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_family_runner_flushes_all_branches_on_stream_end() {
        let mut v2 = MockExtractor::new();
        v2.expect_flush()
            .once()
            .returning(|| Ok(()));
        let mut v3 = MockExtractor::new();
        v3.expect_flush()
            .once()
            .returning(|| Ok(()));

        let dispatcher = FamilyBlockChangesDispatcher::new([
            FamilyBranchSpec {
                protocol_system: "uniswap_v2".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
            },
            FamilyBranchSpec {
                protocol_system: "uniswap_v3".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
            },
        ])
        .expect("dispatcher builds");

        let runner = family_runner_for_tests(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![Ok(BlockResponse::Ended)]))),
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ]),
            dispatcher,
        );

        runner.run().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_family_runner_subscribe_resolves_alias_named_handle_to_protocol_system() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v2_alias".to_string(),
            });
        v2.expect_protocol_system()
            .return_const("uniswap_v2".to_string());

        let mut v3 = MockExtractor::new();
        v3.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v3_alias".to_string(),
            });
        v3.expect_protocol_system()
            .return_const("uniswap_v3".to_string());

        let dispatcher = FamilyBlockChangesDispatcher::new([
            FamilyBranchSpec {
                protocol_system: "uniswap_v2".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
            },
            FamilyBranchSpec {
                protocol_system: "uniswap_v3".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
            },
        ])
        .expect("dispatcher builds");

        let v2_subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let v3_subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let mut runner = family_runner_for_tests(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::empty())),
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::clone(&v2_subscriptions)),
                ("uniswap_v3".to_string(), Arc::clone(&v3_subscriptions)),
            ]),
            dispatcher,
        );

        let (tx, _rx) = mpsc::channel(4);
        runner
            .subscribe(
                ExtractorIdentity { chain: Chain::Ethereum, name: "uniswap_v2_alias".to_string() },
                tx,
            )
            .await;

        assert_eq!(
            v2_subscriptions.lock().await.len(),
            1,
            "alias-named family handle should subscribe against protocol-system branch"
        );
        assert_eq!(
            v3_subscriptions.lock().await.len(),
            0,
            "subscription should not be attached to a different branch"
        );

        assert_eq!(
            runner
                .branch_subscription_index()
                .get("uniswap_v2"),
            Some("uniswap_v2"),
            "protocol-system keyed subscriptions should still resolve directly"
        );
    }

    #[test]
    fn test_family_branch_subscription_index_learns_aliases_from_extractors() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v2_alias".to_string(),
            });
        let mut v3 = MockExtractor::new();
        v3.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v3_alias".to_string(),
            });

        let extractors = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let mut index = FamilyBranchSubscriptionIndex::from_extractors(&extractors);

        assert_eq!(index.get("uniswap_v2"), Some("uniswap_v2"));
        assert_eq!(index.get("uniswap_v2_alias"), Some("uniswap_v2"));
        assert_eq!(
            index.resolve_or_learn(
                &ExtractorIdentity {
                    chain: Chain::Ethereum,
                    name: "uniswap_v2_alias".to_string(),
                },
                &extractors,
            ),
            Some("uniswap_v2".to_string())
        );
        assert_eq!(index.get("uniswap_v2_alias"), Some("uniswap_v2"));
    }

    #[tokio::test]
    async fn test_family_branch_runtime_wiring_uses_protocol_system_keys_and_emits_handles() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v2_alias".to_string(),
            });

        let mut v3 = MockExtractor::new();
        v3.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v3_alias".to_string(),
            });

        let extractors = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let (control_tx, _control_rx) = mpsc::channel(4);

        let wiring = FamilyBranchRuntimeWiring::from_extractors(extractors, &control_tx);

        let subscription_keys = wiring
            .subscriptions
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        assert_eq!(
            subscription_keys,
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()]),
            "family runner wiring should key subscriptions by protocol_system"
        );
        for subscribers in wiring.subscriptions.values() {
            assert!(
                subscribers.lock().await.is_empty(),
                "family runner wiring should start with empty subscriber maps"
            );
        }

        let handle_names = wiring
            .handles
            .into_iter()
            .map(|handle| handle.get_id().name)
            .collect::<HashSet<_>>();
        assert_eq!(
            handle_names,
            HashSet::from(["uniswap_v2_alias".to_string(), "uniswap_v3_alias".to_string()]),
            "family runner wiring should emit one handle per branch extractor identity"
        );
    }

    #[test]
    fn test_family_runner_extractors_by_protocol_system_uses_protocol_system_keys_for_aliased_members(
    ) {
        let v2_config = ExtractorConfig {
            name: "uniswap_v2_alias".to_string(),
            protocol_system: "uniswap_v2".to_string(),
            ..Default::default()
        };
        let v3_config = ExtractorConfig {
            name: "uniswap_v3_alias".to_string(),
            protocol_system: "uniswap_v3".to_string(),
            ..Default::default()
        };
        let v2_extractor: Arc<dyn Extractor> = Arc::new(MockExtractor::new());
        let v3_extractor: Arc<dyn Extractor> = Arc::new(MockExtractor::new());

        let extractors = extractors_by_protocol_system(vec![
            ExtractorBuilder::new(&v2_config, "http://localhost:9000", None, "token")
                .set_extractor(Arc::clone(&v2_extractor)),
            ExtractorBuilder::new(&v3_config, "http://localhost:9000", None, "token")
                .set_extractor(Arc::clone(&v3_extractor)),
        ]);

        let keys = extractors
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        assert_eq!(
            keys,
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()]),
            "family startup should normalize built branch extractors onto protocol_system keys"
        );
        assert!(
            Arc::ptr_eq(
                extractors
                    .get("uniswap_v2")
                    .expect("v2 extractor is keyed by protocol_system"),
                &v2_extractor
            ),
            "family startup should preserve the built extractor for the v2 protocol_system key"
        );
        assert!(
            Arc::ptr_eq(
                extractors
                    .get("uniswap_v3")
                    .expect("v3 extractor is keyed by protocol_system"),
                &v3_extractor
            ),
            "family startup should preserve the built extractor for the v3 protocol_system key"
        );
    }

    #[tokio::test]
    async fn test_family_runner_build_managed_from_startup_preserves_protocol_system_keyed_shape() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v2_alias".to_string(),
            });
        let mut v3 = MockExtractor::new();
        v3.expect_get_id()
            .return_const(ExtractorIdentity {
                chain: Chain::Ethereum,
                name: "uniswap_v3_alias".to_string(),
            });

        let configs = make_uniswap_family_bootstrap_test_configs();
        let config_refs = configs.iter().collect::<Vec<_>>();
        let family_execution =
            resolved_family_execution_config_from_extractor_configs_for_tests(&config_refs)
                .expect("family execution should derive from test configs");
        let protocol_cache = ProtocolMemoryCache::new(
            Chain::Ethereum,
            chrono::Duration::seconds(60),
            Arc::new(MockGateway::new()),
        );
        let extractors = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let dispatcher = FamilyBlockChangesDispatcher::from_protocol_cache(
            &family_execution.branch_specs,
            &protocol_cache,
        )
        .await
        .expect("dispatcher should seed from protocol cache");
        let prepared_startup = PreparedFamilyRunnerStartup {
            extractors: extractors.clone(),
            stream: SubstreamsStream::from_stream(Box::pin(stream::empty())),
            runtime_state: crate::extractor::family_runtime_execution::FamilyRuntimeState::new(
                &extractors,
                dispatcher,
                protocol_cache.clone(),
            ),
        };

        let (runner, handles) = build_family_managed_runner_from_startup(
            prepared_startup,
            None,
            false,
        )
        .await
        .expect("family-owned managed build should assemble runner from prepared startup");

        let handle_names = handles
            .into_iter()
            .map(|handle| handle.get_id().name)
            .collect::<HashSet<_>>();
        assert_eq!(
            handle_names,
            HashSet::from(["uniswap_v2_alias".to_string(), "uniswap_v3_alias".to_string()]),
            "family-owned build surface should emit one handle per branch identity"
        );

        let ManagedRunner::Family(runner) = runner else {
            panic!("family startup should assemble a family managed runner");
        };
        assert_eq!(
            runner
                .extractors
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()]),
            "family-owned build surface should preserve protocol_system keyed extractors"
        );
        assert_eq!(
            runner
                .subscriptions
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()]),
            "family-owned build surface should preserve protocol_system keyed subscriptions"
        );
    }

    #[test]
    fn test_validate_bootstrap_config_accepts_matching_runtime_blocks() {
        let config = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            start_block: 42,
            ..Default::default()
        };
        let bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let plan = SharedBootstrapPlan::for_extractor_config(&config, &bootstrap)
            .expect("matching bootstrap config should validate");

        assert_eq!(plan.bootstrap_block, 42);
        assert_eq!(plan.branches.len(), 1);
    }

    #[test]
    fn test_validate_bootstrap_config_rejects_runtime_block_mismatch() {
        let config = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            start_block: 43,
            ..Default::default()
        };
        let bootstrap = BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block: 42,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234".to_owned(),
        };

        let err = SharedBootstrapPlan::for_extractor_config(&config, &bootstrap)
            .expect_err("mismatched start blocks must fail");

        assert!(err
            .to_string()
            .contains("runtime start_block"));
    }
}
