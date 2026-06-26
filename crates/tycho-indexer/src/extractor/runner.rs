use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
};

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
    family_dispatch::{FamilyBlockChangesDispatcher, FamilyBranchSpec},
    family_runtime::{validate_family_runtime_membership, DetectedFamilyRuntime, ResolvedFamilyRuntime},
        post_processors::POST_PROCESSOR_REGISTRY,
        protocol_cache::ProtocolMemoryCache,
        protocol_extractor::{ExtractorPgGateway, ProtocolExtractor},
        shared_bootstrap::{
            materialize_plan_block, split_plan_block_by_protocol_system, SharedBootstrapPlan,
        },
        ExtractionError, Extractor, ExtractorExtension, ExtractorMsg,
    },
    pb::sf::substreams::{rpc::v2::BlockScopedData, v1::Package},
    substreams::{
        stream::{BlockResponse, SubstreamsStream},
        SubstreamsEndpoint,
    },
};

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
    fn new(id: ExtractorIdentity, control_tx: Sender<ControlMessage>) -> Self {
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
type SubscriptionsMap = HashMap<u64, Sender<ExtractorMsg>>;
type BranchSubscriptionsMap = HashMap<String, Arc<Mutex<SubscriptionsMap>>>;

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

pub struct FamilyExtractorRunner {
    extractors: HashMap<String, Arc<dyn Extractor>>,
    substreams: SubstreamsStream,
    subscriptions: BranchSubscriptionsMap,
    next_subscriber_id: u64,
    control_rx: Receiver<ControlMessage>,
    runtime_handle: Option<Handle>,
    partial_blocks: bool,
    dispatcher: FamilyBlockChangesDispatcher,
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
    async fn process_block_data(
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
    async fn propagate_msg(subscribers: &Arc<Mutex<SubscriptionsMap>>, message: ExtractorMsg) {
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

impl FamilyExtractorRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extractors: HashMap<String, Arc<dyn Extractor>>,
        substreams: SubstreamsStream,
        subscriptions: BranchSubscriptionsMap,
        control_rx: Receiver<ControlMessage>,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
        dispatcher: FamilyBlockChangesDispatcher,
    ) -> Self {
        Self {
            extractors,
            substreams,
            subscriptions,
            next_subscriber_id: 0,
            control_rx,
            runtime_handle,
            partial_blocks,
            dispatcher,
        }
    }

    pub fn run(mut self) -> JoinHandle<Result<(), ExtractionError>> {
        let runtime = self
            .runtime_handle
            .clone()
            .unwrap_or_else(|| Handle::current());

        runtime.spawn(async move {
            let family_id = self
                .extractors
                .keys()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let mut partials_in_block: u32 = 0;

            loop {
                let loop_span = info_span!(
                    parent: None,
                    "extractor_family",
                    extractor_id = %family_id,
                    sf_trace_id = tracing::field::Empty,
                    block_number = tracing::field::Empty,
                    otel.status_code = tracing::field::Empty,
                );

                let should_continue = async {
                    tokio::select! {
                        Some(ctrl) = self.control_rx.recv() => {
                            match ctrl {
                                ControlMessage::Stop => {
                                    warn!("Family runner stop signal received; exiting!");
                                    return Ok(false);
                                }
                                ControlMessage::Subscribe { extractor_id, sender } => {
                                    self.subscribe(extractor_id, sender).await;
                                }
                            }
                        }
                        val = self.substreams.next().instrument(info_span!("substreams_waiting")) => {
                            match val {
                                None => {
                                    error!("family stream ended");
                                    tracing::Span::current().record("otel.status_code", "error");
                                    return Err(ExtractionError::SubstreamsError(format!("{family_id}: stream ended")));
                                }
                                Some(Ok(BlockResponse::New(data))) => {
                                    let block_number = data.clock.as_ref().map(|v| v.number).unwrap_or(0);
                                    tracing::Span::current().record("block_number", block_number);

                                    if data.is_partial {
                                        partials_in_block += 1;
                                    }
                                    if data.is_last_partial == Some(true) || data.partial_index.is_none() {
                                        partials_in_block = 0;
                                    }

                                    let mut branch_payloads = self
                                        .dispatcher
                                        .dispatch_block_scoped_data(data)?
                                        .into_iter()
                                        .collect::<Vec<_>>();
                                    branch_payloads
                                        .sort_by(|(left, _), (right, _)| left.cmp(right));
                                    let mut pending_msgs = Vec::new();
                                    for (extractor_id, branch_data) in branch_payloads {
                                        let Some(extractor) = self.extractors.get(&extractor_id) else {
                                            return Err(ExtractionError::Setup(format!(
                                                "family runner missing extractor for {extractor_id}"
                                            )));
                                        };
                                        let msgs = ExtractorRunner::process_block_data(
                                            extractor.as_ref(),
                                            &branch_data,
                                            self.partial_blocks,
                                        )
                                        .await
                                        .map_err(|err| {
                                            error!(error = %err, extractor_id = %extractor_id, "Error while processing family branch block data");
                                            tracing::Span::current().record("otel.status_code", "error");
                                            err
                                        })?;
                                        let subscribers = self
                                            .subscriptions
                                            .get(&extractor_id)
                                            .expect("branch subscriptions initialized")
                                            .clone();
                                        for msg in msgs {
                                            pending_msgs.push((subscribers.clone(), msg));
                                        }
                                    }
                                    for (subscribers, msg) in pending_msgs {
                                        ExtractorRunner::propagate_msg(&subscribers, msg).await;
                                    }
                                }
                                Some(Ok(BlockResponse::Undo(undo_signal))) => {
                                    partials_in_block = 0;
                                    for (extractor_id, extractor) in &self.extractors {
                                        match extractor.handle_revert(undo_signal.clone()).await {
                                            Ok(Some(msg)) => {
                                                let subscribers = self
                                                    .subscriptions
                                                    .get(extractor_id)
                                                    .expect("branch subscriptions initialized");
                                                ExtractorRunner::propagate_msg(subscribers, msg).await;
                                            }
                                            Ok(None) => {}
                                            Err(err) => {
                                                error!(error = %err, extractor_id = %extractor_id, "Error while processing family revert");
                                                tracing::Span::current().record("otel.status_code", "error");
                                                return Err(err);
                                            }
                                        }
                                    }
                                }
                                Some(Ok(BlockResponse::Ended)) => {
                                    for extractor in self.extractors.values() {
                                        extractor.flush().await?;
                                    }
                                    tracing::Span::current().record("otel.status_code", "ok");
                                    return Ok(false);
                                }
                                Some(Err(err)) => {
                                    error!(error = %err, "Family stream terminated with error.");
                                    tracing::Span::current().record("otel.status_code", "error");
                                    return Err(ExtractionError::SubstreamsError(err.to_string()));
                                }
                            }
                        }
                    }
                    tracing::Span::current().record("otel.status_code", "ok");
                    Ok(true)
                }.instrument(loop_span).await?;

                if !should_continue {
                    break Ok(());
                }
            }
        })
    }

    async fn subscribe(&mut self, extractor_id: ExtractorIdentity, sender: Sender<ExtractorMsg>) {
        let subscriber_id = self.next_subscriber_id;
        self.next_subscriber_id += 1;
        tracing::Span::current().record("subscriber_id", subscriber_id);
        info!(?subscriber_id, ?extractor_id, "New family branch subscription");

        if let Some(subscribers) = self
            .subscriptions
            .get(&extractor_id.name)
        {
            subscribers
                .lock()
                .await
                .insert(subscriber_id, sender);
        } else {
            warn!(?extractor_id, "Ignoring subscription for unknown family branch extractor");
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

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct FamilyRuntimeConfig {
    pub family: String,
    #[serde(default)]
    pub shared_spkg: Option<String>,
    #[serde(default)]
    pub shared_module: Option<String>,
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

    pub fn family_runtime(&self) -> Option<&FamilyRuntimeConfig> {
        self.family_runtime.as_ref()
    }

    pub fn with_family_runtime(mut self, family_runtime: Option<FamilyRuntimeConfig>) -> Self {
        self.family_runtime = family_runtime;
        self
    }

    pub fn with_protocol_system(mut self, protocol_system: impl Into<String>) -> Self {
        self.protocol_system = protocol_system.into();
        self
    }
}

pub(crate) fn configured_stream_start_block(
    config: &ExtractorConfig,
) -> Result<i64, ExtractionError> {
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
    final_block_only: bool,
    partial_blocks: bool,
    /// Handle of the tokio runtime on which the extraction tasks will be run.
    /// If 'None' the default runtime will be used.
    runtime_handle: Option<Handle>,
}

impl ExtractorBuilder {
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

        let gw = ExtractorPgGateway::new(
            &self.config.name,
            self.config.chain,
            self.config.sync_batch_size,
            cached_gw.clone(),
            self.config
                .family_runtime()
                .map(|runtime| format!("family::{}", runtime.family)),
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

        self.extractor = Some(Arc::new(
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
                token_pre_processor.clone(),
                post_processor,
                dci_plugin,
            )
            .await?,
        ));

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
        let plan = SharedBootstrapPlan::for_extractor_config(&self.config, bootstrap)?;

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

        let changes = materialize_plan_block(rpc_client, &plan).await?;
        let bootstrap_block_hash = changes.block.hash.clone();

        extractor
            .handle_block_changes(changes, format!("bootstrap@{}", plan.bootstrap_block))
            .await?;

        extractor.flush().await?;
        extractor
            .mark_bootstrap_completed(plan.bootstrap_block, bootstrap_block_hash)
            .await?;

        info!(
            extractor_id = %extractor_id,
            bootstrap_block = plan.bootstrap_block,
            "BootstrapExecutorCompleted"
        );

        Ok(())
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
        let extractor = self
            .extractor
            .clone()
            .expect("Extractor not set");
        let extractor_id = extractor.get_id();

        tracing::Span::current().record("id", format!("{extractor_id}"));

        let loaded_substreams = load_substreams_package(
            self.s3_bucket.as_deref(),
            &self.config.spkg,
            &self.endpoint_url,
            Some(self.token.clone()),
        )
        .await?;

        // Determine the start block for the Substreams stream.
        //
        // We never pass a cursor on fresh start (process restart). Instead, we
        // resume from the block after the last one committed to DB. This is safe
        // because we only commit finalized block -1 to the DB. So we know last committed block + 1
        // is finalized.
        let mut last_block = extractor
            .get_last_processed_block()
            .await;
        if last_block.is_none() {
            if let Some(bootstrap) = &self.config.bootstrap {
                let completed_bootstrap_block = extractor
                    .get_completed_bootstrap_block()
                    .await?;
                let configured_bootstrap_block =
                    u64::try_from(bootstrap.start_block).map_err(|_| {
                        ExtractionError::Setup(format!(
                            "bootstrap start_block must be non-negative for extractor `{}`",
                            self.config.name
                        ))
                    })?;

                if completed_bootstrap_block == Some(configured_bootstrap_block) {
                    info!(
                        extractor_id = %extractor_id,
                        bootstrap_block = bootstrap.start_block,
                        "Bootstrap already completed in storage; skipping bootstrap run"
                    );
                } else {
                    info!(
                        bootstrap_block = bootstrap.start_block,
                        extractor_id = %extractor_id,
                        "Running bootstrap block before starting event stream"
                    );
                    tokio::select! {
                        res = self.run_bootstrap_once(
                            extractor.clone(),
                            bootstrap,
                            &extractor_id,
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
                    last_block = extractor
                        .get_last_processed_block()
                        .await;
                }
            }
        }

        let configured_stream_start = if self.config.bootstrap.is_some() {
            self.config
                .start_block
                .checked_add(1)
                .ok_or_else(|| ExtractionError::Setup("stream start block overflow".to_string()))?
        } else {
            self.config.start_block
        };
        // `None` means no blocks have been committed for this protocol yet (fresh
        // indexing), so fall back to the configured start block.
        let start_block = last_block
            .as_ref()
            .map(|b| {
                let next = b
                    .number
                    .checked_add(1)
                    .ok_or_else(|| ExtractionError::Setup("block number overflow".to_string()))?;
                i64::try_from(next)
                    .map_err(|_| ExtractionError::Setup("block number exceeds i64".to_string()))
            })
            .transpose()?
            .unwrap_or(configured_stream_start);

        if let Some(block) = &last_block {
            info!(
                start_block,
                last_committed_block = block.number,
                config_start_block = self.config.start_block,
                "Fresh start: resuming from block after last committed"
            );
        }

        let stream = SubstreamsStream::new(
            loaded_substreams.endpoint,
            None, // No cursor on fresh start; stream tracks cursor for hot reconnections
            Some(loaded_substreams.spkg),
            self.config.module_name,
            start_block,
            self.config.stop_block.unwrap_or(0) as u64,
            self.final_block_only,
            extractor_id.to_string(),
            self.partial_blocks,
            self.config.substreams_params,
        );

        let (ctrl_tx, ctrl_rx) = mpsc::channel(128);
        let runner = ExtractorRunner::new(
            extractor,
            stream,
            Arc::new(Mutex::new(HashMap::new())),
            ctrl_rx,
            self.runtime_handle,
            self.partial_blocks,
        );

        Ok((runner, ExtractorHandle::new(extractor_id, ctrl_tx)))
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn build_family_runner(
    resolved_family: &ResolvedFamilyRuntime<'_>,
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
    let family = &resolved_family.family;
    let extractor_configs = &resolved_family.extractor_configs;

    if extractor_configs.is_empty() {
        return Err(ExtractionError::Setup(format!(
            "cannot build {} family runner without extractors",
            family.family_name
        )));
    }
    validate_family_runner_membership(family, extractor_configs)?;

    let family_context = FamilyRunnerContext::from_resolved_family(resolved_family)?;
    let mut built_extractors = HashMap::new();
    let mut handles = Vec::new();

    for extractor_config in extractor_configs {
        let builder =
            ExtractorBuilder::new(extractor_config, endpoint_url, s3_bucket, substreams_api_token)
                .database_insert_batch_size(database_insert_batch_size)
                .partial_blocks(partial_blocks)
                .build(chain_state, cached_gw, token_pre_processor, protocol_cache, rpc_client)
                .await?;

        let extractor = builder
            .extractor
            .clone()
            .expect("extractor initialized in build()");
        built_extractors.insert(
            builder
                .config
                .protocol_system()
                .to_string(),
            extractor,
        );
    }

    run_family_bootstrap_if_needed(&built_extractors, extractor_configs, rpc_client).await?;

    let start_block = resolve_family_stream_start(&built_extractors, extractor_configs).await?;

    let loaded_substreams = load_substreams_package(
        s3_bucket,
        &family.shared_spkg,
        endpoint_url,
        Some(substreams_api_token.to_string()),
    )
    .await?;
    let stream = build_family_substreams_stream(
        family,
        family_context.stop_block,
        family_context.merged_substreams_params.clone(),
        loaded_substreams,
        start_block,
        partial_blocks,
    );

    let (ctrl_tx, ctrl_rx) = mpsc::channel(128);
    let mut subscriptions = HashMap::new();
    for (extractor_name, extractor) in &built_extractors {
        subscriptions.insert(extractor_name.clone(), Arc::new(Mutex::new(HashMap::new())));
        handles.push(ExtractorHandle::new(extractor.get_id(), ctrl_tx.clone()));
    }

    let dispatcher =
        build_family_dispatcher_from_cache(
            &family_context.branch_specs,
            protocol_cache,
        )
        .await?;
    let runner = FamilyExtractorRunner::new(
        built_extractors,
        stream,
        subscriptions,
        ctrl_rx,
        runtime,
        partial_blocks,
        dispatcher,
    );

    Ok((ManagedRunner::Family(runner), handles))
}

fn build_family_substreams_stream(
    family: &DetectedFamilyRuntime,
    stop_block: u64,
    merged_substreams_params: HashMap<String, String>,
    loaded_substreams: LoadedSubstreamsPackage,
    start_block: i64,
    partial_blocks: bool,
) -> SubstreamsStream {
    SubstreamsStream::new(
        loaded_substreams.endpoint,
        None,
        Some(loaded_substreams.spkg),
        family.output_module.clone(),
        start_block,
        stop_block,
        false,
        family.stream_extractor_id(),
        partial_blocks,
        merged_substreams_params,
    )
}

fn validate_family_runner_membership(
    family: &DetectedFamilyRuntime,
    extractor_configs: &[&ExtractorConfig],
) -> Result<(), ExtractionError> {
    validate_family_runtime_membership(family, extractor_configs)
}

async fn build_family_dispatcher_from_cache(
    branch_specs: &[FamilyBranchSpec],
    protocol_cache: &ProtocolMemoryCache,
) -> Result<FamilyBlockChangesDispatcher, ExtractionError> {
    let seed =
        FamilyBranchSpec::dispatcher_seed_from_protocol_cache(branch_specs, protocol_cache).await;
    FamilyBlockChangesDispatcher::new_with_seed(branch_specs.iter().cloned(), seed)
}

pub(crate) fn merge_substreams_params(
    merged: &mut HashMap<String, String>,
    incoming: &HashMap<String, String>,
    extractor_name: &str,
) -> Result<(), ExtractionError> {
    for (key, value) in incoming {
        if let Some(existing) = merged.get(key) {
            if existing != value {
                return Err(ExtractionError::Setup(format!(
                    "conflicting substreams param `{key}` while building family runner for `{extractor_name}`"
                )));
            }
        } else {
            merged.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}

pub(crate) fn merged_family_substreams_params(
    extractor_configs: &[&ExtractorConfig],
) -> Result<HashMap<String, String>, ExtractionError> {
    let mut merged_substreams_params = HashMap::new();

    for config in extractor_configs {
        merge_substreams_params(
            &mut merged_substreams_params,
            &config.substreams_params,
            config.name(),
        )?;
    }

    Ok(merged_substreams_params)
}

fn resolve_family_stop_block(
    extractor_configs: &[&ExtractorConfig],
) -> Result<u64, ExtractionError> {
    let mut resolved = None;

    for config in extractor_configs {
        let candidate = config.stop_block();
        if let Some(existing) = resolved {
            if existing != candidate {
                return Err(ExtractionError::Setup(format!(
                    "family runner requires one shared stop_block, found {:?} on `{}` and {:?} on another family member",
                    candidate,
                    config.name(),
                    existing
                )));
            }
        } else {
            resolved = Some(candidate);
        }
    }

    Ok(resolved.flatten().unwrap_or(0) as u64)
}

#[derive(Debug)]
struct FamilyRunnerContext {
    branch_specs: Vec<FamilyBranchSpec>,
    merged_substreams_params: HashMap<String, String>,
    stop_block: u64,
}

impl FamilyRunnerContext {
    fn from_resolved_family(
        resolved_family: &ResolvedFamilyRuntime<'_>,
    ) -> Result<Self, ExtractionError> {
        Self::from_extractor_configs(&resolved_family.extractor_configs)
    }

    fn from_extractor_configs(
        extractor_configs: &[&ExtractorConfig],
    ) -> Result<Self, ExtractionError> {
        let branch_specs = FamilyBranchSpec::from_extractor_configs(extractor_configs)?;
        let merged_substreams_params = merged_family_substreams_params(extractor_configs)?;
        let stop_block = resolve_family_stop_block(extractor_configs)?;

        Ok(Self {
            branch_specs,
            merged_substreams_params,
            stop_block,
        })
    }
}

fn family_extractor_config_by_protocol_system<'a>(
    extractor_configs: &[&'a ExtractorConfig],
    protocol_system: &str,
) -> Result<&'a ExtractorConfig, ExtractionError> {
    let mut matches = extractor_configs
        .iter()
        .copied()
        .filter(|config| config.protocol_system() == protocol_system);

    let first = matches.next().ok_or_else(|| {
        ExtractionError::Setup(format!(
            "family runner is missing extractor config for protocol system `{protocol_system}`"
        ))
    })?;

    if matches.next().is_some() {
        return Err(ExtractionError::Setup(format!(
            "family runner received duplicate extractor configs for protocol system `{protocol_system}`"
        )));
    }

    Ok(first)
}

async fn run_family_bootstrap_if_needed(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    extractor_configs: &[&ExtractorConfig],
    rpc_client: &EthereumRpcClient,
) -> Result<(), ExtractionError> {
    let (resume_blocks, missing_progress) = collect_family_progress(extractors).await;
    validate_family_progress_consistency(&resume_blocks, &missing_progress, "before bootstrap")?;

    let mut plan_inputs = Vec::new();
    let mut fresh_without_bootstrap = Vec::new();

    for (extractor_id, extractor) in extractors {
        let cfg = family_extractor_config_by_protocol_system(extractor_configs, extractor_id)?;
        let last_block = extractor
            .get_last_processed_block()
            .await;
        if last_block.is_some() {
            continue;
        }
        let Some(bootstrap) = cfg.bootstrap.as_ref() else {
            fresh_without_bootstrap.push(cfg.protocol_system().to_string());
            continue;
        };
        plan_inputs.push((cfg, bootstrap));
    }

    validate_family_bootstrap_configuration_consistency(&plan_inputs, &fresh_without_bootstrap)?;

    if plan_inputs.is_empty() {
        return Ok(());
    }

    let plan = SharedBootstrapPlan::for_extractor_configs(plan_inputs.iter().copied())?;
    let merged_changes = materialize_plan_block(rpc_client, &plan).await?;
    apply_family_bootstrap_plan(extractors, &plan, merged_changes).await
}

fn validate_family_bootstrap_configuration_consistency(
    plan_inputs: &[(&ExtractorConfig, &BootstrapConfig)],
    fresh_without_bootstrap: &[String],
) -> Result<(), ExtractionError> {
    if plan_inputs.is_empty() || fresh_without_bootstrap.is_empty() {
        return Ok(());
    }

    let configured = plan_inputs
        .iter()
        .map(|(config, _)| config.protocol_system().to_string())
        .collect::<Vec<_>>();

    Err(ExtractionError::Setup(format!(
        "family runner requires shared bootstrap configuration consistency across fresh branches; bootstrapped branches: {:?}, missing bootstrap branches: {:?}",
        configured, fresh_without_bootstrap
    )))
}

async fn collect_family_progress(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
) -> (Vec<(String, u64)>, Vec<String>) {
    let mut resume_blocks = Vec::new();
    let mut missing_progress = Vec::new();

    for (extractor_id, extractor) in extractors {
        match extractor
            .get_last_processed_block()
            .await
        {
            Some(block) => resume_blocks.push((extractor_id.clone(), block.number)),
            None => missing_progress.push(extractor_id.clone()),
        }
    }

    (resume_blocks, missing_progress)
}

fn validate_family_progress_consistency(
    resume_blocks: &[(String, u64)],
    missing_progress: &[String],
    context: &str,
) -> Result<(), ExtractionError> {
    if !resume_blocks.is_empty() && !missing_progress.is_empty() {
        return Err(ExtractionError::Setup(format!(
            "family runner requires consistent branch progress {context}; resumed branches: {:?}, fresh branches: {:?}",
            resume_blocks, missing_progress
        )));
    }

    Ok(())
}

async fn apply_family_bootstrap_plan(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    plan: &SharedBootstrapPlan,
    merged_changes: crate::extractor::models::BlockChanges,
) -> Result<(), ExtractionError> {
    let Some(marker_extractor) = plan
        .branches
        .first()
        .and_then(|branch| extractors.get(&branch.protocol_system))
    else {
        return Ok(());
    };

    let completed_bootstrap_block = marker_extractor
        .get_completed_bootstrap_block()
        .await?;
    if completed_bootstrap_block == Some(plan.bootstrap_block) {
        return Ok(());
    }

    let bootstrap_block_hash = merged_changes.block.hash.clone();
    let split_changes = split_plan_block_by_protocol_system(merged_changes)?;

    for branch in &plan.branches {
        let extractor = extractors
            .get(&branch.protocol_system)
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "missing family bootstrap extractor for {}",
                    branch.protocol_system
                ))
            })?;
        let changes = split_changes
            .get(&branch.protocol_system)
            .cloned()
            .ok_or_else(|| {
                ExtractionError::Setup(format!(
                    "shared bootstrap plan did not produce branch block for {}",
                    branch.protocol_system
                ))
            })?;
        extractor
            .handle_block_changes(changes, format!("bootstrap@{}", plan.bootstrap_block))
            .await?;
        extractor.flush().await?;
    }

    marker_extractor
        .mark_bootstrap_completed(plan.bootstrap_block, bootstrap_block_hash)
        .await?;

    Ok(())
}

async fn resolve_family_stream_start(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    extractor_configs: &[&ExtractorConfig],
) -> Result<i64, ExtractionError> {
    let (resume_blocks, missing_progress) = collect_family_progress(extractors).await;

    validate_family_progress_consistency(&resume_blocks, &missing_progress, "before stream start")?;

    if let Some((_, first_block)) = resume_blocks.first() {
        if resume_blocks
            .iter()
            .any(|(_, block_number)| block_number != first_block)
        {
            return Err(ExtractionError::Setup(format!(
                "family runner requires aligned branch progress, found {:?}",
                resume_blocks
            )));
        }
        let next = first_block
            .checked_add(1)
            .ok_or_else(|| ExtractionError::Setup("block number overflow".to_string()))?;
        return i64::try_from(next)
            .map_err(|_| ExtractionError::Setup("block number exceeds i64".to_string()));
    }

    let mut configured_starts = Vec::new();
    for cfg in extractor_configs {
        let configured_stream_start = configured_stream_start_block(cfg)?;
        configured_starts.push((cfg.protocol_system().to_string(), configured_stream_start));
    }

    let (_, first_start) = configured_starts
        .first()
        .ok_or_else(|| ExtractionError::Setup("family runner has no branch configs".to_string()))?;
    if configured_starts
        .iter()
        .any(|(_, start_block)| start_block != first_start)
    {
        return Err(ExtractionError::Setup(format!(
            "family runner requires aligned branch start blocks, found {:?}",
            configured_starts
        )));
    }

    Ok(*first_start)
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

async fn ensure_spkg_path(
    s3_bucket: Option<&str>,
    spkg_path: &str,
) -> Result<(), ExtractionError> {
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

struct LoadedSubstreamsPackage {
    spkg: Package,
    endpoint: Arc<SubstreamsEndpoint>,
}

async fn load_substreams_package(
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
        extractor::{protocol_cache::ProtocolDataCache, MockExtractor},
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
                name: "map_uniswap_family_protocol_changes".to_string(),
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
                name: "map_uniswap_family_protocol_changes".to_string(),
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: family_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock { id: block_number.to_string(), number: block_number, timestamp: None }),
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
                name: "map_uniswap_family_protocol_changes".to_string(),
                map_output: Some(prost_types::Any {
                    type_url: "type.googleapis.com/tycho.evm.v1.BlockChanges".to_string(),
                    value: family_changes.encode_to_vec(),
                }),
                debug_info: None,
            }),
            clock: Some(Clock { id: block_number.to_string(), number: block_number, timestamp: None }),
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
    async fn test_family_runner_dispatches_shared_stream_into_branch_extractors() {
        let family_block = make_family_block_scoped_data();

        let mut v2 = MockExtractor::new();
        v2.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
                assert_eq!(decoded.changes.len(), 1);
                assert_eq!(
                    decoded.changes[0]
                        .component_changes
                        .len(),
                    1
                );
                assert_eq!(decoded.changes[0].component_changes[0].id, "v2-pool");
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });
        v2.expect_flush().once().returning(|| Ok(()));

        let mut v3 = MockExtractor::new();
        v3.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
                assert_eq!(decoded.changes.len(), 1);
                assert_eq!(
                    decoded.changes[0]
                        .component_changes
                        .len(),
                    1
                );
                assert_eq!(decoded.changes[0].component_changes[0].id, "v3-pool");
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });
        v3.expect_flush().once().returning(|| Ok(()));

        let v2_subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let v3_subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let (v2_tx, mut v2_rx) = mpsc::channel(4);
        let (v3_tx, mut v3_rx) = mpsc::channel(4);
        v2_subscriptions
            .lock()
            .await
            .insert(0, v2_tx);
        v3_subscriptions
            .lock()
            .await
            .insert(0, v3_tx);

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

        let runner = FamilyExtractorRunner::new(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
                Ok(BlockResponse::New(family_block)),
                Ok(BlockResponse::Ended),
            ]))),
            HashMap::from([
                ("uniswap_v2".to_string(), v2_subscriptions),
                ("uniswap_v3".to_string(), v3_subscriptions),
            ]),
            mpsc::channel(4).1,
            None,
            false,
            dispatcher,
        );

        runner.run().await.unwrap().unwrap();

        assert!(v2_rx.recv().await.is_some(), "v2 subscriber should receive a message");
        assert!(v3_rx.recv().await.is_some(), "v3 subscriber should receive a message");
        assert!(v2_rx.try_recv().is_err(), "v2 should receive exactly one message");
        assert!(v3_rx.try_recv().is_err(), "v3 should receive exactly one message");
    }

    #[tokio::test]
    async fn test_family_runner_does_not_propagate_partial_branch_results_when_later_branch_fails() {
        let family_block = make_family_block_scoped_data();

        let mut v2 = MockExtractor::new();
        v2.expect_handle_tick_scoped_data()
            .once()
            .returning(|_| Ok(Some(Arc::new(BlockAggregatedChanges::default()))));

        let mut v3 = MockExtractor::new();
        v3.expect_handle_tick_scoped_data()
            .once()
            .returning(|_| Err(ExtractionError::Unknown("simulated v3 failure".to_string())));

        let v2_subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let v3_subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let (v2_tx, mut v2_rx) = mpsc::channel(4);
        let (v3_tx, mut v3_rx) = mpsc::channel(4);
        v2_subscriptions
            .lock()
            .await
            .insert(0, v2_tx);
        v3_subscriptions
            .lock()
            .await
            .insert(0, v3_tx);

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

        let runner = FamilyExtractorRunner::new(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
                Ok(BlockResponse::New(family_block)),
                Ok(BlockResponse::Ended),
            ]))),
            HashMap::from([
                ("uniswap_v2".to_string(), v2_subscriptions),
                ("uniswap_v3".to_string(), v3_subscriptions),
            ]),
            mpsc::channel(4).1,
            None,
            false,
            dispatcher,
        );

        let err = runner.run().await.unwrap().expect_err("family runner should fail");
        assert!(
            matches!(err, ExtractionError::Unknown(ref message) if message == "simulated v3 failure"),
            "unexpected error: {err:?}"
        );
        assert!(
            v2_rx.try_recv().is_err(),
            "v2 subscriber should not receive a message from a failed family block"
        );
        assert!(
            v3_rx.try_recv().is_err(),
            "v3 subscriber should not receive a message from a failed family block"
        );
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
                            value: Bytes::from(reserve0).lpad(32, 0).to_vec(),
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
                                implementation_type:
                                    substreams::ImplementationType::Custom as i32,
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
                                implementation_type:
                                    substreams::ImplementationType::Custom as i32,
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

            let v2_gateway =
                ExtractorPgGateway::new("uniswap_v2", chain, 1000, cached_gw.clone(), None);
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

            let runner = FamilyExtractorRunner::new(
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
                mpsc::channel(4).1,
                None,
                false,
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
        extractor.expect_flush().once().returning(|| Ok(()));

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
        v2.expect_flush().once().returning(|| Ok(()));
        let mut v3 = MockExtractor::new();
        v3.expect_flush().once().returning(|| Ok(()));

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

        let runner = FamilyExtractorRunner::new(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![Ok(BlockResponse::Ended)]))),
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ]),
            mpsc::channel(4).1,
            None,
            false,
            dispatcher,
        );

        runner.run().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_family_runner_reconnects_and_dispatches_follow_up_updates() {
        use std::sync::Mutex as StdMutex;

        use crate::{
            pb::sf::substreams::rpc::v2::{response::Message, Response, SessionInit},
            substreams::{
                mock::{start_scripted_mock_substreams, MockSubstreamsScript},
                SubstreamsEndpoint,
            },
        };

        fn session_response(start_block: u64) -> Response {
            Response {
                message: Some(Message::Session(SessionInit {
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

        fn block_response(block: BlockScopedData) -> Response {
            Response { message: Some(Message::BlockScopedData(block)) }
        }

        let first_block = make_family_block_scoped_data();
        let second_block = make_family_follow_up_block_scoped_data(43, "cursor-43");
        let (captured, addr) = start_scripted_mock_substreams(vec![
            MockSubstreamsScript {
                responses: vec![
                    session_response(42),
                    block_response(first_block.clone()),
                ],
                grpc_status: "13",
                grpc_message: Some("forced-reconnect"),
            },
            MockSubstreamsScript {
                responses: vec![
                    session_response(43),
                    block_response(second_block.clone()),
                ],
                grpc_status: "0",
                grpc_message: None,
            },
        ])
        .await;

        let v2_calls = Arc::new(StdMutex::new(0usize));
        let v3_calls = Arc::new(StdMutex::new(0usize));

        let mut v2 = MockExtractor::new();
        {
            let v2_calls = v2_calls.clone();
            v2.expect_handle_tick_scoped_data()
                .times(2)
                .returning(move |inp: BlockScopedData| {
                    let raw = &inp
                        .output
                        .as_ref()
                        .expect("output")
                        .map_output
                        .as_ref()
                        .expect("map output")
                        .value;
                    let decoded =
                        substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
                    let mut call = v2_calls.lock().unwrap();
                    *call += 1;
                    match *call {
                        1 => {
                            assert_eq!(inp.cursor, "cursor-42");
                            assert_eq!(decoded.changes[0].component_changes.len(), 1);
                            assert_eq!(decoded.changes[0].component_changes[0].id, "v2-pool");
                        }
                        2 => {
                            assert_eq!(inp.cursor, "cursor-43");
                            assert_eq!(decoded.changes[0].component_changes.len(), 0);
                            assert_eq!(decoded.changes[0].entity_changes.len(), 1);
                            assert_eq!(
                                decoded.changes[0].entity_changes[0].component_id,
                                "v2-pool"
                            );
                        }
                        _ => panic!("unexpected v2 call count"),
                    }
                    Ok(Some(Arc::new(BlockAggregatedChanges::default())))
                });
        }

        let mut v3 = MockExtractor::new();
        {
            let v3_calls = v3_calls.clone();
            v3.expect_handle_tick_scoped_data()
                .times(2)
                .returning(move |inp: BlockScopedData| {
                    let raw = &inp
                        .output
                        .as_ref()
                        .expect("output")
                        .map_output
                        .as_ref()
                        .expect("map output")
                        .value;
                    let decoded =
                        substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
                    let mut call = v3_calls.lock().unwrap();
                    *call += 1;
                    match *call {
                        1 => {
                            assert_eq!(inp.cursor, "cursor-42");
                            assert_eq!(decoded.changes[0].component_changes.len(), 1);
                            assert_eq!(decoded.changes[0].component_changes[0].id, "v3-pool");
                        }
                        2 => {
                            assert_eq!(inp.cursor, "cursor-43");
                            assert_eq!(decoded.changes[0].component_changes.len(), 0);
                            assert_eq!(decoded.changes[0].entity_changes.len(), 1);
                            assert_eq!(
                                decoded.changes[0].entity_changes[0].component_id,
                                "v3-pool"
                            );
                        }
                        _ => panic!("unexpected v3 call count"),
                    }
                    Ok(Some(Arc::new(BlockAggregatedChanges::default())))
                });
        }

        let endpoint = Arc::new(
            SubstreamsEndpoint::new(format!("http://{addr}"), None)
                .await
                .expect("endpoint builds"),
        );
        let stream = SubstreamsStream::new(
            endpoint,
            None,
            None,
            "map_uniswap_family_protocol_changes".to_string(),
            42,
            0,
            false,
            "ethereum:uniswap_family".to_string(),
            false,
            HashMap::new(),
        );
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

        let runner = FamilyExtractorRunner::new(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            stream,
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ]),
            mpsc::channel(4).1,
            None,
            false,
            dispatcher,
        );

        runner.run().await.unwrap().unwrap();

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 2, "expected initial request and reconnect");
        assert!(requests[0].start_cursor.is_empty());
        assert_eq!(requests[1].start_cursor, "cursor-42");
        assert_eq!(*v2_calls.lock().unwrap(), 2);
        assert_eq!(*v3_calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_family_runner_routes_existing_components_after_restart_style_preseed() {
        let follow_up_block = make_family_follow_up_block_scoped_data(43, "cursor-43");

        let mut v2 = MockExtractor::new();
        v2.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
                assert_eq!(inp.cursor, "cursor-43");
                assert_eq!(decoded.changes.len(), 1);
                assert_eq!(decoded.changes[0].component_changes.len(), 0);
                assert_eq!(decoded.changes[0].entity_changes.len(), 1);
                assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v2-pool");
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });

        let mut v3 = MockExtractor::new();
        v3.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
                assert_eq!(inp.cursor, "cursor-43");
                assert_eq!(decoded.changes.len(), 1);
                assert_eq!(decoded.changes[0].component_changes.len(), 0);
                assert_eq!(decoded.changes[0].entity_changes.len(), 1);
                assert_eq!(decoded.changes[0].entity_changes[0].component_id, "v3-pool");
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });

        let dispatcher = {
            let mut dispatcher = FamilyBlockChangesDispatcher::new([
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
            dispatcher.register_component_systems(HashMap::from([
                ("v2-pool".to_string(), "uniswap_v2".to_string()),
                ("v3-pool".to_string(), "uniswap_v3".to_string()),
            ]));
            dispatcher
        };

        let runner = FamilyExtractorRunner::new(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
                Ok(BlockResponse::New(follow_up_block)),
                Ok(BlockResponse::Ended),
            ]))),
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ]),
            mpsc::channel(4).1,
            None,
            false,
            dispatcher,
        );

        runner.run().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_family_runner_routes_contract_and_storage_follow_ups_after_restart_style_preseed()
    {
        let follow_up_block =
            make_family_contract_and_storage_follow_up_block_scoped_data(44, "cursor-44");

        let mut v2 = MockExtractor::new();
        v2.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v2 branch");
                assert_eq!(inp.cursor, "cursor-44");
                assert_eq!(decoded.changes.len(), 1);
                assert_eq!(decoded.changes[0].contract_changes.len(), 1);
                assert_eq!(decoded.changes[0].contract_changes[0].address, vec![0x44; 20]);
                assert!(decoded.storage_changes.is_empty());
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });

        let mut v3 = MockExtractor::new();
        v3.expect_handle_tick_scoped_data()
            .once()
            .returning(|inp: BlockScopedData| {
                let raw = &inp
                    .output
                    .as_ref()
                    .expect("output")
                    .map_output
                    .as_ref()
                    .expect("map output")
                    .value;
                let decoded =
                    substreams::BlockChanges::decode(raw.as_slice()).expect("decode v3 branch");
                assert_eq!(inp.cursor, "cursor-44");
                assert!(decoded.changes.is_empty());
                assert_eq!(decoded.storage_changes.len(), 1);
                assert_eq!(decoded.storage_changes[0].storage_changes[0].address, vec![0x55; 20]);
                Ok(Some(Arc::new(BlockAggregatedChanges::default())))
            });

        let dispatcher = {
            let mut dispatcher = FamilyBlockChangesDispatcher::new([
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
            dispatcher.register_contract_systems(HashMap::from([
                (vec![0x44; 20], "uniswap_v2".to_string()),
                (vec![0x55; 20], "uniswap_v3".to_string()),
            ]));
            dispatcher
        };

        let runner = FamilyExtractorRunner::new(
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
                ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
            ]),
            SubstreamsStream::from_stream(Box::pin(stream::iter(vec![
                Ok(BlockResponse::New(follow_up_block)),
                Ok(BlockResponse::Ended),
            ]))),
            HashMap::from([
                ("uniswap_v2".to_string(), Arc::new(Mutex::new(HashMap::new()))),
                ("uniswap_v3".to_string(), Arc::new(Mutex::new(HashMap::new()))),
            ]),
            mpsc::channel(4).1,
            None,
            false,
            dispatcher,
        );

        runner.run().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_build_family_dispatcher_from_cache_preseeds_component_and_contract_ownership() {
        let protocol_cache = ProtocolMemoryCache::new(
            Chain::Ethereum,
            chrono::Duration::seconds(60),
            Arc::new(MockGateway::new()),
        );
        protocol_cache
            .add_components(vec![
                ProtocolComponent::new(
                    "v2-pool",
                    "uniswap_v2",
                    "pool",
                    Chain::Ethereum,
                    vec![],
                    vec![Bytes::from(vec![0x44; 20])],
                    HashMap::new(),
                    ChangeType::Creation,
                    Bytes::default(),
                    NaiveDateTime::default(),
                ),
                ProtocolComponent::new(
                    "v3-pool",
                    "uniswap_v3",
                    "pool",
                    Chain::Ethereum,
                    vec![],
                    vec![Bytes::from(vec![0x55; 20])],
                    HashMap::new(),
                    ChangeType::Creation,
                    Bytes::default(),
                    NaiveDateTime::default(),
                ),
            ])
            .await
            .expect("seed protocol cache");

        let branch_specs = vec![
            FamilyBranchSpec {
                protocol_system: "uniswap_v2".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
            },
            FamilyBranchSpec {
                protocol_system: "uniswap_v3".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
            },
        ];
        let mut dispatcher = build_family_dispatcher_from_cache(
            &branch_specs,
            &protocol_cache,
        )
        .await
        .expect("dispatcher builds from cache");

        let dispatched = dispatcher
            .dispatch_block_scoped_data(make_family_contract_and_storage_follow_up_block_scoped_data(
                44,
                "cursor-44",
            ))
            .expect("contract/storage follow-up routes from cache preload");

        let v2 = dispatched
            .get("uniswap_v2")
            .expect("v2 branch present");
        let v2_changes = substreams::BlockChanges::decode(
            v2.output
                .as_ref()
                .and_then(|output| output.map_output.as_ref())
                .expect("v2 map output")
                .value
                .as_slice(),
        )
        .expect("decode v2 block changes");
        assert_eq!(v2_changes.changes.len(), 1);
        assert_eq!(v2_changes.storage_changes.len(), 0);
        assert_eq!(v2_changes.changes[0].contract_changes.len(), 1);
        assert_eq!(v2_changes.changes[0].contract_changes[0].address, vec![0x44; 20]);

        let v3 = dispatched
            .get("uniswap_v3")
            .expect("v3 branch present");
        let v3_changes = substreams::BlockChanges::decode(
            v3.output
                .as_ref()
                .and_then(|output| output.map_output.as_ref())
                .expect("v3 map output")
                .value
                .as_slice(),
        )
        .expect("decode v3 block changes");
        assert_eq!(v3_changes.changes.len(), 0);
        assert_eq!(v3_changes.storage_changes.len(), 1);
        assert_eq!(v3_changes.storage_changes[0].storage_changes.len(), 1);
        assert_eq!(
            v3_changes.storage_changes[0].storage_changes[0].address,
            vec![0x55; 20]
        );
    }

    #[tokio::test]
    async fn test_build_family_dispatcher_from_populated_cache_uses_gateway_seeded_components() {
        let chain = Chain::Ethereum;
        let mut gateway = MockGateway::new();
        gateway
            .expect_get_tokens()
            .return_once(move |_, _, _, _, _| {
                let token = Token::new(
                    &Bytes::from(vec![0xaa; 20]),
                    "TKN",
                    18,
                    0,
                    &[],
                    chain,
                    100,
                );
                Box::pin(async move {
                    Ok(WithTotal {
                        entity: vec![token],
                        total: Some(1),
                    })
                })
            });
        gateway
            .expect_get_protocol_components()
            .return_once(|_, _, _, _, _| {
                Box::pin(async move {
                    Ok(WithTotal {
                        entity: vec![
                            ProtocolComponent::new(
                                "v2-pool",
                                "uniswap_v2",
                                "pool",
                                Chain::Ethereum,
                                vec![],
                                vec![Bytes::from(vec![0x44; 20])],
                                HashMap::new(),
                                ChangeType::Creation,
                                Bytes::default(),
                                NaiveDateTime::default(),
                            ),
                            ProtocolComponent::new(
                                "v3-pool",
                                "uniswap_v3",
                                "pool",
                                Chain::Ethereum,
                                vec![],
                                vec![Bytes::from(vec![0x55; 20])],
                                HashMap::new(),
                                ChangeType::Creation,
                                Bytes::default(),
                                NaiveDateTime::default(),
                            ),
                        ],
                        total: Some(2),
                    })
                })
            });
        gateway
            .expect_get_token_prices()
            .with(mockall::predicate::eq(chain))
            .times(1)
            .return_once(|_| Box::pin(async { Ok(HashMap::new()) }));

        let protocol_cache = ProtocolMemoryCache::new(
            chain,
            chrono::Duration::seconds(60),
            Arc::new(gateway),
        );
        protocol_cache
            .populate()
            .await
            .expect("populate protocol cache from gateway");

        let branch_specs = vec![
            FamilyBranchSpec {
                protocol_system: "uniswap_v2".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v2_pool".to_string()]),
            },
            FamilyBranchSpec {
                protocol_system: "uniswap_v3".to_string(),
                protocol_type_names: HashSet::from(["uniswap_v3_pool".to_string()]),
            },
        ];
        let mut dispatcher = build_family_dispatcher_from_cache(
            &branch_specs,
            &protocol_cache,
        )
        .await
        .expect("dispatcher builds from populated cache");

        let dispatched = dispatcher
            .dispatch_block_scoped_data(make_family_contract_and_storage_follow_up_block_scoped_data(
                44,
                "cursor-44",
            ))
            .expect("dispatch follow-up block after populated-cache preseed");

        assert!(dispatched.contains_key("uniswap_v2"));
        assert!(dispatched.contains_key("uniswap_v3"));
    }

    #[tokio::test]
    async fn test_resolve_family_stream_start_uses_next_aligned_resume_block() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_last_processed_block()
            .once()
            .returning(|| Some(Block { number: 100, ..Default::default() }));
        let mut v3 = MockExtractor::new();
        v3.expect_get_last_processed_block()
            .once()
            .returning(|| Some(Block { number: 100, ..Default::default() }));

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let configs = [ExtractorConfig::default(), ExtractorConfig::default()];
        let config_refs = configs.iter().collect::<Vec<_>>();

        let start = resolve_family_stream_start(&extractors, &config_refs)
            .await
            .expect("aligned progress should resolve");

        assert_eq!(start, 101);
    }

    #[tokio::test]
    async fn test_resolve_family_stream_start_rejects_misaligned_resume_blocks() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_last_processed_block()
            .once()
            .returning(|| Some(Block { number: 100, ..Default::default() }));
        let mut v3 = MockExtractor::new();
        v3.expect_get_last_processed_block()
            .once()
            .returning(|| Some(Block { number: 101, ..Default::default() }));

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let configs = [ExtractorConfig::default(), ExtractorConfig::default()];
        let config_refs = configs.iter().collect::<Vec<_>>();

        let err = resolve_family_stream_start(&extractors, &config_refs)
            .await
            .expect_err("misaligned progress should fail");

        assert!(err
            .to_string()
            .contains("family runner requires aligned branch progress"));
    }

    #[tokio::test]
    async fn test_resolve_family_stream_start_rejects_mixed_resumed_and_fresh_branches() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_last_processed_block()
            .once()
            .returning(|| Some(Block { number: 100, ..Default::default() }));
        let mut v3 = MockExtractor::new();
        v3.expect_get_last_processed_block()
            .once()
            .returning(|| None);

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let configs = [ExtractorConfig::default(), ExtractorConfig::default()];
        let config_refs = configs.iter().collect::<Vec<_>>();

        let err = resolve_family_stream_start(&extractors, &config_refs)
            .await
            .expect_err("mixed branch progress should fail");

        assert!(err
            .to_string()
            .contains("family runner requires consistent branch progress"));
    }

    #[tokio::test]
    async fn test_resolve_family_stream_start_uses_bootstrap_adjusted_aligned_fresh_start() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_last_processed_block()
            .times(2)
            .returning(|| None);
        let mut v3 = MockExtractor::new();
        v3.expect_get_last_processed_block()
            .times(2)
            .returning(|| None);

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let configs = [
            ExtractorConfig {
                name: "uniswap_v2".to_owned(),
                protocol_system: "uniswap_v2".to_string(),
                start_block: 42,
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                            .to_owned(),
                }),
                ..Default::default()
            },
            ExtractorConfig {
                name: "uniswap_v3".to_owned(),
                protocol_system: "uniswap_v3".to_string(),
                start_block: 42,
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678"
                            .to_owned(),
                }),
                ..Default::default()
            },
        ];
        let config_refs = configs.iter().collect::<Vec<_>>();

        let start = resolve_family_stream_start(&extractors, &config_refs)
            .await
            .expect("aligned fresh bootstrap branches should resolve");

        assert_eq!(start, 43);
    }

    #[tokio::test]
    async fn test_resolve_family_stream_start_rejects_misaligned_fresh_branch_starts() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_last_processed_block()
            .once()
            .returning(|| None);
        let mut v3 = MockExtractor::new();
        v3.expect_get_last_processed_block()
            .once()
            .returning(|| None);

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let configs = [
            ExtractorConfig {
                name: "uniswap_v2".to_owned(),
                protocol_system: "uniswap_v2".to_string(),
                start_block: 42,
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                            .to_owned(),
                }),
                ..Default::default()
            },
            ExtractorConfig {
                name: "uniswap_v3".to_owned(),
                protocol_system: "uniswap_v3".to_string(),
                start_block: 45,
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 45,
                    params:
                        "bootstrap_block=45&pool=0x0000000000000000000000000000000000005678"
                            .to_owned(),
                }),
                ..Default::default()
            },
        ];
        let config_refs = configs.iter().collect::<Vec<_>>();

        let err = resolve_family_stream_start(&extractors, &config_refs)
            .await
            .expect_err("misaligned fresh family starts should fail");

        assert!(err
            .to_string()
            .contains("family runner requires aligned branch start blocks"));
    }

    #[tokio::test]
    async fn test_run_family_bootstrap_if_needed_rejects_mixed_progress_before_bootstrap() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_last_processed_block()
            .once()
            .returning(|| Some(Block { number: 100, ..Default::default() }));
        let mut v3 = MockExtractor::new();
        v3.expect_get_last_processed_block()
            .once()
            .returning(|| None);

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let configs = [
            ExtractorConfig {
                name: "uniswap_v2".to_owned(),
                protocol_system: "uniswap_v2".to_string(),
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                            .to_owned(),
                }),
                ..Default::default()
            },
            ExtractorConfig {
                name: "uniswap_v3".to_owned(),
                protocol_system: "uniswap_v3".to_string(),
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000005678"
                            .to_owned(),
                }),
                ..Default::default()
            },
        ];
        let config_refs = configs.iter().collect::<Vec<_>>();
        let rpc_client = EthereumRpcClient::new("http://localhost:8545")
            .expect("rpc client builds for non-networked preflight");

        let err = run_family_bootstrap_if_needed(&extractors, &config_refs, &rpc_client)
            .await
            .expect_err("mixed progress should fail before bootstrap materialization");

        assert!(err
            .to_string()
            .contains("family runner requires consistent branch progress before bootstrap"));
    }

    #[tokio::test]
    async fn test_run_family_bootstrap_if_needed_rejects_partial_shared_bootstrap_config() {
        let mut v2 = MockExtractor::new();
        v2.expect_get_last_processed_block()
            .times(2)
            .returning(|| None);
        let mut v3 = MockExtractor::new();
        v3.expect_get_last_processed_block()
            .times(2)
            .returning(|| None);

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);
        let configs = [
            ExtractorConfig {
                name: "uniswap_v2".to_owned(),
                protocol_system: "uniswap_v2".to_string(),
                bootstrap: Some(BootstrapConfig {
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    start_block: 42,
                    params:
                        "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                            .to_owned(),
                }),
                ..Default::default()
            },
            ExtractorConfig {
                name: "uniswap_v3".to_owned(),
                protocol_system: "uniswap_v3".to_string(),
                bootstrap: None,
                ..Default::default()
            },
        ];
        let config_refs = configs.iter().collect::<Vec<_>>();
        let rpc_client = EthereumRpcClient::new("http://localhost:8545")
            .expect("rpc client builds for non-networked preflight");

        let err = run_family_bootstrap_if_needed(&extractors, &config_refs, &rpc_client)
            .await
            .expect_err("partial shared bootstrap config should fail");

        assert!(err
            .to_string()
            .contains("family runner requires shared bootstrap configuration consistency across fresh branches"));
    }

    #[test]
    fn test_family_runner_context_derives_shared_branch_and_stream_settings() {
        let v2 = ExtractorConfig {
            name: "uniswap_v2".to_owned(),
            protocol_system: "uniswap_v2".to_string(),
            stop_block: Some(120),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v2_pool".to_string(),
                FinancialType::Swap,
            )],
            substreams_params: HashMap::from([(
                "map_pool_events".to_string(),
                "factory=0x01".to_string(),
            )]),
            ..Default::default()
        };
        let v3 = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            protocol_system: "uniswap_v3".to_string(),
            stop_block: Some(120),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            substreams_params: HashMap::from([(
                "map_events".to_string(),
                "factory=0x02".to_string(),
            )]),
            ..Default::default()
        };

        let context = FamilyRunnerContext::from_extractor_configs(&[&v2, &v3])
            .expect("family context derives");

        assert_eq!(context.stop_block, 120);
        assert_eq!(
            FamilyBranchSpec::protocol_system_set(context.branch_specs.iter()),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );
        assert_eq!(context.branch_specs.len(), 2);
        assert_eq!(
            context.merged_substreams_params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
    }

    #[test]
    fn test_family_runner_context_derives_from_resolved_family_runtime() {
        let v2 = ExtractorConfig {
            name: "uniswap_v2_primary".to_owned(),
            protocol_system: "uniswap_v2".to_string(),
            stop_block: Some(120),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v2_pool".to_string(),
                FinancialType::Swap,
            )],
            substreams_params: HashMap::from([(
                "map_pool_events".to_string(),
                "factory=0x01".to_string(),
            )]),
            ..Default::default()
        };
        let v3 = ExtractorConfig {
            name: "uniswap_v3_primary".to_owned(),
            protocol_system: "uniswap_v3".to_string(),
            stop_block: Some(120),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            substreams_params: HashMap::from([(
                "map_events".to_string(),
                "factory=0x02".to_string(),
            )]),
            ..Default::default()
        };
        let resolved_family = ResolvedFamilyRuntime {
            family: DetectedFamilyRuntime {
                family_name: "uniswap".to_string(),
                chain: tycho_common::models::Chain::Ethereum,
                member_protocol_systems: vec![
                    "uniswap_v2".to_string(),
                    "uniswap_v3".to_string(),
                ],
                shared_spkg: "/tmp/uniswap-family.spkg".to_string(),
                output_module: "map_uniswap_family_protocol_changes".to_string(),
            },
            extractor_configs: vec![&v2, &v3],
        };

        let context = FamilyRunnerContext::from_resolved_family(&resolved_family)
            .expect("family context derives from resolved family runtime");

        assert_eq!(context.stop_block, 120);
        assert_eq!(
            FamilyBranchSpec::protocol_system_set(context.branch_specs.iter()),
            HashSet::from(["uniswap_v2".to_string(), "uniswap_v3".to_string()])
        );
        assert_eq!(context.branch_specs.len(), 2);
        assert_eq!(
            context.merged_substreams_params,
            HashMap::from([
                ("map_pool_events".to_string(), "factory=0x01".to_string()),
                ("map_events".to_string(), "factory=0x02".to_string()),
            ])
        );
    }

    #[test]
    fn test_family_runner_context_rejects_conflicting_stop_blocks() {
        let v2 = ExtractorConfig {
            name: "uniswap_v2".to_owned(),
            protocol_system: "uniswap_v2".to_string(),
            stop_block: Some(110),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v2_pool".to_string(),
                FinancialType::Swap,
            )],
            ..Default::default()
        };
        let v3 = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            protocol_system: "uniswap_v3".to_string(),
            stop_block: Some(120),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            ..Default::default()
        };

        let err = FamilyRunnerContext::from_extractor_configs(&[&v2, &v3])
            .expect_err("conflicting stop blocks should fail");

        assert!(err
            .to_string()
            .contains("family runner requires one shared stop_block"));
    }

    #[test]
    fn test_family_runner_context_rejects_conflicting_substreams_params() {
        let v2 = ExtractorConfig {
            name: "uniswap_v2".to_owned(),
            protocol_system: "uniswap_v2".to_string(),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v2_pool".to_string(),
                FinancialType::Swap,
            )],
            substreams_params: HashMap::from([(
                "map_pool_events".to_string(),
                "factory=0x01".to_string(),
            )]),
            ..Default::default()
        };
        let v3 = ExtractorConfig {
            name: "uniswap_v3".to_owned(),
            protocol_system: "uniswap_v3".to_string(),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            substreams_params: HashMap::from([(
                "map_pool_events".to_string(),
                "factory=0x02".to_string(),
            )]),
            ..Default::default()
        };

        let err = FamilyRunnerContext::from_extractor_configs(&[&v2, &v3])
            .expect_err("conflicting family params should fail");

        assert!(err
            .to_string()
            .contains("conflicting substreams param `map_pool_events`"));
    }

    #[test]
    fn test_validate_family_progress_consistency_allows_all_resumed_or_all_fresh() {
        validate_family_progress_consistency(
            &[("uniswap_v2".to_string(), 100), ("uniswap_v3".to_string(), 100)],
            &[],
            "before stream start",
        )
        .expect("all resumed should be allowed");

        validate_family_progress_consistency(
            &[],
            &["uniswap_v2".to_string(), "uniswap_v3".to_string()],
            "before bootstrap",
        )
        .expect("all fresh should be allowed");
    }

    #[test]
    fn test_validate_family_progress_consistency_rejects_mixed_progress() {
        let err = validate_family_progress_consistency(
            &[("uniswap_v2".to_string(), 100)],
            &["uniswap_v3".to_string()],
            "before stream start",
        )
        .expect_err("mixed progress should fail");

        assert!(err
            .to_string()
            .contains("family runner requires consistent branch progress before stream start"));
    }

    #[test]
    fn test_validate_family_runner_membership_accepts_exact_member_set() {
        let family = DetectedFamilyRuntime {
            family_name: "uniswap".to_string(),
            chain: Chain::Ethereum,
            member_protocol_systems: vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()],
            shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                .to_string(),
            output_module: "map_uniswap_family_protocol_changes".to_string(),
        };
        let v2 = ExtractorConfig {
            name: "uniswap_v2".to_string(),
            chain: Chain::Ethereum,
            protocol_system: "uniswap_v2".to_string(),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v2_pool".to_string(),
                FinancialType::Swap,
            )],
            ..Default::default()
        };
        let v3 = ExtractorConfig {
            name: "uniswap_v3".to_string(),
            chain: Chain::Ethereum,
            protocol_system: "uniswap_v3".to_string(),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v3_pool".to_string(),
                FinancialType::Swap,
            )],
            ..Default::default()
        };

        validate_family_runner_membership(&family, &[&v2, &v3])
            .expect("exact family members should be accepted");
    }

    #[test]
    fn test_validate_family_runner_membership_rejects_missing_or_extra_members() {
        let family = DetectedFamilyRuntime {
            family_name: "uniswap".to_string(),
            chain: Chain::Ethereum,
            member_protocol_systems: vec!["uniswap_v2".to_string(), "uniswap_v3".to_string()],
            shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                .to_string(),
            output_module: "map_uniswap_family_protocol_changes".to_string(),
        };
        let only_v2 = ExtractorConfig {
            name: "uniswap_v2".to_string(),
            chain: Chain::Ethereum,
            protocol_system: "uniswap_v2".to_string(),
            protocol_types: vec![ProtocolTypeConfig::new(
                "uniswap_v2_pool".to_string(),
                FinancialType::Swap,
            )],
            ..Default::default()
        };
        let curve = ExtractorConfig {
            name: "curve".to_string(),
            chain: Chain::Ethereum,
            protocol_system: "curve".to_string(),
            protocol_types: vec![ProtocolTypeConfig::new(
                "curve_pool".to_string(),
                FinancialType::Swap,
            )],
            ..Default::default()
        };

        let missing_err = validate_family_runner_membership(&family, &[&only_v2])
            .expect_err("missing member should fail");
        assert!(missing_err
            .to_string()
            .contains("requires exact member protocol systems"));

        let extra_err = validate_family_runner_membership(&family, &[&only_v2, &curve])
            .expect_err("extra non-family member should fail");
        assert!(extra_err
            .to_string()
            .contains("requires exact member protocol systems"));
    }

    #[test]
    fn test_validate_family_runner_membership_rejects_chain_mismatch() {
        let family = DetectedFamilyRuntime {
            family_name: "uniswap".to_string(),
            chain: Chain::Ethereum,
            member_protocol_systems: vec!["uniswap_v2".to_string()],
            shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                .to_string(),
            output_module: "map_uniswap_family_protocol_changes".to_string(),
        };
        let base_v2 = ExtractorConfig {
            name: "base_v2".to_string(),
            protocol_system: "uniswap_v2".to_string(),
            chain: Chain::Base,
            ..Default::default()
        };

        let err = validate_family_runner_membership(&family, &[&base_v2])
            .expect_err("chain mismatch should fail");
        assert!(err
            .to_string()
            .contains("requires chain `ethereum`, but extractor `base_v2` uses `base`"));
    }

    #[test]
    fn test_validate_family_runner_membership_rejects_explicit_family_mismatch() {
        let family = DetectedFamilyRuntime {
            family_name: "uniswap".to_string(),
            chain: Chain::Ethereum,
            member_protocol_systems: vec!["uniswap_v2".to_string()],
            shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                .to_string(),
            output_module: "map_uniswap_family_protocol_changes".to_string(),
        };
        let wrong_family_v2 = ExtractorConfig {
            name: "wrong_family_v2".to_string(),
            protocol_system: "uniswap_v2".to_string(),
            family_runtime: Some(FamilyRuntimeConfig {
                family: "future_swap".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = validate_family_runner_membership(&family, &[&wrong_family_v2])
            .expect_err("explicit family mismatch should fail");
        assert!(err
            .to_string()
            .contains("cannot include extractor `wrong_family_v2` declared for family `future_swap`"));
    }

    #[test]
    fn test_validate_family_runner_membership_rejects_missing_protocol_types() {
        let family = DetectedFamilyRuntime {
            family_name: "uniswap".to_string(),
            chain: Chain::Ethereum,
            member_protocol_systems: vec!["uniswap_v2".to_string()],
            shared_spkg: "protocols/substreams/ethereum-uniswap-v2-v3-combined/test.spkg"
                .to_string(),
            output_module: "map_uniswap_family_protocol_changes".to_string(),
        };
        let typeless_v2 = ExtractorConfig {
            name: "typeless_v2".to_string(),
            protocol_system: "uniswap_v2".to_string(),
            protocol_types: vec![],
            ..Default::default()
        };

        let err = validate_family_runner_membership(&family, &[&typeless_v2])
            .expect_err("missing protocol types should fail");
        assert!(err
            .to_string()
            .contains("requires extractor `typeless_v2` to declare at least one protocol type for branch routing"));
    }

    #[tokio::test]
    async fn test_apply_family_bootstrap_plan_splits_once_and_updates_each_branch() {
        let plan = SharedBootstrapPlan {
            family_name: Some("uniswap".to_string()),
            bootstrap_block: 42,
            branches: vec![
                crate::extractor::shared_bootstrap::BootstrapBranchDescriptor {
                    extractor_name: "uniswap_v2".to_string(),
                    protocol_system: "uniswap_v2".to_string(),
                    chain: Chain::Ethereum,
                    strategy: BootstrapStrategy::UniswapV2Rpc,
                    params: crate::extractor::shared_bootstrap::SharedBootstrapParams {
                        bootstrap_block: 42,
                        pools: vec![],
                    },
                },
                crate::extractor::shared_bootstrap::BootstrapBranchDescriptor {
                    extractor_name: "uniswap_v3".to_string(),
                    protocol_system: "uniswap_v3".to_string(),
                    chain: Chain::Ethereum,
                    strategy: BootstrapStrategy::UniswapV3Rpc,
                    params: crate::extractor::shared_bootstrap::SharedBootstrapParams {
                        bootstrap_block: 42,
                        pools: vec![],
                    },
                },
            ],
        };

        let block = Block {
            number: 42,
            hash: Bytes::from(vec![0x01; 32]),
            parent_hash: Bytes::from(vec![0x02; 32]),
            chain: Chain::Ethereum,
            ts: chrono::NaiveDateTime::default(),
        };
        let tx = tycho_common::models::blockchain::Transaction {
            hash: Bytes::from(vec![0xaa; 32]),
            block_hash: block.hash.clone(),
            from: Bytes::from(vec![0x11; 20]),
            to: None,
            index: 0,
        };
        let merged_changes = crate::extractor::models::BlockChanges::new(
            "uniswap_family".to_string(),
            Chain::Ethereum,
            block.clone(),
            42,
            false,
            vec![tycho_common::models::blockchain::TxWithChanges {
                tx: tx.clone(),
                protocol_components: HashMap::from([
                    (
                        "v2-pool".to_string(),
                        tycho_common::models::protocol::ProtocolComponent {
                            id: "v2-pool".to_string(),
                            protocol_system: "uniswap_v2".to_string(),
                            ..Default::default()
                        },
                    ),
                    (
                        "v3-pool".to_string(),
                        tycho_common::models::protocol::ProtocolComponent {
                            id: "v3-pool".to_string(),
                            protocol_system: "uniswap_v3".to_string(),
                            ..Default::default()
                        },
                    ),
                ]),
                ..Default::default()
            }],
            vec![],
        );

        let mut v2 = MockExtractor::new();
        v2.expect_get_completed_bootstrap_block()
            .once()
            .returning(|| Ok(None));
        v2.expect_handle_block_changes()
            .once()
            .returning(|changes, cursor| {
                assert_eq!(cursor, "bootstrap@42");
                assert_eq!(changes.txs_with_update.len(), 1);
                assert_eq!(
                    changes.txs_with_update[0]
                        .protocol_components
                        .len(),
                    1
                );
                assert!(changes.txs_with_update[0]
                    .protocol_components
                    .contains_key("v2-pool"));
                Ok(None)
            });
        v2.expect_flush()
            .once()
            .returning(|| Ok(()));
        v2.expect_mark_bootstrap_completed()
            .once()
            .returning(|bootstrap_block, block_hash| {
                assert_eq!(bootstrap_block, 42);
                assert_eq!(block_hash, Bytes::from(vec![0x01; 32]));
                Ok(())
            });

        let mut v3 = MockExtractor::new();
        v3.expect_handle_block_changes()
            .once()
            .returning(|changes, cursor| {
                assert_eq!(cursor, "bootstrap@42");
                assert_eq!(changes.txs_with_update.len(), 1);
                assert_eq!(
                    changes.txs_with_update[0]
                        .protocol_components
                        .len(),
                    1
                );
                assert!(changes.txs_with_update[0]
                    .protocol_components
                    .contains_key("v3-pool"));
                Ok(None)
            });
        v3.expect_flush()
            .once()
            .returning(|| Ok(()));

        let extractors: HashMap<String, Arc<dyn Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn Extractor>),
        ]);

        apply_family_bootstrap_plan(&extractors, &plan, merged_changes)
            .await
            .expect("shared family bootstrap should apply");
    }

    #[tokio::test]
    async fn test_apply_family_bootstrap_plan_skips_completed_family() {
        let plan = SharedBootstrapPlan {
            family_name: Some("uniswap".to_string()),
            bootstrap_block: 42,
            branches: vec![crate::extractor::shared_bootstrap::BootstrapBranchDescriptor {
                extractor_name: "uniswap_v2".to_string(),
                protocol_system: "uniswap_v2".to_string(),
                chain: Chain::Ethereum,
                strategy: BootstrapStrategy::UniswapV2Rpc,
                params: crate::extractor::shared_bootstrap::SharedBootstrapParams {
                    bootstrap_block: 42,
                    pools: vec![],
                },
            }],
        };
        let block = Block {
            number: 42,
            hash: Bytes::from(vec![0x01; 32]),
            parent_hash: Bytes::from(vec![0x02; 32]),
            chain: Chain::Ethereum,
            ts: chrono::NaiveDateTime::default(),
        };
        let merged_changes = crate::extractor::models::BlockChanges::new(
            "uniswap_family".to_string(),
            Chain::Ethereum,
            block,
            42,
            false,
            vec![tycho_common::models::blockchain::TxWithChanges {
                tx: tycho_common::models::blockchain::Transaction {
                    hash: Bytes::from(vec![0xaa; 32]),
                    block_hash: Bytes::from(vec![0x01; 32]),
                    from: Bytes::from(vec![0x11; 20]),
                    to: None,
                    index: 0,
                },
                protocol_components: HashMap::from([(
                    "v2-pool".to_string(),
                    tycho_common::models::protocol::ProtocolComponent {
                        id: "v2-pool".to_string(),
                        protocol_system: "uniswap_v2".to_string(),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }],
            vec![],
        );

        let mut v2 = MockExtractor::new();
        v2.expect_get_completed_bootstrap_block()
            .once()
            .returning(|| Ok(Some(42)));

        let extractors: HashMap<String, Arc<dyn Extractor>> =
            HashMap::from([("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn Extractor>)]);

        apply_family_bootstrap_plan(&extractors, &plan, merged_changes)
            .await
            .expect("completed family bootstrap should be skipped cleanly");
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
