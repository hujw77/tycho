use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;
use deepsize::DeepSizeOf;
use mockall::automock;
use prost::DecodeError;
use thiserror::Error;
use tycho_common::{
    models::{
        blockchain::{Block, BlockAggregatedChanges, BlockScoped},
        contract::AccountBalance,
        protocol::ComponentBalance,
        Address, BlockHash, ExtractorIdentity, MergeError,
    },
    storage::StorageError,
    Bytes,
};

use crate::{
    extractor::{
        dynamic_contract_indexer::cache::DCICacheError,
        models::BlockChanges,
        reorg_buffer::{
            AccountStateIdType, AccountStateKeyType, AccountStateValueType,
            BufferedProtocolStateValue, ProtocolStateIdType, ProtocolStateKeyType,
            ProtocolStateValueType, StateUpdateBufferEntry,
        },
        shared_bootstrap::BootstrapCompletionSnapshot,
    },
    pb::sf::substreams::{
        rpc::v2::{BlockScopedData, BlockUndoSignal, ModulesProgress},
        v1::Clock,
    },
};

#[cfg(test)]
use crate::extractor::shared_bootstrap::{
    BootstrapCompletionDecision, BootstrapCompletionPolicy,
};

pub mod bootstrap_lifecycle;
pub mod chain_state;
pub mod control;
mod dynamic_contract_indexer;
pub mod execution_loop;
pub mod extractor_config;
pub mod extractor_lifecycle;
pub mod family_bootstrap_registry;
pub mod family_default_registry;
pub mod family_dispatch;
pub mod family_dispatch_payloads;
pub mod family_dispatch_registry;
pub mod family_dispatch_splitter;
pub mod family_lifecycle;
pub mod family_managed_startup;
pub mod family_registry;
pub mod family_runner_wiring;
pub mod family_runtime;
pub mod family_runtime_execution;
pub mod family_runtime_metadata;
pub mod family_runtime_planning;
pub mod family_uniswap;
pub mod managed_extractor_initialization;
pub mod managed_stream_startup;
pub mod managed_substreams_request;
pub mod models;
pub mod post_processors;
pub mod protobuf_deserialisation;
pub mod protocol_cache;
pub mod protocol_extractor;
pub(crate) mod protocol_message_registry;
pub mod reorg_buffer;
pub mod runner;
pub mod runtime_target_planning;
pub mod runtime_targets_startup;
pub mod shared_bootstrap;
pub mod shared_config;
pub mod single_runtime_execution;
pub mod standalone_managed_startup;
pub mod startup;
pub mod substreams_package_loader;
#[cfg(test)]
pub(crate) mod test_support;
pub mod token_analysis_cron;
mod u256_num;
pub mod uniswap_v2_bootstrap;
pub mod uniswap_v3_bootstrap;
pub mod uniswap_v3_stream;

#[derive(Error, Debug, PartialEq)]
pub enum ExtractionError {
    #[error("Extractor setup failed: {0}")]
    Setup(String),
    #[error("Failed to decode: {0}")]
    DecodeError(String),
    #[error("Protobuf error: {0}")]
    ProtobufError(#[from] DecodeError),
    #[error("Can't decode an empty message")]
    Empty,
    #[error("Unexpected extraction error: {0}")]
    Unknown(String),
    #[error("Storage failure: {0}")]
    Storage(#[from] StorageError),
    #[error("Stream errored: {0}")]
    SubstreamsError(String),
    #[error("Service error: {0}")]
    ServiceError(String),
    #[error("Merge error: {0}")]
    MergeError(#[from] MergeError),
    #[error("Reorg buffer error: {0}")]
    ReorgBufferError(String),
    #[error("Partial block buffer error: {0}")]
    PartialBlockBufferError(String),
    #[error("Tracing error: {0}")]
    TracingError(String),
    #[error("Account extraction error: {0}")]
    AccountExtractionError(String),
    #[error("DCI cache error: {0}")]
    DCICacheError(#[from] DCICacheError),
}

#[derive(Error, Debug)]
pub enum RPCError {
    #[error("RPC setup error: {0}")]
    SetupError(String),
    #[error("RPC error: {0}")]
    RequestError(String),
}

pub type ExtractorMsg = Arc<BlockAggregatedChanges>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExtractorProgressSnapshot {
    pub cursor: String,
    pub last_processed_block: Option<Block>,
    pub completed_bootstrap_block: Option<u64>,
    pub cursor_scope: PersistedExtractorStateScope,
    pub completed_bootstrap_scope: PersistedExtractorStateScope,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NamedExtractorProgressSnapshot {
    pub extractor_id: String,
    pub progress: ExtractorProgressSnapshot,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PersistedExtractorStateScope {
    #[default]
    Unknown,
    ExtractorLocal,
    SharedDurability,
    LegacyExtractorFallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedExtractorStateKind {
    Cursor,
    CompletedBootstrap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PersistedSharedCursorState {
    Fresh,
    Stream(String),
    BootstrapMarker(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedSharedResumeState {
    pub last_processed_block: Option<u64>,
    pub cursor: PersistedSharedCursorState,
}

impl PersistedExtractorStateKind {
    fn is_present(self, progress: &ExtractorProgressSnapshot) -> bool {
        match self {
            Self::Cursor => progress.last_processed_block.is_some(),
            Self::CompletedBootstrap => progress.completed_bootstrap_block.is_some(),
        }
    }

    fn scope(self, progress: &ExtractorProgressSnapshot) -> PersistedExtractorStateScope {
        match self {
            Self::Cursor => progress.cursor_scope,
            Self::CompletedBootstrap => progress.completed_bootstrap_scope,
        }
    }

    fn shared_state_label(self) -> &'static str {
        match self {
            Self::Cursor => "shared cursor state",
            Self::CompletedBootstrap => "shared bootstrap completion state",
        }
    }

    fn legacy_fallback_label(self) -> &'static str {
        match self {
            Self::Cursor => "legacy extractor-scoped fallback cursor state",
            Self::CompletedBootstrap => "legacy extractor-scoped fallback bootstrap state",
        }
    }
}

pub fn legacy_fallback_progress_ids<'a>(
    progress: impl IntoIterator<Item = (&'a str, &'a ExtractorProgressSnapshot)>,
    state_kind: PersistedExtractorStateKind,
) -> Vec<String> {
    progress
        .into_iter()
        .filter(|(_, progress)| state_kind.is_present(progress))
        .filter(|(_, progress)| {
            state_kind.scope(progress) == PersistedExtractorStateScope::LegacyExtractorFallback
        })
        .map(|(extractor_id, _)| extractor_id.to_string())
        .collect()
}

pub fn validate_no_legacy_fallback_progress_scope<'a>(
    owner_label: &str,
    durability_scope: &str,
    progress: impl IntoIterator<Item = (&'a str, &'a ExtractorProgressSnapshot)>,
    state_kind: PersistedExtractorStateKind,
) -> Result<(), ExtractionError> {
    let legacy_fallback_ids = legacy_fallback_progress_ids(progress, state_kind);
    if legacy_fallback_ids.is_empty() {
        return Ok(());
    }

    Err(ExtractionError::Setup(format!(
        "{owner_label} requires persisted {} under durability scope `{durability_scope}`, but branches {:?} resumed from {}",
        state_kind.shared_state_label(),
        legacy_fallback_ids,
        state_kind.legacy_fallback_label()
    )))
}

pub(crate) fn validate_named_progress_scope(
    owner_label: &str,
    durability_scope: &str,
    progress: &[NamedExtractorProgressSnapshot],
    state_kind: PersistedExtractorStateKind,
) -> Result<(), ExtractionError> {
    validate_no_legacy_fallback_progress_scope(
        owner_label,
        durability_scope,
        progress
            .iter()
            .map(|branch| (branch.extractor_id.as_str(), &branch.progress)),
        state_kind,
    )
}

#[cfg(test)]
pub(crate) fn shared_bootstrap_already_completed_from_named_progress(
    owner_label: &str,
    configured_bootstrap_block: u64,
    durability_scope: &str,
    progress: &[NamedExtractorProgressSnapshot],
) -> Result<bool, ExtractionError> {
    validate_named_progress_scope(
        owner_label,
        durability_scope,
        progress,
        PersistedExtractorStateKind::CompletedBootstrap,
    )?;
    let completion_snapshot = collect_shared_bootstrap_completion_snapshot(
        progress
            .iter()
            .map(|branch| (branch.extractor_id.as_str(), &branch.progress)),
    );

    Ok(matches!(
        crate::extractor::shared_bootstrap::decide_bootstrap_completion(
            &completion_snapshot,
            configured_bootstrap_block,
            owner_label,
            BootstrapCompletionPolicy::RequireConfiguredMatch,
        )?,
        BootstrapCompletionDecision::AlreadyCompleted
    ))
}

pub(crate) fn validate_shared_progress_consistency(
    owner_label: &str,
    resume_blocks: &[(String, u64)],
    missing_progress: &[String],
    context: &str,
) -> Result<Option<u64>, ExtractionError> {
    if !resume_blocks.is_empty() && !missing_progress.is_empty() {
        return Err(ExtractionError::Setup(format!(
            "{owner_label} requires consistent branch progress {context}; resumed branches: {:?}, fresh branches: {:?}",
            resume_blocks, missing_progress
        )));
    }

    let Some((_, first_block)) = resume_blocks.first() else {
        return Ok(None);
    };
    if resume_blocks
        .iter()
        .any(|(_, block_number)| block_number != first_block)
    {
        return Err(ExtractionError::Setup(format!(
            "{owner_label} requires aligned branch progress, found {:?}",
            resume_blocks
        )));
    }

    Ok(Some(*first_block))
}

pub(crate) fn collect_shared_resume_progress<'a>(
    progress: impl IntoIterator<Item = (&'a str, &'a ExtractorProgressSnapshot)>,
) -> (Vec<(String, u64)>, Vec<String>) {
    let mut resume_blocks = Vec::new();
    let mut missing_progress = Vec::new();

    for (extractor_id, progress) in progress {
        match progress
            .last_processed_block
            .as_ref()
        {
            Some(block) => resume_blocks.push((extractor_id.to_string(), block.number)),
            None => missing_progress.push(extractor_id.to_string()),
        }
    }

    (resume_blocks, missing_progress)
}

pub(crate) fn collect_shared_bootstrap_completion_snapshot<'a>(
    progress: impl IntoIterator<Item = (&'a str, &'a ExtractorProgressSnapshot)>,
) -> BootstrapCompletionSnapshot {
    let mut completed_blocks = Vec::new();
    let mut missing_completion = Vec::new();

    for (extractor_id, progress) in progress {
        match progress.completed_bootstrap_block {
            Some(block) => completed_blocks.push((extractor_id.to_string(), block)),
            None => missing_completion.push(extractor_id.to_string()),
        }
    }

    BootstrapCompletionSnapshot { completed_blocks, missing_completion }
}

fn parse_bootstrap_cursor_marker(cursor: &str) -> Option<u64> {
    cursor
        .strip_prefix("bootstrap@")
        .and_then(|block| block.parse::<u64>().ok())
}

pub(crate) fn resolve_shared_resume_state<'a>(
    owner_label: &str,
    progress: impl IntoIterator<Item = (&'a str, &'a ExtractorProgressSnapshot)>,
) -> Result<PersistedSharedResumeState, ExtractionError> {
    let progress = progress.into_iter().collect::<Vec<_>>();
    let resume_blocks = progress
        .iter()
        .filter_map(|(extractor_id, progress)| {
            progress
                .last_processed_block
                .as_ref()
                .map(|block| ((*extractor_id).to_string(), block.number))
        })
        .collect::<Vec<_>>();
    let missing_progress = progress
        .iter()
        .filter(|(_, progress)| progress.last_processed_block.is_none())
        .map(|(extractor_id, _)| (*extractor_id).to_string())
        .collect::<Vec<_>>();

    let Some(first_block) = validate_shared_progress_consistency(
        owner_label,
        &resume_blocks,
        &missing_progress,
        "before stream resume",
    )?
    else {
        return Ok(PersistedSharedResumeState {
            last_processed_block: None,
            cursor: PersistedSharedCursorState::Fresh,
        });
    };

    let mut resolved_cursor = None;
    let mut bootstrap_cursor_block = None;
    for (extractor_id, progress) in progress {
        let cursor = progress.cursor.clone();
        if cursor.is_empty() {
            return Err(ExtractionError::Setup(format!(
                "{owner_label} requires a persisted shared cursor for resumed branch `{extractor_id}`"
            )));
        }

        if let Some(marker_block) = parse_bootstrap_cursor_marker(&cursor) {
            if resolved_cursor.is_some() {
                return Err(ExtractionError::Setup(format!(
                    "{owner_label} cannot mix bootstrap-only marker cursors with persisted shared stream cursors, found bootstrap marker `{cursor}` for resumed branch `{extractor_id}`"
                )));
            }

            if let Some(existing_block) = bootstrap_cursor_block {
                if existing_block != marker_block {
                    return Err(ExtractionError::Setup(format!(
                        "{owner_label} requires aligned bootstrap-only marker cursors, found bootstrap blocks `{existing_block}` and `{marker_block}`"
                    )));
                }
            } else {
                bootstrap_cursor_block = Some(marker_block);
            }
            continue;
        }

        if bootstrap_cursor_block.is_some() {
            return Err(ExtractionError::Setup(format!(
                "{owner_label} cannot mix persisted shared stream cursors with bootstrap-only marker cursors, found stream cursor `{cursor}` for resumed branch `{extractor_id}`"
            )));
        }

        if let Some(existing) = &resolved_cursor {
            if existing != &cursor {
                return Err(ExtractionError::Setup(format!(
                    "{owner_label} requires aligned branch cursors, found `{existing}` and `{cursor}`"
                )));
            }
        } else {
            resolved_cursor = Some(cursor);
        }
    }

    let cursor = if let Some(marker_block) = bootstrap_cursor_block {
        if marker_block != first_block {
            return Err(ExtractionError::Setup(format!(
                "{owner_label} requires bootstrap-only marker cursor block `{marker_block}` to match last processed block `{first_block}`"
            )));
        }
        PersistedSharedCursorState::BootstrapMarker(marker_block)
    } else {
        PersistedSharedCursorState::Stream(
            resolved_cursor.expect("resumed shared branches should resolve one shared cursor"),
        )
    };

    Ok(PersistedSharedResumeState { last_processed_block: Some(first_block), cursor })
}

pub async fn load_extractor_progress_snapshot(
    extractor: &dyn Extractor,
) -> Result<ExtractorProgressSnapshot, ExtractionError> {
    let cursor = extractor.get_cursor().await;
    let last_processed_block = extractor
        .get_last_processed_block()
        .await;
    let completed_bootstrap_block = extractor
        .get_completed_bootstrap_block()
        .await?;
    let supports_scope = extractor.supports_persisted_state_scope();
    let cursor_scope = if supports_scope && last_processed_block.is_some() {
        extractor
            .get_cursor_state_scope()
            .await?
    } else {
        PersistedExtractorStateScope::Unknown
    };
    let completed_bootstrap_scope = if supports_scope && completed_bootstrap_block.is_some() {
        extractor
            .get_completed_bootstrap_state_scope()
            .await?
    } else {
        PersistedExtractorStateScope::Unknown
    };

    Ok(ExtractorProgressSnapshot {
        cursor,
        last_processed_block,
        completed_bootstrap_block,
        cursor_scope,
        completed_bootstrap_scope,
    })
}

#[cfg(test)]
pub(crate) async fn load_extractor_bootstrap_completion_snapshot(
    extractor: &dyn Extractor,
) -> Result<ExtractorProgressSnapshot, ExtractionError> {
    let completed_bootstrap_block = extractor
        .get_completed_bootstrap_block()
        .await?;
    let supports_scope = extractor.supports_persisted_state_scope();
    let completed_bootstrap_scope = if supports_scope && completed_bootstrap_block.is_some() {
        extractor
            .get_completed_bootstrap_state_scope()
            .await?
    } else {
        PersistedExtractorStateScope::Unknown
    };

    Ok(ExtractorProgressSnapshot {
        cursor: String::new(),
        last_processed_block: None,
        completed_bootstrap_block,
        cursor_scope: PersistedExtractorStateScope::Unknown,
        completed_bootstrap_scope,
    })
}

pub(crate) type BoxedExtractorProgressFuture<'a> = Pin<
    Box<dyn Future<Output = Result<ExtractorProgressSnapshot, ExtractionError>> + Send + 'a>,
>;

pub(crate) async fn load_named_extractor_progress_snapshots(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    load_progress: for<'a> fn(&'a dyn Extractor) -> BoxedExtractorProgressFuture<'a>,
) -> Result<Vec<NamedExtractorProgressSnapshot>, ExtractionError> {
    let mut progress = Vec::with_capacity(extractors.len());
    for (extractor_id, extractor) in extractors {
        progress.push(NamedExtractorProgressSnapshot {
            extractor_id: extractor_id.clone(),
            progress: load_progress(extractor.as_ref()).await?,
        });
    }
    Ok(progress)
}

#[automock]
#[async_trait]
pub trait Extractor: Send + Sync {
    /// Returns the unique identity of this extractor.
    fn get_id(&self) -> ExtractorIdentity;

    /// Returns the stable protocol-system identity served by this extractor.
    fn protocol_system(&self) -> String;

    /// Indicates whether this extractor can report persisted-state scope provenance precisely.
    fn supports_persisted_state_scope(&self) -> bool {
        false
    }

    /// Ensures all protocol types this extractor needs are registered in
    /// storage. Safe to call multiple times.
    ///
    /// # Errors
    /// Returns an [`ExtractionError`] if the protocol types could not be persisted.
    async fn ensure_protocol_types(&self) -> Result<(), ExtractionError>;

    /// Returns the current stream cursor, or an empty string if no block has
    /// been processed yet. At startup this reflects the last persisted cursor;
    /// during runtime it advances with every incoming block.
    async fn get_cursor(&self) -> String;

    /// Returns the last block processed by this extractor, or `None` if no
    /// block has been processed yet. At startup this reflects the last
    /// persisted block; during runtime it advances with every incoming block.
    async fn get_last_processed_block(&self) -> Option<Block>;

    /// Processes a single block-scoped data message from the source stream.
    async fn handle_tick_scoped_data(
        &self,
        inp: BlockScopedData,
    ) -> Result<Option<ExtractorMsg>, ExtractionError>;

    /// Processes a fully materialized block update produced outside the Substreams stream.
    ///
    /// This is used for local bootstrap flows that want to reuse the normal extractor, reorg
    /// buffer, aggregation, and persistence pipeline without fabricating a `BlockScopedData`.
    async fn handle_block_changes(
        &self,
        changes: BlockChanges,
        cursor: String,
    ) -> Result<Option<ExtractorMsg>, ExtractionError>;

    /// Returns the bootstrap block that has already been fully materialized and committed, if
    /// any.
    async fn get_completed_bootstrap_block(&self) -> Result<Option<u64>, ExtractionError>;

    /// Persists that bootstrap for `bootstrap_block` has completed and is durable in storage.
    async fn mark_bootstrap_completed(
        &self,
        bootstrap_block: u64,
        block_hash: tycho_common::models::BlockHash,
    ) -> Result<(), ExtractionError>;

    /// Returns the scope of the currently persisted cursor state, when known.
    async fn get_cursor_state_scope(
        &self,
    ) -> Result<PersistedExtractorStateScope, ExtractionError> {
        Ok(PersistedExtractorStateScope::Unknown)
    }

    /// Returns the scope of the currently persisted bootstrap-completion state, when known.
    async fn get_completed_bootstrap_state_scope(
        &self,
    ) -> Result<PersistedExtractorStateScope, ExtractionError> {
        Ok(PersistedExtractorStateScope::Unknown)
    }

    /// Drains the partial block buffer and processes the accumulated block as a full block.
    /// The runner calls this when it has sent the last partial for a block.
    async fn collect_and_process_full_block(
        &self,
        cursor: String,
        final_block_height: u64,
        clock: Option<Clock>,
    ) -> Result<Option<ExtractorMsg>, ExtractionError>;

    /// Forces all buffered finalized blocks to be committed to storage.
    async fn flush(&self) -> Result<(), ExtractionError>;

    /// Processes a chain reorg signal.
    async fn handle_revert(
        &self,
        inp: BlockUndoSignal,
    ) -> Result<Option<ExtractorMsg>, ExtractionError>;

    /// Processes a progress report from the source stream.
    async fn handle_progress(&self, inp: ModulesProgress) -> Result<(), ExtractionError>;
}

#[automock]
#[async_trait]
pub trait ExtractorExtension: Send + Sync {
    /// Process a block update message and update it in-place.
    async fn process_block_update(
        &mut self,
        block_changes: &mut BlockChanges,
    ) -> Result<(), ExtractionError>;

    /// Process a revert
    async fn process_revert(&mut self, target_block: &BlockHash) -> Result<(), ExtractionError>;

    /// Returns the approximate size of the internal cache used by this extension, in bytes.
    fn cache_size(&self) -> usize;

    /// Emits granular cache metrics (per-sub-cache size, key counts, top tracked contracts).
    fn emit_cache_metrics(&self, _chain: &str, _extractor: &str) {}
}

/// Wrapper to carry a cursor along with another struct.
#[derive(Clone, Debug, DeepSizeOf)]
pub(crate) struct BlockUpdateWithCursor<B: std::fmt::Debug> {
    block_update: B,
    cursor: String,
}

impl<B: std::fmt::Debug + DeepSizeOf> BlockUpdateWithCursor<B> {
    pub(crate) fn new(block_update: B, cursor: String) -> Self {
        Self { block_update, cursor }
    }

    pub(crate) fn cursor(&self) -> &String {
        &self.cursor
    }

    pub(crate) fn block_update(&self) -> &B {
        &self.block_update
    }
}

impl<B> BlockScoped for BlockUpdateWithCursor<B>
where
    B: BlockScoped + std::fmt::Debug,
{
    fn block(&self) -> Block {
        self.block_update.block()
    }
}

impl<B> StateUpdateBufferEntry for BlockUpdateWithCursor<B>
where
    B: StateUpdateBufferEntry,
{
    fn get_filtered_component_balance_update(
        &self,
        keys: Vec<(&String, &Bytes)>,
    ) -> HashMap<(String, Bytes), ComponentBalance> {
        self.block_update
            .get_filtered_component_balance_update(keys)
    }

    fn get_filtered_account_balance_update(
        &self,
        keys: Vec<(&Address, &Address)>,
    ) -> HashMap<(Address, Address), AccountBalance> {
        self.block_update
            .get_filtered_account_balance_update(keys)
    }

    fn get_filtered_protocol_state_update(
        &self,
        keys: Vec<(&ProtocolStateIdType, &ProtocolStateKeyType)>,
    ) -> HashMap<(ProtocolStateIdType, ProtocolStateKeyType), BufferedProtocolStateValue> {
        self.block_update
            .get_filtered_protocol_state_update(keys)
    }

    fn get_protocol_state_update_for_components(
        &self,
        component_ids: Vec<&ProtocolStateIdType>,
    ) -> HashMap<(ProtocolStateIdType, ProtocolStateKeyType), BufferedProtocolStateValue> {
        self.block_update
            .get_protocol_state_update_for_components(component_ids)
    }

    fn get_filtered_account_state_update(
        &self,
        keys: Vec<(&AccountStateIdType, &AccountStateKeyType)>,
    ) -> HashMap<(AccountStateIdType, AccountStateKeyType), AccountStateValueType> {
        self.block_update
            .get_filtered_account_state_update(keys)
    }
}

#[cfg(test)]
mod tests {
    use tycho_common::models::{blockchain::Block, Chain};

    use super::{
        collect_shared_bootstrap_completion_snapshot, collect_shared_resume_progress,
        validate_shared_progress_consistency, ExtractorProgressSnapshot,
        PersistedExtractorStateScope,
    };

    fn progress_snapshot(
        last_processed_block: Option<u64>,
        completed_bootstrap_block: Option<u64>,
    ) -> ExtractorProgressSnapshot {
        ExtractorProgressSnapshot {
            cursor: String::new(),
            last_processed_block: last_processed_block.map(|number| Block {
                number,
                hash: Default::default(),
                parent_hash: Default::default(),
                chain: Chain::Ethereum,
                ts: chrono::NaiveDateTime::default(),
            }),
            completed_bootstrap_block,
            cursor_scope: PersistedExtractorStateScope::Unknown,
            completed_bootstrap_scope: PersistedExtractorStateScope::Unknown,
        }
    }

    #[test]
    fn shared_progress_consistency_returns_aligned_resume_block() {
        let resume_blocks = vec![
            ("uniswap_v2".to_string(), 42),
            ("uniswap_v3".to_string(), 42),
        ];

        let resolved = validate_shared_progress_consistency(
            "family runner",
            &resume_blocks,
            &[],
            "before stream resume",
        )
        .expect("aligned progress should validate");

        assert_eq!(resolved, Some(42));
    }

    #[test]
    fn shared_progress_consistency_rejects_mixed_resumed_and_fresh_branches() {
        let resume_blocks = vec![("uniswap_v2".to_string(), 42)];
        let missing_progress = vec!["uniswap_v3".to_string()];

        let err = validate_shared_progress_consistency(
            "family runner",
            &resume_blocks,
            &missing_progress,
            "before bootstrap",
        )
        .expect_err("mixed progress should fail");

        assert!(err
            .to_string()
            .contains("family runner requires consistent branch progress before bootstrap"));
    }

    #[test]
    fn shared_progress_consistency_rejects_misaligned_resume_blocks() {
        let resume_blocks = vec![
            ("uniswap_v2".to_string(), 42),
            ("uniswap_v3".to_string(), 43),
        ];

        let err = validate_shared_progress_consistency(
            "family runner",
            &resume_blocks,
            &[],
            "before stream resume",
        )
        .expect_err("misaligned progress should fail");

        assert!(err
            .to_string()
            .contains("family runner requires aligned branch progress"));
    }

    #[test]
    fn collect_shared_resume_progress_splits_resumed_and_fresh_branches() {
        let v2 = progress_snapshot(Some(42), Some(11));
        let v3 = progress_snapshot(None, None);

        let (resume_blocks, missing_progress) = collect_shared_resume_progress([
            ("uniswap_v2", &v2),
            ("uniswap_v3", &v3),
        ]);

        assert_eq!(resume_blocks, vec![("uniswap_v2".to_string(), 42)]);
        assert_eq!(missing_progress, vec!["uniswap_v3".to_string()]);
    }

    #[test]
    fn collect_shared_bootstrap_completion_snapshot_tracks_completed_and_missing_branches() {
        let v2 = progress_snapshot(Some(42), Some(11));
        let v3 = progress_snapshot(Some(42), None);

        let snapshot = collect_shared_bootstrap_completion_snapshot([
            ("uniswap_v2", &v2),
            ("uniswap_v3", &v3),
        ]);

        assert_eq!(snapshot.completed_blocks, vec![("uniswap_v2".to_string(), 11)]);
        assert_eq!(snapshot.missing_completion, vec!["uniswap_v3".to_string()]);
    }
}
