use std::{collections::HashMap, sync::Arc};

use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    family_runtime::{
        PreparedSubstreamsRequest, ResolvedFamilyExecutionConfig, ResolvedFamilyRuntime,
        ResolvedRuntimeTarget,
    },
    models::BlockChanges,
    shared_bootstrap::{
        commit_materialized_bootstrap, decide_bootstrap_completion,
        split_plan_block_by_protocol_system, BootstrapCompletionDecision,
        BootstrapCompletionPolicy, BootstrapCompletionSnapshot, SharedBootstrapPlan,
    },
    ExtractionError, Extractor,
};

fn find_family_extractor_for_protocol_system<'a>(
    extractors: &'a HashMap<String, Arc<dyn Extractor>>,
    protocol_system: &str,
) -> Option<&'a Arc<dyn Extractor>> {
    extractors
        .values()
        .find(|extractor| extractor.protocol_system() == protocol_system)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedFamilyStreamPosition {
    pub start_block: i64,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FamilyResumeCursorState {
    Fresh,
    Stream(String),
    BootstrapMarker(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FamilyResumeState {
    last_processed_block: Option<u64>,
    cursor: FamilyResumeCursorState,
}

fn parse_bootstrap_cursor_marker(cursor: &str) -> Option<u64> {
    cursor
        .strip_prefix("bootstrap@")
        .and_then(|block| block.parse::<u64>().ok())
}

async fn resolve_family_resume_state(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
) -> Result<FamilyResumeState, ExtractionError> {
    let (resume_blocks, missing_progress) = collect_family_progress(extractors).await;

    validate_family_progress_consistency(
        &resume_blocks,
        &missing_progress,
        "before stream resume",
    )?;

    if resume_blocks.is_empty() {
        return Ok(FamilyResumeState {
            last_processed_block: None,
            cursor: FamilyResumeCursorState::Fresh,
        });
    }

    let Some((_, first_block)) = resume_blocks.first() else {
        return Err(ExtractionError::Setup("family runner has no branch progress".to_string()));
    };
    if resume_blocks
        .iter()
        .any(|(_, block_number)| block_number != first_block)
    {
        return Err(ExtractionError::Setup(format!(
            "family runner requires aligned branch progress, found {:?}",
            resume_blocks
        )));
    }

    let mut resolved_cursor = None;
    let mut bootstrap_cursor_block = None;
    for (extractor_id, extractor) in extractors {
        let cursor = extractor.get_cursor().await;
        if cursor.is_empty() {
            return Err(ExtractionError::Setup(format!(
                "family runner requires a persisted shared cursor for resumed branch `{extractor_id}`"
            )));
        }

        if let Some(marker_block) = parse_bootstrap_cursor_marker(&cursor) {
            if resolved_cursor.is_some() {
                return Err(ExtractionError::Setup(format!(
                    "family runner cannot mix bootstrap-only marker cursors with persisted shared stream cursors, found bootstrap marker `{cursor}` for resumed branch `{extractor_id}`"
                )));
            }

            if let Some(existing_block) = bootstrap_cursor_block {
                if existing_block != marker_block {
                    return Err(ExtractionError::Setup(format!(
                        "family runner requires aligned bootstrap-only marker cursors, found bootstrap blocks `{existing_block}` and `{marker_block}`"
                    )));
                }
            } else {
                bootstrap_cursor_block = Some(marker_block);
            }
            continue;
        }

        if bootstrap_cursor_block.is_some() {
            return Err(ExtractionError::Setup(format!(
                "family runner cannot mix persisted shared stream cursors with bootstrap-only marker cursors, found stream cursor `{cursor}` for resumed branch `{extractor_id}`"
            )));
        }

        if let Some(existing) = &resolved_cursor {
            if existing != &cursor {
                return Err(ExtractionError::Setup(format!(
                    "family runner requires aligned branch cursors, found `{existing}` and `{cursor}`"
                )));
            }
        } else {
            resolved_cursor = Some(cursor);
        }
    }

    let cursor = if let Some(marker_block) = bootstrap_cursor_block {
        if marker_block != *first_block {
            return Err(ExtractionError::Setup(format!(
                "family runner requires bootstrap-only marker cursor block `{marker_block}` to match last processed block `{first_block}`"
            )));
        }
        FamilyResumeCursorState::BootstrapMarker(marker_block)
    } else {
        FamilyResumeCursorState::Stream(
            resolved_cursor.expect("resumed family branches should resolve one shared cursor"),
        )
    };

    Ok(FamilyResumeState { last_processed_block: Some(*first_block), cursor })
}

pub(crate) async fn run_family_bootstrap_if_needed(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    family_execution: &ResolvedFamilyExecutionConfig,
    rpc_client: &EthereumRpcClient,
) -> Result<(), ExtractionError> {
    let (resume_blocks, missing_progress) = collect_family_progress(extractors).await;
    validate_family_progress_consistency(&resume_blocks, &missing_progress, "before bootstrap")?;

    if !resume_blocks.is_empty() {
        return Ok(());
    }

    let Some(plan) = family_execution.bootstrap_plan.as_ref() else {
        return Ok(());
    };
    if family_bootstrap_already_completed(extractors, plan.bootstrap_block).await? {
        return Ok(());
    }
    let merged_changes = materialize_family_bootstrap_block(
        rpc_client,
        plan,
        &family_execution.shared_bootstrap_execution,
    )
    .await?;
    apply_family_bootstrap_plan(extractors, plan, merged_changes).await
}

pub(crate) async fn family_bootstrap_already_completed(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    bootstrap_block: u64,
) -> Result<bool, ExtractionError> {
    let snapshot = collect_family_bootstrap_completion_snapshot(extractors).await?;
    Ok(matches!(
        decide_bootstrap_completion(
            &snapshot,
            bootstrap_block,
            "family runner",
            BootstrapCompletionPolicy::RequireConfiguredMatch,
        )?,
        BootstrapCompletionDecision::AlreadyCompleted
    ))
}

pub(crate) async fn materialize_family_bootstrap_block(
    rpc_client: &EthereumRpcClient,
    plan: &SharedBootstrapPlan,
    shared_bootstrap_execution: &crate::extractor::family_runtime::ResolvedSharedBootstrapExecution,
) -> Result<BlockChanges, ExtractionError> {
    shared_bootstrap_execution
        .materialize_plan(rpc_client, plan)
        .await
}

pub(crate) async fn collect_family_progress(
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

async fn collect_family_bootstrap_completion_snapshot(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
) -> Result<BootstrapCompletionSnapshot, ExtractionError> {
    let mut completed_blocks = Vec::new();
    let mut missing_completion = Vec::new();

    for (extractor_id, extractor) in extractors {
        match extractor
            .get_completed_bootstrap_block()
            .await?
        {
            Some(block) => completed_blocks.push((extractor_id.clone(), block)),
            None => missing_completion.push(extractor_id.clone()),
        }
    }

    Ok(BootstrapCompletionSnapshot { completed_blocks, missing_completion })
}

pub(crate) fn validate_family_progress_consistency(
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

pub(crate) async fn apply_family_bootstrap_plan(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    plan: &SharedBootstrapPlan,
    merged_changes: BlockChanges,
) -> Result<(), ExtractionError> {
    if family_bootstrap_already_completed(extractors, plan.bootstrap_block).await? {
        return Ok(());
    }

    let bootstrap_block_hash = merged_changes.block.hash.clone();
    let split_changes = split_plan_block_by_protocol_system(merged_changes)?;
    let mut branch_targets = Vec::with_capacity(plan.branches.len());
    let mut shared_state_extractor = None;

    for branch in &plan.branches {
        let extractor =
            find_family_extractor_for_protocol_system(extractors, &branch.protocol_system)
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
        branch_targets.push((extractor.clone(), changes));
        if shared_state_extractor.is_none() {
            shared_state_extractor = Some(extractor.clone());
        }
    }

    let Some(shared_state_extractor) = shared_state_extractor else {
        return Ok(());
    };
    commit_materialized_bootstrap(
        branch_targets,
        shared_state_extractor,
        plan.bootstrap_block,
        bootstrap_block_hash,
    )
    .await?;

    Ok(())
}

#[cfg(test)]
pub(crate) async fn resolve_family_stream_start(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    family_execution: &ResolvedFamilyExecutionConfig,
) -> Result<i64, ExtractionError> {
    Ok(resolve_family_stream_position(extractors, family_execution)
        .await?
        .start_block)
}

#[cfg(test)]
pub(crate) async fn resolve_family_stream_cursor(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    family_execution: &ResolvedFamilyExecutionConfig,
) -> Result<Option<String>, ExtractionError> {
    Ok(resolve_family_stream_position(extractors, family_execution)
        .await?
        .cursor)
}

pub(crate) async fn resolve_family_stream_position(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    family_execution: &ResolvedFamilyExecutionConfig,
) -> Result<ResolvedFamilyStreamPosition, ExtractionError> {
    let resume_state = resolve_family_resume_state(extractors).await?;

    match resume_state.last_processed_block {
        None => Ok(ResolvedFamilyStreamPosition {
            start_block: family_execution.configured_start_block,
            cursor: None,
        }),
        Some(last_processed_block) => {
            let next = last_processed_block
                .checked_add(1)
                .ok_or_else(|| ExtractionError::Setup("block number overflow".to_string()))?;
            let start_block = i64::try_from(next)
                .map_err(|_| ExtractionError::Setup("block number exceeds i64".to_string()))?;
            let cursor = match resume_state.cursor {
                FamilyResumeCursorState::Fresh => None,
                FamilyResumeCursorState::Stream(cursor) => Some(cursor),
                FamilyResumeCursorState::BootstrapMarker(_) => None,
            };

            Ok(ResolvedFamilyStreamPosition { start_block, cursor })
        }
    }
}

impl<'a> ResolvedFamilyRuntime<'a> {
    pub(crate) async fn prepare_substreams_request(
        &self,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
        rpc_client: &EthereumRpcClient,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        run_family_bootstrap_if_needed(extractors, &self.execution, rpc_client).await?;

        let stream_position = resolve_family_stream_position(extractors, &self.execution).await?;
        let request = ResolvedRuntimeTarget::Family(self.clone())
            .substreams_execution_request_with_start_block(stream_position.start_block)?;

        Ok(PreparedSubstreamsRequest { request, cursor: stream_position.cursor })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::find_family_extractor_for_protocol_system;
    use crate::extractor::MockExtractor;

    #[tokio::test]
    async fn find_family_extractor_for_protocol_system_ignores_map_key_aliases() {
        let mut v2 = MockExtractor::new();
        v2.expect_protocol_system()
            .return_const("uniswap_v2".to_string());

        let mut v3 = MockExtractor::new();
        v3.expect_protocol_system()
            .return_const("uniswap_v3".to_string());

        let extractors: HashMap<String, Arc<dyn crate::extractor::Extractor>> = HashMap::from([
            ("v2_alias".to_string(), Arc::new(v2) as Arc<dyn crate::extractor::Extractor>),
            ("v3_alias".to_string(), Arc::new(v3) as Arc<dyn crate::extractor::Extractor>),
        ]);

        let resolved = find_family_extractor_for_protocol_system(&extractors, "uniswap_v2")
            .expect("alias-keyed extractor should still resolve by protocol system");

        assert_eq!(resolved.protocol_system(), "uniswap_v2".to_string());
    }
}
