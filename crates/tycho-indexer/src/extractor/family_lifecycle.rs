use std::{collections::HashMap, sync::Arc};

use tycho_ethereum::rpc::EthereumRpcClient;

#[cfg(test)]
use crate::extractor::shared_bootstrap::{
    commit_materialized_bootstrap, resolve_materialized_bootstrap_commit_targets,
};
use crate::extractor::{
    bootstrap_lifecycle::{
        decide_bootstrap_run, execute_bootstrap_run_decision_with_progress_reload,
        BootstrapRunDecision,
    },
    collect_shared_bootstrap_completion_snapshot, collect_shared_resume_progress,
    family_runtime_planning::ResolvedFamilyRuntime, load_extractor_progress_snapshot,
    load_named_extractor_progress_snapshots,
    managed_substreams_request::{
        resolved_stream_position_from_resume, FamilyPreparedRequestContext, ResolvedStreamPosition,
    },
    shared_bootstrap::{
        execute_materialized_bootstrap_plan, BootstrapCompletionPolicy,
        BootstrapCompletionSnapshot,
    },
    resolve_shared_resume_state, validate_shared_progress_consistency,
    validate_named_progress_scope, ExtractionError, Extractor,
    NamedExtractorProgressSnapshot, PersistedExtractorStateKind, PersistedSharedCursorState,
};

#[cfg(test)]
use crate::extractor::{
    load_extractor_bootstrap_completion_snapshot,
    shared_bootstrap_already_completed_from_named_progress,
};

#[cfg(test)]
use crate::extractor::{
    bootstrap_lifecycle::execute_bootstrap_run_decision,
    family_runner_wiring::FamilyBootstrapCommitWiring, shared_bootstrap::SharedBootstrapPlan,
};

#[cfg(test)]
fn family_branch_extractor<'a>(
    extractors: &'a HashMap<String, Arc<dyn Extractor>>,
    protocol_system: &str,
) -> Option<&'a Arc<dyn Extractor>> {
    extractors.get(protocol_system)
}

pub(crate) type ResolvedFamilyStreamPosition = ResolvedStreamPosition;

struct FamilyLifecycleProgress {
    progress: Vec<NamedExtractorProgressSnapshot>,
    resume_blocks: Vec<(String, u64)>,
    missing_progress: Vec<String>,
    completion_snapshot: BootstrapCompletionSnapshot,
}

async fn load_family_progress_snapshot(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
) -> Result<Vec<NamedExtractorProgressSnapshot>, ExtractionError> {
    load_named_extractor_progress_snapshots(extractors, |extractor| {
        Box::pin(load_extractor_progress_snapshot(extractor))
    })
    .await
}

#[cfg(test)]
async fn load_family_bootstrap_completion_snapshot(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
) -> Result<Vec<NamedExtractorProgressSnapshot>, ExtractionError> {
    load_named_extractor_progress_snapshots(
        extractors,
        |extractor| Box::pin(load_extractor_bootstrap_completion_snapshot(extractor)),
    )
    .await
}

impl FamilyLifecycleProgress {
    async fn load(
        extractors: &HashMap<String, Arc<dyn Extractor>>,
    ) -> Result<Self, ExtractionError> {
        let progress = load_family_progress_snapshot(extractors).await?;
        let (resume_blocks, missing_progress) = collect_shared_resume_progress(
            progress
                .iter()
                .map(|branch| (branch.extractor_id.as_str(), &branch.progress)),
        );
        let completion_snapshot = collect_shared_bootstrap_completion_snapshot(
            progress
                .iter()
                .map(|branch| (branch.extractor_id.as_str(), &branch.progress)),
        );

        Ok(Self {
            progress,
            resume_blocks,
            missing_progress,
            completion_snapshot,
        })
    }

    fn validate_cursor_scope(
        &self,
        durability_scope: &str,
    ) -> Result<(), ExtractionError> {
        validate_named_progress_scope(
            "family runner",
            durability_scope,
            &self.progress,
            PersistedExtractorStateKind::Cursor,
        )
    }

    fn validate_bootstrap_scope(
        &self,
        durability_scope: &str,
    ) -> Result<(), ExtractionError> {
        validate_named_progress_scope(
            "family runner",
            durability_scope,
            &self.progress,
            PersistedExtractorStateKind::CompletedBootstrap,
        )
    }

    fn validate_progress_consistency(
        &self,
        context: &str,
    ) -> Result<(), ExtractionError> {
        validate_shared_progress_consistency(
            "family runner",
            &self.resume_blocks,
            &self.missing_progress,
            context,
        )
        .map(|_| ())
    }

    fn has_resume_progress(&self) -> bool {
        !self.resume_blocks.is_empty()
    }

    fn decide_bootstrap_run(
        &self,
        bootstrap_block: u64,
    ) -> Result<BootstrapRunDecision, ExtractionError> {
        decide_bootstrap_run(
            self.has_resume_progress(),
            Some(bootstrap_block),
            &self.completion_snapshot,
            "family runner",
            BootstrapCompletionPolicy::RequireConfiguredMatch,
        )
    }

    fn resolve_stream_position(
        &self,
        configured_start_block: i64,
    ) -> Result<ResolvedFamilyStreamPosition, ExtractionError> {
        let resume_state = resolve_shared_resume_state(
            "family runner",
            self.progress
                .iter()
                .map(|branch| (branch.extractor_id.as_str(), &branch.progress)),
        )?;

        match resume_state.last_processed_block {
            None => Ok(ResolvedFamilyStreamPosition {
                start_block: configured_start_block,
                cursor: None,
            }),
            Some(last_processed_block) => resolved_stream_position_from_resume(
                Some(last_processed_block),
                configured_start_block,
                match resume_state.cursor {
                    PersistedSharedCursorState::Fresh => None,
                    PersistedSharedCursorState::Stream(cursor) => Some(cursor),
                    PersistedSharedCursorState::BootstrapMarker(_) => None,
                },
            ),
        }
    }
}

impl<'a> ResolvedFamilyRuntime<'a> {
    pub(crate) async fn prepare_stream_position_after_bootstrap(
        &self,
        context: &FamilyPreparedRequestContext,
        rpc_client: &EthereumRpcClient,
    ) -> Result<ResolvedFamilyStreamPosition, ExtractionError> {
        let mut progress = FamilyLifecycleProgress::load(&context.extractors).await?;
        progress.validate_cursor_scope(&self.family.durability_scope())?;
        progress.validate_progress_consistency("before bootstrap")?;

        progress = if let Some(plan) = self.shared_bootstrap_plan() {
            progress.validate_bootstrap_scope(&self.family.durability_scope())?;
            let decision = progress.decide_bootstrap_run(plan.bootstrap_block)?;
            execute_bootstrap_run_decision_with_progress_reload(
                progress,
                decision,
                self.shared_extractor_id(),
                "family",
                || FamilyLifecycleProgress::load(&context.extractors),
                |_| async move {
                    execute_materialized_bootstrap_plan(
                        rpc_client,
                        plan,
                        self.shared_bootstrap_execution(),
                        context.bootstrap_commit_wiring.branch_targets(),
                        context.bootstrap_commit_wiring.completion_extractor(),
                    )
                    .await
                },
            )
            .await?
        } else {
            progress
        };

        progress.validate_cursor_scope(&self.family.durability_scope())?;
        progress.validate_bootstrap_scope(&self.family.durability_scope())?;
        progress.resolve_stream_position(self.configured_start_block())
    }

}

#[cfg(test)]
pub(crate) async fn run_family_bootstrap_if_needed(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    family: &ResolvedFamilyRuntime<'_>,
    rpc_client: &EthereumRpcClient,
) -> Result<(), ExtractionError> {
    let progress = FamilyLifecycleProgress::load(extractors).await?;
    progress.validate_cursor_scope(&family.family.durability_scope())?;
    progress.validate_progress_consistency("before bootstrap")?;

    let Some(plan) = family.shared_bootstrap_plan() else {
        return Ok(());
    };

    progress.validate_bootstrap_scope(&family.family.durability_scope())?;
    let decision = progress.decide_bootstrap_run(plan.bootstrap_block)?;
    execute_bootstrap_run_decision(
        decision,
        family.shared_extractor_id(),
        "family",
        |_| async move {
            let bootstrap_commit_wiring =
                FamilyBootstrapCommitWiring::from_runtime_contract(
                    &family.runtime_contract(),
                    extractors,
                )?;
            execute_materialized_bootstrap_plan(
                rpc_client,
                plan,
                family.shared_bootstrap_execution(),
                bootstrap_commit_wiring.branch_targets(),
                bootstrap_commit_wiring.completion_extractor(),
            )
            .await
        },
    )
    .await
}

#[cfg(test)]
pub(crate) async fn family_bootstrap_already_completed(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    bootstrap_block: u64,
) -> Result<bool, ExtractionError> {
    let progress = load_family_bootstrap_completion_snapshot(extractors).await?;
    shared_bootstrap_already_completed_from_named_progress(
        "family runner",
        bootstrap_block,
        &format!("bootstrap block `{bootstrap_block}`"),
        &progress,
    )
}

#[cfg(test)]
pub(crate) async fn apply_family_bootstrap_plan(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    plan: &SharedBootstrapPlan,
    merged_changes: crate::extractor::models::BlockChanges,
) -> Result<(), ExtractionError> {
    if family_bootstrap_already_completed(extractors, plan.bootstrap_block).await? {
        return Ok(());
    }

    let bootstrap_block_hash = merged_changes.block.hash.clone();
    let runtime_contract = crate::extractor::family_runtime_planning::ResolvedFamilyRuntimeContract {
        shared_extractor_id: "family::test".to_string(),
        branch_specs: plan
            .branches
            .iter()
            .map(|branch| crate::extractor::family_dispatch::FamilyBranchSpec {
                protocol_system: branch.protocol_system.clone(),
                protocol_type_names: std::collections::HashSet::new(),
            })
            .collect(),
    };
    let bootstrap_commit_wiring =
        FamilyBootstrapCommitWiring::from_runtime_contract(&runtime_contract, extractors)?;
    let commit_targets =
        resolve_materialized_bootstrap_commit_targets(
            bootstrap_commit_wiring.branch_targets(),
            merged_changes,
        )?;
    commit_materialized_bootstrap(
        commit_targets,
        bootstrap_commit_wiring.completion_extractor(),
        plan.bootstrap_block,
        bootstrap_block_hash,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn resolve_family_stream_position(
    extractors: &HashMap<String, Arc<dyn Extractor>>,
    family: &ResolvedFamilyRuntime<'_>,
) -> Result<ResolvedFamilyStreamPosition, ExtractionError> {
    let progress = FamilyLifecycleProgress::load(extractors).await?;
    progress.validate_cursor_scope(&family.family.durability_scope())?;
    progress.validate_bootstrap_scope(&family.family.durability_scope())?;
    progress.resolve_stream_position(family.configured_start_block())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::family_branch_extractor;
    use crate::extractor::MockExtractor;

    #[tokio::test]
    async fn family_branch_extractor_resolves_protocol_system_keyed_map() {
        let mut v2 = MockExtractor::new();
        v2.expect_protocol_system()
            .return_const("uniswap_v2".to_string());

        let mut v3 = MockExtractor::new();
        v3.expect_protocol_system()
            .return_const("uniswap_v3".to_string());

        let extractors: HashMap<String, Arc<dyn crate::extractor::Extractor>> = HashMap::from([
            ("uniswap_v2".to_string(), Arc::new(v2) as Arc<dyn crate::extractor::Extractor>),
            ("uniswap_v3".to_string(), Arc::new(v3) as Arc<dyn crate::extractor::Extractor>),
        ]);

        let resolved = family_branch_extractor(&extractors, "uniswap_v2")
            .expect("protocol-system keyed extractor should resolve directly");

        assert_eq!(resolved.protocol_system(), "uniswap_v2".to_string());
    }
}
