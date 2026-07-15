use crate::extractor::{
    shared_bootstrap::{
        decide_bootstrap_completion, BootstrapCompletionDecision, BootstrapCompletionPolicy,
        BootstrapCompletionSnapshot,
    },
    ExtractionError,
};
use std::future::Future;
use tracing::{info, warn};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BootstrapRunDecision {
    Skip,
    AlreadyCompleted { configured_bootstrap_block: u64 },
    Run { configured_bootstrap_block: u64 },
}

pub(crate) fn decide_bootstrap_run(
    has_existing_progress: bool,
    configured_bootstrap_block: Option<u64>,
    completion_snapshot: &BootstrapCompletionSnapshot,
    owner_label: &str,
    completion_policy: BootstrapCompletionPolicy,
) -> Result<BootstrapRunDecision, ExtractionError> {
    if has_existing_progress {
        return Ok(BootstrapRunDecision::Skip);
    }

    let Some(configured_bootstrap_block) = configured_bootstrap_block else {
        return Ok(BootstrapRunDecision::Skip);
    };

    Ok(
        match decide_bootstrap_completion(
            completion_snapshot,
            configured_bootstrap_block,
            owner_label,
            completion_policy,
        )? {
            BootstrapCompletionDecision::AlreadyCompleted => {
                BootstrapRunDecision::AlreadyCompleted { configured_bootstrap_block }
            }
            BootstrapCompletionDecision::NeedsBootstrap => {
                BootstrapRunDecision::Run { configured_bootstrap_block }
            }
        },
    )
}

pub(crate) fn resolve_resume_start_block(
    last_processed_block: Option<u64>,
    default_start_block: i64,
) -> Result<i64, ExtractionError> {
    last_processed_block
        .map(resolve_next_start_block)
        .transpose()?
        .map_or(Ok(default_start_block), Ok)
}

pub(crate) fn resolve_next_start_block(last_processed_block: u64) -> Result<i64, ExtractionError> {
    let next = last_processed_block
        .checked_add(1)
        .ok_or_else(|| ExtractionError::Setup("block number overflow".to_string()))?;
    i64::try_from(next).map_err(|_| ExtractionError::Setup("block number exceeds i64".to_string()))
}

pub(crate) async fn execute_bootstrap_run_decision<RunBootstrap, RunBootstrapFut>(
    decision: BootstrapRunDecision,
    startup_scope_id: &str,
    startup_scope_kind: &'static str,
    run_bootstrap: RunBootstrap,
) -> Result<(), ExtractionError>
where
    RunBootstrap: FnOnce(u64) -> RunBootstrapFut,
    RunBootstrapFut: Future<Output = Result<(), ExtractionError>>,
{
    match decision {
        BootstrapRunDecision::Skip => Ok(()),
        BootstrapRunDecision::AlreadyCompleted { configured_bootstrap_block } => {
            info!(
                startup_scope_kind,
                startup_scope_id = %startup_scope_id,
                bootstrap_block = configured_bootstrap_block,
                "Bootstrap already completed in storage; skipping bootstrap run"
            );
            Ok(())
        }
        BootstrapRunDecision::Run { configured_bootstrap_block } => {
            info!(
                startup_scope_kind,
                startup_scope_id = %startup_scope_id,
                bootstrap_block = configured_bootstrap_block,
                "Running bootstrap block before starting event stream"
            );
            tokio::select! {
                res = run_bootstrap(configured_bootstrap_block) => res,
                _ = tokio::signal::ctrl_c() => {
                    warn!(
                        startup_scope_kind,
                        startup_scope_id = %startup_scope_id,
                        bootstrap_block = configured_bootstrap_block,
                        "Bootstrap interrupted by SIGINT before startup completed"
                    );
                    Err(ExtractionError::Unknown(format!(
                        "bootstrap interrupted for {startup_scope_id}"
                    )))
                }
            }
        }
    }
}

pub(crate) async fn execute_bootstrap_run_decision_with_progress_reload<
    Progress,
    LoadProgress,
    LoadProgressFut,
    RunBootstrap,
    RunBootstrapFut,
>(
    progress: Progress,
    decision: BootstrapRunDecision,
    startup_scope_id: &str,
    startup_scope_kind: &'static str,
    load_progress: LoadProgress,
    run_bootstrap: RunBootstrap,
) -> Result<Progress, ExtractionError>
where
    LoadProgress: FnOnce() -> LoadProgressFut,
    LoadProgressFut: Future<Output = Result<Progress, ExtractionError>>,
    RunBootstrap: FnOnce(u64) -> RunBootstrapFut,
    RunBootstrapFut: Future<Output = Result<(), ExtractionError>>,
{
    let should_reload_progress = matches!(decision, BootstrapRunDecision::Run { .. });
    execute_bootstrap_run_decision(
        decision,
        startup_scope_id,
        startup_scope_kind,
        run_bootstrap,
    )
    .await?;

    if should_reload_progress {
        load_progress().await
    } else {
        Ok(progress)
    }
}

#[cfg(test)]
mod tests {
    use crate::extractor::shared_bootstrap::BootstrapCompletionSnapshot;

    use super::*;

    #[test]
    fn bootstrap_run_skips_when_progress_exists() {
        let decision = decide_bootstrap_run(
            true,
            Some(42),
            &BootstrapCompletionSnapshot { completed_blocks: vec![], missing_completion: vec![] },
            "extractor",
            BootstrapCompletionPolicy::AllowRerunOnConfiguredDrift,
        )
        .expect("existing progress should skip bootstrap");

        assert_eq!(decision, BootstrapRunDecision::Skip);
    }

    #[test]
    fn bootstrap_run_skips_when_no_bootstrap_block() {
        let decision = decide_bootstrap_run(
            false,
            None,
            &BootstrapCompletionSnapshot { completed_blocks: vec![], missing_completion: vec![] },
            "extractor",
            BootstrapCompletionPolicy::AllowRerunOnConfiguredDrift,
        )
        .expect("missing bootstrap config should skip");

        assert_eq!(decision, BootstrapRunDecision::Skip);
    }

    #[test]
    fn bootstrap_run_marks_completed_when_snapshot_matches() {
        let decision = decide_bootstrap_run(
            false,
            Some(42),
            &BootstrapCompletionSnapshot {
                completed_blocks: vec![("uniswap_v3".to_string(), 42)],
                missing_completion: vec![],
            },
            "extractor",
            BootstrapCompletionPolicy::AllowRerunOnConfiguredDrift,
        )
        .expect("matching completion should skip rerun");

        assert_eq!(
            decision,
            BootstrapRunDecision::AlreadyCompleted { configured_bootstrap_block: 42 }
        );
    }

    #[test]
    fn bootstrap_run_requests_rerun_when_snapshot_drifted() {
        let decision = decide_bootstrap_run(
            false,
            Some(42),
            &BootstrapCompletionSnapshot {
                completed_blocks: vec![("uniswap_v3".to_string(), 43)],
                missing_completion: vec![],
            },
            "extractor",
            BootstrapCompletionPolicy::AllowRerunOnConfiguredDrift,
        )
        .expect("drift should rerun under permissive policy");

        assert_eq!(decision, BootstrapRunDecision::Run { configured_bootstrap_block: 42 });
    }

    #[test]
    fn resolve_resume_start_block_uses_default_without_progress() {
        assert_eq!(
            resolve_resume_start_block(None, 42).expect("default start block should be used"),
            42
        );
    }

    #[test]
    fn resolve_resume_start_block_increments_last_processed_block() {
        assert_eq!(
            resolve_resume_start_block(Some(77), 42)
                .expect("resume block should resolve from existing progress"),
            78
        );
    }

    #[tokio::test]
    async fn execute_bootstrap_run_decision_returns_skip_without_running_bootstrap() {
        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();

        execute_bootstrap_run_decision(
            BootstrapRunDecision::Skip,
            "uniswap_family",
            "family",
            move |_| {
                let called_clone = called_clone.clone();
                async move {
                    called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await
        .expect("skip should succeed");

        assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn execute_bootstrap_run_decision_runs_bootstrap_closure_for_run_action() {
        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();

        execute_bootstrap_run_decision(
            BootstrapRunDecision::Run { configured_bootstrap_block: 42 },
            "uniswap_family",
            "family",
            move |bootstrap_block| {
                let called_clone = called_clone.clone();
                async move {
                    assert_eq!(bootstrap_block, 42);
                    called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await
        .expect("run action should execute bootstrap closure");

        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }
}
