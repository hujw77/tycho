use crate::extractor::{
    bootstrap_lifecycle::{
        decide_bootstrap_run, execute_bootstrap_run_decision_with_progress_reload,
    },
    extractor_config::BootstrapConfig,
    family_registry::FamilyRuntimeRegistry,
    load_extractor_progress_snapshot,
    managed_substreams_request::{resolved_stream_position_from_resume, ResolvedStreamPosition},
    shared_bootstrap::{
        configured_bootstrap_block, execute_materialized_bootstrap_plan,
        BootstrapCompletionPolicy, BootstrapCompletionSnapshot,
        MaterializedBootstrapCommitTarget, SharedBootstrapPlan,
    },
    runtime_target_planning::ResolvedStandaloneRuntime,
    ExtractionError, Extractor, ExtractorProgressSnapshot,
};
use std::sync::Arc;
use tracing::info;
use tycho_common::models::ExtractorIdentity;
use tycho_ethereum::rpc::EthereumRpcClient;

pub(crate) use crate::extractor::bootstrap_lifecycle::BootstrapRunDecision as StandaloneBootstrapAction;

pub(crate) type ResolvedStandaloneStreamPosition = ResolvedStreamPosition;

pub(crate) struct StandaloneLifecycleProgress {
    progress: ExtractorProgressSnapshot,
}

async fn run_standalone_bootstrap_once(
    standalone: &ResolvedStandaloneRuntime<'_>,
    extractor: Arc<dyn Extractor>,
    extractor_id: &ExtractorIdentity,
    rpc_client: &EthereumRpcClient,
    registry: FamilyRuntimeRegistry<'static>,
) -> Result<(), ExtractionError> {
    let config = standalone.extractor_config;
    let bootstrap = config
        .bootstrap
        .as_ref()
        .expect("standalone bootstrap execution requires bootstrap config");
    let plan =
        SharedBootstrapPlan::for_extractor_config_with_registry(config, bootstrap, registry)?;
    let shared_bootstrap_execution = registry
        .resolve_shared_bootstrap_execution_for_protocol_system(config.protocol_system())?;

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

    execute_materialized_bootstrap_plan(
        rpc_client,
        &plan,
        &shared_bootstrap_execution,
        vec![MaterializedBootstrapCommitTarget::whole_block(extractor.clone())],
        extractor,
    )
    .await?;

    info!(
        extractor_id = %extractor_id,
        bootstrap_block = plan.bootstrap_block,
        "BootstrapExecutorCompleted"
    );

    Ok(())
}

pub(crate) async fn load_standalone_progress_snapshot(
    extractor: &dyn Extractor,
) -> Result<ExtractorProgressSnapshot, ExtractionError> {
    load_extractor_progress_snapshot(extractor).await
}

impl StandaloneLifecycleProgress {
    pub(crate) async fn load(
        extractor: &dyn Extractor,
    ) -> Result<Self, ExtractionError> {
        Ok(Self { progress: load_standalone_progress_snapshot(extractor).await? })
    }

    pub(crate) fn last_processed_block(&self) -> Option<u64> {
        self.progress
            .last_processed_block
            .as_ref()
            .map(|block| block.number)
    }

    pub(crate) fn decide_bootstrap_action(
        &self,
        extractor_name: &str,
        bootstrap: Option<&BootstrapConfig>,
    ) -> Result<StandaloneBootstrapAction, ExtractionError> {
        decide_standalone_bootstrap_action(&self.progress, extractor_name, bootstrap)
    }

    pub(crate) fn resolve_stream_position(
        &self,
        default_start_block: i64,
    ) -> Result<ResolvedStandaloneStreamPosition, ExtractionError> {
        resolved_stream_position_from_resume(
            self.last_processed_block(),
            default_start_block,
            None,
        )
    }
}

pub(crate) fn decide_standalone_bootstrap_action(
    progress: &ExtractorProgressSnapshot,
    extractor_name: &str,
    bootstrap: Option<&BootstrapConfig>,
) -> Result<StandaloneBootstrapAction, ExtractionError> {
    decide_bootstrap_run(
        progress.last_processed_block.is_some(),
        bootstrap
            .map(|bootstrap| configured_bootstrap_block(bootstrap.start_block, extractor_name))
            .transpose()?,
        &BootstrapCompletionSnapshot {
            completed_blocks: progress
                .completed_bootstrap_block
                .map(|block| vec![(extractor_name.to_string(), block)])
                .unwrap_or_default(),
            missing_completion: Vec::new(),
        },
        "extractor",
        BootstrapCompletionPolicy::AllowRerunOnConfiguredDrift,
    )
}

#[cfg(test)]
pub(crate) fn resolve_standalone_stream_start_block(
    progress: &ExtractorProgressSnapshot,
    default_start_block: i64,
) -> Result<i64, ExtractionError> {
    Ok(
        resolved_stream_position_from_resume(
            progress
                .last_processed_block
                .as_ref()
                .map(|block| block.number),
            default_start_block,
            None,
        )?
        .start_block,
    )
}

impl<'a> ResolvedStandaloneRuntime<'a> {
    pub(crate) async fn prepare_stream_position_after_bootstrap(
        &self,
        extractor: Arc<dyn Extractor>,
        extractor_id: &ExtractorIdentity,
        rpc_client: &EthereumRpcClient,
        registry: FamilyRuntimeRegistry<'static>,
    ) -> Result<ResolvedStandaloneStreamPosition, ExtractionError> {
        let progress = StandaloneLifecycleProgress::load(extractor.as_ref()).await?;
        let decision = progress.decide_bootstrap_action(
            self.extractor_config.name(),
            self.extractor_config.bootstrap.as_ref(),
        )?;
        let bootstrap_extractor = extractor.clone();
        let progress = execute_bootstrap_run_decision_with_progress_reload(
            progress,
            decision,
            &extractor_id.to_string(),
            "extractor",
            || StandaloneLifecycleProgress::load(extractor.as_ref()),
            move |_| {
                run_standalone_bootstrap_once(
                    self,
                    bootstrap_extractor.clone(),
                    extractor_id,
                    rpc_client,
                    registry,
                )
            },
        )
        .await?;
        let stream_position =
            progress.resolve_stream_position(self.substreams_execution_request()?.start_block)?;

        if let Some(block_number) = progress.last_processed_block() {
            info!(
                start_block = stream_position.start_block,
                last_committed_block = block_number,
                config_start_block = self.extractor_config.start_block(),
                "Fresh start: resuming from block after last committed"
            );
        }

        Ok(stream_position)
    }

}

#[cfg(test)]
mod tests {
    use tycho_common::models::{
        blockchain::Block, Chain, FinancialType, ImplementationType,
    };

    use crate::extractor::{
        extractor_config::{BootstrapStrategy, ExtractorConfig, ProtocolTypeConfig},
        runtime_target_planning::ResolvedStandaloneRuntime,
        MockExtractor, PersistedExtractorStateScope,
    };

    use super::*;

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
                chain: tycho_common::models::Chain::Ethereum,
                ts: chrono::NaiveDateTime::default(),
            }),
            completed_bootstrap_block,
            cursor_scope: PersistedExtractorStateScope::Unknown,
            completed_bootstrap_scope: PersistedExtractorStateScope::Unknown,
        }
    }

    fn bootstrap_config(start_block: i64) -> BootstrapConfig {
        BootstrapConfig {
            strategy: BootstrapStrategy::UniswapV3Rpc,
            start_block,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                .to_string(),
        }
    }

    #[test]
    fn standalone_bootstrap_action_skips_when_progress_exists() {
        let progress = progress_snapshot(Some(77), Some(42));

        let action = decide_standalone_bootstrap_action(
            &progress,
            "uniswap_v3",
            Some(&bootstrap_config(42)),
        )
        .expect("existing progress should skip bootstrap");

        assert_eq!(action, StandaloneBootstrapAction::Skip);
    }

    #[test]
    fn standalone_bootstrap_action_recognizes_completed_bootstrap() {
        let progress = progress_snapshot(None, Some(42));

        let action = decide_standalone_bootstrap_action(
            &progress,
            "uniswap_v3",
            Some(&bootstrap_config(42)),
        )
        .expect("matching completed bootstrap should be accepted");

        assert_eq!(
            action,
            StandaloneBootstrapAction::AlreadyCompleted { configured_bootstrap_block: 42 }
        );
    }

    #[test]
    fn standalone_bootstrap_action_reruns_when_completed_block_drifted() {
        let progress = progress_snapshot(None, Some(43));

        let action = decide_standalone_bootstrap_action(
            &progress,
            "uniswap_v3",
            Some(&bootstrap_config(42)),
        )
        .expect("configured drift should allow rerun for standalone extractor");

        assert_eq!(action, StandaloneBootstrapAction::Run { configured_bootstrap_block: 42 });
    }

    #[test]
    fn resolve_standalone_stream_start_block_uses_next_committed_block() {
        let progress = progress_snapshot(Some(77), None);

        let start_block = resolve_standalone_stream_start_block(&progress, 42)
            .expect("resume block should resolve");

        assert_eq!(start_block, 78);
    }

    #[tokio::test]
    async fn resolve_standalone_stream_position_uses_next_committed_block_without_cursor() {
        let config = ExtractorConfig::new(
            "curve".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1000,
            100,
            Some(150),
            vec![ProtocolTypeConfig::new("curve_pool".to_string(), FinancialType::Swap)],
            "protocols/substreams/curve/curve.spkg".to_string(),
            "map_curve".to_string(),
            vec![],
            0,
            None,
            None,
            Default::default(),
            None,
        );
        let standalone = ResolvedStandaloneRuntime {
            protocol_system: "curve",
            extractor_config: &config,
        };

        let mut extractor = MockExtractor::new();
        extractor
            .expect_get_cursor()
            .once()
            .returning(|| "cursor:ignored-for-standalone".to_string());
        extractor
            .expect_get_last_processed_block()
            .once()
            .returning(|| {
                Some(Block {
                    number: 123,
                    hash: Default::default(),
                    parent_hash: Default::default(),
                    chain: Chain::Ethereum,
                    ts: chrono::NaiveDateTime::default(),
                })
            });
        extractor
            .expect_get_completed_bootstrap_block()
            .once()
            .returning(|| Ok(None));
        extractor
            .expect_supports_persisted_state_scope()
            .once()
            .return_const(false);

        let progress =
            StandaloneLifecycleProgress::load(&extractor).await.expect("standalone progress");
        let position = progress
            .resolve_stream_position(standalone.substreams_execution_request().expect("request").start_block)
            .expect("standalone stream position resolves");

        assert_eq!(position.start_block, 124);
        assert_eq!(position.cursor, None);
    }
}
