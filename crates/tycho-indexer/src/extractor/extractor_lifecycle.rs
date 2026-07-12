use crate::extractor::{
    load_extractor_progress_snapshot, runner::BootstrapConfig, shared_bootstrap::{
        configured_bootstrap_block, decide_bootstrap_completion, BootstrapCompletionDecision,
        BootstrapCompletionPolicy, BootstrapCompletionSnapshot,
    }, ExtractionError, Extractor, ExtractorProgressSnapshot,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StandaloneBootstrapAction {
    Skip,
    AlreadyCompleted { configured_bootstrap_block: u64 },
    Run { configured_bootstrap_block: u64 },
}

pub(crate) async fn load_standalone_progress_snapshot(
    extractor: &dyn Extractor,
) -> Result<ExtractorProgressSnapshot, ExtractionError> {
    load_extractor_progress_snapshot(extractor).await
}

pub(crate) fn decide_standalone_bootstrap_action(
    progress: &ExtractorProgressSnapshot,
    extractor_name: &str,
    bootstrap: Option<&BootstrapConfig>,
) -> Result<StandaloneBootstrapAction, ExtractionError> {
    if progress.last_processed_block.is_some() {
        return Ok(StandaloneBootstrapAction::Skip);
    }

    let Some(bootstrap) = bootstrap else {
        return Ok(StandaloneBootstrapAction::Skip);
    };

    let configured_bootstrap_block =
        configured_bootstrap_block(bootstrap.start_block, extractor_name)?;
    let completion_decision = decide_bootstrap_completion(
        &BootstrapCompletionSnapshot {
            completed_blocks: progress
                .completed_bootstrap_block
                .map(|block| vec![(extractor_name.to_string(), block)])
                .unwrap_or_default(),
            missing_completion: Vec::new(),
        },
        configured_bootstrap_block,
        "extractor",
        BootstrapCompletionPolicy::AllowRerunOnConfiguredDrift,
    )?;

    Ok(match completion_decision {
        BootstrapCompletionDecision::AlreadyCompleted => {
            StandaloneBootstrapAction::AlreadyCompleted { configured_bootstrap_block }
        }
        BootstrapCompletionDecision::NeedsBootstrap => {
            StandaloneBootstrapAction::Run { configured_bootstrap_block }
        }
    })
}

pub(crate) fn resolve_standalone_stream_start_block(
    progress: &ExtractorProgressSnapshot,
    default_start_block: i64,
) -> Result<i64, ExtractionError> {
    progress
        .last_processed_block
        .as_ref()
        .map(|block| {
            let next = block
                .number
                .checked_add(1)
                .ok_or_else(|| ExtractionError::Setup("block number overflow".to_string()))?;
            i64::try_from(next)
                .map_err(|_| ExtractionError::Setup("block number exceeds i64".to_string()))
        })
        .transpose()?
        .map_or(Ok(default_start_block), Ok)
}

#[cfg(test)]
mod tests {
    use tycho_common::models::blockchain::Block;

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
        }
    }

    fn bootstrap_config(start_block: i64) -> BootstrapConfig {
        BootstrapConfig {
            strategy: crate::extractor::runner::BootstrapStrategy::UniswapV3Rpc,
            start_block,
            params: "bootstrap_block=42&pool=0x0000000000000000000000000000000000001234"
                .to_string(),
        }
    }

    #[test]
    fn standalone_bootstrap_action_skips_when_progress_exists() {
        let progress = progress_snapshot(Some(77), Some(42));

        let action =
            decide_standalone_bootstrap_action(&progress, "uniswap_v3", Some(&bootstrap_config(42)))
                .expect("existing progress should skip bootstrap");

        assert_eq!(action, StandaloneBootstrapAction::Skip);
    }

    #[test]
    fn standalone_bootstrap_action_recognizes_completed_bootstrap() {
        let progress = progress_snapshot(None, Some(42));

        let action =
            decide_standalone_bootstrap_action(&progress, "uniswap_v3", Some(&bootstrap_config(42)))
                .expect("matching completed bootstrap should be accepted");

        assert_eq!(
            action,
            StandaloneBootstrapAction::AlreadyCompleted { configured_bootstrap_block: 42 }
        );
    }

    #[test]
    fn standalone_bootstrap_action_reruns_when_completed_block_drifted() {
        let progress = progress_snapshot(None, Some(43));

        let action =
            decide_standalone_bootstrap_action(&progress, "uniswap_v3", Some(&bootstrap_config(42)))
                .expect("configured drift should allow rerun for standalone extractor");

        assert_eq!(
            action,
            StandaloneBootstrapAction::Run { configured_bootstrap_block: 42 }
        );
    }

    #[test]
    fn resolve_standalone_stream_start_block_uses_next_committed_block() {
        let progress = progress_snapshot(Some(77), None);

        let start_block =
            resolve_standalone_stream_start_block(&progress, 42).expect("resume block should resolve");

        assert_eq!(start_block, 78);
    }
}
