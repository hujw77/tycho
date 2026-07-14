use std::{collections::HashMap, sync::Arc};

use tracing::{info, warn};
use tycho_common::models::ExtractorIdentity;
use tycho_ethereum::rpc::EthereumRpcClient;

use crate::extractor::{
    extractor_config::{BootstrapConfig, ExtractorConfig},
    extractor_lifecycle::{
        decide_standalone_bootstrap_action, load_standalone_progress_snapshot,
        resolve_standalone_stream_start_block, StandaloneBootstrapAction,
    },
    family_lifecycle::{resolve_family_stream_position, run_family_bootstrap_if_needed},
    family_registry::FamilyRuntimeRegistry,
    family_runtime_planning::ResolvedFamilyRuntime,
    runtime_target_planning::ResolvedStandaloneRuntime,
    shared_bootstrap::{commit_materialized_bootstrap, SharedBootstrapPlan},
    ExtractionError, Extractor,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedSubstreamsRequest {
    pub request: crate::extractor::runtime_target_planning::ResolvedSubstreamsExecutionRequest,
    pub cursor: Option<String>,
}

async fn run_standalone_bootstrap_once(
    config: &ExtractorConfig,
    extractor: Arc<dyn Extractor>,
    bootstrap: &BootstrapConfig,
    extractor_id: &ExtractorIdentity,
    rpc_client: &EthereumRpcClient,
    registry: FamilyRuntimeRegistry<'static>,
) -> Result<(), ExtractionError> {
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

impl<'a> ResolvedFamilyRuntime<'a> {
    pub(crate) async fn prepare_substreams_request(
        &self,
        extractors: &HashMap<String, Arc<dyn Extractor>>,
        rpc_client: &EthereumRpcClient,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        run_family_bootstrap_if_needed(extractors, &self.execution, rpc_client).await?;

        let stream_position = resolve_family_stream_position(extractors, &self.execution).await?;
        let request = self.substreams_execution_request_with_start_block(stream_position.start_block)?;

        Ok(PreparedSubstreamsRequest { request, cursor: stream_position.cursor })
    }
}

impl<'a> ResolvedStandaloneRuntime<'a> {
    pub(crate) async fn prepare_substreams_request(
        &self,
        extractor: Arc<dyn Extractor>,
        extractor_id: &ExtractorIdentity,
        rpc_client: &EthereumRpcClient,
        registry: FamilyRuntimeRegistry<'static>,
    ) -> Result<PreparedSubstreamsRequest, ExtractionError> {
        let config = self.extractor_config;
        let default_request = self.substreams_execution_request()?;
        let mut progress = load_standalone_progress_snapshot(extractor.as_ref()).await?;

        match decide_standalone_bootstrap_action(
            &progress,
            config.name(),
            config.bootstrap.as_ref(),
        )? {
            StandaloneBootstrapAction::Skip => {}
            StandaloneBootstrapAction::AlreadyCompleted { .. } => {
                let bootstrap = config
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
                let bootstrap = config
                    .bootstrap
                    .as_ref()
                    .expect("run bootstrap action requires bootstrap config");
                info!(
                    bootstrap_block = bootstrap.start_block,
                    extractor_id = %extractor_id,
                    "Running bootstrap block before starting event stream"
                );
                tokio::select! {
                    res = run_standalone_bootstrap_once(
                        config,
                        extractor.clone(),
                        bootstrap,
                        extractor_id,
                        rpc_client,
                        registry,
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

        let start_block =
            resolve_standalone_stream_start_block(&progress, default_request.start_block)?;

        if let Some(block) = &progress.last_processed_block {
            info!(
                start_block,
                last_committed_block = block.number,
                config_start_block = config.start_block(),
                "Fresh start: resuming from block after last committed"
            );
        }

        let request = self.substreams_execution_request_with_start_block(start_block)?;
        Ok(PreparedSubstreamsRequest { request, cursor: None })
    }
}
