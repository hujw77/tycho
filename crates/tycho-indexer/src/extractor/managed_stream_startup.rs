use crate::{
    extractor::{
        control::{
            build_runtime_control_wiring, RuntimeControlWiring, SubscriptionsMap,
            new_subscriptions_map,
        },
        Extractor,
    },
};
use std::sync::Arc;

#[cfg(test)]
use crate::extractor::{
    runtime_targets_startup::PreparedRuntimeTargetStartup,
    standalone_managed_startup::prepared_single_runtime_owner,
};

use tokio::sync::Mutex;

#[cfg(test)]
use tokio::runtime::Handle;

#[cfg(test)]
use crate::extractor::{
    control::ExtractorHandle,
    extractor_config::ExtractorConfig,
    family_registry::default_family_runtime_registry,
    managed_substreams_request::{
        prepare_substreams_request_for_runtime_target, StandalonePreparedRequestContext,
    },
    runner::ManagedRunner,
    runtime_target_planning::ResolvedStandaloneRuntime,
    single_runtime_execution::ExtractorRunner,
    ExtractionError,
};

pub(crate) struct SingleRuntimeWiring {
    pub(crate) extractor: Arc<dyn Extractor>,
    pub(crate) subscriptions: Arc<Mutex<SubscriptionsMap>>,
    pub(crate) control: RuntimeControlWiring,
}

impl SingleRuntimeWiring {
    pub(crate) fn from_extractor(extractor: Arc<dyn Extractor>) -> Self {
        let mut control = build_runtime_control_wiring([extractor.get_id()]);
        let subscriptions = new_subscriptions_map();
        control.handles = vec![control
            .handles
            .pop()
            .expect("single runtime wiring handle")];
        Self { extractor, subscriptions, control }
    }
}

#[cfg(test)]
pub(crate) async fn build_test_single_runner(
    config: &ExtractorConfig,
    extractor: Arc<dyn Extractor>,
    endpoint_url: &str,
    s3_bucket: Option<&str>,
    substreams_api_token: &str,
    final_block_only: bool,
    partial_blocks: bool,
    runtime_handle: Option<Handle>,
) -> Result<(ExtractorRunner, ExtractorHandle), ExtractionError> {
    build_test_single_runner_with_registry(
        config,
        extractor,
        endpoint_url,
        s3_bucket,
        substreams_api_token,
        final_block_only,
        partial_blocks,
        runtime_handle,
        default_family_runtime_registry(),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn build_test_single_runner_with_registry(
    config: &ExtractorConfig,
    extractor: Arc<dyn Extractor>,
    endpoint_url: &str,
    s3_bucket: Option<&str>,
    substreams_api_token: &str,
    final_block_only: bool,
    partial_blocks: bool,
    runtime_handle: Option<Handle>,
    registry: crate::extractor::family_registry::FamilyRuntimeRegistry<'static>,
) -> Result<(ExtractorRunner, ExtractorHandle), ExtractionError> {
    let extractor_id = extractor.get_id();
    let runtime_target = ResolvedStandaloneRuntime::from_extractor_config_with_registry(
        config, registry,
    )?;
    let rpc_client = tycho_ethereum::rpc::EthereumRpcClient::new("http://localhost:0000")
        .expect("build stub rpc client for request preparation");
    let prepared_request = prepare_substreams_request_for_runtime_target(
        &runtime_target,
        &StandalonePreparedRequestContext {
            extractor: extractor.clone(),
            startup_scope_id: extractor_id.to_string(),
            shared_bootstrap: runtime_target
                .bootstrap_runtime()
                .cloned()
                .map(|runtime| crate::extractor::managed_substreams_request::PreparedSharedBootstrap::for_standalone(
                    runtime,
                    extractor.clone(),
                )),
        },
        &rpc_client,
    )
    .await?;
    let stream = prepared_request
        .load_stream(
        s3_bucket,
        endpoint_url,
        substreams_api_token,
        final_block_only,
        partial_blocks,
    )
    .await?;
    let prepared_startup = PreparedRuntimeTargetStartup::new(
        prepared_single_runtime_owner(extractor, extractor_id),
        stream,
    );
    let (runner, mut handles) = prepared_startup
        .build_managed_runner(runtime_handle, partial_blocks)?;
    let runner = match runner {
        ManagedRunner::Single(runner) => runner,
        ManagedRunner::Family(_) => {
            panic!("single-runner test helper should never build a family managed runner")
        }
    };
    Ok((
        runner,
        handles
            .pop()
            .expect("single runner handle"),
    ))
}
