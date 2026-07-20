use async_trait::async_trait;

use crate::extractor::{
    managed_extractor_initialization::ManagedExtractorBuildContext,
    managed_stream_startup::SingleRuntimeWiring,
    managed_substreams_request::{PreparedSharedBootstrap, StandalonePreparedRequestContext},
    runtime_targets_startup::{
        ManagedRunnerFactory, ManagedStartupLifecycleView, PreparedManagedRuntimeOwner,
    },
    runtime_target_planning::ResolvedStandaloneRuntime,
    ExtractionError, Extractor,
};
use std::sync::Arc;
use tokio::runtime::Handle;

use tycho_common::models::ExtractorIdentity;

use crate::substreams::stream::SubstreamsStream;

#[derive(Clone)]
pub(crate) struct SingleRuntimeRunnerFactory {
    extractor: Arc<dyn Extractor>,
}

pub(crate) type PreparedSingleRuntimeOwner =
    PreparedManagedRuntimeOwner<SingleRuntimeRunnerFactory, StandalonePreparedRequestContext>;

impl SingleRuntimeRunnerFactory {
    fn new(extractor: Arc<dyn Extractor>) -> Self {
        Self { extractor }
    }
}

pub(crate) fn prepared_single_runtime_owner(
    extractor: Arc<dyn Extractor>,
    extractor_id: ExtractorIdentity,
) -> PreparedSingleRuntimeOwner {
    PreparedManagedRuntimeOwner::new(
        SingleRuntimeRunnerFactory::new(extractor.clone()),
        StandalonePreparedRequestContext {
            extractor,
            startup_scope_id: extractor_id.to_string(),
            shared_bootstrap: None,
        },
    )
}

impl ManagedRunnerFactory for SingleRuntimeRunnerFactory {
    fn into_managed_runner(
        self: Box<Self>,
        stream: SubstreamsStream,
        runtime_handle: Option<Handle>,
        partial_blocks: bool,
    ) -> Result<
        (
            crate::extractor::runner::ManagedRunner,
            Vec<crate::extractor::control::ExtractorHandle>,
        ),
        ExtractionError,
    > {
        let wiring = SingleRuntimeWiring::from_extractor(self.extractor);
        let runner = crate::extractor::single_runtime_execution::ExtractorRunner::new(
            wiring.extractor,
            stream,
            wiring.subscriptions,
            wiring.control.control_rx,
            runtime_handle,
            partial_blocks,
        );

        Ok((
            crate::extractor::runner::ManagedRunner::new_single(runner),
            wiring.control.handles,
        ))
    }
}

#[async_trait]
impl<'a> ManagedStartupLifecycleView<'a> for ResolvedStandaloneRuntime<'a> {
    type RuntimeOwner = PreparedSingleRuntimeOwner;

    async fn build_managed_runtime_owner(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<Self::RuntimeOwner, ExtractionError> {
        let extractor = extractor_build
            .build_unique_runtime_target_extractor(self)
            .await?;
        let extractor_id = extractor.get_id();
        let mut runtime_owner = prepared_single_runtime_owner(extractor.clone(), extractor_id);
        runtime_owner.prepared_request_context_mut().shared_bootstrap = self
            .bootstrap_runtime()
            .cloned()
            .map(|runtime| PreparedSharedBootstrap::for_standalone(runtime, extractor));
        Ok(runtime_owner)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tycho_common::models::{Chain, ExtractorIdentity, FinancialType, ImplementationType};

    use super::*;
    use crate::extractor::{
        extractor_config::{ExtractorConfig, ProtocolTypeConfig},
        protocol_message_registry::{
            AuxiliaryProtocolMessageBuildFuture, AuxiliaryProtocolMessageContext,
            AuxiliaryProtocolMessageDecoder,
        },
        MockExtractor,
    };

    fn build_future_events_for_startup_test<'a>(
        _context: &'a dyn AuxiliaryProtocolMessageContext,
        _value: &'a [u8],
        _finalized_block_height: u64,
        _partial_block_index: Option<u32>,
    ) -> AuxiliaryProtocolMessageBuildFuture<'a> {
        Box::pin(async {
            Err(ExtractionError::Unknown("test-only decoder should not run".to_string()))
        })
    }

    #[tokio::test]
    async fn prepare_standalone_managed_startup_injects_custom_registry_decoders() {
        const FUTURE_DECODERS: &[AuxiliaryProtocolMessageDecoder] =
            &[AuxiliaryProtocolMessageDecoder {
                protocol_system: "future_v1",
                type_url_suffix: "FutureEvents",
                build_block_changes: build_future_events_for_startup_test,
            }];
        let registry =
            crate::extractor::test_support::future_family_runtime_registry_with_auxiliary_decoders_and_explicit_progress_owner_for_tests(
                &["future_v1"],
                "future_v1",
                "family::future_swap",
                FUTURE_DECODERS,
            );
        let config = ExtractorConfig::new(
            "future_v1_alias".to_string(),
            Chain::Ethereum,
            ImplementationType::Custom,
            1,
            42,
            None,
            vec![ProtocolTypeConfig::new("future_pool".to_string(), FinancialType::Swap)],
            "/tmp/future-v1-test.spkg".to_string(),
            "map_future_pool_events".to_string(),
            vec![],
            0,
            None,
            None,
            HashMap::new(),
            None,
        )
        .with_protocol_system("future_v1");

        let decoders = ManagedExtractorBuildContext::
            auxiliary_runtime_hooks_for_protocol_system_with_registry(
                config.protocol_system(),
                registry,
            )
            .message_decoders;

        assert_eq!(decoders.len(), 1);
        assert_eq!(decoders[0].protocol_system, "future_v1");
        assert_eq!(decoders[0].type_url_suffix, "FutureEvents");
    }

    #[test]
    fn prepared_request_context_preserves_custom_family_registry() {
        let registry = crate::extractor::test_support::future_family_runtime_registry_with_explicit_progress_owner_for_tests(
            &["future_v1"],
            "future_v1",
            "family::future_swap",
        );
        let extractor = Arc::new(MockExtractor::new());
        let runtime_owner = prepared_single_runtime_owner(
            extractor.clone(),
            ExtractorIdentity::new(Chain::Ethereum, "future_v1_alias"),
        );
        let context = crate::extractor::runtime_targets_startup::ManagedStartupPreparedRequestContext::prepared_request_context(&runtime_owner);
        let extractor_trait: Arc<dyn crate::extractor::Extractor> = extractor.clone();
        assert_eq!(context.startup_scope_id, "ethereum:future_v1_alias");
        assert!(
            Arc::ptr_eq(&context.extractor, &extractor_trait),
            "prepared request context should retain the built extractor"
        );

        assert_eq!(
            registry
                .require_family_name_for_protocol_systems(
                    ["future_v1"].into_iter(),
                    "standalone prepared request test",
                )
                .expect("custom registry should resolve future family"),
            "future_swap"
        );
    }
}
