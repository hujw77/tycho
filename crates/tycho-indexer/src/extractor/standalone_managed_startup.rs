use async_trait::async_trait;

use crate::extractor::{
    managed_extractor_initialization::ManagedExtractorBuildContext,
    managed_stream_startup::PreparedSingleRunnerStartup,
    managed_substreams_request::StandalonePreparedRequestContext,
    runtime_targets_startup::{
        prepare_managed_startup_request_from_payload, ManagedStartupLifecycleView,
        ManagedStartupPreparedRequestPayload, PreparedManagedStartupDraft,
        PreparedManagedStartupPayload,
    },
    runtime_target_planning::ResolvedStandaloneRuntime,
    ExtractionError, Extractor,
};
use std::sync::Arc;

use tycho_common::models::ExtractorIdentity;

use crate::substreams::stream::SubstreamsStream;

pub(crate) type PreparedSingleRunnerDraft = PreparedManagedStartupDraft<PreparedSingleRunnerPayload>;

pub(crate) struct PreparedSingleRunnerPayload {
    pub(crate) extractor: Arc<dyn Extractor>,
    pub(crate) extractor_id: ExtractorIdentity,
}

impl<'a> ResolvedStandaloneRuntime<'a> {
    pub(crate) async fn prepare_managed_startup_draft(
        self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedSingleRunnerDraft, ExtractionError> {
        <Self as ManagedStartupLifecycleView>::prepare_managed_startup_draft(
            &self,
            extractor_build,
        )
        .await
    }
}

#[async_trait]
impl<'a> ManagedStartupLifecycleView<'a> for ResolvedStandaloneRuntime<'a> {
    type Payload = PreparedSingleRunnerPayload;

    async fn build_managed_startup_payload(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedSingleRunnerPayload, ExtractionError> {
        let extractor = extractor_build
            .build_unique_runtime_target_extractor(self)
            .await?;
        let extractor_id = extractor.get_id();
        Ok(PreparedSingleRunnerPayload { extractor, extractor_id })
    }

    async fn prepare_substreams_request_for_managed_startup(
        &self,
        payload: &Self::Payload,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<crate::extractor::managed_substreams_request::PreparedSubstreamsRequest, ExtractionError>
    {
        prepare_managed_startup_request_from_payload(self, payload, extractor_build).await
    }
}

impl PreparedManagedStartupPayload for PreparedSingleRunnerPayload {
    type PreparedStartup = PreparedSingleRunnerStartup;

    fn into_prepared_startup(
        self,
        stream: SubstreamsStream,
    ) -> PreparedSingleRunnerStartup {
        PreparedSingleRunnerStartup {
            extractor: self.extractor,
            extractor_id: self.extractor_id,
            stream,
        }
    }
}

impl ManagedStartupPreparedRequestPayload for PreparedSingleRunnerPayload {
    type PreparedRequestContext = StandalonePreparedRequestContext;

    fn prepared_request_context(
        &self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Self::PreparedRequestContext {
        StandalonePreparedRequestContext {
            extractor: self.extractor.clone(),
            extractor_id: self.extractor_id.clone(),
            registry: extractor_build.family_runtime_registry,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use super::*;
    use crate::extractor::{
        extractor_config::{ExtractorConfig, ProtocolTypeConfig},
        protocol_message_registry::{
            AuxiliaryProtocolMessageBuildFuture, AuxiliaryProtocolMessageContext,
            AuxiliaryProtocolMessageDecoder,
        },
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
            crate::extractor::test_support::future_family_runtime_registry_with_auxiliary_decoders_for_tests(
                &["future_v1"],
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
            auxiliary_protocol_message_decoders_for_protocol_system_with_registry(
                config.protocol_system(),
                registry,
            );

        assert_eq!(decoders.len(), 1);
        assert_eq!(decoders[0].protocol_system, "future_v1");
        assert_eq!(decoders[0].type_url_suffix, "FutureEvents");
    }
}
