use crate::extractor::{
    extractor_config::ExtractorConfig,
    family_registry::FamilyRuntimeRegistry,
    managed_extractor_initialization::ManagedExtractorBuildContext,
    managed_substreams_request::PreparedSubstreamsRequest,
    managed_stream_startup::PreparedSingleRunnerStartup,
    protocol_message_registry::{
        default_auxiliary_protocol_message_decoders_for_protocol_system,
        default_auxiliary_protocol_state_hydrators_for_protocol_system,
    },
    runtime_target_planning::ResolvedStandaloneRuntime,
    ExtractionError, Extractor,
};
use std::sync::Arc;

use tycho_common::models::ExtractorIdentity;

use crate::substreams::stream::SubstreamsStream;

pub(crate) struct PreparedSingleRunnerDraft {
    pub(crate) extractor: Arc<dyn Extractor>,
    pub(crate) extractor_id: ExtractorIdentity,
    pub(crate) prepared_request: PreparedSubstreamsRequest,
}

fn standalone_auxiliary_protocol_message_decoders(
    extractor_config: &ExtractorConfig,
    registry: FamilyRuntimeRegistry<'static>,
) -> Vec<crate::extractor::protocol_message_registry::AuxiliaryProtocolMessageDecoder> {
    default_auxiliary_protocol_message_decoders_for_protocol_system(
        extractor_config.protocol_system(),
        registry,
    )
}

fn standalone_auxiliary_protocol_state_hydrators(
    extractor_config: &ExtractorConfig,
    registry: FamilyRuntimeRegistry<'static>,
) -> Vec<crate::extractor::protocol_message_registry::AuxiliaryProtocolStateHydrator> {
    default_auxiliary_protocol_state_hydrators_for_protocol_system(
        extractor_config.protocol_system(),
        registry,
    )
}

impl<'a> ResolvedStandaloneRuntime<'a> {
    pub(crate) async fn prepare_managed_startup_draft(
        self,
        extractor_build: ManagedExtractorBuildContext<'_>,
    ) -> Result<PreparedSingleRunnerDraft, ExtractionError> {
        let auxiliary_protocol_message_decoders = standalone_auxiliary_protocol_message_decoders(
            self.extractor_config,
            extractor_build.family_runtime_registry,
        );
        let auxiliary_protocol_state_hydrators = standalone_auxiliary_protocol_state_hydrators(
            self.extractor_config,
            extractor_build.family_runtime_registry,
        );
        let extractor = extractor_build
            .build_initialized_extractor(
                self.extractor_config,
                auxiliary_protocol_message_decoders,
                auxiliary_protocol_state_hydrators,
            )
            .await?;
        let extractor_id = extractor.get_id();
        let prepared_request = self
            .prepare_substreams_request(
                extractor.clone(),
                &extractor_id,
                extractor_build.rpc_client,
                extractor_build.family_runtime_registry,
            )
            .await?;
        Ok(PreparedSingleRunnerDraft {
            extractor,
            extractor_id,
            prepared_request,
        })
    }
}

impl PreparedSingleRunnerDraft {
    pub(crate) fn into_prepared_startup(
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tycho_common::models::{Chain, FinancialType, ImplementationType};

    use super::*;
    use crate::extractor::{
        extractor_config::ProtocolTypeConfig,
        family_registry::{FamilyRuntimeRegistry, FamilyRuntimeSpec},
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
            Err(ExtractionError::Unknown(
                "test-only decoder should not run".to_string(),
            ))
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
        const FUTURE_FAMILY: FamilyRuntimeSpec =
            crate::extractor::family_registry::shared_family_runtime_spec_with_auxiliary_decoders(
                "future_swap",
                &[crate::extractor::family_registry::shared_family_member_spec(
                    "future_v1",
                    &["futurev1"],
                    None,
                )],
                "map_future_swap_family_protocol_changes",
                "future_swap_family",
                "family::future_swap",
                None,
                FUTURE_DECODERS,
            );
        let registry = FamilyRuntimeRegistry::new(&[FUTURE_FAMILY]);
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

        let decoders = standalone_auxiliary_protocol_message_decoders(&config, registry);

        assert_eq!(decoders.len(), 1);
        assert_eq!(decoders[0].protocol_system, "future_v1");
        assert_eq!(decoders[0].type_url_suffix, "FutureEvents");
    }
}
