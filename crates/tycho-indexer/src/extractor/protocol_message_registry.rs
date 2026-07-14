use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
};

use async_trait::async_trait;
use tycho_common::{
    models::{
        protocol::{ComponentBalance, ProtocolComponent},
        Address, Chain, ComponentId,
    },
    Bytes,
};

use crate::extractor::{
    models::BlockChanges,
    ExtractionError,
};
use crate::extractor::family_registry::FamilyRuntimeRegistry;

pub(crate) type AuxiliaryProtocolMessageBuildFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BlockChanges, ExtractionError>> + Send + 'a>>;

#[async_trait]
pub(crate) trait AuxiliaryProtocolMessageContext: Send + Sync {
    fn extractor_name(&self) -> &str;

    fn chain(&self) -> Chain;

    fn protocol_system(&self) -> &str;

    async fn get_protocol_components(
        &self,
        component_ids: &[ComponentId],
    ) -> Result<HashMap<ComponentId, ProtocolComponent>, ExtractionError>;

    async fn get_protocol_state_values_at_tip(
        &self,
        keys: &[(String, String)],
    ) -> Result<HashMap<String, HashMap<String, Bytes>>, ExtractionError>;

    async fn get_component_balances_at_tip(
        &self,
        reverted_balances_keys: &[(&String, &Bytes)],
    ) -> Result<HashMap<String, HashMap<Address, ComponentBalance>>, ExtractionError>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuxiliaryProtocolMessageDecoder {
    pub protocol_system: &'static str,
    pub type_url_suffix: &'static str,
    pub build_block_changes: for<'a> fn(
        &'a dyn AuxiliaryProtocolMessageContext,
        &'a [u8],
        u64,
        Option<u32>,
    ) -> AuxiliaryProtocolMessageBuildFuture<'a>,
}

pub(crate) fn auxiliary_protocol_message_decoder_for(
    decoders: &[AuxiliaryProtocolMessageDecoder],
    protocol_system: &str,
    type_url: &str,
    ) -> Option<AuxiliaryProtocolMessageDecoder> {
    decoders.iter().find(|decoder| {
        decoder.protocol_system == protocol_system && type_url.ends_with(decoder.type_url_suffix)
    }).copied()
}

pub(crate) fn default_auxiliary_protocol_message_decoders_for_protocol_system(
    protocol_system: &str,
    registry: FamilyRuntimeRegistry<'_>,
) -> Vec<AuxiliaryProtocolMessageDecoder> {
    registry
        .registered_protocol_system_defaults(protocol_system)
        .map(|defaults| defaults.auxiliary_protocol_message_decoders().to_vec())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extractor::family_registry::default_family_runtime_registry;

    #[test]
    fn finds_registered_uniswap_v3_auxiliary_decoder() {
        let decoder = auxiliary_protocol_message_decoder_for(
            &default_auxiliary_protocol_message_decoders_for_protocol_system(
                "uniswap_v3",
                default_family_runtime_registry(),
            ),
            "uniswap_v3",
            "type.googleapis.com/tycho.evm.uniswap.v3.Events",
        );

        assert!(decoder.is_some());
    }

    #[test]
    fn ignores_unregistered_auxiliary_protocol_message() {
        let decoder = auxiliary_protocol_message_decoder_for(
            &default_auxiliary_protocol_message_decoders_for_protocol_system(
                "future_swap_v1",
                default_family_runtime_registry(),
            ),
            "future_swap_v1",
            "type.googleapis.com/future.swap.Events",
        );

        assert!(decoder.is_none());
    }
}
