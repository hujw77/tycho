use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
};

use async_trait::async_trait;
use tycho_ethereum::rpc::EthereumRpcClient;
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

pub(crate) type AuxiliaryProtocolStateHydrationFuture<'a> =
    Pin<
        Box<
            dyn Future<
                    Output = Result<HashMap<ComponentId, ChainHydratedComponentState>, ExtractionError>,
                > + Send
                + 'a,
        >,
    >;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChainHydratedComponentState {
    pub attributes: HashMap<String, Bytes>,
    pub balances: HashMap<Address, Bytes>,
}

#[async_trait]
pub(crate) trait AuxiliaryProtocolMessageContext: Send + Sync {
    fn extractor_name(&self) -> &str;

    fn chain(&self) -> Chain;

    fn protocol_system(&self) -> &str;

    async fn get_protocol_components(
        &self,
        component_ids: &[ComponentId],
    ) -> Result<HashMap<ComponentId, ProtocolComponent>, ExtractionError>;

    async fn get_protocol_states_at_tip(
        &self,
        component_ids: &[ComponentId],
    ) -> Result<HashMap<ComponentId, HashMap<String, Bytes>>, ExtractionError>;

    fn rpc_client(&self) -> Option<EthereumRpcClient>;

    async fn hydrate_protocol_components_from_chain(
        &self,
        protocol_components: &[ProtocolComponent],
        block_number: u64,
    ) -> Result<HashMap<ComponentId, ChainHydratedComponentState>, ExtractionError>;

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

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuxiliaryProtocolStateHydrator {
    pub protocol_system: &'static str,
    pub hydrate_components_from_chain: for<'a> fn(
        &'a dyn AuxiliaryProtocolMessageContext,
        &'a [ProtocolComponent],
        u64,
    ) -> AuxiliaryProtocolStateHydrationFuture<'a>,
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

pub(crate) fn default_auxiliary_protocol_state_hydrators_for_protocol_system(
    protocol_system: &str,
    registry: FamilyRuntimeRegistry<'_>,
) -> Vec<AuxiliaryProtocolStateHydrator> {
    registry
        .registered_protocol_system_defaults(protocol_system)
        .map(|defaults| defaults.auxiliary_protocol_state_hydrators().to_vec())
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
