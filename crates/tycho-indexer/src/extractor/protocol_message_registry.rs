use crate::extractor::{family_registry, ExtractionError};

pub(crate) enum AuxiliaryProtocolMessage {
    UniswapV3Events(crate::extractor::uniswap_v3_stream::Events),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuxiliaryProtocolMessageDecoder {
    pub protocol_system: &'static str,
    pub type_url_suffix: &'static str,
    pub decode: fn(&[u8]) -> Result<AuxiliaryProtocolMessage, ExtractionError>,
}

pub(crate) fn decode_auxiliary_protocol_message_with_decoders(
    decoders: &[AuxiliaryProtocolMessageDecoder],
    protocol_system: &str,
    type_url: &str,
    value: &[u8],
) -> Result<Option<AuxiliaryProtocolMessage>, ExtractionError> {
    let Some(decoder) = decoders.iter().find(|decoder| {
        decoder.protocol_system == protocol_system && type_url.ends_with(decoder.type_url_suffix)
    }) else {
        return Ok(None);
    };

    (decoder.decode)(value).map(Some)
}

pub(crate) fn decode_auxiliary_protocol_message(
    protocol_system: &str,
    type_url: &str,
    value: &[u8],
) -> Result<Option<AuxiliaryProtocolMessage>, ExtractionError> {
    for family in family_registry::default_family_runtime_specs() {
        if let Some(decoded) = decode_auxiliary_protocol_message_with_decoders(
            family.auxiliary_protocol_message_decoders,
            protocol_system,
            type_url,
            value,
        )? {
            return Ok(Some(decoded));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use crate::extractor::uniswap_v3_stream;

    #[test]
    fn decodes_registered_uniswap_v3_auxiliary_message() {
        let events = uniswap_v3_stream::Events::default();
        let decoded = decode_auxiliary_protocol_message(
            "uniswap_v3",
            "type.googleapis.com/tycho.evm.uniswap.v3.Events",
            &events.encode_to_vec(),
        )
        .expect("registered decoder should succeed");

        assert!(matches!(
            decoded,
            Some(AuxiliaryProtocolMessage::UniswapV3Events(_))
        ));
    }

    #[test]
    fn ignores_unregistered_auxiliary_protocol_message() {
        let decoded = decode_auxiliary_protocol_message(
            "future_swap_v1",
            "type.googleapis.com/future.swap.Events",
            &[],
        )
        .expect("unknown protocol should be ignored");

        assert!(decoded.is_none());
    }
}
