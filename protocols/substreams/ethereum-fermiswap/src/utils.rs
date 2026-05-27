use serde::Deserialize;
use tiny_keccak::{Hasher, Keccak};

pub const ACTIVE_ATTRIBUTE: &str = "active";

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(with = "hex::serde")]
    pub engine_address: Vec<u8>,
    #[serde(with = "hex::serde")]
    pub trader_vault: Vec<u8>,
}

pub fn component_id(base_asset: &[u8], quote_asset: &[u8]) -> String {
    let mut input = Vec::with_capacity(40);
    input.extend_from_slice(base_asset);
    input.extend_from_slice(quote_asset);

    let mut out = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(&input);
    hasher.finalize(&mut out);

    format!("0x{}", hex::encode(out))
}
