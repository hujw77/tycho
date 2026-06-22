use std::sync::Arc;

use alloy::{primitives::Address, rpc::types::BlockNumberOrTag, sol_types::SolCall};
use async_trait::async_trait;
use tracing::{instrument, warn};
use tycho_common::{
    models::{blockchain::BlockTag, token::Token, Chain},
    traits::{TokenOwnerFinding, TokenPreProcessor},
    Bytes,
};
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    erc20::{decimalsCall, symbolCall},
    rpc::EthereumRpcClient,
    services::token_analyzer::{call_request, map_block_tag},
    BytesCodec,
};

#[derive(Debug, Clone)]
pub struct EthereumTokenPreProcessor {
    rpc: EthereumRpcClient,
    chain: Chain,
}

impl EthereumTokenPreProcessor {
    pub fn new(rpc: &EthereumRpcClient, chain: Chain, _settlement_contract: Address) -> Self {
        EthereumTokenPreProcessor { rpc: rpc.clone(), chain }
    }

    fn decode_symbol_result(token: Address, result: &[u8]) -> String {
        match symbolCall::abi_decode_returns_validate(result) {
            Ok(symbol) => symbol,
            Err(dynamic_err) => {
                if result.len() == 32 {
                    let symbol_end = result
                        .iter()
                        .position(|b| *b == 0)
                        .unwrap_or(result.len());
                    let raw_symbol = &result[..symbol_end];

                    if !raw_symbol.is_empty() {
                        if let Ok(symbol) = std::str::from_utf8(raw_symbol) {
                            return symbol.to_string();
                        }
                    }
                }

                warn!(
                    ?dynamic_err,
                    ?token,
                    "Failed to decode symbol function result, using address as fallback"
                );
                format!("0x{:x}", token)
            }
        }
    }

    async fn call_symbol(&self, token: Address, block: BlockNumberOrTag) -> String {
        let calldata = symbolCall {}.abi_encode();

        let result = match self
            .rpc
            .eth_call(call_request(None, token, calldata), block)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                warn!(?e, ?token, "Failed to call symbol function, using address as fallback");
                return format!("0x{:x}", token);
            }
        };

        Self::decode_symbol_result(token, &result)
    }

    async fn call_symbols(&self, tokens: &[Address], block: BlockNumberOrTag) -> Vec<String> {
        let calldata = symbolCall {}.abi_encode();
        let requests = tokens
            .iter()
            .map(|token| call_request(None, *token, calldata.clone()))
            .collect();

        let results = match self
            .rpc
            .eth_call_many(requests, block.clone())
            .await
        {
            Ok(results) => results,
            Err(e) => {
                warn!(
                    ?e,
                    n_tokens = tokens.len(),
                    "Failed batched symbol calls, falling back to per-token requests"
                );

                let mut symbols = Vec::with_capacity(tokens.len());
                for token in tokens {
                    symbols.push(
                        self.call_symbol(*token, block.clone())
                            .await,
                    );
                }
                return symbols;
            }
        };

        tokens
            .iter()
            .zip(results)
            .map(|(token, result)| Self::decode_symbol_result(*token, &result))
            .collect()
    }

    async fn call_decimals(&self, token: Address, block: BlockNumberOrTag) -> u8 {
        let calldata = decimalsCall {}.abi_encode();

        let result = match self
            .rpc
            .eth_call(call_request(None, token, calldata), block)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                warn!(?e, ?token, "Failed to call decimals function, using default decimals 18");
                return 18;
            }
        };

        match decimalsCall::abi_decode_returns_validate(&result) {
            Ok(decimals) => decimals,
            Err(e) => {
                warn!(
                    ?e,
                    ?token,
                    "Failed to decode decimals function result, using default decimals 18"
                );
                18
            }
        }
    }

    async fn call_decimals_many(&self, tokens: &[Address], block: BlockNumberOrTag) -> Vec<u8> {
        let calldata = decimalsCall {}.abi_encode();
        let requests = tokens
            .iter()
            .map(|token| call_request(None, *token, calldata.clone()))
            .collect();

        let results = match self
            .rpc
            .eth_call_many(requests, block.clone())
            .await
        {
            Ok(results) => results,
            Err(e) => {
                warn!(
                    ?e,
                    n_tokens = tokens.len(),
                    "Failed batched decimals calls, falling back to per-token requests"
                );

                let mut decimals = Vec::with_capacity(tokens.len());
                for token in tokens {
                    decimals.push(
                        self.call_decimals(*token, block.clone())
                            .await,
                    );
                }
                return decimals;
            }
        };

        tokens
            .iter()
            .zip(results)
            .map(|(token, result)| match decimalsCall::abi_decode_returns_validate(&result) {
                Ok(decimals) => decimals,
                Err(e) => {
                    warn!(
                        ?e,
                        ?token,
                        "Failed to decode decimals function result, using default decimals 18"
                    );
                    18
                }
            })
            .collect()
    }
}

#[async_trait]
impl TokenPreProcessor for EthereumTokenPreProcessor {
    #[instrument(skip_all, fields(n_addresses=addresses.len(), block = ?block))]
    async fn get_tokens(
        &self,
        addresses: Vec<Bytes>,
        _token_finder: Arc<dyn TokenOwnerFinding>,
        block: BlockTag,
    ) -> Vec<Token> {
        let block = map_block_tag(block);
        let token_addresses = addresses
            .iter()
            .map(|address| Address::from_bytes(address))
            .collect::<Vec<_>>();
        let symbols = self
            .call_symbols(&token_addresses, block.clone())
            .await;
        let decimals = self
            .call_decimals_many(&token_addresses, block)
            .await;
        let mut tokens_info = Vec::with_capacity(addresses.len());

        for ((address, symbol), decimals) in addresses
            .into_iter()
            .zip(symbols.into_iter())
            .zip(decimals.into_iter())
        {
            tokens_info.push(Token {
                address,
                symbol: symbol
                    .replace('\0', "")
                    .graphemes(true)
                    .take(255)
                    .collect::<String>(),
                decimals: decimals.into(),
                // Defer heavy token analysis to the cron job so new pools do not block block
                // ingestion on state-override eth_call retries.
                tax: 0,
                gas: Vec::new(),
                chain: self.chain,
                quality: 10,
            });
        }

        tokens_info
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::primitives::address;
    use tycho_common::models::token::TokenOwnerStore;

    use super::*;
    use crate::test_fixtures::{TestFixture, TEST_BLOCK_NUMBER, TOKEN_HOLDERS, USDC_STR, WETH_STR};

    const COWSWAP_SETTLEMENT: Address = address!("c9f2e6ea1637E499406986ac50ddC92401ce1f58");

    impl TestFixture {
        fn create_token_preprocessor(&self) -> EthereumTokenPreProcessor {
            // Enable batching so the integration tests exercise the production path.
            let rpc = self.create_rpc_client(true);

            EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, COWSWAP_SETTLEMENT)
        }
    }

    #[tokio::test]
    #[ignore = "require RPC connection"]
    async fn test_call_symbol() {
        let fixture = TestFixture::new();
        let processor = fixture.create_token_preprocessor();

        // Test WETH symbol
        let weth_address = Address::from_str(WETH_STR).expect("Failed to parse WETH address");
        let symbol = processor
            .call_symbol(weth_address, BlockNumberOrTag::Latest)
            .await;
        assert_eq!(symbol, "WETH", "Expected WETH symbol");

        // Test USDC symbol
        let usdc_address = Address::from_str(USDC_STR).expect("Failed to parse USDC address");
        let symbol = processor
            .call_symbol(usdc_address, BlockNumberOrTag::Latest)
            .await;
        assert_eq!(symbol, "USDC", "Expected USDC symbol");
    }

    #[tokio::test]
    #[ignore = "require RPC connection"]
    async fn test_call_decimals() {
        let fixture = TestFixture::new();
        let processor = fixture.create_token_preprocessor();

        // Test WETH decimals (18)
        let weth_address = Address::from_str(WETH_STR).expect("Failed to parse WETH address");
        let decimals = processor
            .call_decimals(weth_address, BlockNumberOrTag::Latest)
            .await;
        assert_eq!(decimals, 18, "Expected WETH to have 18 decimals");

        // Test USDC decimals (6)
        let usdc_address = Address::from_str(USDC_STR).expect("Failed to parse USDC address");
        let decimals = processor
            .call_decimals(usdc_address, BlockNumberOrTag::Latest)
            .await;
        assert_eq!(decimals, 6, "Expected USDC to have 6 decimals");
    }

    #[test]
    fn test_decode_symbol_result_supports_bytes32() {
        let token = address!("9f8f72aa9304c8b593d555f12ef6589cc3a579a2");
        let raw = [
            0x4d, 0x4b, 0x52, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ];

        let symbol = EthereumTokenPreProcessor::decode_symbol_result(token, &raw);

        assert_eq!(symbol, "MKR");
    }

    #[test]
    fn test_decode_symbol_result_falls_back_to_address_for_invalid_bytes32() {
        let token = address!("9f8f72aa9304c8b593d555f12ef6589cc3a579a2");
        let raw = [
            0xff, 0xfe, 0xfd, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ];

        let symbol = EthereumTokenPreProcessor::decode_symbol_result(token, &raw);

        assert_eq!(symbol, format!("0x{:x}", token));
    }

    #[tokio::test]
    #[ignore = "require archive RPC connection"]
    async fn test_get_tokens() {
        let fixture = TestFixture::new();
        let processor = fixture.create_token_preprocessor();

        let tf = TokenOwnerStore::new(TOKEN_HOLDERS.clone());

        let fake_address: &str = "0xA0b86991c7456b36c1d19D4a2e9Eb0cE3606eB48";
        let addresses = vec![
            Bytes::from_str(WETH_STR).unwrap(),
            Bytes::from_str(USDC_STR).unwrap(),
            Bytes::from_str(fake_address).unwrap(),
        ];

        let results = processor
            .get_tokens(addresses, Arc::new(tf), BlockTag::Number(TEST_BLOCK_NUMBER))
            .await;
        assert_eq!(results.len(), 3);
        let relevant_attrs: Vec<(String, u32, u32)> = results
            .iter()
            .map(|t| (t.symbol.clone(), t.decimals, t.quality))
            .collect();
        assert_eq!(
            relevant_attrs,
            vec![
                ("WETH".to_string(), 18, 10),
                ("USDC".to_string(), 6, 10),
                (fake_address.to_lowercase(), 18, 10)
            ]
        );
    }
}
