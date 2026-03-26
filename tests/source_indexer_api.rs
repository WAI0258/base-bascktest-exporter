use std::collections::HashMap;

use base_backtest_exporter::{
    IndexerApiAdapter, IndexerHttpClient, IndexerMetadataProvider, SourceError,
};
use serde_json::{json, Value};

#[derive(Debug, Default)]
struct MockIndexerHttpClient {
    responses: HashMap<String, Value>,
}

impl MockIndexerHttpClient {
    fn with_response(mut self, url: &str, payload: Value) -> Self {
        self.responses.insert(url.to_owned(), payload);
        self
    }
}

impl IndexerHttpClient for MockIndexerHttpClient {
    fn get_json(&self, url: &str) -> Result<Value, SourceError> {
        self.responses
            .get(url)
            .cloned()
            .ok_or_else(|| SourceError::HttpStatus {
                url: url.to_owned(),
                status: 404,
            })
    }
}

#[test]
fn indexer_adapter_normalizes_pool_and_token_payloads() {
    let base_url = "http://indexer.local";
    let pool_address = "0x1111111111111111111111111111111111111111";
    let token0 = "0x2222222222222222222222222222222222222222";
    let token1 = "0x3333333333333333333333333333333333333333";
    let client = MockIndexerHttpClient::default()
        .with_response(
            &format!("{base_url}/pools/{pool_address}"),
            json!({
                "pool": {
                    "address": "0x1111111111111111111111111111111111111111",
                    "protocol": "UniswapV3",
                    "tokens": [token0, token1],
                    "creation_block_number": 123,
                    "fee": 500,
                    "tick_spacing": 10
                }
            }),
        )
        .with_response(
            &format!("{base_url}/tokens/{token0}"),
            json!({
                "token": {
                    "address": token0,
                    "decimals": 6,
                    "symbol": "USDC",
                    "name": "USD Coin"
                }
            }),
        );
    let adapter = IndexerApiAdapter::with_client(base_url, client);

    let pool = match adapter.fetch_pool_metadata("0x1111111111111111111111111111111111111111") {
        Ok(pool) => pool,
        Err(error) => panic!("pool fetch should succeed: {error}"),
    };
    assert_eq!(pool.address, pool_address);
    assert_eq!(pool.tokens, vec![token0.to_owned(), token1.to_owned()]);
    assert_eq!(pool.protocol, "UniswapV3");
    assert_eq!(pool.creation_block_number, Some(123));
    assert_eq!(pool.fee, Some(500));
    assert_eq!(pool.tick_spacing, Some(10));

    let token = match adapter.fetch_token_metadata("0x2222222222222222222222222222222222222222") {
        Ok(token) => token,
        Err(error) => panic!("token fetch should succeed: {error}"),
    };
    assert_eq!(token.address, token0);
    assert_eq!(token.decimals, 6);
    assert_eq!(token.symbol.as_deref(), Some("USDC"));
    assert_eq!(token.name.as_deref(), Some("USD Coin"));
}

#[test]
fn indexer_adapter_maps_missing_resources_and_invalid_address_errors() {
    let base_url = "http://indexer.local";
    let client = MockIndexerHttpClient::default();
    let adapter = IndexerApiAdapter::with_client(base_url, client);

    let missing = match adapter.fetch_pool_metadata("0x1111111111111111111111111111111111111111") {
        Ok(_) => panic!("missing pool must fail"),
        Err(error) => error,
    };
    assert!(
        matches!(missing, SourceError::NotFound { resource: "pool", .. }),
        "unexpected error: {missing}"
    );

    let bad_address = match adapter.fetch_token_metadata("0x1234") {
        Ok(_) => panic!("bad token address must fail"),
        Err(error) => error,
    };
    assert!(
        matches!(bad_address, SourceError::InvalidAddress { .. }),
        "unexpected error: {bad_address}"
    );
}
