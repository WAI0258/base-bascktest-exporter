use std::collections::HashMap;

use base_backtest_exporter::{
    build_resolved_pool_catalog, IndexerMetadataProvider, IndexerPoolMetadata, IndexerTokenMetadata,
    NormalizedProtocol, SourceError, StableTokenEntry, StableTokenList, V3SwapPayloadShape,
};

#[derive(Debug, Default)]
struct MockIndexerProvider {
    pools: HashMap<String, IndexerPoolMetadata>,
    tokens: HashMap<String, IndexerTokenMetadata>,
}

impl MockIndexerProvider {
    fn with_pool(mut self, key: &str, value: IndexerPoolMetadata) -> Self {
        self.pools.insert(key.to_owned(), value);
        self
    }

    fn with_token(mut self, key: &str, value: IndexerTokenMetadata) -> Self {
        self.tokens.insert(key.to_owned(), value);
        self
    }
}

impl IndexerMetadataProvider for MockIndexerProvider {
    fn fetch_pool_metadata(&self, pool_address: &str) -> Result<IndexerPoolMetadata, SourceError> {
        match self.pools.get(pool_address) {
            Some(pool) => Ok(pool.clone()),
            None => Err(SourceError::NotFound {
                resource: "pool",
                address: pool_address.to_owned(),
            }),
        }
    }

    fn fetch_token_metadata(
        &self,
        token_address: &str,
    ) -> Result<IndexerTokenMetadata, SourceError> {
        match self.tokens.get(token_address) {
            Some(token) => Ok(token.clone()),
            None => Err(SourceError::NotFound {
                resource: "token",
                address: token_address.to_owned(),
            }),
        }
    }
}

fn stable_list(addresses: &[&str]) -> StableTokenList {
    StableTokenList {
        version: 1,
        tokens: addresses
            .iter()
            .map(|address| StableTokenEntry {
                address: (*address).to_owned(),
                symbol: "USD".to_owned(),
                name: "USD Token".to_owned(),
            })
            .collect(),
    }
}

fn token(address: &str, symbol: &str, name: &str) -> IndexerTokenMetadata {
    IndexerTokenMetadata {
        address: address.to_owned(),
        decimals: 18,
        symbol: Some(symbol.to_owned()),
        name: Some(name.to_owned()),
    }
}

fn pool(protocol: &str, token0: &str, token1: &str) -> IndexerPoolMetadata {
    IndexerPoolMetadata {
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        protocol: protocol.to_owned(),
        tokens: vec![token0.to_owned(), token1.to_owned()],
        creation_block_number: Some(1234),
        fee: Some(500),
        tick_spacing: Some(10),
    }
}

#[test]
fn catalog_builder_resolves_complete_supported_pool() {
    let pool_address = "0x1111111111111111111111111111111111111111";
    let token0 = "0x2222222222222222222222222222222222222222";
    let token1 = "0x3333333333333333333333333333333333333333";
    let provider = MockIndexerProvider::default()
        .with_pool(pool_address, pool("UniswapV3", token0, token1))
        .with_token(token0, token(token0, "USDC", "USD Coin"))
        .with_token(token1, token(token1, "WETH", "Wrapped Ether"));

    let output = match build_resolved_pool_catalog(
        &provider,
        &[pool_address.to_owned()],
        &stable_list(&["0x2222222222222222222222222222222222222222"]),
    ) {
        Ok(output) => output,
        Err(error) => panic!("catalog build should succeed: {error}"),
    };

    assert_eq!(output.resolved.len(), 1);
    assert!(output.unresolved_stable_side.is_empty());
    assert!(output.unsupported_or_invalid.is_empty());

    let entry = &output.resolved[0];
    assert_eq!(entry.pool_address, pool_address);
    assert_eq!(entry.protocol, NormalizedProtocol::UniswapV3);
    assert_eq!(entry.swap_payload_shape, V3SwapPayloadShape::StandardV3);
    assert!(entry.token0_is_stable);
    assert!(!entry.token1_is_stable);
}

#[test]
fn catalog_builder_matches_allowlist_addresses_case_insensitively() {
    let pool_address = "0x1111111111111111111111111111111111111111";
    let token0 = "0x2222222222222222222222222222222222222222";
    let token1 = "0x3333333333333333333333333333333333333333";
    let provider = MockIndexerProvider::default()
        .with_pool(pool_address, pool("UniswapV3", token0, token1))
        .with_token(token0, token(token0, "USDC", "USD Coin"))
        .with_token(token1, token(token1, "WETH", "Wrapped Ether"));

    let uppercase = "0x2222222222222222222222222222222222222222".to_uppercase();
    let output = match build_resolved_pool_catalog(
        &provider,
        &[pool_address.to_owned()],
        &stable_list(&[uppercase.as_str()]),
    ) {
        Ok(output) => output,
        Err(error) => panic!("catalog build should succeed: {error}"),
    };
    assert_eq!(output.resolved.len(), 1);
    assert!(output.resolved[0].token0_is_stable);
}

#[test]
fn catalog_builder_marks_unresolved_when_allowlist_cannot_determine_stable_side() {
    let pool_address = "0x1111111111111111111111111111111111111111";
    let token0 = "0x2222222222222222222222222222222222222222";
    let token1 = "0x3333333333333333333333333333333333333333";
    let provider = MockIndexerProvider::default()
        .with_pool(pool_address, pool("UniswapV3", token0, token1))
        .with_token(token0, token(token0, "TOKEN0", "Token Zero"))
        .with_token(token1, token(token1, "TOKEN1", "Token One"));

    let output = match build_resolved_pool_catalog(
        &provider,
        &[pool_address.to_owned()],
        &stable_list(&[]),
    ) {
        Ok(output) => output,
        Err(error) => panic!("catalog build should succeed: {error}"),
    };

    assert!(output.resolved.is_empty());
    assert_eq!(output.unresolved_stable_side.len(), 1);
    assert_eq!(
        output.unresolved_stable_side[0].reason,
        "no_tokens_in_stable_allowlist"
    );
}

#[test]
fn catalog_builder_routes_unsupported_and_invalid_pools() {
    let pool_address = "0x1111111111111111111111111111111111111111";
    let token0 = "0x2222222222222222222222222222222222222222";
    let token1 = "0x3333333333333333333333333333333333333333";
    let mut missing_fee_pool = pool("UniswapV3", token0, token1);
    missing_fee_pool.fee = None;

    let provider = MockIndexerProvider::default()
        .with_pool(pool_address, pool("UniswapV2", token0, token1))
        .with_pool(
            "0x4444444444444444444444444444444444444444",
            missing_fee_pool,
        )
        .with_token(token0, token(token0, "USDC", "USD Coin"))
        .with_token(token1, token(token1, "WETH", "Wrapped Ether"));

    let output = match build_resolved_pool_catalog(
        &provider,
        &[
            pool_address.to_owned(),
            "0x4444444444444444444444444444444444444444".to_owned(),
        ],
        &stable_list(&["0x2222222222222222222222222222222222222222"]),
    ) {
        Ok(output) => output,
        Err(error) => panic!("catalog build should succeed: {error}"),
    };

    assert!(output.resolved.is_empty());
    assert_eq!(output.unsupported_or_invalid.len(), 2);
    assert!(
        output.unsupported_or_invalid[0]
            .reason
            .starts_with("unsupported_protocol"),
        "unexpected reason: {}",
        output.unsupported_or_invalid[0].reason
    );
    assert_eq!(output.unsupported_or_invalid[1].reason, "missing_fee_tier");
}
