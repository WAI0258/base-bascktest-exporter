use std::collections::HashSet;

use thiserror::Error;

use crate::{
    contract::{
        validate_stable_token_list_str, ContractError, StableTokenList, UnresolvedStableSideItem,
        UnresolvedStableSideToken, V3SwapPayloadShape,
    },
    protocol::registry::{resolve_protocol, NormalizedProtocol, ProtocolResolution},
    source::{normalize_evm_address, PoolMetadataProvider, SourceError, TokenMetadataProvider},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTokenRef {
    pub address: String,
    pub decimals: u8,
    pub symbol: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPoolCatalogEntry {
    pub pool_address: String,
    pub protocol: NormalizedProtocol,
    pub token0: ResolvedTokenRef,
    pub token1: ResolvedTokenRef,
    pub fee_tier: u32,
    pub tick_spacing: i32,
    pub creation_block_number: u64,
    pub swap_payload_shape: V3SwapPayloadShape,
    pub token0_is_stable: bool,
    pub token1_is_stable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedOrInvalidPool {
    pub pool_address: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolvedPoolCatalog {
    pub resolved: Vec<ResolvedPoolCatalogEntry>,
    pub unresolved_stable_side: Vec<UnresolvedStableSideItem>,
    pub unsupported_or_invalid: Vec<UnsupportedOrInvalidPool>,
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("stable token contract error: {0}")]
    StableTokenContract(#[from] ContractError),
    #[error("source adapter error: {0}")]
    Source(#[from] SourceError),
}

pub fn build_resolved_pool_catalog_from_json<P, T>(
    pool_metadata_provider: &P,
    token_metadata_provider: &T,
    selected_pool_addresses: &[String],
    stable_tokens_json: &str,
) -> Result<ResolvedPoolCatalog, CatalogError>
where
    P: PoolMetadataProvider,
    T: TokenMetadataProvider,
{
    let stable_tokens = validate_stable_token_list_str(stable_tokens_json)?;
    build_resolved_pool_catalog(
        pool_metadata_provider,
        token_metadata_provider,
        selected_pool_addresses,
        &stable_tokens,
    )
}

pub fn build_resolved_pool_catalog<P, T>(
    pool_metadata_provider: &P,
    token_metadata_provider: &T,
    selected_pool_addresses: &[String],
    stable_tokens: &StableTokenList,
) -> Result<ResolvedPoolCatalog, CatalogError>
where
    P: PoolMetadataProvider,
    T: TokenMetadataProvider,
{
    let stable_allowlist = build_stable_allowlist(stable_tokens)?;
    let mut out = ResolvedPoolCatalog::default();

    for selected_pool in selected_pool_addresses {
        let normalized_pool = match normalize_evm_address("selected_pool_address", selected_pool) {
            Ok(value) => value,
            Err(_) => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: selected_pool.to_owned(),
                    reason: "invalid_pool_address".to_owned(),
                });
                continue;
            }
        };

        let pool = match pool_metadata_provider.fetch_pool_metadata(&normalized_pool) {
            Ok(pool) => pool,
            Err(SourceError::NotFound { .. }) => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: normalized_pool,
                    reason: "pool_not_found".to_owned(),
                });
                continue;
            }
            Err(error) => return Err(CatalogError::Source(error)),
        };

        let protocol = match resolve_protocol(&pool.protocol) {
            ProtocolResolution::Supported(spec) => spec,
            ProtocolResolution::Unsupported { input } => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: pool.address.clone(),
                    reason: format!("unsupported_protocol:{input}"),
                });
                continue;
            }
        };

        if pool.tokens.len() != 2 {
            out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                pool_address: pool.address.clone(),
                reason: format!("invalid_token_count:{}", pool.tokens.len()),
            });
            continue;
        }

        let creation_block_number = match pool.creation_block_number {
            Some(value) => value,
            None => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: pool.address.clone(),
                    reason: "missing_creation_block_number".to_owned(),
                });
                continue;
            }
        };
        let fee_tier = match pool.fee {
            Some(value) => value,
            None => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: pool.address.clone(),
                    reason: "missing_fee_tier".to_owned(),
                });
                continue;
            }
        };
        let tick_spacing = match pool.tick_spacing {
            Some(value) => value,
            None => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: pool.address.clone(),
                    reason: "missing_tick_spacing".to_owned(),
                });
                continue;
            }
        };

        let token0 = match fetch_resolved_token(
            token_metadata_provider,
            &pool.tokens[0],
            creation_block_number,
        )? {
            Some(token) => token,
            None => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: pool.address.clone(),
                    reason: "missing_token0_metadata".to_owned(),
                });
                continue;
            }
        };
        let token1 = match fetch_resolved_token(
            token_metadata_provider,
            &pool.tokens[1],
            creation_block_number,
        )? {
            Some(token) => token,
            None => {
                out.unsupported_or_invalid.push(UnsupportedOrInvalidPool {
                    pool_address: pool.address.clone(),
                    reason: "missing_token1_metadata".to_owned(),
                });
                continue;
            }
        };

        let token0_is_stable = stable_allowlist.contains(&token0.address);
        let token1_is_stable = stable_allowlist.contains(&token1.address);
        if token0_is_stable == token1_is_stable {
            let reason = if token0_is_stable {
                "both_tokens_in_stable_allowlist"
            } else {
                "no_tokens_in_stable_allowlist"
            };
            out.unresolved_stable_side.push(UnresolvedStableSideItem {
                pool_address: pool.address,
                token0: UnresolvedStableSideToken {
                    address: token0.address,
                },
                token1: UnresolvedStableSideToken {
                    address: token1.address,
                },
                reason: reason.to_owned(),
            });
            continue;
        }

        out.resolved.push(ResolvedPoolCatalogEntry {
            pool_address: pool.address,
            protocol: protocol.protocol,
            token0,
            token1,
            fee_tier,
            tick_spacing,
            creation_block_number,
            swap_payload_shape: protocol.swap_payload_shape,
            token0_is_stable,
            token1_is_stable,
        });
    }

    Ok(out)
}

fn build_stable_allowlist(stable_tokens: &StableTokenList) -> Result<HashSet<String>, SourceError> {
    stable_tokens
        .tokens
        .iter()
        .map(|entry| normalize_evm_address("stable_tokens.tokens[].address", &entry.address))
        .collect()
}

fn fetch_resolved_token<T>(
    token_metadata_provider: &T,
    token_address: &str,
    block_number: u64,
) -> Result<Option<ResolvedTokenRef>, CatalogError>
where
    T: TokenMetadataProvider,
{
    let token = match token_metadata_provider.fetch_token_metadata(token_address, block_number) {
        Ok(Some(token)) => token,
        Ok(None) => return Ok(None),
        Err(SourceError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(CatalogError::Source(error)),
    };

    let symbol = token.symbol.trim();
    if symbol.is_empty() {
        return Ok(None);
    }
    let name = token.name.trim();
    if name.is_empty() {
        return Ok(None);
    }

    Ok(Some(ResolvedTokenRef {
        address: token.address,
        decimals: token.decimals,
        symbol: symbol.to_owned(),
        name: name.to_owned(),
    }))
}
