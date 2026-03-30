mod fallback_token;
mod indexer_api;
mod node_rpc;
mod pool_metadata_backfill;

use thiserror::Error;

pub use fallback_token::FallbackTokenMetadataProvider;
pub use indexer_api::{
    HttpIndexerApiClient, IndexerApiAdapter, IndexerHttpClient, IndexerPoolMetadata,
    PoolMetadataProvider, TokenMetadataProvider, TokenMetadataRef,
};
pub use node_rpc::{BaseNodeRpcAdapter, HttpJsonRpcClient, JsonRpcClient};
pub use pool_metadata_backfill::RpcBackfilledPoolMetadataProvider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeaderRef {
    pub block_number: u64,
    pub block_hash: String,
    pub timestamp_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptLogRef {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub log_index: u64,
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionReceiptRef {
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub logs: Vec<ReceiptLogRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockWithReceiptsRef {
    pub header: BlockHeaderRef,
    pub receipts: Vec<TransactionReceiptRef>,
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("invalid address for {field}: {value}")]
    InvalidAddress { field: &'static str, value: String },
    #[error("invalid hex scalar for {field}: {value}")]
    InvalidHexScalar { field: &'static str, value: String },
    #[error("invalid rpc response: {message}")]
    InvalidRpcResponse { message: String },
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("http status {status} for {url}")]
    HttpStatus { url: String, status: u16 },
    #[error("http request failed for {url}: {message}")]
    HttpRequest { url: String, message: String },
    #[error("json decode failed for {label}: {source}")]
    JsonDecode {
        label: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("resource not found: {resource} {address}")]
    NotFound {
        resource: &'static str,
        address: String,
    },
}

pub fn normalize_evm_address(field: &'static str, value: &str) -> Result<String, SourceError> {
    let normalized =
        normalize_prefixed_hex(field, value, 40).map_err(|_| SourceError::InvalidAddress {
            field,
            value: value.to_owned(),
        })?;
    Ok(normalized)
}

pub fn normalize_prefixed_hex(
    field: &'static str,
    value: &str,
    expected_nibbles: usize,
) -> Result<String, SourceError> {
    let trimmed = value.trim();
    let digits = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .ok_or_else(|| SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        })?;
    if digits.len() != expected_nibbles || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        });
    }
    Ok(format!("0x{}", digits.to_ascii_lowercase()))
}

pub fn normalize_hex_bytes(field: &'static str, value: &str) -> Result<String, SourceError> {
    let trimmed = value.trim();
    let digits = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .ok_or_else(|| SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        })?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        });
    }
    if digits.len() % 2 != 0 {
        return Err(SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        });
    }
    Ok(format!("0x{}", digits.to_ascii_lowercase()))
}

pub fn parse_hex_u64(field: &'static str, value: &str) -> Result<u64, SourceError> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .ok_or_else(|| SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        })?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        });
    }
    u64::from_str_radix(digits, 16).map_err(|_| SourceError::InvalidHexScalar {
        field,
        value: value.to_owned(),
    })
}
