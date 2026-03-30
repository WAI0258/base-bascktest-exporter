use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;

use super::{normalize_evm_address, SourceError};

pub trait IndexerHttpClient {
    fn get_json(&self, url: &str) -> Result<Value, SourceError>;
}

#[derive(Debug, Clone)]
pub struct HttpIndexerApiClient {
    client: reqwest::blocking::Client,
}

impl HttpIndexerApiClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
        }
    }
}

impl Default for HttpIndexerApiClient {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexerHttpClient for HttpIndexerApiClient {
    fn get_json(&self, url: &str) -> Result<Value, SourceError> {
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|error| SourceError::HttpRequest {
                url: url.to_owned(),
                message: error.to_string(),
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(SourceError::HttpStatus {
                url: url.to_owned(),
                status: status.as_u16(),
            });
        }
        response
            .json::<Value>()
            .map_err(|error| SourceError::HttpRequest {
                url: url.to_owned(),
                message: format!("failed to decode indexer json: {error}"),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexerPoolMetadata {
    pub address: String,
    pub protocol: String,
    pub tokens: Vec<String>,
    pub factory_address: Option<String>,
    pub creation_block_number: Option<u64>,
    pub fee: Option<u32>,
    pub tick_spacing: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenMetadataRef {
    pub address: String,
    pub decimals: u8,
    pub symbol: String,
    pub name: String,
}

pub trait PoolMetadataProvider {
    fn fetch_pool_metadata(&self, pool_address: &str) -> Result<IndexerPoolMetadata, SourceError>;
}

pub trait TokenMetadataProvider {
    fn fetch_token_metadata(
        &self,
        token_address: &str,
        block_number: u64,
    ) -> Result<Option<TokenMetadataRef>, SourceError>;
}

#[derive(Debug, Clone)]
pub struct IndexerApiAdapter<C> {
    base_url: String,
    client: C,
}

impl IndexerApiAdapter<HttpIndexerApiClient> {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: HttpIndexerApiClient::new(),
        }
    }
}

impl<C> IndexerApiAdapter<C>
where
    C: IndexerHttpClient,
{
    pub fn with_client(base_url: impl Into<String>, client: C) -> Self {
        Self {
            base_url: base_url.into(),
            client,
        }
    }

    fn fetch_wrapped<T>(
        &self,
        resource: &'static str,
        address: &str,
        suffix: &str,
    ) -> Result<T, SourceError>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), suffix);
        let payload = self.client.get_json(&url).map_err(|error| match error {
            SourceError::HttpStatus { status: 404, .. } => SourceError::NotFound {
                resource,
                address: address.to_owned(),
            },
            other => other,
        })?;
        serde_json::from_value::<T>(payload).map_err(|source| SourceError::JsonDecode {
            label: format!("indexer payload {resource} {address}"),
            source,
        })
    }
}

impl<C> PoolMetadataProvider for IndexerApiAdapter<C>
where
    C: IndexerHttpClient,
{
    fn fetch_pool_metadata(&self, pool_address: &str) -> Result<IndexerPoolMetadata, SourceError> {
        let normalized_pool = normalize_evm_address("pool_address", pool_address)?;
        let payload = self.fetch_wrapped::<PoolEnvelope>(
            "pool",
            &normalized_pool,
            &format!("pools/{normalized_pool}"),
        )?;
        let pool = payload.pool;
        let tokens = pool
            .tokens
            .iter()
            .map(|token| normalize_evm_address("pool.tokens[]", token))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(IndexerPoolMetadata {
            address: normalize_evm_address("pool.address", &pool.address)?,
            protocol: pool.protocol,
            tokens,
            factory_address: pool
                .factory_address
                .as_deref()
                .map(|address| normalize_evm_address("pool.factory_address", address))
                .transpose()?,
            creation_block_number: pool.creation_block_number,
            fee: pool.fee,
            tick_spacing: pool.tick_spacing,
        })
    }
}

#[derive(Debug, Deserialize)]
struct PoolEnvelope {
    pool: PoolResponseDto,
}

#[derive(Debug, Deserialize)]
struct PoolResponseDto {
    address: String,
    protocol: String,
    tokens: Vec<String>,
    factory_address: Option<String>,
    creation_block_number: Option<u64>,
    fee: Option<u32>,
    tick_spacing: Option<i32>,
}
