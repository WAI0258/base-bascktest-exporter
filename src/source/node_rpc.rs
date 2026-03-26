use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{
    normalize_evm_address, normalize_prefixed_hex, parse_hex_u64, BlockHeaderRef,
    BlockWithReceiptsRef, ReceiptLogRef, SourceError, TransactionReceiptRef,
};

pub trait JsonRpcClient {
    fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, SourceError>;
}

#[derive(Debug, Clone)]
pub struct HttpJsonRpcClient {
    endpoint: String,
    client: reqwest::blocking::Client,
}

impl HttpJsonRpcClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: reqwest::blocking::Client::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcErrorPayload {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct JsonRpcEnvelope<T> {
    result: Option<T>,
    error: Option<JsonRpcErrorPayload>,
}

impl JsonRpcClient for HttpJsonRpcClient {
    fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, SourceError> {
        let request_body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response = self
            .client
            .post(&self.endpoint)
            .json(&request_body)
            .send()
            .map_err(|error| SourceError::HttpRequest {
                url: self.endpoint.clone(),
                message: error.to_string(),
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(SourceError::HttpStatus {
                url: self.endpoint.clone(),
                status: status.as_u16(),
            });
        }
        let envelope = response
            .json::<JsonRpcEnvelope<Value>>()
            .map_err(|error| SourceError::HttpRequest {
                url: self.endpoint.clone(),
                message: format!("failed to decode json-rpc envelope for {method}: {error}"),
            })?;
        if let Some(error) = envelope.error {
            return Err(SourceError::Rpc {
                code: error.code,
                message: error.message,
            });
        }
        envelope.result.ok_or_else(|| SourceError::InvalidRpcResponse {
            message: format!("json-rpc method {method} missing result"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct BaseNodeRpcAdapter<C> {
    client: C,
}

impl BaseNodeRpcAdapter<HttpJsonRpcClient> {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            client: HttpJsonRpcClient::new(endpoint),
        }
    }
}

impl<C> BaseNodeRpcAdapter<C>
where
    C: JsonRpcClient,
{
    pub fn with_client(client: C) -> Self {
        Self { client }
    }

    pub fn fetch_block_header(&self, block_number: u64) -> Result<BlockHeaderRef, SourceError> {
        let block_hex = format!("0x{block_number:x}");
        let raw = self.call_and_decode::<Option<RpcBlock>>(
            "eth_getBlockByNumber",
            vec![Value::String(block_hex), Value::Bool(false)],
        )?;
        let block = raw.ok_or_else(|| SourceError::NotFound {
            resource: "block",
            address: format!("0x{block_number:x}"),
        })?;
        Ok(BlockHeaderRef {
            block_number: parse_hex_u64("block.number", &block.number)?,
            block_hash: normalize_prefixed_hex("block.hash", &block.hash, 64)?,
            timestamp_secs: parse_hex_u64("block.timestamp", &block.timestamp)?,
        })
    }

    pub fn fetch_block_receipts(
        &self,
        block_number: u64,
    ) -> Result<Vec<TransactionReceiptRef>, SourceError> {
        let block_hex = format!("0x{block_number:x}");
        let receipts = self.call_and_decode::<Vec<RpcReceipt>>(
            "eth_getBlockReceipts",
            vec![Value::String(block_hex)],
        )?;
        receipts
            .into_iter()
            .map(TransactionReceiptRef::try_from)
            .collect()
    }

    pub fn fetch_block_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<BlockWithReceiptsRef>, SourceError> {
        if from_block > to_block {
            return Err(SourceError::InvalidRpcResponse {
                message: format!(
                    "block range must be monotonic: from={from_block}, to={to_block}"
                ),
            });
        }

        let mut out = Vec::new();
        for block_number in from_block..=to_block {
            let header = self.fetch_block_header(block_number)?;
            let receipts = self.fetch_block_receipts(block_number)?;
            for receipt in &receipts {
                if receipt.block_number != header.block_number {
                    return Err(SourceError::InvalidRpcResponse {
                        message: format!(
                            "receipt block number {} mismatches header {}",
                            receipt.block_number, header.block_number
                        ),
                    });
                }
                if receipt.block_hash != header.block_hash {
                    return Err(SourceError::InvalidRpcResponse {
                        message: format!(
                            "receipt block hash {} mismatches header {}",
                            receipt.block_hash, header.block_hash
                        ),
                    });
                }
            }
            out.push(BlockWithReceiptsRef { header, receipts });
        }
        Ok(out)
    }

    fn call_and_decode<T>(&self, method: &str, params: Vec<Value>) -> Result<T, SourceError>
    where
        T: DeserializeOwned,
    {
        let result = self.client.call(method, params)?;
        serde_json::from_value::<T>(result).map_err(|source| SourceError::JsonDecode {
            label: format!("json-rpc result {method}"),
            source,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
    number: String,
    hash: String,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcReceipt {
    transaction_hash: String,
    transaction_index: String,
    block_number: String,
    block_hash: String,
    logs: Vec<RpcLog>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
    log_index: String,
    #[serde(default)]
    removed: bool,
}

impl TryFrom<RpcReceipt> for TransactionReceiptRef {
    type Error = SourceError;

    fn try_from(value: RpcReceipt) -> Result<Self, Self::Error> {
        let logs = value
            .logs
            .into_iter()
            .map(ReceiptLogRef::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            transaction_hash: normalize_prefixed_hex(
                "receipt.transactionHash",
                &value.transaction_hash,
                64,
            )?,
            transaction_index: parse_hex_u64("receipt.transactionIndex", &value.transaction_index)?,
            block_number: parse_hex_u64("receipt.blockNumber", &value.block_number)?,
            block_hash: normalize_prefixed_hex("receipt.blockHash", &value.block_hash, 64)?,
            logs,
        })
    }
}

impl TryFrom<RpcLog> for ReceiptLogRef {
    type Error = SourceError;

    fn try_from(value: RpcLog) -> Result<Self, Self::Error> {
        let topics = value
            .topics
            .iter()
            .map(|topic| normalize_prefixed_hex("log.topics[]", topic, 64))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            address: normalize_evm_address("log.address", &value.address)?,
            topics,
            data: value.data,
            log_index: parse_hex_u64("log.logIndex", &value.log_index)?,
            removed: value.removed,
        })
    }
}
