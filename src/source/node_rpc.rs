use num_bigint::BigUint;
use num_traits::{Num, ToPrimitive};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};

use super::{
    normalize_evm_address, normalize_hex_bytes, normalize_prefixed_hex, parse_hex_u64,
    BlockHeaderRef, BlockWithReceiptsRef, ReceiptLogRef, SourceError, TokenMetadataProvider,
    TokenMetadataRef, TransactionReceiptRef,
};

const ERC20_BALANCE_OF_SELECTOR: &str = "0x70a08231";
const ERC20_DECIMALS_SELECTOR: &str = "0x313ce567";
const ERC20_SYMBOL_SELECTOR: &str = "0x95d89b41";
const ERC20_NAME_SELECTOR: &str = "0x06fdde03";
const AERODROME_V3_GET_SWAP_FEE_SIGNATURE: &str = "getSwapFee(address)";

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
        let envelope = response.json::<JsonRpcEnvelope<Value>>().map_err(|error| {
            SourceError::HttpRequest {
                url: self.endpoint.clone(),
                message: format!("failed to decode json-rpc envelope for {method}: {error}"),
            }
        })?;
        if let Some(error) = envelope.error {
            return Err(SourceError::Rpc {
                code: error.code,
                message: error.message,
            });
        }
        envelope
            .result
            .ok_or_else(|| SourceError::InvalidRpcResponse {
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
                message: format!("block range must be monotonic: from={from_block}, to={to_block}"),
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

    pub fn eth_call_at_block(
        &self,
        to: &str,
        data: &str,
        block_number: u64,
    ) -> Result<String, SourceError> {
        let normalized_to = normalize_evm_address("eth_call.to", to)?;
        let normalized_data = normalize_hex_bytes("eth_call.data", data)?;
        let block_hex = format!("0x{block_number:x}");
        let raw = self.call_and_decode::<String>(
            "eth_call",
            vec![
                json!({
                    "to": normalized_to,
                    "data": normalized_data,
                }),
                Value::String(block_hex),
            ],
        )?;
        normalize_hex_call_result("eth_call.result", &raw)
    }

    pub fn fetch_erc20_balance(
        &self,
        token_address: &str,
        owner_address: &str,
        block_number: u64,
    ) -> Result<String, SourceError> {
        let token_address = normalize_evm_address("token_address", token_address)?;
        let owner_address = normalize_evm_address("owner_address", owner_address)?;
        let call_data = encode_erc20_balance_of_calldata(&owner_address);
        let response = self.eth_call_at_block(&token_address, &call_data, block_number)?;
        parse_balance_of_result_to_decimal("eth_call.result", &response)
    }

    pub fn fetch_token_metadata(
        &self,
        token_address: &str,
        block_number: u64,
    ) -> Result<Option<TokenMetadataRef>, SourceError> {
        let token_address = normalize_evm_address("token_address", token_address)?;

        let decimals_raw = match map_metadata_call_result(self.eth_call_at_block(
            &token_address,
            encode_erc20_decimals_calldata(),
            block_number,
        ))? {
            Some(value) => value,
            None => return Ok(None),
        };
        let decimals = match decode_abi_uint_u8(&decimals_raw) {
            Some(value) => value,
            None => return Ok(None),
        };

        let symbol_raw = match map_metadata_call_result(self.eth_call_at_block(
            &token_address,
            encode_erc20_symbol_calldata(),
            block_number,
        ))? {
            Some(value) => value,
            None => return Ok(None),
        };
        let symbol = match decode_abi_dynamic_string(&symbol_raw) {
            Some(value) => value,
            None => return Ok(None),
        };

        let name_raw = match map_metadata_call_result(self.eth_call_at_block(
            &token_address,
            encode_erc20_name_calldata(),
            block_number,
        ))? {
            Some(value) => value,
            None => return Ok(None),
        };
        let name = match decode_abi_dynamic_string(&name_raw) {
            Some(value) => value,
            None => return Ok(None),
        };

        Ok(Some(TokenMetadataRef {
            address: token_address,
            decimals,
            symbol,
            name,
        }))
    }

    pub fn fetch_aerodrome_v3_fee(
        &self,
        factory_address: &str,
        pool_address: &str,
        block_number: u64,
    ) -> Result<u32, SourceError> {
        let factory_address = normalize_evm_address("factory_address", factory_address)?;
        let call_data = encode_aerodrome_v3_get_swap_fee_calldata(pool_address)?;
        let response = self.eth_call_at_block(&factory_address, &call_data, block_number)?;
        decode_abi_uint_u32(&response).ok_or_else(|| SourceError::InvalidRpcResponse {
            message: format!(
                "failed to decode AerodromeV3 getSwapFee(address) result for pool {pool_address}"
            ),
        })
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

impl<C> TokenMetadataProvider for BaseNodeRpcAdapter<C>
where
    C: JsonRpcClient,
{
    fn fetch_token_metadata(
        &self,
        token_address: &str,
        block_number: u64,
    ) -> Result<Option<TokenMetadataRef>, SourceError> {
        BaseNodeRpcAdapter::fetch_token_metadata(self, token_address, block_number)
    }
}

fn encode_erc20_balance_of_calldata(owner_address: &str) -> String {
    let owner_digits = owner_address.trim_start_matches("0x");
    format!("{ERC20_BALANCE_OF_SELECTOR}{owner_digits:0>64}")
}

fn encode_erc20_decimals_calldata() -> &'static str {
    ERC20_DECIMALS_SELECTOR
}

fn encode_erc20_symbol_calldata() -> &'static str {
    ERC20_SYMBOL_SELECTOR
}

fn encode_erc20_name_calldata() -> &'static str {
    ERC20_NAME_SELECTOR
}

pub(crate) fn encode_aerodrome_v3_get_swap_fee_calldata(
    pool_address: &str,
) -> Result<String, SourceError> {
    encode_single_address_call(AERODROME_V3_GET_SWAP_FEE_SIGNATURE, pool_address)
}

fn encode_single_address_call(signature: &str, address: &str) -> Result<String, SourceError> {
    let normalized_address = normalize_evm_address("call.address", address)?;
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    let selector = hasher.finalize();
    Ok(format!(
        "0x{}{:0>64}",
        hex::encode(&selector[..4]),
        normalized_address.trim_start_matches("0x")
    ))
}

fn normalize_hex_call_result(field: &'static str, value: &str) -> Result<String, SourceError> {
    let trimmed = value.trim();
    let digits = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .ok_or_else(|| SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        })?;
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        });
    }
    if digits.is_empty() {
        return Ok("0x".to_owned());
    }
    if digits.len() % 2 == 0 {
        return Ok(format!("0x{}", digits.to_ascii_lowercase()));
    }
    Ok(format!("0x0{}", digits.to_ascii_lowercase()))
}

fn map_metadata_call_result(
    result: Result<String, SourceError>,
) -> Result<Option<String>, SourceError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(SourceError::InvalidHexScalar {
            field: "eth_call.result",
            ..
        }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn decode_abi_uint_u8(value: &str) -> Option<u8> {
    let bytes = decode_hex_bytes(value)?;
    if bytes.len() != 32 {
        return None;
    }
    BigUint::from_bytes_be(&bytes).to_u8()
}

fn decode_abi_uint_u32(value: &str) -> Option<u32> {
    let bytes = decode_hex_bytes(value)?;
    if bytes.len() != 32 {
        return None;
    }
    BigUint::from_bytes_be(&bytes).to_u32()
}

fn decode_abi_dynamic_string(value: &str) -> Option<String> {
    let bytes = decode_hex_bytes(value)?;
    if bytes.len() < 64 || bytes.len() % 32 != 0 {
        return None;
    }

    let offset = decode_abi_word_as_usize(&bytes[0..32])?;
    if offset % 32 != 0 {
        return None;
    }

    let len_word_end = offset.checked_add(32)?;
    if len_word_end > bytes.len() {
        return None;
    }
    let data_len = decode_abi_word_as_usize(&bytes[offset..len_word_end])?;

    let data_start = len_word_end;
    let data_end = data_start.checked_add(data_len)?;
    if data_end > bytes.len() {
        return None;
    }

    let padded_len = data_len.checked_add(31)? / 32 * 32;
    let padded_end = data_start.checked_add(padded_len)?;
    if padded_end > bytes.len() {
        return None;
    }

    let string_bytes = &bytes[data_start..data_end];
    let string = std::str::from_utf8(string_bytes).ok()?.trim();
    if string.is_empty() {
        return None;
    }
    Some(string.to_owned())
}

fn decode_hex_bytes(value: &str) -> Option<Vec<u8>> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    if digits.is_empty() || digits.len() % 2 != 0 {
        return None;
    }
    hex::decode(digits).ok()
}

fn decode_abi_word_as_usize(word: &[u8]) -> Option<usize> {
    if word.len() != 32 {
        return None;
    }
    let width = std::mem::size_of::<usize>();
    if word[..(32 - width)].iter().any(|byte| *byte != 0) {
        return None;
    }

    let mut out = 0usize;
    for byte in &word[(32 - width)..] {
        out = out.checked_mul(256)?;
        out = out.checked_add(usize::from(*byte))?;
    }
    Some(out)
}

fn parse_balance_of_result_to_decimal(
    field: &'static str,
    value: &str,
) -> Result<String, SourceError> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .ok_or_else(|| SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        })?;
    if digits.is_empty()
        || digits.len() > 64
        || !digits.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        });
    }
    let balance =
        BigUint::from_str_radix(digits, 16).map_err(|_| SourceError::InvalidHexScalar {
            field,
            value: value.to_owned(),
        })?;
    Ok(balance.to_str_radix(10))
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
