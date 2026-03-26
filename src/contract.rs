use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const CONTRACT_VERSION: u32 = 1;
pub const RAW_DIR: &str = "raw";
pub const STATE_DIR: &str = "state";
pub const META_FILE: &str = "meta.json";
pub const MANIFEST_FILE: &str = "manifest.json";
pub const CANONICAL_POOL_MANIFEST_FILE: &str = "pool_manifest.json";
pub const STABLE_TOKENS_FILE: &str = "stable_tokens.json";
pub const UNRESOLVED_STABLE_SIDE_REPORT_FILE: &str = "unresolved_stable_side_report.json";
pub const GENERATED_POOLS_FILE: &str = "pools.generated.toml";
pub const RAW_EVENT_DIRS: [&str; 4] = ["swap", "mint", "burn", "collect"];
pub const CURRENT_LPBOT_BASE_SWAP_WORDS: usize = 5;
pub const PANCAKE_V3_SWAP_WORDS: usize = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractView {
    CurrentLpbotBase,
    TargetReplay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventContractStatus {
    Accepted,
    AcceptedWithLpbotBaseDecoderExtension,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V3SwapPayloadShape {
    StandardV3,
    PancakeV3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolCapability {
    pub protocol: &'static str,
    pub swap: EventContractStatus,
    pub mint: EventContractStatus,
    pub burn: EventContractStatus,
    pub collect: EventContractStatus,
    pub note: &'static str,
}

const PROTOCOL_CAPABILITY_MATRIX: [ProtocolCapability; 4] = [
    ProtocolCapability {
        protocol: "UniswapV3",
        swap: EventContractStatus::Accepted,
        mint: EventContractStatus::Accepted,
        burn: EventContractStatus::Accepted,
        collect: EventContractStatus::Accepted,
        note: "Common V3 path.",
    },
    ProtocolCapability {
        protocol: "SushiV3 / SushiswapV3",
        swap: EventContractStatus::Accepted,
        mint: EventContractStatus::Accepted,
        burn: EventContractStatus::Accepted,
        collect: EventContractStatus::Accepted,
        note: "Sushi V3 follows the UniswapV3 event path in base-dex-indexer.",
    },
    ProtocolCapability {
        protocol: "AerodromeV3 / Slipstream",
        swap: EventContractStatus::Accepted,
        mint: EventContractStatus::Accepted,
        burn: EventContractStatus::Accepted,
        collect: EventContractStatus::Accepted,
        note: "AerodromeV3 / Slipstream uses the same V3 event structure.",
    },
    ProtocolCapability {
        protocol: "PancakeV3",
        swap: EventContractStatus::AcceptedWithLpbotBaseDecoderExtension,
        mint: EventContractStatus::Accepted,
        burn: EventContractStatus::Accepted,
        collect: EventContractStatus::Accepted,
        note: "Native swap payload is 7 ABI words; Step 1 freezes the lpbot-base decoder upgrade path.",
    },
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CurrentRawTopicLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub block_hash: String,
    pub log_index: String,
    #[serde(default)]
    pub removed: bool,
    pub block_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TargetRawTopicLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub block_hash: String,
    pub log_index: String,
    #[serde(default)]
    pub removed: bool,
    pub block_timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateLine {
    pub pool_address: String,
    pub block_number: Value,
    pub token0_balance_raw: Value,
    pub token1_balance_raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolManifest {
    pub version: u32,
    pub pools: Vec<PoolManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StableTokenList {
    pub version: u32,
    pub tokens: Vec<StableTokenEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StableTokenEntry {
    pub address: String,
    pub symbol: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnresolvedStableSideReport {
    pub version: u32,
    pub items: Vec<UnresolvedStableSideItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnresolvedStableSideItem {
    pub pool_address: String,
    pub token0: UnresolvedStableSideToken,
    pub token1: UnresolvedStableSideToken,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnresolvedStableSideToken {
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolManifestEntry {
    pub pool_address: String,
    pub protocol: String,
    pub token0: TokenMetadata,
    pub token1: TokenMetadata,
    pub fee_tier: u32,
    pub token0_is_stable: bool,
    pub token1_is_stable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenMetadata {
    pub address: String,
    pub decimals: u8,
}

struct RawFieldView<'a> {
    address: &'a str,
    topics: &'a [String],
    data: &'a str,
    block_number: &'a str,
    transaction_hash: &'a str,
    transaction_index: &'a str,
    block_hash: &'a str,
    log_index: &'a str,
    block_timestamp: Option<&'a str>,
    view: ContractView,
}

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid json in {label}: {source}")]
    Json {
        label: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("missing required path: {path}")]
    MissingPath { path: PathBuf },
    #[error("invalid {field}: {message}")]
    InvalidField {
        field: &'static str,
        message: String,
    },
    #[error("invalid replay layout: {message}")]
    InvalidLayout { message: String },
}

pub fn protocol_capabilities() -> &'static [ProtocolCapability] {
    &PROTOCOL_CAPABILITY_MATRIX
}

pub fn current_lpbot_base_accepts_swap_word_count(word_count: usize) -> bool {
    word_count == CURRENT_LPBOT_BASE_SWAP_WORDS
}

pub fn validate_current_raw_line(line: &str) -> Result<CurrentRawTopicLog, ContractError> {
    let raw = parse_json::<CurrentRawTopicLog>(line, "current raw line")?;
    validate_raw_fields(RawFieldView {
        address: &raw.address,
        topics: &raw.topics,
        data: &raw.data,
        block_number: &raw.block_number,
        transaction_hash: &raw.transaction_hash,
        transaction_index: &raw.transaction_index,
        block_hash: &raw.block_hash,
        log_index: &raw.log_index,
        block_timestamp: raw.block_timestamp.as_deref(),
        view: ContractView::CurrentLpbotBase,
    })?;
    Ok(raw)
}

pub fn validate_target_raw_line(line: &str) -> Result<TargetRawTopicLog, ContractError> {
    let raw = parse_json::<TargetRawTopicLog>(line, "target raw line")?;
    validate_raw_fields(RawFieldView {
        address: &raw.address,
        topics: &raw.topics,
        data: &raw.data,
        block_number: &raw.block_number,
        transaction_hash: &raw.transaction_hash,
        transaction_index: &raw.transaction_index,
        block_hash: &raw.block_hash,
        log_index: &raw.log_index,
        block_timestamp: Some(raw.block_timestamp.as_str()),
        view: ContractView::TargetReplay,
    })?;
    Ok(raw)
}

pub fn validate_state_line_str(line: &str) -> Result<StateLine, ContractError> {
    let state = parse_json::<StateLine>(line, "state line")?;
    validate_address("pool_address", &state.pool_address)?;
    validate_unsigned_value("block_number", &state.block_number)?;
    validate_unsigned_value("token0_balance_raw", &state.token0_balance_raw)?;
    validate_unsigned_value("token1_balance_raw", &state.token1_balance_raw)?;
    Ok(state)
}

pub fn validate_pool_manifest_str(contents: &str) -> Result<PoolManifest, ContractError> {
    let manifest = parse_json::<PoolManifest>(contents, CANONICAL_POOL_MANIFEST_FILE)?;
    validate_contract_version("version", manifest.version)?;
    for pool in &manifest.pools {
        validate_address("pools[].pool_address", &pool.pool_address)?;
        validate_non_empty("pools[].protocol", &pool.protocol)?;
        validate_address("pools[].token0.address", &pool.token0.address)?;
        validate_address("pools[].token1.address", &pool.token1.address)?;
    }
    Ok(manifest)
}

pub fn validate_stable_token_list_str(contents: &str) -> Result<StableTokenList, ContractError> {
    let stable_tokens = parse_json::<StableTokenList>(contents, STABLE_TOKENS_FILE)?;
    validate_contract_version("version", stable_tokens.version)?;
    for token in &stable_tokens.tokens {
        validate_address("tokens[].address", &token.address)?;
        validate_non_empty("tokens[].symbol", &token.symbol)?;
        validate_non_empty("tokens[].name", &token.name)?;
    }
    Ok(stable_tokens)
}

pub fn validate_unresolved_stable_side_report_str(
    contents: &str,
) -> Result<UnresolvedStableSideReport, ContractError> {
    let report =
        parse_json::<UnresolvedStableSideReport>(contents, UNRESOLVED_STABLE_SIDE_REPORT_FILE)?;
    validate_contract_version("version", report.version)?;
    for item in &report.items {
        validate_address("items[].pool_address", &item.pool_address)?;
        validate_address("items[].token0.address", &item.token0.address)?;
        validate_address("items[].token1.address", &item.token1.address)?;
        validate_non_empty("items[].reason", &item.reason)?;
    }
    Ok(report)
}

pub fn validate_replay_root(root: &Path) -> Result<(), ContractError> {
    if !root.is_dir() {
        return Err(ContractError::InvalidLayout {
            message: format!("replay root is not a directory: {}", root.display()),
        });
    }

    require_dir(&root.join(RAW_DIR))?;
    require_dir(&root.join(STATE_DIR))?;
    for event_dir in RAW_EVENT_DIRS {
        require_dir(&root.join(RAW_DIR).join(event_dir))?;
    }

    let meta_path = root.join(META_FILE);
    let manifest_path = root.join(MANIFEST_FILE);
    let pool_manifest_path = root.join(CANONICAL_POOL_MANIFEST_FILE);

    validate_required_json_object_file(&meta_path, META_FILE)?;
    validate_required_json_object_file(&manifest_path, MANIFEST_FILE)?;
    let pool_manifest = read_to_string(&pool_manifest_path)?;
    let _ = validate_pool_manifest_str(&pool_manifest)?;
    Ok(())
}

pub fn detect_v3_swap_payload_shape(data: &str) -> Result<V3SwapPayloadShape, ContractError> {
    let word_count = data_word_count(data)?;
    match word_count {
        CURRENT_LPBOT_BASE_SWAP_WORDS => Ok(V3SwapPayloadShape::StandardV3),
        PANCAKE_V3_SWAP_WORDS => Ok(V3SwapPayloadShape::PancakeV3),
        other => Err(ContractError::InvalidField {
            field: "data",
            message: format!("unsupported V3 swap payload word count: {other}"),
        }),
    }
}

fn parse_json<T>(contents: &str, label: &str) -> Result<T, ContractError>
where
    T: DeserializeOwned,
{
    serde_json::from_str(contents).map_err(|source| ContractError::Json {
        label: label.to_owned(),
        source,
    })
}

fn validate_contract_version(field: &'static str, version: u32) -> Result<(), ContractError> {
    if version != CONTRACT_VERSION {
        return Err(ContractError::InvalidField {
            field,
            message: format!("expected contract version {CONTRACT_VERSION}, got {version}"),
        });
    }
    Ok(())
}

fn validate_raw_fields(raw: RawFieldView<'_>) -> Result<(), ContractError> {
    validate_address("address", raw.address)?;
    if raw.topics.is_empty() {
        return Err(ContractError::InvalidField {
            field: "topics",
            message: "topics cannot be empty".to_owned(),
        });
    }
    for topic in raw.topics {
        validate_fixed_hex("topics[]", topic, 64)?;
    }
    validate_hex_bytes("data", raw.data)?;
    if data_word_count(raw.data)? == 0 {
        return Err(ContractError::InvalidField {
            field: "data",
            message: "ABI payload must contain at least one 32-byte word".to_owned(),
        });
    }
    validate_hex_scalar("blockNumber", raw.block_number)?;
    validate_fixed_hex("transactionHash", raw.transaction_hash, 64)?;
    validate_hex_scalar("transactionIndex", raw.transaction_index)?;
    validate_fixed_hex("blockHash", raw.block_hash, 64)?;
    validate_hex_scalar("logIndex", raw.log_index)?;

    match (raw.view, raw.block_timestamp) {
        (ContractView::CurrentLpbotBase, Some(timestamp)) => {
            validate_hex_scalar("blockTimestamp", timestamp)?;
        }
        (ContractView::CurrentLpbotBase, None) => {}
        (ContractView::TargetReplay, Some(timestamp)) => {
            validate_hex_scalar("blockTimestamp", timestamp)?;
        }
        (ContractView::TargetReplay, None) => {
            return Err(ContractError::InvalidField {
                field: "blockTimestamp",
                message: "target replay contract requires blockTimestamp".to_owned(),
            });
        }
    }
    Ok(())
}

fn require_dir(path: &Path) -> Result<(), ContractError> {
    if !path.is_dir() {
        return Err(ContractError::MissingPath {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn validate_required_json_object_file(path: &Path, label: &str) -> Result<(), ContractError> {
    let contents = read_to_string(path)?;
    let value = parse_json::<Value>(&contents, label)?;
    if !value.is_object() {
        return Err(ContractError::InvalidLayout {
            message: format!("{label} must be a JSON object"),
        });
    }
    Ok(())
}

fn read_to_string(path: &Path) -> Result<String, ContractError> {
    fs::read_to_string(path).map_err(|source| ContractError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), ContractError> {
    if value.trim().is_empty() {
        return Err(ContractError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    Ok(())
}

fn validate_address(field: &'static str, value: &str) -> Result<(), ContractError> {
    validate_prefixed_hex(field, value, 40)
}

fn validate_fixed_hex(
    field: &'static str,
    value: &str,
    nibble_len: usize,
) -> Result<(), ContractError> {
    validate_prefixed_hex(field, value, nibble_len)
}

fn validate_prefixed_hex(
    field: &'static str,
    value: &str,
    nibble_len: usize,
) -> Result<(), ContractError> {
    let digits = value
        .strip_prefix("0x")
        .ok_or_else(|| ContractError::InvalidField {
            field,
            message: "value must start with 0x".to_owned(),
        })?;
    if digits.len() != nibble_len {
        return Err(ContractError::InvalidField {
            field,
            message: format!("expected {nibble_len} hex digits, got {}", digits.len()),
        });
    }
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ContractError::InvalidField {
            field,
            message: "value must contain only ASCII hex digits".to_owned(),
        });
    }
    Ok(())
}

fn validate_hex_scalar(field: &'static str, value: &str) -> Result<(), ContractError> {
    let digits = value
        .strip_prefix("0x")
        .ok_or_else(|| ContractError::InvalidField {
            field,
            message: "value must start with 0x".to_owned(),
        })?;
    if digits.is_empty() {
        return Err(ContractError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ContractError::InvalidField {
            field,
            message: "value must contain only ASCII hex digits".to_owned(),
        });
    }
    Ok(())
}

fn validate_hex_bytes(field: &'static str, value: &str) -> Result<(), ContractError> {
    let digits = value
        .strip_prefix("0x")
        .ok_or_else(|| ContractError::InvalidField {
            field,
            message: "value must start with 0x".to_owned(),
        })?;
    if digits.is_empty() {
        return Err(ContractError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    if digits.len() % 2 != 0 {
        return Err(ContractError::InvalidField {
            field,
            message: "value must contain an even number of hex digits".to_owned(),
        });
    }
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ContractError::InvalidField {
            field,
            message: "value must contain only ASCII hex digits".to_owned(),
        });
    }
    Ok(())
}

fn data_word_count(data: &str) -> Result<usize, ContractError> {
    validate_hex_bytes("data", data)?;
    let digits = data.trim_start_matches("0x");
    if digits.len() % 64 != 0 {
        return Err(ContractError::InvalidField {
            field: "data",
            message: format!(
                "ABI payload length must be a multiple of 64 hex digits, got {}",
                digits.len()
            ),
        });
    }
    Ok(digits.len() / 64)
}

fn validate_unsigned_value(field: &'static str, value: &Value) -> Result<(), ContractError> {
    match value {
        Value::Number(number) => {
            if number.as_u64().is_none() {
                return Err(ContractError::InvalidField {
                    field,
                    message: "numeric value must be an unsigned integer".to_owned(),
                });
            }
            Ok(())
        }
        Value::String(text) => validate_unsigned_string(field, text),
        _ => Err(ContractError::InvalidField {
            field,
            message: "value must be a string or unsigned integer".to_owned(),
        }),
    }
}

fn validate_unsigned_string(field: &'static str, value: &str) -> Result<(), ContractError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ContractError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }

    let digits = if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        if hex.is_empty() {
            return Err(ContractError::InvalidField {
                field,
                message: "hex value cannot be empty".to_owned(),
            });
        }
        if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ContractError::InvalidField {
                field,
                message: "hex string must contain only ASCII hex digits".to_owned(),
            });
        }
        return Ok(());
    } else {
        trimmed
    };

    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ContractError::InvalidField {
            field,
            message: "decimal string must contain only ASCII digits".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        protocol_capabilities, EventContractStatus, CURRENT_LPBOT_BASE_SWAP_WORDS, RAW_EVENT_DIRS,
    };

    #[test]
    fn pancake_swap_is_marked_as_decoder_gap() {
        let pancake = protocol_capabilities()
            .iter()
            .find(|entry| entry.protocol == "PancakeV3");
        match pancake {
            Some(entry) => assert_eq!(
                entry.swap,
                EventContractStatus::AcceptedWithLpbotBaseDecoderExtension
            ),
            None => panic!("expected PancakeV3 capability entry"),
        }
    }

    #[test]
    fn canonical_raw_event_dirs_match_target_layout() {
        assert_eq!(RAW_EVENT_DIRS, ["swap", "mint", "burn", "collect"]);
        assert_eq!(CURRENT_LPBOT_BASE_SWAP_WORDS, 5);
    }
}
