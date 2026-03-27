mod pools_manifest;
mod raw;
mod shard;
mod state;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{contract::ContractError, source::SourceError};

pub use pools_manifest::export_replay_metadata;
pub use raw::export_raw_range;
pub use state::export_historical_state;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawExportRequest {
    pub run_root: PathBuf,
    pub from_block: u64,
    pub to_block: u64,
    pub shard_size_blocks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawExportResult {
    pub shard_entries: Vec<RawShardManifestEntry>,
    pub totals: RawExportTotals,
    pub skipped_existing_shards: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateExportRequest {
    pub run_root: PathBuf,
    pub validation_stride_targets: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StateExportResult {
    pub state_shards: Vec<StateShardManifestEntry>,
    pub skipped_existing_state_shards: u64,
    pub repaired_state_shards: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataExportRequest {
    pub run_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataExportResult {
    pub resolved_pool_count: u64,
    pub unresolved_pool_count: u64,
    pub stable_token_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawEventCounts {
    pub swap: u64,
    pub mint: u64,
    pub burn: u64,
    pub collect: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawShardFiles {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collect: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawShardDigests {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collect: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawShardManifestEntry {
    pub from_block: u64,
    pub to_block: u64,
    pub counts: RawEventCounts,
    pub files: RawShardFiles,
    #[serde(default)]
    pub digests: RawShardDigests,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateShardGenerationMode {
    IncrementalValidated,
    ExactFallback,
}

impl Default for StateShardGenerationMode {
    fn default() -> Self {
        Self::IncrementalValidated
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateShardManifestEntry {
    pub pool_address: String,
    pub from_block: u64,
    pub to_block: u64,
    pub line_count: u64,
    pub file: String,
    #[serde(default)]
    pub digest: String,
    #[serde(default)]
    pub generation_mode: StateShardGenerationMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RawExportTotals {
    pub event_counts: RawEventCounts,
    pub ignored_non_selected_pool: u64,
    pub ignored_non_target_topic: u64,
    pub dropped_duplicates: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RawEventKind {
    Swap,
    Mint,
    Burn,
    Collect,
}

impl RawEventKind {
    pub(crate) const ALL: [RawEventKind; 4] = [
        RawEventKind::Swap,
        RawEventKind::Mint,
        RawEventKind::Burn,
        RawEventKind::Collect,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RawEventKind::Swap => "swap",
            RawEventKind::Mint => "mint",
            RawEventKind::Burn => "burn",
            RawEventKind::Collect => "collect",
        }
    }
}

impl RawEventCounts {
    pub(crate) fn add_event(&mut self, event: RawEventKind) {
        match event {
            RawEventKind::Swap => self.swap += 1,
            RawEventKind::Mint => self.mint += 1,
            RawEventKind::Burn => self.burn += 1,
            RawEventKind::Collect => self.collect += 1,
        }
    }

    pub(crate) fn add_assign(&mut self, other: &RawEventCounts) {
        self.swap += other.swap;
        self.mint += other.mint;
        self.burn += other.burn;
        self.collect += other.collect;
    }

    pub(crate) fn by_event(&self, event: RawEventKind) -> u64 {
        match event {
            RawEventKind::Swap => self.swap,
            RawEventKind::Mint => self.mint,
            RawEventKind::Burn => self.burn,
            RawEventKind::Collect => self.collect,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("invalid raw export request: {message}")]
    InvalidRequest { message: String },
    #[error("source adapter error: {0}")]
    Source(#[from] SourceError),
    #[error("contract validation error: {0}")]
    Contract(#[from] ContractError),
    #[error("json decode failed for {label}: {source}")]
    JsonDecode {
        label: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("json encode failed for {label}: {source}")]
    JsonEncode {
        label: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to read {path}: {source}")]
    IoRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    IoWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("resume mismatch for shard {from_block}_{to_block}: {message}")]
    ResumeMismatch {
        from_block: u64,
        to_block: u64,
        message: String,
    },
}
