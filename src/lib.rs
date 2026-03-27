pub mod catalog;
pub mod cli;
pub mod contract;
pub mod export;
pub mod protocol;
pub mod source;

pub use catalog::{
    build_resolved_pool_catalog, build_resolved_pool_catalog_from_json, CatalogError,
    ResolvedPoolCatalog, ResolvedPoolCatalogEntry, ResolvedTokenRef, UnsupportedOrInvalidPool,
};

pub use contract::{
    current_lpbot_base_accepts_swap_word_count, protocol_capabilities, validate_current_raw_line,
    validate_pool_manifest_str, validate_replay_root, validate_stable_token_list_str,
    validate_state_line_str, validate_target_raw_line, validate_unresolved_stable_side_report_str,
    ContractError, CurrentRawTopicLog, EventContractStatus, PoolManifest, PoolManifestEntry,
    StableTokenEntry, StableTokenList, StateLine, TargetRawTopicLog, TokenMetadata,
    UnresolvedStableSideItem, UnresolvedStableSideReport, UnresolvedStableSideToken,
    V3SwapPayloadShape, CANONICAL_POOL_MANIFEST_FILE, CONTRACT_VERSION, GENERATED_POOLS_FILE,
    MANIFEST_FILE, META_FILE, RAW_DIR, RAW_EVENT_DIRS, STABLE_TOKENS_FILE, STATE_DIR,
    UNRESOLVED_STABLE_SIDE_REPORT_FILE,
};

pub use export::{
    export_historical_state, export_raw_range, export_replay_metadata, ExportError,
    MetadataExportRequest, MetadataExportResult, RawEventCounts, RawExportRequest, RawExportResult,
    RawExportTotals, RawShardDigests, RawShardFiles, RawShardManifestEntry, StateExportRequest,
    StateExportResult, StateShardGenerationMode, StateShardManifestEntry,
};

pub use protocol::registry::{
    resolve_protocol, NormalizedProtocol, ProtocolResolution, RawEventFamily, SupportedProtocolSpec,
};

pub use source::{
    normalize_evm_address, parse_hex_u64, BaseNodeRpcAdapter, BlockHeaderRef, BlockWithReceiptsRef,
    HttpIndexerApiClient, HttpJsonRpcClient, IndexerApiAdapter, IndexerHttpClient,
    IndexerMetadataProvider, IndexerPoolMetadata, IndexerTokenMetadata, JsonRpcClient,
    ReceiptLogRef, SourceError, TransactionReceiptRef,
};
