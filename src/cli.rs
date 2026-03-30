use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use clap::{Args, Parser, Subcommand};
use thiserror::Error;

use crate::{
    build_resolved_pool_catalog, export_historical_state, export_raw_range, export_replay_metadata,
    validate_replay_root, validate_stable_token_list_str, validate_token_override_list_str,
    BaseNodeRpcAdapter, CatalogError, ContractError, ExportError, FallbackTokenMetadataProvider,
    IndexerApiAdapter, JsonRpcClient, MetadataExportRequest, MetadataExportResult,
    PoolMetadataProvider, RawExportRequest, RawExportResult, RawExportTotals, SourceError,
    StableTokenList, StateExportRequest, StateExportResult, TokenMetadataRef,
    UnsupportedOrInvalidPool,
};

#[derive(Debug, Parser)]
#[command(
    name = "base-backtest-exporter",
    version,
    about = "Base replay dataset exporter"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Export(ExportArgs),
    Verify(VerifyArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ExportArgs {
    #[arg(long)]
    pub run_root: PathBuf,
    #[arg(long)]
    pub rpc_url: String,
    #[arg(long)]
    pub indexer_url: String,
    #[arg(long)]
    pub selected_pools_file: PathBuf,
    #[arg(long)]
    pub stable_tokens_file: PathBuf,
    #[arg(long)]
    pub token_overrides_file: Option<PathBuf>,
    #[arg(long)]
    pub from_block: u64,
    #[arg(long)]
    pub to_block: u64,
    #[arg(long)]
    pub shard_size_blocks: u64,
    #[arg(long)]
    pub validation_stride_targets: u64,
}

#[derive(Debug, Clone, Args)]
pub struct VerifyArgs {
    #[arg(long)]
    pub run_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportSummary {
    pub run_root: PathBuf,
    pub resolved_pool_count: u64,
    pub unresolved_pool_count: u64,
    pub unsupported_pool_count: u64,
    pub unsupported_pool_details: Vec<UnsupportedOrInvalidPool>,
    pub raw_totals: RawExportTotals,
    pub raw_skipped_existing_shards: u64,
    pub state_shard_count: u64,
    pub skipped_existing_state_shards: u64,
    pub repaired_state_shards: u64,
    pub metadata_resolved_pool_count: u64,
    pub metadata_unresolved_pool_count: u64,
    pub metadata_stable_token_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifySummary {
    pub run_root: PathBuf,
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("failed to read {path}: {source}")]
    IoRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid selected pool address at {path}:{line}: {value} ({source})")]
    InvalidSelectedPoolAddress {
        path: PathBuf,
        line: usize,
        value: String,
        #[source]
        source: SourceError,
    },
    #[error("stable token contract error: {0}")]
    Contract(#[from] ContractError),
    #[error("catalog resolution error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("export error: {0}")]
    Export(#[from] ExportError),
}

pub fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    run_with_cli(cli)
}

pub fn run_with_cli(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Export(args) => {
            let _ = run_export_command(&args)?;
        }
        Command::Verify(args) => {
            let _ = run_verify_command(&args)?;
        }
    }
    Ok(())
}

pub fn run_export_command(args: &ExportArgs) -> Result<ExportSummary, CliError> {
    let indexer_adapter = IndexerApiAdapter::new(args.indexer_url.clone());
    let node_adapter = BaseNodeRpcAdapter::new(args.rpc_url.clone());
    run_export_with_adapters(args, &indexer_adapter, &node_adapter)
}

pub fn run_export_with_adapters<P, C>(
    args: &ExportArgs,
    pool_metadata_provider: &P,
    node_adapter: &BaseNodeRpcAdapter<C>,
) -> Result<ExportSummary, CliError>
where
    P: PoolMetadataProvider,
    C: JsonRpcClient,
{
    let selected_pools = read_selected_pools_file(&args.selected_pools_file)?;
    let stable_tokens = read_stable_tokens_file(&args.stable_tokens_file)?;
    let token_overrides = read_token_overrides_file(args.token_overrides_file.as_deref())?;

    let token_provider = FallbackTokenMetadataProvider::new(node_adapter, token_overrides);
    let pool_provider = crate::RpcBackfilledPoolMetadataProvider::new(
        pool_metadata_provider,
        node_adapter,
        args.to_block,
    );
    let catalog = build_resolved_pool_catalog(
        &pool_provider,
        &token_provider,
        &selected_pools,
        &stable_tokens,
    )?;
    if catalog.resolved.is_empty() {
        return Err(CliError::InvalidRequest {
            message: "resolved pool catalog is empty; export requires at least one resolved pool"
                .to_owned(),
        });
    }

    let raw_result = export_raw_range(
        &RawExportRequest {
            run_root: args.run_root.clone(),
            from_block: args.from_block,
            to_block: args.to_block,
            shard_size_blocks: args.shard_size_blocks,
        },
        &catalog,
        node_adapter,
    )?;

    let state_result = export_historical_state(
        &StateExportRequest {
            run_root: args.run_root.clone(),
            validation_stride_targets: args.validation_stride_targets,
        },
        &catalog,
        node_adapter,
    )?;

    let metadata_result = export_replay_metadata(
        &MetadataExportRequest {
            run_root: args.run_root.clone(),
        },
        &catalog,
        &stable_tokens,
    )?;

    validate_replay_root(&args.run_root)?;

    let summary = build_summary(args, &catalog, raw_result, state_result, metadata_result);
    print_export_summary(&summary);
    Ok(summary)
}

pub fn run_verify_command(args: &VerifyArgs) -> Result<VerifySummary, CliError> {
    validate_replay_root(&args.run_root)?;
    let summary = VerifySummary {
        run_root: args.run_root.clone(),
    };
    print_verify_summary(&summary);
    Ok(summary)
}

pub fn read_selected_pools_file(path: &Path) -> Result<Vec<String>, CliError> {
    let contents = fs::read_to_string(path).map_err(|source| CliError::IoRead {
        path: path.to_path_buf(),
        source,
    })?;

    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();

    for (idx, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let normalized =
            crate::normalize_evm_address("selected_pools_file", trimmed).map_err(|source| {
                CliError::InvalidSelectedPoolAddress {
                    path: path.to_path_buf(),
                    line: idx + 1,
                    value: trimmed.to_owned(),
                    source,
                }
            })?;

        if !seen.insert(normalized.clone()) {
            return Err(CliError::InvalidRequest {
                message: format!(
                    "duplicate pool address in selected_pools_file after normalization: {normalized}"
                ),
            });
        }

        out.push(normalized);
    }

    Ok(out)
}

fn read_stable_tokens_file(path: &Path) -> Result<StableTokenList, CliError> {
    let contents = fs::read_to_string(path).map_err(|source| CliError::IoRead {
        path: path.to_path_buf(),
        source,
    })?;
    let stable_tokens = validate_stable_token_list_str(&contents)?;
    Ok(stable_tokens)
}

fn read_token_overrides_file(
    path: Option<&Path>,
) -> Result<HashMap<String, TokenMetadataRef>, CliError> {
    let Some(path) = path else {
        return Ok(HashMap::new());
    };

    let contents = fs::read_to_string(path).map_err(|source| CliError::IoRead {
        path: path.to_path_buf(),
        source,
    })?;
    let token_overrides = validate_token_override_list_str(&contents)?;

    let mut map = HashMap::<String, TokenMetadataRef>::new();
    for token in token_overrides.tokens {
        let normalized =
            crate::normalize_evm_address("token_overrides.tokens[].address", &token.address)
                .map_err(|source| CliError::InvalidRequest {
                    message: format!(
                        "invalid token_overrides address {} ({source})",
                        token.address
                    ),
                })?;

        map.insert(
            normalized.clone(),
            TokenMetadataRef {
                address: normalized,
                decimals: token.decimals,
                symbol: token.symbol.trim().to_owned(),
                name: token.name.trim().to_owned(),
            },
        );
    }

    Ok(map)
}

fn build_summary(
    args: &ExportArgs,
    catalog: &crate::ResolvedPoolCatalog,
    raw_result: RawExportResult,
    state_result: StateExportResult,
    metadata_result: MetadataExportResult,
) -> ExportSummary {
    ExportSummary {
        run_root: args.run_root.clone(),
        resolved_pool_count: catalog.resolved.len() as u64,
        unresolved_pool_count: catalog.unresolved_stable_side.len() as u64,
        unsupported_pool_count: catalog.unsupported_or_invalid.len() as u64,
        unsupported_pool_details: catalog.unsupported_or_invalid.clone(),
        raw_totals: raw_result.totals,
        raw_skipped_existing_shards: raw_result.skipped_existing_shards,
        state_shard_count: state_result.state_shards.len() as u64,
        skipped_existing_state_shards: state_result.skipped_existing_state_shards,
        repaired_state_shards: state_result.repaired_state_shards,
        metadata_resolved_pool_count: metadata_result.resolved_pool_count,
        metadata_unresolved_pool_count: metadata_result.unresolved_pool_count,
        metadata_stable_token_count: metadata_result.stable_token_count,
    }
}

fn print_export_summary(summary: &ExportSummary) {
    for line in export_summary_lines(summary) {
        println!("{line}");
    }
}

fn export_summary_lines(summary: &ExportSummary) -> Vec<String> {
    let mut lines = vec![
        "export summary:".to_owned(),
        format!("resolved_pools={}", summary.resolved_pool_count),
        format!(
            "unresolved_stable_side_pools={}",
            summary.unresolved_pool_count
        ),
        format!(
            "unsupported_or_invalid_pools={}",
            summary.unsupported_pool_count
        ),
    ];

    for pool in &summary.unsupported_pool_details {
        lines.push(format!(
            "unsupported_or_invalid_pool={} reason={}",
            pool.pool_address, pool.reason
        ));
    }

    lines.extend([
        format!("raw_swap_count={}", summary.raw_totals.event_counts.swap),
        format!("raw_mint_count={}", summary.raw_totals.event_counts.mint),
        format!("raw_burn_count={}", summary.raw_totals.event_counts.burn),
        format!(
            "raw_collect_count={}",
            summary.raw_totals.event_counts.collect
        ),
        format!(
            "raw_ignored_non_selected_pool={}",
            summary.raw_totals.ignored_non_selected_pool
        ),
        format!(
            "raw_ignored_non_target_topic={}",
            summary.raw_totals.ignored_non_target_topic
        ),
        format!(
            "raw_dropped_duplicates={}",
            summary.raw_totals.dropped_duplicates
        ),
        format!(
            "raw_skipped_existing_shards={}",
            summary.raw_skipped_existing_shards
        ),
        format!("state_shard_count={}", summary.state_shard_count),
        format!(
            "state_skipped_existing_shards={}",
            summary.skipped_existing_state_shards
        ),
        format!("state_repaired_shards={}", summary.repaired_state_shards),
        format!(
            "metadata_resolved_pool_count={}",
            summary.metadata_resolved_pool_count
        ),
        format!(
            "metadata_unresolved_pool_count={}",
            summary.metadata_unresolved_pool_count
        ),
        format!(
            "metadata_stable_token_count={}",
            summary.metadata_stable_token_count
        ),
        format!("run_root={}", summary.run_root.display()),
    ]);

    lines
}

fn print_verify_summary(summary: &VerifySummary) {
    println!("verify summary: replay root is valid");
    println!("run_root={}", summary.run_root.display());
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
        rc::Rc,
    };

    use clap::Parser;
    use serde_json::{json, Value};
    use tempfile::TempDir;

    use crate::{source::IndexerPoolMetadata, PoolMetadataProvider};

    use super::{
        export_summary_lines, read_selected_pools_file, run_export_with_adapters,
        run_verify_command, Cli, CliError, Command, ExportArgs, ExportSummary, VerifyArgs,
    };

    const SWAP_TOPIC0: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    const BALANCE_OF_SELECTOR: &str = "0x70a08231";
    const DECIMALS_SELECTOR: &str = "0x313ce567";
    const SYMBOL_SELECTOR: &str = "0x95d89b41";
    const NAME_SELECTOR: &str = "0x06fdde03";

    #[derive(Debug, Clone)]
    struct MockIndexerProvider {
        pools: HashMap<String, IndexerPoolMetadata>,
    }

    impl PoolMetadataProvider for MockIndexerProvider {
        fn fetch_pool_metadata(
            &self,
            pool_address: &str,
        ) -> Result<IndexerPoolMetadata, crate::SourceError> {
            self.pools
                .get(pool_address)
                .cloned()
                .ok_or_else(|| crate::SourceError::NotFound {
                    resource: "pool",
                    address: pool_address.to_owned(),
                })
        }
    }

    #[derive(Debug, Clone)]
    struct MockJsonRpcClient {
        call_log: Rc<RefCell<Vec<String>>>,
        block_number: u64,
        block_hash: String,
        timestamp_secs: u64,
        receipts: Value,
        balances_by_token: HashMap<String, String>,
        decimals_by_token: HashMap<String, String>,
        symbols_by_token: HashMap<String, String>,
        names_by_token: HashMap<String, String>,
        fail_metadata_eth_call: bool,
    }

    impl crate::JsonRpcClient for MockJsonRpcClient {
        fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, crate::SourceError> {
            self.call_log.borrow_mut().push(method.to_owned());
            match method {
                "eth_getBlockByNumber" => {
                    let Some(Value::String(block_hex)) = params.first() else {
                        return Err(crate::SourceError::InvalidRpcResponse {
                            message: "missing block parameter".to_owned(),
                        });
                    };
                    let requested = crate::parse_hex_u64("eth_getBlockByNumber.block", block_hex)?;
                    if requested != self.block_number {
                        return Err(crate::SourceError::InvalidRpcResponse {
                            message: format!(
                                "unexpected block number {requested}, expected {}",
                                self.block_number
                            ),
                        });
                    }
                    Ok(json!({
                        "number": format!("0x{:x}", self.block_number),
                        "hash": self.block_hash,
                        "timestamp": format!("0x{:x}", self.timestamp_secs),
                    }))
                }
                "eth_getBlockReceipts" => Ok(self.receipts.clone()),
                "eth_call" => {
                    let Some(Value::Object(call_obj)) = params.first() else {
                        return Err(crate::SourceError::InvalidRpcResponse {
                            message: "missing eth_call object".to_owned(),
                        });
                    };
                    let Some(Value::String(token)) = call_obj.get("to") else {
                        return Err(crate::SourceError::InvalidRpcResponse {
                            message: "missing eth_call.to".to_owned(),
                        });
                    };
                    let Some(Value::String(data)) = call_obj.get("data") else {
                        return Err(crate::SourceError::InvalidRpcResponse {
                            message: "missing eth_call.data".to_owned(),
                        });
                    };
                    let selector = data.get(0..10).unwrap_or_default().to_ascii_lowercase();
                    if self.fail_metadata_eth_call && selector != BALANCE_OF_SELECTOR {
                        return Err(crate::SourceError::Rpc {
                            code: -32000,
                            message: "mock metadata eth_call failure".to_owned(),
                        });
                    }
                    let response = match selector.as_str() {
                        BALANCE_OF_SELECTOR => self.balances_by_token.get(token).cloned(),
                        DECIMALS_SELECTOR => self.decimals_by_token.get(token).cloned(),
                        SYMBOL_SELECTOR => self.symbols_by_token.get(token).cloned(),
                        NAME_SELECTOR => self.names_by_token.get(token).cloned(),
                        _ => None,
                    }
                    .ok_or_else(|| crate::SourceError::InvalidRpcResponse {
                        message: format!("missing mock eth_call response for {selector} {token}"),
                    })?;
                    Ok(Value::String(response))
                }
                other => Err(crate::SourceError::Rpc {
                    code: -32601,
                    message: format!("unsupported method {other}"),
                }),
            }
        }
    }

    #[test]
    fn export_command_requires_all_mandatory_flags() {
        let parsed =
            Cli::try_parse_from(["base-backtest-exporter", "export", "--run-root", "/tmp/x"]);
        assert!(parsed.is_err());
        let error_text = match parsed {
            Ok(_) => String::new(),
            Err(err) => err.to_string(),
        };
        assert!(error_text.contains("--rpc-url"));
        assert!(error_text.contains("--indexer-url"));
        assert!(error_text.contains("--selected-pools-file"));
        assert!(error_text.contains("--stable-tokens-file"));
        assert!(error_text.contains("--from-block"));
        assert!(error_text.contains("--to-block"));
        assert!(error_text.contains("--shard-size-blocks"));
        assert!(error_text.contains("--validation-stride-targets"));
    }

    #[test]
    fn export_command_ignores_empty_token_overrides_env_var() {
        let previous = std::env::var_os("TOKEN_OVERRIDES_FILE");
        std::env::set_var("TOKEN_OVERRIDES_FILE", "");

        let parsed = Cli::try_parse_from([
            "base-backtest-exporter",
            "export",
            "--run-root",
            "/tmp/x",
            "--rpc-url",
            "http://127.0.0.1:8545",
            "--indexer-url",
            "http://127.0.0.1:8080",
            "--selected-pools-file",
            "/tmp/selected_pools.txt",
            "--stable-tokens-file",
            "/tmp/stable_tokens.json",
            "--from-block",
            "1",
            "--to-block",
            "1",
            "--shard-size-blocks",
            "1",
            "--validation-stride-targets",
            "1",
        ]);

        match previous {
            Some(value) => std::env::set_var("TOKEN_OVERRIDES_FILE", value),
            None => std::env::remove_var("TOKEN_OVERRIDES_FILE"),
        }

        let cli = parsed.unwrap_or_else(|error| panic!("unexpected parse error: {error}"));
        match cli.command {
            Command::Export(args) => assert!(args.token_overrides_file.is_none()),
            Command::Verify(_) => panic!("expected export command"),
        }
    }

    #[test]
    fn selected_pools_file_allows_comments_and_normalizes_addresses() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let file = temp.path().join("selected_pools.txt");
        write_file(
            &file,
            "\n# comment\n  0x1111111111111111111111111111111111111111\n\n0x2222222222222222222222222222222222222222  \n",
        );

        let parsed = read_selected_pools_file(&file);
        match parsed {
            Ok(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], "0x1111111111111111111111111111111111111111");
                assert_eq!(values[1], "0x2222222222222222222222222222222222222222");
            }
            Err(error) => panic!("unexpected parse error: {error}"),
        }
    }

    #[test]
    fn selected_pools_file_rejects_invalid_address() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let file = temp.path().join("selected_pools.txt");
        write_file(&file, "0xnot-a-valid-address\n");

        let parsed = read_selected_pools_file(&file);
        match parsed {
            Ok(_) => panic!("expected invalid-address failure"),
            Err(CliError::InvalidSelectedPoolAddress { .. }) => {}
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn selected_pools_file_rejects_duplicate_after_normalization() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let file = temp.path().join("selected_pools.txt");
        write_file(
            &file,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
        );

        let parsed = read_selected_pools_file(&file);
        match parsed {
            Ok(_) => panic!("expected duplicate-address failure"),
            Err(CliError::InvalidRequest { message }) => {
                assert!(message.contains("duplicate pool address"));
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn export_runs_raw_state_metadata_and_supports_resume_skip() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        let selected_pools_file = temp.path().join("selected_pools.txt");
        let stable_tokens_file = temp.path().join("stable_tokens.json");

        let addresses = TestAddresses::default();
        write_file(
            &selected_pools_file,
            &format!(
                "{}\n{}\n{}\n",
                addresses.pool_resolved, addresses.pool_unresolved, addresses.pool_unsupported
            ),
        );
        write_file(
            &stable_tokens_file,
            &format!(
                "{{\n  \"version\": 1,\n  \"tokens\": [\n    {{\"address\": \"{}\", \"symbol\": \"USDC\", \"name\": \"USD Coin\"}}\n  ]\n}}",
                addresses.token_stable
            ),
        );

        let args = ExportArgs {
            run_root: run_root.clone(),
            rpc_url: "http://unused-rpc".to_owned(),
            indexer_url: "http://unused-indexer".to_owned(),
            selected_pools_file,
            stable_tokens_file,
            token_overrides_file: None,
            from_block: 100,
            to_block: 100,
            shard_size_blocks: 10,
            validation_stride_targets: 1,
        };

        let provider = build_mock_provider(&addresses);
        let call_log = Rc::new(RefCell::new(Vec::<String>::new()));
        let rpc_client = build_mock_rpc_client(&addresses, call_log.clone());
        let node_adapter = crate::BaseNodeRpcAdapter::with_client(rpc_client);

        let summary = run_export_with_adapters(&args, &provider, &node_adapter)
            .unwrap_or_else(|error| panic!("unexpected export error: {error}"));
        assert_eq!(summary.resolved_pool_count, 1);
        assert_eq!(summary.unresolved_pool_count, 1);
        assert_eq!(summary.unsupported_pool_count, 1);
        assert_eq!(
            summary.unsupported_pool_details,
            vec![crate::UnsupportedOrInvalidPool {
                pool_address: addresses.pool_unsupported.clone(),
                reason: "unsupported_protocol:UniswapV2".to_owned(),
            }]
        );
        assert_eq!(summary.raw_totals.event_counts.swap, 1);
        assert_eq!(summary.state_shard_count, 1);
        assert_eq!(summary.metadata_resolved_pool_count, 1);
        assert_eq!(summary.metadata_unresolved_pool_count, 1);
        assert_eq!(summary.metadata_stable_token_count, 1);
        assert_eq!(summary.raw_skipped_existing_shards, 0);
        assert_eq!(summary.skipped_existing_state_shards, 0);
        assert_eq!(call_log.borrow().len(), 18);

        assert_file_exists(&run_root.join("raw/swap/100_100.jsonl"));
        assert_file_exists(&run_root.join("state"));
        assert_file_exists(&run_root.join("pool_manifest.json"));
        assert_file_exists(&run_root.join("stable_tokens.json"));
        assert_file_exists(&run_root.join("unresolved_stable_side_report.json"));
        assert_file_exists(&run_root.join("pools.generated.toml"));

        let rerun = run_export_with_adapters(&args, &provider, &node_adapter)
            .unwrap_or_else(|error| panic!("unexpected rerun error: {error}"));
        assert_eq!(rerun.raw_skipped_existing_shards, 1);
        assert_eq!(rerun.skipped_existing_state_shards, 1);
        assert_eq!(call_log.borrow().len(), 30);
    }

    #[test]
    fn export_fails_when_stable_tokens_file_is_invalid() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        let selected_pools_file = temp.path().join("selected_pools.txt");
        let stable_tokens_file = temp.path().join("stable_tokens.json");

        write_file(
            &selected_pools_file,
            "0x1111111111111111111111111111111111111111\n",
        );
        write_file(&stable_tokens_file, "{\"version\":1,\"tokens\":[{}]}");

        let args = ExportArgs {
            run_root,
            rpc_url: "http://unused-rpc".to_owned(),
            indexer_url: "http://unused-indexer".to_owned(),
            selected_pools_file,
            stable_tokens_file,
            token_overrides_file: None,
            from_block: 100,
            to_block: 100,
            shard_size_blocks: 1,
            validation_stride_targets: 1,
        };

        let provider = MockIndexerProvider {
            pools: HashMap::new(),
        };
        let node_adapter = crate::BaseNodeRpcAdapter::with_client(MockJsonRpcClient {
            call_log: Rc::new(RefCell::new(Vec::new())),
            block_number: 0,
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            timestamp_secs: 1,
            receipts: Value::Array(Vec::new()),
            balances_by_token: HashMap::new(),
            decimals_by_token: HashMap::new(),
            symbols_by_token: HashMap::new(),
            names_by_token: HashMap::new(),
            fail_metadata_eth_call: false,
        });

        let result = run_export_with_adapters(&args, &provider, &node_adapter);
        match result {
            Ok(_) => panic!("expected stable-token contract error"),
            Err(CliError::Contract(_)) => {}
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn export_fails_fast_when_catalog_has_zero_resolved_pools() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        let selected_pools_file = temp.path().join("selected_pools.txt");
        let stable_tokens_file = temp.path().join("stable_tokens.json");

        let addresses = TestAddresses::default();
        write_file(
            &selected_pools_file,
            &format!("{}\n", addresses.pool_unresolved),
        );
        write_file(
            &stable_tokens_file,
            "{\"version\":1,\"tokens\":[{\"address\":\"0x7777777777777777777777777777777777777777\",\"symbol\":\"USDX\",\"name\":\"USDX\"}]}",
        );

        let args = ExportArgs {
            run_root,
            rpc_url: "http://unused-rpc".to_owned(),
            indexer_url: "http://unused-indexer".to_owned(),
            selected_pools_file,
            stable_tokens_file,
            token_overrides_file: None,
            from_block: 100,
            to_block: 100,
            shard_size_blocks: 1,
            validation_stride_targets: 1,
        };
        let provider = build_mock_provider(&addresses);
        let call_log = Rc::new(RefCell::new(Vec::<String>::new()));
        let rpc_client = build_mock_rpc_client(&addresses, call_log.clone());
        let node_adapter = crate::BaseNodeRpcAdapter::with_client(rpc_client);

        let result = run_export_with_adapters(&args, &provider, &node_adapter);
        match result {
            Ok(_) => panic!("expected resolved-catalog-empty error"),
            Err(CliError::InvalidRequest { message }) => {
                assert!(message.contains("resolved pool catalog is empty"));
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
        assert_eq!(call_log.borrow().len(), 6);
    }

    #[test]
    fn export_fails_when_token_overrides_file_is_invalid() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        let selected_pools_file = temp.path().join("selected_pools.txt");
        let stable_tokens_file = temp.path().join("stable_tokens.json");
        let token_overrides_file = temp.path().join("token_overrides.json");

        let addresses = TestAddresses::default();
        write_file(
            &selected_pools_file,
            &format!("{}\n", addresses.pool_resolved),
        );
        write_file(
            &stable_tokens_file,
            &format!(
                r#"{{"version":1,"tokens":[{{"address":"{}","symbol":"USDC","name":"USD Coin"}}]}}"#,
                addresses.token_stable
            ),
        );
        write_file(
            &token_overrides_file,
            &format!(
                r#"{{"version":1,"tokens":[{{"address":"{}","decimals":18,"symbol":"WETH","name":"Wrapped Ether"}},{{"address":"{}","decimals":18,"symbol":"WETH","name":"Wrapped Ether"}}]}}"#,
                addresses.token_quote,
                addresses.token_quote.to_uppercase()
            ),
        );

        let args = ExportArgs {
            run_root,
            rpc_url: "http://unused-rpc".to_owned(),
            indexer_url: "http://unused-indexer".to_owned(),
            selected_pools_file,
            stable_tokens_file,
            token_overrides_file: Some(token_overrides_file),
            from_block: 100,
            to_block: 100,
            shard_size_blocks: 1,
            validation_stride_targets: 1,
        };

        let provider = build_mock_provider(&addresses);
        let call_log = Rc::new(RefCell::new(Vec::<String>::new()));
        let rpc_client = build_mock_rpc_client(&addresses, call_log.clone());
        let node_adapter = crate::BaseNodeRpcAdapter::with_client(rpc_client);

        let result = run_export_with_adapters(&args, &provider, &node_adapter);
        match result {
            Err(CliError::Contract(_)) => {}
            Ok(_) => panic!("expected token override contract error"),
            Err(other) => panic!("unexpected error: {other}"),
        }
        assert_eq!(call_log.borrow().len(), 0);
    }

    #[test]
    fn export_uses_token_overrides_when_rpc_metadata_fails() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        let selected_pools_file = temp.path().join("selected_pools.txt");
        let stable_tokens_file = temp.path().join("stable_tokens.json");
        let token_overrides_file = temp.path().join("token_overrides.json");

        let addresses = TestAddresses::default();
        write_file(
            &selected_pools_file,
            &format!("{}\n", addresses.pool_resolved),
        );
        write_file(
            &stable_tokens_file,
            &format!(
                r#"{{"version":1,"tokens":[{{"address":"{}","symbol":"USDC","name":"USD Coin"}}]}}"#,
                addresses.token_stable
            ),
        );
        write_file(
            &token_overrides_file,
            &format!(
                r#"{{"version":1,"tokens":[{{"address":"{}","decimals":6,"symbol":"USDC","name":"USD Coin"}},{{"address":"{}","decimals":18,"symbol":"WETH","name":"Wrapped Ether"}}]}}"#,
                addresses.token_stable, addresses.token_quote
            ),
        );

        let args = ExportArgs {
            run_root,
            rpc_url: "http://unused-rpc".to_owned(),
            indexer_url: "http://unused-indexer".to_owned(),
            selected_pools_file,
            stable_tokens_file,
            token_overrides_file: Some(token_overrides_file),
            from_block: 100,
            to_block: 100,
            shard_size_blocks: 1,
            validation_stride_targets: 1,
        };

        let provider = build_mock_provider(&addresses);
        let call_log = Rc::new(RefCell::new(Vec::<String>::new()));
        let mut rpc_client = build_mock_rpc_client(&addresses, call_log.clone());
        rpc_client.fail_metadata_eth_call = true;
        let node_adapter = crate::BaseNodeRpcAdapter::with_client(rpc_client);

        let summary = run_export_with_adapters(&args, &provider, &node_adapter)
            .unwrap_or_else(|error| panic!("unexpected export error: {error}"));
        assert_eq!(summary.resolved_pool_count, 1);
    }

    #[test]
    fn export_fails_fast_when_rpc_metadata_fails_and_override_misses() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        let selected_pools_file = temp.path().join("selected_pools.txt");
        let stable_tokens_file = temp.path().join("stable_tokens.json");

        let addresses = TestAddresses::default();
        write_file(
            &selected_pools_file,
            &format!("{}\n", addresses.pool_resolved),
        );
        write_file(
            &stable_tokens_file,
            &format!(
                r#"{{"version":1,"tokens":[{{"address":"{}","symbol":"USDC","name":"USD Coin"}}]}}"#,
                addresses.token_stable
            ),
        );

        let args = ExportArgs {
            run_root,
            rpc_url: "http://unused-rpc".to_owned(),
            indexer_url: "http://unused-indexer".to_owned(),
            selected_pools_file,
            stable_tokens_file,
            token_overrides_file: None,
            from_block: 100,
            to_block: 100,
            shard_size_blocks: 1,
            validation_stride_targets: 1,
        };

        let provider = build_mock_provider(&addresses);
        let call_log = Rc::new(RefCell::new(Vec::<String>::new()));
        let mut rpc_client = build_mock_rpc_client(&addresses, call_log.clone());
        rpc_client.fail_metadata_eth_call = true;
        let node_adapter = crate::BaseNodeRpcAdapter::with_client(rpc_client);

        let result = run_export_with_adapters(&args, &provider, &node_adapter);
        match result {
            Err(CliError::InvalidRequest { message }) => {
                assert!(message.contains("resolved pool catalog is empty"));
            }
            Ok(_) => panic!("expected resolved-catalog-empty error"),
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn export_summary_lines_include_unsupported_pool_reasons() {
        let summary = ExportSummary {
            run_root: PathBuf::from("/tmp/run"),
            resolved_pool_count: 3,
            unresolved_pool_count: 0,
            unsupported_pool_count: 2,
            unsupported_pool_details: vec![
                crate::UnsupportedOrInvalidPool {
                    pool_address: "0x1111111111111111111111111111111111111111".to_owned(),
                    reason: "unsupported_protocol:curve_v2".to_owned(),
                },
                crate::UnsupportedOrInvalidPool {
                    pool_address: "0x2222222222222222222222222222222222222222".to_owned(),
                    reason: "missing_fee_tier".to_owned(),
                },
            ],
            raw_totals: crate::RawExportTotals::default(),
            raw_skipped_existing_shards: 0,
            state_shard_count: 0,
            skipped_existing_state_shards: 0,
            repaired_state_shards: 0,
            metadata_resolved_pool_count: 3,
            metadata_unresolved_pool_count: 0,
            metadata_stable_token_count: 1,
        };

        let lines = export_summary_lines(&summary);
        assert!(lines.contains(&"unsupported_or_invalid_pools=2".to_owned()));
        assert!(lines.contains(
            &"unsupported_or_invalid_pool=0x1111111111111111111111111111111111111111 reason=unsupported_protocol:curve_v2".to_owned()
        ));
        assert!(lines.contains(
            &"unsupported_or_invalid_pool=0x2222222222222222222222222222222222222222 reason=missing_fee_tier".to_owned()
        ));
    }

    #[test]
    fn verify_accepts_valid_root_and_rejects_missing_required_files() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run_ok");
        create_minimal_valid_replay_root(&run_root);

        let ok = run_verify_command(&VerifyArgs {
            run_root: run_root.clone(),
        });
        assert!(ok.is_ok());

        let missing_pool_manifest = temp.path().join("run_missing_pool_manifest");
        create_minimal_valid_replay_root(&missing_pool_manifest);
        remove_file(&missing_pool_manifest.join("pool_manifest.json"));
        let missing_pool_manifest_result = run_verify_command(&VerifyArgs {
            run_root: missing_pool_manifest,
        });
        assert!(missing_pool_manifest_result.is_err());

        let missing_manifest = temp.path().join("run_missing_manifest");
        create_minimal_valid_replay_root(&missing_manifest);
        remove_file(&missing_manifest.join("manifest.json"));
        let missing_manifest_result = run_verify_command(&VerifyArgs {
            run_root: missing_manifest,
        });
        assert!(missing_manifest_result.is_err());

        let missing_meta = temp.path().join("run_missing_meta");
        create_minimal_valid_replay_root(&missing_meta);
        remove_file(&missing_meta.join("meta.json"));
        let missing_meta_result = run_verify_command(&VerifyArgs {
            run_root: missing_meta,
        });
        assert!(missing_meta_result.is_err());

        let broken_layout = temp.path().join("run_broken_layout");
        create_minimal_valid_replay_root(&broken_layout);
        remove_dir_all(&broken_layout.join("raw/mint"));
        let broken_layout_result = run_verify_command(&VerifyArgs {
            run_root: broken_layout,
        });
        assert!(broken_layout_result.is_err());
    }

    #[derive(Debug, Clone)]
    struct TestAddresses {
        pool_resolved: String,
        pool_unresolved: String,
        pool_unsupported: String,
        token_stable: String,
        token_quote: String,
        token_other: String,
    }

    impl Default for TestAddresses {
        fn default() -> Self {
            Self {
                pool_resolved: "0x1111111111111111111111111111111111111111".to_owned(),
                pool_unresolved: "0x2222222222222222222222222222222222222222".to_owned(),
                pool_unsupported: "0x3333333333333333333333333333333333333333".to_owned(),
                token_stable: "0x4444444444444444444444444444444444444444".to_owned(),
                token_quote: "0x5555555555555555555555555555555555555555".to_owned(),
                token_other: "0x6666666666666666666666666666666666666666".to_owned(),
            }
        }
    }

    fn build_mock_provider(addresses: &TestAddresses) -> MockIndexerProvider {
        let mut pools = HashMap::<String, IndexerPoolMetadata>::new();
        pools.insert(
            addresses.pool_resolved.clone(),
            IndexerPoolMetadata {
                address: addresses.pool_resolved.clone(),
                protocol: "UniswapV3".to_owned(),
                tokens: vec![
                    addresses.token_stable.clone(),
                    addresses.token_quote.clone(),
                ],
                factory_address: None,
                creation_block_number: Some(99),
                fee: Some(500),
                tick_spacing: Some(10),
            },
        );
        pools.insert(
            addresses.pool_unresolved.clone(),
            IndexerPoolMetadata {
                address: addresses.pool_unresolved.clone(),
                protocol: "UniswapV3".to_owned(),
                tokens: vec![addresses.token_quote.clone(), addresses.token_other.clone()],
                factory_address: None,
                creation_block_number: Some(99),
                fee: Some(3000),
                tick_spacing: Some(60),
            },
        );
        pools.insert(
            addresses.pool_unsupported.clone(),
            IndexerPoolMetadata {
                address: addresses.pool_unsupported.clone(),
                protocol: "UniswapV2".to_owned(),
                tokens: vec![
                    addresses.token_stable.clone(),
                    addresses.token_quote.clone(),
                ],
                factory_address: None,
                creation_block_number: Some(99),
                fee: Some(3000),
                tick_spacing: Some(60),
            },
        );

        MockIndexerProvider { pools }
    }

    fn encode_abi_uint_word(value: u64) -> String {
        format!("0x{value:064x}")
    }

    fn encode_abi_dynamic_string(value: &str) -> String {
        let mut data_hex = hex::encode(value.as_bytes());
        let padded_len = value.len().div_ceil(32) * 64;
        while data_hex.len() < padded_len {
            data_hex.push('0');
        }

        format!("0x{:064x}{:064x}{}", 32, value.len(), data_hex)
    }

    fn build_mock_rpc_client(
        addresses: &TestAddresses,
        call_log: Rc<RefCell<Vec<String>>>,
    ) -> MockJsonRpcClient {
        let receipt = json!({
            "transactionHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "transactionIndex": "0x0",
            "blockNumber": "0x64",
            "blockHash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "logs": [
                {
                    "address": addresses.pool_resolved,
                    "topics": [
                        SWAP_TOPIC0,
                        "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "logIndex": "0x0"
                }
            ]
        });

        let mut balances_by_token = HashMap::<String, String>::new();
        balances_by_token.insert(addresses.token_stable.clone(), "0x3e8".to_owned());
        balances_by_token.insert(addresses.token_quote.clone(), "0x7d0".to_owned());

        let mut decimals_by_token = HashMap::<String, String>::new();
        decimals_by_token.insert(addresses.token_stable.clone(), encode_abi_uint_word(6));
        decimals_by_token.insert(addresses.token_quote.clone(), encode_abi_uint_word(18));
        decimals_by_token.insert(addresses.token_other.clone(), encode_abi_uint_word(8));

        let mut symbols_by_token = HashMap::<String, String>::new();
        symbols_by_token.insert(
            addresses.token_stable.clone(),
            encode_abi_dynamic_string("USDC"),
        );
        symbols_by_token.insert(
            addresses.token_quote.clone(),
            encode_abi_dynamic_string("WETH"),
        );
        symbols_by_token.insert(
            addresses.token_other.clone(),
            encode_abi_dynamic_string("WBTC"),
        );

        let mut names_by_token = HashMap::<String, String>::new();
        names_by_token.insert(
            addresses.token_stable.clone(),
            encode_abi_dynamic_string("USD Coin"),
        );
        names_by_token.insert(
            addresses.token_quote.clone(),
            encode_abi_dynamic_string("Wrapped Ether"),
        );
        names_by_token.insert(
            addresses.token_other.clone(),
            encode_abi_dynamic_string("Wrapped BTC"),
        );

        MockJsonRpcClient {
            call_log,
            block_number: 100,
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            timestamp_secs: 1_700_000_000,
            receipts: Value::Array(vec![receipt]),
            balances_by_token,
            decimals_by_token,
            symbols_by_token,
            names_by_token,
            fail_metadata_eth_call: false,
        }
    }

    fn create_minimal_valid_replay_root(run_root: &Path) {
        for dir in ["raw/swap", "raw/mint", "raw/burn", "raw/collect", "state"] {
            let path = run_root.join(dir);
            fs::create_dir_all(&path)
                .unwrap_or_else(|error| panic!("failed to create {}: {error}", path.display()));
        }

        write_file(&run_root.join("meta.json"), "{}");
        write_file(&run_root.join("manifest.json"), "{}");
        write_file(
            &run_root.join("pool_manifest.json"),
            "{\"version\":1,\"pools\":[]}",
        );
    }

    fn write_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .unwrap_or_else(|error| panic!("failed to create {}: {error}", parent.display()));
        }
        fs::write(path, contents)
            .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
    }

    fn assert_file_exists(path: &Path) {
        assert!(path.exists(), "expected path to exist: {}", path.display());
    }

    fn remove_file(path: &Path) {
        fs::remove_file(path)
            .unwrap_or_else(|error| panic!("failed to remove {}: {error}", path.display()));
    }

    fn remove_dir_all(path: &Path) {
        fs::remove_dir_all(path)
            .unwrap_or_else(|error| panic!("failed to remove dir {}: {error}", path.display()));
    }
}
