use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::{
    catalog::ResolvedPoolCatalog,
    contract::{
        validate_pool_manifest_str, validate_stable_token_list_str,
        validate_unresolved_stable_side_report_str, PoolManifest, PoolManifestEntry,
        StableTokenEntry, StableTokenList, TokenMetadata, UnresolvedStableSideReport,
        CANONICAL_POOL_MANIFEST_FILE, CONTRACT_VERSION, GENERATED_POOLS_FILE, STABLE_TOKENS_FILE,
        UNRESOLVED_STABLE_SIDE_REPORT_FILE,
    },
    protocol::registry::NormalizedProtocol,
    source::normalize_evm_address,
};

use super::{ExportError, MetadataExportRequest, MetadataExportResult};

#[derive(Debug, Serialize)]
struct GeneratedPoolsToml {
    pools: BTreeMap<String, GeneratedPoolEntry>,
}

#[derive(Debug, Serialize)]
struct GeneratedPoolEntry {
    token_symbol: String,
    token0_decimals: u8,
    token1_decimals: u8,
    fee_tier: u32,
    token0_is_stable: bool,
    token1_is_stable: bool,
}

pub fn export_replay_metadata(
    request: &MetadataExportRequest,
    resolved_catalog: &ResolvedPoolCatalog,
    stable_tokens: &StableTokenList,
) -> Result<MetadataExportResult, ExportError> {
    validate_unique_resolved_pool_addresses(resolved_catalog)?;
    let pool_manifest = build_pool_manifest(resolved_catalog);
    let unresolved_report = build_unresolved_report(resolved_catalog);
    let stable_snapshot = build_canonical_stable_token_list(stable_tokens)?;
    let pools_generated_toml = build_generated_pools_toml(resolved_catalog);

    let pool_manifest_json =
        serde_json::to_string_pretty(&pool_manifest).map_err(|source| ExportError::JsonEncode {
            label: CANONICAL_POOL_MANIFEST_FILE.to_owned(),
            source,
        })?;
    let stable_tokens_json = serde_json::to_string_pretty(&stable_snapshot).map_err(|source| {
        ExportError::JsonEncode {
            label: STABLE_TOKENS_FILE.to_owned(),
            source,
        }
    })?;
    let unresolved_json = serde_json::to_string_pretty(&unresolved_report).map_err(|source| {
        ExportError::JsonEncode {
            label: UNRESOLVED_STABLE_SIDE_REPORT_FILE.to_owned(),
            source,
        }
    })?;
    let pools_generated = toml::to_string_pretty(&pools_generated_toml).map_err(|source| {
        ExportError::JsonEncode {
            label: GENERATED_POOLS_FILE.to_owned(),
            source: serde_json::Error::io(std::io::Error::other(source.to_string())),
        }
    })?;

    let _ = validate_pool_manifest_str(&pool_manifest_json)?;
    let _ = validate_stable_token_list_str(&stable_tokens_json)?;
    let _ = validate_unresolved_stable_side_report_str(&unresolved_json)?;

    write_files_atomically(
        &request.run_root,
        &[
            (CANONICAL_POOL_MANIFEST_FILE, pool_manifest_json),
            (STABLE_TOKENS_FILE, stable_tokens_json),
            (UNRESOLVED_STABLE_SIDE_REPORT_FILE, unresolved_json),
            (GENERATED_POOLS_FILE, pools_generated),
        ],
    )?;

    Ok(MetadataExportResult {
        resolved_pool_count: pool_manifest.pools.len() as u64,
        unresolved_pool_count: unresolved_report.items.len() as u64,
        stable_token_count: stable_snapshot.tokens.len() as u64,
    })
}

fn validate_unique_resolved_pool_addresses(
    catalog: &ResolvedPoolCatalog,
) -> Result<(), ExportError> {
    let mut seen = HashSet::<String>::new();
    for pool in &catalog.resolved {
        let normalized = normalize_evm_address("resolved.pool_address", &pool.pool_address)?;
        if !seen.insert(normalized.clone()) {
            return Err(ExportError::InvalidRequest {
                message: format!("duplicate pool address in resolved catalog: {normalized}"),
            });
        }
    }
    Ok(())
}

fn build_pool_manifest(catalog: &ResolvedPoolCatalog) -> PoolManifest {
    let mut pools = catalog
        .resolved
        .iter()
        .map(|entry| PoolManifestEntry {
            pool_address: entry.pool_address.clone(),
            protocol: protocol_name(entry.protocol).to_owned(),
            token0: TokenMetadata {
                address: entry.token0.address.clone(),
                decimals: entry.token0.decimals,
            },
            token1: TokenMetadata {
                address: entry.token1.address.clone(),
                decimals: entry.token1.decimals,
            },
            fee_tier: entry.fee_tier,
            token0_is_stable: entry.token0_is_stable,
            token1_is_stable: entry.token1_is_stable,
        })
        .collect::<Vec<_>>();
    pools.sort_by(|left, right| left.pool_address.cmp(&right.pool_address));
    PoolManifest {
        version: CONTRACT_VERSION,
        pools,
    }
}

fn build_unresolved_report(catalog: &ResolvedPoolCatalog) -> UnresolvedStableSideReport {
    let mut items = catalog.unresolved_stable_side.clone();
    items.sort_by(|left, right| left.pool_address.cmp(&right.pool_address));
    UnresolvedStableSideReport {
        version: CONTRACT_VERSION,
        items,
    }
}

fn build_canonical_stable_token_list(
    stable_tokens: &StableTokenList,
) -> Result<StableTokenList, ExportError> {
    let mut seen = HashSet::<String>::new();
    let mut tokens = Vec::<StableTokenEntry>::new();
    for token in &stable_tokens.tokens {
        let address = normalize_evm_address("stable_tokens.tokens[].address", &token.address)?;
        if !seen.insert(address.clone()) {
            return Err(ExportError::InvalidRequest {
                message: format!("duplicate stable token address in allowlist: {address}"),
            });
        }
        tokens.push(StableTokenEntry {
            address,
            symbol: token.symbol.clone(),
            name: token.name.clone(),
        });
    }
    tokens.sort_by(|left, right| left.address.cmp(&right.address));
    Ok(StableTokenList {
        version: stable_tokens.version,
        tokens,
    })
}

fn build_generated_pools_toml(catalog: &ResolvedPoolCatalog) -> GeneratedPoolsToml {
    let mut pools = BTreeMap::<String, GeneratedPoolEntry>::new();
    for pool in &catalog.resolved {
        pools.insert(
            pool.pool_address.to_ascii_lowercase(),
            GeneratedPoolEntry {
                token_symbol: format!("{}/{}", pool.token0.symbol, pool.token1.symbol),
                token0_decimals: pool.token0.decimals,
                token1_decimals: pool.token1.decimals,
                fee_tier: pool.fee_tier,
                token0_is_stable: pool.token0_is_stable,
                token1_is_stable: pool.token1_is_stable,
            },
        );
    }
    GeneratedPoolsToml { pools }
}

fn protocol_name(protocol: NormalizedProtocol) -> &'static str {
    match protocol {
        NormalizedProtocol::UniswapV3 => "UniswapV3",
        NormalizedProtocol::PancakeV3 => "PancakeV3",
        NormalizedProtocol::SushiswapV3 => "SushiswapV3",
        NormalizedProtocol::AerodromeV3 => "AerodromeV3",
        NormalizedProtocol::AlienV3 => "AlienV3",
    }
}

fn write_files_atomically(run_root: &Path, files: &[(&str, String)]) -> Result<(), ExportError> {
    fs::create_dir_all(run_root).map_err(|source| ExportError::IoWrite {
        path: run_root.to_path_buf(),
        source,
    })?;

    let nonce = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    let mut temp_files = Vec::<(PathBuf, PathBuf)>::new();

    for (name, contents) in files {
        let final_path = run_root.join(name);
        let temp_path = run_root.join(format!(".{name}.{nonce}.tmp"));
        fs::write(&temp_path, contents).map_err(|source| ExportError::IoWrite {
            path: temp_path.clone(),
            source,
        })?;
        temp_files.push((temp_path, final_path));
    }

    for (temp_path, final_path) in &temp_files {
        if let Err(source) = fs::rename(temp_path, final_path) {
            for (left_temp, _) in &temp_files {
                let _ = fs::remove_file(left_temp);
            }
            return Err(ExportError::IoWrite {
                path: final_path.clone(),
                source,
            });
        }
    }

    Ok(())
}
