use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{Num, Zero};
use serde::Serialize;
use serde_json::Value;

use crate::{
    catalog::{ResolvedPoolCatalog, ResolvedPoolCatalogEntry},
    contract::{validate_state_line_str, validate_target_raw_line, StateLine},
    source::{
        normalize_evm_address, parse_hex_u64, BaseNodeRpcAdapter, BlockWithReceiptsRef,
        JsonRpcClient,
    },
};

use super::{
    shard::{
        count_jsonl_lines, is_lowercase_sha256_hex, load_existing_manifest, sha256_hex_file_bytes,
        write_state_manifest_file, ManifestFile, ShardRange,
    },
    ExportError, StateExportRequest, StateExportResult, StateShardGenerationMode,
    StateShardManifestEntry,
};

const TRANSFER_TOPIC0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const VALIDATION_REPORT_PATH: &str = "state/validation_report.json";

#[derive(Debug, Clone, Default)]
struct PoolTokenDelta {
    token0: BigInt,
    token1: BigInt,
}

#[derive(Debug, Clone)]
struct PoolTokenMeta {
    token0: String,
    token1: String,
}

#[derive(Debug, Clone)]
struct PoolShardTargetPlan {
    pool_address: String,
    from_block: u64,
    to_block: u64,
    target_blocks: Vec<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StateValidationReport {
    items: Vec<StateValidationReportItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StateValidationReportItem {
    pool_address: String,
    from_block: u64,
    to_block: u64,
    target_count: u64,
    checkpoint_count: u64,
    used_exact_fallback: bool,
    generation_mode: StateShardGenerationMode,
    drifts: Vec<StateDriftDetail>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StateDriftDetail {
    block_number: u64,
    token0_drift: String,
    token1_drift: String,
}

pub fn export_historical_state<C>(
    request: &StateExportRequest,
    resolved_catalog: &ResolvedPoolCatalog,
    node_adapter: &BaseNodeRpcAdapter<C>,
) -> Result<StateExportResult, ExportError>
where
    C: JsonRpcClient,
{
    validate_state_request(request)?;
    ensure_state_output_root(&request.run_root)?;

    let existing_manifest =
        load_existing_manifest(&request.run_root)?.ok_or_else(|| ExportError::InvalidRequest {
            message: "manifest.json is required; run raw export before state export".to_owned(),
        })?;
    let target_plans = extract_swap_targets(&request.run_root, &existing_manifest)?;

    let mut pool_meta_by_address = HashMap::<String, PoolTokenMeta>::new();
    for pool in &resolved_catalog.resolved {
        pool_meta_by_address.insert(
            pool.pool_address.clone(),
            PoolTokenMeta {
                token0: pool.token0.address.clone(),
                token1: pool.token1.address.clone(),
            },
        );
    }

    let mut result = StateExportResult::default();
    let mut report_items = Vec::<StateValidationReportItem>::new();

    for (range, pool_targets) in target_plans {
        let mut to_generate = Vec::<PoolShardTargetPlan>::new();
        for (pool_address, target_blocks) in pool_targets {
            let skip = try_skip_existing_state_shard(
                &request.run_root,
                &existing_manifest,
                range,
                &pool_address,
            )?;
            if let Some(entry) = skip {
                result.state_shards.push(entry);
                result.skipped_existing_state_shards += 1;
                continue;
            }

            let pool_meta = pool_meta_by_address.get(&pool_address).ok_or_else(|| {
                ExportError::InvalidRequest {
                    message: format!("resolved pool metadata missing for {}", pool_address),
                }
            })?;
            let _ = pool_meta;

            to_generate.push(PoolShardTargetPlan {
                pool_address,
                from_block: range.from_block,
                to_block: range.to_block,
                target_blocks,
            });
        }
        if to_generate.is_empty() {
            continue;
        }

        let block_refs = node_adapter.fetch_block_range(range.from_block, range.to_block)?;
        let transfer_deltas =
            build_transfer_deltas_by_pool(&block_refs, &to_generate, &pool_meta_by_address)?;

        for plan in to_generate {
            let pool_entry = find_resolved_pool_entry(resolved_catalog, &plan.pool_address)?;
            let (entry, report_item, used_fallback) =
                build_pool_shard_state(request, node_adapter, pool_entry, &plan, &transfer_deltas)?;
            if used_fallback {
                result.repaired_state_shards += 1;
            }
            result.state_shards.push(entry);
            report_items.push(report_item);
        }
    }

    result.state_shards.sort_by(|left, right| {
        (left.from_block, left.to_block, left.pool_address.as_str()).cmp(&(
            right.from_block,
            right.to_block,
            right.pool_address.as_str(),
        ))
    });
    report_items.sort_by(|left, right| {
        (left.from_block, left.to_block, left.pool_address.as_str()).cmp(&(
            right.from_block,
            right.to_block,
            right.pool_address.as_str(),
        ))
    });

    write_state_manifest_file(&request.run_root, &existing_manifest, &result.state_shards)?;
    write_validation_report(&request.run_root, &report_items)?;
    Ok(result)
}

fn validate_state_request(request: &StateExportRequest) -> Result<(), ExportError> {
    if request.validation_stride_targets == 0 {
        return Err(ExportError::InvalidRequest {
            message: "validation_stride_targets must be > 0".to_owned(),
        });
    }
    Ok(())
}

fn ensure_state_output_root(run_root: &Path) -> Result<(), ExportError> {
    let state_dir = run_root.join("state");
    fs::create_dir_all(&state_dir).map_err(|source| ExportError::IoWrite {
        path: state_dir,
        source,
    })?;
    Ok(())
}

fn extract_swap_targets(
    run_root: &Path,
    manifest: &ManifestFile,
) -> Result<BTreeMap<ShardRange, BTreeMap<String, Vec<u64>>>, ExportError> {
    let mut dedup_targets = BTreeMap::<ShardRange, BTreeMap<String, BTreeSet<u64>>>::new();

    for shard in &manifest.raw_shards {
        if shard.counts.swap == 0 {
            continue;
        }
        let swap_rel = shard
            .files
            .swap
            .as_ref()
            .ok_or_else(|| ExportError::InvalidRequest {
                message: format!(
                    "raw shard {}_{} has swap count but missing swap file path",
                    shard.from_block, shard.to_block
                ),
            })?;
        let swap_abs = run_root.join(swap_rel);
        if !swap_abs.is_file() {
            return Err(ExportError::InvalidRequest {
                message: format!(
                    "raw swap shard file not found for {}_{}: {}",
                    shard.from_block,
                    shard.to_block,
                    swap_abs.display()
                ),
            });
        }

        let reader =
            BufReader::new(
                fs::File::open(&swap_abs).map_err(|source| ExportError::IoRead {
                    path: swap_abs.clone(),
                    source,
                })?,
            );
        let range = ShardRange {
            from_block: shard.from_block,
            to_block: shard.to_block,
        };

        for line in reader.lines() {
            let line = line.map_err(|source| ExportError::IoRead {
                path: swap_abs.clone(),
                source,
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let raw = validate_target_raw_line(&line)?;
            let pool_address = normalize_evm_address("raw.swap.address", &raw.address)?;
            let block_number = parse_hex_u64("raw.swap.blockNumber", &raw.block_number)?;
            if block_number < range.from_block || block_number > range.to_block {
                return Err(ExportError::InvalidRequest {
                    message: format!(
                        "swap raw line block {} is outside shard {}_{}",
                        block_number, range.from_block, range.to_block
                    ),
                });
            }
            dedup_targets
                .entry(range)
                .or_default()
                .entry(pool_address)
                .or_default()
                .insert(block_number);
        }
    }

    let mut out = BTreeMap::<ShardRange, BTreeMap<String, Vec<u64>>>::new();
    for (range, per_pool) in dedup_targets {
        let mut pool_targets = BTreeMap::<String, Vec<u64>>::new();
        for (pool_address, block_set) in per_pool {
            if block_set.is_empty() {
                continue;
            }
            pool_targets.insert(pool_address, block_set.into_iter().collect());
        }
        if !pool_targets.is_empty() {
            out.insert(range, pool_targets);
        }
    }
    Ok(out)
}

fn try_skip_existing_state_shard(
    run_root: &Path,
    manifest: &ManifestFile,
    range: ShardRange,
    pool_address: &str,
) -> Result<Option<StateShardManifestEntry>, ExportError> {
    let expected_rel = state_relative_path(pool_address, range);
    let expected_abs = run_root.join(&expected_rel);
    let existing_entry = manifest
        .state_shards
        .iter()
        .find(|entry| {
            entry.pool_address == pool_address
                && entry.from_block == range.from_block
                && entry.to_block == range.to_block
        })
        .cloned();

    if !expected_abs.is_file() {
        if existing_entry.is_some() {
            return Err(ExportError::ResumeMismatch {
                from_block: range.from_block,
                to_block: range.to_block,
                message: format!(
                    "state shard manifest entry exists but file is missing for pool {}",
                    pool_address
                ),
            });
        }
        return Ok(None);
    }

    let entry = existing_entry.unwrap_or(StateShardManifestEntry {
        pool_address: pool_address.to_owned(),
        from_block: range.from_block,
        to_block: range.to_block,
        line_count: count_jsonl_lines(&expected_abs)?,
        file: expected_rel.clone(),
        digest: sha256_hex_file_bytes(&expected_abs)?,
        generation_mode: StateShardGenerationMode::IncrementalValidated,
    });

    if entry.file != expected_rel {
        return Err(ExportError::ResumeMismatch {
            from_block: range.from_block,
            to_block: range.to_block,
            message: format!(
                "state shard path mismatch for pool {}: expected {}",
                pool_address, expected_rel
            ),
        });
    }
    if entry.digest.trim().is_empty() || !is_lowercase_sha256_hex(&entry.digest) {
        return Err(ExportError::ResumeMismatch {
            from_block: range.from_block,
            to_block: range.to_block,
            message: format!(
                "state shard digest missing or invalid for pool {}",
                pool_address
            ),
        });
    }

    let actual_line_count = count_jsonl_lines(&expected_abs)?;
    if actual_line_count != entry.line_count {
        return Err(ExportError::ResumeMismatch {
            from_block: range.from_block,
            to_block: range.to_block,
            message: format!(
                "state shard line-count mismatch for pool {}: manifest={}, file={}",
                pool_address, entry.line_count, actual_line_count
            ),
        });
    }

    let actual_digest = sha256_hex_file_bytes(&expected_abs)?;
    if actual_digest != entry.digest {
        return Err(ExportError::ResumeMismatch {
            from_block: range.from_block,
            to_block: range.to_block,
            message: format!(
                "state shard digest mismatch for pool {}: manifest={}, file={}",
                pool_address, entry.digest, actual_digest
            ),
        });
    }

    Ok(Some(entry))
}

fn find_resolved_pool_entry<'a>(
    catalog: &'a ResolvedPoolCatalog,
    pool_address: &str,
) -> Result<&'a ResolvedPoolCatalogEntry, ExportError> {
    catalog
        .resolved
        .iter()
        .find(|pool| pool.pool_address == pool_address)
        .ok_or_else(|| ExportError::InvalidRequest {
            message: format!("pool {} missing from resolved catalog", pool_address),
        })
}

fn build_transfer_deltas_by_pool(
    blocks: &[BlockWithReceiptsRef],
    plans: &[PoolShardTargetPlan],
    pool_meta_map: &HashMap<String, PoolTokenMeta>,
) -> Result<HashMap<String, BTreeMap<u64, PoolTokenDelta>>, ExportError> {
    let tracked_pools = plans
        .iter()
        .map(|plan| plan.pool_address.as_str())
        .collect::<BTreeSet<_>>();
    let mut out = HashMap::<String, BTreeMap<u64, PoolTokenDelta>>::new();

    for block in blocks {
        for receipt in &block.receipts {
            for log in &receipt.logs {
                if !is_transfer_topic(log.topics.first()) {
                    continue;
                }

                match log.topics.len() {
                    3 => {
                        let from_address = decode_topic_address("transfer.topic1", &log.topics[1])?;
                        let to_address = decode_topic_address("transfer.topic2", &log.topics[2])?;
                        let applies_to_from_pool = tracked_pools.contains(from_address.as_str())
                            && pool_tracks_token(pool_meta_map, &from_address, &log.address);
                        let applies_to_to_pool = to_address != from_address
                            && tracked_pools.contains(to_address.as_str())
                            && pool_tracks_token(pool_meta_map, &to_address, &log.address);
                        if !applies_to_from_pool && !applies_to_to_pool {
                            continue;
                        }

                        let amount = parse_hex_u256_to_bigint("transfer.data", &log.data).map_err(
                            |error| match error {
                                ExportError::InvalidRequest { message } => {
                                    ExportError::InvalidRequest {
                                        message: format!(
                                            "{message}; token/log address={}, block_number={}, tx_hash={}, log_index={}",
                                            log.address,
                                            receipt.block_number,
                                            receipt.transaction_hash,
                                            log.log_index
                                        ),
                                    }
                                }
                                other => other,
                            },
                        )?;
                        if amount.is_zero() {
                            continue;
                        }

                        if applies_to_from_pool {
                            apply_transfer_delta(
                                &mut out,
                                block.header.block_number,
                                &from_address,
                                &log.address,
                                &(-amount.clone()),
                                pool_meta_map,
                            );
                        }
                        if applies_to_to_pool {
                            apply_transfer_delta(
                                &mut out,
                                block.header.block_number,
                                &to_address,
                                &log.address,
                                &amount,
                                pool_meta_map,
                            );
                        }
                    }
                    4 => continue,
                    _ => continue,
                }
            }
        }
    }

    Ok(out)
}

fn pool_tracks_token(
    pool_meta_map: &HashMap<String, PoolTokenMeta>,
    pool_address: &str,
    token_address: &str,
) -> bool {
    match pool_meta_map.get(pool_address) {
        Some(meta) => token_address == meta.token0 || token_address == meta.token1,
        None => false,
    }
}

fn apply_transfer_delta(
    delta_map: &mut HashMap<String, BTreeMap<u64, PoolTokenDelta>>,
    block_number: u64,
    pool_address: &str,
    token_address: &str,
    amount: &BigInt,
    pool_meta_map: &HashMap<String, PoolTokenMeta>,
) {
    let Some(meta) = pool_meta_map.get(pool_address) else {
        return;
    };
    if token_address != meta.token0 && token_address != meta.token1 {
        return;
    }

    let pool_delta = delta_map
        .entry(pool_address.to_owned())
        .or_default()
        .entry(block_number)
        .or_default();
    if token_address == meta.token0 {
        pool_delta.token0 += amount;
    }
    if token_address == meta.token1 {
        pool_delta.token1 += amount;
    }
}

fn build_pool_shard_state<C>(
    request: &StateExportRequest,
    node_adapter: &BaseNodeRpcAdapter<C>,
    pool: &ResolvedPoolCatalogEntry,
    plan: &PoolShardTargetPlan,
    transfer_deltas: &HashMap<String, BTreeMap<u64, PoolTokenDelta>>,
) -> Result<(StateShardManifestEntry, StateValidationReportItem, bool), ExportError>
where
    C: JsonRpcClient,
{
    let target_count = plan.target_blocks.len() as u64;
    if target_count == 0 {
        return Err(ExportError::InvalidRequest {
            message: format!(
                "state target extraction produced empty target set for pool {} shard {}_{}",
                plan.pool_address, plan.from_block, plan.to_block
            ),
        });
    }

    let first_block = plan.target_blocks[0];
    let mut checkpoint_count = 0u64;
    let mut drifts = Vec::<StateDriftDetail>::new();
    let mut used_fallback = false;

    let mut current_token0 = fetch_balance_as_bigint(
        node_adapter,
        &pool.token0.address,
        &pool.pool_address,
        first_block,
    )?;
    let mut current_token1 = fetch_balance_as_bigint(
        node_adapter,
        &pool.token1.address,
        &pool.pool_address,
        first_block,
    )?;
    let mut incremental_points =
        vec![(first_block, current_token0.clone(), current_token1.clone())];

    let delta_by_block = transfer_deltas.get(&plan.pool_address);
    let stride = request.validation_stride_targets as usize;

    let mut prev_target = first_block;
    let mut drift_found = false;
    for (idx, target_block) in plan.target_blocks.iter().enumerate().skip(1) {
        if let Some(per_block) = delta_by_block {
            for (_, delta) in per_block.range((prev_target + 1)..=*target_block) {
                current_token0 += delta.token0.clone();
                current_token1 += delta.token1.clone();
            }
        }
        if is_checkpoint_target(idx, plan.target_blocks.len(), stride) {
            checkpoint_count += 1;
            let exact_token0 = fetch_balance_as_bigint(
                node_adapter,
                &pool.token0.address,
                &pool.pool_address,
                *target_block,
            )?;
            let exact_token1 = fetch_balance_as_bigint(
                node_adapter,
                &pool.token1.address,
                &pool.pool_address,
                *target_block,
            )?;
            let drift0 = exact_token0.clone() - current_token0.clone();
            let drift1 = exact_token1.clone() - current_token1.clone();
            if !drift0.is_zero() || !drift1.is_zero() {
                drifts.push(StateDriftDetail {
                    block_number: *target_block,
                    token0_drift: drift0.to_string(),
                    token1_drift: drift1.to_string(),
                });
                drift_found = true;
                break;
            }
            current_token0 = exact_token0;
            current_token1 = exact_token1;
        }
        incremental_points.push((
            *target_block,
            current_token0.clone(),
            current_token1.clone(),
        ));
        prev_target = *target_block;
    }

    let (points, generation_mode) = if drift_found {
        used_fallback = true;
        let mut exact_points = Vec::<(u64, BigInt, BigInt)>::new();
        for block_number in &plan.target_blocks {
            let token0 = fetch_balance_as_bigint(
                node_adapter,
                &pool.token0.address,
                &pool.pool_address,
                *block_number,
            )?;
            let token1 = fetch_balance_as_bigint(
                node_adapter,
                &pool.token1.address,
                &pool.pool_address,
                *block_number,
            )?;
            exact_points.push((*block_number, token0, token1));
        }
        (exact_points, StateShardGenerationMode::ExactFallback)
    } else {
        (
            incremental_points,
            StateShardGenerationMode::IncrementalValidated,
        )
    };

    let mut lines = Vec::<String>::new();
    for (block_number, token0, token1) in points {
        lines.push(encode_state_line(
            &pool.pool_address,
            block_number,
            &token0,
            &token1,
        )?);
    }
    let line_count = lines.len() as u64;
    let file_rel = state_relative_path(
        &pool.pool_address,
        ShardRange {
            from_block: plan.from_block,
            to_block: plan.to_block,
        },
    );
    let file_abs = request.run_root.join(&file_rel);
    write_jsonl_file(&file_abs, &lines)?;
    let digest = sha256_hex_file_bytes(&file_abs)?;

    let entry = StateShardManifestEntry {
        pool_address: pool.pool_address.clone(),
        from_block: plan.from_block,
        to_block: plan.to_block,
        line_count,
        file: file_rel,
        digest,
        generation_mode: generation_mode.clone(),
    };
    let report = StateValidationReportItem {
        pool_address: pool.pool_address.clone(),
        from_block: plan.from_block,
        to_block: plan.to_block,
        target_count,
        checkpoint_count,
        used_exact_fallback: used_fallback,
        generation_mode,
        drifts,
    };
    Ok((entry, report, used_fallback))
}

fn fetch_balance_as_bigint<C>(
    node_adapter: &BaseNodeRpcAdapter<C>,
    token_address: &str,
    owner_address: &str,
    block_number: u64,
) -> Result<BigInt, ExportError>
where
    C: JsonRpcClient,
{
    let decimal = node_adapter.fetch_erc20_balance(token_address, owner_address, block_number)?;
    parse_decimal_to_bigint("eth_call.balanceOf", &decimal)
}

fn parse_decimal_to_bigint(field: &'static str, value: &str) -> Result<BigInt, ExportError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ExportError::InvalidRequest {
            message: format!("invalid decimal value for {field}: {value}"),
        });
    }
    BigInt::from_str_radix(trimmed, 10).map_err(|_| ExportError::InvalidRequest {
        message: format!("failed to parse decimal value for {field}: {value}"),
    })
}

fn parse_hex_u256_to_bigint(field: &'static str, value: &str) -> Result<BigInt, ExportError> {
    let trimmed = value.trim();
    let digits = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if digits.len() != 64 || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ExportError::InvalidRequest {
            message: format!("{field} must be exactly 64 hex chars (optional 0x prefix)"),
        });
    }
    let unsigned =
        BigUint::from_str_radix(digits, 16).map_err(|_| ExportError::InvalidRequest {
            message: format!("failed to parse {field} as uint256"),
        })?;
    Ok(BigInt::from_biguint(Sign::Plus, unsigned))
}

fn decode_topic_address(field: &'static str, topic: &str) -> Result<String, ExportError> {
    let normalized = crate::source::normalize_prefixed_hex(field, topic, 64)?;
    let digits = normalized
        .strip_prefix("0x")
        .ok_or_else(|| ExportError::InvalidRequest {
            message: format!("{field} must be 0x-prefixed"),
        })?;
    Ok(format!("0x{}", &digits[24..]))
}

fn is_transfer_topic(topic0: Option<&String>) -> bool {
    match topic0 {
        Some(value) => value.eq_ignore_ascii_case(TRANSFER_TOPIC0),
        None => false,
    }
}

fn is_checkpoint_target(index: usize, total: usize, stride: usize) -> bool {
    if index + 1 == total {
        return true;
    }
    index % stride == 0
}

fn state_relative_path(pool_address: &str, range: ShardRange) -> String {
    format!(
        "state/{}/{}_{}.jsonl",
        pool_address, range.from_block, range.to_block
    )
}

fn encode_state_line(
    pool_address: &str,
    block_number: u64,
    token0_balance: &BigInt,
    token1_balance: &BigInt,
) -> Result<String, ExportError> {
    if token0_balance.sign() == Sign::Minus || token1_balance.sign() == Sign::Minus {
        return Err(ExportError::InvalidRequest {
            message: format!(
                "negative token balance encountered for pool {}",
                pool_address
            ),
        });
    }
    let state = StateLine {
        pool_address: pool_address.to_owned(),
        block_number: Value::String(block_number.to_string()),
        token0_balance_raw: Value::String(token0_balance.to_string()),
        token1_balance_raw: Value::String(token1_balance.to_string()),
    };
    let line = serde_json::to_string(&state).map_err(|source| ExportError::JsonEncode {
        label: "state line".to_owned(),
        source,
    })?;
    let _ = validate_state_line_str(&line)?;
    Ok(line)
}

fn write_jsonl_file(path: &Path, lines: &[String]) -> Result<(), ExportError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ExportError::IoWrite {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let file = fs::File::create(path).map_err(|source| ExportError::IoWrite {
        path: path.to_path_buf(),
        source,
    })?;
    let mut writer = BufWriter::new(file);
    for line in lines {
        writer
            .write_all(line.as_bytes())
            .map_err(|source| ExportError::IoWrite {
                path: path.to_path_buf(),
                source,
            })?;
        writer
            .write_all(b"\n")
            .map_err(|source| ExportError::IoWrite {
                path: path.to_path_buf(),
                source,
            })?;
    }
    writer.flush().map_err(|source| ExportError::IoWrite {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn write_validation_report(
    run_root: &Path,
    items: &[StateValidationReportItem],
) -> Result<(), ExportError> {
    let report = StateValidationReport {
        items: items.to_vec(),
    };
    let encoded =
        serde_json::to_string_pretty(&report).map_err(|source| ExportError::JsonEncode {
            label: "state/validation_report.json".to_owned(),
            source,
        })?;
    let path = run_root.join(VALIDATION_REPORT_PATH);
    fs::write(&path, encoded).map_err(|source| ExportError::IoWrite { path, source })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{
        BlockHeaderRef, BlockWithReceiptsRef, ReceiptLogRef, TransactionReceiptRef,
    };

    fn encode_u256_word(value: u64) -> String {
        format!("0x{value:064x}")
    }

    fn encode_topic_address(address: &str) -> String {
        let digits = address
            .strip_prefix("0x")
            .or_else(|| address.strip_prefix("0X"))
            .unwrap_or(address);
        format!("0x{:0>24}{digits}", "")
    }

    fn build_block_with_one_log(
        block_number: u64,
        tx_hash: &str,
        log_index: u64,
        log_address: &str,
        topics: Vec<String>,
        data: &str,
    ) -> BlockWithReceiptsRef {
        BlockWithReceiptsRef {
            header: BlockHeaderRef {
                block_number,
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                timestamp_secs: 0,
            },
            receipts: vec![TransactionReceiptRef {
                transaction_hash: tx_hash.to_owned(),
                transaction_index: 0,
                block_number,
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                logs: vec![ReceiptLogRef {
                    address: log_address.to_owned(),
                    topics,
                    data: data.to_owned(),
                    log_index,
                    removed: false,
                }],
            }],
        }
    }

    fn build_pool_plan(pool_address: &str, block_number: u64) -> PoolShardTargetPlan {
        PoolShardTargetPlan {
            pool_address: pool_address.to_owned(),
            from_block: block_number,
            to_block: block_number,
            target_blocks: vec![block_number],
        }
    }

    fn build_pool_meta_map(
        pool_address: &str,
        token0: &str,
        token1: &str,
    ) -> HashMap<String, PoolTokenMeta> {
        let mut pool_meta_map = HashMap::<String, PoolTokenMeta>::new();
        pool_meta_map.insert(
            pool_address.to_owned(),
            PoolTokenMeta {
                token0: token0.to_owned(),
                token1: token1.to_owned(),
            },
        );
        pool_meta_map
    }

    #[test]
    fn transfer_topics3_with_0x_amount_is_parsed_as_erc20() {
        let pool_address = "0x1111111111111111111111111111111111111111";
        let external_address = "0x9999999999999999999999999999999999999999";
        let token0 = "0x2222222222222222222222222222222222222222";
        let token1 = "0x3333333333333333333333333333333333333333";
        let block_number = 27_800_001u64;
        let plans = vec![build_pool_plan(pool_address, block_number)];
        let pool_meta_map = build_pool_meta_map(pool_address, token0, token1);
        let block = build_block_with_one_log(
            block_number,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
            token0,
            vec![
                TRANSFER_TOPIC0.to_owned(),
                encode_topic_address(pool_address),
                encode_topic_address(external_address),
            ],
            &encode_u256_word(15),
        );

        let out = build_transfer_deltas_by_pool(&[block], &plans, &pool_meta_map)
            .expect("erc20 transfer should parse");

        let per_pool = out
            .get(pool_address)
            .expect("pool delta should exist for tracked pool");
        let delta = per_pool
            .get(&block_number)
            .expect("delta should be recorded for transfer block");
        assert_eq!(delta.token0, BigInt::from(-15));
        assert_eq!(delta.token1, BigInt::from(0));
    }

    #[test]
    fn transfer_topics4_erc721_style_is_ignored() {
        let pool_address = "0x1111111111111111111111111111111111111111";
        let external_address = "0x9999999999999999999999999999999999999999";
        let token0 = "0x2222222222222222222222222222222222222222";
        let token1 = "0x3333333333333333333333333333333333333333";
        let block_number = 27_800_002u64;
        let plans = vec![build_pool_plan(pool_address, block_number)];
        let pool_meta_map = build_pool_meta_map(pool_address, token0, token1);
        let block = build_block_with_one_log(
            block_number,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            1,
            token0,
            vec![
                TRANSFER_TOPIC0.to_owned(),
                encode_topic_address(pool_address),
                encode_topic_address(external_address),
                encode_u256_word(42),
            ],
            "0x",
        );

        let out = build_transfer_deltas_by_pool(&[block], &plans, &pool_meta_map)
            .expect("erc721-style transfer should be ignored");
        assert!(
            out.is_empty(),
            "erc721-style transfer must not affect deltas"
        );
    }

    #[test]
    fn transfer_topics3_invalid_amount_for_untracked_addresses_is_ignored() {
        let pool_address = "0x1111111111111111111111111111111111111111";
        let external_address = "0x9999999999999999999999999999999999999999";
        let unrelated_address = "0x8888888888888888888888888888888888888888";
        let token0 = "0x2222222222222222222222222222222222222222";
        let token1 = "0x3333333333333333333333333333333333333333";
        let block_number = 27_800_003u64;
        let plans = vec![build_pool_plan(pool_address, block_number)];
        let pool_meta_map = build_pool_meta_map(pool_address, token0, token1);
        let block = build_block_with_one_log(
            block_number,
            "0xabababababababababababababababababababababababababababababababab",
            2,
            token0,
            vec![
                TRANSFER_TOPIC0.to_owned(),
                encode_topic_address(external_address),
                encode_topic_address(unrelated_address),
            ],
            "0x1234",
        );

        let out = build_transfer_deltas_by_pool(&[block], &plans, &pool_meta_map)
            .expect("untracked invalid transfer should be ignored");
        assert!(
            out.is_empty(),
            "untracked invalid transfer must not affect deltas"
        );
    }

    #[test]
    fn transfer_topics3_invalid_amount_for_untracked_token_is_ignored() {
        let pool_address = "0x1111111111111111111111111111111111111111";
        let external_address = "0x9999999999999999999999999999999999999999";
        let token0 = "0x2222222222222222222222222222222222222222";
        let token1 = "0x3333333333333333333333333333333333333333";
        let unrelated_token = "0x4444444444444444444444444444444444444444";
        let block_number = 27_800_004u64;
        let plans = vec![build_pool_plan(pool_address, block_number)];
        let pool_meta_map = build_pool_meta_map(pool_address, token0, token1);
        let block = build_block_with_one_log(
            block_number,
            "0xbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc",
            3,
            unrelated_token,
            vec![
                TRANSFER_TOPIC0.to_owned(),
                encode_topic_address(pool_address),
                encode_topic_address(external_address),
            ],
            "0x1234",
        );

        let out = build_transfer_deltas_by_pool(&[block], &plans, &pool_meta_map)
            .expect("tracked pool transfer with unrelated token should be ignored");
        assert!(
            out.is_empty(),
            "transfer with unrelated token must not affect deltas"
        );
    }

    #[test]
    fn transfer_topics3_invalid_amount_has_log_context() {
        let pool_address = "0x1111111111111111111111111111111111111111";
        let external_address = "0x9999999999999999999999999999999999999999";
        let token0 = "0x2222222222222222222222222222222222222222";
        let token1 = "0x3333333333333333333333333333333333333333";
        let block_number = 27_800_005u64;
        let tx_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let log_index = 7u64;
        let plans = vec![build_pool_plan(pool_address, block_number)];
        let pool_meta_map = build_pool_meta_map(pool_address, token0, token1);
        let block = build_block_with_one_log(
            block_number,
            tx_hash,
            log_index,
            token0,
            vec![
                TRANSFER_TOPIC0.to_owned(),
                encode_topic_address(pool_address),
                encode_topic_address(external_address),
            ],
            "0x1234",
        );

        let error = build_transfer_deltas_by_pool(&[block], &plans, &pool_meta_map)
            .expect_err("invalid erc20 amount should fail fast");
        let message = match error {
            ExportError::InvalidRequest { message } => message,
            other => panic!("unexpected error variant: {other}"),
        };
        assert!(message.contains("transfer.data"));
        assert!(message.contains(token0));
        assert!(message.contains(&format!("block_number={block_number}")));
        assert!(message.contains(tx_hash));
        assert!(message.contains(&format!("log_index={log_index}")));
    }
}
