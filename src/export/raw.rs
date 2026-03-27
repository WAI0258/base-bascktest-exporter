use std::collections::HashSet;

use crate::{
    catalog::ResolvedPoolCatalog,
    contract::{validate_target_raw_line, TargetRawTopicLog},
    source::{BaseNodeRpcAdapter, BlockWithReceiptsRef, JsonRpcClient},
};

use super::{
    shard::{
        build_shard_plan, ensure_output_dirs, load_existing_manifest, try_skip_existing_shard,
        write_manifest_file, write_meta_file, write_shard_files, ShardEventLines,
    },
    ExportError, RawEventKind, RawExportRequest, RawExportResult, RawExportTotals,
};

const SWAP_TOPIC0: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const MINT_TOPIC0: &str = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde";
const BURN_TOPIC0: &str = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c";
const COLLECT_TOPIC0: &str = "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0";

#[derive(Debug, Clone)]
struct CanonicalRawLine {
    event: RawEventKind,
    block_number: u64,
    transaction_index: u64,
    log_index: u64,
    transaction_hash: String,
    line: String,
}

pub fn export_raw_range<C>(
    request: &RawExportRequest,
    resolved_catalog: &ResolvedPoolCatalog,
    node_adapter: &BaseNodeRpcAdapter<C>,
) -> Result<RawExportResult, ExportError>
where
    C: JsonRpcClient,
{
    validate_request(request)?;
    ensure_output_dirs(&request.run_root)?;

    let selected_pool_addresses = resolved_catalog
        .resolved
        .iter()
        .map(|entry| entry.pool_address.clone())
        .collect::<HashSet<_>>();
    let selected_pool_count = resolved_catalog.resolved.len() as u64;

    let existing_manifest = load_existing_manifest(&request.run_root)?;
    let shard_plan = build_shard_plan(
        request.from_block,
        request.to_block,
        request.shard_size_blocks,
    );

    let mut result = RawExportResult {
        shard_entries: Vec::new(),
        totals: RawExportTotals::default(),
        skipped_existing_shards: 0,
    };

    for shard in shard_plan {
        if let Some(existing_entry) =
            try_skip_existing_shard(&request.run_root, shard, existing_manifest.as_ref())?
        {
            result
                .totals
                .event_counts
                .add_assign(&existing_entry.counts);
            result.shard_entries.push(existing_entry);
            result.skipped_existing_shards += 1;
            continue;
        }

        let block_refs = node_adapter.fetch_block_range(shard.from_block, shard.to_block)?;
        let lines = collect_shard_lines(&block_refs, &selected_pool_addresses, &mut result.totals)?;
        let entry = write_shard_files(&request.run_root, shard, &lines)?;
        result.totals.event_counts.add_assign(&entry.counts);
        result.shard_entries.push(entry);
    }

    write_meta_file(request, selected_pool_count)?;
    write_manifest_file(&result, &request.run_root)?;
    Ok(result)
}

fn validate_request(request: &RawExportRequest) -> Result<(), ExportError> {
    if request.from_block > request.to_block {
        return Err(ExportError::InvalidRequest {
            message: format!(
                "from_block must be <= to_block, got {} > {}",
                request.from_block, request.to_block
            ),
        });
    }
    if request.shard_size_blocks == 0 {
        return Err(ExportError::InvalidRequest {
            message: "shard_size_blocks must be > 0".to_owned(),
        });
    }
    Ok(())
}

fn collect_shard_lines(
    blocks: &[BlockWithReceiptsRef],
    selected_pool_addresses: &HashSet<String>,
    totals: &mut RawExportTotals,
) -> Result<ShardEventLines, ExportError> {
    let mut collected = Vec::<CanonicalRawLine>::new();

    for block in blocks {
        if block.header.timestamp_secs == 0 {
            return Err(ExportError::InvalidRequest {
                message: format!(
                    "block {} has zero timestamp, export requires a valid block timestamp",
                    block.header.block_number
                ),
            });
        }
        let block_timestamp_hex = format_hex_scalar(block.header.timestamp_secs);

        for receipt in &block.receipts {
            for log in &receipt.logs {
                if !selected_pool_addresses.contains(&log.address) {
                    totals.ignored_non_selected_pool += 1;
                    continue;
                }

                let topic0 = match log.topics.first() {
                    Some(value) => value.as_str(),
                    None => {
                        totals.ignored_non_target_topic += 1;
                        continue;
                    }
                };
                let event = match topic_to_event(topic0) {
                    Some(event) => event,
                    None => {
                        totals.ignored_non_target_topic += 1;
                        continue;
                    }
                };

                let raw_line = TargetRawTopicLog {
                    address: log.address.clone(),
                    topics: log.topics.clone(),
                    data: log.data.clone(),
                    block_number: format_hex_scalar(receipt.block_number),
                    transaction_hash: receipt.transaction_hash.clone(),
                    transaction_index: format_hex_scalar(receipt.transaction_index),
                    block_hash: receipt.block_hash.clone(),
                    log_index: format_hex_scalar(log.log_index),
                    removed: log.removed,
                    block_timestamp: block_timestamp_hex.clone(),
                };
                let line =
                    serde_json::to_string(&raw_line).map_err(|source| ExportError::JsonEncode {
                        label: "target raw line".to_owned(),
                        source,
                    })?;
                let _ = validate_target_raw_line(&line)?;

                collected.push(CanonicalRawLine {
                    event,
                    block_number: receipt.block_number,
                    transaction_index: receipt.transaction_index,
                    log_index: log.log_index,
                    transaction_hash: receipt.transaction_hash.clone(),
                    line,
                });
            }
        }
    }

    collected.sort_by(|left, right| {
        (left.block_number, left.transaction_index, left.log_index).cmp(&(
            right.block_number,
            right.transaction_index,
            right.log_index,
        ))
    });

    let mut dedup_seen = HashSet::<(u64, String, u64)>::new();
    let mut out = ShardEventLines::default();
    for item in collected {
        let key = (
            item.block_number,
            item.transaction_hash.clone(),
            item.log_index,
        );
        if !dedup_seen.insert(key) {
            totals.dropped_duplicates += 1;
            continue;
        }

        match item.event {
            RawEventKind::Swap => out.swap.push(item.line),
            RawEventKind::Mint => out.mint.push(item.line),
            RawEventKind::Burn => out.burn.push(item.line),
            RawEventKind::Collect => out.collect.push(item.line),
        }
    }
    Ok(out)
}

fn format_hex_scalar(value: u64) -> String {
    format!("0x{value:x}")
}

fn topic_to_event(topic0: &str) -> Option<RawEventKind> {
    if topic0.eq_ignore_ascii_case(SWAP_TOPIC0) {
        return Some(RawEventKind::Swap);
    }
    if topic0.eq_ignore_ascii_case(MINT_TOPIC0) {
        return Some(RawEventKind::Mint);
    }
    if topic0.eq_ignore_ascii_case(BURN_TOPIC0) {
        return Some(RawEventKind::Burn);
    }
    if topic0.eq_ignore_ascii_case(COLLECT_TOPIC0) {
        return Some(RawEventKind::Collect);
    }
    None
}
