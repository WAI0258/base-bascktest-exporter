use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ExportError, RawEventCounts, RawEventKind, RawExportRequest, RawExportResult, RawShardDigests,
    RawShardFiles, RawShardManifestEntry, StateShardManifestEntry,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ShardRange {
    pub from_block: u64,
    pub to_block: u64,
}

#[derive(Debug, Default)]
pub(crate) struct ShardEventLines {
    pub swap: Vec<String>,
    pub mint: Vec<String>,
    pub burn: Vec<String>,
    pub collect: Vec<String>,
}

impl ShardEventLines {
    fn by_event(&self, event: RawEventKind) -> &Vec<String> {
        match event {
            RawEventKind::Swap => &self.swap,
            RawEventKind::Mint => &self.mint,
            RawEventKind::Burn => &self.burn,
            RawEventKind::Collect => &self.collect,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManifestFile {
    pub raw_shards: Vec<RawShardManifestEntry>,
    pub totals: RawEventCounts,
    pub skipped_existing_shards: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub state_shards: Vec<StateShardManifestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaFile {
    chain: String,
    from_block: u64,
    to_block: u64,
    shard_size_blocks: u64,
    selected_pool_count: u64,
}

pub(crate) fn build_shard_plan(
    from_block: u64,
    to_block: u64,
    shard_size_blocks: u64,
) -> Vec<ShardRange> {
    let mut out = Vec::new();
    let mut current = from_block;
    while current <= to_block {
        let end = current
            .saturating_add(shard_size_blocks.saturating_sub(1))
            .min(to_block);
        out.push(ShardRange {
            from_block: current,
            to_block: end,
        });
        if end == u64::MAX {
            break;
        }
        current = end.saturating_add(1);
    }
    out
}

pub(crate) fn ensure_output_dirs(run_root: &Path) -> Result<(), ExportError> {
    for event in RawEventKind::ALL {
        let dir = run_root.join("raw").join(event.as_str());
        fs::create_dir_all(&dir).map_err(|source| ExportError::IoWrite { path: dir, source })?;
    }
    Ok(())
}

pub(crate) fn shard_relative_path(event: RawEventKind, range: ShardRange) -> String {
    format!(
        "raw/{}/{}_{}.jsonl",
        event.as_str(),
        range.from_block,
        range.to_block
    )
}

pub(crate) fn load_existing_manifest(run_root: &Path) -> Result<Option<ManifestFile>, ExportError> {
    let path = run_root.join("manifest.json");
    if !path.is_file() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&path).map_err(|source| ExportError::IoRead {
        path: path.clone(),
        source,
    })?;
    let manifest = serde_json::from_str::<ManifestFile>(&contents).map_err(|source| {
        ExportError::JsonDecode {
            label: "manifest.json".to_owned(),
            source,
        }
    })?;
    Ok(Some(manifest))
}

pub(crate) fn try_skip_existing_shard(
    run_root: &Path,
    range: ShardRange,
    existing_manifest: Option<&ManifestFile>,
) -> Result<Option<RawShardManifestEntry>, ExportError> {
    let has_existing_files = RawEventKind::ALL.iter().any(|event| {
        let rel = shard_relative_path(*event, range);
        run_root.join(rel).is_file()
    });
    if !has_existing_files {
        return Ok(None);
    }

    let entry = existing_manifest
        .and_then(|manifest| {
            manifest
                .raw_shards
                .iter()
                .find(|entry| entry.from_block == range.from_block && entry.to_block == range.to_block)
                .cloned()
        })
        .unwrap_or(rebuild_manifest_entry_from_files(run_root, range)?);

    validate_manifest_entry_matches_files(run_root, range, &entry)?;
    Ok(Some(entry))
}

fn rebuild_manifest_entry_from_files(
    run_root: &Path,
    range: ShardRange,
) -> Result<RawShardManifestEntry, ExportError> {
    let mut files = RawShardFiles::default();
    let mut digests = RawShardDigests::default();
    let mut counts = RawEventCounts::default();

    for event in RawEventKind::ALL {
        let rel = shard_relative_path(event, range);
        let abs = run_root.join(&rel);
        if !abs.is_file() {
            continue;
        }

        let line_count = count_jsonl_lines(&abs)?;
        let digest = sha256_hex_file_bytes(&abs)?;
        assign_file_path(&mut files, event, rel);
        assign_file_digest(&mut digests, event, digest);
        for _ in 0..line_count {
            counts.add_event(event);
        }
    }

    Ok(RawShardManifestEntry {
        from_block: range.from_block,
        to_block: range.to_block,
        counts,
        files,
        digests,
    })
}

fn validate_manifest_entry_matches_files(
    run_root: &Path,
    range: ShardRange,
    entry: &RawShardManifestEntry,
) -> Result<(), ExportError> {
    for event in RawEventKind::ALL {
        let expected_rel = shard_relative_path(event, range);
        let expected_abs = run_root.join(&expected_rel);
        let declared_count = entry.counts.by_event(event);
        let declared_path = file_path_by_event(&entry.files, event);
        let declared_digest = file_digest_by_event(&entry.digests, event);
        let file_exists = expected_abs.is_file();

        if declared_count == 0 {
            if declared_path.is_some() {
                return Err(ExportError::ResumeMismatch {
                    from_block: range.from_block,
                    to_block: range.to_block,
                    message: format!(
                        "event {} has zero count but manifest path is present",
                        event.as_str()
                    ),
                });
            }
            if declared_digest.is_some() {
                return Err(ExportError::ResumeMismatch {
                    from_block: range.from_block,
                    to_block: range.to_block,
                    message: format!(
                        "event {} has zero count but manifest digest is present",
                        event.as_str()
                    ),
                });
            }
            if file_exists {
                return Err(ExportError::ResumeMismatch {
                    from_block: range.from_block,
                    to_block: range.to_block,
                    message: format!(
                        "event {} has zero count but shard file exists",
                        event.as_str()
                    ),
                });
            }
            continue;
        }

        if declared_path.map(|path| path.as_str()) != Some(expected_rel.as_str()) {
            return Err(ExportError::ResumeMismatch {
                from_block: range.from_block,
                to_block: range.to_block,
                message: format!(
                    "event {} manifest path mismatch: expected {}",
                    event.as_str(),
                    expected_rel
                ),
            });
        }
        if !file_exists {
            return Err(ExportError::ResumeMismatch {
                from_block: range.from_block,
                to_block: range.to_block,
                message: format!(
                    "event {} manifest declares data but file is missing",
                    event.as_str()
                ),
            });
        }
        let actual_line_count = count_jsonl_lines(&expected_abs)?;
        if actual_line_count != declared_count {
            return Err(ExportError::ResumeMismatch {
                from_block: range.from_block,
                to_block: range.to_block,
                message: format!(
                    "event {} line-count mismatch: manifest={}, file={}",
                    event.as_str(),
                    declared_count,
                    actual_line_count
                ),
            });
        }

        let declared_digest = declared_digest.ok_or_else(|| ExportError::ResumeMismatch {
            from_block: range.from_block,
            to_block: range.to_block,
            message: format!(
                "event {} has non-zero count but manifest digest is missing",
                event.as_str()
            ),
        })?;
        if !is_lowercase_sha256_hex(declared_digest) {
            return Err(ExportError::ResumeMismatch {
                from_block: range.from_block,
                to_block: range.to_block,
                message: format!(
                    "event {} manifest digest format is invalid: expected lowercase hex SHA-256",
                    event.as_str()
                ),
            });
        }
        let actual_digest = sha256_hex_file_bytes(&expected_abs)?;
        if declared_digest.as_str() != actual_digest.as_str() {
            return Err(ExportError::ResumeMismatch {
                from_block: range.from_block,
                to_block: range.to_block,
                message: format!(
                    "event {} digest mismatch: manifest={}, file={}",
                    event.as_str(),
                    declared_digest,
                    actual_digest
                ),
            });
        }
    }

    Ok(())
}

pub(crate) fn count_jsonl_lines(path: &Path) -> Result<u64, ExportError> {
    let file = fs::File::open(path).map_err(|source| ExportError::IoRead {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = BufReader::new(file);
    let mut count = 0u64;
    for line in reader.lines() {
        let text = line.map_err(|source| ExportError::IoRead {
            path: path.to_path_buf(),
            source,
        })?;
        if !text.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

pub(crate) fn sha256_hex_file_bytes(path: &Path) -> Result<String, ExportError> {
    let mut file = fs::File::open(path).map_err(|source| ExportError::IoRead {
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8 * 1024];
    loop {
        let read_len = file
            .read(&mut buffer)
            .map_err(|source| ExportError::IoRead {
                path: path.to_path_buf(),
                source,
            })?;
        if read_len == 0 {
            break;
        }
        hasher.update(&buffer[..read_len]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub(crate) fn is_lowercase_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (*byte >= b'a' && *byte <= b'f'))
}

pub(crate) fn write_shard_files(
    run_root: &Path,
    range: ShardRange,
    lines: &ShardEventLines,
) -> Result<RawShardManifestEntry, ExportError> {
    let mut files = RawShardFiles::default();
    let mut digests = RawShardDigests::default();
    let mut counts = RawEventCounts::default();

    for event in RawEventKind::ALL {
        let event_lines = lines.by_event(event);
        if event_lines.is_empty() {
            continue;
        }

        let rel = shard_relative_path(event, range);
        let abs = run_root.join(&rel);
        write_jsonl_file(&abs, event_lines)?;
        let digest = sha256_hex_file_bytes(&abs)?;
        assign_file_path(&mut files, event, rel);
        assign_file_digest(&mut digests, event, digest);
        for _ in event_lines {
            counts.add_event(event);
        }
    }

    Ok(RawShardManifestEntry {
        from_block: range.from_block,
        to_block: range.to_block,
        counts,
        files,
        digests,
    })
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

pub(crate) fn write_meta_file(
    request: &RawExportRequest,
    selected_pool_count: u64,
) -> Result<(), ExportError> {
    let mut meta = MetaFile {
        chain: "base".to_owned(),
        from_block: request.from_block,
        to_block: request.to_block,
        shard_size_blocks: request.shard_size_blocks,
        selected_pool_count,
    };
    let meta_path = request.run_root.join("meta.json");
    if meta_path.is_file() {
        let contents = fs::read_to_string(&meta_path).map_err(|source| ExportError::IoRead {
            path: meta_path.clone(),
            source,
        })?;
        let existing = serde_json::from_str::<MetaFile>(&contents).map_err(|source| {
            ExportError::JsonDecode {
                label: "meta.json".to_owned(),
                source,
            }
        })?;
        if existing.chain == meta.chain
            && existing.shard_size_blocks == meta.shard_size_blocks
            && existing.selected_pool_count == meta.selected_pool_count
        {
            meta.from_block = existing.from_block.min(meta.from_block);
            meta.to_block = existing.to_block.max(meta.to_block);
        }
    }
    write_json_object(&meta_path, &meta, "meta.json")
}

pub(crate) fn write_manifest_file(
    result: &RawExportResult,
    run_root: &Path,
) -> Result<(), ExportError> {
    let existing_manifest = load_existing_manifest(run_root)?;
    let existing_state_shards = existing_manifest
        .as_ref()
        .map(|value| value.state_shards.clone())
        .unwrap_or_default();
    let merged_raw_shards = merge_raw_shards(
        existing_manifest
            .as_ref()
            .map(|value| value.raw_shards.as_slice())
            .unwrap_or(&[]),
        &result.shard_entries,
    );
    let manifest = ManifestFile {
        totals: total_counts_for_shards(&merged_raw_shards),
        raw_shards: merged_raw_shards,
        skipped_existing_shards: result.skipped_existing_shards,
        state_shards: existing_state_shards,
    };
    write_json_object(&run_root.join("manifest.json"), &manifest, "manifest.json")
}

fn merge_raw_shards(
    existing_entries: &[RawShardManifestEntry],
    new_entries: &[RawShardManifestEntry],
) -> Vec<RawShardManifestEntry> {
    let mut merged = BTreeMap::<(u64, u64), RawShardManifestEntry>::new();
    for entry in existing_entries {
        merged.insert((entry.from_block, entry.to_block), entry.clone());
    }
    for entry in new_entries {
        merged.insert((entry.from_block, entry.to_block), entry.clone());
    }
    merged.into_values().collect()
}

fn total_counts_for_shards(entries: &[RawShardManifestEntry]) -> RawEventCounts {
    let mut totals = RawEventCounts::default();
    for entry in entries {
        totals.add_assign(&entry.counts);
    }
    totals
}

pub(crate) fn write_state_manifest_file(
    run_root: &Path,
    existing_manifest: &ManifestFile,
    state_shards: &[StateShardManifestEntry],
) -> Result<(), ExportError> {
    let manifest = ManifestFile {
        raw_shards: existing_manifest.raw_shards.clone(),
        totals: existing_manifest.totals.clone(),
        skipped_existing_shards: existing_manifest.skipped_existing_shards,
        state_shards: merge_state_shards(&existing_manifest.state_shards, state_shards),
    };
    write_json_object(&run_root.join("manifest.json"), &manifest, "manifest.json")
}

fn merge_state_shards(
    existing_entries: &[StateShardManifestEntry],
    new_entries: &[StateShardManifestEntry],
) -> Vec<StateShardManifestEntry> {
    let mut merged = BTreeMap::<(String, u64, u64), StateShardManifestEntry>::new();
    for entry in existing_entries {
        merged.insert(
            (entry.pool_address.clone(), entry.from_block, entry.to_block),
            entry.clone(),
        );
    }
    for entry in new_entries {
        merged.insert(
            (entry.pool_address.clone(), entry.from_block, entry.to_block),
            entry.clone(),
        );
    }
    merged.into_values().collect()
}

fn write_json_object<T>(path: &Path, value: &T, label: &str) -> Result<(), ExportError>
where
    T: Serialize,
{
    let encoded =
        serde_json::to_string_pretty(value).map_err(|source| ExportError::JsonEncode {
            label: label.to_owned(),
            source,
        })?;
    fs::write(path, encoded).map_err(|source| ExportError::IoWrite {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn assign_file_path(files: &mut RawShardFiles, event: RawEventKind, path: String) {
    match event {
        RawEventKind::Swap => files.swap = Some(path),
        RawEventKind::Mint => files.mint = Some(path),
        RawEventKind::Burn => files.burn = Some(path),
        RawEventKind::Collect => files.collect = Some(path),
    }
}

fn file_path_by_event(files: &RawShardFiles, event: RawEventKind) -> Option<&String> {
    match event {
        RawEventKind::Swap => files.swap.as_ref(),
        RawEventKind::Mint => files.mint.as_ref(),
        RawEventKind::Burn => files.burn.as_ref(),
        RawEventKind::Collect => files.collect.as_ref(),
    }
}

fn assign_file_digest(digests: &mut RawShardDigests, event: RawEventKind, digest: String) {
    match event {
        RawEventKind::Swap => digests.swap = Some(digest),
        RawEventKind::Mint => digests.mint = Some(digest),
        RawEventKind::Burn => digests.burn = Some(digest),
        RawEventKind::Collect => digests.collect = Some(digest),
    }
}

fn file_digest_by_event(digests: &RawShardDigests, event: RawEventKind) -> Option<&String> {
    match event {
        RawEventKind::Swap => digests.swap.as_ref(),
        RawEventKind::Mint => digests.mint.as_ref(),
        RawEventKind::Burn => digests.burn.as_ref(),
        RawEventKind::Collect => digests.collect.as_ref(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::RawExportTotals;
    use tempfile::TempDir;

    #[test]
    fn try_skip_existing_shard_rebuilds_missing_manifest_entry_from_files() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        ensure_output_dirs(&run_root)
            .unwrap_or_else(|error| panic!("ensure_output_dirs failed: {error}"));
        let range = ShardRange {
            from_block: 100,
            to_block: 199,
        };
        let mut lines = ShardEventLines::default();
        lines.swap.push(r#"{"kind":"swap"}"#.to_owned());
        let expected = write_shard_files(&run_root, range, &lines)
            .unwrap_or_else(|error| panic!("write_shard_files failed: {error}"));
        let manifest = ManifestFile {
            raw_shards: Vec::new(),
            totals: RawEventCounts::default(),
            skipped_existing_shards: 0,
            state_shards: Vec::new(),
        };
        write_json_object(&run_root.join("manifest.json"), &manifest, "manifest.json")
            .unwrap_or_else(|error| panic!("write_json_object failed: {error}"));

        let loaded_manifest = load_existing_manifest(&run_root)
            .unwrap_or_else(|error| panic!("load_existing_manifest failed: {error}"));
        let rebuilt = try_skip_existing_shard(&run_root, range, loaded_manifest.as_ref())
            .unwrap_or_else(|error| panic!("try_skip_existing_shard failed: {error}"))
            .unwrap_or_else(|| panic!("expected existing shard entry"));

        assert_eq!(rebuilt, expected);
    }

    #[test]
    fn write_meta_file_merges_block_range_across_invocations() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        fs::create_dir_all(&run_root)
            .unwrap_or_else(|error| panic!("create_dir_all failed: {error}"));
        let first_request = RawExportRequest {
            run_root: run_root.clone(),
            from_block: 200,
            to_block: 299,
            shard_size_blocks: 10,
        };
        write_meta_file(&first_request, 5)
            .unwrap_or_else(|error| panic!("write_meta_file first failed: {error}"));

        let second_request = RawExportRequest {
            run_root: run_root.clone(),
            from_block: 100,
            to_block: 199,
            shard_size_blocks: 10,
        };
        write_meta_file(&second_request, 5)
            .unwrap_or_else(|error| panic!("write_meta_file second failed: {error}"));

        let contents = fs::read_to_string(run_root.join("meta.json"))
            .unwrap_or_else(|error| panic!("read meta.json failed: {error}"));
        let meta = serde_json::from_str::<MetaFile>(&contents)
            .unwrap_or_else(|error| panic!("parse meta.json failed: {error}"));
        assert_eq!(meta.from_block, 100);
        assert_eq!(meta.to_block, 299);
        assert_eq!(meta.shard_size_blocks, 10);
        assert_eq!(meta.selected_pool_count, 5);
    }

    #[test]
    fn write_state_manifest_file_merges_state_shards_across_invocations() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        fs::create_dir_all(&run_root)
            .unwrap_or_else(|error| panic!("create_dir_all failed: {error}"));

        let existing_manifest = ManifestFile {
            raw_shards: Vec::new(),
            totals: RawEventCounts::default(),
            skipped_existing_shards: 0,
            state_shards: vec![StateShardManifestEntry {
                pool_address: "0x1111111111111111111111111111111111111111".to_owned(),
                from_block: 100,
                to_block: 199,
                line_count: 1,
                file: "state/0x1111111111111111111111111111111111111111/100_199.jsonl".to_owned(),
                digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                generation_mode: super::super::StateShardGenerationMode::IncrementalValidated,
            }],
        };
        write_json_object(&run_root.join("manifest.json"), &existing_manifest, "manifest.json")
            .unwrap_or_else(|error| panic!("write_json_object failed: {error}"));

        let new_state_shards = vec![StateShardManifestEntry {
            pool_address: "0x2222222222222222222222222222222222222222".to_owned(),
            from_block: 200,
            to_block: 299,
            line_count: 2,
            file: "state/0x2222222222222222222222222222222222222222/200_299.jsonl".to_owned(),
            digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            generation_mode: super::super::StateShardGenerationMode::ExactFallback,
        }];
        write_state_manifest_file(&run_root, &existing_manifest, &new_state_shards)
            .unwrap_or_else(|error| panic!("write_state_manifest_file failed: {error}"));

        let manifest = load_existing_manifest(&run_root)
            .unwrap_or_else(|error| panic!("load_existing_manifest failed: {error}"))
            .unwrap_or_else(|| panic!("expected manifest"));
        assert_eq!(manifest.state_shards.len(), 2);
        assert_eq!(manifest.state_shards[0].pool_address, "0x1111111111111111111111111111111111111111");
        assert_eq!(manifest.state_shards[1].pool_address, "0x2222222222222222222222222222222222222222");
    }

    #[test]
    fn write_manifest_file_merges_raw_shards_across_invocations() {
        let temp = TempDir::new().unwrap_or_else(|error| panic!("tempdir failed: {error}"));
        let run_root = temp.path().join("run");
        ensure_output_dirs(&run_root)
            .unwrap_or_else(|error| panic!("ensure_output_dirs failed: {error}"));

        let first_range = ShardRange {
            from_block: 100,
            to_block: 199,
        };
        let second_range = ShardRange {
            from_block: 200,
            to_block: 299,
        };

        let mut first_lines = ShardEventLines::default();
        first_lines.swap.push(r#"{"kind":"swap1"}"#.to_owned());
        let first_entry = write_shard_files(&run_root, first_range, &first_lines)
            .unwrap_or_else(|error| panic!("write_shard_files first failed: {error}"));
        let mut first_totals = RawExportTotals::default();
        first_totals.event_counts.add_assign(&first_entry.counts);
        write_manifest_file(
            &RawExportResult {
                shard_entries: vec![first_entry.clone()],
                totals: first_totals,
                skipped_existing_shards: 0,
            },
            &run_root,
        )
        .unwrap_or_else(|error| panic!("write_manifest_file first failed: {error}"));

        let mut second_lines = ShardEventLines::default();
        second_lines.swap.push(r#"{"kind":"swap2"}"#.to_owned());
        let second_entry = write_shard_files(&run_root, second_range, &second_lines)
            .unwrap_or_else(|error| panic!("write_shard_files second failed: {error}"));
        let mut second_totals = RawExportTotals::default();
        second_totals.event_counts.add_assign(&second_entry.counts);
        write_manifest_file(
            &RawExportResult {
                shard_entries: vec![second_entry.clone()],
                totals: second_totals,
                skipped_existing_shards: 0,
            },
            &run_root,
        )
        .unwrap_or_else(|error| panic!("write_manifest_file second failed: {error}"));

        let manifest = load_existing_manifest(&run_root)
            .unwrap_or_else(|error| panic!("load_existing_manifest failed: {error}"))
            .unwrap_or_else(|| panic!("expected manifest"));
        assert_eq!(manifest.raw_shards, vec![first_entry, second_entry]);
        assert_eq!(manifest.totals.swap, 2);
    }
}
