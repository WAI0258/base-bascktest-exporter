use std::{
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

    let manifest = existing_manifest.ok_or_else(|| ExportError::ResumeMismatch {
        from_block: range.from_block,
        to_block: range.to_block,
        message: "existing shard files found but manifest.json is missing".to_owned(),
    })?;
    let entry = manifest
        .raw_shards
        .iter()
        .find(|entry| entry.from_block == range.from_block && entry.to_block == range.to_block)
        .cloned()
        .ok_or_else(|| ExportError::ResumeMismatch {
            from_block: range.from_block,
            to_block: range.to_block,
            message: "shard files found but shard metadata is missing in manifest.json".to_owned(),
        })?;

    validate_manifest_entry_matches_files(run_root, range, &entry)?;
    Ok(Some(entry))
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
    let meta = MetaFile {
        chain: "base".to_owned(),
        from_block: request.from_block,
        to_block: request.to_block,
        shard_size_blocks: request.shard_size_blocks,
        selected_pool_count,
    };
    write_json_object(&request.run_root.join("meta.json"), &meta, "meta.json")
}

pub(crate) fn write_manifest_file(
    result: &RawExportResult,
    run_root: &Path,
) -> Result<(), ExportError> {
    let existing_state_shards = load_existing_manifest(run_root)
        .map(|manifest| manifest.map(|value| value.state_shards).unwrap_or_default())?;
    let manifest = ManifestFile {
        raw_shards: result.shard_entries.clone(),
        totals: result.totals.event_counts.clone(),
        skipped_existing_shards: result.skipped_existing_shards,
        state_shards: existing_state_shards,
    };
    write_json_object(&run_root.join("manifest.json"), &manifest, "manifest.json")
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
        state_shards: state_shards.to_vec(),
    };
    write_json_object(&run_root.join("manifest.json"), &manifest, "manifest.json")
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
