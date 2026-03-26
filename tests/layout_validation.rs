use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use base_backtest_exporter::contract::{
    validate_replay_root, CANONICAL_POOL_MANIFEST_FILE, MANIFEST_FILE, META_FILE, RAW_DIR,
    RAW_EVENT_DIRS, STATE_DIR,
};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    std::env::temp_dir().join(format!("base-backtest-exporter-{name}-{nonce}"))
}

fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        if let Err(err) = fs::create_dir_all(parent) {
            panic!("failed to create {}: {err}", parent.display());
        }
    }
    if let Err(err) = fs::write(path, contents) {
        panic!("failed to write {}: {err}", path.display());
    }
}

fn valid_pool_manifest_fixture() -> &'static str {
    r#"{
  "version": 1,
  "pools": [
    {
      "pool_address": "0x1111111111111111111111111111111111111111",
      "protocol": "UniswapV3",
      "token0": {
        "address": "0x2222222222222222222222222222222222222222",
        "decimals": 6
      },
      "token1": {
        "address": "0x3333333333333333333333333333333333333333",
        "decimals": 18
      },
      "fee_tier": 500,
      "token0_is_stable": true,
      "token1_is_stable": false
    }
  ]
}"#
}

fn create_minimal_root(root: &Path) {
    for event_dir in RAW_EVENT_DIRS {
        let dir = root.join(RAW_DIR).join(event_dir);
        if let Err(err) = fs::create_dir_all(&dir) {
            panic!("failed to create {}: {err}", dir.display());
        }
    }
    let state_dir = root.join(STATE_DIR);
    if let Err(err) = fs::create_dir_all(&state_dir) {
        panic!("failed to create {}: {err}", state_dir.display());
    }

    write_file(&root.join(META_FILE), "{}");
    write_file(&root.join(MANIFEST_FILE), "{}");
    write_file(
        &root.join(CANONICAL_POOL_MANIFEST_FILE),
        valid_pool_manifest_fixture(),
    );
}

#[test]
fn minimal_replay_root_layout_passes_without_transitional_pools_toml() {
    let root = temp_dir("valid-root");
    create_minimal_root(&root);

    match validate_replay_root(&root) {
        Ok(_) => {}
        Err(err) => panic!("expected valid replay root layout: {err}"),
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn replay_root_validation_fails_when_required_metadata_file_is_missing() {
    let root = temp_dir("missing-meta");
    create_minimal_root(&root);
    if let Err(err) = fs::remove_file(root.join(META_FILE)) {
        panic!("failed to remove required metadata file: {err}");
    }

    let err = match validate_replay_root(&root) {
        Ok(_) => panic!("layout missing meta.json must fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains(META_FILE),
        "unexpected error: {err}"
    );

    let _ = fs::remove_dir_all(root);
}
