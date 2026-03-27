# Base Backtest Exporter

`base-backtest-exporter` is a standalone exporter for Base replay datasets.

Current status:
- Step 1-5 exporter core is implemented in this repository.
- Step 6 (`lpbot-base` replay-native metadata + decoder upgrades) is implemented in `/home/wayne/lpbot_V3_CLMM`.
- This repository does not modify `lpbot-base` at runtime.

## Scope

Implemented here:
- replay contract validation (`raw/`, `state/`, `meta.json`, `manifest.json`, `pool_manifest.json`)
- source adapters (Base node JSON-RPC + indexer HTTP metadata)
- raw shard export (deterministic order/dedup + resume checks)
- historical state export (swap-target-driven + validation/fallback)
- metadata export (`pool_manifest.json`, `stable_tokens.json`, `unresolved_stable_side_report.json`, `pools.generated.toml`)
- CLI orchestration (`export`, `verify`)

Not implemented here:
- `lpbot-base` runtime logic
- `base-dex-indexer` server-side changes

## Historical Gaps (Resolved In Step 6)

These are no longer active gaps in `lpbot-base`:
- replay-native metadata loading from `pool_manifest.json`
- strict replay fail-fast for missing `blockTimestamp`
- PancakeV3 7-word swap payload support in the shared V3 decoder

## Test Policy

Implementation-time validation only:
- step-specific module acceptance tests can be removed after a step is accepted
- long-lived checks are crate-level tests that still provide value for current contract and behavior

Current crate-level validation command:

```bash
TMPDIR=/tmp TMP=/tmp TEMP=/tmp CARGO_BUILD_JOBS=1 cargo test
```

## CLI

The binary provides exactly two subcommands:
- `export`
- `verify`

## Ubuntu Quick Start

This repository now includes:
- committed template: `.env.example`
- local config file: `.env` (gitignored)
- one-command wrapper: `deploy.sh`

Recommended first use on the server:

```bash
cd /home/wayne/lp/base-backtest-exporter
cp .env.example .env
$EDITOR .env
bash ./deploy.sh export
```

`deploy.sh export` will:
- install Rust via `rustup` automatically if `cargo` is missing
- load `.env`
- run exporter `export`
- run exporter `verify` automatically when `VERIFY_AFTER_EXPORT=1`

Other modes:

```bash
bash ./deploy.sh build
bash ./deploy.sh verify
```

### `export` Required Flags

- `--run-root <path>`
- `--rpc-url <url>`
- `--indexer-url <url>`
- `--selected-pools-file <path>`
- `--stable-tokens-file <path>`
- `--from-block <u64>`
- `--to-block <u64>`
- `--shard-size-blocks <u64>`
- `--validation-stride-targets <u64>`

### `selected-pools-file` Contract

- UTF-8 text file
- one pool address per line
- blank lines allowed
- comment lines starting with `#` allowed
- each non-comment line is trimmed, normalized to lowercase EVM address
- duplicate addresses after normalization fail fast (no silent dedupe)

### `export` Execution Order

`export` runs the fixed sequence:
1. read and validate selected pools file
2. read and validate `stable_tokens.json`
3. build resolved pool catalog from indexer metadata + allowlist
4. fail fast when `resolved == 0`
5. run raw export
6. run historical state export
7. run replay metadata export
8. validate replay root layout/contracts locally
9. print summary (resolved/unresolved/unsupported + raw/state/metadata counts + run root)

`unresolved_stable_side` and `unsupported_or_invalid` do not block export when `resolved > 0`.

`export` keeps existing resume semantics. There is no force-overwrite mode.

### `verify`

- required flag: `--run-root <path>`
- validates replay root locally using contract/root validators
- does not call RPC and does not call indexer

## Output Layout

```text
<RUN_ROOT>/
  raw/
    swap/<from>_<to>.jsonl
    mint/<from>_<to>.jsonl
    burn/<from>_<to>.jsonl
    collect/<from>_<to>.jsonl
  state/
    <pool_address>/<from>_<to>.jsonl
    validation_report.json
  meta.json
  manifest.json
  pool_manifest.json
  stable_tokens.json
  unresolved_stable_side_report.json
  pools.generated.toml
```

## Minimal Usage

### 1) Prepare `selected-pools-file`

Example:

```text
# Base V3 pools
0x1111111111111111111111111111111111111111
0x2222222222222222222222222222222222222222
```

### 2) Prepare `stable_tokens.json`

Use the frozen contract (`version`, `tokens[]`, `address`, `symbol`, `name`).

### 3) Run `export`

```bash
cd /home/wayne/lp/base-backtest-exporter
bash ./deploy.sh export
```

Equivalent raw CLI form:

```bash
cd /home/wayne/lp/base-backtest-exporter
TMPDIR=/tmp TMP=/tmp TEMP=/tmp CARGO_BUILD_JOBS=1 cargo run --release -- export \
  --run-root /tmp/base_replay_run \
  --rpc-url http://127.0.0.1:8545 \
  --indexer-url http://127.0.0.1:8080 \
  --selected-pools-file /tmp/selected_pools.txt \
  --stable-tokens-file /tmp/stable_tokens.json \
  --from-block 27800000 \
  --to-block 27801000 \
  --shard-size-blocks 200 \
  --validation-stride-targets 20
```

### 4) Run `verify`

```bash
cd /home/wayne/lp/base-backtest-exporter
bash ./deploy.sh verify
```

## Manual Cross-Repo Acceptance (`lpbot-base-backtest`)

Exporter does not invoke sibling repositories automatically. Use manual acceptance in `/home/wayne/lpbot_V3_CLMM`.

### `tvl.mode = none`

```bash
cd /home/wayne/lpbot_V3_CLMM
CONFIG_FILE=/tmp/config.base.none.toml CARGO_BUILD_JOBS=1 cargo run -p lpbot-base --bin lpbot-base-backtest
```

### `tvl.mode = historical_state`

```bash
cd /home/wayne/lpbot_V3_CLMM
CONFIG_FILE=/tmp/config.base.historical_state.toml CARGO_BUILD_JOBS=1 cargo run -p lpbot-base --bin lpbot-base-backtest
```

Both config files should point `[replay].data_roots` to the exported `<RUN_ROOT>`.
