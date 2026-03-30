# Base Backtest Exporter

`base-backtest-exporter` is a standalone exporter for Base replay datasets.

Current status:
- Step 1-5 exporter core is implemented in this repository.
- Step 6 (`lpbot-base` replay-native metadata + decoder upgrades) is implemented in `/home/wayne/lpbot_V3_CLMM`.
- This repository does not modify `lpbot-base` at runtime.

## Scope

Implemented here:
- replay contract validation (`raw/`, `state/`, `meta.json`, `manifest.json`, `pool_manifest.json`)
- source adapters (Base node JSON-RPC + indexer pool metadata + optional local token overrides fallback)
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

Default committed execution case:
- Uniswap V3 `WETH/USDC` on Base: `0x6c561b446416e1a00e8e93e221854d6ea4171372`
- PancakeSwap V3 `WETH/USDC` on Base: `0x72ab388e2e2f6facef59e3c3fa2c4e29011c2d38`
- stable token input: Base `USDC` `0x833589fcd6edb6e08f4c7c32d4f71b54bda02913`

The committed defaults are:
- `config/default_selected_pools.base_eth_usdc.txt`
- `config/default_stable_tokens.base_eth_usdc.json`

These defaults were checked on 2026-03-27 against public pool pages/rankings and are intended as the first live export case, not as a forever-frozen liquidity ranking.

`deploy.sh export` will:
- install Rust via `rustup` automatically if `cargo` is missing
- load `.env`
- run exporter `export`
- run exporter `verify` automatically when `VERIFY_AFTER_EXPORT=1`

For large historical ranges, `deploy.sh` can split one export into sequential
sub-runs against the same `RUN_ROOT`:

```bash
EXPORT_CHUNK_BLOCKS=50000 bash ./deploy.sh export
```

Or set `EXPORT_CHUNK_BLOCKS` in `.env`. The script will run each block slice in
order, preserve existing resume behavior, and execute `verify` once after the
final chunk.

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
- optional: `--token-overrides-file <path>`
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

### `token-overrides-file` Contract (Optional)

`--token-overrides-file` is optional. When omitted, exporter uses RPC token metadata only.

`TOKEN_OVERRIDES_FILE` in `.env` is consumed by `deploy.sh`; raw CLI parsing does not auto-read this env var.

JSON shape:

```json
{
  "version": 1,
  "tokens": [
    {
      "address": "0x4200000000000000000000000000000000000006",
      "decimals": 18,
      "symbol": "WETH",
      "name": "Wrapped Ether"
    }
  ]
}
```

Contract rules:
- `version` must be `1`
- each token entry must include full metadata (`address`, `decimals`, `symbol`, `name`)
- `address` must be a valid EVM address
- `symbol` and `name` must be non-empty
- duplicate addresses after normalization fail fast

Resolved catalog metadata source priority:
1. RPC token metadata at `pool.creation_block_number`
2. local override entry (used when RPC metadata is unavailable or RPC metadata call errors)
3. existing missing-token path (`missing_token0_metadata` / `missing_token1_metadata`)

### `export` Execution Order

`export` runs the fixed sequence:
1. read and validate selected pools file
2. read and validate `stable_tokens.json`
3. read and validate optional `token_overrides.json`
4. build resolved pool catalog from indexer pool metadata + token metadata source priority + stable allowlist
5. fail fast when `resolved == 0`
6. run raw export
7. run historical state export
8. run replay metadata export
9. validate replay root layout/contracts locally
10. print summary (resolved/unresolved/unsupported + raw/state/metadata counts + run root)

`token_overrides` only affects resolved-catalog token metadata sourcing; replay output file shapes remain unchanged.

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

For the default live case in this repository, use:

```text
config/default_selected_pools.base_eth_usdc.txt
```

### 2) Prepare `stable_tokens.json`

Use the frozen contract (`version`, `tokens[]`, `address`, `symbol`, `name`).

For the default live case in this repository, use:

```text
config/default_stable_tokens.base_eth_usdc.json
```

Optional fallback template for non-standard ERC20 metadata:

```text
config/token_overrides.example.json
```

Set `TOKEN_OVERRIDES_FILE` in `.env` when running `bash ./deploy.sh export`.

For raw `cargo run -- export`, pass `--token-overrides-file` explicitly when needed.

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

Add `--token-overrides-file /path/to/token_overrides.json` only when you need fallback metadata for non-standard ERC20 tokens.

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
