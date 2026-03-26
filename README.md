# Base Backtest Exporter Step 1 Contract Freeze

This crate owns the Step 1 replay contract freeze for Base backtest export.

Scope locked in this step:
- freeze the replay dataset contract only
- document current `lpbot-base` assumptions versus the target contract
- ship schema types, validation helpers, fixtures, and crate-level contract tests

Explicitly out of scope in this step:
- source adapters
- raw export
- state generation
- `lpbot-base` runtime changes

## Repository Boundary

Implemented here:
- replay contract spec
- replay root layout validation
- protocol capability matrix
- `lpbot-base` gap list
- Step 1 fixtures and tests

Referenced read-only from `/home/wayne/lpbot_V3_CLMM/crates/lpbot-base`:
- current `RawTopicLog` shape
- current replay/state validation behavior
- current decoder limits
- current `[pools]` metadata dependency

## Current Contract vs Target Contract

Current `lpbot-base` contract facts:
- `RawTopicLog.blockTimestamp` is optional in `src/raw/types.rs`
- missing `blockTimestamp` is normalized to `timestamp_ms = 0` in `src/raw/reader.rs`
- `historical_state` lookup is strict on exact `(pool_address, block_number)` in `src/state/mod.rs`
- replay TVL and `amount_in_usd` still depend on `[pools]` metadata in `src/pipeline.rs`
- `Swap.data` is decoded as exactly 5 ABI words in `src/decode/uniswap_v3.rs`

Target replay contract frozen by Step 1:
- replay root contains `raw/`, `state/`, `pool_manifest.json`, `manifest.json`, `meta.json`
- `pools.generated.toml` is optional transitional compatibility output generated from `pool_manifest.json`
- target raw logs preserve canonical protocol payloads; exporter does not rewrite PancakeV3 swap payloads in Step 1
- `RawTopicLog.blockTimestamp` is required for target replay datasets
- `state` lines are fixed to `pool_address`, `block_number`, `token0_balance_raw`, `token1_balance_raw`
- `pool_manifest.json` is canonical replay metadata and requires explicit boolean `token0_is_stable` / `token1_is_stable`
- stable-side canonical source is a token-address allowlist (`stable_tokens.json`), not symbol/name inference
- unresolved pools never enter canonical `pool_manifest.json`; they are emitted to `unresolved_stable_side_report.json` for allowlist maintenance
- replay dataset metadata is canonical; local `[pools]` becomes override or fallback in later `lpbot-base` work
- `tvl.mode = none` and `tvl.mode = historical_state` share the same replay dataset; the difference is only whether `state/` is read

## Target Replay Root Layout

```text
<REPLAY_ROOT>/
  raw/
    swap/
    mint/
    burn/
    collect/
  state/
  pool_manifest.json
  manifest.json
  meta.json
  pools.generated.toml        # optional transitional compatibility output
```

`manifest.json` and `meta.json` are frozen in Step 1 as required JSON-object placeholders. Their detailed field set can expand later without changing the root layout contract.
`unresolved_stable_side_report.json` is not a required replay-root file and is not consumed by `lpbot-base`; it only supports manual stable-token allowlist review.

## Target Data Shapes

Target raw line:
- identical to the current `lpbot-base` `RawTopicLog` field set
- `blockTimestamp` is required, not optional

Target state line:
- `pool_address`
- `block_number`
- `token0_balance_raw`
- `token1_balance_raw`

Target `pool_manifest.json`:
- top-level `version`
- top-level `pools`
- per pool:
  - `pool_address`
  - `protocol`
  - `token0.address`
  - `token0.decimals`
  - `token1.address`
  - `token1.decimals`
  - `fee_tier`
  - `token0_is_stable`
  - `token1_is_stable`

Validation rule:
- `version` must equal `CONTRACT_VERSION`
- `token0_is_stable` and `token1_is_stable` are required booleans (no tri-state / nullable form)

Target `stable_tokens.json` (frozen input contract):
- top-level `version`
- top-level `tokens`
- per token:
  - `address`
  - `symbol`
  - `name`

Target `unresolved_stable_side_report.json` (review artifact):
- top-level `version`
- top-level `items`
- per item:
  - `pool_address`
  - `token0.address`
  - `token1.address`
  - `reason`

Minimal manifest example:

```json
{
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
}
```

## Protocol Capability Matrix

Matrix rule: this table records whether the target replay contract accepts canonical raw payloads unchanged.

| Protocol | Swap | Mint | Burn | Collect | Note |
| --- | --- | --- | --- | --- | --- |
| UniswapV3 | Accepted | Accepted | Accepted | Accepted | Common V3 path |
| SushiV3 / SushiswapV3 | Accepted | Accepted | Accepted | Accepted | `base-dex-indexer` treats Sushi V3 as UniswapV3-compatible |
| AerodromeV3 / Slipstream | Accepted | Accepted | Accepted | Accepted | `base-dex-indexer` documents the same event structure as UniswapV3 |
| PancakeV3 | Accepted, but current `lpbot-base` decoder gap remains | Accepted | Accepted | Accepted | Native raw swap is 7 ABI words; Step 1 freezes the decoder-upgrade path instead of exporter-side rewrite |

## `lpbot-base` Gap List

Frozen gaps to address later:
- replay-native metadata is missing; current replay still requires `[pools]` as the primary metadata source
- stable metadata precedence (replay metadata vs local config) still needs explicit `lpbot-base` implementation
- current `historical_state` lookup must keep exact `(pool, block)` matching; this is not relaxed by Step 1
- current PancakeV3 `Swap.data` support is limited to the 5-word UniswapV3 shape
- replay validation must become fail-fast for missing target-contract metadata and missing `blockTimestamp`

## Step 1 Test Surface

Fixtures included under `tests/fixtures/`:
- standard V3 raw
- target-invalid raw missing `blockTimestamp`
- PancakeV3 raw with native 7-word swap payload
- valid and invalid `state` lines
- valid and invalid `pool_manifest.json` (including version mismatch)
- valid and invalid `stable_tokens.json`
- valid and invalid `unresolved_stable_side_report.json`

Crate tests cover:
- target raw acceptance for standard V3 and PancakeV3 fixtures
- rejection of raw lines missing `blockTimestamp`
- `state` line shape validation
- `pool_manifest.json` required-field and version validation
- `stable_tokens.json` required-field and address-format validation
- `unresolved_stable_side_report.json` required-field validation
- replay root layout validation
