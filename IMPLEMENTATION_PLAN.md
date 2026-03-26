# Base Backtest Exporter

Standalone tool workspace for producing complete `lpbot-base` replay/backtest datasets directly from the self-hosted Base node and `base-dex-indexer` metadata, without depending on the legacy `~/lp` pipeline.

## Goal

Produce deterministic, complete replay roots for Base V3-family protocols, with `historical_state` as the primary target and `none` as a degraded read mode over the same dataset.

Primary target:
- `raw/{swap,mint,burn,collect}/*.jsonl`
- `state/**/*.jsonl` when `tvl.mode = historical_state`
- dataset-native pool metadata manifest consumed by `lpbot-base`

## Scope

Included:
- UniswapV3-style protocols on Base
- UniswapV3
- PancakeV3
- AerodromeV3 / Slipstream-style V3 pools
- SushiV3
- AlienV3 or any other protocol whose event contract is compatible with the V3-family path
- replay dataset sharding by block range
- deterministic ordering and resumable export
- dataset validation against `lpbot-base` replay contract
- coordinated `lpbot-base` changes when the current replay contract is not good enough for the Base backtest end state

Excluded:
- V2 pools
- V4 pools
- live trading runtime
- arbitrary analytics APIs
- reuse of legacy `~/lp` code as an implementation dependency

## What `lpbot-base` Needs Today

Hard replay contract:
- replay root must contain `raw/{swap,mint,burn,collect}/*.jsonl`
- each raw line must match `RawTopicLog`
- `blockTimestamp` is optional in schema, but should be treated as required for usable replay because rolling volume, volatility, and duration all depend on timestamps
- when `tvl.mode = historical_state`, state rows must contain only:
  - `pool_address`
  - `block_number`
  - `token0_balance_raw`
  - `token1_balance_raw`

Behavioral contract:
- `lpbot-base` produces snapshots and replay updates only from `Swap` events
- `historical_state_tvl` is only queried on swap-derived replay updates
- `amount_in_usd` depends on stable-side config from `[pools]`
- fee rate depends on configured `fee_tier`

Implication:
- raw export is mandatory
- state export only needs to cover blocks that will actually be consumed by replay, which currently means swap blocks for selected pools
- pool metadata is mandatory for useful replay because price, fee rate, USD volume, and historical TVL all depend on decimals, fee tier, and stable-side flags

## Target End State

Desired contract after coordinated `lpbot-base` cleanup:
- one replay dataset is sufficient for both `tvl.mode = none` and `tvl.mode = historical_state`
- `historical_state` is the full-fidelity mode
- `none` simply means replay without reading `state`, not a different dataset shape
- pool metadata should ship with the replay dataset instead of living only in handwritten `[pools]`
- replay acceptance for supported Base V3-family protocols should not depend on exporter-side mutation of otherwise canonical raw data unless we intentionally freeze that as part of the contract

## Source of Truth

Primary source:
- Base node receipts and block headers
- `base-dex-indexer` pool metadata / protocol identification

Not primary truth:
- `block_analysis.db`

Reason:
- `block_analysis.db` is useful for inspection, but it is not the safest canonical replay source
- current `base-dex-indexer` runtime skips business processing while the node is syncing, and historical backfill currently focuses on pool discovery rather than complete replay-grade export coverage
- replay export needs direct access to block timestamp, receipt ordering, and protocol-specific raw event shape

## Key Architecture Choices

1. Standalone workspace

Reason:
- keeps `lpbot`-specific replay assumptions out of `base-dex-indexer`
- allows independent CLI, dataset layout, and validation rules
- reduces coupling to node runtime concerns

2. Export raw first, not decoded snapshots

Reason:
- `lpbot-base` already owns decoding and snapshot building
- replay artifacts should preserve the exact event provenance the bot consumes
- this keeps the exporter narrow and auditable

3. Prefer protocol compatibility in `lpbot-base`, not exporter-only rewrites

Reason:
- when a protocol is logically part of the supported Base V3-family surface, `lpbot-base` should understand that protocol cleanly
- exporter-side normalization should be reserved for stable contract shaping, not for hiding decoder gaps that belong in `lpbot-base`
- this matters most for PancakeV3 swap decoding

4. State generation should be replay-driven, not raw-totality-driven

Reason:
- current `lpbot-base` only reads historical state for swap replay updates
- generating state for every `mint/burn/collect` block adds large RPC cost without improving replay output

5. Replay dataset should own pool metadata

Reason:
- current `[pools]` TOML is operationally fragile and duplicates information that the exporter can already derive
- Base backtest should be able to point at a replay root and get all required metadata with minimal manual wiring
- unresolved stable-side allowlist cases can still be surfaced for manual review, but canonical replay metadata remains dataset-first

## Protocol Handling

### Common V3-family path

Protocols that share the UniswapV3 event contract can be exported with the same raw/state pipeline:
- Swap
- Mint
- Burn
- Collect

### PancakeV3 special case

Known difference:
- PancakeV3 `Swap` includes two extra trailing fields: `protocolFeesToken0` and `protocolFeesToken1`

Impact:
- current `lpbot-base` swap decoder expects the UniswapV3 swap payload length exactly
- feeding native PancakeV3 swap raw directly into the current decoder will fail today

Planned handling:
- preferred path: extend `lpbot-base` decoding so PancakeV3 swap logs are accepted natively while preserving the canonical raw payload in replay datasets
- fallback path: only if we decide the replay contract itself is canonical-Uniswap-only, normalize PancakeV3 swap payloads at export time and document that normalization as part of the replay contract
- Mint / Burn / Collect stay on the common V3-family path unless new incompatibilities are observed

## Dataset Output

Proposed workspace layout:

```text
/home/wayne/lp/base-backtest-exporter/
  IMPLEMENTATION_PLAN.md
  README.md
  src/
  tests/
  out/
```

Proposed run output layout:

```text
<RUN_ROOT>/
  meta.json
  manifest.json
  pool_manifest.json
  pools.generated.toml
  shards/
    <from>_<to>/
      raw/
        swap/<from>_<to>.jsonl
        mint/<from>_<to>.jsonl
        burn/<from>_<to>.jsonl
        collect/<from>_<to>.jsonl
      state/
        <pool_address>/<from>_<to>.jsonl
      qa/
        validation_report.json
```

`pool_manifest.json` is the canonical replay metadata payload:
- pool address
- token addresses
- token decimals
- fee tier
- protocol
- required `token0_is_stable` / `token1_is_stable` booleans
- any protocol-specific decode hints needed by `lpbot-base`

Stable-side contract rule:
- canonical stable-side source is token-address allowlist input (`stable_tokens.json`)
- `symbol` and `name` are informational and review-facing, not canonical matching keys
- pools unresolved by the allowlist are excluded from canonical `pool_manifest.json` and emitted into `unresolved_stable_side_report.json`
- `unresolved_stable_side_report.json` is a review artifact only; it is not a replay-root required file and is not consumed by `lpbot-base`

`pools.generated.toml` is transitional compatibility output:
- generated from `pool_manifest.json`
- kept only until `lpbot-base` can consume replay-native metadata directly

## State Strategy

Target behavior:
- generate state only for selected pools and only at replay-consumed swap blocks
- avoid the legacy pattern of scanning every raw target and doing two historical `balanceOf` reads per point by default

Preferred strategy:
- determine selected pools first
- determine replay-consumed swap blocks second
- build per-pool balance state with event-driven deltas where protocol semantics are reliable
- use direct historical reads for shard bootstrap, gap repair, and validation

Validation strategy:
- sample exact historical `balanceOf` reads at configurable checkpoints
- fail fast when drift exceeds zero for strict mode
- allow resumable reruns without rewriting completed shard files

Open risk:
- if a protocol or pool can change balances through paths not fully represented by the replayed events, pure incremental balance tracking can drift
- for that reason, checkpoint validation and repair are part of the core design, not an optional extra

## Required `lpbot-base` Changes

Expected coordinated changes:
- allow replay roots to provide canonical pool metadata without relying only on handwritten `[pools]`
- define precedence when both replay metadata and local config are present
- support PancakeV3 swap decoding cleanly if we keep canonical raw payloads
- tighten replay validation so missing timestamps and missing required pool metadata fail early with explicit errors
- preserve `tvl.mode = none` as a read-mode toggle over the same replay dataset rather than a separate export target

## Implementation Plan

### Step 1
Objective:
- freeze exporter-side replay contracts and publish the `lpbot-base` gap list (read-only for `lpbot-base` code)

Artifact:
- `README.md`
- `src/contract.rs`
- validation fixtures under `tests/fixtures/`
- `lpbot-base` contract gap list

Inputs:
- `lpbot-base` raw/state contract
- V3-family protocol list
- current `lpbot-base` config and decoder assumptions

Outputs:
- documented current contract and target contract
- protocol capability matrix
- frozen `stable_tokens.json` input contract
- frozen `unresolved_stable_side_report.json` review artifact contract
- explicit list of `lpbot-base` changes required to reach the target contract

Tests:
- fixture-based contract validation for raw/state line shape
- path/layout validation for replay roots

Complexity:
- low

Status:
- accepted as an exporter-only contract-freeze batch
- stable-side review is closed by requiring explicit booleans in `pool_manifest.json`
- canonical stable-side source is `stable_tokens.json`
- unresolved pools are excluded from canonical `pool_manifest.json` and emitted to `unresolved_stable_side_report.json`
- no `lpbot-base` code changes are part of Step 1

Immediate handoff:
- the next executable batch is Step 2 in the exporter only
- Step 2 must not change `/home/wayne/lpbot_V3_CLMM`

### Step 2
Objective:
- build exporter-only source adapters for Base node blocks, receipts, headers, pool metadata, and stable-token allowlist inputs

Artifact:
- `src/source/`
- `src/protocol/registry.rs`

Inputs:
- Base node access
- `base-dex-indexer` metadata and protocol identification
- `stable_tokens.json`

Outputs:
- block-range iterator with deterministic ordering
- pool metadata lookup for selected pools
- canonical pool manifest model
- stable-token allowlist loader with normalized address matching
- protocol-tagged pool registry ready for later raw export
- no shard writing and no replay output files yet

Tests:
- block ordering tests
- metadata resolution tests
- stable-token allowlist address-normalization tests
- resume cursor tests

Complexity:
- medium

Status:
- accepted as an exporter-only source-adapter batch
- Base node access is frozen to exporter-owned JSON-RPC adapters for block headers and block receipts
- `base-dex-indexer` access is frozen to exporter-owned HTTP API adapters for pool and token metadata
- explicit pool-address selection, protocol normalization, stable-token allowlist matching, and resolved/unresolved catalog splitting are now implemented exporter-side
- no raw shard writing, state generation, or `lpbot-base` code changes are part of Step 2

Immediate handoff:
- the next executable batch is Step 3 in the exporter only
- Step 3 must consume Step 2 resolved catalog outputs instead of re-reading protocol semantics from scratch
- Step 3 must not change `/home/wayne/lpbot_V3_CLMM`

### Step 3
Objective:
- export replay raw logs for selected pools and block ranges

Artifact:
- `src/export/raw.rs`
- `src/export/shard.rs`

Inputs:
- receipts
- block headers
- selected pools
- protocol-specific raw handling rules

Outputs:
- shard-local raw jsonl files
- manifest entries with counts and ranges

Tests:
- deterministic ordering tests
- duplicate suppression tests
- timestamp presence tests
- PancakeV3 compatibility tests against the agreed replay contract

Complexity:
- medium

### Step 4
Objective:
- generate replay-driven historical state for swap-consumed blocks as the primary TVL path

Artifact:
- `src/state/targets.rs`
- `src/state/engine.rs`
- `src/state/validate.rs`

Inputs:
- exported swap stream
- pool metadata
- Base node historical reads

Outputs:
- `state/**/*.jsonl` covering replay-consumed swap blocks
- validation report showing exact coverage and drift checks

Tests:
- coverage tests for selected swap blocks
- strict validation tests against sampled historical reads
- resume/restart tests

Complexity:
- high

### Step 5
Objective:
- produce canonical replay pool metadata, stable-token allowlist input, and transitional `lpbot-base` compatibility output

Artifact:
- `src/export/pools_manifest.rs`

Inputs:
- pool metadata
- token metadata
- stable-token allowlist registry

Outputs:
- `pool_manifest.json`
- `stable_tokens.json`
- `pools.generated.toml`
- `unresolved_stable_side_report.json` for allowlist maintenance

Tests:
- decimals extraction tests
- fee-tier mapping tests
- stable-token allowlist mapping tests
- replay metadata compatibility tests for `lpbot-base`

Complexity:
- medium

### Step 6
Objective:
- implement the required `lpbot-base` replay contract upgrades

Artifact:
- `lpbot-base` config loading changes
- `lpbot-base` replay metadata loading path
- `lpbot-base` decoder updates for supported Base V3-family protocols

Inputs:
- frozen target replay contract
- `pool_manifest.json`
- supported protocol matrix

Outputs:
- `lpbot-base` accepts canonical replay datasets with minimal local config
- PancakeV3 and other supported V3-family protocols replay cleanly under the agreed contract

Tests:
- crate-level parser/config tests
- decoder tests for protocol-specific payloads
- replay pipeline tests using replay-native metadata

Complexity:
- high

### Step 7
Objective:
- ship CLI, QA, and end-to-end replay verification

Artifact:
- `src/main.rs`
- `src/cli.rs`
- `tests/e2e/`

Inputs:
- all previous modules
- test block ranges for supported protocols

Outputs:
- runnable exporter CLI
- end-to-end dataset that `lpbot-base-backtest` accepts directly in both `tvl.mode = none` and `tvl.mode = historical_state`

Tests:
- crate-level CLI tests
- end-to-end dataset generation test
- replay acceptance test by invoking `lpbot-base` on produced shards

Complexity:
- high

## Main Risks

- The current `lpbot-base` config contract is too config-centric; if replay-native metadata is only bolted on, the result will stay awkward.
- PancakeV3 raw incompatibility with the current `lpbot-base` decoder must be resolved explicitly in the target contract or replay will fail.
- Stable-token allowlist can drift from market reality if not maintained; unresolved cases must stay manual and cannot enter canonical manifests.
- Event-driven balance updates can drift if a protocol changes balances outside the replayed event set; validation and repair are mandatory.
- Large block ranges can produce heavy disk IO; shard sizing and resumable manifests must be built in from the start.
- Reorg handling must be explicit for any export that runs close to chain head; historical bounded exports should default to finalized ranges.

## Approval Gate

No implementation should start until this document is approved.
