#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/.env}"
MODE="${1:-export}"

usage() {
  cat <<'EOF'
Usage:
  bash ./deploy.sh export
  bash ./deploy.sh verify
  bash ./deploy.sh build

Environment:
  By default the script loads ./.env.
  Override with:
    ENV_FILE=/path/to/file.env bash ./deploy.sh export
EOF
}

ensure_rust_toolchain() {
  if command -v cargo >/dev/null 2>&1; then
    return 0
  fi

  if ! command -v curl >/dev/null 2>&1; then
    echo "missing required command: curl" >&2
    echo "install curl or preinstall Rust before running deploy.sh" >&2
    exit 1
  fi

  echo "[deploy] cargo not found; installing rustup (minimal profile)..." >&2
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

  if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1090
    source "$HOME/.cargo/env"
  fi

  if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo is still unavailable after rustup install" >&2
    exit 1
  fi
}

load_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "missing env file: $ENV_FILE" >&2
    echo "copy $SCRIPT_DIR/.env.example to $SCRIPT_DIR/.env and edit it first" >&2
    exit 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required env var: $name" >&2
    exit 1
  fi
}

require_file() {
  local label="$1"
  local path="$2"
  if [[ ! -f "$path" ]]; then
    echo "missing required file for $label: $path" >&2
    exit 1
  fi
}

require_export_env() {
  require_var RUN_ROOT
  require_var RPC_URL
  require_var INDEXER_URL
  require_var SELECTED_POOLS_FILE
  require_var STABLE_TOKENS_FILE
  require_var FROM_BLOCK
  require_var TO_BLOCK
  require_var SHARD_SIZE_BLOCKS
  require_var VALIDATION_STRIDE_TARGETS

  require_file SELECTED_POOLS_FILE "$SELECTED_POOLS_FILE"
  require_file STABLE_TOKENS_FILE "$STABLE_TOKENS_FILE"

  mkdir -p "$(dirname -- "$RUN_ROOT")"
}

cargo_args() {
  if [[ "${EXPORTER_PROFILE:-release}" == "release" ]]; then
    printf '%s\n' run --release
  else
    printf '%s\n' run
  fi
}

run_export() {
  require_export_env

  mapfile -t base_args < <(cargo_args)
  cargo "${base_args[@]}" -- export \
    --run-root "$RUN_ROOT" \
    --rpc-url "$RPC_URL" \
    --indexer-url "$INDEXER_URL" \
    --selected-pools-file "$SELECTED_POOLS_FILE" \
    --stable-tokens-file "$STABLE_TOKENS_FILE" \
    --from-block "$FROM_BLOCK" \
    --to-block "$TO_BLOCK" \
    --shard-size-blocks "$SHARD_SIZE_BLOCKS" \
    --validation-stride-targets "$VALIDATION_STRIDE_TARGETS"

  if [[ "${VERIFY_AFTER_EXPORT:-1}" == "1" ]]; then
    run_verify
  fi
}

run_verify() {
  require_var RUN_ROOT

  mapfile -t base_args < <(cargo_args)
  cargo "${base_args[@]}" -- verify \
    --run-root "$RUN_ROOT"
}

run_build() {
  if [[ "${EXPORTER_PROFILE:-release}" == "release" ]]; then
    cargo build --release
  else
    cargo build
  fi
}

main() {
  case "$MODE" in
    export|verify|build|help|-h|--help)
      ;;
    *)
      echo "unsupported mode: $MODE" >&2
      usage
      exit 1
      ;;
  esac

  if [[ "$MODE" == "help" || "$MODE" == "-h" || "$MODE" == "--help" ]]; then
    usage
    exit 0
  fi

  ensure_rust_toolchain
  load_env

  export TMPDIR="${TMPDIR:-/tmp}"
  export TMP="${TMP:-/tmp}"
  export TEMP="${TEMP:-/tmp}"
  export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"

  cd "$SCRIPT_DIR"

  case "$MODE" in
    export)
      run_export
      ;;
    verify)
      run_verify
      ;;
    build)
      run_build
      ;;
  esac
}

main "$@"
