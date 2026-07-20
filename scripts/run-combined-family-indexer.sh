#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/combined-family-common.sh"

usage() {
  cat <<EOF
Usage:
  scripts/run-combined-family-indexer.sh doctor [--strict]
  scripts/run-combined-family-indexer.sh command
  scripts/run-combined-family-indexer.sh run

Modes:
  doctor   Report whether the local ${TYCHO_INDEXER_ENTRYPOINT_LABEL_VALUE} indexer startup environment is ready.
           With `--strict`, exits non-zero when readiness is false.
  command  Print the exact command to start the ${TYCHO_INDEXER_ENTRYPOINT_LABEL_VALUE} indexer.
  run      Start the ${TYCHO_INDEXER_ENTRYPOINT_LABEL_VALUE} indexer with the resolved command and environment.

Environment:
  AUTH_API_KEY                  Default: dummy
  SUBSTREAMS_API_TOKEN          Required
  TYCHO_INDEXER_ENTRYPOINT_LABEL
                                Default: combined-family
  TYCHO_INDEXER_ENDPOINT        Default: https://mainnet.eth.streamingfast.io
  TYCHO_INDEXER_DATABASE_URL    Default: postgres://postgres:mypassword@localhost:5431/tycho_indexer_0
  TYCHO_INDEXER_RPC_URL         Default: https://rpc.mevblocker.io
  TYCHO_INDEXER_EXTRACTORS_CONFIG
                                Default: crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml
  TYCHO_INDEXER_RUST_LOG        Default: info

Notes:
  - `command` prints `export SUBSTREAMS_API_TOKEN=...` before `cargo run ... --api_token "$SUBSTREAMS_API_TOKEN"`
    to avoid the shell-expansion bug where inline env assignment leaves `--api_token` empty.
  - `run` exports the same variables in-process, so it is safe to use even when
    `SUBSTREAMS_API_TOKEN` is not already exported in the caller shell.
EOF
}

REPO_ROOT="${TYCHO_COMBINED_FAMILY_REPO_ROOT}"
CANONICAL_EXTRACTORS_CONFIG="${TYCHO_COMBINED_FAMILY_CANONICAL_EXTRACTORS_CONFIG}"
CANONICAL_ABS_EXTRACTORS_CONFIG="${TYCHO_COMBINED_FAMILY_CANONICAL_EXTRACTORS_CONFIG_ABS}"

MODE="${1:-}"
STRICT_DOCTOR="false"

if [[ -z "${MODE}" || "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" == "doctor" && "${2:-}" == "--strict" ]]; then
  STRICT_DOCTOR="true"
fi

AUTH_API_KEY_VALUE="${AUTH_API_KEY:-dummy}"
SUBSTREAMS_API_TOKEN_VALUE="${SUBSTREAMS_API_TOKEN:-}"
TYCHO_INDEXER_ENTRYPOINT_LABEL_VALUE="${TYCHO_INDEXER_ENTRYPOINT_LABEL:-combined-family}"
TYCHO_INDEXER_ENDPOINT_VALUE="${TYCHO_INDEXER_ENDPOINT:-https://mainnet.eth.streamingfast.io}"
TYCHO_INDEXER_DATABASE_URL_VALUE="${TYCHO_INDEXER_DATABASE_URL:-postgres://postgres:mypassword@localhost:5431/tycho_indexer_0}"
TYCHO_INDEXER_RPC_URL_VALUE="${TYCHO_INDEXER_RPC_URL:-https://rpc.mevblocker.io}"
TYCHO_INDEXER_EXTRACTORS_CONFIG_VALUE="${TYCHO_INDEXER_EXTRACTORS_CONFIG:-${CANONICAL_EXTRACTORS_CONFIG}}"
TYCHO_INDEXER_RUST_LOG_VALUE="${TYCHO_INDEXER_RUST_LOG:-info}"

ABS_EXTRACTORS_CONFIG="${REPO_ROOT}/${TYCHO_INDEXER_EXTRACTORS_CONFIG_VALUE}"
if [[ "${TYCHO_INDEXER_EXTRACTORS_CONFIG_VALUE}" == /* ]]; then
  ABS_EXTRACTORS_CONFIG="${TYCHO_INDEXER_EXTRACTORS_CONFIG_VALUE}"
fi

doctor() {
  local ready="true"
  local cargo_state="available"
  local config_state="present"
  local config_contract_state="canonical"
  local token_state="set"
  local db_state="unverified"
  local psql_state="missing"

  if ! command -v cargo >/dev/null 2>&1; then
    cargo_state="missing"
    ready="false"
  fi

  if [[ ! -f "${ABS_EXTRACTORS_CONFIG}" ]]; then
    config_state="missing"
    ready="false"
  fi

  if [[ "${ABS_EXTRACTORS_CONFIG}" != "${CANONICAL_ABS_EXTRACTORS_CONFIG}" ]]; then
    config_contract_state="noncanonical"
    ready="false"
  fi

  if [[ -z "${SUBSTREAMS_API_TOKEN_VALUE}" ]]; then
    token_state="missing"
    ready="false"
  fi

  if command -v psql >/dev/null 2>&1; then
    psql_state="available"
    if PGPASSWORD="${PGPASSWORD:-}" psql "${TYCHO_INDEXER_DATABASE_URL_VALUE}" -At -F $'\t' -c "select 1;" >/dev/null 2>&1; then
      db_state="reachable"
    else
      db_state="unreachable"
      ready="false"
    fi
  fi

  cat <<EOF
ready=${ready}
entrypoint_label=${TYCHO_INDEXER_ENTRYPOINT_LABEL_VALUE}
repo_root=${REPO_ROOT}
cargo_state=${cargo_state}
extractors_config=${TYCHO_INDEXER_EXTRACTORS_CONFIG_VALUE}
extractors_config_state=${config_state}
extractors_config_contract_state=${config_contract_state}
canonical_extractors_config=${CANONICAL_EXTRACTORS_CONFIG}
database_url=${TYCHO_INDEXER_DATABASE_URL_VALUE}
database_state=${db_state}
psql_state=${psql_state}
endpoint=${TYCHO_INDEXER_ENDPOINT_VALUE}
rpc_url=${TYCHO_INDEXER_RPC_URL_VALUE}
auth_api_key_state=$(if [[ -n "${AUTH_API_KEY_VALUE}" ]]; then printf 'set'; else printf 'missing'; fi)
substreams_api_token_state=${token_state}
rust_log=${TYCHO_INDEXER_RUST_LOG_VALUE}
EOF

  if [[ "${STRICT_DOCTOR}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

render_command() {
cat <<EOF
cd $(tycho_combined_family_shell_escape "${REPO_ROOT}")
export AUTH_API_KEY=$(tycho_combined_family_shell_escape "${AUTH_API_KEY_VALUE}")
export SUBSTREAMS_API_TOKEN=$(tycho_combined_family_shell_escape "${SUBSTREAMS_API_TOKEN_VALUE:-<set SUBSTREAMS_API_TOKEN>}")
export RUST_LOG=$(tycho_combined_family_shell_escape "${TYCHO_INDEXER_RUST_LOG_VALUE}")
cargo run --bin tycho-indexer -- \\
  --endpoint $(tycho_combined_family_shell_escape "${TYCHO_INDEXER_ENDPOINT_VALUE}") \\
  --database-url $(tycho_combined_family_shell_escape "${TYCHO_INDEXER_DATABASE_URL_VALUE}") \\
  --rpc-url $(tycho_combined_family_shell_escape "${TYCHO_INDEXER_RPC_URL_VALUE}") \\
  index \\
  --extractors-config $(tycho_combined_family_shell_escape "${TYCHO_INDEXER_EXTRACTORS_CONFIG_VALUE}") \\
  --api_token "\$SUBSTREAMS_API_TOKEN"
EOF
}

run_indexer() {
  local previous_strict_doctor="${STRICT_DOCTOR}"
  STRICT_DOCTOR="true"
  doctor >/dev/null
  STRICT_DOCTOR="${previous_strict_doctor}"

  cd "${REPO_ROOT}"
  export AUTH_API_KEY="${AUTH_API_KEY_VALUE}"
  export SUBSTREAMS_API_TOKEN="${SUBSTREAMS_API_TOKEN_VALUE}"
  export RUST_LOG="${TYCHO_INDEXER_RUST_LOG_VALUE}"

  cargo run --bin tycho-indexer -- \
    --endpoint "${TYCHO_INDEXER_ENDPOINT_VALUE}" \
    --database-url "${TYCHO_INDEXER_DATABASE_URL_VALUE}" \
    --rpc-url "${TYCHO_INDEXER_RPC_URL_VALUE}" \
    index \
    --extractors-config "${TYCHO_INDEXER_EXTRACTORS_CONFIG_VALUE}" \
    --api_token "${SUBSTREAMS_API_TOKEN}"
}

case "${MODE}" in
  doctor)
    doctor
    ;;
  command)
    render_command
    ;;
  run)
    run_indexer
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 1
    ;;
esac
