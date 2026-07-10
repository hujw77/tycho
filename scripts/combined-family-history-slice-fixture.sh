#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/combined-family-history-slice-fixture.sh doctor [--strict]
  scripts/combined-family-history-slice-fixture.sh preflight
  scripts/combined-family-history-slice-fixture.sh command
  scripts/combined-family-history-slice-fixture.sh record

Modes:
  doctor     Report whether the environment is ready for the real live-capture workflow.
             With `--strict`, exits non-zero when readiness is false.
  preflight  Resolve the checked-in combined-family Substreams request and print it as JSON.
             This does not open a Substreams session or write the fixture.
  command    Print the exact live-capture command for the checked-in fixture workflow.
  record     Capture the checked-in combined-family history-slice fixture.

Optional environment overrides:
  TYCHO_COMBINED_FIXTURE_START_BLOCK   Default: 25384601
  TYCHO_COMBINED_FIXTURE_STOP_BLOCK    Default: +2
  TYCHO_COMBINED_FIXTURE_OUTPUT        Default:
                                       crates/tycho-indexer/tests/fixtures/combined_family_real_history_slice.json
  TYCHO_COMBINED_FIXTURE_CONFIG        Default:
                                       crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml
  TYCHO_RECORD_ENDPOINT                Required in record mode.
  TYCHO_RECORD_RPC_URL                 Required in record mode.
  SUBSTREAMS_API_TOKEN                 Required in record mode.

Examples:
  scripts/combined-family-history-slice-fixture.sh preflight

  TYCHO_RECORD_ENDPOINT=https://mainnet.eth.streamingfast.io \
  TYCHO_RECORD_RPC_URL=https://rpc.mevblocker.io \
  SUBSTREAMS_API_TOKEN=... \
  scripts/combined-family-history-slice-fixture.sh record
EOF
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required environment variable: ${name}" >&2
    exit 1
  fi
}

shell_escape() {
  local arg="$1"
  if [[ "${arg}" =~ ^[A-Za-z0-9_./:+=-]+$ ]]; then
    printf '%s' "${arg}"
    return
  fi
  printf "'%s'" "${arg//\'/\'\"\'\"\'}"
}

MODE="${1:-}"
STRICT_DOCTOR="false"
if [[ -z "${MODE}" || "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" == "doctor" && "${2:-}" == "--strict" ]]; then
  STRICT_DOCTOR="true"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

START_BLOCK="${TYCHO_COMBINED_FIXTURE_START_BLOCK:-25384601}"
STOP_BLOCK="${TYCHO_COMBINED_FIXTURE_STOP_BLOCK:-+2}"
OUTPUT_PATH="${TYCHO_COMBINED_FIXTURE_OUTPUT:-${REPO_ROOT}/crates/tycho-indexer/tests/fixtures/combined_family_real_history_slice.json}"
EXTRACTORS_CONFIG="${TYCHO_COMBINED_FIXTURE_CONFIG:-${REPO_ROOT}/crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml}"

BASE_CMD=(
  cargo run --bin tycho-indexer --
  --database-url postgres://unused
  --endpoint http://localhost:9000
  --rpc-url http://localhost:8545
  record-substreams
  --substreams-api-token token
  --extractors-config "${EXTRACTORS_CONFIG}"
  --family uniswap
  --start-block "${START_BLOCK}"
  --stop-block "${STOP_BLOCK}"
  --output "${OUTPUT_PATH}"
)

render_record_cmd() {
  local endpoint="${TYCHO_RECORD_ENDPOINT:-<set TYCHO_RECORD_ENDPOINT>}"
  local rpc_url="${TYCHO_RECORD_RPC_URL:-<set TYCHO_RECORD_RPC_URL>}"
  local api_token="${SUBSTREAMS_API_TOKEN:-<set SUBSTREAMS_API_TOKEN>}"
  cat <<EOF
cargo run --bin tycho-indexer -- \\
  --database-url postgres://unused \\
  --endpoint $(shell_escape "${endpoint}") \\
  --rpc-url $(shell_escape "${rpc_url}") \\
  record-substreams \\
  --substreams-api-token $(shell_escape "${api_token}") \\
  --extractors-config $(shell_escape "${EXTRACTORS_CONFIG}") \\
  --family uniswap \\
  --start-block $(shell_escape "${START_BLOCK}") \\
  --stop-block $(shell_escape "${STOP_BLOCK}") \\
  --output $(shell_escape "${OUTPUT_PATH}")
EOF
}

doctor() {
  local ready="true"
  local token_state="set"
  local endpoint_state="set"
  local rpc_state="set"

  if [[ -z "${SUBSTREAMS_API_TOKEN:-}" ]]; then
    ready="false"
    token_state="missing"
  fi
  if [[ -z "${TYCHO_RECORD_ENDPOINT:-}" ]]; then
    ready="false"
    endpoint_state="missing"
  fi
  if [[ -z "${TYCHO_RECORD_RPC_URL:-}" ]]; then
    ready="false"
    rpc_state="missing"
  fi

  cat <<EOF
ready=${ready}
start_block=${START_BLOCK}
stop_block=${STOP_BLOCK}
extractors_config=${EXTRACTORS_CONFIG}
output_path=${OUTPUT_PATH}
substreams_api_token=${token_state}
record_endpoint=${endpoint_state}
record_rpc_url=${rpc_state}
EOF

  if [[ "${STRICT_DOCTOR}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

case "${MODE}" in
  doctor)
    doctor
    ;;
  preflight)
    cd "${REPO_ROOT}"
    "${BASE_CMD[@]}" --print-request
    ;;
  command)
    render_record_cmd
    ;;
  record)
    require_env TYCHO_RECORD_ENDPOINT
    require_env TYCHO_RECORD_RPC_URL
    require_env SUBSTREAMS_API_TOKEN
    cd "${REPO_ROOT}"
    cargo run --bin tycho-indexer -- \
      --database-url postgres://unused \
      --endpoint "${TYCHO_RECORD_ENDPOINT}" \
      --rpc-url "${TYCHO_RECORD_RPC_URL}" \
      record-substreams \
      --substreams-api-token "${SUBSTREAMS_API_TOKEN}" \
      --extractors-config "${EXTRACTORS_CONFIG}" \
      --family uniswap \
      --start-block "${START_BLOCK}" \
      --stop-block "${STOP_BLOCK}" \
      --output "${OUTPUT_PATH}"
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 1
    ;;
esac
