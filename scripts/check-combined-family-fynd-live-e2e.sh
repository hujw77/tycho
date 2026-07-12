#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/check-combined-family-fynd-live-e2e.sh doctor [--strict]
  scripts/check-combined-family-fynd-live-e2e.sh command [route|settlement|all]
  scripts/check-combined-family-fynd-live-e2e.sh run-route
  scripts/check-combined-family-fynd-live-e2e.sh run-settlement
  scripts/check-combined-family-fynd-live-e2e.sh run-all

Modes:
  doctor          Report whether the local live combined-family Fynd E2E environment is ready.
                  With `--strict`, exits non-zero when readiness is false.
  command         Print the exact command for the selected live E2E mode.
  run-route       Run the combined-family route-return ignored test in the sibling Fynd repo.
  run-settlement  Run the combined-family quote-settlement ignored test in the sibling Fynd repo.
  run-all         Run both combined-family ignored tests in sequence.

Optional environment overrides:
  FYND_REPO_ROOT          Default: sibling ../fynd
  FYND_E2E_TYCHO_URL      Default: 127.0.0.1:4242
  FYND_E2E_RPC_URL        Default: https://rpc.mevblocker.io
  FYND_E2E_RUST_LOG       Default: info,tycho_client=info,tycho_simulation=info,fynd=info
  FYND_E2E_ROUTE_TEST     Default: quote_returns_route_for_combined_uniswap_family
  FYND_E2E_SETTLEMENT_TEST
                         Default: quote_settles_within_encoded_bounds_at_quote_block_for_combined_uniswap_family
  TYCHO_STREAM_WS_BUFFER_SIZE
  TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE

Examples:
  scripts/check-combined-family-fynd-live-e2e.sh doctor
  scripts/check-combined-family-fynd-live-e2e.sh command all
  FYND_E2E_TYCHO_URL=127.0.0.1:4242 \
  FYND_E2E_RPC_URL=https://rpc.mevblocker.io \
  scripts/check-combined-family-fynd-live-e2e.sh run-all
EOF
}

shell_escape() {
  local arg="$1"
  if [[ "${arg}" =~ ^[A-Za-z0-9_./:+=,-]+$ ]]; then
    printf '%s' "${arg}"
    return
  fi
  printf "'%s'" "${arg//\'/\'\"\'\"\'}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_FYND_REPO_ROOT="$(cd "${REPO_ROOT}/.." && pwd)/fynd"

FYND_REPO_ROOT="${FYND_REPO_ROOT:-${DEFAULT_FYND_REPO_ROOT}}"
FYND_E2E_TYCHO_URL="${FYND_E2E_TYCHO_URL:-127.0.0.1:4242}"
FYND_E2E_RPC_URL="${FYND_E2E_RPC_URL:-https://rpc.mevblocker.io}"
FYND_E2E_RUST_LOG="${FYND_E2E_RUST_LOG:-info,tycho_client=info,tycho_simulation=info,fynd=info}"
TYCHO_STREAM_WS_BUFFER_SIZE_VALUE="${TYCHO_STREAM_WS_BUFFER_SIZE:-}"
TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE="${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE:-}"

ROUTE_TEST="${FYND_E2E_ROUTE_TEST:-quote_returns_route_for_combined_uniswap_family}"
SETTLEMENT_TEST="${FYND_E2E_SETTLEMENT_TEST:-quote_settles_within_encoded_bounds_at_quote_block_for_combined_uniswap_family}"

mode="${1:-}"
strict="false"
if [[ -z "${mode}" || "${mode}" == "-h" || "${mode}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${mode}" == "doctor" && "${2:-}" == "--strict" ]]; then
  strict="true"
fi

fynd_repo_exists="true"
fynd_test_exists="true"
tycho_health="unknown"
curl_available="true"
ready="true"

if [[ ! -d "${FYND_REPO_ROOT}" || ! -f "${FYND_REPO_ROOT}/Cargo.toml" ]]; then
  fynd_repo_exists="false"
  ready="false"
fi

if [[ ! -f "${FYND_REPO_ROOT}/tests/e2e_quote.rs" ]]; then
  fynd_test_exists="false"
  ready="false"
fi

if ! command -v curl >/dev/null 2>&1; then
  curl_available="false"
  tycho_health="unverified"
else
  if curl -fsS "http://${FYND_E2E_TYCHO_URL}/v1/health" >/dev/null 2>&1; then
    tycho_health="reachable"
  else
    tycho_health="unreachable"
    ready="false"
  fi
fi

doctor() {
  cat <<EOF
ready=${ready}
fynd_repo_root=${FYND_REPO_ROOT}
fynd_repo_exists=${fynd_repo_exists}
fynd_test_exists=${fynd_test_exists}
tycho_url=${FYND_E2E_TYCHO_URL}
tycho_health=${tycho_health}
rpc_url=${FYND_E2E_RPC_URL}
rust_log=${FYND_E2E_RUST_LOG}
route_test=${ROUTE_TEST}
settlement_test=${SETTLEMENT_TEST}
curl_available=${curl_available}
tycho_stream_ws_buffer_size=${TYCHO_STREAM_WS_BUFFER_SIZE_VALUE:-default}
tycho_stream_subscription_buffer_size=${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE:-default}
EOF

  if [[ "${strict}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

render_command() {
  local selected="${1:-all}"
  local env_prefix
  env_prefix="$(render_env_prefix)"
  local test_name
  case "${selected}" in
    route)
      test_name="${ROUTE_TEST}"
      ;;
    settlement)
      test_name="${SETTLEMENT_TEST}"
      ;;
    all)
      cat <<EOF
cd $(shell_escape "${FYND_REPO_ROOT}") && \\
${env_prefix}
cargo test --test e2e_quote ${ROUTE_TEST} -- --ignored --nocapture && \\
${env_prefix}
cargo test --test e2e_quote ${SETTLEMENT_TEST} -- --ignored --nocapture
EOF
      return
      ;;
    *)
      echo "unknown command selection: ${selected}" >&2
      exit 1
      ;;
  esac

  cat <<EOF
cd $(shell_escape "${FYND_REPO_ROOT}") && \\
${env_prefix}
cargo test --test e2e_quote ${test_name} -- --ignored --nocapture
EOF
}

render_env_prefix() {
  printf 'RUST_LOG=%s \\\n' \
    "$(shell_escape "${FYND_E2E_RUST_LOG}")"
  printf 'FYND_E2E_TYCHO_URL=%s \\\n' \
    "$(shell_escape "${FYND_E2E_TYCHO_URL}")"
  printf 'FYND_E2E_RPC_URL=%s \\\n' \
    "$(shell_escape "${FYND_E2E_RPC_URL}")"
  if [[ -n "${TYCHO_STREAM_WS_BUFFER_SIZE_VALUE}" ]]; then
    printf 'TYCHO_STREAM_WS_BUFFER_SIZE=%s \\\n' \
      "$(shell_escape "${TYCHO_STREAM_WS_BUFFER_SIZE_VALUE}")"
  fi
  if [[ -n "${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE}" ]]; then
    printf 'TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE=%s \\\n' \
      "$(shell_escape "${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE}")"
  fi
}

run_one() {
  local test_name="$1"
  cd "${FYND_REPO_ROOT}"
  RUST_LOG="${FYND_E2E_RUST_LOG}" \
  FYND_E2E_TYCHO_URL="${FYND_E2E_TYCHO_URL}" \
  FYND_E2E_RPC_URL="${FYND_E2E_RPC_URL}" \
  TYCHO_STREAM_WS_BUFFER_SIZE="${TYCHO_STREAM_WS_BUFFER_SIZE_VALUE}" \
  TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE="${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE}" \
  cargo test --test e2e_quote "${test_name}" -- --ignored --nocapture
}

case "${mode}" in
  doctor)
    doctor
    ;;
  command)
    render_command "${2:-all}"
    ;;
  run-route)
    strict="true"
    doctor >/dev/null
    run_one "${ROUTE_TEST}"
    ;;
  run-settlement)
    strict="true"
    doctor >/dev/null
    run_one "${SETTLEMENT_TEST}"
    ;;
  run-all)
    strict="true"
    doctor >/dev/null
    run_one "${ROUTE_TEST}"
    run_one "${SETTLEMENT_TEST}"
    ;;
  *)
    echo "unknown mode: ${mode}" >&2
    usage >&2
    exit 1
    ;;
esac
