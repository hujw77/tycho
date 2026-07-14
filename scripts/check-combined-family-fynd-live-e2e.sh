#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/check-combined-family-fynd-live-e2e.sh doctor [--strict]
  scripts/check-combined-family-fynd-live-e2e.sh list
  scripts/check-combined-family-fynd-live-e2e.sh command [route|settlement|all]
  scripts/check-combined-family-fynd-live-e2e.sh run-route
  scripts/check-combined-family-fynd-live-e2e.sh run-settlement
  scripts/check-combined-family-fynd-live-e2e.sh run-all

Modes:
  doctor          Report whether the local live combined-family Fynd E2E environment is ready.
                  With `--strict`, exits non-zero when readiness is false.
  list            Print the manifest-backed route/settlement ignored-test mapping.
  command         Print the exact command for the selected live E2E mode.
  run-route       Run the combined-family route-return ignored test in the sibling Fynd repo.
  run-settlement  Run the combined-family quote-settlement ignored test in the sibling Fynd repo.
  run-all         Run both combined-family ignored tests in sequence.

Optional environment overrides:
  FYND_REPO_ROOT          Default: sibling ../fynd
  FYND_E2E_TYCHO_URL      Default: 127.0.0.1:4242
  FYND_E2E_RPC_URL        Default: https://rpc.mevblocker.io
  FYND_E2E_RUST_LOG       Default: info,tycho_client=info,tycho_simulation=info,fynd=info
  FYND_E2E_HEALTH_TIMEOUT_SECS
                         Default: 300
  FYND_E2E_TRADED_N_DAYS_AGO
                         Default: 3
  FYND_E2E_CLIENT_TIMEOUT_SECS
                         Default: 5
  FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS
                         Default: 1
  FYND_E2E_MIN_TOKEN_QUALITY
                         Default: 100
  FYND_E2E_HEALTH_MODE   Optional override forwarded to the Fynd e2e test.
  FYND_E2E_QUOTE_TIMEOUT_SECS
                         Default: 420
                         Optional override forwarded to the Fynd e2e test.
  FYND_E2E_CONNECTOR_TOKENS
                         Default: WETH,USDC,USDT,DAI,WBTC
                         Optional comma-separated connector-token allowlist for intermediate hops.
  TYCHO_COMBINED_FAMILY_LIVE_TEST_MANIFEST
                         Optional override manifest path for the live route/settlement test map.
  FYND_E2E_ROUTE_TEST     Default: quote_returns_route_for_combined_uniswap_family
  FYND_E2E_SETTLEMENT_TEST
                         Default: quote_settles_within_encoded_bounds_at_quote_block_for_combined_uniswap_family
  TYCHO_STREAM_WS_BUFFER_SIZE
  TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE
  TYCHO_COMBINED_FAMILY_CHAIN
                         Default: ethereum

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
LIVE_TEST_MANIFEST="${TYCHO_COMBINED_FAMILY_LIVE_TEST_MANIFEST:-${REPO_ROOT}/crates/tycho-indexer/tests/combined_family_live_gate.tests}"
DEFAULT_FYND_E2E_CONNECTOR_TOKENS="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,0xdac17f958d2ee523a2206206994597c13d831ec7,0x6b175474e89094c44da98b954eedeac495271d0f,0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

FYND_REPO_ROOT="${FYND_REPO_ROOT:-${DEFAULT_FYND_REPO_ROOT}}"
FYND_E2E_TYCHO_URL="${FYND_E2E_TYCHO_URL:-127.0.0.1:4242}"
FYND_E2E_RPC_URL="${FYND_E2E_RPC_URL:-https://rpc.mevblocker.io}"
FYND_E2E_RUST_LOG="${FYND_E2E_RUST_LOG:-info,tycho_client=info,tycho_simulation=info,fynd=info}"
FYND_E2E_HEALTH_TIMEOUT_SECS="${FYND_E2E_HEALTH_TIMEOUT_SECS:-300}"
FYND_E2E_TRADED_N_DAYS_AGO="${FYND_E2E_TRADED_N_DAYS_AGO:-3}"
FYND_E2E_CLIENT_TIMEOUT_SECS="${FYND_E2E_CLIENT_TIMEOUT_SECS:-5}"
FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS="${FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS:-1}"
FYND_E2E_MIN_TOKEN_QUALITY="${FYND_E2E_MIN_TOKEN_QUALITY:-100}"
FYND_E2E_HEALTH_MODE_VALUE="${FYND_E2E_HEALTH_MODE:-}"
FYND_E2E_QUOTE_TIMEOUT_SECS_VALUE="${FYND_E2E_QUOTE_TIMEOUT_SECS:-420}"
FYND_E2E_CONNECTOR_TOKENS_VALUE="${FYND_E2E_CONNECTOR_TOKENS:-${DEFAULT_FYND_E2E_CONNECTOR_TOKENS}}"
TYCHO_STREAM_WS_BUFFER_SIZE_VALUE="${TYCHO_STREAM_WS_BUFFER_SIZE:-}"
TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE="${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE:-}"
TYCHO_COMBINED_FAMILY_CHAIN_VALUE="${TYCHO_COMBINED_FAMILY_CHAIN:-ethereum}"

ROUTE_TEST=""
SETTLEMENT_TEST=""

mode="${1:-}"
strict="false"
if [[ -z "${mode}" || "${mode}" == "-h" || "${mode}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${mode}" == "doctor" && "${2:-}" == "--strict" ]]; then
  strict="true"
fi

load_live_tests() {
  if [[ ! -f "${LIVE_TEST_MANIFEST}" ]]; then
    echo "missing combined-family live gate manifest: ${LIVE_TEST_MANIFEST}" >&2
    exit 1
  fi

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="$(printf '%s' "${line}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
    if [[ -z "${line}" || "${line}" =~ ^# ]]; then
      continue
    fi

    local selection
    local test_name
    read -r selection test_name extra <<<"${line}"
    if [[ -n "${extra:-}" || -z "${selection:-}" || -z "${test_name:-}" ]]; then
      echo "invalid combined-family live gate manifest entry: ${line}" >&2
      exit 1
    fi

    case "${selection}" in
      route)
        ROUTE_TEST="${test_name}"
        ;;
      settlement)
        SETTLEMENT_TEST="${test_name}"
        ;;
      *)
        echo "unknown combined-family live gate selection in manifest: ${selection}" >&2
        exit 1
        ;;
    esac
  done < "${LIVE_TEST_MANIFEST}"

  if [[ -z "${ROUTE_TEST}" || -z "${SETTLEMENT_TEST}" ]]; then
    echo "combined-family live gate manifest must define both route and settlement tests" >&2
    exit 1
  fi

  ROUTE_TEST="${FYND_E2E_ROUTE_TEST:-${ROUTE_TEST}}"
  SETTLEMENT_TEST="${FYND_E2E_SETTLEMENT_TEST:-${SETTLEMENT_TEST}}"
}

load_live_tests

fynd_repo_exists="true"
fynd_test_exists="true"
route_test_exists="unknown"
settlement_test_exists="unknown"
live_test_mapping_ready="unknown"
tycho_health="unknown"
tycho_protocols_ready="unknown"
protocol_v2_ready="unknown"
protocol_v3_ready="unknown"
curl_available="true"
ready="true"
fynd_e2e_test_path="${FYND_REPO_ROOT}/tests/e2e_quote.rs"

if [[ ! -d "${FYND_REPO_ROOT}" || ! -f "${FYND_REPO_ROOT}/Cargo.toml" ]]; then
  fynd_repo_exists="false"
  ready="false"
fi

if [[ ! -f "${fynd_e2e_test_path}" ]]; then
  fynd_test_exists="false"
  ready="false"
fi

fynd_declares_ignored_test() {
  local test_name="$1"
  local test_file="$2"
  grep -Eq "fn[[:space:]]+${test_name}[[:space:]]*\\(" "${test_file}"
}

if [[ "${fynd_test_exists}" == "true" ]]; then
  if fynd_declares_ignored_test "${ROUTE_TEST}" "${fynd_e2e_test_path}"; then
    route_test_exists="true"
  else
    route_test_exists="false"
    ready="false"
  fi

  if fynd_declares_ignored_test "${SETTLEMENT_TEST}" "${fynd_e2e_test_path}"; then
    settlement_test_exists="true"
  else
    settlement_test_exists="false"
    ready="false"
  fi

  if [[ "${route_test_exists}" == "true" && "${settlement_test_exists}" == "true" ]]; then
    live_test_mapping_ready="true"
  else
    live_test_mapping_ready="false"
  fi
fi

if ! command -v curl >/dev/null 2>&1; then
  curl_available="false"
  tycho_health="unverified"
  tycho_protocols_ready="unverified"
else
  if curl -fsS "http://${FYND_E2E_TYCHO_URL}/v1/health" >/dev/null 2>&1; then
    tycho_health="reachable"

    protocol_components_ready() {
      local protocol_system="$1"
      local payload
      payload=$(cat <<EOF
{"chain":"${TYCHO_COMBINED_FAMILY_CHAIN_VALUE}","protocol_system":"${protocol_system}","tvl_gt":0,"pagination":{"page":0,"page_size":1}}
EOF
)

      local response
      response="$(
        curl -fsS \
          -X POST \
          "http://${FYND_E2E_TYCHO_URL}/v1/protocol_components" \
          -H 'content-type: application/json' \
          -d "${payload}" \
          2>/dev/null || true
      )"
      [[ -n "${response}" ]] && printf '%s' "${response}" | tr -d '[:space:]' | grep -Eq '"total":[1-9][0-9]*'
    }

    if protocol_components_ready "uniswap_v2"; then
      protocol_v2_ready="true"
    else
      protocol_v2_ready="false"
    fi

    if protocol_components_ready "uniswap_v3"; then
      protocol_v3_ready="true"
    else
      protocol_v3_ready="false"
    fi

    if [[ "${protocol_v2_ready}" == "true" && "${protocol_v3_ready}" == "true" ]]; then
      tycho_protocols_ready="true"
    else
      tycho_protocols_ready="false"
      ready="false"
    fi
  else
    tycho_health="unreachable"
    tycho_protocols_ready="unreachable"
    ready="false"
  fi
fi

doctor() {
  cat <<EOF
ready=${ready}
fynd_repo_root=${FYND_REPO_ROOT}
fynd_repo_exists=${fynd_repo_exists}
fynd_test_exists=${fynd_test_exists}
route_test_exists=${route_test_exists}
settlement_test_exists=${settlement_test_exists}
live_test_mapping_ready=${live_test_mapping_ready}
tycho_url=${FYND_E2E_TYCHO_URL}
tycho_health=${tycho_health}
tycho_protocols_ready=${tycho_protocols_ready}
protocol_v2_ready=${protocol_v2_ready}
protocol_v3_ready=${protocol_v3_ready}
chain=${TYCHO_COMBINED_FAMILY_CHAIN_VALUE}
rpc_url=${FYND_E2E_RPC_URL}
rust_log=${FYND_E2E_RUST_LOG}
health_timeout_secs=${FYND_E2E_HEALTH_TIMEOUT_SECS}
traded_n_days_ago=${FYND_E2E_TRADED_N_DAYS_AGO}
client_timeout_secs=${FYND_E2E_CLIENT_TIMEOUT_SECS}
client_retry_max_attempts=${FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS}
min_token_quality=${FYND_E2E_MIN_TOKEN_QUALITY}
health_mode_override=${FYND_E2E_HEALTH_MODE_VALUE:-default}
route_health_mode=$(effective_health_mode_for_selection route)
settlement_health_mode=$(effective_health_mode_for_selection settlement)
quote_timeout_secs=${FYND_E2E_QUOTE_TIMEOUT_SECS_VALUE}
connector_tokens=${FYND_E2E_CONNECTOR_TOKENS_VALUE}
route_test=${ROUTE_TEST}
settlement_test=${SETTLEMENT_TEST}
curl_available=${curl_available}
tycho_stream_ws_buffer_size=${TYCHO_STREAM_WS_BUFFER_SIZE_VALUE:-default}
tycho_stream_subscription_buffer_size=${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE:-default}
test_manifest=${LIVE_TEST_MANIFEST}
EOF

  if [[ "${strict}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

list_tests() {
  printf 'route=%s\n' "${ROUTE_TEST}"
  printf 'settlement=%s\n' "${SETTLEMENT_TEST}"
}

render_command() {
  local selected="${1:-all}"
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
$(render_env_prefix route)
cargo test --test e2e_quote ${ROUTE_TEST} -- --ignored --nocapture && \\
$(render_env_prefix settlement)
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
$(render_env_prefix "${selected}")
cargo test --test e2e_quote ${test_name} -- --ignored --nocapture
EOF
}

effective_health_mode_for_selection() {
  local selection="$1"
  if [[ -n "${FYND_E2E_HEALTH_MODE_VALUE}" ]]; then
    printf '%s' "${FYND_E2E_HEALTH_MODE_VALUE}"
    return
  fi

  case "${selection}" in
    route)
      printf '%s' "quote_ready"
      ;;
    settlement)
      printf '%s' "quote_ready"
      ;;
    *)
      echo "unknown health-mode selection: ${selection}" >&2
      exit 1
      ;;
  esac
}

render_env_prefix() {
  local selection="${1:-route}"
  local effective_health_mode
  effective_health_mode="$(effective_health_mode_for_selection "${selection}")"
  printf 'RUST_LOG=%s \\\n' \
    "$(shell_escape "${FYND_E2E_RUST_LOG}")"
  printf 'FYND_E2E_TYCHO_URL=%s \\\n' \
    "$(shell_escape "${FYND_E2E_TYCHO_URL}")"
  printf 'FYND_E2E_RPC_URL=%s \\\n' \
    "$(shell_escape "${FYND_E2E_RPC_URL}")"
  printf 'FYND_E2E_HEALTH_TIMEOUT_SECS=%s \\\n' \
    "$(shell_escape "${FYND_E2E_HEALTH_TIMEOUT_SECS}")"
  printf 'FYND_E2E_TRADED_N_DAYS_AGO=%s \\\n' \
    "$(shell_escape "${FYND_E2E_TRADED_N_DAYS_AGO}")"
  printf 'FYND_E2E_CLIENT_TIMEOUT_SECS=%s \\\n' \
    "$(shell_escape "${FYND_E2E_CLIENT_TIMEOUT_SECS}")"
  printf 'FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS=%s \\\n' \
      "$(shell_escape "${FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS}")"
  printf 'FYND_E2E_MIN_TOKEN_QUALITY=%s \\\n' \
    "$(shell_escape "${FYND_E2E_MIN_TOKEN_QUALITY}")"
  printf 'FYND_E2E_HEALTH_MODE=%s \\\n' \
    "$(shell_escape "${effective_health_mode}")"
  if [[ -n "${FYND_E2E_QUOTE_TIMEOUT_SECS_VALUE}" ]]; then
    printf 'FYND_E2E_QUOTE_TIMEOUT_SECS=%s \\\n' \
      "$(shell_escape "${FYND_E2E_QUOTE_TIMEOUT_SECS_VALUE}")"
  fi
  if [[ -n "${FYND_E2E_CONNECTOR_TOKENS_VALUE}" ]]; then
    printf 'FYND_E2E_CONNECTOR_TOKENS=%s \\\n' \
      "$(shell_escape "${FYND_E2E_CONNECTOR_TOKENS_VALUE}")"
  fi
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
  local selection="$2"
  local effective_health_mode
  effective_health_mode="$(effective_health_mode_for_selection "${selection}")"
  cd "${FYND_REPO_ROOT}"
  local -a env_cmd=(env)
  local -a env_args=(
    "RUST_LOG=${FYND_E2E_RUST_LOG}"
    "FYND_E2E_TYCHO_URL=${FYND_E2E_TYCHO_URL}"
    "FYND_E2E_RPC_URL=${FYND_E2E_RPC_URL}"
    "FYND_E2E_HEALTH_TIMEOUT_SECS=${FYND_E2E_HEALTH_TIMEOUT_SECS}"
    "FYND_E2E_TRADED_N_DAYS_AGO=${FYND_E2E_TRADED_N_DAYS_AGO}"
    "FYND_E2E_CLIENT_TIMEOUT_SECS=${FYND_E2E_CLIENT_TIMEOUT_SECS}"
    "FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS=${FYND_E2E_CLIENT_RETRY_MAX_ATTEMPTS}"
    "FYND_E2E_MIN_TOKEN_QUALITY=${FYND_E2E_MIN_TOKEN_QUALITY}"
    "FYND_E2E_HEALTH_MODE=${effective_health_mode}"
    "FYND_E2E_CONNECTOR_TOKENS=${FYND_E2E_CONNECTOR_TOKENS_VALUE}"
  )
  if [[ -n "${FYND_E2E_QUOTE_TIMEOUT_SECS_VALUE}" ]]; then
    env_args+=("FYND_E2E_QUOTE_TIMEOUT_SECS=${FYND_E2E_QUOTE_TIMEOUT_SECS_VALUE}")
  fi
  if [[ -n "${TYCHO_STREAM_WS_BUFFER_SIZE_VALUE}" ]]; then
    env_args+=("TYCHO_STREAM_WS_BUFFER_SIZE=${TYCHO_STREAM_WS_BUFFER_SIZE_VALUE}")
  else
    env_cmd+=(-u "TYCHO_STREAM_WS_BUFFER_SIZE")
  fi
  if [[ -n "${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE}" ]]; then
    env_args+=("TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE=${TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE_VALUE}")
  else
    env_cmd+=(-u "TYCHO_STREAM_SUBSCRIPTION_BUFFER_SIZE")
  fi

  "${env_cmd[@]}" "${env_args[@]}" cargo test --test e2e_quote "${test_name}" -- --ignored --nocapture
}

case "${mode}" in
  doctor)
    doctor
    ;;
  list)
    list_tests
    ;;
  command)
    render_command "${2:-all}"
    ;;
  run-route)
    strict="true"
    doctor >/dev/null
    run_one "${ROUTE_TEST}" "route"
    ;;
  run-settlement)
    strict="true"
    doctor >/dev/null
    run_one "${SETTLEMENT_TEST}" "settlement"
    ;;
  run-all)
    strict="true"
    doctor >/dev/null
    run_one "${ROUTE_TEST}" "route"
    run_one "${SETTLEMENT_TEST}" "settlement"
    ;;
  *)
    echo "unknown mode: ${mode}" >&2
    usage >&2
    exit 1
    ;;
esac
