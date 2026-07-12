#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/check-combined-family.sh doctor [--strict]
  scripts/check-combined-family.sh command [acceptance|repo|live|live-managed|full|full-managed|all]
  scripts/check-combined-family.sh run-acceptance
  scripts/check-combined-family.sh run-repo
  scripts/check-combined-family.sh run-live
  scripts/check-combined-family.sh run-live-managed
  scripts/check-combined-family.sh run-full
  scripts/check-combined-family.sh run-full-managed
  scripts/check-combined-family.sh run-all

Modes:
  doctor    Report whether the combined-family validation surface is ready.
            With `--strict`, exits non-zero when any required dependency is not ready.
  command   Print the exact command for the selected validation mode.
            `acceptance` runs the repo-local DB-backed shared-runtime acceptance gate.
            `repo` runs the repo-local DB-backed regression gate.
            `live` runs the live combined-family Fynd E2E gate.
            `live-managed` starts the combined-family indexer, waits for health, then runs the
            live Fynd E2E gate.
            `full` runs `acceptance` first, then `live`.
            `full-managed` runs `acceptance` first, then `live-managed`.
            `all` runs `repo` first, then `live`.
  run-acceptance Execute the repo-local DB-backed shared-runtime acceptance gate.
  run-repo  Execute the repo-local DB-backed regression gate.
  run-live  Execute the live combined-family Fynd E2E gate.
  run-live-managed Start the combined-family indexer, then execute the live Fynd E2E gate.
  run-full  Execute the repo-local DB gate, then the live Fynd E2E gate.
  run-full-managed Execute the repo-local DB gate, then the managed live Fynd E2E gate.
  run-all   Execute the repo-local DB gate, then the live Fynd E2E gate.

Environment:
  DATABASE_URL           Forwarded to `check-combined-family-db.sh`
  TYCHO_COMBINED_FAMILY_DB_TEST_MANIFEST
                         Forwarded to `check-combined-family-db.sh`
  TYCHO_COMBINED_FAMILY_LIVE_SELECTION
                         Default: all
                         One of: route, settlement, all
  SUBSTREAMS_API_TOKEN   Forwarded to `run-combined-family-indexer.sh` doctor diagnostics
  AUTH_API_KEY           Forwarded to `run-combined-family-indexer.sh` doctor diagnostics
  TYCHO_INDEXER_ENDPOINT Forwarded to `run-combined-family-indexer.sh` doctor diagnostics
  TYCHO_INDEXER_DATABASE_URL
                         Forwarded to `run-combined-family-indexer.sh` doctor diagnostics
  TYCHO_INDEXER_RPC_URL  Forwarded to `run-combined-family-indexer.sh` doctor diagnostics
  TYCHO_INDEXER_EXTRACTORS_CONFIG
                         Forwarded to `run-combined-family-indexer.sh` doctor diagnostics
  TYCHO_INDEXER_RUST_LOG Forwarded to `run-combined-family-indexer.sh` doctor diagnostics
  FYND_REPO_ROOT         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_TYCHO_URL     Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_RPC_URL       Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_RUST_LOG      Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_ROUTE_TEST    Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_SETTLEMENT_TEST
                         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  TYCHO_COMBINED_FAMILY_MANAGED_HEALTH_TIMEOUT_SECS
                         Default: 90
                         Used by `run-live-managed` and `run-full-managed`
  TYCHO_COMBINED_FAMILY_MANAGED_INDEXER_LOG
                         Optional fixed log file for the managed indexer process
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
DB_GATE_SCRIPT="${SCRIPT_DIR}/check-combined-family-db.sh"
LIVE_GATE_SCRIPT="${SCRIPT_DIR}/check-combined-family-fynd-live-e2e.sh"
INDEXER_RUN_SCRIPT="${SCRIPT_DIR}/run-combined-family-indexer.sh"

mode="${1:-}"
strict="false"
LIVE_SELECTION="${TYCHO_COMBINED_FAMILY_LIVE_SELECTION:-all}"
MANAGED_HEALTH_TIMEOUT_SECS="${TYCHO_COMBINED_FAMILY_MANAGED_HEALTH_TIMEOUT_SECS:-90}"

if [[ -z "${mode}" || "${mode}" == "-h" || "${mode}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${mode}" == "doctor" && "${2:-}" == "--strict" ]]; then
  strict="true"
fi

case "${LIVE_SELECTION}" in
  route|settlement|all)
    ;;
  *)
    echo "invalid TYCHO_COMBINED_FAMILY_LIVE_SELECTION: ${LIVE_SELECTION}" >&2
    echo "expected one of: route, settlement, all" >&2
    exit 1
    ;;
esac

flatten_output() {
  tr '\n' ' ' | sed 's/[[:space:]]\+/ /g; s/ $//'
}

is_live_tycho_healthy() {
  curl -fsS "http://${FYND_E2E_TYCHO_URL:-127.0.0.1:4242}/v1/health" >/dev/null 2>&1
}

wait_for_live_tycho_health() {
  local timeout_secs="$1"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    if is_live_tycho_healthy; then
      return 0
    fi

    local now_ts
    now_ts="$(date +%s)"
    if (( now_ts - start_ts >= timeout_secs )); then
      return 1
    fi

    sleep 2
  done
}

managed_indexer_log_path() {
  if [[ -n "${TYCHO_COMBINED_FAMILY_MANAGED_INDEXER_LOG:-}" ]]; then
    printf '%s' "${TYCHO_COMBINED_FAMILY_MANAGED_INDEXER_LOG}"
    return
  fi

  mktemp "${TMPDIR:-/tmp}/tycho-combined-family-indexer.XXXXXX.log"
}

run_live_managed() {
  if is_live_tycho_healthy; then
    run_live
    return
  fi

  "${INDEXER_RUN_SCRIPT}" doctor --strict >/dev/null

  local log_path
  log_path="$(managed_indexer_log_path)"

  cd "${SCRIPT_DIR}"
  "${INDEXER_RUN_SCRIPT}" run >"${log_path}" 2>&1 &
  local indexer_pid=$!

  cleanup_managed_indexer() {
    if kill -0 "${indexer_pid}" >/dev/null 2>&1; then
      kill "${indexer_pid}" >/dev/null 2>&1 || true
      wait "${indexer_pid}" >/dev/null 2>&1 || true
    fi
  }

  trap cleanup_managed_indexer EXIT

  if ! wait_for_live_tycho_health "${MANAGED_HEALTH_TIMEOUT_SECS}"; then
    echo "managed combined-family indexer did not become healthy within ${MANAGED_HEALTH_TIMEOUT_SECS}s" >&2
    echo "managed indexer log: ${log_path}" >&2
    return 1
  fi

  run_live
}

readiness_from_output() {
  local rendered="$1"
  printf '%s\n' "${rendered}" | awk -F= '$1 == "ready" { print $2; exit }'
}

run_doctor_capture() {
  local target_script="$1"
  local output
  if ! output="$("${target_script}" doctor)"; then
    echo "failed to execute doctor mode for ${target_script}" >&2
    exit 1
  fi
  printf '%s' "${output}"
}

doctor() {
  local repo_output
  local live_output
  local operator_output
  local repo_ready
  local live_ready
  local operator_ready
  local ready="true"

  repo_output="$(run_doctor_capture "${DB_GATE_SCRIPT}")"
  live_output="$(run_doctor_capture "${LIVE_GATE_SCRIPT}")"
  operator_output="$(run_doctor_capture "${INDEXER_RUN_SCRIPT}")"
  repo_ready="$(readiness_from_output "${repo_output}")"
  live_ready="$(readiness_from_output "${live_output}")"
  operator_ready="$(readiness_from_output "${operator_output}")"

  if [[ "${repo_ready}" != "true" || "${live_ready}" != "true" ]]; then
    ready="false"
  fi

  cat <<EOF
ready=${ready}
acceptance_ready=${repo_ready}
full_ready=${ready}
repo_ready=${repo_ready}
live_ready=${live_ready}
operator_ready=${operator_ready}
managed_live_ready=$(if [[ "${operator_ready}" == "true" ]]; then printf 'true'; else printf 'false'; fi)
managed_full_ready=$(if [[ "${repo_ready}" == "true" && "${operator_ready}" == "true" ]]; then printf 'true'; else printf 'false'; fi)
db_gate_script=${DB_GATE_SCRIPT}
live_gate_script=${LIVE_GATE_SCRIPT}
indexer_run_script=${INDEXER_RUN_SCRIPT}
repo_doctor_command=$(printf '%s doctor' "$(shell_escape "${DB_GATE_SCRIPT}")")
live_doctor_command=$(printf '%s doctor' "$(shell_escape "${LIVE_GATE_SCRIPT}")")
operator_doctor_command=$(printf '%s doctor' "$(shell_escape "${INDEXER_RUN_SCRIPT}")")
acceptance_run_command=$("${DB_GATE_SCRIPT}" command | flatten_output)
repo_run_command=$("${DB_GATE_SCRIPT}" command | flatten_output)
live_run_command=$("${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}" | flatten_output)
managed_live_run_command=$(printf '%s run-live-managed' "$(shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
operator_run_command=$("${INDEXER_RUN_SCRIPT}" command | flatten_output)
full_run_command=$(
  {
    "${DB_GATE_SCRIPT}" command
    "${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}"
  } | flatten_output
)
managed_full_run_command=$(printf '%s run-full-managed' "$(shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
EOF

  if [[ "${strict}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

render_command() {
  local selection="${1:-all}"

  case "${selection}" in
    acceptance)
      "${DB_GATE_SCRIPT}" command
      ;;
    repo)
      "${DB_GATE_SCRIPT}" command
      ;;
    live)
      "${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}"
      ;;
    live-managed)
      cat <<EOF
$(printf '%s run-live-managed' "$(shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
EOF
      ;;
    full)
      cat <<EOF
$("${DB_GATE_SCRIPT}" command)
$("${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}")
EOF
      ;;
    full-managed)
      cat <<EOF
$(printf '%s run-full-managed' "$(shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
EOF
      ;;
    all)
      cat <<EOF
$("${DB_GATE_SCRIPT}" command)
$("${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}")
EOF
      ;;
    *)
      echo "unknown command selection: ${selection}" >&2
      exit 1
      ;;
  esac
}

run_repo() {
  "${DB_GATE_SCRIPT}" run
}

run_acceptance() {
  run_repo
}

run_live() {
  case "${LIVE_SELECTION}" in
    route)
      "${LIVE_GATE_SCRIPT}" run-route
      ;;
    settlement)
      "${LIVE_GATE_SCRIPT}" run-settlement
      ;;
    all)
      "${LIVE_GATE_SCRIPT}" run-all
      ;;
  esac
}

run_full() {
  run_acceptance
  run_live
}

run_full_managed() {
  run_acceptance
  run_live_managed
}

run_all() {
  run_full
}

case "${mode}" in
  doctor)
    doctor
    ;;
  command)
    render_command "${2:-all}"
    ;;
  run-acceptance)
    run_acceptance
    ;;
  run-repo)
    run_repo
    ;;
  run-live)
    run_live
    ;;
  run-live-managed)
    run_live_managed
    ;;
  run-full)
    run_full
    ;;
  run-full-managed)
    run_full_managed
    ;;
  run-all)
    run_all
    ;;
  *)
    echo "unknown mode: ${mode}" >&2
    usage >&2
    exit 1
    ;;
esac
