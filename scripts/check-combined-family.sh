#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/combined-family-common.sh"

usage() {
  cat <<'EOF'
Usage:
  scripts/check-combined-family.sh doctor [--strict]
  scripts/check-combined-family.sh command [acceptance|acceptance-managed|repo|live|live-managed|full|full-managed|all]
  scripts/check-combined-family.sh run-acceptance
  scripts/check-combined-family.sh run-acceptance-managed
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
            `acceptance` runs the repo-local extensibility contract gate, the repo-local Fynd
            replay contract gate, and the DB-backed shared-runtime acceptance gate.
            `acceptance-managed` runs `acceptance` first, then `live-managed`.
            `repo` runs the repo-local DB-backed regression gate.
            `live` runs the live combined-family Fynd E2E gate.
            `live-managed` starts the combined-family indexer, waits for health, then runs the
            live Fynd E2E gate.
            `full` runs `acceptance` first, then `live`.
            `full-managed` is a compatibility alias for `acceptance-managed`.
            `all` runs `repo` first, then `live`.
  run-acceptance Execute the repo-local extensibility contract gate, the repo-local Fynd replay
                 contract gate, then the DB-backed shared-runtime acceptance gate.
  run-acceptance-managed Execute the repo-local extensibility contract gate, the repo-local Fynd replay
                         contract gate, then the DB-backed shared-runtime acceptance gate, then the
                         managed live Fynd E2E gate.
  run-repo  Execute the repo-local DB-backed regression gate.
  run-live  Execute the live combined-family Fynd E2E gate.
  run-live-managed Start the combined-family indexer, then execute the live Fynd E2E gate.
  run-full  Execute the repo-local extensibility contract gate, the repo-local Fynd replay
            contract gate, then the DB-backed shared-runtime acceptance gate, then the live
            Fynd E2E gate.
  run-full-managed Compatibility alias for `run-acceptance-managed`.
  run-all   Execute the repo-local DB gate, then the live Fynd E2E gate.

Environment:
  DATABASE_URL           Forwarded to `check-combined-family-db.sh`
  TYCHO_COMBINED_FAMILY_DB_TEST_MANIFEST
                         Forwarded to `check-combined-family-db.sh`
  TYCHO_COMBINED_FAMILY_EXTENSIBILITY_TEST_MANIFEST
                         Forwarded to `check-combined-family-extensibility.sh`
  TYCHO_COMBINED_FAMILY_FYND_REPLAY_TEST_MANIFEST
                         Forwarded to `check-combined-family-fynd-replay.sh`
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
  FYND_E2E_HEALTH_TIMEOUT_SECS
                         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_TRADED_N_DAYS_AGO
                         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_MIN_TOKEN_QUALITY
                         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_CONNECTOR_TOKENS
                         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  TYCHO_COMBINED_FAMILY_LIVE_TEST_MANIFEST
                         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_ROUTE_TEST    Forwarded to `check-combined-family-fynd-live-e2e.sh`
  FYND_E2E_SETTLEMENT_TEST
                         Forwarded to `check-combined-family-fynd-live-e2e.sh`
  TYCHO_COMBINED_FAMILY_MANAGED_HEALTH_TIMEOUT_SECS
                         Default: FYND_E2E_HEALTH_TIMEOUT_SECS or 300
                         Used by `run-live-managed`, `run-acceptance-managed`, and `run-full-managed`
  TYCHO_COMBINED_FAMILY_MANAGED_INDEXER_LOG
                         Optional fixed log file for the managed indexer process
EOF
}

DB_GATE_SCRIPT="${SCRIPT_DIR}/check-combined-family-db.sh"
EXTENSIBILITY_GATE_SCRIPT="${SCRIPT_DIR}/check-combined-family-extensibility.sh"
FYND_REPLAY_GATE_SCRIPT="${SCRIPT_DIR}/check-combined-family-fynd-replay.sh"
LIVE_GATE_SCRIPT="${SCRIPT_DIR}/check-combined-family-fynd-live-e2e.sh"
INDEXER_RUN_SCRIPT="${SCRIPT_DIR}/run-combined-family-indexer.sh"

mode="${1:-}"
strict="false"
LIVE_SELECTION="${TYCHO_COMBINED_FAMILY_LIVE_SELECTION:-all}"
MANAGED_HEALTH_TIMEOUT_SECS="${TYCHO_COMBINED_FAMILY_MANAGED_HEALTH_TIMEOUT_SECS:-${FYND_E2E_HEALTH_TIMEOUT_SECS:-300}}"

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

is_live_tycho_ready_for_combined_family() {
  local live_output
  live_output="$(run_doctor_capture "${LIVE_GATE_SCRIPT}")"
  local live_ready
  live_ready="$(readiness_from_output "${live_output}")"
  [[ "${live_ready}" == "true" ]]
}

wait_for_live_tycho_readiness() {
  local timeout_secs="$1"
  local managed_pid="${2:-}"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    if is_live_tycho_ready_for_combined_family; then
      return 0
    fi

    if [[ -n "${managed_pid}" ]] && ! kill -0 "${managed_pid}" >/dev/null 2>&1; then
      return 2
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

  mktemp "${TMPDIR:-/tmp}/tycho-combined-family-indexer.XXXXXX"
}

cleanup_managed_indexer() {
  local managed_pid="${1:-}"
  if [[ -n "${managed_pid}" ]] && kill -0 "${managed_pid}" >/dev/null 2>&1; then
    kill "${managed_pid}" >/dev/null 2>&1 || true
    wait "${managed_pid}" >/dev/null 2>&1 || true
  fi
}

emit_managed_indexer_failure_hint() {
  local log_path="$1"
  if [[ ! -f "${log_path}" ]]; then
    return
  fi

  if grep -Fq 'invalid JWT token' "${log_path}"; then
    echo "managed indexer failure hint: StreamingFast rejected SUBSTREAMS_API_TOKEN (invalid JWT token)" >&2
    return
  fi

  if grep -Fq 'status: Unauthenticated' "${log_path}"; then
    echo "managed indexer failure hint: upstream Substreams authentication failed; verify SUBSTREAMS_API_TOKEN" >&2
    return
  fi
}

run_live_managed() {
  local log_path
  local indexer_pid=""

  if is_live_tycho_ready_for_combined_family; then
    run_live
    return
  fi

  if is_live_tycho_healthy; then
    echo "existing Tycho instance at ${FYND_E2E_TYCHO_URL:-127.0.0.1:4242} is healthy but not ready for combined-family live validation" >&2
    echo "expected both uniswap_v2 and uniswap_v3 protocol components to be queryable before running managed live validation" >&2
    return 1
  fi

  "${INDEXER_RUN_SCRIPT}" doctor --strict >/dev/null

  log_path="$(managed_indexer_log_path)"

  cd "${SCRIPT_DIR}"
  "${INDEXER_RUN_SCRIPT}" run >"${log_path}" 2>&1 &
  indexer_pid=$!
  export TYCHO_COMBINED_FAMILY_MANAGED_PID="${indexer_pid}"

  trap 'cleanup_managed_indexer "${TYCHO_COMBINED_FAMILY_MANAGED_PID:-}"' EXIT

  local wait_status=0
  if ! wait_for_live_tycho_readiness "${MANAGED_HEALTH_TIMEOUT_SECS}" "${indexer_pid}"; then
    wait_status=$?
  fi
  if [[ "${wait_status}" -ne 0 ]]; then
    if [[ "${wait_status}" -eq 2 ]]; then
      echo "managed combined-family indexer exited before becoming ready" >&2
    else
      echo "managed combined-family indexer did not become ready within ${MANAGED_HEALTH_TIMEOUT_SECS}s" >&2
    fi
    echo "managed indexer log: ${log_path}" >&2
    emit_managed_indexer_failure_hint "${log_path}"
    tail -n 40 "${log_path}" >&2 || true
    return 1
  fi

  if ! kill -0 "${indexer_pid}" >/dev/null 2>&1; then
    echo "managed combined-family indexer exited immediately after reporting readiness" >&2
    echo "managed indexer log: ${log_path}" >&2
    emit_managed_indexer_failure_hint "${log_path}"
    tail -n 40 "${log_path}" >&2 || true
    return 1
  fi

  if ! run_live; then
    if ! kill -0 "${indexer_pid}" >/dev/null 2>&1; then
      echo "managed combined-family indexer exited during live validation" >&2
      echo "managed indexer log: ${log_path}" >&2
      emit_managed_indexer_failure_hint "${log_path}"
      tail -n 40 "${log_path}" >&2 || true
    fi
    return 1
  fi
}

readiness_from_output() {
  local rendered="$1"
  printf '%s\n' "${rendered}" | awk -F= '$1 == "ready" { print $2; exit }'
}

field_from_output() {
  local rendered="$1"
  local field_name="$2"
  printf '%s\n' "${rendered}" | awk -F= -v field_name="${field_name}" '$1 == field_name { print $2; exit }'
}

run_doctor_capture() {
  local target_script="$1"
  local output_file
  output_file="$(mktemp "${TMPDIR:-/tmp}/combined-family-doctor.XXXXXX")"
  if ! "${target_script}" doctor >"${output_file}"; then
    rm -f "${output_file}"
    echo "failed to execute doctor mode for ${target_script}" >&2
    exit 1
  fi
  cat "${output_file}"
  rm -f "${output_file}"
}

doctor() {
  local repo_output
  local extensibility_output
  local live_output
  local fynd_replay_output
  local operator_output
  local extensibility_ready
  local fynd_replay_ready
  local repo_ready
  local live_ready
  local operator_ready
  local live_fynd_repo_exists
  local live_fynd_test_exists
  local live_test_mapping_ready
  local live_curl_available
  local extensibility_test_manifest
  local extensibility_test_count
  local fynd_replay_test_manifest
  local fynd_replay_test_count
  local repo_test_manifest
  local repo_test_count
  local live_test_manifest
  local live_test_count
  local managed_live_ready
  local acceptance_managed_ready
  local managed_full_ready
  local ready="true"

  extensibility_output="$(run_doctor_capture "${EXTENSIBILITY_GATE_SCRIPT}")"
  fynd_replay_output="$(run_doctor_capture "${FYND_REPLAY_GATE_SCRIPT}")"
  repo_output="$(run_doctor_capture "${DB_GATE_SCRIPT}")"
  live_output="$(run_doctor_capture "${LIVE_GATE_SCRIPT}")"
  operator_output="$(run_doctor_capture "${INDEXER_RUN_SCRIPT}")"
  extensibility_ready="$(readiness_from_output "${extensibility_output}")"
  fynd_replay_ready="$(readiness_from_output "${fynd_replay_output}")"
  repo_ready="$(readiness_from_output "${repo_output}")"
  live_ready="$(readiness_from_output "${live_output}")"
  operator_ready="$(readiness_from_output "${operator_output}")"
  live_fynd_repo_exists="$(field_from_output "${live_output}" "fynd_repo_exists")"
  live_fynd_test_exists="$(field_from_output "${live_output}" "fynd_test_exists")"
  live_test_mapping_ready="$(field_from_output "${live_output}" "live_test_mapping_ready")"
  live_curl_available="$(field_from_output "${live_output}" "curl_available")"
  extensibility_test_manifest="$(field_from_output "${extensibility_output}" "test_manifest")"
  extensibility_test_count="$(field_from_output "${extensibility_output}" "test_count")"
  fynd_replay_test_manifest="$(field_from_output "${fynd_replay_output}" "test_manifest")"
  fynd_replay_test_count="$(field_from_output "${fynd_replay_output}" "test_count")"
  repo_test_manifest="$(field_from_output "${repo_output}" "test_manifest")"
  repo_test_count="$(field_from_output "${repo_output}" "test_count")"
  live_test_manifest="$(field_from_output "${live_output}" "test_manifest")"
  live_test_count="$(field_from_output "${live_output}" "test_count")"

  if [[ "${extensibility_ready}" != "true" || "${fynd_replay_ready}" != "true" || "${repo_ready}" != "true" || "${live_ready}" != "true" ]]; then
    ready="false"
  fi

  managed_live_ready="false"
  if [[ "${operator_ready}" == "true" \
    && "${live_fynd_repo_exists}" == "true" \
    && "${live_fynd_test_exists}" == "true" \
    && "${live_test_mapping_ready}" == "true" \
    && "${live_curl_available}" == "true" ]]; then
    managed_live_ready="true"
  fi

  acceptance_managed_ready="false"
  if [[ "${extensibility_ready}" == "true" && "${fynd_replay_ready}" == "true" && "${repo_ready}" == "true" && "${managed_live_ready}" == "true" ]]; then
    acceptance_managed_ready="true"
  fi

  managed_full_ready="false"
  if [[ "${acceptance_managed_ready}" == "true" ]]; then
    managed_full_ready="true"
  fi

  cat <<EOF
ready=${ready}
acceptance_ready=$([[ "${extensibility_ready}" == "true" && "${fynd_replay_ready}" == "true" && "${repo_ready}" == "true" ]] && printf 'true' || printf 'false')
full_ready=${ready}
extensibility_ready=${extensibility_ready}
fynd_replay_ready=${fynd_replay_ready}
repo_ready=${repo_ready}
live_ready=${live_ready}
operator_ready=${operator_ready}
managed_live_ready=${managed_live_ready}
acceptance_managed_ready=${acceptance_managed_ready}
managed_full_ready=${managed_full_ready}
live_fynd_repo_exists=${live_fynd_repo_exists}
live_fynd_test_exists=${live_fynd_test_exists}
live_test_mapping_ready=${live_test_mapping_ready}
live_curl_available=${live_curl_available}
extensibility_test_manifest=${extensibility_test_manifest}
extensibility_test_count=${extensibility_test_count}
fynd_replay_test_manifest=${fynd_replay_test_manifest}
fynd_replay_test_count=${fynd_replay_test_count}
repo_test_manifest=${repo_test_manifest}
repo_test_count=${repo_test_count}
live_test_manifest=${live_test_manifest}
live_test_count=${live_test_count}
extensibility_gate_script=${EXTENSIBILITY_GATE_SCRIPT}
fynd_replay_gate_script=${FYND_REPLAY_GATE_SCRIPT}
db_gate_script=${DB_GATE_SCRIPT}
live_gate_script=${LIVE_GATE_SCRIPT}
indexer_run_script=${INDEXER_RUN_SCRIPT}
extensibility_doctor_command=$(printf '%s doctor' "$(tycho_combined_family_shell_escape "${EXTENSIBILITY_GATE_SCRIPT}")")
fynd_replay_doctor_command=$(printf '%s doctor' "$(tycho_combined_family_shell_escape "${FYND_REPLAY_GATE_SCRIPT}")")
repo_doctor_command=$(printf '%s doctor' "$(tycho_combined_family_shell_escape "${DB_GATE_SCRIPT}")")
live_doctor_command=$(printf '%s doctor' "$(tycho_combined_family_shell_escape "${LIVE_GATE_SCRIPT}")")
operator_doctor_command=$(printf '%s doctor' "$(tycho_combined_family_shell_escape "${INDEXER_RUN_SCRIPT}")")
acceptance_run_command=$(
  {
    "${EXTENSIBILITY_GATE_SCRIPT}" command
    "${FYND_REPLAY_GATE_SCRIPT}" command
    "${DB_GATE_SCRIPT}" command
  } | flatten_output
)
repo_run_command=$("${DB_GATE_SCRIPT}" command | flatten_output)
live_run_command=$("${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}" | flatten_output)
managed_live_run_command=$(printf '%s run-live-managed' "$(tycho_combined_family_shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
acceptance_managed_run_command=$(printf '%s run-acceptance-managed' "$(tycho_combined_family_shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
operator_run_command=$("${INDEXER_RUN_SCRIPT}" command | flatten_output)
full_run_command=$(
  {
    "${EXTENSIBILITY_GATE_SCRIPT}" command
    "${FYND_REPLAY_GATE_SCRIPT}" command
    "${DB_GATE_SCRIPT}" command
    "${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}"
  } | flatten_output
)
managed_full_run_command=$(printf '%s run-full-managed' "$(tycho_combined_family_shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
EOF

  if [[ "${strict}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

render_command() {
  local selection="${1:-all}"

  case "${selection}" in
    acceptance)
      cat <<EOF
$("${EXTENSIBILITY_GATE_SCRIPT}" command)
$("${FYND_REPLAY_GATE_SCRIPT}" command)
$("${DB_GATE_SCRIPT}" command)
EOF
      ;;
    acceptance-managed)
      cat <<EOF
$(printf '%s run-acceptance-managed' "$(tycho_combined_family_shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
EOF
      ;;
    repo)
      "${DB_GATE_SCRIPT}" command
      ;;
    live)
      "${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}"
      ;;
    live-managed)
      cat <<EOF
$(printf '%s run-live-managed' "$(tycho_combined_family_shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
EOF
      ;;
    full)
      cat <<EOF
$("${EXTENSIBILITY_GATE_SCRIPT}" command)
$("${FYND_REPLAY_GATE_SCRIPT}" command)
$("${DB_GATE_SCRIPT}" command)
$("${LIVE_GATE_SCRIPT}" command "${LIVE_SELECTION}")
EOF
      ;;
    full-managed)
      cat <<EOF
$(printf '%s run-full-managed' "$(tycho_combined_family_shell_escape "${SCRIPT_DIR}/check-combined-family.sh")")
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
  TYCHO_COMBINED_FAMILY_SKIP_DB_DOCTOR=1 "${DB_GATE_SCRIPT}" run
}

run_acceptance() {
  "${EXTENSIBILITY_GATE_SCRIPT}" run
  "${FYND_REPLAY_GATE_SCRIPT}" run
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

run_acceptance_managed() {
  run_acceptance
  run_live_managed
}

run_all() {
  run_repo
  run_live
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
  run-acceptance-managed)
    run_acceptance_managed
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
    run_acceptance_managed
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
