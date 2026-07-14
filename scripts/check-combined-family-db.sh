#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/check-combined-family-db.sh doctor [--strict]
  scripts/check-combined-family-db.sh list
  scripts/check-combined-family-db.sh db-command
  scripts/check-combined-family-db.sh command
  scripts/check-combined-family-db.sh run

Modes:
  doctor     Report whether the local DB-backed combined-family regression environment is ready.
             With `--strict`, exits non-zero when readiness is false.
  list       Print the exact shared-family Phase 3 regression test names.
  db-command Print the exact command to start the local Postgres dependency for this gate.
  command    Print the exact command that `run` will execute.
  run        Execute the strict DB-backed shared-family regression gate.

Environment:
  DATABASE_URL           Default: postgres://postgres:mypassword@localhost:5431/tycho_indexer_0
  TYCHO_COMBINED_FAMILY_DB_NAME
                         Optional fixed isolated database name for this gate.
  TYCHO_COMBINED_FAMILY_DB_TEST_MANIFEST
                         Optional override manifest path for the DB-backed gate test list.
  TYCHO_COMBINED_FAMILY_SKIP_DB_DOCTOR
                         Default: 0
                         When set to 1, `run` skips the initial readiness probe and relies on the
                         subsequent `psql` setup + test execution as the authoritative check.
  TYCHO_REQUIRE_TEST_DB  Forced to 1 during `run`

Notes:
  - Any reachable Postgres can satisfy this gate via `DATABASE_URL`; the checked-in Docker
    compose path is only the default local bootstrap path.
  - `run` executes against an isolated temporary database derived from `DATABASE_URL`, so a
    dirty local dev database does not invalidate the shared-family regression gate.
  - `run` executes the focused Phase 3 close-out tests, not the entire serial_db suite.
  - The selected tests cover fixture-backed history-slice replay, restart resume, and
    reconnect / restart behavior after dynamic component admission.
  - `db-command` uses `TYCHO_IMAGE=alpine` because the repo docker compose file also declares
    a `tycho-indexer` service and Compose requires that image variable to be non-empty even when
    only the `db` service is started.
EOF
}

shell_escape() {
  local arg="$1"
  if [[ "${arg}" =~ ^[A-Za-z0-9_./:+=-]+$ ]]; then
    printf '%s' "${arg}"
    return
  fi
  printf "'%s'" "${arg//\'/\'\"\'\"\'}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MODE="${1:-}"
STRICT_DOCTOR="false"
TEST_MANIFEST="${TYCHO_COMBINED_FAMILY_DB_TEST_MANIFEST:-${REPO_ROOT}/crates/tycho-indexer/tests/combined_family_db_gate.tests}"
DOCKER_COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yaml"

if [[ -z "${MODE}" || "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" == "doctor" && "${2:-}" == "--strict" ]]; then
  STRICT_DOCTOR="true"
fi

BASE_DATABASE_URL_VALUE="${DATABASE_URL:-postgres://postgres:mypassword@localhost:5431/tycho_indexer_0}"

postgres_url_db_name() {
  local url="$1"
  local tail="${url##*/}"
  printf '%s' "${tail%%\?*}"
}

postgres_url_with_db_name() {
  local url="$1"
  local db_name="$2"
  local prefix="${url%/*}"
  local tail="${url##*/}"
  local query=""
  if [[ "${tail}" == *\?* ]]; then
    query="?${tail#*\?}"
  fi
  printf '%s/%s%s' "${prefix}" "${db_name}" "${query}"
}

sanitize_db_name() {
  local value="$1"
  value="$(printf '%s' "${value}" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_' '_')"
  value="${value##_}"
  value="${value%%_}"
  if [[ -z "${value}" ]]; then
    value="combined_family_gate"
  fi
  printf '%.63s' "${value}"
}

derive_run_database_name() {
  if [[ -n "${TYCHO_COMBINED_FAMILY_DB_NAME:-}" ]]; then
    sanitize_db_name "${TYCHO_COMBINED_FAMILY_DB_NAME}"
    return
  fi

  local base_name
  base_name="$(postgres_url_db_name "${BASE_DATABASE_URL_VALUE}")"
  local user_part="${USER:-codex}"
  local raw="${base_name}_combined_family_gate_${user_part}_$$"
  sanitize_db_name "${raw}"
}

RUN_DATABASE_NAME="$(derive_run_database_name)"
RUN_DATABASE_URL="$(postgres_url_with_db_name "${BASE_DATABASE_URL_VALUE}" "${RUN_DATABASE_NAME}")"
MAINTENANCE_DATABASE_URL="$(postgres_url_with_db_name "${BASE_DATABASE_URL_VALUE}" "postgres")"
SKIP_DB_DOCTOR="${TYCHO_COMBINED_FAMILY_SKIP_DB_DOCTOR:-0}"

render_db_start_command() {
  cat <<EOF
cd $(shell_escape "${REPO_ROOT}")
TYCHO_IMAGE=alpine docker compose -f $(shell_escape "${DOCKER_COMPOSE_FILE}") up -d db
EOF
}

load_tests() {
  if [[ ! -f "${TEST_MANIFEST}" ]]; then
    echo "missing combined-family DB gate manifest: ${TEST_MANIFEST}" >&2
    exit 1
  fi

  TESTS=()
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ -z "${line}" || "${line}" =~ ^# ]]; then
      continue
    fi
    TESTS+=("${line}")
  done < "${TEST_MANIFEST}"
}

load_tests

render_test_binary_resolve_command() {
  cat <<'EOF'
TEST_BINARY="$(cargo test -p tycho-indexer --bin tycho-indexer --no-run 2>&1 | sed -n 's/^  Executable .* (\(.*\))$/\1/p' | tail -n 1)"
if [[ -z "${TEST_BINARY}" || ! -x "${TEST_BINARY}" ]]; then
  echo "failed to resolve tycho-indexer test binary path" >&2
  exit 1
fi
EOF
}

resolve_test_binary_path() {
  TEST_BINARY="$(cargo test -p tycho-indexer --bin tycho-indexer --no-run 2>&1 | sed -n 's/^  Executable .* (\(.*\))$/\1/p' | tail -n 1)"
  if [[ -z "${TEST_BINARY}" || ! -x "${TEST_BINARY}" ]]; then
    echo "failed to resolve tycho-indexer test binary path" >&2
    exit 1
  fi
}

doctor() {
  local ready="true"
  local db_state="reachable"
  local docker_cli="available"
  local docker_daemon="unknown"

  if ! command -v docker >/dev/null 2>&1; then
    docker_cli="missing"
    docker_daemon="unavailable"
  elif docker info >/dev/null 2>&1; then
    docker_daemon="reachable"
  else
    docker_daemon="unreachable"
  fi

  if ! PGPASSWORD="${PGPASSWORD:-}" psql "${MAINTENANCE_DATABASE_URL}" -At -F $'\t' -c "select 1;" >/dev/null 2>&1; then
    ready="false"
    db_state="unreachable"
  fi

  cat <<EOF
ready=${ready}
base_database_url=${BASE_DATABASE_URL_VALUE}
run_database_name=${RUN_DATABASE_NAME}
run_database_url=${RUN_DATABASE_URL}
maintenance_database_url=${MAINTENANCE_DATABASE_URL}
database_state=${db_state}
docker_cli=${docker_cli}
docker_daemon=${docker_daemon}
docker_compose_file=${DOCKER_COMPOSE_FILE}
database_start_command=$(render_db_start_command | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g; s/ $//')
test_count=${#TESTS[@]}
test_manifest=${TEST_MANIFEST}
EOF

  if [[ "${STRICT_DOCTOR}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

list_tests() {
  printf '%s\n' "${TESTS[@]}"
}

render_run_command() {
  cat <<EOF
cd $(shell_escape "${REPO_ROOT}")
export DATABASE_URL=$(shell_escape "${RUN_DATABASE_URL}")
export TYCHO_REQUIRE_TEST_DB=1
psql $(shell_escape "${MAINTENANCE_DATABASE_URL}") -v ON_ERROR_STOP=1 \\
  -c "DROP DATABASE IF EXISTS \\"${RUN_DATABASE_NAME}\\";" \\
  -c "CREATE DATABASE \\"${RUN_DATABASE_NAME}\\";"
trap 'psql $(shell_escape "${MAINTENANCE_DATABASE_URL}") -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \\"${RUN_DATABASE_NAME}\\";" >/dev/null' EXIT
$(render_test_binary_resolve_command)
for test_name in \\
$(printf '  %s \\\n' "${TESTS[@]}")
; do
  "\${TEST_BINARY}" "\${test_name}" --exact --nocapture
done
EOF
}

run_tests() {
  if [[ "${SKIP_DB_DOCTOR}" != "1" ]]; then
    local previous_strict_doctor="${STRICT_DOCTOR}"
    STRICT_DOCTOR="true"
    doctor
    STRICT_DOCTOR="${previous_strict_doctor}"
  fi
  cd "${REPO_ROOT}"
  export DATABASE_URL="${RUN_DATABASE_URL}"
  export TYCHO_REQUIRE_TEST_DB=1
  PGPASSWORD="${PGPASSWORD:-}" psql "${MAINTENANCE_DATABASE_URL}" -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS \"${RUN_DATABASE_NAME}\";" \
    -c "CREATE DATABASE \"${RUN_DATABASE_NAME}\";"
  cleanup_db() {
    PGPASSWORD="${PGPASSWORD:-}" psql "${MAINTENANCE_DATABASE_URL}" -v ON_ERROR_STOP=1 \
      -c "DROP DATABASE IF EXISTS \"${RUN_DATABASE_NAME}\";" >/dev/null
  }
  trap cleanup_db EXIT

  resolve_test_binary_path

  for test_name in "${TESTS[@]}"; do
    "${TEST_BINARY}" "${test_name}" --exact --nocapture
  done
}

case "${MODE}" in
  doctor)
    doctor
    ;;
  list)
    list_tests
    ;;
  db-command)
    render_db_start_command
    ;;
  command)
    render_run_command
    ;;
  run)
    run_tests
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 1
    ;;
esac
