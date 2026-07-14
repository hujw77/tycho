#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/check-combined-family-extensibility.sh doctor [--strict]
  scripts/check-combined-family-extensibility.sh list
  scripts/check-combined-family-extensibility.sh command
  scripts/check-combined-family-extensibility.sh run

Modes:
  doctor   Report whether the repo-local combined-family extensibility contract environment is ready.
           With `--strict`, exits non-zero when readiness is false.
  list     Print the exact manifest-backed extensibility contract entries.
  command  Print the exact command that `run` will execute.
  run      Execute the manifest-backed extensibility contract gate.

Environment:
  TYCHO_COMBINED_FAMILY_EXTENSIBILITY_TEST_MANIFEST
           Optional override manifest path for the extensibility contract test list.
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
TEST_MANIFEST="${TYCHO_COMBINED_FAMILY_EXTENSIBILITY_TEST_MANIFEST:-${REPO_ROOT}/crates/tycho-indexer/tests/combined_family_extensibility_contract.tests}"

if [[ -z "${MODE}" || "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" == "doctor" && "${2:-}" == "--strict" ]]; then
  STRICT_DOCTOR="true"
fi

load_entries() {
  if [[ ! -f "${TEST_MANIFEST}" ]]; then
    echo "missing combined-family extensibility contract manifest: ${TEST_MANIFEST}" >&2
    exit 1
  fi

  ENTRY_LINES=()
  ENTRY_TESTS=()
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ -z "${line}" || "${line}" =~ ^# ]]; then
      continue
    fi

    local file_path
    local test_name
    local extra
    read -r file_path test_name extra <<<"${line}"
    if [[ -n "${extra:-}" || -z "${file_path:-}" || -z "${test_name:-}" ]]; then
      echo "invalid combined-family extensibility contract manifest entry: ${line}" >&2
      exit 1
    fi
    ENTRY_LINES+=("${line}")
    ENTRY_TESTS+=("${test_name}")
  done < "${TEST_MANIFEST}"
}

load_entries

render_test_binary_resolve_command() {
  cat <<'EOF'
mapfile -t TEST_BINARIES < <(
  cargo test -p tycho-indexer --no-run 2>&1 |
    sed -n 's/^  Executable .* (\(.*\))$/\1/p'
)
if [[ "${#TEST_BINARIES[@]}" -eq 0 ]]; then
  echo "failed to resolve tycho-indexer test binaries" >&2
  exit 1
fi
EOF
}

resolve_test_binary_path() {
  mapfile -t TEST_BINARIES < <(
    cargo test -p tycho-indexer --no-run 2>&1 |
      sed -n 's/^  Executable .* (\(.*\))$/\1/p'
  )
  if [[ "${#TEST_BINARIES[@]}" -eq 0 ]]; then
    echo "failed to resolve tycho-indexer test binaries" >&2
    exit 1
  fi
}

render_test_binary_index_command() {
  cat <<'EOF'
declare -A ENTRY_EXPECTED=()
declare -A TEST_BINARY_BY_ENTRY=()
declare -A TEST_FULL_NAME_BY_ENTRY=()
LIST_OUTPUT_FILE="$(mktemp)"
trap 'rm -f "${LIST_OUTPUT_FILE}"' EXIT
for test_name in "${ENTRY_TESTS[@]}"; do
  ENTRY_EXPECTED["${test_name}"]=1
done
for test_binary in "${TEST_BINARIES[@]}"; do
  if ! "${test_binary}" --list >"${LIST_OUTPUT_FILE}" 2>/dev/null; then
    continue
  fi
  while IFS= read -r listed_line; do
    full_name="${listed_line%:*}"
    short_name="${full_name##*::}"
    if [[ -z "${full_name}" || -z "${short_name}" ]]; then
      continue
    fi
    if [[ -n "${ENTRY_EXPECTED[${short_name}]:-}" && -z "${TEST_BINARY_BY_ENTRY[${short_name}]:-}" ]]; then
      TEST_BINARY_BY_ENTRY["${short_name}"]="${test_binary}"
      TEST_FULL_NAME_BY_ENTRY["${short_name}"]="${full_name}"
    fi
  done < "${LIST_OUTPUT_FILE}"
done
for test_name in "${ENTRY_TESTS[@]}"; do
  if [[ -z "${TEST_BINARY_BY_ENTRY[${test_name}]:-}" || -z "${TEST_FULL_NAME_BY_ENTRY[${test_name}]:-}" ]]; then
    echo "failed to resolve executable for extensibility test ${test_name}" >&2
    exit 1
  fi
done
EOF
}

build_test_binary_index() {
  declare -gA ENTRY_EXPECTED=()
  declare -gA TEST_BINARY_BY_ENTRY=()
  declare -gA TEST_FULL_NAME_BY_ENTRY=()
  local list_output_file
  local test_binary
  local listed_line
  local full_name
  local short_name

  list_output_file="$(mktemp)"
  trap 'rm -f "${list_output_file}"' EXIT

  for test_name in "${ENTRY_TESTS[@]}"; do
    ENTRY_EXPECTED["${test_name}"]=1
  done
  for test_binary in "${TEST_BINARIES[@]}"; do
    if ! "${test_binary}" --list >"${list_output_file}" 2>/dev/null; then
      continue
    fi
    while IFS= read -r listed_line; do
      full_name="${listed_line%:*}"
      short_name="${full_name##*::}"
      if [[ -z "${full_name}" || -z "${short_name}" ]]; then
        continue
      fi
      if [[ -n "${ENTRY_EXPECTED[${short_name}]:-}" && -z "${TEST_BINARY_BY_ENTRY[${short_name}]:-}" ]]; then
        TEST_BINARY_BY_ENTRY["${short_name}"]="${test_binary}"
        TEST_FULL_NAME_BY_ENTRY["${short_name}"]="${full_name}"
      fi
    done < "${list_output_file}"
  done
  rm -f "${list_output_file}"
  trap - EXIT
  for test_name in "${ENTRY_TESTS[@]}"; do
    if [[ -z "${TEST_BINARY_BY_ENTRY[${test_name}]:-}" || -z "${TEST_FULL_NAME_BY_ENTRY[${test_name}]:-}" ]]; then
      echo "failed to resolve executable for extensibility test ${test_name}" >&2
      exit 1
    fi
  done
}

doctor() {
  local ready="true"
  local cargo_state="available"

  if ! command -v cargo >/dev/null 2>&1; then
    ready="false"
    cargo_state="missing"
  fi

  cat <<EOF
ready=${ready}
repo_root=${REPO_ROOT}
test_manifest=${TEST_MANIFEST}
test_count=${#ENTRY_TESTS[@]}
cargo_state=${cargo_state}
EOF

  if [[ "${STRICT_DOCTOR}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

list_entries() {
  printf '%s\n' "${ENTRY_LINES[@]}"
}

render_run_command() {
  cat <<EOF
cd $(shell_escape "${REPO_ROOT}")
$(render_test_binary_resolve_command)
ENTRY_TESTS=(
$(printf '  %s\n' "${ENTRY_TESTS[@]}")
)
$(render_test_binary_index_command)
for test_name in \\
$(printf '  %s \\\n' "${ENTRY_TESTS[@]}")
; do
  "\${TEST_BINARY_BY_ENTRY[\${test_name}]}" "\${TEST_FULL_NAME_BY_ENTRY[\${test_name}]}" --exact --nocapture
done
EOF
}

run_tests() {
  local previous_strict_doctor="${STRICT_DOCTOR}"
  STRICT_DOCTOR="true"
  doctor
  STRICT_DOCTOR="${previous_strict_doctor}"

  cd "${REPO_ROOT}"
  resolve_test_binary_path
  build_test_binary_index
  for test_name in "${ENTRY_TESTS[@]}"; do
    "${TEST_BINARY_BY_ENTRY[${test_name}]}" "${TEST_FULL_NAME_BY_ENTRY[${test_name}]}" --exact --nocapture
  done
}

case "${MODE}" in
  doctor)
    doctor
    ;;
  list)
    list_entries
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
