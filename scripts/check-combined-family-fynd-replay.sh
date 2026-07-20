#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/combined-family-common.sh"

usage() {
  cat <<'EOF'
Usage:
  scripts/check-combined-family-fynd-replay.sh doctor [--strict]
  scripts/check-combined-family-fynd-replay.sh list
  scripts/check-combined-family-fynd-replay.sh command
  scripts/check-combined-family-fynd-replay.sh run

Modes:
  doctor   Report whether the repo-local combined-family Fynd replay contract environment is ready.
           With `--strict`, exits non-zero when readiness is false.
  list     Print the exact manifest-backed Fynd replay contract entries.
  command  Print the exact command that `run` will execute.
  run      Execute the manifest-backed Fynd replay contract gate.

Environment:
  FYND_REPO_ROOT
           Default: sibling ../fynd
  TYCHO_COMBINED_FAMILY_FYND_REPLAY_TEST_MANIFEST
           Optional override manifest path for the Fynd replay contract test list.
  FYND_REPLAY_CARGO_TARGET_DIR
           Optional cargo target directory override for this gate. Useful when the sibling
           Fynd workspace target directory is full and the replay gate should build in `/tmp`
           or another scratch location instead.
EOF
}

REPO_ROOT="${TYCHO_COMBINED_FAMILY_REPO_ROOT}"
DEFAULT_FYND_REPO_ROOT="${TYCHO_COMBINED_FAMILY_DEFAULT_FYND_REPO_ROOT}"
MODE="${1:-}"
STRICT_DOCTOR="false"
FYND_REPO_ROOT="${FYND_REPO_ROOT:-${DEFAULT_FYND_REPO_ROOT}}"
TEST_MANIFEST="${TYCHO_COMBINED_FAMILY_FYND_REPLAY_TEST_MANIFEST:-${REPO_ROOT}/crates/tycho-indexer/tests/combined_family_fynd_replay_gate.tests}"
FYND_REPLAY_CARGO_TARGET_DIR="${FYND_REPLAY_CARGO_TARGET_DIR:-${CARGO_TARGET_DIR:-}}"

if [[ -z "${MODE}" || "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" == "doctor" && "${2:-}" == "--strict" ]]; then
  STRICT_DOCTOR="true"
fi

load_entries() {
  if [[ ! -f "${TEST_MANIFEST}" ]]; then
    echo "missing combined-family Fynd replay gate manifest: ${TEST_MANIFEST}" >&2
    exit 1
  fi

  ENTRY_LINES=()
  ENTRY_PACKAGES=()
  ENTRY_TESTS=()
  declare -A SEEN_ENTRIES=()
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ -z "${line}" || "${line}" =~ ^# ]]; then
      continue
    fi

    local package_name
    local test_name
    local extra
    read -r package_name test_name extra <<<"${line}"
    if [[ -n "${extra:-}" || -z "${package_name:-}" || -z "${test_name:-}" ]]; then
      echo "invalid combined-family Fynd replay gate manifest entry: ${line}" >&2
      exit 1
    fi
    if [[ -n "${SEEN_ENTRIES[${package_name}:${test_name}]:-}" ]]; then
      echo "duplicate combined-family Fynd replay gate manifest entry: ${package_name} ${test_name}" >&2
      exit 1
    fi
    SEEN_ENTRIES["${package_name}:${test_name}"]=1
    ENTRY_LINES+=("${line}")
    ENTRY_PACKAGES+=("${package_name}")
    ENTRY_TESTS+=("${test_name}")
  done < "${TEST_MANIFEST}"
}

load_entries

render_test_binary_resolve_command() {
  cat <<'EOF'
declare -A TEST_BINARIES_BY_PACKAGE=()
for package_name in "${ENTRY_PACKAGES[@]}"; do
  if [[ -n "${TEST_BINARIES_BY_PACKAGE[${package_name}]:-}" ]]; then
    continue
  fi
  mapfile -t package_test_binaries < <(
    cargo test -p "${package_name}" --no-run 2>&1 |
      sed -n 's/^  Executable .* (\(.*\))$/\1/p'
  )
  if [[ "${#package_test_binaries[@]}" -eq 0 ]]; then
    echo "failed to resolve test binaries for Fynd replay package ${package_name}" >&2
    exit 1
  fi
  TEST_BINARIES_BY_PACKAGE["${package_name}"]="$(printf '%s\n' "${package_test_binaries[@]}")"
done
EOF
}

resolve_test_binary_paths() {
  declare -gA TEST_BINARIES_BY_PACKAGE=()
  local package_name
  local package_test_binaries
  for package_name in "${ENTRY_PACKAGES[@]}"; do
    if [[ -n "${TEST_BINARIES_BY_PACKAGE[${package_name}]:-}" ]]; then
      continue
    fi
    mapfile -t package_test_binaries < <(
      cargo test -p "${package_name}" --no-run 2>&1 |
        sed -n 's/^  Executable .* (\(.*\))$/\1/p'
    )
    if [[ "${#package_test_binaries[@]}" -eq 0 ]]; then
      echo "failed to resolve test binaries for Fynd replay package ${package_name}" >&2
      exit 1
    fi
    TEST_BINARIES_BY_PACKAGE["${package_name}"]="$(printf '%s\n' "${package_test_binaries[@]}")"
  done
}

render_test_binary_index_command() {
  cat <<'EOF'
declare -A TEST_BINARY_BY_ENTRY=()
declare -A TEST_FULL_NAME_BY_ENTRY=()
LIST_OUTPUT_FILE="$(mktemp)"
trap 'rm -f "${LIST_OUTPUT_FILE}"' EXIT
for entry_index in "${!ENTRY_PACKAGES[@]}"; do
  package_name="${ENTRY_PACKAGES[${entry_index}]}"
  test_name="${ENTRY_TESTS[${entry_index}]}"
  while IFS= read -r test_binary; do
    if [[ -z "${test_binary}" ]]; then
      continue
    fi
    if ! "${test_binary}" --list >"${LIST_OUTPUT_FILE}" 2>/dev/null; then
      continue
    fi
    while IFS= read -r listed_line; do
      full_name="${listed_line%:*}"
      short_name="${full_name##*::}"
      if [[ -z "${full_name}" || -z "${short_name}" ]]; then
        continue
      fi
      if [[ "${short_name}" == "${test_name}" ]]; then
        TEST_BINARY_BY_ENTRY["${package_name}:${test_name}"]="${test_binary}"
        TEST_FULL_NAME_BY_ENTRY["${package_name}:${test_name}"]="${full_name}"
        break 2
      fi
    done < "${LIST_OUTPUT_FILE}"
  done <<<"${TEST_BINARIES_BY_PACKAGE[${package_name}]}"
  if [[ -z "${TEST_BINARY_BY_ENTRY[${package_name}:${test_name}]:-}" || -z "${TEST_FULL_NAME_BY_ENTRY[${package_name}:${test_name}]:-}" ]]; then
    echo "failed to resolve executable for Fynd replay test ${package_name}:${test_name}" >&2
    exit 1
  fi
done
EOF
}

build_test_binary_index() {
  declare -gA TEST_BINARY_BY_ENTRY=()
  declare -gA TEST_FULL_NAME_BY_ENTRY=()
  local list_output_file
  local entry_index
  local package_name
  local test_name
  local test_binary
  local listed_line
  local full_name
  local short_name

  list_output_file="$(mktemp)"
  trap 'rm -f "${list_output_file}"' EXIT

  for entry_index in "${!ENTRY_PACKAGES[@]}"; do
    package_name="${ENTRY_PACKAGES[${entry_index}]}"
    test_name="${ENTRY_TESTS[${entry_index}]}"
    while IFS= read -r test_binary; do
      if [[ -z "${test_binary}" ]]; then
        continue
      fi
      if ! "${test_binary}" --list >"${list_output_file}" 2>/dev/null; then
        continue
      fi
      while IFS= read -r listed_line; do
        full_name="${listed_line%:*}"
        short_name="${full_name##*::}"
        if [[ -z "${full_name}" || -z "${short_name}" ]]; then
          continue
        fi
        if [[ "${short_name}" == "${test_name}" ]]; then
          TEST_BINARY_BY_ENTRY["${package_name}:${test_name}"]="${test_binary}"
          TEST_FULL_NAME_BY_ENTRY["${package_name}:${test_name}"]="${full_name}"
          break 2
        fi
      done < "${list_output_file}"
    done <<<"${TEST_BINARIES_BY_PACKAGE[${package_name}]}"
    if [[ -z "${TEST_BINARY_BY_ENTRY[${package_name}:${test_name}]:-}" || -z "${TEST_FULL_NAME_BY_ENTRY[${package_name}:${test_name}]:-}" ]]; then
      echo "failed to resolve executable for Fynd replay test ${package_name}:${test_name}" >&2
      exit 1
    fi
  done

  rm -f "${list_output_file}"
  trap - EXIT
}

doctor() {
  local ready="true"
  local cargo_state="available"
  local fynd_repo_exists="true"

  if ! command -v cargo >/dev/null 2>&1; then
    ready="false"
    cargo_state="missing"
  fi

  if [[ ! -d "${FYND_REPO_ROOT}" || ! -f "${FYND_REPO_ROOT}/Cargo.toml" ]]; then
    ready="false"
    fynd_repo_exists="false"
  fi

  cat <<EOF
ready=${ready}
fynd_repo_root=${FYND_REPO_ROOT}
fynd_repo_exists=${fynd_repo_exists}
test_manifest=${TEST_MANIFEST}
test_count=${#ENTRY_TESTS[@]}
cargo_state=${cargo_state}
cargo_target_dir=${FYND_REPLAY_CARGO_TARGET_DIR:-default}
EOF

  if [[ "${STRICT_DOCTOR}" == "true" && "${ready}" != "true" ]]; then
    return 1
  fi
}

list_entries() {
  printf '%s\n' "${ENTRY_LINES[@]}"
}

render_run_command() {
  local cargo_target_export=""
  if [[ -n "${FYND_REPLAY_CARGO_TARGET_DIR}" ]]; then
    cargo_target_export="export CARGO_TARGET_DIR=$(tycho_combined_family_shell_escape "${FYND_REPLAY_CARGO_TARGET_DIR}")"
  fi

cat <<EOF
cd $(tycho_combined_family_shell_escape "${FYND_REPO_ROOT}")
${cargo_target_export}
ENTRY_PACKAGES=(
$(printf '  %s\n' "${ENTRY_PACKAGES[@]}")
)
ENTRY_TESTS=(
$(printf '  %s\n' "${ENTRY_TESTS[@]}")
)
$(render_test_binary_resolve_command)
$(render_test_binary_index_command)
for entry_index in "\${!ENTRY_PACKAGES[@]}"; do
  package_name="\${ENTRY_PACKAGES[\${entry_index}]}"
  test_name="\${ENTRY_TESTS[\${entry_index}]}"
  entry_key="\${package_name}:\${test_name}"
  "\${TEST_BINARY_BY_ENTRY[\${entry_key}]}" "\${TEST_FULL_NAME_BY_ENTRY[\${entry_key}]}" --exact --nocapture
done
EOF
}

run_tests() {
  local previous_strict_doctor="${STRICT_DOCTOR}"
  STRICT_DOCTOR="true"
  doctor
  STRICT_DOCTOR="${previous_strict_doctor}"

  cd "${FYND_REPO_ROOT}"
  if [[ -n "${FYND_REPLAY_CARGO_TARGET_DIR}" ]]; then
    export CARGO_TARGET_DIR="${FYND_REPLAY_CARGO_TARGET_DIR}"
  fi
  resolve_test_binary_paths
  build_test_binary_index
  local entry_index
  local package_name
  local test_name
  local entry_key
  for entry_index in "${!ENTRY_PACKAGES[@]}"; do
    package_name="${ENTRY_PACKAGES[${entry_index}]}"
    test_name="${ENTRY_TESTS[${entry_index}]}"
    entry_key="${package_name}:${test_name}"
    "${TEST_BINARY_BY_ENTRY[${entry_key}]}" "${TEST_FULL_NAME_BY_ENTRY[${entry_key}]}" --exact --nocapture
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
