#!/usr/bin/env bash

if [[ -n "${TYCHO_COMBINED_FAMILY_COMMON_SH_LOADED:-}" ]]; then
  return 0
fi
TYCHO_COMBINED_FAMILY_COMMON_SH_LOADED=1

TYCHO_COMBINED_FAMILY_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TYCHO_COMBINED_FAMILY_REPO_ROOT="$(cd "${TYCHO_COMBINED_FAMILY_SCRIPT_DIR}/.." && pwd)"
TYCHO_COMBINED_FAMILY_CANONICAL_EXTRACTORS_CONFIG="crates/tycho-indexer/extractors.uniswap_v2_v3.combined.yaml"
TYCHO_COMBINED_FAMILY_CANONICAL_EXTRACTORS_CONFIG_ABS="${TYCHO_COMBINED_FAMILY_REPO_ROOT}/${TYCHO_COMBINED_FAMILY_CANONICAL_EXTRACTORS_CONFIG}"
TYCHO_COMBINED_FAMILY_DEFAULT_FYND_REPO_ROOT="$(cd "${TYCHO_COMBINED_FAMILY_REPO_ROOT}/.." && pwd)/fynd"

tycho_combined_family_shell_escape() {
  local arg="$1"
  if [[ "${arg}" =~ ^[A-Za-z0-9_./:+=,-]+$ ]]; then
    printf '%s' "${arg}"
    return
  fi
  printf "'%s'" "${arg//\'/\'\"\'\"\'}"
}
