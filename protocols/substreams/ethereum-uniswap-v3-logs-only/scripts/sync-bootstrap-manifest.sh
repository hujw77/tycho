#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
CONFIG_PATH="${REPO_ROOT}/crates/tycho-indexer/config/uniswap_v3_bootstrap.yaml"
MANIFEST_PATH="${REPO_ROOT}/protocols/substreams/ethereum-uniswap-v3-logs-only/uniswap-v3-bootstrap.yaml"
TMP_PATH="${MANIFEST_PATH}.tmp"

start_block="$(sed -n 's/^start_block:[[:space:]]*//p' "${CONFIG_PATH}")"
if [[ -z "${start_block}" ]]; then
  echo "failed to read start_block from ${CONFIG_PATH}" >&2
  exit 1
fi

pools="$(
  sed -n 's/^[[:space:]]*-[[:space:]]*"\(0x[0-9a-fA-F]\{40\}\)".*/\1/p' "${CONFIG_PATH}" \
    | paste -sd, -
)"
if [[ -z "${pools}" ]]; then
  echo "failed to read pools from ${CONFIG_PATH}" >&2
  exit 1
fi

bootstrap_params="bootstrap_block=${start_block}&pools=${pools}"

awk -v start_block="${start_block}" -v bootstrap_params="${bootstrap_params}" '
  /^params:/ {
    in_params = 1
    print
    next
  }

  in_params && /^[[:space:]][[:space:]]map_bootstrap_pools_created:/ {
    print "  map_bootstrap_pools_created: \"" bootstrap_params "\""
    next
  }

  /^[[:space:]]*initialBlock:/ {
    sub(/initialBlock:.*/, "initialBlock: " start_block)
    print
    next
  }

  { print }
' "${MANIFEST_PATH}" > "${TMP_PATH}"

mv "${TMP_PATH}" "${MANIFEST_PATH}"
echo "synced ${MANIFEST_PATH} from ${CONFIG_PATH}"
