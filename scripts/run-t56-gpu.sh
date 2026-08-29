#!/usr/bin/env bash
set -Eeuo pipefail

# Maintained entrypoint for reproducing the historical T5.6 procedure.  The
# archived runner is evidence from the real run and must remain byte-identical;
# it is therefore invoked only through this compatibility boundary.
SUPPORTED_VLLM_VERSION="0.28.0"
REQUESTED_VLLM_VERSION="${VLLM_VERSION:-$SUPPORTED_VLLM_VERSION}"

if [[ "$REQUESTED_VLLM_VERSION" != "$SUPPORTED_VLLM_VERSION" ]]; then
  printf 'ERROR: unsupported VLLM_VERSION=%s; T5.6 supports exactly %s\n' \
    "$REQUESTED_VLLM_VERSION" "$SUPPORTED_VLLM_VERSION" >&2
  exit 64
fi

export VLLM_VERSION="$SUPPORTED_VLLM_VERSION"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ARCHIVED_RUNNER="$REPO_ROOT/artifacts/t5.6/20260828T211053Z/run_t56_gpu.sh"

exec bash "$ARCHIVED_RUNNER" "$@"
