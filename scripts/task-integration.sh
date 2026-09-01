#!/usr/bin/env bash
# Complete Docker-backed integration surface (SPEC-068).
set -euo pipefail

if ! docker info >/dev/null 2>&1; then
  echo "ERRORE: task test:integration richiede un daemon Docker raggiungibile." >&2
  exit 1
fi

while IFS= read -r dir; do
  echo "== integration (go) ${dir} =="
  (cd "${dir}" && go test -tags=integration ./...)
done < <(bash scripts/module-inventory.sh --kind go)

while IFS= read -r manifest; do
  dir="${manifest%/go.mod}"
  echo "== integration (go harness) ${dir} =="
  (cd "${dir}" && go test -tags=integration ./...)
done < <(git ls-files 'tests/integration/*/go.mod' | LC_ALL=C sort)

echo "== integration (cargo ignored) services/ingestion =="
cargo test --manifest-path services/ingestion/Cargo.toml -- --ignored --test-threads=1

echo "== integration (pytest) services/orchestrator =="
services/orchestrator/.venv/bin/python -m pip install -q -e libs/py -e 'services/orchestrator[test]'
services/orchestrator/.venv/bin/python -m pytest -m integration services/orchestrator

echo "== integration (MinIO WORM) services/verification =="
bash scripts/test-minio-worm.sh
