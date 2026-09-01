#!/usr/bin/env bash
# Full Docker-backed pipeline E2E suite (SPEC-019, exposed by SPEC-068).
set -euo pipefail

if ! docker info >/dev/null 2>&1; then
  echo "ERRORE: task test:e2e richiede un daemon Docker raggiungibile." >&2
  exit 1
fi

venv="tests/e2e/.venv"
if [ ! -x "${venv}/bin/python" ]; then
  python3 -m venv "${venv}" >/dev/null 2>&1 || python3 -m virtualenv -q "${venv}"
fi
"${venv}/bin/python" -m pip install -q \
  -e libs/py -e 'services/orchestrator[test]' \
  pytest 'testcontainers[neo4j,postgres,kafka]>=4.15' packaging
PYTHONPATH="tests/e2e${PYTHONPATH:+:${PYTHONPATH}}" \
  "${venv}/bin/python" -m pytest tests/e2e -v
