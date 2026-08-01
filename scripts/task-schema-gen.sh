#!/usr/bin/env bash
# task schema:gen — SPEC-003 §2. Genera i modelli Pydantic v2 da
# contracts/jsonschema/{hybrid-graph,outbox-event}.json via
# datamodel-code-generator, installato nel venv di tooling .venv-tools/
# (stesso pattern di task-proto-gen.sh: mai nel Python di sistema).
#
# Il discriminatore `domain` di CodeNode (if/then/const) non viene mappato
# nativamente da datamodel-code-generator su una discriminated union
# Pydantic v2: verificato che `ext` collassa a `dict[str, Any] | None` nel
# modello generato. Il fallback manuale previsto dalla SPEC (CodeNodeCode/
# Doc/Legal + parse_code_node) vive quindi a mano in eci_core/code_node.py,
# non generato, per non essere sovrascritto ad ogni rigenerazione.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_VENV="${REPO_ROOT}/.venv-tools"

if [ ! -x "${TOOLS_VENV}/bin/python" ] || ! "${TOOLS_VENV}/bin/python" -c "import datamodel_code_generator" >/dev/null 2>&1; then
  echo "ERRORE: 'datamodel-code-generator' non installato nel venv di tooling ${TOOLS_VENV}." >&2
  echo "Crea/popola con:" >&2
  echo "  python3 -m venv ${TOOLS_VENV} && ${TOOLS_VENV}/bin/pip install datamodel-code-generator" >&2
  exit 1
fi

mkdir -p "${REPO_ROOT}/libs/py/eci_core"
touch "${REPO_ROOT}/libs/py/eci_core/__init__.py"

GEN_FLAGS=(
  --input-file-type jsonschema
  --output-model-type pydantic_v2.BaseModel
  --disable-timestamp
  --use-double-quotes
  --target-python-version 3.11
)

echo "== schema:gen (Pydantic, hybrid-graph.json) =="
"${TOOLS_VENV}/bin/python" -m datamodel_code_generator \
  "${GEN_FLAGS[@]}" \
  --input "${REPO_ROOT}/contracts/jsonschema/hybrid-graph.json" \
  --output "${REPO_ROOT}/libs/py/eci_core/models.py" || exit 1

echo "== schema:gen (Pydantic, outbox-event.json) =="
"${TOOLS_VENV}/bin/python" -m datamodel_code_generator \
  "${GEN_FLAGS[@]}" \
  --class-name OutboxEvent \
  --input "${REPO_ROOT}/contracts/jsonschema/outbox-event.json" \
  --output "${REPO_ROOT}/libs/py/eci_core/outbox.py" || exit 1

echo "schema:gen completato."
