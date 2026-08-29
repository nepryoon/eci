#!/usr/bin/env bash
# task lint — SPEC-001 §2. go vet / cargo clippy / ruff check su ogni modulo
# popolato. Un modulo non ancora popolato viene saltato con un messaggio
# (SPEC-001 §4). I moduli Python usano l'interprete del virtualenv dedicato
# per servizio (services/<nome>/.venv), mai il Python di sistema (SPEC-001 §4).
set -uo pipefail

status=0

GO_SERVICES=(sink-graph sink-vector sink-search retrieval-engine llm-gateway semantic-cache api-gateway)
GO_TOOLS=(gds-impact)
RUST_SERVICES=(ingestion)
PY_SERVICES=(orchestrator verification summarization)

# Crea services/<nome>/.venv se non esiste già (vedi scripts/task-build.sh).
ensure_venv() {
  local venv="$1"
  if [ -x "${venv}/bin/python" ]; then
    return 0
  fi
  local err_log
  err_log="$(mktemp)"
  if python3 -m venv "${venv}" >"${err_log}" 2>&1; then
    rm -f "${err_log}"
    return 0
  fi
  if python3 -m virtualenv -q "${venv}" >>"${err_log}" 2>&1; then
    rm -f "${err_log}"
    return 0
  fi
  echo "ERRORE: impossibile creare il virtualenv in ${venv}." >&2
  echo "Installa 'python3-venv' (es. 'apt install python3-venv') oppure 'pip install virtualenv'." >&2
  cat "${err_log}" >&2
  rm -f "${err_log}"
  return 1
}

for svc in "${GO_SERVICES[@]}"; do
  dir="services/${svc}"
  if [ ! -f "${dir}/go.mod" ]; then
    echo "skip: ${dir} non ancora popolato (nessun go.mod)"
    continue
  fi
  echo "== lint (go vet) ${svc} =="
  (cd "${dir}" && go vet ./...) || status=1
done

for tool in "${GO_TOOLS[@]}"; do
  echo "== lint (go vet tool) ${tool} =="
  (cd "tools/${tool}" && go vet ./...) || status=1
done

for svc in "${RUST_SERVICES[@]}"; do
  dir="services/${svc}"
  if [ ! -f "${dir}/Cargo.toml" ]; then
    echo "skip: ${dir} non ancora popolato (nessun Cargo.toml)"
    continue
  fi
  echo "== lint (cargo clippy) ${svc} =="
  (cd "${dir}" && cargo clippy --all-targets) || status=1
done

for svc in "${PY_SERVICES[@]}"; do
  dir="services/${svc}"
  if [ ! -f "${dir}/pyproject.toml" ]; then
    echo "skip: ${dir} non ancora popolato (nessun pyproject.toml)"
    continue
  fi
  venv="${dir}/.venv"
  if ! ensure_venv "${venv}"; then
    status=1
    continue
  fi
  if [ ! -x "${venv}/bin/ruff" ]; then
    "${venv}/bin/python" -m pip install -q ruff || { status=1; continue; }
  fi
  echo "== lint (venv ruff check) ${svc} =="
  "${venv}/bin/ruff" check "${dir}" || status=1
done

# libs/go, libs/py — generati da SPEC-002 (contratto proto D7).
if [ -f "libs/go/go.mod" ]; then
  echo "== lint (go vet) libs/go =="
  (cd libs/go && go vet ./...) || status=1
else
  echo "skip: libs/go non ancora popolato (nessun go.mod)"
fi

if [ -f "libs/py/pyproject.toml" ]; then
  venv="libs/py/.venv"
  if ! ensure_venv "${venv}"; then
    status=1
  else
    if [ ! -x "${venv}/bin/ruff" ]; then
      "${venv}/bin/python" -m pip install -q ruff || status=1
    fi
    if [ -x "${venv}/bin/ruff" ]; then
      echo "== lint (venv ruff check) libs/py =="
      "${venv}/bin/ruff" check "libs/py" || status=1
    fi
  fi
else
  echo "skip: libs/py non ancora popolato (nessun pyproject.toml)"
fi

exit "${status}"
