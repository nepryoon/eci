#!/usr/bin/env bash
# task build — SPEC-001 §2. Compila ogni modulo Go/Rust/Python popolato.
# Un modulo non ancora popolato viene saltato con un messaggio, non è un errore
# (SPEC-001 §4). I moduli Python usano un virtualenv dedicato per servizio
# (services/<nome>/.venv), mai il Python di sistema (SPEC-001 §4).
set -uo pipefail

status=0

GO_SERVICES=(sink-graph sink-vector sink-search retrieval-engine llm-gateway semantic-cache)
RUST_SERVICES=(ingestion)
PY_SERVICES=(orchestrator verification summarization)

# Crea services/<nome>/.venv se non esiste già. python3 -m venv è il percorso
# standard (funziona su CI); su un Python di sistema "externally managed"
# senza il pacchetto python3-venv, ripiega su virtualenv (bundla il proprio
# pip e non richiede ensurepip).
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
  echo "== build (go) ${svc} =="
  (cd "${dir}" && go build ./...) || status=1
done

for svc in "${RUST_SERVICES[@]}"; do
  dir="services/${svc}"
  if [ ! -f "${dir}/Cargo.toml" ]; then
    echo "skip: ${dir} non ancora popolato (nessun Cargo.toml)"
    continue
  fi
  echo "== build (cargo) ${svc} =="
  (cd "${dir}" && cargo build) || status=1
done

for svc in "${PY_SERVICES[@]}"; do
  dir="services/${svc}"
  if [ ! -f "${dir}/pyproject.toml" ]; then
    echo "skip: ${dir} non ancora popolato (nessun pyproject.toml)"
    continue
  fi
  venv="${dir}/.venv"
  echo "== build (venv + pip install -e .) ${svc} =="
  if ! ensure_venv "${venv}"; then
    status=1
    continue
  fi
  "${venv}/bin/python" -m pip install -q -e "libs/py" || { status=1; continue; }
  "${venv}/bin/python" -m pip install -q -e "${dir}" || status=1
done

# libs/go, libs/py — generati da SPEC-002 (contratto proto D7).
if [ -f "libs/go/go.mod" ]; then
  echo "== build (go) libs/go =="
  (cd libs/go && go build ./...) || status=1
else
  echo "skip: libs/go non ancora popolato (nessun go.mod)"
fi

if [ -f "libs/py/pyproject.toml" ]; then
  venv="libs/py/.venv"
  echo "== build (venv + pip install -e .) libs/py =="
  if ! ensure_venv "${venv}"; then
    status=1
  else
    "${venv}/bin/python" -m pip install -q -e "libs/py" || status=1
  fi
else
  echo "skip: libs/py non ancora popolato (nessun pyproject.toml)"
fi

exit "${status}"
