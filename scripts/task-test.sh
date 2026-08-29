#!/usr/bin/env bash
# task test — SPEC-001 §2. go test / cargo test / pytest su ogni modulo
# popolato, più i test unitari di scripts/guard.sh (SPEC-001 §7). Un modulo non
# ancora popolato viene saltato con un messaggio (SPEC-001 §4). pytest che
# esce con codice 5 ("no tests collected") è uno skip esplicito, non un errore.
# I moduli Python usano l'interprete del virtualenv dedicato per servizio
# (services/<nome>/.venv), mai il Python di sistema (SPEC-001 §4).
set -uo pipefail

status=0

GO_SERVICES=(sink-graph sink-vector sink-search retrieval-engine llm-gateway semantic-cache)
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
  echo "== test (go) ${svc} =="
  (cd "${dir}" && go test ./...) || status=1
done

for svc in "${RUST_SERVICES[@]}"; do
  dir="services/${svc}"
  if [ ! -f "${dir}/Cargo.toml" ]; then
    echo "skip: ${dir} non ancora popolato (nessun Cargo.toml)"
    continue
  fi
  echo "== test (cargo) ${svc} =="
  (cd "${dir}" && cargo test) || status=1
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
  "${venv}/bin/python" -m pip install -q -e "libs/py" || { status=1; continue; }
  # [test]: dipendenze extra dichiarate solo da alcuni servizi (es.
  # testcontainers per orchestrator, SPEC-018) — richiedere l'extra su un
  # pacchetto che non la definisce produce solo un warning pip innocuo
  # ("does not provide the extra"), non un errore, verificato prima di
  # applicare questo cambio a tutti e tre i PY_SERVICES indistintamente.
  "${venv}/bin/python" -m pip install -q -e "${dir}[test]" || { status=1; continue; }
  if [ ! -x "${venv}/bin/pytest" ]; then
    "${venv}/bin/python" -m pip install -q pytest || { status=1; continue; }
  fi
  echo "== test (venv pytest) ${svc} =="
  "${venv}/bin/python" -m pytest "${dir}"
  rc=$?
  if [ "${rc}" -eq 5 ]; then
    echo "  skip: nessun test raccolto in ${dir} (pytest exit 5)"
  elif [ "${rc}" -ne 0 ]; then
    status=1
  fi
done

# libs/go, libs/py — generati da SPEC-002 (contratto proto D7).
if [ -f "libs/go/go.mod" ]; then
  echo "== test (go) libs/go =="
  (cd libs/go && go test ./...) || status=1
else
  echo "skip: libs/go non ancora popolato (nessun go.mod)"
fi

if [ -f "libs/py/pyproject.toml" ]; then
  venv="libs/py/.venv"
  if ! ensure_venv "${venv}"; then
    status=1
  else
    if [ ! -x "${venv}/bin/pytest" ]; then
      "${venv}/bin/python" -m pip install -q pytest || status=1
    fi
    if [ -x "${venv}/bin/pytest" ]; then
      echo "== test (venv pytest) libs/py =="
      "${venv}/bin/python" -m pytest "libs/py"
      rc=$?
      if [ "${rc}" -eq 5 ]; then
        echo "  skip: nessun test raccolto in libs/py (pytest exit 5)"
      elif [ "${rc}" -ne 0 ]; then
        status=1
      fi
    fi
  fi
else
  echo "skip: libs/py non ancora popolato (nessun pyproject.toml)"
fi

echo "== test (unit: scripts/guard.sh) =="
python3 tests/unit/guard/test_guard.py || status=1

echo "== test (unit: T5.6 runner provenance boundary) =="
python3 tests/unit/t5_6_runner/test_t5_6_runner.py || status=1

exit "${status}"
