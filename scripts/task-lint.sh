#!/usr/bin/env bash
# task lint — every tracked production manifest (SPEC-068).
set -uo pipefail

status=0
mapfile -t GO_MODULES < <(bash scripts/module-inventory.sh --kind go)
mapfile -t RUST_MODULES < <(bash scripts/module-inventory.sh --kind rust)
mapfile -t PY_MODULES < <(bash scripts/module-inventory.sh --kind python)

ensure_venv() {
  local venv="$1"
  if [ -x "${venv}/bin/python" ]; then
    return 0
  fi
  python3 -m venv "${venv}" >/dev/null 2>&1 || python3 -m virtualenv -q "${venv}"
}

for dir in "${GO_MODULES[@]}"; do
  echo "== lint (go vet) ${dir} =="
  (cd "${dir}" && go vet ./...) || status=1
done

for dir in "${RUST_MODULES[@]}"; do
  echo "== lint (cargo clippy -D warnings) ${dir} =="
  cargo clippy --manifest-path "${dir}/Cargo.toml" --all-targets -- -D warnings || status=1
done

for dir in "${PY_MODULES[@]}"; do
  venv="${dir}/.venv"
  if ! ensure_venv "${venv}"; then
    echo "ERRORE: impossibile creare ${venv}" >&2
    status=1
    continue
  fi
  if [ ! -x "${venv}/bin/ruff" ]; then
    "${venv}/bin/python" -m pip install -q ruff==0.16.1 || { status=1; continue; }
  fi
  echo "== lint (ruff) ${dir} =="
  "${venv}/bin/ruff" check "${dir}" || status=1
done

exit "${status}"
