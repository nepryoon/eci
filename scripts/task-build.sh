#!/usr/bin/env bash
# task build — every tracked production manifest (SPEC-068).
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
  cat "${err_log}" >&2
  rm -f "${err_log}"
  return 1
}

for dir in "${GO_MODULES[@]}"; do
  echo "== build (go) ${dir} =="
  (cd "${dir}" && go build ./...) || status=1
done

for dir in "${RUST_MODULES[@]}"; do
  echo "== build (cargo) ${dir} =="
  cargo build --manifest-path "${dir}/Cargo.toml" || status=1
done

for dir in "${PY_MODULES[@]}"; do
  venv="${dir}/.venv"
  echo "== build (venv + editable install) ${dir} =="
  if ! ensure_venv "${venv}"; then
    status=1
    continue
  fi
  if [ "${dir}" != "libs/py" ]; then
    "${venv}/bin/python" -m pip install -q -e "libs/py" || { status=1; continue; }
  fi
  "${venv}/bin/python" -m pip install -q -e "${dir}" || status=1
done

exit "${status}"
