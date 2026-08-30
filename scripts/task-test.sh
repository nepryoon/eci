#!/usr/bin/env bash
# task test — CPU/unit tests for every tracked production manifest (SPEC-068).
# Docker-backed tests are marked integration and run through task test:integration.
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
  echo "== test (go) ${dir} =="
  (cd "${dir}" && go test ./...) || status=1
done

for dir in "${RUST_MODULES[@]}"; do
  echo "== test (cargo) ${dir} =="
  cargo test --manifest-path "${dir}/Cargo.toml" || status=1
done

for dir in "${PY_MODULES[@]}"; do
  venv="${dir}/.venv"
  if ! ensure_venv "${venv}"; then
    echo "ERRORE: impossibile creare ${venv}" >&2
    status=1
    continue
  fi
  if [ "${dir}" != "libs/py" ]; then
    "${venv}/bin/python" -m pip install -q -e "libs/py" || { status=1; continue; }
  fi
  case "${dir}" in
    services/*) install_target="${dir}[test]" ;;
    *) install_target="${dir}" ;;
  esac
  "${venv}/bin/python" -m pip install -q -e "${install_target}" || { status=1; continue; }
  if [ ! -x "${venv}/bin/pytest" ]; then
    "${venv}/bin/python" -m pip install -q pytest || { status=1; continue; }
  fi
  echo "== test (pytest, no integration) ${dir} =="
  "${venv}/bin/python" -m pytest -m "not integration" "${dir}"
  rc=$?
  if [ "${rc}" -eq 5 ]; then
    echo "  no tests collected in ${dir}"
  elif [ "${rc}" -ne 0 ]; then
    status=1
  fi
done

while IFS= read -r test_file; do
  echo "== test (repository unit) ${test_file} =="
  python3 "${test_file}" || status=1
done < <(git ls-files 'tests/unit/**/test_*.py' | LC_ALL=C sort)

exit "${status}"
