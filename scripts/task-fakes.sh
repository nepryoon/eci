#!/usr/bin/env bash
# Explicit deterministic fake-service suite (SPEC-068).
set -euo pipefail

while IFS= read -r dir; do
  case "${dir}" in fakes/*) ;; *) continue ;; esac
  venv="${dir}/.venv"
  if [ ! -x "${venv}/bin/python" ]; then
    python3 -m venv "${venv}" >/dev/null 2>&1 || python3 -m virtualenv -q "${venv}"
  fi
  "${venv}/bin/python" -m pip install -q -e "${dir}" pytest
  echo "== fake (pytest) ${dir} =="
  "${venv}/bin/python" -m pytest "${dir}"
done < <(bash scripts/module-inventory.sh --kind python)
