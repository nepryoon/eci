#!/usr/bin/env bash
# Canonical inventory of populated production modules (SPEC-068).
set -euo pipefail

usage() {
  echo "usage: $0 --list | --kind go|rust|python" >&2
  exit 2
}

mode="${1:---list}"
kind_filter="${2:-}"
if [ "${mode}" = "--kind" ]; then
  case "${kind_filter}" in go|rust|python) ;; *) usage ;; esac
elif [ "${mode}" != "--list" ] || [ "$#" -ne 1 ]; then
  usage
fi

git ls-files -z | while IFS= read -r -d '' path; do
  case "${path}" in
    services/*/go.mod|tools/*/go.mod|libs/*/go.mod) kind=go ;;
    services/*/Cargo.toml|tools/*/Cargo.toml|libs/*/Cargo.toml) kind=rust ;;
    services/*/pyproject.toml|tools/*/pyproject.toml|libs/*/pyproject.toml|fakes/*/pyproject.toml) kind=python ;;
    *) continue ;;
  esac
  dir="${path%/*}"
  if [ "${mode}" = "--list" ]; then
    printf '%s\t%s\n' "${kind}" "${dir}"
  elif [ "${kind}" = "${kind_filter}" ]; then
    printf '%s\n' "${dir}"
  fi
done | LC_ALL=C sort
