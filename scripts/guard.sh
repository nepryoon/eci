#!/usr/bin/env bash
# Fallisce se contracts/ o docs/add/ sono modificati (M), cancellati (D) o
# rinominati/copiati (R/C, in entrata o in uscita), senza un ADR aggiunto in
# docs/decisions/ nello stesso diff. L'aggiunta di file nuovi (A) non
# richiede ADR. Vedi SPEC-001 §2.
set -euo pipefail

BASE_REF="${BASE_REF:-origin/main}"

if ! git rev-parse --verify --quiet "${BASE_REF}" >/dev/null; then
  echo "ERRORE: impossibile risolvere BASE_REF='${BASE_REF}'." >&2
  echo "Esegui un fetch più profondo (es. 'git fetch --unshallow' o 'git fetch origin main') e riprova." >&2
  exit 1
fi

CHANGED="$(git diff --name-status "${BASE_REF}"...HEAD)"

protected_touched=()
has_adr_added=false

is_protected() {
  case "$1" in
    contracts/*|docs/add/*) return 0 ;;
    *) return 1 ;;
  esac
}

while IFS=$'\t' read -r status path1 path2; do
  [ -z "${status}" ] && continue
  case "${status}" in
    R*|C*)
      if is_protected "${path1}" || is_protected "${path2}"; then
        protected_touched+=("${status}"$'\t'"${path1} -> ${path2}")
      fi
      ;;
    *)
      if is_protected "${path1}"; then
        case "${status}" in
          M|D)
            protected_touched+=("${status}"$'\t'"${path1}")
            ;;
        esac
      fi
      if [[ "${status}" == "A" ]] && [[ "${path1}" == docs/decisions/* ]]; then
        has_adr_added=true
      fi
      ;;
  esac
done <<< "${CHANGED}"

if [ ${#protected_touched[@]} -eq 0 ]; then
  exit 0
fi

if [ "${has_adr_added}" = true ]; then
  exit 0
fi

echo "Modifiche a contracts/ o docs/add/ richiedono un ADR in docs/decisions/ nello stesso commit." >&2
echo "" >&2
echo "File protetti toccati:" >&2
for line in "${protected_touched[@]}"; do
  echo "  ${line}" >&2
done
exit 1
