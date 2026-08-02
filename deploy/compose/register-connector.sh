#!/usr/bin/env bash
# SPEC-007 §2 — registra il connector Debezium outbox su Kafka Connect via
# REST API, con retry/backoff se Connect non è ancora pronto (non un
# tentativo secco). Un 409 Conflict (connector già registrato) è trattato
# come successo, non come errore — coerente con la necessità che `task up`
# resti rilanciabile senza errori (scenario 3).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTOR_JSON="${SCRIPT_DIR}/debezium-outbox-connector.json"
CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
TIMEOUT_SECONDS="${REGISTER_TIMEOUT_SECONDS:-60}"
RETRY_INTERVAL_SECONDS=3

response_file="$(mktemp)"
curlerr_file="$(mktemp)"
trap 'rm -f "${response_file}" "${curlerr_file}"' EXIT

elapsed=0
while true; do
  http_code="$(curl -s -o "${response_file}" -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data "@${CONNECTOR_JSON}" \
    "${CONNECT_URL}/connectors" 2>"${curlerr_file}")"
  curl_rc=$?
  body="$(cat "${response_file}" 2>/dev/null)"
  curl_err="$(cat "${curlerr_file}" 2>/dev/null)"

  if [ "${curl_rc}" -eq 0 ] && { [ "${http_code}" = "201" ] || [ "${http_code}" = "409" ]; }; then
    if [ "${http_code}" = "201" ]; then
      echo "Connector eci-outbox-connector registrato (201 Created)."
    else
      echo "Connector eci-outbox-connector già registrato (409 Conflict, trattato come successo)."
    fi
    exit 0
  fi

  if [ "${elapsed}" -ge "${TIMEOUT_SECONDS}" ]; then
    echo "ERRORE: registrazione del connector fallita dopo ${TIMEOUT_SECONDS}s." >&2
    echo "Ultimo tentativo: http_code=${http_code:-n/a} curl_rc=${curl_rc}" >&2
    [ -n "${curl_err}" ] && echo "curl: ${curl_err}" >&2
    [ -n "${body}" ] && echo "risposta: ${body}" >&2
    exit 1
  fi

  sleep "${RETRY_INTERVAL_SECONDS}"
  elapsed=$((elapsed + RETRY_INTERVAL_SECONDS))
done
