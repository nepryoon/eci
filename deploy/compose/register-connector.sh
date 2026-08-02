#!/usr/bin/env bash
# SPEC-007 §2 — registra il connector Debezium outbox su Kafka Connect via
# REST API, con retry/backoff se Connect non è ancora pronto (non un
# tentativo secco). Un 409 Conflict (connector già registrato) è trattato
# come successo, non come errore — coerente con la necessità che `task up`
# resti rilanciabile senza errori (scenario 3).
#
# Il 201/409 della POST conferma solo che la registrazione è stata
# accettata, non che il task sia RUNNING: in modalità distribuita, Kafka
# Connect assegna il task dopo un rebalance che richiede alcuni secondi —
# subito dopo la registrazione, GET .../status può mostrare temporaneamente
# tasks: [] (nessun task ancora assegnato). Per questo, dopo il 201/409 lo
# script fa il poll di GET .../status finché connector.state=RUNNING e
# almeno un elemento di tasks[] ha state=RUNNING, con un timeout separato
# (SPEC-007, review post-implementazione).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTOR_JSON="${SCRIPT_DIR}/debezium-outbox-connector.json"
CONNECTOR_NAME="$(jq -r '.name' "${CONNECTOR_JSON}")"
CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
REGISTER_TIMEOUT_SECONDS="${REGISTER_TIMEOUT_SECONDS:-60}"
RUNNING_TIMEOUT_SECONDS="${RUNNING_TIMEOUT_SECONDS:-60}"
RETRY_INTERVAL_SECONDS=3

response_file="$(mktemp)"
curlerr_file="$(mktemp)"
trap 'rm -f "${response_file}" "${curlerr_file}"' EXIT

register() {
  local elapsed=0
  while true; do
    local http_code curl_rc body curl_err
    http_code="$(curl -s -o "${response_file}" -w '%{http_code}' \
      -X POST -H 'Content-Type: application/json' \
      --data "@${CONNECTOR_JSON}" \
      "${CONNECT_URL}/connectors" 2>"${curlerr_file}")"
    curl_rc=$?
    body="$(cat "${response_file}" 2>/dev/null)"
    curl_err="$(cat "${curlerr_file}" 2>/dev/null)"

    if [ "${curl_rc}" -eq 0 ] && { [ "${http_code}" = "201" ] || [ "${http_code}" = "409" ]; }; then
      if [ "${http_code}" = "201" ]; then
        echo "Connector ${CONNECTOR_NAME} registrato (201 Created)."
      else
        echo "Connector ${CONNECTOR_NAME} già registrato (409 Conflict, trattato come successo)."
      fi
      return 0
    fi

    if [ "${elapsed}" -ge "${REGISTER_TIMEOUT_SECONDS}" ]; then
      echo "ERRORE: registrazione del connector fallita dopo ${REGISTER_TIMEOUT_SECONDS}s." >&2
      echo "Ultimo tentativo: http_code=${http_code:-n/a} curl_rc=${curl_rc}" >&2
      [ -n "${curl_err}" ] && echo "curl: ${curl_err}" >&2
      [ -n "${body}" ] && echo "risposta: ${body}" >&2
      return 1
    fi

    sleep "${RETRY_INTERVAL_SECONDS}"
    elapsed=$((elapsed + RETRY_INTERVAL_SECONDS))
  done
}

# Poll GET .../status finché connector.state=RUNNING e almeno un task è
# RUNNING. Il 201/409 della registrazione non basta: il task assignment in
# modalità distribuita non è istantaneo (vedi commento in testa al file).
wait_for_running() {
  local start_ts elapsed status_json connector_state running_tasks task_count
  start_ts="$(date +%s)"
  while true; do
    status_json="$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null)"
    connector_state="$(echo "${status_json}" | jq -r '.connector.state // "sconosciuto"' 2>/dev/null)"
    running_tasks="$(echo "${status_json}" | jq -r '[.tasks[]? | select(.state == "RUNNING")] | length' 2>/dev/null)"
    task_count="$(echo "${status_json}" | jq -r '.tasks | length' 2>/dev/null)"
    elapsed=$(( $(date +%s) - start_ts ))

    if [ "${connector_state}" = "RUNNING" ] && [ "${running_tasks:-0}" -ge 1 ]; then
      echo "Connector ${CONNECTOR_NAME}: connector.state=RUNNING, ${running_tasks}/${task_count} task RUNNING (dopo ${elapsed}s di poll)."
      return 0
    fi

    if [ "${elapsed}" -ge "${RUNNING_TIMEOUT_SECONDS}" ]; then
      echo "ERRORE: il connector ${CONNECTOR_NAME} non è arrivato a RUNNING con almeno un task RUNNING entro ${RUNNING_TIMEOUT_SECONDS}s." >&2
      echo "Ultimo stato osservato: connector.state=${connector_state}, task RUNNING=${running_tasks:-0}/${task_count:-0}" >&2
      echo "Risposta completa: ${status_json}" >&2
      return 1
    fi

    sleep "${RETRY_INTERVAL_SECONDS}"
  done
}

register && wait_for_running
