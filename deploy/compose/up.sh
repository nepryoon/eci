#!/usr/bin/env bash
# SPEC-006 §2 — `task up`: avvia lo stack e attende che tutti i servizi con
# healthcheck risultino "healthy" entro un timeout complessivo (default 180s,
# include il primo bootstrap/import JVM di Keycloak da SPEC-055).
# Se il timeout scade, fallisce elencando esplicitamente quali servizi non
# sono diventati healthy (non un timeout muto).
# SPEC-007 §2 (estensione): una volta tutti healthy (kafka/kafka-connect
# inclusi), registra il connector Debezium outbox.
set -uo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"
TIMEOUT_SECONDS="${UP_TIMEOUT_SECONDS:-180}"
POLL_INTERVAL_SECONDS=3

docker compose -f "${COMPOSE_FILE}" up -d
up_rc=$?
if [ "${up_rc}" -ne 0 ]; then
  echo "ERRORE: 'docker compose up -d' fallito (rc=${up_rc})." >&2
  exit "${up_rc}"
fi

elapsed=0
while true; do
  statuses="$(docker compose -f "${COMPOSE_FILE}" ps --format json | jq -r '"\(.Service)\t\(.Health)"')"

  not_healthy=()
  while IFS=$'\t' read -r service health; do
    [ -z "${service}" ] && continue
    if [ "${health}" != "healthy" ]; then
      not_healthy+=("${service} (stato: ${health:-nessun healthcheck riportato})")
    fi
  done <<<"${statuses}"

  if [ "${#not_healthy[@]}" -eq 0 ]; then
    echo "Tutti i servizi sono healthy dopo ${elapsed}s."
    # SPEC-007 §2: registra (o conferma già registrato, 409=successo) il
    # connector Debezium outbox una volta che kafka-connect è healthy.
    bash "$(dirname "${BASH_SOURCE[0]}")/register-connector.sh"
    exit $?
  fi

  if [ "${elapsed}" -ge "${TIMEOUT_SECONDS}" ]; then
    echo "ERRORE: timeout (${TIMEOUT_SECONDS}s) in attesa che tutti i servizi diventino healthy." >&2
    echo "Servizi non healthy:" >&2
    for entry in "${not_healthy[@]}"; do
      echo "  - ${entry}" >&2
    done
    exit 1
  fi

  sleep "${POLL_INTERVAL_SECONDS}"
  elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
done
