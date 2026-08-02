#!/usr/bin/env bash
# SPEC-006 §3 scenario 2 — `task up:verify`: connettività reale contro lo
# stack già in esecuzione (non solo "container up"). Ogni servizio è
# verificato e riportato esplicitamente: nome + esito + motivo in caso di
# fallimento, mai un errore aggregato generico. Se lo stack non è in
# esecuzione, fallisce dicendolo esplicitamente (non con un timeout di
# connessione criptico).
# SPEC-007 §2/§3 scenario 2 (estensione): verifica anche che il connector
# Debezium outbox sia connector.state=RUNNING con tutti i task RUNNING.
set -uo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"
POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-5432}"
QDRANT_HOST_PORT="${QDRANT_HOST_PORT:-6333}"
OPENSEARCH_HOST_PORT="${OPENSEARCH_HOST_PORT:-9200}"
MINIO_HOST_PORT="${MINIO_HOST_PORT:-9000}"
KAFKA_CONNECT_HOST_PORT="${KAFKA_CONNECT_HOST_PORT:-8083}"
CONNECTOR_NAME="eci-outbox-connector"

overall_rc=0

running_services="$(docker compose -f "${COMPOSE_FILE}" ps --status running --format json 2>/dev/null | jq -r '.Service' || true)"

check_running() {
  local service="$1"
  if ! grep -qx "${service}" <<<"${running_services}"; then
    echo "FAIL ${service}: container non in esecuzione (stack non avviato o servizio fermo — esegui 'task up' prima di 'task up:verify')"
    overall_rc=1
    return 1
  fi
  return 0
}

if [ -z "${running_services}" ]; then
  echo "FAIL: nessun container dello stack eci-dev risulta in esecuzione. Esegui 'task up' prima di 'task up:verify'." >&2
  exit 1
fi

# postgres: SELECT 1
if check_running postgres; then
  if out="$(docker compose -f "${COMPOSE_FILE}" exec -T postgres psql -U eci -d eci -tAc 'SELECT 1;' 2>&1)"; then
    if [ "$(echo "${out}" | tr -d '[:space:]')" = "1" ]; then
      echo "OK   postgres: SELECT 1 riuscita"
    else
      echo "FAIL postgres: SELECT 1 ha risposto in modo inatteso: ${out}"
      overall_rc=1
    fi
  else
    echo "FAIL postgres: SELECT 1 fallita: ${out}"
    overall_rc=1
  fi
fi

# neo4j: RETURN 1 via cypher-shell (bolt), credenziali dell'env NEO4J_AUTH
if check_running neo4j; then
  if out="$(docker compose -f "${COMPOSE_FILE}" exec -T neo4j cypher-shell -u neo4j -p eci-dev-only 'RETURN 1;' 2>&1)"; then
    echo "OK   neo4j: RETURN 1 riuscita via bolt"
  else
    echo "FAIL neo4j: RETURN 1 fallita: ${out}"
    overall_rc=1
  fi
fi

# qdrant: richiesta REST
if check_running qdrant; then
  http_code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${QDRANT_HOST_PORT}/collections" 2>&1)"
  if [ "${http_code}" = "200" ]; then
    echo "OK   qdrant: GET /collections -> 200"
  else
    echo "FAIL qdrant: GET /collections -> ${http_code} (atteso 200)"
    overall_rc=1
  fi
fi

# opensearch: /_cluster/health status diverso da red
if check_running opensearch; then
  body="$(curl -s "http://localhost:${OPENSEARCH_HOST_PORT}/_cluster/health" 2>&1)"
  status="$(echo "${body}" | jq -r '.status // "sconosciuto"' 2>/dev/null)"
  if [ "${status}" = "green" ] || [ "${status}" = "yellow" ]; then
    echo "OK   opensearch: /_cluster/health status=${status}"
  else
    echo "FAIL opensearch: /_cluster/health status=${status} (risposta: ${body})"
    overall_rc=1
  fi
fi

# minio: /minio/health/live
if check_running minio; then
  http_code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${MINIO_HOST_PORT}/minio/health/live" 2>&1)"
  if [ "${http_code}" = "200" ]; then
    echo "OK   minio: GET /minio/health/live -> 200"
  else
    echo "FAIL minio: GET /minio/health/live -> ${http_code} (atteso 200)"
    overall_rc=1
  fi
fi

# kafka-connect: connector.state=RUNNING e tutti i task in tasks[] RUNNING
# (SPEC-007 §3 scenario 2)
if check_running kafka-connect; then
  status_body="$(curl -s "http://localhost:${KAFKA_CONNECT_HOST_PORT}/connectors/${CONNECTOR_NAME}/status" 2>&1)"
  connector_state="$(echo "${status_body}" | jq -r '.connector.state // "sconosciuto"' 2>/dev/null)"
  failed_tasks="$(echo "${status_body}" | jq -r '[.tasks[]? | select(.state != "RUNNING") | "\(.id):\(.state)"] | join(",")' 2>/dev/null)"
  task_count="$(echo "${status_body}" | jq -r '.tasks | length' 2>/dev/null)"
  if [ "${connector_state}" = "RUNNING" ] && [ -z "${failed_tasks}" ]; then
    echo "OK   kafka-connect: connector ${CONNECTOR_NAME} RUNNING (${task_count} task, tutti RUNNING)"
  else
    echo "FAIL kafka-connect: connector ${CONNECTOR_NAME} state=${connector_state}, task non RUNNING: [${failed_tasks}] (risposta: ${status_body})"
    overall_rc=1
  fi
fi

exit "${overall_rc}"
