#!/usr/bin/env bash
set -euo pipefail

KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
command -v "$KUBECTL_BIN" >/dev/null || { echo "required executable not found: $KUBECTL_BIN" >&2; exit 1; }
[[ "$($KUBECTL_BIN config current-context)" == kind-eci-dev ]] || { echo "refusing verification outside kind-eci-dev" >&2; exit 1; }

diagnostics() {
  "$KUBECTL_BIN" get pods -A -o wide || true
  "$KUBECTL_BIN" get events -A --sort-by=.lastTimestamp | tail -n 100 || true
}
trap diagnostics ERR

"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready cluster/eci-postgres --timeout=15m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready kafka/eci-kafka --timeout=15m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Available deployment/kafka-connect --timeout=10m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Available deployment/redis deployment/minio --timeout=10m
"$KUBECTL_BIN" -n query-plane wait --for=condition=Available deployment/opa --timeout=10m
"$KUBECTL_BIN" -n ingress wait --for=condition=Available deployment/keycloak --timeout=10m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready pod -l app.kubernetes.io/name=qdrant --timeout=10m
"$KUBECTL_BIN" -n data-plane rollout status statefulset/neo4j --timeout=15m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready pod -l opster.io/opensearch-cluster=eci-opensearch --timeout=15m
postgres_primary="$("$KUBECTL_BIN" -n data-plane get pod -l cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}')"
test -n "$postgres_primary"
"$KUBECTL_BIN" -n data-plane exec "$postgres_primary" -- psql -U postgres -tAc 'SHOW wal_level' | grep -qx logical

"$KUBECTL_BIN" -n observability delete pod eci-connectivity --ignore-not-found --wait=true >/dev/null
"$KUBECTL_BIN" apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: eci-connectivity
  namespace: observability
  labels:
    app.kubernetes.io/name: eci-connectivity
    app.kubernetes.io/part-of: eci
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: eci-connectivity
      image: nicolaka/netshoot:v0.14
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      command: [sh, -ec]
      args:
        - |
          for endpoint in \
            eci-postgres-rw.data-plane.svc:5432 \
            eci-kafka-kafka-bootstrap.data-plane.svc:9092 \
            kafka-connect.data-plane.svc:8083 \
            neo4j.data-plane.svc:7687 \
            qdrant.data-plane.svc:6333 \
            qdrant.data-plane.svc:6334 \
            eci-opensearch.data-plane.svc:9200 \
            redis.data-plane.svc:6379 \
            minio.data-plane.svc:9000 \
            opa.query-plane.svc:8181 \
            keycloak.ingress.svc:8080; do
            host=${endpoint%:*}; port=${endpoint##*:}; nc -zvw5 "$host" "$port"
          done
          curl -fsS http://kafka-connect.data-plane.svc:8083/connector-plugins | \
            grep -q 'io.debezium.connector.postgresql.PostgresConnector'
          echo 'Debezium PostgreSQL connector plugin: PASS'
      resources:
        requests: {cpu: 10m, memory: 16Mi}
        limits: {cpu: 100m, memory: 128Mi}
EOF
"$KUBECTL_BIN" -n observability wait --for=jsonpath='{.status.phase}'=Succeeded pod/eci-connectivity --timeout=3m
"$KUBECTL_BIN" -n observability logs eci-connectivity
echo "eci-dev connectivity: PASS"
