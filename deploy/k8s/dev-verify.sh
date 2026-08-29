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
for user in kafka-connect embedding-worker sink-graph sink-vector sink-search; do
  "$KUBECTL_BIN" -n data-plane wait --for=condition=Ready "kafkauser/eci-kafka-${user}" --timeout=5m
done
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready kafkatopic --all --timeout=5m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Available deployment/kafka-connect --timeout=10m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Available deployment/redis --timeout=10m
"$KUBECTL_BIN" -n data-plane rollout status statefulset/minio --timeout=10m
"$KUBECTL_BIN" -n query-plane wait --for=condition=Available deployment/opa --timeout=10m
"$KUBECTL_BIN" -n ingress wait --for=condition=Available deployment/keycloak --timeout=10m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready pod -l app.kubernetes.io/name=qdrant --timeout=10m
"$KUBECTL_BIN" -n data-plane rollout status statefulset/neo4j --timeout=15m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready pod -l opster.io/opensearch-cluster=eci-opensearch --timeout=15m
postgres_primary="$("$KUBECTL_BIN" -n data-plane get pod -l cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}')"
test -n "$postgres_primary"
"$KUBECTL_BIN" -n data-plane exec "$postgres_primary" -- psql -U postgres -tAc 'SHOW wal_level' | grep -qx logical

"$KUBECTL_BIN" -n data-plane delete pod eci-connectivity --ignore-not-found --wait=true >/dev/null
"$KUBECTL_BIN" apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: eci-connectivity
  namespace: data-plane
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
            eci-kafka-kafka-bootstrap.data-plane.svc:9093 \
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
          curl -fsS -H 'content-type: application/json' \
            --data '{"input":{"action":"/eci.retrieval.v1.RetrievalEngine/GetNode","subject":{"tenant_id":"smoke-tenant","user_id":"smoke-user","allowed_repos":["smoke-repo"],"acl_groups":["smoke-acl"]}}}' \
            http://opa.query-plane.svc:8181/v1/data/eci/authz/decision | grep -q '"allow":true'
          curl -fsS -H 'content-type: application/json' \
            --data '{"input":{"action":"/eci.retrieval.v1.RetrievalEngine/GetNode","subject":{"user_id":"smoke-user","allowed_repos":["smoke-repo"],"acl_groups":["smoke-acl"]}}}' \
            http://opa.query-plane.svc:8181/v1/data/eci/authz/decision | grep -q '"reason":"missing_tenant"'
          echo 'OPA allow and fail-closed decisions: PASS'
      resources:
        requests: {cpu: 10m, memory: 16Mi}
        limits: {cpu: 100m, memory: 128Mi}
EOF
"$KUBECTL_BIN" -n data-plane wait --for=jsonpath='{.status.phase}'=Succeeded pod/eci-connectivity --timeout=3m
"$KUBECTL_BIN" -n data-plane logs eci-connectivity

# Exercise both sides of the ACL boundary with the exact client identity used
# by embedding-worker. The allowed topic must be describable; a topic owned by
# sink-vector must be denied. The broker image is reused by immutable imageID.
kafka_pod="$($KUBECTL_BIN -n data-plane get pod -l strimzi.io/component-type=kafka -o jsonpath='{.items[0].metadata.name}')"
kafka_image_id="$($KUBECTL_BIN -n data-plane get pod "$kafka_pod" -o jsonpath='{.status.containerStatuses[?(@.name=="kafka")].imageID}')"
kafka_image="${kafka_image_id#docker-pullable://}"
test -n "$kafka_image" && [[ "$kafka_image" == *@sha256:* ]]
"$KUBECTL_BIN" -n data-plane delete pod eci-kafka-acl-smoke --ignore-not-found --wait=true >/dev/null
"$KUBECTL_BIN" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: eci-kafka-acl-smoke
  namespace: data-plane
  labels:
    app.kubernetes.io/name: eci-connectivity
    app.kubernetes.io/part-of: eci
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 1001
    runAsGroup: 1001
    fsGroup: 1001
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: acl-smoke
      image: ${kafka_image}
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      command: [/bin/bash, -ec]
      args:
        - |
          password="\$(cat /etc/kafka-user/user.password)"
          umask 077
          cat >/tmp/client.properties <<PROPERTIES
          security.protocol=SSL
          ssl.truststore.type=PEM
          ssl.truststore.location=/etc/kafka-ca/ca.crt
          ssl.keystore.type=PKCS12
          ssl.keystore.location=/etc/kafka-user/user.p12
          ssl.keystore.password=\${password}
          ssl.key.password=\${password}
          PROPERTIES
          bin/kafka-producer-perf-test.sh \
            --bootstrap-server eci-kafka-kafka-bootstrap.data-plane.svc:9093 \
            --command-config /tmp/client.properties --topic outbox.event.CodeChunk.DLQ \
            --num-records 1 --record-size 22 --throughput -1
          set +e
          bin/kafka-producer-perf-test.sh \
            --bootstrap-server eci-kafka-kafka-bootstrap.data-plane.svc:9093 \
            --command-config /tmp/client.properties --topic outbox.event.CodeEmbedding.DLQ \
            --num-records 1 --record-size 22 --throughput -1 \
            >/tmp/denied.log 2>&1
          set -e
          if ! grep -q 'TopicAuthorizationException' /tmp/denied.log || \
             ! grep -q '^0 records sent' /tmp/denied.log; then
            cat /tmp/denied.log >&2
            echo 'cross-workload Kafka ACL unexpectedly allowed' >&2
            exit 1
          fi
          echo 'Kafka mTLS identity and literal ACL isolation: PASS'
      resources:
        requests: {cpu: 10m, memory: 64Mi}
        limits: {cpu: 500m, memory: 512Mi}
      volumeMounts:
        - {name: kafka-ca, mountPath: /etc/kafka-ca, readOnly: true}
        - {name: kafka-user, mountPath: /etc/kafka-user, readOnly: true}
        - {name: tmp, mountPath: /tmp}
  volumes:
    - name: kafka-ca
      secret:
        secretName: eci-kafka-cluster-ca-cert
        items: [{key: ca.crt, path: ca.crt}]
    - name: kafka-user
      secret:
        secretName: eci-kafka-embedding-worker
        items:
          - {key: user.p12, path: user.p12}
          - {key: user.password, path: user.password}
    - name: tmp
      emptyDir: {}
EOF
"$KUBECTL_BIN" -n data-plane wait --for=jsonpath='{.status.phase}'=Succeeded pod/eci-kafka-acl-smoke --timeout=3m
"$KUBECTL_BIN" -n data-plane logs eci-kafka-acl-smoke
echo "eci-dev connectivity: PASS"
