#!/usr/bin/env bash
set -euo pipefail

KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
for executable in "$KUBECTL_BIN" openssl base64; do
  command -v "$executable" >/dev/null || { echo "required executable not found: $executable" >&2; exit 1; }
done
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
"$KUBECTL_BIN" -n data-plane exec deployment/kafka-connect -c kafka-connect -- \
  curl -fsS http://127.0.0.1:8083/connector-plugins | \
  grep -q 'io.debezium.connector.postgresql.PostgresConnector'
connect_pod="$($KUBECTL_BIN -n data-plane get pod -l app.kubernetes.io/name=kafka-connect -o jsonpath='{.items[0].metadata.name}')"
connect_pod_ip="$($KUBECTL_BIN -n data-plane get pod "$connect_pod" -o jsonpath='{.status.podIP}')"
test -n "$connect_pod_ip"
"$KUBECTL_BIN" -n data-plane exec "$connect_pod" -c kafka-connect -- \
  /bin/bash -ec 'if curl -fsS --connect-timeout 2 "http://${1}:8083/connectors" >/dev/null 2>&1; then echo "Kafka Connect REST listens on pod IP" >&2; exit 1; fi' _ "$connect_pod_ip"
echo 'Debezium PostgreSQL connector plugin through loopback-only REST: PASS'
"$KUBECTL_BIN" -n data-plane rollout status statefulset/redis --timeout=10m
"$KUBECTL_BIN" -n data-plane rollout status statefulset/minio --timeout=10m
"$KUBECTL_BIN" -n query-plane wait --for=condition=Available deployment/opa --timeout=10m
"$KUBECTL_BIN" -n ingress wait --for=condition=Available deployment/keycloak --timeout=10m
"$KUBECTL_BIN" -n ingress get secret eci-keycloak-tls -o 'jsonpath={.data.tls\.crt}' | \
  base64 --decode | openssl x509 -checkhost keycloak.ingress.svc -noout
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready pod -l app.kubernetes.io/name=qdrant --timeout=10m
"$KUBECTL_BIN" -n data-plane rollout status statefulset/neo4j --timeout=15m
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready pod -l opster.io/opensearch-cluster=eci-opensearch --timeout=15m
postgres_primary="$("$KUBECTL_BIN" -n data-plane get pod -l cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}')"
test -n "$postgres_primary"
"$KUBECTL_BIN" -n data-plane exec "$postgres_primary" -- psql -U postgres -tAc 'SHOW wal_level' | grep -qx logical
"$KUBECTL_BIN" -n data-plane exec "$postgres_primary" -- psql -U postgres -tAc \
  "SELECT rolreplication AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls FROM pg_roles WHERE rolname='eci_cdc'" | grep -qx t
echo 'PostgreSQL dedicated eci_cdc replication role: PASS'

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
      image: nicolaka/netshoot@sha256:7f08c4aff13ff61a35d30e30c5c1ea8396eac6ab4ce19fd02d5a4b3b5d0d09a2
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      command: [sh, -ec]
      args:
        - |
          for endpoint in \
            eci-postgres-rw.data-plane.svc:5432 \
            eci-kafka-kafka-bootstrap.data-plane.svc:9093 \
            neo4j.data-plane.svc:7687 \
            qdrant.data-plane.svc:6333 \
            qdrant.data-plane.svc:6334 \
            eci-opensearch.data-plane.svc:9200 \
            redis.data-plane.svc:6379 \
            minio.data-plane.svc:9000 \
            opa.query-plane.svc:8181 \
            keycloak.ingress.svc:8443; do
            host=${endpoint%:*}; port=${endpoint##*:}; nc -zvw5 "$host" "$port"
          done
          curl -fsS -H 'content-type: application/json' \
            --data '{"input":{"action":"/eci.retrieval.v1.RetrievalEngine/GetNode","subject":{"tenant_id":"smoke-tenant","user_id":"smoke-user","allowed_repos":["smoke-repo"],"acl_groups":["smoke-acl"]}}}' \
            http://opa.query-plane.svc:8181/v1/data/eci/authz/decision | grep -q '"allow":true'
          curl -fsS -H 'content-type: application/json' \
            --data '{"input":{"action":"/eci.retrieval.v1.RetrievalEngine/GetNode","subject":{"user_id":"smoke-user","allowed_repos":["smoke-repo"],"acl_groups":["smoke-acl"]}}}' \
            http://opa.query-plane.svc:8181/v1/data/eci/authz/decision | grep -q '"reason":"missing_tenant"'
          echo 'OPA allow and fail-closed decisions: PASS'
          curl -fsS --cacert /etc/eci/keycloak/ca.crt \
            https://keycloak.ingress.svc:8443/realms/master/.well-known/openid-configuration | \
            grep -q '"issuer":"https://keycloak.ingress.svc:8443/realms/master"'
          echo 'Keycloak HTTPS discovery with hostname-bound trust: PASS'
      resources:
        requests: {cpu: 10m, memory: 16Mi}
        limits: {cpu: 100m, memory: 128Mi}
      volumeMounts:
        - {name: keycloak-ca, mountPath: /etc/eci/keycloak, readOnly: true}
  volumes:
    - name: keycloak-ca
      configMap: {name: eci-keycloak-ca}
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
            --command-config /tmp/client.properties --topic outbox.event.CodeChunk.retry.embedding-worker \
            --num-records 1 --record-size 22 --throughput -1
          bin/kafka-console-consumer.sh \
            --bootstrap-server eci-kafka-kafka-bootstrap.data-plane.svc:9093 \
            --consumer.config /tmp/client.properties --topic outbox.event.CodeChunk.retry.embedding-worker \
            --group embedding-worker --from-beginning --max-messages 1 --timeout-ms 30000 \
            >/tmp/consumed.log 2>/tmp/consumer.log
          test "\$(wc -c </tmp/consumed.log)" -ge 22
          grep -q 'Processed a total of 1 messages' /tmp/consumer.log
          bin/kafka-consumer-groups.sh \
            --bootstrap-server eci-kafka-kafka-bootstrap.data-plane.svc:9093 \
            --command-config /tmp/client.properties --group embedding-worker --describe \
            | grep -q 'outbox.event.CodeChunk.retry.embedding-worker'
          echo 'Kafka mTLS topic metadata, consume and group authorization: PASS'
          set +e
          bin/kafka-producer-perf-test.sh \
            --bootstrap-server eci-kafka-kafka-bootstrap.data-plane.svc:9093 \
            --command-config /tmp/client.properties --topic outbox.event.CodeChunk \
            --num-records 1 --record-size 22 --throughput -1 \
            >/tmp/denied.log 2>&1
          set -e
          if ! grep -q 'TopicAuthorizationException' /tmp/denied.log || \
             ! grep -q '^0 records sent' /tmp/denied.log; then
            cat /tmp/denied.log >&2
            echo 'consumer unexpectedly allowed to forge a primary Kafka event' >&2
            exit 1
          fi
          echo 'Kafka mTLS retry identity and primary-topic provenance: PASS'
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
