#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/deploy/k8s/lib/dev-password.sh"
source "$ROOT_DIR/deploy/k8s/lib/kind-cluster.sh"
source "$ROOT_DIR/deploy/k8s/lib/tls-keypair.sh"
HELM_BIN="${HELM_BIN:-helm}"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
KIND_BIN="${KIND_BIN:-kind}"
DOCKER_BIN="${DOCKER_BIN:-docker}"

for executable in "$DOCKER_BIN" "$HELM_BIN" "$KUBECTL_BIN" "$KIND_BIN" openssl base64; do
  command -v "$executable" >/dev/null || { echo "required executable not found: $executable" >&2; exit 1; }
done
"$DOCKER_BIN" info >/dev/null
umask 077
ECI_DEV_TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$ECI_DEV_TMP_DIR"' EXIT

if ! "$KIND_BIN" get clusters | grep -qx eci-dev; then
  "$KIND_BIN" create cluster --name eci-dev --config "$ROOT_DIR/deploy/k8s/kind-config.yaml" --image "$ECI_KIND_NODE_IMAGE"
fi
eci_verify_existing_kind_cluster "$ECI_KIND_NODE_IMAGE" "$ECI_KIND_KUBERNETES_VERSION" "$ECI_DEV_TMP_DIR/kubeconfig"
export KUBECONFIG="$ECI_DEV_TMP_DIR/kubeconfig"

"$HELM_BIN" template eci "$ROOT_DIR/deploy/k8s/eci-platform" --show-only templates/namespaces.yaml | "$KUBECTL_BIN" apply -f -

ECI_STORED_DEV_PASSWORD=""
if "$KUBECTL_BIN" -n data-plane get secret eci-runtime >/dev/null 2>&1; then
  ECI_STORED_DEV_PASSWORD="$("$KUBECTL_BIN" -n data-plane get secret eci-runtime -o jsonpath='{.data.password}' | base64 --decode)"
  test -n "$ECI_STORED_DEV_PASSWORD" || { echo "existing eci-runtime secret has no password" >&2; exit 1; }
fi
ECI_DEV_PASSWORD="$(eci_resolve_dev_password "$ECI_STORED_DEV_PASSWORD" "${ECI_DEV_PASSWORD:-}")"
unset ECI_STORED_DEV_PASSWORD
if "$KUBECTL_BIN" -n data-plane get secret eci-postgres-cdc >/dev/null 2>&1; then
  ECI_CDC_PASSWORD="$("$KUBECTL_BIN" -n data-plane get secret eci-postgres-cdc -o jsonpath='{.data.password}' | base64 --decode)"
  test -n "$ECI_CDC_PASSWORD" || { echo "existing eci-postgres-cdc secret has no password" >&2; exit 1; }
else
  ECI_CDC_PASSWORD="$(openssl rand -hex 18)"
fi
printf '%s\n' \
  'username=eci' \
  "password=$ECI_DEV_PASSWORD" \
  "NEO4J_AUTH=neo4j/$ECI_DEV_PASSWORD" \
  "redis-password=$ECI_DEV_PASSWORD" \
  'minio-user=eci-dev' \
  "minio-password=$ECI_DEV_PASSWORD" \
  'keycloak-user=eci-admin' \
  "keycloak-password=$ECI_DEV_PASSWORD" \
  >"$ECI_DEV_TMP_DIR/runtime.env"
chmod 0600 "$ECI_DEV_TMP_DIR/runtime.env"
for namespace in data-plane query-plane ingestion-plane ingress; do
  "$KUBECTL_BIN" -n "$namespace" create secret generic eci-runtime \
    --from-env-file="$ECI_DEV_TMP_DIR/runtime.env" \
    --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
done

# MinIO is TLS-only in both profiles. Reuse a valid hostname-bound certificate
# for the disposable cluster, or generate it locally; only the public CA is
# copied to the ingestion namespace.
ECI_MINIO_TLS_ROTATED=false
if "$KUBECTL_BIN" -n data-plane get secret eci-minio-tls >/dev/null 2>&1; then
  "$KUBECTL_BIN" -n data-plane get secret eci-minio-tls \
    -o 'jsonpath={.data.tls\.crt}' | base64 --decode >"$ECI_DEV_TMP_DIR/minio.crt"
  "$KUBECTL_BIN" -n data-plane get secret eci-minio-tls \
    -o 'jsonpath={.data.ca\.crt}' | base64 --decode >"$ECI_DEV_TMP_DIR/minio-ca.crt"
  "$KUBECTL_BIN" -n data-plane get secret eci-minio-tls \
    -o 'jsonpath={.data.tls\.key}' | base64 --decode >"$ECI_DEV_TMP_DIR/minio.key"
  if ! openssl x509 -in "$ECI_DEV_TMP_DIR/minio.crt" -checkend 86400 -noout >/dev/null 2>&1 || \
     ! openssl x509 -in "$ECI_DEV_TMP_DIR/minio.crt" -checkhost minio.data-plane.svc.cluster.local -noout >/dev/null 2>&1 || \
     ! openssl x509 -in "$ECI_DEV_TMP_DIR/minio.crt" -noout -text | grep -q 'CA:FALSE' || \
     ! openssl x509 -in "$ECI_DEV_TMP_DIR/minio-ca.crt" -checkend 86400 -noout >/dev/null 2>&1 || \
     ! openssl x509 -in "$ECI_DEV_TMP_DIR/minio-ca.crt" -noout -text | grep -q 'CA:TRUE' || \
     ! openssl verify -CAfile "$ECI_DEV_TMP_DIR/minio-ca.crt" "$ECI_DEV_TMP_DIR/minio.crt" >/dev/null 2>&1 || \
     ! eci_tls_private_key_matches_certificate "$ECI_DEV_TMP_DIR/minio.crt" "$ECI_DEV_TMP_DIR/minio.key"; then
    ECI_MINIO_TLS_ROTATED=true
  fi
else
  ECI_MINIO_TLS_ROTATED=true
fi
if [[ "$ECI_MINIO_TLS_ROTATED" == true ]]; then
  openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 365 \
    -subj '/CN=eci-minio-dev-ca' \
    -addext 'basicConstraints=critical,CA:TRUE,pathlen:0' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' \
    -keyout "$ECI_DEV_TMP_DIR/minio-ca.key" -out "$ECI_DEV_TMP_DIR/minio-ca.crt" >/dev/null 2>&1
  openssl req -new -newkey rsa:2048 -sha256 -nodes \
    -subj '/CN=minio.data-plane.svc' \
    -keyout "$ECI_DEV_TMP_DIR/minio.key" -out "$ECI_DEV_TMP_DIR/minio.csr" >/dev/null 2>&1
  printf '%s\n' \
    'basicConstraints=critical,CA:FALSE' \
    'keyUsage=critical,digitalSignature,keyEncipherment' \
    'extendedKeyUsage=serverAuth' \
    'subjectAltName=DNS:minio,DNS:minio.data-plane.svc,DNS:minio.data-plane.svc.cluster.local,DNS:*.minio-headless.data-plane.svc.cluster.local' \
    >"$ECI_DEV_TMP_DIR/minio-leaf.ext"
  openssl x509 -req -in "$ECI_DEV_TMP_DIR/minio.csr" \
    -CA "$ECI_DEV_TMP_DIR/minio-ca.crt" -CAkey "$ECI_DEV_TMP_DIR/minio-ca.key" \
    -CAcreateserial -days 365 -sha256 -extfile "$ECI_DEV_TMP_DIR/minio-leaf.ext" \
    -out "$ECI_DEV_TMP_DIR/minio.crt" >/dev/null 2>&1
  chmod 0600 "$ECI_DEV_TMP_DIR/minio-ca.key" "$ECI_DEV_TMP_DIR/minio-ca.crt" \
    "$ECI_DEV_TMP_DIR/minio.key" "$ECI_DEV_TMP_DIR/minio.crt"
  "$KUBECTL_BIN" -n data-plane create secret generic eci-minio-tls \
    --from-file=tls.crt="$ECI_DEV_TMP_DIR/minio.crt" \
    --from-file=tls.key="$ECI_DEV_TMP_DIR/minio.key" \
    --from-file=ca.crt="$ECI_DEV_TMP_DIR/minio-ca.crt" \
    --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
fi
"$KUBECTL_BIN" -n ingestion-plane create secret generic eci-minio-ca \
  --from-file=ca.crt="$ECI_DEV_TMP_DIR/minio-ca.crt" \
  --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null

# The in-cluster development issuer is HTTPS-only. Reuse a still-valid
# hostname-bound certificate so repeated installs do not rotate trust
# unexpectedly; generate it locally when the disposable cluster needs one.
ECI_KEYCLOAK_TLS_ROTATED=false
if "$KUBECTL_BIN" -n ingress get secret eci-keycloak-tls >/dev/null 2>&1; then
  "$KUBECTL_BIN" -n ingress get secret eci-keycloak-tls \
    -o 'jsonpath={.data.tls\.crt}' | base64 --decode >"$ECI_DEV_TMP_DIR/keycloak.crt"
  if ! openssl x509 -in "$ECI_DEV_TMP_DIR/keycloak.crt" -checkend 86400 -noout >/dev/null 2>&1 || \
     ! openssl x509 -in "$ECI_DEV_TMP_DIR/keycloak.crt" -checkhost keycloak.ingress.svc -noout >/dev/null 2>&1; then
    ECI_KEYCLOAK_TLS_ROTATED=true
  fi
else
  ECI_KEYCLOAK_TLS_ROTATED=true
fi
if [[ "$ECI_KEYCLOAK_TLS_ROTATED" == true ]]; then
  openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 365 \
    -subj '/CN=keycloak.ingress.svc' \
    -addext 'subjectAltName=DNS:keycloak.ingress.svc,DNS:keycloak.ingress.svc.cluster.local' \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -keyout "$ECI_DEV_TMP_DIR/keycloak.key" -out "$ECI_DEV_TMP_DIR/keycloak.crt" >/dev/null 2>&1
  chmod 0600 "$ECI_DEV_TMP_DIR/keycloak.key" "$ECI_DEV_TMP_DIR/keycloak.crt"
  "$KUBECTL_BIN" -n ingress create secret tls eci-keycloak-tls \
    --cert="$ECI_DEV_TMP_DIR/keycloak.crt" --key="$ECI_DEV_TMP_DIR/keycloak.key" \
    --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
fi
# Connectivity smoke needs only the public certificate. Never copy tls.key.
"$KUBECTL_BIN" -n data-plane create configmap eci-keycloak-ca \
  --from-file=ca.crt="$ECI_DEV_TMP_DIR/keycloak.crt" \
  --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
printf '%s\n' 'username=eci_cdc' "password=$ECI_CDC_PASSWORD" \
  >"$ECI_DEV_TMP_DIR/postgres-cdc.env"
chmod 0600 "$ECI_DEV_TMP_DIR/postgres-cdc.env"
"$KUBECTL_BIN" -n data-plane create secret generic eci-postgres-cdc \
  --from-env-file="$ECI_DEV_TMP_DIR/postgres-cdc.env" \
  --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
unset ECI_CDC_PASSWORD

# Operator 2.8.0 expects its admin identity in the credentials secret and in
# the supplied security configuration. Generate the bcrypt hash at install
# time so the repository never contains a default or literal dev password.
ECI_OPENSEARCH_HASH="$(printf '%s\n' "$ECI_DEV_PASSWORD" | \
  "$DOCKER_BIN" run --rm -i docker.io/opensearchproject/opensearch@sha256:23297b8d8545e129dd58c254ed08d786dc552410ba772983ad2af31048d2f04b /bin/bash -c \
  'IFS= read -r password; exec /usr/share/opensearch/plugins/opensearch-security/tools/hash.sh -p "$password"' | \
  tail -n 1)"
test -n "$ECI_OPENSEARCH_HASH"
printf '%s\n' \
  '_meta:' \
  '  type: "internalusers"' \
  '  config_version: 2' \
  'admin:' \
  "  hash: \"$ECI_OPENSEARCH_HASH\"" \
  '  reserved: true' \
  '  backend_roles:' \
  '    - "admin"' \
  '  description: "ECI development administrator"' \
  >"$ECI_DEV_TMP_DIR/internal_users.yml"
printf '%s\n' 'username=admin' "password=$ECI_DEV_PASSWORD" \
  >"$ECI_DEV_TMP_DIR/opensearch-admin.env"
chmod 0600 "$ECI_DEV_TMP_DIR/opensearch-admin.env" "$ECI_DEV_TMP_DIR/internal_users.yml"
"$KUBECTL_BIN" -n data-plane create secret generic eci-opensearch-admin \
  --from-env-file="$ECI_DEV_TMP_DIR/opensearch-admin.env" \
  --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
"$KUBECTL_BIN" -n data-plane create secret generic eci-opensearch-security-config \
  --from-file=internal_users.yml="$ECI_DEV_TMP_DIR/internal_users.yml" \
  --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
unset ECI_OPENSEARCH_HASH
unset ECI_DEV_PASSWORD

ECI_K8S_PROFILE=dev HELM_BIN="$HELM_BIN" KUBECTL_BIN="$KUBECTL_BIN" "$ROOT_DIR/deploy/k8s/install-operators.sh"
"$HELM_BIN" upgrade --install eci "$ROOT_DIR/deploy/k8s/eci-platform" \
  --namespace query-plane --values "$ROOT_DIR/deploy/k8s/eci-platform/values-dev.yaml" \
  --wait --atomic --timeout 20m

# CloudNativePG owns the PostgreSQL server CA. Copy only ca.crt to the worker
# namespace after the Cluster has reconciled; never copy ca.key.
"$KUBECTL_BIN" -n data-plane wait --for=condition=Ready cluster/eci-postgres --timeout=10m
"$KUBECTL_BIN" -n data-plane get secret eci-postgres-ca \
  -o 'jsonpath={.data.ca\.crt}' | base64 --decode >"$ECI_DEV_TMP_DIR/postgres-ca.crt"
test -s "$ECI_DEV_TMP_DIR/postgres-ca.crt"
chmod 0600 "$ECI_DEV_TMP_DIR/postgres-ca.crt"
"$KUBECTL_BIN" -n ingestion-plane create secret generic eci-postgres-ca \
  --from-file=ca.crt="$ECI_DEV_TMP_DIR/postgres-ca.crt" \
  --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
if [[ "$ECI_KEYCLOAK_TLS_ROTATED" == true ]]; then
  "$KUBECTL_BIN" -n ingress rollout restart deployment/keycloak >/dev/null
  "$KUBECTL_BIN" -n ingress rollout status deployment/keycloak --timeout=10m
fi

# Strimzi owns client private keys in data-plane. The dev bootstrap copies only
# each workload's own public CA/certificate/private key into ingestion-plane;
# it never copies a CA private key or shares one client identity across apps.
for workload in ingestion embedding-worker sink-graph sink-vector sink-search; do
  user="eci-kafka-${workload}"
  "$KUBECTL_BIN" -n data-plane wait --for=condition=Ready "kafkauser/${user}" --timeout=5m
  identity_dir="$ECI_DEV_TMP_DIR/$user"
  mkdir -m 0700 "$identity_dir"
  "$KUBECTL_BIN" -n data-plane get secret eci-kafka-cluster-ca-cert \
    -o 'jsonpath={.data.ca\.crt}' | base64 --decode >"$identity_dir/ca.crt"
  test -s "$identity_dir/ca.crt"
  chmod 0600 "$identity_dir/ca.crt"
  for key in user.crt user.key; do
    json_key="${key//./\\.}"
    "$KUBECTL_BIN" -n data-plane get secret "$user" -o "jsonpath={.data.${json_key}}" | \
      base64 --decode >"$identity_dir/$key"
    test -s "$identity_dir/$key"
    chmod 0600 "$identity_dir/$key"
  done
  "$KUBECTL_BIN" -n ingestion-plane create secret generic "$user" \
    --from-file=ca.crt="$identity_dir/ca.crt" \
    --from-file=user.crt="$identity_dir/user.crt" \
    --from-file=user.key="$identity_dir/user.key" \
    --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f - >/dev/null
done

echo "eci-dev installed; run task k8s:dev:verify"
