#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HELM_BIN="${HELM_BIN:-helm}"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
KIND_BIN="${KIND_BIN:-kind}"

for executable in docker "$HELM_BIN" "$KUBECTL_BIN" "$KIND_BIN" openssl base64; do
  command -v "$executable" >/dev/null || { echo "required executable not found: $executable" >&2; exit 1; }
done
docker info >/dev/null

if ! "$KIND_BIN" get clusters | grep -qx eci-dev; then
  "$KIND_BIN" create cluster --name eci-dev --config "$ROOT_DIR/deploy/k8s/kind-config.yaml" --image kindest/node:v1.34.0
fi
"$KUBECTL_BIN" config use-context kind-eci-dev >/dev/null

"$HELM_BIN" template eci "$ROOT_DIR/deploy/k8s/eci-platform" --show-only templates/namespaces.yaml | "$KUBECTL_BIN" apply -f -

if [[ -z "${ECI_DEV_PASSWORD:-}" ]]; then
  if "$KUBECTL_BIN" -n data-plane get secret eci-runtime >/dev/null 2>&1; then
    ECI_DEV_PASSWORD="$("$KUBECTL_BIN" -n data-plane get secret eci-runtime -o jsonpath='{.data.password}' | base64 --decode)"
    test -n "$ECI_DEV_PASSWORD" || { echo "existing eci-runtime secret has no password" >&2; exit 1; }
  else
    ECI_DEV_PASSWORD="$(openssl rand -hex 18)"
  fi
fi
ECI_DEV_TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$ECI_DEV_TMP_DIR"' EXIT
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

# Operator 2.8.0 expects its admin identity in the credentials secret and in
# the supplied security configuration. Generate the bcrypt hash at install
# time so the repository never contains a default or literal dev password.
ECI_OPENSEARCH_HASH="$(printf '%s\n' "$ECI_DEV_PASSWORD" | \
  docker run --rm -i docker.io/opensearchproject/opensearch:3.2.0 /bin/bash -c \
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

echo "eci-dev installed; run task k8s:dev:verify"
