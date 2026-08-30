#!/usr/bin/env bash
set -euo pipefail

HELM_BIN="${HELM_BIN:-helm}"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

require() { command -v "$1" >/dev/null || { echo "required executable not found: $1" >&2; exit 1; }; }
require "$HELM_BIN"
require "$KUBECTL_BIN"
require curl
require sha256sum
require python3

require_digest_image() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]]; then
    echo "$name must be an immutable name@sha256:<64 lowercase hex> reference" >&2
    exit 1
  fi
}

CHART_DIR="$(mktemp -d)"
trap 'rm -f "$CHART_DIR"/*.tgz; rmdir "$CHART_DIR"' EXIT

download_chart() {
  local filename="$1"
  local url="$2"
  local expected_sha256="$3"
  local destination="$CHART_DIR/$filename"
  curl --proto '=https' --tlsv1.2 -fsSL "$url" -o "$destination"
  printf '%s  %s\n' "$expected_sha256" "$destination" | sha256sum -c -
}

download_chart strimzi-1.2.0.tgz \
  https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.2.0/strimzi-kafka-operator-helm-3-chart-1.2.0.tgz \
  0f8a50b2f19bd99482f9fd6e17cf42902f72f9e594a136813ac3f0b7af422efd
download_chart cloudnative-pg-0.29.0.tgz \
  https://github.com/cloudnative-pg/charts/releases/download/cloudnative-pg-v0.29.0/cloudnative-pg-0.29.0.tgz \
  668e065ff53508d58238788fd35b355a925060843629a951df0e6a9362e6d32f
download_chart opensearch-operator-2.8.0.tgz \
  https://github.com/opensearch-project/opensearch-k8s-operator/releases/download/opensearch-operator-2.8.0/opensearch-operator-2.8.0.tgz \
  f289e27e553c45b55e20952c78971b19a1b5defe9f89bea1f6910f3ee3da81eb
download_chart neo4j-5.26.30.tgz \
  https://helm.neo4j.com/neo4j/neo4j-5.26.30.tgz \
  b7dd64379ae449b48f9249c94ae8c8d2a48223e74d9ecd6760beef274bb37c78
download_chart qdrant-1.19.0.tgz \
  https://github.com/qdrant/qdrant-helm/releases/download/qdrant-1.19.0/qdrant-1.19.0.tgz \
  131236b52d7959ee600f86ba43e48e88ff715b12a26451871fe57c2ba5809f0b

if ! "$KUBECTL_BIN" get namespace data-plane >/dev/null 2>&1; then
  "$KUBECTL_BIN" create namespace data-plane
fi

"$HELM_BIN" upgrade --install strimzi \
  "$CHART_DIR/strimzi-1.2.0.tgz" \
  --namespace data-plane --wait --atomic --timeout 5m

"$HELM_BIN" upgrade --install cloudnative-pg "$CHART_DIR/cloudnative-pg-0.29.0.tgz" \
  --namespace data-plane --wait --atomic --timeout 5m
"$HELM_BIN" upgrade --install opensearch-operator "$CHART_DIR/opensearch-operator-2.8.0.tgz" \
  --namespace data-plane \
  --set kubeRbacProxy.image.repository=registry.k8s.io/kubebuilder/kube-rbac-proxy \
  --set kubeRbacProxy.image.tag=v0.15.0 \
  --set securityContext.runAsNonRoot=true \
  --set securityContext.seccompProfile.type=RuntimeDefault \
  --set manager.securityContext.allowPrivilegeEscalation=false \
  --set manager.securityContext.readOnlyRootFilesystem=true \
  --set 'manager.securityContext.capabilities.drop[0]=ALL' \
  --set 'manager.extraEnv[0].name=SKIP_INIT_CONTAINER' \
  --set-string 'manager.extraEnv[0].value=true' \
  --post-renderer "$ROOT_DIR/deploy/k8s/opensearch-operator-post-renderer.sh" \
  --wait --atomic --timeout 5m

if [[ "${ECI_K8S_PROFILE:-production-like}" == dev ]]; then
  "$HELM_BIN" upgrade --install neo4j "$CHART_DIR/neo4j-5.26.30.tgz" \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/neo4j-dev.yaml" \
    --wait --atomic --timeout 10m
  "$HELM_BIN" upgrade --install qdrant "$CHART_DIR/qdrant-1.19.0.tgz" \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/qdrant-dev.yaml" \
    --post-renderer "$ROOT_DIR/deploy/k8s/qdrant-post-renderer.sh" \
    --wait --atomic --timeout 10m
else
  case "${NEO4J_ACCEPT_LICENSE_AGREEMENT:-}" in
    yes|eval) ;;
    *) echo "production-like Neo4j requires NEO4J_ACCEPT_LICENSE_AGREEMENT=yes or eval" >&2; exit 1 ;;
  esac
  : "${NEO4J_GDS_IMAGE:?production-like Neo4j requires a published NEO4J_GDS_IMAGE}"
  require_digest_image NEO4J_GDS_IMAGE "$NEO4J_GDS_IMAGE"
  # The official chart models one Neo4j server per Helm release. Install all
  # three initial primaries before waiting, otherwise the first member cannot
  # satisfy minimumClusterSize=3. Readiness remains bounded below.
  for member in 1 2 3; do
    "$HELM_BIN" upgrade --install "neo4j-core-${member}" "$CHART_DIR/neo4j-5.26.30.tgz" \
      --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/neo4j-core.yaml" \
      --set-string neo4j.acceptLicenseAgreement="$NEO4J_ACCEPT_LICENSE_AGREEMENT"
  done
  for member in 1 2 3; do
    "$KUBECTL_BIN" -n data-plane rollout status "statefulset/neo4j-core-${member}" --timeout=15m
  done
  "$HELM_BIN" upgrade --install neo4j-gds "$CHART_DIR/neo4j-5.26.30.tgz" \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/neo4j-gds.yaml" \
    --set-string image.customImage="$NEO4J_GDS_IMAGE" \
    --set-string neo4j.acceptLicenseAgreement="$NEO4J_ACCEPT_LICENSE_AGREEMENT" \
    --wait --atomic --timeout 15m
  "$HELM_BIN" upgrade --install qdrant "$CHART_DIR/qdrant-1.19.0.tgz" \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/qdrant.yaml" \
    --post-renderer "$ROOT_DIR/deploy/k8s/qdrant-post-renderer.sh" \
    --wait --atomic --timeout 15m
fi
