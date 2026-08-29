#!/usr/bin/env bash
set -euo pipefail

HELM_BIN="${HELM_BIN:-helm}"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

require() { command -v "$1" >/dev/null || { echo "required executable not found: $1" >&2; exit 1; }; }
require "$HELM_BIN"
require "$KUBECTL_BIN"

if ! "$KUBECTL_BIN" get namespace data-plane >/dev/null 2>&1; then
  "$KUBECTL_BIN" create namespace data-plane
fi

"$HELM_BIN" upgrade --install strimzi \
  "https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.2.0/strimzi-kafka-operator-helm-3-chart-1.2.0.tgz" \
  --namespace data-plane --wait --atomic --timeout 5m

"$HELM_BIN" repo add cnpg https://cloudnative-pg.github.io/charts --force-update
"$HELM_BIN" repo add opensearch-operator https://opensearch-project.github.io/opensearch-k8s-operator/ --force-update
"$HELM_BIN" repo add neo4j https://helm.neo4j.com/neo4j --force-update
"$HELM_BIN" repo add qdrant https://qdrant.github.io/qdrant-helm --force-update
"$HELM_BIN" repo update

"$HELM_BIN" upgrade --install cloudnative-pg cnpg/cloudnative-pg \
  --version 0.29.0 --namespace data-plane --wait --atomic --timeout 5m
"$HELM_BIN" upgrade --install opensearch-operator opensearch-operator/opensearch-operator \
  --version 2.8.0 --namespace data-plane \
  --set kubeRbacProxy.image.repository=registry.k8s.io/kubebuilder/kube-rbac-proxy \
  --set kubeRbacProxy.image.tag=v0.15.0 \
  --set securityContext.runAsNonRoot=true \
  --set securityContext.seccompProfile.type=RuntimeDefault \
  --set manager.securityContext.allowPrivilegeEscalation=false \
  --set manager.securityContext.readOnlyRootFilesystem=true \
  --set 'manager.securityContext.capabilities.drop[0]=ALL' \
  --set 'manager.extraEnv[0].name=SKIP_INIT_CONTAINER' \
  --set-string 'manager.extraEnv[0].value=true' \
  --wait --atomic --timeout 5m

if [[ "${ECI_K8S_PROFILE:-production-like}" == dev ]]; then
  "$HELM_BIN" upgrade --install neo4j neo4j/neo4j --version 5.26.30 \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/neo4j-dev.yaml" \
    --wait --atomic --timeout 10m
  "$HELM_BIN" upgrade --install qdrant qdrant/qdrant --version 1.19.0 \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/qdrant-dev.yaml" \
    --wait --atomic --timeout 10m
else
  case "${NEO4J_ACCEPT_LICENSE_AGREEMENT:-}" in
    yes|eval) ;;
    *) echo "production-like Neo4j requires NEO4J_ACCEPT_LICENSE_AGREEMENT=yes or eval" >&2; exit 1 ;;
  esac
  # The official chart models one Neo4j server per Helm release. Install all
  # three initial primaries before waiting, otherwise the first member cannot
  # satisfy minimumClusterSize=3. Readiness remains bounded below.
  for member in 1 2 3; do
    "$HELM_BIN" upgrade --install "neo4j-core-${member}" neo4j/neo4j --version 5.26.30 \
      --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/neo4j-core.yaml" \
      --set-string neo4j.acceptLicenseAgreement="$NEO4J_ACCEPT_LICENSE_AGREEMENT"
  done
  for member in 1 2 3; do
    "$KUBECTL_BIN" -n data-plane rollout status "statefulset/neo4j-core-${member}" --timeout=15m
  done
  "$HELM_BIN" upgrade --install neo4j-gds neo4j/neo4j --version 5.26.30 \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/neo4j-gds.yaml" \
    --set-string neo4j.acceptLicenseAgreement="$NEO4J_ACCEPT_LICENSE_AGREEMENT" \
    --wait --atomic --timeout 15m
  "$HELM_BIN" upgrade --install qdrant qdrant/qdrant --version 1.19.0 \
    --namespace data-plane -f "$ROOT_DIR/deploy/k8s/vendor-values/qdrant.yaml" \
    --wait --atomic --timeout 15m
fi
