#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HELM_BIN="${HELM_BIN:-helm}"
KUBECONFORM_BIN="${KUBECONFORM_BIN:-kubeconform}"

for executable in "$HELM_BIN" "$KUBECONFORM_BIN" python3; do
  command -v "$executable" >/dev/null || { echo "required executable not found: $executable" >&2; exit 1; }
done
python3 -c 'import yaml' 2>/dev/null || { echo "required Python package missing: PyYAML" >&2; exit 1; }

TMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "${TMP_DIR}"' EXIT

"$HELM_BIN" lint "$ROOT_DIR/deploy/k8s/eci-platform"
for profile in standard dev; do
  args=()
  if [[ "$profile" == dev ]]; then args+=(--values "$ROOT_DIR/deploy/k8s/eci-platform/values-dev.yaml"); fi
  "$HELM_BIN" template eci "$ROOT_DIR/deploy/k8s/eci-platform" --namespace query-plane "${args[@]}" >"$TMP_DIR/$profile.yaml"
  python3 "$ROOT_DIR/scripts/k8s-policy.py" "$TMP_DIR/$profile.yaml" "$ROOT_DIR/deploy/k8s/schemas"
  "$KUBECONFORM_BIN" -strict -summary -kubernetes-version 1.34.0 \
    -schema-location default \
    -schema-location "$ROOT_DIR/deploy/k8s/schemas/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json" \
    "$TMP_DIR/$profile.yaml"
done

# Render the opt-in application topology with explicitly synthetic digest-shaped
# references. This validates template/schema/policy completeness only; it is
# never deployment or image-publication evidence.
catalog_args=(
  --set applications.enabled=true
  --set-string routing.oidcIssuerEgressCIDRs[0]=192.0.2.10/32
)
test_digest="$(printf '0123456789abcdef%.0s' {1..4})"
for application in \
  api-gateway retrieval-engine llm-gateway semantic-cache ingestion embedding-worker sink-graph \
  sink-vector sink-search gds-impact; do
  catalog_args+=(--set-string "global.imageReferences.${application}=registry.example.invalid/eci-test/${application}@sha256:${test_digest}")
done
"$HELM_BIN" template eci "$ROOT_DIR/deploy/k8s/eci-platform" --namespace query-plane \
  "${catalog_args[@]}" >"$TMP_DIR/application-catalog.yaml"
python3 "$ROOT_DIR/scripts/k8s-policy.py" "$TMP_DIR/application-catalog.yaml" "$ROOT_DIR/deploy/k8s/schemas"
"$KUBECONFORM_BIN" -strict -summary -kubernetes-version 1.34.0 \
  -schema-location default \
  -schema-location "$ROOT_DIR/deploy/k8s/schemas/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json" \
  "$TMP_DIR/application-catalog.yaml"

HELM_BIN="$HELM_BIN" python3 -m unittest tests.k8s.test_platform
