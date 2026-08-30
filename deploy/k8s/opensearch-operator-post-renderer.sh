#!/usr/bin/env bash
set -euo pipefail

python3 -c '
import re
import sys

replacements = {
    "opensearchproject/opensearch-operator:2.8.0":
        "opensearchproject/opensearch-operator@sha256:ad86464ea5b1661ea25294058e78b3697286cc6b742df7a543fd96d2de0bc61a",
    "registry.k8s.io/kubebuilder/kube-rbac-proxy:v0.15.0":
        "registry.k8s.io/kubebuilder/kube-rbac-proxy@sha256:d8cc6ffb98190e8dd403bfe67ddcb454e6127d32b87acc237b3e5240f70a20fb",
}
rendered = sys.stdin.read()
for source, target in replacements.items():
    matches = len(re.findall(r"(?m)^\s*-?\s*image:\s*\"?" + re.escape(source) + r"\"?\s*$", rendered))
    if matches != 1:
        raise SystemExit(
            f"OpenSearch operator post-renderer expected exactly one {source} image, found {matches}"
        )
    rendered = rendered.replace(source, target)
for line in rendered.splitlines():
    if re.match(r"^\s*-?\s*image:\s*\S", line) and "@sha256:" not in line:
        raise SystemExit(
            f"OpenSearch operator post-renderer rejected mutable image: {line.strip()}"
        )
sys.stdout.write(rendered)
'
