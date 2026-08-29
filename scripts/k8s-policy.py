#!/usr/bin/env python3
"""Fail-closed static policy checks for rendered ECI Kubernetes resources."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml


SENSITIVE_ENV = re.compile(r"(?:PASSWORD|TOKEN|SECRET|API_?KEY|DSN)$", re.IGNORECASE)
PLACEHOLDER = re.compile(r"(^|\s)(sleep|tail\s+-f)(\s|$)", re.IGNORECASE)
CUSTOM_RESOURCES = {
    ("kafka.strimzi.io/v1", "Kafka"),
    ("kafka.strimzi.io/v1", "KafkaNodePool"),
    ("postgresql.cnpg.io/v1", "Cluster"),
    ("opensearch.opster.io/v1", "OpenSearchCluster"),
}


def fail(message: str) -> None:
    raise SystemExit(f"k8s policy violation: {message}")


def containers(obj: dict) -> list[dict]:
    kind = obj.get("kind")
    if kind in {"Deployment", "StatefulSet", "DaemonSet", "Job"}:
        pod = obj.get("spec", {}).get("template", {}).get("spec", {})
        return (pod.get("initContainers") or []) + (pod.get("containers") or [])
    if kind == "CronJob":
        pod = obj.get("spec", {}).get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec", {})
        return (pod.get("initContainers") or []) + (pod.get("containers") or [])
    return []


def image_is_pinned(image: str) -> bool:
    if not image or image.endswith(":latest") or image.endswith(":dev"):
        return False
    final = image.rsplit("/", 1)[-1]
    return "@sha256:" in image or ":" in final


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: k8s-policy.py RENDERED_YAML SCHEMA_ROOT")
    rendered, schema_root = Path(sys.argv[1]), Path(sys.argv[2])
    docs = [doc for doc in yaml.safe_load_all(rendered.read_text()) if isinstance(doc, dict)]
    if not docs:
        fail("render is empty")

    identities: set[tuple[str, str, str]] = set()
    for obj in docs:
        kind = str(obj.get("kind", ""))
        metadata = obj.get("metadata", {})
        identity = (kind, str(metadata.get("namespace", "")), str(metadata.get("name", "")))
        if identity in identities:
            fail(f"duplicate object {identity}")
        identities.add(identity)
        if kind == "Secret":
            fail(f"Secret must be supplied externally, found {identity}")
        api_kind = (str(obj.get("apiVersion", "")), kind)
        if api_kind in CUSTOM_RESOURCES:
            group, version = api_kind[0].split("/", 1)
            schema = schema_root / group / f"{kind.lower()}_{version}.json"
            if not schema.is_file():
                fail(f"missing pinned schema for {api_kind}: {schema}")
        for container in containers(obj):
            image = str(container.get("image", ""))
            if not image_is_pinned(image):
                fail(f"mutable or missing image for {identity}/{container.get('name')}: {image!r}")
            command = " ".join(str(v) for v in container.get("command", []) + container.get("args", []))
            if PLACEHOLDER.search(command):
                fail(f"placeholder command for {identity}/{container.get('name')}")
            security = container.get("securityContext") or {}
            if security.get("runAsNonRoot") is not True:
                fail(f"container may run as root for {identity}/{container.get('name')}")
            if security.get("allowPrivilegeEscalation") is not False:
                fail(f"privilege escalation not disabled for {identity}/{container.get('name')}")
            if "ALL" not in (security.get("capabilities", {}).get("drop") or []):
                fail(f"Linux capabilities not dropped for {identity}/{container.get('name')}")
            for env in container.get("env") or []:
                if SENSITIVE_ENV.search(str(env.get("name", ""))) and "value" in env:
                    fail(f"literal sensitive env {env['name']} in {identity}")

    required_default_denies = {
        ("NetworkPolicy", namespace, "default-deny")
        for namespace in ("ingress", "query-plane", "gpu-plane", "ingestion-plane", "data-plane", "observability")
    }
    missing = required_default_denies - identities
    if missing:
        fail(f"missing default-deny policies: {sorted(missing)}")

    print(f"k8s policy: PASS ({len(docs)} objects)")


if __name__ == "__main__":
    main()
