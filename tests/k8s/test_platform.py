#!/usr/bin/env python3
"""Deterministic acceptance tests for SPEC-062 (T7.1)."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy" / "k8s" / "eci-platform"
HELM = os.environ.get("HELM_BIN", "helm")


def render(values: str | None = None) -> list[dict]:
    command = [HELM, "template", "eci", str(CHART), "--namespace", "query-plane"]
    if values:
        command.extend(["--values", str(CHART / values)])
    output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    return [doc for doc in yaml.safe_load_all(output) if isinstance(doc, dict)]


def keyed(objects: list[dict]) -> dict[tuple[str, str, str], dict]:
    result = {}
    for obj in objects:
        metadata = obj.get("metadata", {})
        result[(obj.get("kind", ""), metadata.get("namespace", ""), metadata.get("name", ""))] = obj
    return result


class PlatformChartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.standard = render()
        cls.dev = render("values-dev.yaml")
        cls.by_key = keyed(cls.standard)

    def test_scenario_1_complete_d8_inventory(self) -> None:
        namespaces = {
            obj["metadata"]["name"] for obj in self.standard if obj.get("kind") == "Namespace"
        }
        self.assertEqual(
            namespaces,
            {"ingress", "query-plane", "gpu-plane", "ingestion-plane", "data-plane", "observability"},
        )
        deployments = {
            (obj["metadata"].get("namespace"), obj["metadata"]["name"])
            for obj in self.standard
            if obj.get("kind") == "Deployment"
        }
        expected = {
            ("ingress", "envoy"),
            ("ingress", "api-gateway"),
            ("query-plane", "orchestrator"),
            ("query-plane", "retrieval-engine"),
            ("query-plane", "verification"),
            ("query-plane", "llm-gateway"),
            ("query-plane", "summarization"),
            ("query-plane", "semantic-cache"),
            ("ingestion-plane", "embedding-worker"),
            ("ingestion-plane", "ingestion"),
            ("ingestion-plane", "sink-graph"),
            ("ingestion-plane", "sink-vector"),
            ("ingestion-plane", "sink-search"),
            ("gpu-plane", "vllm"),
            ("gpu-plane", "embedder"),
            ("gpu-plane", "reranker"),
            ("data-plane", "kafka-connect"),
        }
        self.assertTrue(expected.issubset(deployments), expected - deployments)
        cronjobs = {obj["metadata"]["name"] for obj in self.standard if obj.get("kind") == "CronJob"}
        self.assertEqual(cronjobs, {"gds-impact"})
        connector = self.by_key[("ConfigMap", "data-plane", "eci-debezium-connector")]
        connector_config = connector["data"]["connector.json"]
        self.assertIn("io.debezium.connector.postgresql.PostgresConnector", connector_config)
        self.assertIn("io.debezium.transforms.outbox.EventRouter", connector_config)
        self.assertIn("${env:POSTGRES_PASSWORD}", connector_config)
        self.assertNotIn("eci-dev-only", connector_config)

    def test_scenario_2_stateful_operator_contracts(self) -> None:
        kafka = self.by_key[("Kafka", "data-plane", "eci-kafka")]
        pool = self.by_key[("KafkaNodePool", "data-plane", "eci-kafka-nodes")]
        postgres = self.by_key[("Cluster", "data-plane", "eci-postgres")]
        opensearch = self.by_key[("OpenSearchCluster", "data-plane", "eci-opensearch")]
        self.assertEqual(kafka["apiVersion"], "kafka.strimzi.io/v1")
        self.assertEqual(pool["spec"]["replicas"], 3)
        self.assertEqual(set(pool["spec"]["roles"]), {"broker", "controller"})
        self.assertEqual(pool["spec"]["storage"]["type"], "persistent-claim")
        self.assertEqual(postgres["spec"]["instances"], 3)
        self.assertTrue(postgres["spec"]["imageName"].endswith(":17.6"))
        self.assertEqual(postgres["spec"]["postgresql"]["parameters"]["wal_level"], "logical")
        self.assertEqual(opensearch["apiVersion"], "opensearch.opster.io/v1")
        self.assertEqual(opensearch["spec"]["nodePools"][0]["replicas"], 3)
        self.assertEqual(
            opensearch["spec"]["security"]["config"],
            {
                "adminCredentialsSecret": {"name": "eci-opensearch-admin"},
                "securityConfigSecret": {"name": "eci-opensearch-security-config"},
            },
        )
        self.assertTrue(opensearch["spec"]["security"]["tls"]["http"]["generate"])
        self.assertTrue(opensearch["spec"]["security"]["tls"]["transport"]["generate"])

        connect = self.by_key[("Deployment", "data-plane", "kafka-connect")]
        connect_container = connect["spec"]["template"]["spec"]["containers"][0]
        env = {item["name"]: item for item in connect_container["env"]}
        self.assertEqual(env["CONNECT_SECURITY_PROTOCOL"]["value"], "SSL")
        self.assertEqual(env["CONNECT_CONFIG_PROVIDERS"]["value"], "env")
        self.assertIn("secretKeyRef", env["POSTGRES_PASSWORD"]["valueFrom"])
        self.assertEqual(connect_container["securityContext"]["readOnlyRootFilesystem"], True)

        qdrant_values = yaml.safe_load((ROOT / "deploy/k8s/vendor-values/qdrant.yaml").read_text())
        self.assertEqual(qdrant_values["replicaCount"], 3)
        self.assertTrue(qdrant_values["config"]["cluster"]["enabled"])
        bootstrap = self.by_key[("Job", "data-plane", "qdrant-collection-bootstrap")]
        env = bootstrap["spec"]["template"]["spec"]["containers"][0]["env"]
        self.assertIn({"name": "SHARD_NUMBER", "value": "3"}, env)
        self.assertIn({"name": "REPLICATION_FACTOR", "value": "2"}, env)

        neo4j_core = yaml.safe_load((ROOT / "deploy/k8s/vendor-values/neo4j-core.yaml").read_text())
        neo4j_gds = yaml.safe_load((ROOT / "deploy/k8s/vendor-values/neo4j-gds.yaml").read_text())
        self.assertEqual(neo4j_core["neo4j"]["edition"], "enterprise")
        self.assertEqual(neo4j_core["neo4j"]["minimumClusterSize"], 3)
        self.assertEqual(neo4j_core["neo4j"]["resources"]["memory"], "128Gi")
        self.assertEqual(neo4j_core["neo4j"]["acceptLicenseAgreement"], "no")
        self.assertEqual(neo4j_gds["neo4j"]["resources"]["memory"], "256Gi")
        self.assertTrue(neo4j_gds["neo4j"]["operations"]["enableServer"])
        self.assertEqual(neo4j_gds["config"]["server.cluster.system_database_mode"], "SECONDARY")
        self.assertEqual(neo4j_gds["config"]["initial.server.mode_constraint"], "SECONDARY")
        installer = (ROOT / "deploy/k8s/install-operators.sh").read_text()
        self.assertIn("for member in 1 2 3", installer)
        self.assertIn('"neo4j-core-${member}"', installer)
        self.assertIn("NEO4J_ACCEPT_LICENSE_AGREEMENT", installer)

    def test_scenario_3_stateless_availability(self) -> None:
        deployments = [obj for obj in self.standard if obj.get("kind") == "Deployment"]
        pdb_names = {obj["metadata"]["name"] for obj in self.standard if obj.get("kind") == "PodDisruptionBudget"}
        for deployment in deployments:
            name = deployment["metadata"]["name"]
            with self.subTest(name=name):
                strategy = deployment["spec"]["strategy"]["rollingUpdate"]
                self.assertEqual(strategy, {"maxSurge": 1, "maxUnavailable": 0})
                pod_spec = deployment["spec"]["template"]["spec"]
                topology = {item["topologyKey"] for item in pod_spec["topologySpreadConstraints"]}
                self.assertEqual(topology, {"topology.kubernetes.io/zone", "kubernetes.io/hostname"})
                self.assertIn("podAntiAffinity", pod_spec["affinity"])
                self.assertIn(name, pdb_names)
                for container in pod_spec["containers"]:
                    self.assertTrue(container.get("resources", {}).get("requests"))
                    self.assertTrue(container.get("resources", {}).get("limits"))
                    self.assertIn("readinessProbe", container)
                    self.assertIn("livenessProbe", container)

    def test_scenario_4_pod_security_secret_refs_and_network_policy(self) -> None:
        self.assertFalse(any(obj.get("kind") == "Secret" for obj in self.standard))
        workload_kinds = {"Deployment", "CronJob", "Job"}
        for obj in self.standard:
            if obj.get("kind") not in workload_kinds:
                continue
            template = obj["spec"]["jobTemplate"]["spec"]["template"] if obj["kind"] == "CronJob" else obj["spec"]["template"]
            pod = template["spec"]
            with self.subTest(kind=obj["kind"], name=obj["metadata"]["name"]):
                self.assertFalse(pod.get("automountServiceAccountToken", True))
                self.assertEqual(pod["securityContext"]["seccompProfile"]["type"], "RuntimeDefault")
                for container in pod.get("initContainers", []) + pod["containers"]:
                    security = container["securityContext"]
                    self.assertTrue(security["runAsNonRoot"])
                    self.assertFalse(security["allowPrivilegeEscalation"])
                    self.assertEqual(security["capabilities"]["drop"], ["ALL"])
        policies = {(obj["metadata"]["namespace"], obj["metadata"]["name"]) for obj in self.standard if obj.get("kind") == "NetworkPolicy"}
        for namespace in {"ingress", "query-plane", "gpu-plane", "ingestion-plane", "data-plane", "observability"}:
            self.assertIn((namespace, "default-deny"), policies)
        for name in {
            "allow-kube-api-strimzi",
            "allow-kube-api-strimzi-entity",
            "allow-kube-api-cloudnative-pg",
            "allow-kube-api-cnpg-instance",
            "allow-kube-api-opensearch",
        }:
            policy = self.by_key[("NetworkPolicy", "data-plane", name)]
            self.assertEqual(
                policy["spec"]["egress"][0]["ports"],
                [{"protocol": "TCP", "port": 443}, {"protocol": "TCP", "port": 6443}],
            )

    def test_scenario_5_dev_overlay_is_explicitly_reduced(self) -> None:
        standard = keyed(self.standard)
        dev = keyed(self.dev)
        self.assertEqual(dev[("Cluster", "data-plane", "eci-postgres")]["spec"]["instances"], 1)
        self.assertEqual(dev[("KafkaNodePool", "data-plane", "eci-kafka-nodes")]["spec"]["replicas"], 1)
        self.assertEqual(dev[("OpenSearchCluster", "data-plane", "eci-opensearch")]["spec"]["nodePools"][0]["replicas"], 1)
        self.assertEqual(dev[("ConfigMap", "data-plane", "eci-profile")]["data"]["ha"], "false")
        self.assertEqual(dev[("ConfigMap", "data-plane", "eci-profile")]["data"]["neo4jEdition"], "community")
        self.assertEqual(dev[("Deployment", "data-plane", "kafka-connect")]["spec"]["replicas"], 1)
        self.assertEqual(standard[("ConfigMap", "data-plane", "eci-profile")]["data"]["ha"], "true")
        self.assertNotIn(("Deployment", "ingress", "keycloak"), standard)
        self.assertIn(("Deployment", "ingress", "keycloak"), dev)
        self.assertFalse(any(obj.get("kind") == "Secret" for obj in self.dev))
        self.assertFalse(any(obj.get("kind") == "CronJob" for obj in self.dev))
        self.assertFalse(
            any(
                container.get("image", "").startswith("ghcr.io/nepryoon/eci/")
                for obj in self.dev
                for container in self._containers(obj)
            )
        )

    def test_scenario_7_dev_scripts_preserve_restricted_security_and_random_secrets(self) -> None:
        up = (ROOT / "deploy/k8s/dev-up.sh").read_text()
        verify = (ROOT / "deploy/k8s/dev-verify.sh").read_text()
        self.assertIn("openssl rand -hex", up)
        self.assertIn("get secret eci-runtime", up)
        self.assertIn("hash.sh", up)
        self.assertIn("eci-opensearch-security-config", up)
        self.assertNotIn("admin:admin", up)
        self.assertNotIn("--from-literal=password", up)
        self.assertNotIn('-p "$ECI_DEV_PASSWORD"', up)
        self.assertIn("--from-env-file", up)
        self.assertIn("runAsNonRoot: true", verify)
        self.assertIn("seccompProfile: {type: RuntimeDefault}", verify)
        self.assertIn("capabilities: {drop: [ALL]}", verify)
        self.assertIn("neo4j.data-plane.svc:7687", verify)
        self.assertIn("qdrant.data-plane.svc:6334", verify)
        self.assertIn("SHOW wal_level", verify)
        self.assertIn("io.debezium.connector.postgresql.PostgresConnector", verify)

    def test_scenario_6_versions_and_api_groups_are_pinned(self) -> None:
        versions = yaml.safe_load((ROOT / "deploy/k8s/operator-versions.yaml").read_text())
        self.assertEqual(
            versions,
            {
                "strimzi": {"release": "1.2.0"},
                "cloudnativePG": {"chart": "0.29.0", "app": "1.30.0"},
                "openSearchOperator": {"chart": "2.8.0", "app": "2.8.0"},
                "neo4j": {"chart": "5.26.30", "app": "5.26.30"},
                "qdrant": {"chart": "1.19.0", "app": "v1.19.0"},
                "toolchain": {
                    "helm": "3.19.0",
                    "kind": "0.33.0",
                    "kubeconform": "0.8.0",
                    "kubernetes": "1.34.0",
                    "kubectl": "1.34.0",
                },
            },
        )
        for obj in self.standard:
            for container in self._containers(obj):
                image = container.get("image", "")
                self.assertNotIn(":latest", image)
                self.assertNotEqual(image.rsplit(":", 1)[-1], image)

    def test_scenario_8_teardown_is_literal_and_scoped(self) -> None:
        script = (ROOT / "deploy/k8s/dev-down.sh").read_text()
        self.assertIn("kind delete cluster --name eci-dev", script)
        self.assertNotIn("rm -rf", script)
        self.assertNotIn("delete pvc", script)
        self.assertNotIn("delete namespace", script)

    @staticmethod
    def _containers(obj: dict) -> list[dict]:
        kind = obj.get("kind")
        if kind == "Deployment" or kind == "Job":
            return obj["spec"]["template"]["spec"].get("containers", [])
        if kind == "CronJob":
            return obj["spec"]["jobTemplate"]["spec"]["template"]["spec"].get("containers", [])
        return []


if __name__ == "__main__":
    unittest.main()
