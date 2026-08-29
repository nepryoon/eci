#!/usr/bin/env python3
"""Deterministic acceptance tests for SPEC-062 (T7.1)."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy" / "k8s" / "eci-platform"
HELM = os.environ.get("HELM_BIN", "helm")


APPLICATION_IMAGES = (
    "api-gateway", "orchestrator", "retrieval-engine", "verification",
    "llm-gateway", "summarization", "semantic-cache", "ingestion",
    "embedding-worker", "sink-graph", "sink-vector", "sink-search", "gds-impact",
)
TEST_DIGEST = "0123456789abcdef" * 4


def render(values: str | None = None, *, application_catalog: bool = False) -> list[dict]:
    command = [HELM, "template", "eci", str(CHART), "--namespace", "query-plane"]
    if values:
        command.extend(["--values", str(CHART / values)])
    if application_catalog:
        command.extend(["--set", "applications.enabled=true"])
        for name in APPLICATION_IMAGES:
            # Template-only fixture. It proves that every workload requires a
            # digest-shaped reference without pretending an ECI image exists.
            command.extend(
                ["--set-string", f"global.imageReferences.{name}=registry.example.invalid/eci-test/{name}@sha256:{TEST_DIGEST}"]
            )
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
        cls.standard = render(application_catalog=True)
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
        self.assertEqual(
            postgres["spec"]["imageName"],
            "ghcr.io/cloudnative-pg/postgresql:17.6@sha256:30b304a2e300ed80b6d1b740e4369e9b0f25599fb518de78c01fd9f25531791b",
        )
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
        self.assertEqual(
            bootstrap["metadata"]["annotations"],
            {
                "helm.sh/hook": "pre-install,pre-upgrade",
                "helm.sh/hook-weight": "10",
                "helm.sh/hook-delete-policy": "before-hook-creation,hook-succeeded",
            },
        )
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

        data_internal = self.by_key[("NetworkPolicy", "data-plane", "allow-data-plane-internal")]
        self.assertEqual(data_internal["spec"]["ingress"][0]["from"], [{"podSelector": {}}])
        self.assertEqual(data_internal["spec"]["egress"][0]["to"], [{"podSelector": {}}])
        self.assertNotIn(("observability", "allow-observability-probes"), policies)
        dev_policies = {
            (obj["metadata"]["namespace"], obj["metadata"]["name"])
            for obj in self.dev
            if obj.get("kind") == "NetworkPolicy"
        }
        self.assertIn(("data-plane", "allow-dev-connectivity-egress"), dev_policies)
        self.assertIn(("query-plane", "allow-dev-connectivity-to-opa"), dev_policies)
        self.assertIn(("ingress", "allow-dev-connectivity-to-keycloak"), dev_policies)

        webhook = self.by_key[("NetworkPolicy", "data-plane", "allow-kube-api-cloudnative-pg-webhook")]
        self.assertEqual(
            webhook["spec"]["podSelector"]["matchLabels"],
            {"app.kubernetes.io/name": "cloudnative-pg"},
        )
        self.assertEqual(webhook["spec"]["ingress"][0]["from"], [{"ipBlock": {"cidr": "0.0.0.0/0"}}])
        self.assertEqual(webhook["spec"]["ingress"][0]["ports"], [{"protocol": "TCP", "port": 9443}])

    def test_scenario_4_opa_policy_and_durable_minio(self) -> None:
        policy = self.by_key[("ConfigMap", "query-plane", "opa-policy")]
        canonical_policy = (ROOT / "deploy/compose/opa/policies/eci_authz.rego").read_text()
        self.assertEqual(policy["data"]["eci_authz.rego"], canonical_policy)
        opa = self.by_key[("Deployment", "query-plane", "opa")]
        opa_container = opa["spec"]["template"]["spec"]["containers"][0]
        self.assertIn("/policies/eci_authz.rego", opa_container["args"])

        minio = self.by_key[("StatefulSet", "data-plane", "minio")]
        self.assertEqual(minio["spec"]["replicas"], 4)
        self.assertEqual(minio["spec"]["podManagementPolicy"], "Parallel")
        self.assertEqual(minio["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"], "100Gi")
        minio_args = minio["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertTrue(any("minio-{0...3}" in value for value in minio_args))
        dev_minio = keyed(self.dev)[("StatefulSet", "data-plane", "minio")]
        self.assertEqual(dev_minio["spec"]["replicas"], 1)

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
        self.assertIn("namespace: data-plane", verify)
        self.assertNotIn("namespace: observability", verify)
        self.assertIn("SHOW wal_level", verify)
        self.assertIn("io.debezium.connector.postgresql.PostgresConnector", verify)
        self.assertIn("OPA allow and fail-closed decisions: PASS", verify)

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
                    "task": "3.53.1",
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
                self.assertRegex(image, r"@sha256:[0-9a-f]{64}$")

    def test_review_runtime_routes_and_envoy_are_fail_closed(self) -> None:
        routing = self.by_key[("ConfigMap", "query-plane", "eci-runtime-routing")]["data"]
        self.assertEqual(routing["OPA_URL"], "http://opa.query-plane.svc.cluster.local:8181")
        self.assertEqual(routing["RETRIEVAL_ENGINE_ADDR"], ":50053")
        ingress_routing = self.by_key[("ConfigMap", "ingress", "eci-runtime-routing")]["data"]
        self.assertEqual(
            ingress_routing["RETRIEVAL_ENGINE_ADDR"],
            "retrieval-engine.query-plane.svc.cluster.local:50053",
        )
        self.assertEqual(routing["NEO4J_URI"], "bolt://neo4j.data-plane.svc.cluster.local:7687")
        self.assertEqual(routing["QDRANT_HOST"], "qdrant.data-plane.svc.cluster.local")
        self.assertEqual(routing["OPENSEARCH_URL"], "https://eci-opensearch.data-plane.svc.cluster.local:9200")
        self.assertEqual(routing["KAFKA_TLS_ENABLED"], "true")
        self.assertEqual(routing["KAFKA_TLS_CA_FILE"], "/etc/eci/kafka/ca.crt")
        self.assertEqual(routing["OPENSEARCH_CA_FILE"], "/etc/eci/opensearch/ca.crt")
        self.assertEqual(routing["REDIS_REQUIRE_AUTH"], "true")
        self.assertNotIn("localhost", "\n".join(routing.values()))

        for obj in self.standard:
            if obj.get("kind") != "Deployment" or obj["metadata"]["name"] in {
                "envoy", "kafka-connect", "opa", "redis"
            }:
                continue
            refs = obj["spec"]["template"]["spec"]["containers"][0].get("envFrom", [])
            self.assertIn({"configMapRef": {"name": "eci-runtime-routing"}}, refs)
            self.assertFalse(any("secretRef" in ref for ref in refs))
            if obj["metadata"]["namespace"] == "ingestion-plane":
                mounts = obj["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
                self.assertIn(
                    {"name": "kafka-ca", "mountPath": "/etc/eci/kafka", "readOnly": True},
                    mounts,
                )
                volumes = {item["name"]: item for item in obj["spec"]["template"]["spec"]["volumes"]}
                self.assertEqual(volumes["kafka-ca"]["secret"]["secretName"], "eci-kafka-client-ca")
                self.assertEqual(volumes["kafka-ca"]["secret"]["items"], [{"key": "ca.crt", "path": "ca.crt"}])
            if obj["metadata"]["name"] in {"retrieval-engine", "sink-search"}:
                mounts = obj["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
                self.assertIn(
                    {"name": "opensearch-ca", "mountPath": "/etc/eci/opensearch", "readOnly": True},
                    mounts,
                )
                volumes = {item["name"]: item for item in obj["spec"]["template"]["spec"]["volumes"]}
                self.assertEqual(
                    volumes["opensearch-ca"]["secret"],
                    {"secretName": "eci-opensearch-client-ca", "items": [{"key": "ca.crt", "path": "ca.crt"}]},
                )

        semantic_cache = self.by_key[("Deployment", "query-plane", "semantic-cache")]
        cache_env = semantic_cache["spec"]["template"]["spec"]["containers"][0]["env"]
        self.assertIn(
            {
                "name": "REDIS_PASSWORD",
                "valueFrom": {
                    "secretKeyRef": {"name": "eci-runtime-semantic-cache", "key": "REDIS_PASSWORD"}
                },
            },
            cache_env,
        )

        secret_names: dict[str, set[str]] = {}
        for obj in self.standard:
            if obj.get("kind") != "Deployment" or obj["metadata"]["name"] in {
                "envoy", "kafka-connect", "opa", "redis"
            }:
                continue
            app = obj["metadata"]["name"]
            for env in obj["spec"]["template"]["spec"]["containers"][0].get("env", []):
                secret = env.get("valueFrom", {}).get("secretKeyRef", {}).get("name")
                if secret:
                    secret_names.setdefault(app, set()).add(secret)
        used = [name for names in secret_names.values() for name in names]
        self.assertNotIn("eci-runtime", used)
        self.assertEqual(len(used), len(set(used)))
        self.assertEqual(secret_names["retrieval-engine"], {"eci-runtime-retrieval-engine"})
        self.assertEqual(secret_names["sink-graph"], {"eci-runtime-sink-graph"})

        policies = {
            (obj["metadata"]["namespace"], obj["metadata"]["name"]): obj
            for obj in self.standard
            if obj.get("kind") == "NetworkPolicy"
        }
        self.assertNotIn(("query-plane", "allow-d8-internal"), policies)
        self.assertNotIn(("ingestion-plane", "allow-ingestion-to-data"), policies)
        for namespace, name in {
            ("query-plane", "allow-retrieval-engine-to-neo4j"),
            ("data-plane", "allow-retrieval-engine-to-neo4j-ingress"),
            ("ingestion-plane", "allow-sink-vector-to-qdrant"),
            ("data-plane", "allow-sink-vector-to-qdrant-ingress"),
            ("query-plane", "allow-semantic-cache-to-redis"),
            ("data-plane", "allow-semantic-cache-to-redis-ingress"),
        }:
            self.assertIn((namespace, name), policies)

        neo4j_egress = policies[("query-plane", "allow-retrieval-engine-to-neo4j")]
        self.assertEqual(
            neo4j_egress["spec"]["podSelector"]["matchLabels"],
            {"app.kubernetes.io/name": "retrieval-engine"},
        )
        self.assertEqual(neo4j_egress["spec"]["egress"][0]["ports"], [{"protocol": "TCP", "port": 7687}])

        invalid_workloads = json.dumps(
            [
                {
                    "name": "api-gateway",
                    "namespace": "ingress",
                    "port": 8081,
                    "metricsPort": 9107,
                    "replicas": 2,
                    "profile": "io",
                    "runtimeSecret": "eci-runtime",
                    "secretEnv": [{"name": "ECI_OIDC_ISSUER", "key": "ECI_OIDC_ISSUER"}],
                }
            ]
        )
        shared_secret_command = [
            HELM,
            "template",
            "eci",
            str(CHART),
            "--set",
            "applications.enabled=true",
            "--set-json",
            f"applications.workloads={invalid_workloads}",
            "--set-string",
            f"global.imageReferences.api-gateway=registry.example.invalid/eci-test/api-gateway@sha256:{TEST_DIGEST}",
            "--set-string",
            f"global.imageReferences.gds-impact=registry.example.invalid/eci-test/gds-impact@sha256:{TEST_DIGEST}",
        ]
        shared_secret = subprocess.run(shared_secret_command, capture_output=True, text=True)
        self.assertNotEqual(shared_secret.returncode, 0)
        self.assertIn("per-workload isolation", shared_secret.stderr)

        envoy = self.by_key[("Deployment", "ingress", "envoy")]
        container = envoy["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["ports"][0], {"name": "service", "containerPort": 8080})
        mounts = {item["name"]: item for item in container["volumeMounts"]}
        self.assertEqual(mounts["config"]["mountPath"], "/etc/envoy/envoy.yaml")
        self.assertEqual(mounts["descriptor"]["mountPath"], "/etc/envoy/retrieval.pb")
        self.assertEqual(mounts["tls"]["mountPath"], "/etc/envoy/tls")
        volumes = {item["name"]: item for item in envoy["spec"]["template"]["spec"]["volumes"]}
        self.assertEqual(volumes["config"]["configMap"]["name"], "eci-envoy-config")
        self.assertEqual(volumes["tls"]["secret"]["secretName"], "eci-envoy-tls")
        bootstrap = yaml.safe_load((ROOT / "deploy/envoy/envoy.yaml").read_text())
        listener_port = bootstrap["static_resources"]["listeners"][0]["address"]["socket_address"]["port_value"]
        self.assertEqual(listener_port, container["ports"][0]["containerPort"])
        runbook = (ROOT / "docs/runbooks/kubernetes-dev.md").read_text()
        self.assertIn("--from-file=envoy.yaml=deploy/envoy/envoy.yaml", runbook)
        self.assertIn("--from-file=retrieval.pb=deploy/envoy/retrieval.pb", runbook)
        self.assertIn("Never copy `ca.key`", runbook)
        self.assertIn("per-workload", runbook)

        installer = (ROOT / "deploy/k8s/install-operators.sh").read_text()
        self.assertNotIn("helm repo add", installer)
        self.assertIn("sha256sum -c", installer)
        for expected in {
            "0f8a50b2f19bd99482f9fd6e17cf42902f72f9e594a136813ac3f0b7af422efd",
            "668e065ff53508d58238788fd35b355a925060843629a951df0e6a9362e6d32f",
            "f289e27e553c45b55e20952c78971b19a1b5defe9f89bea1f6910f3ee3da81eb",
            "b7dd64379ae449b48f9249c94ae8c8d2a48223e74d9ecd6760beef274bb37c78",
            "131236b52d7959ee600f86ba43e48e88ff715b12a26451871fe57c2ba5809f0b",
        }:
            self.assertIn(expected, installer)

    def test_review_application_enablement_requires_real_release_digests(self) -> None:
        result = subprocess.run(
            [HELM, "template", "eci", str(CHART), "--set", "applications.enabled=true"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("global.imageReferences.api-gateway", result.stderr)

    def test_review_ci_installs_verified_task_release(self) -> None:
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertNotIn("https://taskfile.dev/install.sh", workflow)
        self.assertIn("task_linux_amd64.tar.gz", workflow)
        self.assertIn("a54a408f6861ff921f6e87774180db31bacd8c1e7c944ca696db9fea49a82fc7", workflow)

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
