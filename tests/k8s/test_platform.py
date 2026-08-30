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
    "api-gateway", "retrieval-engine", "llm-gateway", "semantic-cache", "ingestion",
    "embedding-worker", "sink-graph", "sink-vector", "sink-search", "gds-impact",
)
TEST_DIGEST = "0123456789abcdef" * 4


def render(values: str | None = None, *, application_catalog: bool = False) -> list[dict]:
    command = [HELM, "template", "eci", str(CHART), "--namespace", "query-plane"]
    if values:
        command.extend(["--values", str(CHART / values)])
    if application_catalog:
        command.extend(["--set", "applications.enabled=true"])
        command.extend(["--set-string", "routing.oidcIssuerEgressCIDRs[0]=192.0.2.10/32"])
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
            ("query-plane", "retrieval-engine"),
            ("query-plane", "llm-gateway"),
            ("query-plane", "semantic-cache"),
            ("ingestion-plane", "embedding-worker"),
            ("ingestion-plane", "sink-graph"),
            ("ingestion-plane", "sink-vector"),
            ("ingestion-plane", "sink-search"),
            ("gpu-plane", "vllm"),
            ("gpu-plane", "embedder"),
            ("gpu-plane", "reranker"),
            ("data-plane", "kafka-connect"),
        }
        self.assertTrue(expected.issubset(deployments), expected - deployments)
        self.assertNotIn(("query-plane", "orchestrator"), deployments)
        self.assertNotIn(("query-plane", "verification"), deployments)
        self.assertNotIn(("query-plane", "summarization"), deployments)
        cronjobs = {obj["metadata"]["name"] for obj in self.standard if obj.get("kind") == "CronJob"}
        self.assertEqual(cronjobs, {"gds-impact", "ingestion-template"})
        ingestion = self.by_key[("CronJob", "ingestion-plane", "ingestion-template")]
        self.assertTrue(ingestion["spec"]["suspend"])
        ingestion_pod = ingestion["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        self.assertEqual(ingestion_pod["containers"][0]["args"], ["/input/source"])
        self.assertEqual(
            {item["name"] for item in ingestion_pod["containers"][0]["env"]},
            {"POSTGRES_DSN", "ECI_TENANT_ID", "ECI_REPOSITORY", "ECI_ACL_GROUP"},
        )
        self.assertEqual(
            ingestion_pod["volumes"][1]["persistentVolumeClaim"],
            {"claimName": "eci-ingestion-source", "readOnly": True},
        )
        gds = self.by_key[("CronJob", "ingestion-plane", "gds-impact")]
        self.assertTrue(gds["spec"]["suspend"])
        gds_container = gds["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(
            gds_container["args"],
            [
                "--entry-node-id=$(ECI_GDS_ENTRY_NODE_ID)",
                "--tenant-id=$(ECI_GDS_TENANT_ID)",
                "--repo=$(ECI_GDS_REPOSITORY)",
                "--acl-group=$(ECI_GDS_ACL_GROUP)",
            ],
        )
        self.assertEqual(
            {item["name"] for item in gds_container["env"]},
            {
                "NEO4J_USER", "NEO4J_PASSWORD", "ECI_GDS_ENTRY_NODE_ID",
                "ECI_GDS_TENANT_ID", "ECI_GDS_REPOSITORY", "ECI_GDS_ACL_GROUP",
            },
        )
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
        self.assertEqual(kafka["spec"]["kafka"]["authorization"], {"type": "simple"})
        self.assertEqual(
            kafka["spec"]["kafka"]["listeners"],
            [{"name": "clients", "port": 9093, "type": "internal", "tls": True,
              "authentication": {"type": "tls"}}],
        )
        self.assertEqual(pool["spec"]["replicas"], 3)
        self.assertEqual(set(pool["spec"]["roles"]), {"broker", "controller"})
        self.assertEqual(pool["spec"]["storage"]["type"], "persistent-claim")
        self.assertEqual(postgres["spec"]["instances"], 3)
        self.assertEqual(
            postgres["spec"]["imageName"],
            "ghcr.io/cloudnative-pg/postgresql:17.6@sha256:30b304a2e300ed80b6d1b740e4369e9b0f25599fb518de78c01fd9f25531791b",
        )
        self.assertEqual(postgres["spec"]["postgresql"]["parameters"]["wal_level"], "logical")
        self.assertEqual(
            postgres["spec"]["managed"]["roles"],
            [{
                "name": "eci_cdc", "ensure": "present", "login": True,
                "replication": True, "superuser": False, "createdb": False,
                "createrole": False, "bypassrls": False,
                "passwordSecret": {"name": "eci-postgres-cdc"},
            }],
        )
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
        self.assertEqual(
            env["POSTGRES_PASSWORD"]["valueFrom"]["secretKeyRef"],
            {"name": "eci-postgres-cdc", "key": "password"},
        )
        connector = json.loads(self.by_key[("ConfigMap", "data-plane", "eci-debezium-connector")]["data"]["connector.json"])
        self.assertEqual(connector["database.user"], "eci_cdc")
        self.assertEqual(connector["publication.autocreate.mode"], "disabled")
        self.assertEqual(connect_container["securityContext"]["readOnlyRootFilesystem"], True)
        self.assertEqual(env["BOOTSTRAP_SERVERS"]["value"], "eci-kafka-kafka-bootstrap.data-plane.svc:9093")
        self.assertEqual(env["CONNECT_SSL_KEYSTORE_TYPE"]["value"], "PKCS12")
        self.assertEqual(env["REST_HOST_NAME"]["value"], "127.0.0.1")
        self.assertEqual(env["CONNECT_LISTENERS"]["value"], "http://127.0.0.1:8083")
        self.assertNotIn(("Service", "data-plane", "kafka-connect"), self.by_key)
        for probe_name in ("startupProbe", "readinessProbe", "livenessProbe"):
            probe = connect_container[probe_name]
            self.assertNotIn("httpGet", probe)
            self.assertIn("http://127.0.0.1:8083/connectors", probe["exec"]["command"][-1])
        self.assertEqual(env["CONNECT_SSL_KEYSTORE_LOCATION"]["value"], "/etc/kafka-user/user.p12")
        self.assertEqual(
            env["CONNECT_SSL_KEYSTORE_PASSWORD"]["valueFrom"]["secretKeyRef"],
            {"name": "eci-kafka-kafka-connect", "key": "user.password"},
        )

        topics = {obj["spec"]["topicName"]: obj for obj in self.standard if obj.get("kind") == "KafkaTopic"}
        expected_topics = {
            "outbox.event.CodeNode", "outbox.event.CodeRelation", "outbox.event.CodeChunk",
            "outbox.event.CodeEmbedding", "outbox.event.CodeNode.DLQ",
            "outbox.event.CodeRelation.DLQ", "outbox.event.CodeChunk.DLQ",
            "outbox.event.CodeEmbedding.DLQ", "eci_connect_configs", "eci_connect_offsets",
            "eci_connect_status", "outbox.event.CodeChunk.retry.embedding-worker",
            "outbox.event.CodeNode.retry.sink-graph", "outbox.event.CodeRelation.retry.sink-graph",
            "outbox.event.CodeEmbedding.retry.sink-vector", "outbox.event.CodeChunk.retry.sink-search",
        }
        self.assertEqual(set(topics), expected_topics)
        self.assertTrue(all(topic["spec"]["replicas"] == 3 for topic in topics.values()))
        self.assertEqual(topics["eci_connect_offsets"]["spec"]["partitions"], 25)
        self.assertEqual(topics["eci_connect_status"]["spec"]["partitions"], 5)
        self.assertFalse(kafka["spec"]["kafka"]["config"]["auto.create.topics.enable"])

        users = {obj["metadata"]["name"]: obj for obj in self.standard if obj.get("kind") == "KafkaUser"}
        self.assertEqual(
            set(users),
            {
                "eci-kafka-kafka-connect", "eci-kafka-embedding-worker", "eci-kafka-sink-graph",
                "eci-kafka-sink-vector", "eci-kafka-sink-search",
            },
        )
        for name, user in users.items():
            with self.subTest(kafka_user=name):
                self.assertEqual(user["spec"]["authentication"], {"type": "tls"})
                self.assertEqual(user["spec"]["authorization"]["type"], "simple")
                for acl in user["spec"]["authorization"]["acls"]:
                    resource = acl["resource"]
                    if resource["type"] in {"topic", "group"}:
                        self.assertEqual(resource["patternType"], "literal")
                        self.assertNotEqual(resource["name"], "*")

        graph_resources = {
            (acl["resource"]["type"], acl["resource"].get("name"))
            for acl in users["eci-kafka-sink-graph"]["spec"]["authorization"]["acls"]
        }
        self.assertIn(("group", "sink-graph"), graph_resources)
        for user_name, forbidden_primary, expected_retry in {
            ("eci-kafka-embedding-worker", "outbox.event.CodeChunk", "outbox.event.CodeChunk.retry.embedding-worker"),
            ("eci-kafka-sink-search", "outbox.event.CodeChunk", "outbox.event.CodeChunk.retry.sink-search"),
            ("eci-kafka-sink-vector", "outbox.event.CodeEmbedding", "outbox.event.CodeEmbedding.retry.sink-vector"),
        }:
            write_topics = {
                acl["resource"]["name"]
                for acl in users[user_name]["spec"]["authorization"]["acls"]
                if acl["resource"]["type"] == "topic" and "Write" in acl["operations"]
            }
            self.assertNotIn(forbidden_primary, write_topics)
            self.assertIn(expected_retry, write_topics)
        self.assertNotIn(("topic", "outbox.event.CodeChunk"), graph_resources)
        for primary in ("outbox.event.CodeNode", "outbox.event.CodeRelation"):
            graph_write_topics = {
                acl["resource"]["name"]
                for acl in users["eci-kafka-sink-graph"]["spec"]["authorization"]["acls"]
                if acl["resource"]["type"] == "topic" and "Write" in acl["operations"]
            }
            self.assertNotIn(primary, graph_write_topics)
            self.assertIn(f"{primary}.retry.sink-graph", graph_write_topics)

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

        gpu_models = {
            "vllm": ("/models/qwen3-coder-30b-a3b-instruct-fp8", "eci-qwen3-coder-30b-a3b-fp8"),
            "embedder": ("/models/jina-code-embeddings-1.5b", None),
            "reranker": ("/models/bge-reranker-v2-m3", None),
        }
        for name, (model_path, served_name) in gpu_models.items():
            deployment = self.by_key[("Deployment", "gpu-plane", name)]
            pod = deployment["spec"]["template"]["spec"]
            container = pod["containers"][0]
            self.assertIn(model_path, container["args"])
            if served_name:
                self.assertIn(served_name, container["args"])
            self.assertIn("startupProbe", container)
            self.assertIn({"name": "models", "mountPath": "/models", "readOnly": True}, container["volumeMounts"])
            volumes = {item["name"]: item for item in pod["volumes"]}
            self.assertEqual(
                volumes["models"]["persistentVolumeClaim"],
                {"claimName": "eci-gpu-models", "readOnly": True},
            )

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
        self.assertEqual(
            data_internal["spec"]["podSelector"]["matchExpressions"],
            [{"key": "app.kubernetes.io/name", "operator": "NotIn", "values": ["kafka-connect"]}],
        )
        self.assertEqual(data_internal["spec"]["ingress"][0]["from"], [{"podSelector": {}}])
        self.assertEqual(data_internal["spec"]["egress"][0]["to"], [{"podSelector": {}}])
        self.assertNotIn(("observability", "allow-observability-probes"), policies)
        self.assertNotIn(("Service", "data-plane", "kafka-connect"), self.by_key)
        for name in {
            "allow-kafka-connect-to-kafka", "allow-kafka-connect-to-kafka-ingress",
            "allow-kafka-connect-to-postgres", "allow-kafka-connect-to-postgres-ingress",
        }:
            self.assertIn(("data-plane", name), policies)
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

    def test_dev_probe_image_is_immutable(self) -> None:
        verifier = (ROOT / "deploy/k8s/dev-verify.sh").read_text()
        self.assertIn(
            "nicolaka/netshoot@sha256:7f08c4aff13ff61a35d30e30c5c1ea8396eac6ab4ce19fd02d5a4b3b5d0d09a2",
            verifier,
        )
        self.assertNotIn("nicolaka/netshoot:v0.14", verifier)

    def test_scenario_7_dev_scripts_preserve_restricted_security_and_random_secrets(self) -> None:
        up = (ROOT / "deploy/k8s/dev-up.sh").read_text()
        verify = (ROOT / "deploy/k8s/dev-verify.sh").read_text()
        self.assertIn("openssl rand -hex", up)
        self.assertIn("get secret eci-runtime", up)
        self.assertIn("get secret eci-postgres-cdc", up)
        self.assertIn("create secret generic eci-postgres-cdc", up)
        self.assertIn("unset ECI_CDC_PASSWORD", up)
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

    def test_review_opensearch_operator_images_are_post_rendered_to_digests(self) -> None:
        renderer = ROOT / "deploy/k8s/opensearch-operator-post-renderer.sh"
        source = """apiVersion: apps/v1
kind: Deployment
image:
  repository: schema-field-not-a-pod-image
spec:
  template:
    spec:
      containers:
        - image: \"opensearchproject/opensearch-operator:2.8.0\"
        - image: \"registry.k8s.io/kubebuilder/kube-rbac-proxy:v0.15.0\"
"""
        completed = subprocess.run(
            [str(renderer)], input=source, check=True, capture_output=True, text=True
        )
        images = [
            line.strip().removeprefix("- image: ").strip('"')
            for line in completed.stdout.splitlines()
            if "- image:" in line
        ]
        self.assertEqual(
            images,
            [
                "opensearchproject/opensearch-operator@sha256:ad86464ea5b1661ea25294058e78b3697286cc6b742df7a543fd96d2de0bc61a",
                "registry.k8s.io/kubebuilder/kube-rbac-proxy@sha256:d8cc6ffb98190e8dd403bfe67ddcb454e6127d32b87acc237b3e5240f70a20fb",
            ],
        )
        mutable = subprocess.run(
            [str(renderer)],
            input=source + '        - image: "example.invalid/extra:latest"\n',
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(mutable.returncode, 0)
        install = (ROOT / "deploy/k8s/install-operators.sh").read_text()
        self.assertIn('--post-renderer "$ROOT_DIR/deploy/k8s/opensearch-operator-post-renderer.sh"', install)

    def test_review_runtime_routes_and_envoy_are_fail_closed(self) -> None:
        routing = self.by_key[("ConfigMap", "query-plane", "eci-runtime-routing")]["data"]
        self.assertEqual(routing["OPA_URL"], "http://opa.query-plane.svc.cluster.local:8181")
        self.assertEqual(routing["RETRIEVAL_ENGINE_ADDR"], ":50053")
        ingress_routing = self.by_key[("ConfigMap", "ingress", "eci-runtime-routing")]["data"]
        self.assertEqual(
            ingress_routing["RETRIEVAL_ENGINE_ADDR"],
            "retrieval-engine.query-plane.svc.cluster.local:50053",
        )
        self.assertEqual(
            routing["LLM_GATEWAY_DEFAULT"],
            "http://vllm.gpu-plane.svc.cluster.local:8000|eci-qwen3-coder-30b-a3b-fp8",
        )
        self.assertIn("eci-qwen3-coder-30b-a3b-fp8=", routing["LLM_GATEWAY_ROUTES"])
        self.assertEqual(routing["NEO4J_URI"], "bolt://neo4j.data-plane.svc.cluster.local:7687")
        self.assertEqual(routing["QDRANT_HOST"], "qdrant.data-plane.svc.cluster.local")
        self.assertEqual(routing["OPENSEARCH_URL"], "https://eci-opensearch.data-plane.svc.cluster.local:9200")
        self.assertEqual(routing["KAFKA_TLS_ENABLED"], "true")
        self.assertEqual(routing["KAFKA_MTLS_ENABLED"], "true")
        self.assertEqual(routing["KAFKA_TLS_CA_FILE"], "/etc/eci/kafka/ca.crt")
        self.assertEqual(routing["KAFKA_TLS_CERT_FILE"], "/etc/eci/kafka/user.crt")
        self.assertEqual(routing["KAFKA_TLS_KEY_FILE"], "/etc/eci/kafka/user.key")
        self.assertEqual(routing["OPENSEARCH_CA_FILE"], "/etc/eci/opensearch/ca.crt")
        self.assertEqual(routing["REDIS_REQUIRE_AUTH"], "true")
        self.assertNotIn("localhost", "\n".join(routing.values()))

        llm = self.by_key[("Deployment", "query-plane", "llm-gateway")]
        llm_template = llm["spec"]["template"]
        self.assertNotIn("prometheus.io/scrape", llm_template["metadata"].get("annotations", {}))
        self.assertEqual(
            llm_template["spec"]["containers"][0]["ports"],
            [{"name": "service", "containerPort": 8002}],
        )
        self.assertEqual(
            self.by_key[("Service", "query-plane", "llm-gateway")]["spec"]["ports"],
            [{"name": "service", "port": 8002, "targetPort": "service"}],
        )

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
                self.assertEqual(
                    volumes["kafka-ca"]["secret"]["secretName"],
                    f"eci-kafka-{obj['metadata']['name']}",
                )
                self.assertEqual(
                    volumes["kafka-ca"]["secret"]["items"],
                    [
                        {"key": "ca.crt", "path": "ca.crt"},
                        {"key": "user.crt", "path": "user.crt"},
                        {"key": "user.key", "path": "user.key"},
                    ],
                )
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
            ("ingress", "allow-envoy-to-retrieval"),
            ("query-plane", "allow-envoy-to-retrieval-ingress"),
        }:
            self.assertIn((namespace, name), policies)

        oidc = policies[("ingress", "allow-api-gateway-to-oidc-issuer")]
        self.assertEqual(
            oidc["spec"]["egress"],
            [{"to": [{"ipBlock": {"cidr": "192.0.2.10/32"}}], "ports": [{"protocol": "TCP", "port": 443}]}],
        )

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
            "--set-string",
            "routing.oidcIssuerEgressCIDRs[0]=192.0.2.10/32",
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
        clusters = {item["name"]: item for item in bootstrap["static_resources"]["clusters"]}
        for name in {"retrieval_engine", "retrieval_engine_stream"}:
            address = clusters[name]["load_assignment"]["endpoints"][0]["lb_endpoints"][0]["endpoint"]["address"]["socket_address"]["address"]
            self.assertEqual(address, "retrieval-engine.query-plane.svc.cluster.local")
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
        self.assertIn("qdrant-post-renderer.sh", installer)
        self.assertIn("opensearch-operator-post-renderer.sh", installer)
        dev_up = (ROOT / "deploy/k8s/dev-up.sh").read_text()
        self.assertIn(
            "opensearchproject/opensearch@sha256:23297b8d8545e129dd58c254ed08d786dc552410ba772983ad2af31048d2f04b",
            dev_up,
        )
        self.assertNotIn("opensearchproject/opensearch:3.2.0", dev_up)

        post_renderer = ROOT / "deploy/k8s/qdrant-post-renderer.sh"
        mutable = (
            "apiVersion: apps/v1\nkind: StatefulSet\nspec:\n  template:\n    spec:\n"
            "      containers:\n        - name: qdrant\n"
            "          image: docker.io/qdrant/qdrant:v1.19.0-unprivileged\n"
        )
        rendered = subprocess.run(
            [str(post_renderer)], input=mutable, capture_output=True, text=True, check=True
        ).stdout
        self.assertIn(
            "docker.io/qdrant/qdrant:v1.19.0-unprivileged@sha256:"
            "a0e04fe623cb064502cd869cefc1dc7ce359d8edd481063b5bd351c0a0a2c91e",
            rendered,
        )

    def test_review_application_enablement_requires_real_release_digests(self) -> None:
        result = subprocess.run(
            [
                HELM, "template", "eci", str(CHART),
                "--set", "applications.enabled=true",
                "--set-string", "routing.oidcIssuerEgressCIDRs[0]=192.0.2.10/32",
            ],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("global.imageReferences.api-gateway", result.stderr)

        base = [
            HELM, "template", "eci", str(CHART),
            "--set", "applications.enabled=true",
            "--set-string", "routing.oidcIssuerEgressCIDRs[0]=192.0.2.10/32",
        ]
        for name in APPLICATION_IMAGES:
            base.extend(
                [
                    "--set-string",
                    f"global.imageReferences.{name}=registry.example.invalid/eci-test/{name}@sha256:{TEST_DIGEST}",
                ]
            )

        mutable_reference = subprocess.run(
            base
            + [
                "--set-string",
                "global.imageReferences.api-gateway=registry.example.invalid/eci/api-gateway:v1.0.0",
            ],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(mutable_reference.returncode, 0)
        self.assertIn("must match name@sha256", mutable_reference.stderr)

        mutable_inline = subprocess.run(
            base
            + [
                "--set-string",
                "applications.workloads[0].image=registry.example.invalid/eci/api-gateway:v1.0.0",
            ],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(mutable_inline.returncode, 0)
        self.assertIn("must match name@sha256", mutable_inline.stderr)

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
