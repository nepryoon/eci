# ECI Kubernetes development platform

This runbook operates only the local kind cluster named `eci-dev`. Production
installation requires platform-owned storage classes, TLS, image publication,
Neo4j Enterprise licensing and secret management; the dev result is not HA,
GPU, RBAC Enterprise, disaster-recovery or D9 performance evidence.

## Toolchain and validation

Pinned validation versions are Task 3.53.1, Helm 3.19.0 and kubeconform 0.8.0. The dev path
uses kind 0.33.0, Kubernetes 1.34.0 and a compatible kubectl. Install PyYAML in
the active Python environment, then run:

```bash
task k8s:validate
```

The command renders standard and dev infrastructure profiles, rejects every
container image that is not pinned by an OCI SHA-256 digest, inline
secrets and placeholder processes, validates every built-in and operator CR
against a checked-in schema, and runs the SPEC-062 acceptance suite. It does
not need Docker, a cluster or network access.

## Local install and verification

Docker must be running with roughly 12 GiB free memory. `task k8s:dev:up`
creates only `eci-dev`, generates an ephemeral runtime password without printing
it, derives the OpenSearch admin bcrypt configuration at runtime, installs exact
operator/chart versions from `operator-versions.yaml`, verifies every chart
archive against its checked-in SHA-256 before Helm executes it, and
uses atomic Helm upgrades. Source-built ECI applications are disabled by
default because no published image is assumed; infrastructure readiness is
never represented by sleep/placeholder containers.

Before a production-like install, provision `eci-postgres-cdc` in
`data-plane` from the secret manager with exactly `username=eci_cdc` and a
random `password` key. Do not reuse `eci-runtime` or place the value on a Helm
command line. CNPG refuses to reconcile the managed replication role without
that Secret. The local bootstrap creates and reuses an independent random
value automatically without printing it.

## Enabling released ECI applications

The repository does not invent registry digests. A release pipeline must
publish each first-party image and supply its immutable full reference under
`global.imageReferences`; only then may an operator set
`applications.enabled=true`. Helm fails before producing a Deployment if any
reference is absent, and the policy gate rejects tag-only references. The
acceptance test uses `registry.example.invalid` exclusively as a template
fixture and never as deployment evidence.

Before enabling the edge, provision the exact generated bootstrap and TLS
material without checking either into Git:

```bash
kubectl -n ingress create configmap eci-envoy-config \
  --from-file=envoy.yaml=deploy/envoy/envoy.yaml \
  --from-file=retrieval.pb=deploy/envoy/retrieval.pb \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ingress create secret tls eci-envoy-tls \
  --cert=/secure/path/tls.crt --key=/secure/path/tls.key \
  --dry-run=client -o yaml | kubectl apply -f -
```

The Envoy Deployment mounts both ConfigMap keys and the Secret read-only,
listens on the bootstrap's actual TLS port 8080, and fails to schedule/start if
any input is absent. It never falls back to cleartext or generated credentials.
Each application namespace receives `eci-runtime-routing` with concrete
cluster DNS names for OPA, retrieval, Neo4j, Qdrant, OpenSearch, Redis, Kafka,
embedding/reranking and vLLM. Application workloads never import the shared
infrastructure `eci-runtime` Secret. Provision one per-workload Secret using
the exact name and key allow-list in `applications.workloads`: for example,
`eci-runtime-api-gateway` contains only `ECI_OIDC_ISSUER` and
`ECI_OIDC_AUDIENCE`, `eci-runtime-retrieval-engine` contains only its Neo4j and
OpenSearch identities, and each sink has a distinct Secret. Workloads without
credentials receive no Secret. Missing values are deployment errors; do not
restore localhost defaults or reuse a sibling Secret.

The production-like Neo4j route bootstraps the routing driver at
`neo4j-core-1.data-plane.svc.cluster.local`; the dedicated GDS batch job uses
`neo4j-gds.data-plane.svc.cluster.local` directly. The dev overlay overrides
both with the single `neo4j.data-plane.svc.cluster.local` community release.
Do not copy the dev hostname into production-like values or route GDS work to
an arbitrary primary.

Provision a read-only `eci-gpu-models` PVC before enabling GPU workloads. It
must contain these preloaded paths: `/models/qwen3-coder-30b-a3b-instruct-fp8`,
`/models/jina-code-embeddings-1.5b` and `/models/bge-reranker-v2-m3`. The chart
passes those paths to vLLM/TEI explicitly and has no network egress fallback
for model download. Missing model data therefore keeps the rollout Not Ready;
it is never replaced with a different model or an online mutable revision.

The current ingestion binary is a one-shot CLI, so the chart deliberately
renders `ingestion-template` as a suspended CronJob and never as a fake TCP
server. Before cloning it, provision the read-only `eci-ingestion-source` PVC
with the exact source snapshot and create `eci-runtime-ingestion` containing
only `POSTGRES_DSN`, `ECI_TENANT_ID`, `ECI_REPOSITORY` and `ECI_ACL_GROUP`.
Then create one bounded Job explicitly:

```bash
kubectl -n ingestion-plane create job ingestion-<commit-id> \
  --from=cronjob/ingestion-template
kubectl -n ingestion-plane wait --for=condition=complete \
  job/ingestion-<commit-id> --timeout=1h
```

Do not unsuspend the schedule or run two scopes through the same prepared
Secret/PVC. ADR-0016 records the boundary; T7.1a replaces it with the
authenticated durable worker pool required by D8 and T7.2 HPA.

The current orchestrator image must not be added to `applications.workloads`:
it contains only the `eci ask` and `eci eval-golden` CLI entrypoints and opens
no service port. ADR-0017/T7.1b owns the authenticated long-running API,
streaming protocol and real health probes. Until then, orchestration remains a
deliberate deployment gap and T7.1 remains `implemented`, not `verified`.

Verification and summarization must likewise remain absent from
`applications.workloads` while their packages expose no server entrypoint.
ADR-0018/T7.1c owns their authenticated APIs and health lifecycle. The same ADR
defines `gds-impact` as a suspended template. Prepare
`eci-runtime-gds-impact` with `NEO4J_USER`, `NEO4J_PASSWORD`,
`ECI_GDS_ENTRY_NODE_ID`, `ECI_GDS_TENANT_ID`, `ECI_GDS_REPOSITORY` and
`ECI_GDS_ACL_GROUP`, then clone exactly one bounded Job with
`kubectl -n ingestion-plane create job gds-<scope-id> --from=cronjob/gds-impact`.
Never unsuspend the shared schedule or take scope from a user prompt.

Before enabling applications, resolve the trusted OIDC issuer (or a controlled
HTTPS egress proxy) to the smallest stable CIDR set and pass each entry as
`--set-string routing.oidcIssuerEgressCIDRs[N]=<cidr>`. The chart fails when
this list is empty and grants only TCP/443 from the API Gateway pod. Do not use
`0.0.0.0/0`; update the release atomically when issuer addresses rotate.

Application enablement is a two-phase operation because the Strimzi and
OpenSearch operators create identities/CAs only after the infrastructure CRs
exist. After the infrastructure release is Ready, the Strimzi User Operator
creates a different mTLS Secret for every Kafka client. Build one scoped
Secret per consumer in `ingestion-plane`: take the broker trust `ca.crt` from
`eci-kafka-cluster-ca-cert`, and only that user's `user.crt` and `user.key`
from its generated `KafkaUser` Secret. Retain names
`eci-kafka-embedding-worker`, `eci-kafka-sink-graph`,
`eci-kafka-sink-vector` and `eci-kafka-sink-search`. Never copy `ca.key`,
`ca.password`, another user's key, or the whole cluster-CA Secret.
`dev-up.sh` performs this scoped copy for the local cluster after all
`KafkaUser` resources are Ready.
Production replication must be continuously reconciled: when Strimzi rotates
a client certificate or broker CA, the secret manager updates the composite
target and the deployment controller rolls that one workload. A stale or
partially updated identity is a fail-closed outage, never a reason to disable
mTLS or share another client's Secret.

OpenSearch clients need only the public HTTP CA. A local one-off equivalent
for that separate trust material is:

```bash
work_dir="$(mktemp -d)"
trap 'rm -f "$work_dir/ca.crt"; rmdir "$work_dir"' EXIT
kubectl -n data-plane get secret eci-opensearch-ca \
  -o jsonpath='{.data.ca\.crt}' | base64 --decode >"$work_dir/ca.crt"
for namespace in query-plane ingestion-plane; do
  kubectl -n "$namespace" create secret generic eci-opensearch-client-ca \
    --from-file=ca.crt="$work_dir/ca.crt" --dry-run=client -o yaml | kubectl apply -f -
done
```

The workloads mount those Secrets read-only. `KAFKA_MTLS_ENABLED=true`
requires a valid CA plus the workload's certificate/private-key pair on both
kafka-go readers and writers; the broker denies unauthenticated clients and
literal `KafkaUser` ACLs deny sibling topics/groups. HTTPS OpenSearch requires CA,
username and password. Redis is deployed with `requirepass`, so Semantic Cache
also requires the explicit `redis-password` mapping. Semantic Cache startup
and readiness call `/ready` on its metrics listener; that endpoint performs an
authenticated Redis PING and returns only 204/503. A missing password fails
configuration, while a stale password or unavailable Redis keeps the pod Not
Ready. Liveness remains a local TCP check. Each client fails before
serving/consuming, or remains fail-closed Not Ready, if its trust or credential
input is absent or invalid.
Redis deliberately has one standalone StatefulSet replica: a ClusterIP never
balances one logical cache across independent stores. AOF `everysec` persists
to a 20 GiB standard PVC (1 GiB dev), and restart persistence is part of the
kind smoke evidence. Cache loss beyond that boundary remains a reconstructible
degradation; a replicated Redis topology is not claimed by T7.1. During an
upgrade from the former Deployment, the Service selector moves directly to
the `redis-stateful` component so old and new independent stores are never
simultaneous backends.
The four Kafka consumers expose `/ready` on their metrics listener. The check
uses their exact TLS/mTLS transport to request every subscribed topic's
metadata, discover the configured group coordinator and fetch that group's
offsets. TLS, topic or group authorization failures return 503 without broker
details. The liveness probe remains a local TCP check so broker recovery is not
amplified into pod restart churn.
Retrieval Engine exposes the same low-detail 204/503 contract on its metrics
listener. Its bounded concurrent check verifies Neo4j credentials and
connectivity, existence/access of Qdrant `code_embeddings` and OpenSearch
`code_chunks`, plus the native TEI `/health` endpoints of embedder and
reranker. The TEI checks do not run embeddings or reranking, so Kubernetes
probing does not create periodic GPU inference load. Retrieval liveness remains
local TCP.

The default-deny boundary is complemented by per-workload NetworkPolicy pairs:
the egress side selects the calling pod and the ingress side selects the exact
store/service pod, with only its protocol port. A compromised LLM gateway, for
example, has no path to PostgreSQL, Neo4j, Qdrant, OpenSearch or Redis even if
it learns a Service DNS name. External production IdP egress must be added as a
cluster-specific identity/CIDR policy; the portable chart permits only the dev
Keycloak pod and otherwise fails closed. The bundled dev issuer is reachable
only on HTTPS 8443. `dev-up.sh` creates or reuses a hostname-bound, short-lived
`eci-keycloak-tls` Secret and copies only its public certificate into the
connectivity probe ConfigMap. An opt-in dev API gateway mounts only that public
certificate as an additional trust root; no private key crosses workloads.
The temporary connectivity probe runs in `data-plane`, where store-to-store
traffic is already permitted, and receives two dev-only, port-specific paths
to OPA and Keycloak. `observability` receives no general datastore path;
T7.3 must add only exporter/metrics ports together with its ServiceMonitors.

OpenSearch Operator 2.8.0 still defaults its metrics proxy to the removed
`gcr.io/kubebuilder` location. The installer keeps version 0.15.0 byte lineage
but overrides only the registry to the official `registry.k8s.io/kubebuilder`
mirror; it does not disable the proxy or its TLS/auth boundary.
All five operator/datastore Helm archives are fetched from canonical HTTPS
release URLs into a temporary directory and checked with the immutable SHA-256
values in `install-operators.sh`. A digest mismatch stops before any Helm
mutation; do not replace this with `helm repo update` or a version-only pull.

```bash
task k8s:dev:up
task k8s:dev:verify
```

Verification waits with bounded timeouts and runs DNS/TCP probes from inside
the cluster. It also verifies `wal_level=logical`, waits for every Kafka topic
and mTLS user, proves an allowed consumer-scoped retry publish and a denied
primary-event forgery with the embedding-worker identity, and loads the Kafka
Connect plugin inventory through its loopback-only REST listener. On
failure it prints pod state and the latest events. Inspect a
component with `kubectl -n <namespace> describe ...` and `kubectl logs` before
retrying; never disable OpenSearch security or operator readiness to make the
smoke pass.

OPA mounts `eci_authz.rego` from ConfigMap `opa-policy`; the acceptance test
requires it to remain byte-identical to the Compose policy. The dev verifier
submits both an authorized request and a missing-tenant request, and requires
the latter to return the deterministic fail-closed reason. A listening OPA
socket alone is not readiness evidence.

The data-plane namespace is default-deny. Native Kubernetes NetworkPolicy
cannot select the `kubernetes.default` Service by name and CNIs may enforce
policy before or after Service DNAT. Five narrowly selected operator policies
therefore allow only TCP 443/6443 for Strimzi, its entity operator,
CloudNativePG/operator instances, and OpenSearch Operator. They do not grant
application or datastore pods general egress. A separate ingress policy
selects only CloudNativePG operator pods on webhook target port 9443. Native
NetworkPolicy cannot identify an external apiserver, so its portable source is
`0.0.0.0/0`; this does not expose a Service externally and operators should
narrow the CIDR when their control-plane source ranges are known.

Kafka Connect authenticates with `eci-kafka-kafka-connect`; its PKCS#12
password is read only from the Strimzi-generated Secret. Its ACLs cover the
exact three Connect internal topics, `eci-connect` group and four outbox
output topics—never a wildcard. Consumer identities can write only their own
retry/DLQ topics, never the primary outbox topics. Kafka Connect REST binds
only `127.0.0.1:8083`, has no Kubernetes Service and is excluded from the
general data-plane policy; only its Kafka and PostgreSQL data paths are
allowed. This loopback design is intentionally single-replica: Helm rejects
`cdc.replicas != 1`, because followers could not reach the leader's advertised
REST endpoint. `dataPlane.enabled=false` omits Connect, its connector ConfigMap
and its dedicated policies. Kafka Connect is Ready before application migrations by design, but the
`eci-outbox-connector` is not registered automatically against an empty
database. After the checked-in SQL migrations have created `public.outbox`, a
platform deployment step must submit the config-only JSON stored in ConfigMap
`eci-debezium-connector` to
`PUT /connectors/eci-outbox-connector/config` through an administrator-authorized
exec session:

```bash
kubectl -n data-plane get configmap eci-debezium-connector \
  -o jsonpath='{.data.connector\.json}' | \
kubectl -n data-plane exec -i deployment/kafka-connect -c kafka-connect -- \
  curl -fsS -X PUT -H 'content-type: application/json' --data-binary @- \
  http://127.0.0.1:8083/connectors/eci-outbox-connector/config
```

The worker resolves
`${env:POSTGRES_PASSWORD}` from the dedicated `eci-postgres-cdc` Secret. CNPG
manages the `eci_cdc` role with LOGIN+REPLICATION but without superuser,
createdb, createrole or bypassrls. Migration 0005 creates the fixed publication
on `public.outbox` and grants only SELECT; `publication.autocreate.mode` remains
disabled. The ConfigMap and deployment
logs never contain the password. Registration must fail if the table or
connector plugin is absent, and readiness of the worker alone must not be
reported as an active CDC stream.

Operator 2.8.0 requires a custom admin user to exist in both a credentials
Secret and `internal_users.yml`. The dev bootstrap creates both from the same
random value; production must supply `eci-opensearch-admin` and
`eci-opensearch-security-config`. The chart never falls back to `admin/admin`.

## Upgrade, rollback and cleanup

Application upgrades use `helm upgrade --install --atomic --wait`; a failed
upgrade rolls back Kubernetes objects but deliberately retains PVCs. Stateful
immutable-field changes require a backup and explicit migration plan. Never
delete/recreate a StatefulSet or PVC as an automated recovery step.

```bash
helm history eci -n query-plane
helm rollback eci <revision> -n query-plane --wait
task k8s:dev:down
```

The teardown command contains the literal `kind delete cluster --name eci-dev`.
It accepts no namespace/cluster argument and never issues PVC or namespace
deletes. Data in that local kind cluster is destroyed with the cluster and is
not recoverable; no external cluster is targeted.

## Object-storage durability

The standard profile runs four MinIO members in distributed mode, each with a
100 GiB `ReadWriteOnce` PVC; the storage class is platform-supplied. The dev
overlay deliberately reduces this to one member with a 1 GiB local-path PVC.
Setting `dataPlane.enabled=false` omits MinIO together with the operator-backed
data stores; it does not provision hidden object-storage PVCs.
It verifies persistence-backed readiness but is not HA evidence. Failed PVC
provisioning is fail-closed: do not replace the claim with `emptyDir`. Back up
source, summary and artifact buckets before any StatefulSet/storage migration;
Helm rollback does not roll back object data.

## Production-like Neo4j boundary

The ADD requires Neo4j 5.x Enterprise. The pinned production-like layout uses
three distinct Neo4j 5.26.30 Helm releases for the initial 128 GiB primaries,
then one 256 GiB GDS release with `initial.server.mode_constraint=SECONDARY`.
Build and publish `deploy/images/neo4j-gds` through the trusted release builder,
then provide its immutable registry manifest as `NEO4J_GDS_IMAGE`. The image
bundles GDS 2.13.12, the version selected by Neo4j's compatibility catalog for
5.26.30; startup copies the checked artifact locally and requires no internet
egress. The installer rejects tags, local image IDs and missing values. Neo4j,
its operations helper and cleanup kubectl image are also registry-digest pinned.
Before invoking the production-like installer, the platform owner must set
`NEO4J_ACCEPT_LICENSE_AGREEMENT=yes` for an existing agreement or `eval` for an
evaluation accepted by that owner. The script fails before installing Neo4j
for any other value; the repository does not accept commercial terms or embed
a license credential.
