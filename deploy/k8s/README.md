# Kubernetes

`eci-platform/` is the T7.1 Helm chart for ECI workloads and operator-managed
resources. Exact third-party versions are in `operator-versions.yaml`; official
Neo4j and Qdrant values are in `vendor-values/`. See
`docs/runbooks/kubernetes-dev.md` for validation, local install, rollback and
scoped teardown.

```bash
task k8s:validate
task k8s:dev:up
task k8s:dev:verify
task k8s:dev:down
```

The standard render is the production-like infrastructure base.
`values-dev.yaml` is deliberately single-node/reduced and is never evidence of
HA, Neo4j Enterprise RBAC, GPU or performance compliance. Application
templates are opt-in: `applications.enabled=true` is accepted only when every
first-party workload has a registry-issued `name@sha256:<digest>` in
`global.imageReferences`. No unpublished or synthetic ECI image is a chart
default.

The chart only references credentials. `dev-up.sh` creates ephemeral
`eci-runtime`, `eci-opensearch-admin`, and
`eci-opensearch-security-config` Secrets from a random local password. Its
OpenSearch bcrypt hash is generated at install time, so no default credential
is stored in Git. Production-like installations must provision these named
Secrets through their deployment environment before installing the chart.

Application pods receive non-secret service discovery from the namespace-local
`eci-runtime-routing` ConfigMap; addresses never default to localhost. They do
not import the infrastructure-wide `eci-runtime` Secret. Release automation
must provision the per-workload Secret named in `applications.workloads` with
only the explicitly mapped keys for that process. NetworkPolicy likewise
selects the source workload, destination workload/store and TCP port; namespace
membership alone grants no datastore access.

The current Rust ingestion executable is truthfully packaged as the suspended
`ingestion-template` one-shot Job template, not as a listening Deployment. It
requires an external read-only source PVC and a scope-only per-workload Secret.
ADR-0016 records this temporary boundary; T7.1a owns the durable authenticated
worker runtime required before CPU HPA can be claimed.
The Python orchestrator is likewise CLI-only today, so this chart does not
render an orchestrator Deployment or fictional listener. ADR-0017 records that
truth boundary; T7.1b owns the authenticated long-running API, streaming,
readiness and shutdown lifecycle required before T7.1 can be verified.
Verification and summarization are currently importable Python libraries only;
ADR-0018/T7.1c keeps them out of the Deployment catalog until authenticated
server entrypoints and real probes exist. The same ADR models `gds-impact` as a
suspended, scope-bound Job template rather than an argument-less schedule.
Kafka uses a TLS-authenticated listener with simple authorization. Kafka
Connect and every consumer have a distinct Strimzi `KafkaUser`; consumers
mount the broker public `ca.crt` plus only their own `user.crt` and `user.key`, and literal ACLs
limit topic/group access. `dev-up.sh` copies each generated identity into the
consumer namespace without ever copying a CA private key.
OpenSearch clients mount only the generated public HTTP CA and require Basic
Auth. Semantic Cache maps `redis-password` explicitly to `REDIS_PASSWORD`.
Missing keys or CA files remain a fail-closed rollout failure.
Application enablement also requires one or more explicit
`routing.oidcIssuerEgressCIDRs`; they must resolve only the trusted HTTPS issuer
or controlled egress proxy. The chart rejects an empty list instead of opening
broad Internet egress.

`install-operators.sh` downloads every pinned Helm archive from its canonical
HTTPS release URL into a temporary directory and verifies its repository
SHA-256 before Helm sees it. The Qdrant chart is additionally passed through a
fail-closed post-renderer that replaces the chart-compatible tag with the
registry-resolved multi-arch image digest and rejects every remaining mutable
image. A version-only repository lookup is not used.

GPU workloads consume an externally prepared, read-only `eci-gpu-models` PVC.
Their commands name the ADD-prescribed Qwen3-Coder FP8, Jina code embedding
and BGE reranker paths explicitly; the default-deny profile never downloads
model weights at startup.

Envoy is not a generic application pod. When enabled it mounts the externally
provisioned `eci-envoy-config` ConfigMap (`envoy.yaml` and binary
`retrieval.pb`) and `eci-envoy-tls` Secret (`tls.crt`, `tls.key`), and serves
the checked-in bootstrap on port 8080. The runbook contains the exact creation
commands; the chart never synthesizes a certificate or a descriptor.

Kafka Connect runs the pinned Debezium image with its own Strimzi mTLS
identity and literal ACLs. The connector
configuration is stored separately in `eci-debezium-connector`, uses the
Kafka environment config provider for the PostgreSQL password, and is not
submitted before the `public.outbox` migration exists. See the runbook for the
post-migration registration boundary.

The OPA ConfigMap is checked byte-for-byte against the canonical Compose Rego
policy. MinIO uses a four-member distributed StatefulSet with PVCs in the
standard profile and one PVC-backed member in dev; no object data is placed on
`emptyDir`.
