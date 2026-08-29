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

Application pods also receive non-secret service discovery from the
namespace-local `eci-runtime-routing` ConfigMap; addresses never default to
localhost. `eci-runtime` must additionally provide service-specific
credentials and trusted identity configuration, including `POSTGRES_DSN`,
`NEO4J_USER`, `NEO4J_PASSWORD`, `ECI_OIDC_ISSUER`, and
`ECI_OIDC_AUDIENCE`. Missing keys remain a fail-closed rollout failure.

Envoy is not a generic application pod. When enabled it mounts the externally
provisioned `eci-envoy-config` ConfigMap (`envoy.yaml` and binary
`retrieval.pb`) and `eci-envoy-tls` Secret (`tls.crt`, `tls.key`), and serves
the checked-in bootstrap on port 8080. The runbook contains the exact creation
commands; the chart never synthesizes a certificate or a descriptor.

Kafka Connect runs the pinned Debezium image over Strimzi TLS. The connector
configuration is stored separately in `eci-debezium-connector`, uses the
Kafka environment config provider for the PostgreSQL password, and is not
submitted before the `public.outbox` migration exists. See the runbook for the
post-migration registration boundary.

The OPA ConfigMap is checked byte-for-byte against the canonical Compose Rego
policy. MinIO uses a four-member distributed StatefulSet with PVCs in the
standard profile and one PVC-backed member in dev; no object data is placed on
`emptyDir`.
