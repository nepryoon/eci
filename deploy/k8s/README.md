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

The standard render is production-like. `values-dev.yaml` is deliberately
single-node/reduced and is never evidence of HA, Neo4j Enterprise RBAC, GPU or
performance compliance.

The chart only references credentials. `dev-up.sh` creates ephemeral
`eci-runtime`, `eci-opensearch-admin`, and
`eci-opensearch-security-config` Secrets from a random local password. Its
OpenSearch bcrypt hash is generated at install time, so no default credential
is stored in Git. Production-like installations must provision these named
Secrets through their deployment environment before installing the chart.

Kafka Connect runs the pinned Debezium image over Strimzi TLS. The connector
configuration is stored separately in `eci-debezium-connector`, uses the
Kafka environment config provider for the PostgreSQL password, and is not
submitted before the `public.outbox` migration exists. See the runbook for the
post-migration registration boundary.
