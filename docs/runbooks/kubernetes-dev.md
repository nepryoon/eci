# ECI Kubernetes development platform

This runbook operates only the local kind cluster named `eci-dev`. Production
installation requires platform-owned storage classes, TLS, image publication,
Neo4j Enterprise licensing and secret management; the dev result is not HA,
GPU, RBAC Enterprise, disaster-recovery or D9 performance evidence.

## Toolchain and validation

Pinned validation versions are Helm 3.19.0 and kubeconform 0.8.0. The dev path
uses kind 0.33.0, Kubernetes 1.34.0 and a compatible kubectl. Install PyYAML in
the active Python environment, then run:

```bash
task k8s:validate
```

The command renders standard and dev profiles, rejects mutable images, inline
secrets and placeholder processes, validates every built-in and operator CR
against a checked-in schema, and runs the SPEC-062 acceptance suite. It does
not need Docker, a cluster or network access.

## Local install and verification

Docker must be running with roughly 12 GiB free memory. `task k8s:dev:up`
creates only `eci-dev`, generates an ephemeral runtime password without printing
it, derives the OpenSearch admin bcrypt configuration at runtime, installs exact
operator/chart versions from `operator-versions.yaml`, and
uses atomic Helm upgrades. Source-built ECI applications are disabled by
default because no published image is assumed; infrastructure readiness is
never represented by sleep/placeholder containers.

OpenSearch Operator 2.8.0 still defaults its metrics proxy to the removed
`gcr.io/kubebuilder` location. The installer keeps version 0.15.0 byte lineage
but overrides only the registry to the official `registry.k8s.io/kubebuilder`
mirror; it does not disable the proxy or its TLS/auth boundary.

```bash
task k8s:dev:up
task k8s:dev:verify
```

Verification waits with bounded timeouts and runs DNS/TCP probes from inside
the cluster. It also verifies `wal_level=logical` and loads the Kafka Connect
plugin inventory to prove the PostgreSQL Debezium connector is available. On
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
application or datastore pods general egress.

Kafka Connect is Ready before application migrations by design, but the
`eci-outbox-connector` is not registered automatically against an empty
database. After the checked-in SQL migrations have created `public.outbox`, a
platform deployment step must submit the config-only JSON stored in ConfigMap
`eci-debezium-connector` to
`PUT /connectors/eci-outbox-connector/config`. The worker resolves
`${env:POSTGRES_PASSWORD}` from `eci-runtime`; the ConfigMap and deployment
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
It verifies persistence-backed readiness but is not HA evidence. Failed PVC
provisioning is fail-closed: do not replace the claim with `emptyDir`. Back up
source, summary and artifact buckets before any StatefulSet/storage migration;
Helm rollback does not roll back object data.

## Production-like Neo4j boundary

The ADD requires Neo4j 5.x Enterprise. The pinned production-like layout uses
three distinct Neo4j 5.26.30 Helm releases for the initial 128 GiB primaries,
then one 256 GiB GDS release with `initial.server.mode_constraint=SECONDARY`.
Before invoking the production-like installer, the platform owner must set
`NEO4J_ACCEPT_LICENSE_AGREEMENT=yes` for an existing agreement or `eval` for an
evaluation accepted by that owner. The script fails before installing Neo4j
for any other value; the repository does not accept commercial terms or embed
a license credential.
