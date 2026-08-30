# T7.1 local Kubernetes verification evidence

This directory records the CPU-only/static and local kind integration evidence
for SPEC-062. The run was executed on 2026-08-29 against branch
`feat/t7-1-kubernetes-dev`, based on main
`a0a37e36a1f81573f3f6779bfcaaccd4acab4a69`.

The cluster was created from an empty state with `deploy/k8s/dev-up.sh` and
verified with `deploy/k8s/dev-verify.sh`. The evidence contains no Kubernetes
Secret, password, token, PVC data, container log containing source, or external
credential. It proves the reduced dev profile only: it is not evidence for HA,
Neo4j Enterprise licensing/RBAC, GPU capacity, disaster recovery, or D9 SLOs.

Results:

- clean kind bootstrap: PASS;
- all pinned operator/chart releases deployed: PASS;
- PostgreSQL, Kafka, Neo4j 5.26.30, Qdrant, OpenSearch, Redis, MinIO, OPA and
  Keycloak readiness: PASS;
- Kafka Connect/Debezium 3.6.0.Final readiness, distinct Strimzi mTLS identities and
  PostgreSQL connector plugin discovery: PASS;
- PostgreSQL 17.6 `wal_level=logical`: PASS;
- OPA canonical policy loaded; allow and missing-tenant fail-closed decisions:
  PASS;
- MinIO dev StatefulSet backed by a 1 GiB PVC: PASS;
- in-cluster DNS/TCP connectivity to every required store/service: PASS;
- Pod Security admission label `restricted` on all six ECI namespaces: PASS;
- deterministic Helm/policy/kubeconform/unit validation: PASS;
- five Kafka users and eleven explicit topics Ready; embedding-worker publish
  to its own DLQ allowed and publish to sink-vector's DLQ denied: PASS;
- Qdrant live pod spec and imageID match the registry-resolved immutable
  runtime digest: PASS;
- OpenSearch operator manager and kube-rbac-proxy live pod spec/imageID match
  their registry-resolved multi-architecture digests: PASS;
- registry-resolved SHA-256 pins for every rendered third-party container:
  PASS; no non-existent ECI image is a default;
- CloudNativePG webhook ingress on the operator-only selector/TCP 9443: PASS;
- application datastore transport contracts: retrieval bind/client address
  separation, Kafka reader/writer TLS with public CA, OpenSearch HTTPS with
  public CA and Basic Auth, and Redis `requirepass` propagation: PASS;
- immutable Qdrant bootstrap Job upgrade: the first atomic upgrade failed and
  rolled back as designed; the hook lifecycle fix then upgraded successfully
  at revision 6 and the full smoke remained green.

`render-sha256.txt` identifies the exact standard infrastructure and dev Helm renders without
versioning the bulky generated YAML. Re-rendering with the pinned Helm version
must reproduce those digests.

After the initial evidence capture, the adversarial review found missing D8
Kafka Connect and an incorrect CronJob representation for ingestion. The same
cluster was discarded and the final chart was bootstrapped from an empty kind
cluster at 2026-08-29T20:47Z. All releases shown in `readiness.txt` were
revision 1. The directory name records the beginning of the evidence session;
the later clean rerun supersedes its earlier sanitized readiness/render data.
The final five operator API egress policies were then applied as ECI release
revision 2 and the full readiness/connectivity verification passed again.
PR review fixes were applied as revision 3: OPA policy loading, the
ingestion-to-GPU path, and PVC-backed MinIO. The semantic verification passed
again; the standard render uses four distributed 100 GiB MinIO PVCs.
The later supply-chain/routing review fixes were verified at revision 6. The
final least-privilege/supply-chain review fixes were then applied at ECI
revision 8 after all five operator chart archives passed their checked-in
SHA-256 gates and upgraded the real cluster releases to revision 6. The
catalog of 191 application objects was exercised only with an explicit
`registry.example.invalid` unit fixture; this proves template completeness and
digest enforcement, not image publication or application readiness. Released
applications remain opt-in and require real registry digests plus external
runtime/Envoy configuration. Per-workload Secret and NetworkPolicy assertions
are deterministic render evidence, not application runtime evidence. The real
connectivity probe was moved into data-plane with two dev-only OPA/Keycloak
exceptions, removing the previous generic observability-to-datastore path.
The final security review cycle upgraded ECI to revision 11 and every pinned
vendor release to revision 8. Kafka now denies anonymous clients and uses five
separate mTLS identities with literal topic/group ACLs; a real allowed/denied
producer smoke passed. Qdrant is post-rendered to the verified multi-arch
digest before Helm apply. The opt-in GPU manifests now require canonical model
paths from a read-only external PVC; no GPU workload or model download was
claimed as runtime evidence.
The final review also proved that the Python orchestrator is CLI-only. Its
fictional Deployment and unused allow-list flows were removed; ADR-0017/T7.1b
track the authenticated long-running runtime required before T7.1 verification.
OpenSearch Operator revision 9 was then applied through a fail-closed
post-renderer: both the manager and kube-rbac-proxy references, plus their live
image IDs, matched the recorded multi-arch digests and the full connectivity
smoke remained green.
