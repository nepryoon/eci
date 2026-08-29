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
- Kafka Connect/Debezium 3.6.0.Final readiness, Strimzi TLS connection and
  PostgreSQL connector plugin discovery: PASS;
- PostgreSQL 17.6 `wal_level=logical`: PASS;
- OPA canonical policy loaded; allow and missing-tenant fail-closed decisions:
  PASS;
- MinIO dev StatefulSet backed by a 1 GiB PVC: PASS;
- in-cluster DNS/TCP connectivity to every required store/service: PASS;
- Pod Security admission label `restricted` on all six ECI namespaces: PASS;
- deterministic Helm/policy/kubeconform/unit validation: PASS.
- registry-resolved SHA-256 pins for every rendered third-party container:
  PASS; no non-existent ECI image is a default;
- CloudNativePG webhook ingress on the operator-only selector/TCP 9443: PASS;
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
catalog of 126 application objects was exercised only with an explicit
`registry.example.invalid` unit fixture; this proves template completeness and
digest enforcement, not image publication or application readiness. Released
applications remain opt-in and require real registry digests plus external
runtime/Envoy configuration.
