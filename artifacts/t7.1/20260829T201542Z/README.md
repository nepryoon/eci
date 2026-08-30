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
- Keycloak dev HTTPS 8443, hostname-bound certificate and OIDC discovery issuer:
  PASS; no private key is included in evidence;
- Kafka Connect/Debezium 3.6.0.Final readiness, distinct Strimzi mTLS identities and
  PostgreSQL connector plugin discovery: PASS;
- PostgreSQL 17.6 `wal_level=logical`: PASS;
- OPA canonical policy loaded; allow and missing-tenant fail-closed decisions:
  PASS;
- MinIO dev StatefulSet backed by a 1 GiB PVC: PASS;
- in-cluster DNS/TCP connectivity to every required store/service: PASS;
- Pod Security admission label `restricted` on all six ECI namespaces: PASS;
- deterministic Helm/policy/kubeconform/unit validation: PASS;
- five Kafka users and sixteen explicit topics Ready; embedding-worker publish
  to its own retry topic allowed and publish to the primary CodeChunk topic
  denied: PASS;
- Kafka Connect REST reachable on loopback only, absent as a Service and denied
  on the pod IP; plugin discovery through administrative exec: PASS;
- Kafka Connect single-replica invariant and complete omission with the managed
  data plane disabled: PASS (deterministic render; no external backend claimed);
- embedding-worker mTLS identity consumed its retry topic and accessed its exact
  consumer-group offsets; shared worker readiness now also performs a
  non-consuming log-end Fetch that requires Topic Read and rejects TLS/topic/group
  failures with 503 and leaks no broker detail: PASS;
- Semantic Cache authenticated Redis readiness returns closed 503 without
  backend detail on a failed PING, and startup/readiness use that endpoint:
  PASS (deterministic unit/render evidence; first-party image not deployed);
- Retrieval readiness requires authenticated Neo4j, exact Qdrant collection,
  exact OpenSearch index and both native TEI health paths concurrently, bounded
  and without inference or response detail: PASS (deterministic unit/render
  evidence; first-party image not deployed);
- LLM Gateway readiness requires every configured vLLM `/health` path with a
  bounded, non-inference request and returns an empty 503 on failure: PASS
  (deterministic unit/render evidence; first-party image not deployed);
- concurrent `sink-search` index bootstrap forces two initial 404 observations,
  accepts only the typed already-exists loser and reconciles security mapping:
  PASS (deterministic unit plus real OpenSearch integration evidence);
- dedicated CNPG-managed `eci_cdc` role, passwordless
  `eci_cdc_outbox_reader` carrier membership, inherited SELECT-only table
  grant, fixed outbox publication, logical slot and live Debezium
  connector/task: PASS;
- migration 0006 consumer-scoped `processed_events` key applied live; a
  rolled-back probe registered the same event for `embedding-worker` and
  `sink-search`, while integration tests retain same-consumer dedup and verify
  fail-closed rollback: PASS;
- dynamic connectivity probe image pinned by registry digest: PASS;
- Qdrant live pod spec and imageID match the registry-resolved immutable
  runtime digest: PASS;
- OpenSearch operator manager and kube-rbac-proxy live pod spec/imageID match
  their registry-resolved multi-architecture digests: PASS;
- OpenSearch data-node image is pinned in the CR and its recreated live pod
  spec/imageID matches that digest: PASS;
- Qdrant peer/bootstrap/client ingress is component- and port-scoped; a pinned
  unrelated data-plane probe could not reach its unauthenticated API: PASS;
- registry-resolved SHA-256 pins for every rendered third-party container:
  PASS; no non-existent ECI image is a default;
- CloudNativePG webhook ingress on the operator-only selector/TCP 9443: PASS;
- application datastore transport contracts: retrieval bind/client address
  separation, Kafka reader/writer TLS with public CA, OpenSearch HTTPS with
  public CA and Basic Auth, and Redis `requirepass` propagation: PASS;
- immutable Qdrant bootstrap Job upgrade: the first atomic upgrade failed and
  rolled back as designed; the hook lifecycle fix then upgraded successfully
  at revision 6 and the full smoke remained green.

During the final data-node pin verification, deleting the only OpenSearch dev
pod exposed the expected non-HA single-node quorum boundary. The disposable
OpenSearch smoke-test PVC alone was removed and recreated through the operator;
the digest-pinned node then became Ready and the complete connectivity check
passed. No PostgreSQL, Kafka, MinIO or source artifact was deleted. This is not
evidence of an HA rollback or disaster-recovery guarantee.

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
catalog of 193 application objects was exercised only with an explicit
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
The last executable-boundary pass removed library-only verification and
summarization Deployments, made GDS a suspended scope-bound template, removed
the nonexistent LLM Gateway metrics endpoint, and required explicit HTTPS OIDC
issuer/proxy CIDRs. The host-side OpenSearch password hasher is digest-pinned.
These are deterministic render/security checks; no unpublished application was
claimed as live runtime evidence.
ADR-0019 then removed the remaining Kafka confused-deputy paths. Consumer
retries use five explicit per-consumer topics and consumers have no primary
Write ACL. ECI release revision 14 binds Kafka Connect REST to loopback, exposes no Service
and is excluded from the namespace-wide data policy; real allowed/denied
broker publishes and loopback/pod-IP REST probes passed. Vendor release
revision 12 remained Ready after the final upgrade. ADR-0020 then separated
the CDC database identity from the owner: ECI revision 15 reconciled the
least-privilege replication role, migration 0005 and a live connector/slot
successfully, without exposing the generated password.
The final routing review corrected the production-like Neo4j bootstrap DNS to
the installed `neo4j-core-1` release and gives GDS its distinct `neo4j-gds`
endpoint; the dev overlay remains on its real single `neo4j` release. A
regression also proves that `dataPlane.enabled=false` renders no MinIO service,
StatefulSet, PDB or PVC template. These corrections are deterministic render
evidence; they do not claim a licensed Enterprise cluster runtime exercise.
The dev bootstrap now resolves Kubernetes 1.34.0 through the recorded kind node
manifest digest. Production-like Neo4j uses digest-pinned server, operations
and cleanup images, and the GDS installer rejects any missing or mutable custom
image reference. The checked Neo4j-GDS Dockerfile built locally and started on
an internal-only Docker network; `gds.version()` returned `2.13.12`, logs showed
the plugin copied from `/opt/eci`, and no URL resolution/download occurred.
This is offline image-contract evidence, not publication or Enterprise cluster
evidence; the release image still requires trusted registry publication.
The subsequent self-adversarial supply-chain pass also pinned the Strimzi and
CloudNativePG operator manifests plus the Kafka broker and both entity-operator
containers. The real kind cluster upgraded all verified chart archives to
revision 13 and ECI to revision 18; Strimzi observed Kafka generation 4 with
all four runtime container specs on their expected digests, then the complete
readiness, connectivity, OPA and Kafka allowed/denied smoke passed.
The final runtime-routing review pass deduplicated equal service/metrics ports,
kept standalone Redis behind exactly one AOF/PVC-backed StatefulSet backend, and moved the bundled
dev issuer path to HTTPS 8443. ECI revision 20 passed hostname verification,
trusted OIDC discovery, readiness, connectivity, OPA and Kafka ACL smoke; all
vendor releases remained Ready at revision 14. The generated certificate and
private key are intentionally absent from this sanitized evidence directory.
The first Redis migration attempt tried to change the existing ClusterIP to a
headless Service; Kubernetes rejected that immutable-field mutation and Helm's
rollback also reported failure after the new StatefulSet was created. No source
or authoritative datastore was affected. Revision 23 retained the existing
ClusterIP, selected only the new `redis-stateful` pod, removed the old Deployment
and passed the complete smoke. A bounded key survived an explicit Redis pod
restart through its 1 GiB PVC and was then deleted.
The final review hardening keeps the bundled dev issuer entirely on its
namespace/pod-selected 8443 path, rejects mutable overrides for every
value-driven runtime image, and changes all three materialization sinks to
record `processed_events` only after a successful idempotent external write.
Real Postgres-backed failure-path regressions prove that unreachable Neo4j,
Qdrant and OpenSearch writes leave no processed marker and therefore remain
retryable. Qdrant additionally waits for an applied `Completed` result before
the marker; a real Neo4j marker-loss replay proves that an identical MERGE does
not advance GDS partition generation. These are CPU/Docker integration and
deterministic render evidence;
they do not rewrite the earlier live-cluster observations.
The final CDC upgrade regression keeps only the passwordless NOLOGIN privilege
carrier when Connect is disabled, with no `eci_cdc` login role or CDC Secret
reference. PostgreSQL integration applies migration 0005 first, creates the
login role afterward, and proves inherited SELECT. The live kind revision 24
reconciled the same carrier membership, removed the legacy direct grant, and
passed the complete dev verification without replaying or falsifying a
migration.
The closing render review rejects `applications.enabled=true` together with
`dataPlane.enabled=false`: the current chart has no external-backend identity,
trust, or egress contract and therefore cannot claim that topology. This is a
deterministic fail-closed configuration check and does not alter the revision
24 live-cluster evidence.
The dev credential policy also treats the existing `eci-runtime` password as
authoritative. A differing `ECI_DEV_PASSWORD` is rejected before Secret
mutation because the CloudNativePG bootstrap Secret cannot rotate the existing
role. The deterministic unit regression and a real mismatched-override attempt
against the existing kind cluster both exited non-zero before Secret mutation;
neither rotated nor disclosed the live cluster credential.
