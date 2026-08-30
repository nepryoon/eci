# Neo4j 5.26 + GDS 2.13 image

This release image exists because production-like Neo4j pods have no internet
egress. It layers the Neo4j-5.26.30-compatible GDS 2.13.12 JAR into the pinned
official Neo4j base under root-owned `/opt/eci`. Both the base manifest and
remote build input are verified by SHA-256. The image changes only GDS's local
source in Neo4j's plugin catalog; `NEO4J_PLUGINS` copies that file and applies
the normal defaults without resolving a network URL. Keeping the source outside
`NEO4J_HOME` also prevents the entrypoint's ownership preparation from making
the trusted artifact writable by the runtime user.

Build and publish this image through the trusted release builder, sign it, and
pass its registry manifest digest to `deploy/k8s/install-operators.sh`:

```bash
docker build --provenance=false -t REGISTRY/eci-neo4j-gds:5.26.30-2.13.12 deploy/images/neo4j-gds
docker push REGISTRY/eci-neo4j-gds:5.26.30-2.13.12
export NEO4J_GDS_IMAGE='REGISTRY/eci-neo4j-gds@sha256:<registry-manifest-digest>'
```

Never substitute a local image ID, mutable tag, or startup download. Publishing
is intentionally external to the cluster installer so registry credentials do
not enter the repository or operator host command history.
