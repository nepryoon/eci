# Envoy edge (T6.6)

`envoy.yaml` is the production-shaped static bootstrap for the public ECI
edge. It expects the internal DNS names `api-gateway:8081` and
`retrieval-engine:50053`; only Envoy port 8080 may be published. Port 9901,
the helper, metrics and gRPC backend remain cluster-internal.

The supported image is pinned by tag and multi-architecture manifest digest:
`envoyproxy/envoy:v1.39.0@sha256:d59f7f5fa10cff6d5892b6c5e7df5c9297ddfb2c3683e33fbfb82da24de4fa66`.
Validate the checked-in config and descriptor with:

```bash
task envoy:descriptor
task envoy:validate
```

`task envoy:descriptor` must produce no Git diff. `task envoy:validate`
requires Docker and mounts the exact checked-in files read-only. The CPU-only
integration suite renders only the two internal DNS endpoints to host-access
test ports, starts the same pinned Envoy image and proves auth, JSON/gRPC,
SSE and rate-limit behavior.

Never publish `api-gateway:8081`, retrieval service ports, or Envoy admin
port 9901. A failure of the helper denies protected traffic because
`failure_mode_allow` is false.
