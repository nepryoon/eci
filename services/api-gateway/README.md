# API Gateway authentication boundary

T6.1/SPEC-055 implements the OIDC authentication component in
`internal/authn`. It validates Keycloak access tokens before constructing the
shared protobuf `SecurityContext`; tenant, repositories and ACL groups come
only from verified claims. There is no implicit tenant or default scope.

The long-running Envoy edge, JSON/gRPC transcoding, SSE and rate limiting are
T6.6. Until then this module is a reusable internal boundary with no public
listener of its own.

Required production-facing configuration for callers of `authn.New`:

| Field | Meaning |
|---|---|
| `Issuer` | Exact trusted OIDC issuer; HTTPS required outside loopback dev |
| `Audience` | Required JWT audience (`eci-gateway` in the dev realm) |
| `AllowHTTPForDevelopment` | Explicit loopback-only exception; default false |

`Authenticate` additionally requires a valid W3C trace-id supplied by the
gateway tracing layer. It never reads a trace, tenant, repository or group
override from request text.

The local IdP is documented in
[`deploy/compose/README.md`](../../deploy/compose/README.md). Unit and OIDC
cryptographic tests are CPU-only. The Keycloak import/token integration test
uses Docker and the pinned image `quay.io/keycloak/keycloak:26.7.2`; all test
credentials are generated in memory at runtime.
