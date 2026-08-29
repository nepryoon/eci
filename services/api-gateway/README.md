# API Gateway internal edge helper

T6.1/SPEC-055 implements the OIDC authentication component in
`internal/authn`. It validates Keycloak access tokens before constructing the
shared protobuf `SecurityContext`; tenant, repositories and ACL groups come
only from verified claims. There is no implicit tenant or default scope.

T6.6/SPEC-060 wires it into the internal HTTP helper used by Envoy ext-authz
and adds the browser SSE adapter. This process is not a public gateway: Envoy
is the sole external listener and removes reserved metadata before calling it.

Required process configuration:

- `ECI_OIDC_ISSUER`: exact trusted issuer; HTTPS outside loopback dev;
- `ECI_OIDC_AUDIENCE`: required JWT audience;
- `RETRIEVAL_ENGINE_ADDR`: internal gRPC `host:port`, with no default.

Optional bounded configuration:

- `ECI_OIDC_ALLOW_HTTP_DEV` (default `false`);
- `API_GATEWAY_ADDR` (default `:8081`, internal only);
- `API_GATEWAY_METRICS_ADDR` (default `:9107`, internal only);
- `API_GATEWAY_MAX_JSON_BODY_BYTES` (default/max `1048576`);
- `API_GATEWAY_REQUEST_TIMEOUT` (default/max `30s`).

`Authenticate` additionally requires a valid W3C trace-id supplied by the
gateway tracing layer. It never reads a trace, tenant, repository or group
override from request text.

The local IdP is documented in
[`deploy/compose/README.md`](../../deploy/compose/README.md). Unit and OIDC
cryptographic tests are CPU-only. The Keycloak import/token integration test
uses Docker and the pinned image `quay.io/keycloak/keycloak:26.7.2`; all test
credentials are generated in memory at runtime.

The public listener, pinned image and deterministic descriptor are documented
in [`deploy/envoy/README.md`](../../deploy/envoy/README.md). Never expose the
helper, metrics, retrieval gRPC or Envoy admin port directly.
