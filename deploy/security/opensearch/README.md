# OpenSearch DLS configuration

These Security plugin files define the read-only query-plane role. The
Retrieval Engine constructs extended-proxy attributes only from the validated
`SecurityContext`; OpenSearch independently applies the same tenant/repository/
ACL predicate through DLS. Deploy this behind mTLS/network policy so only the
trusted proxy identity can reach OpenSearch. Missing attributes cause DLS
substitution to fail; there are no fallback values or unrestricted read role.

The current Compose profile keeps the Security plugin disabled and is dev-only.
It validates the application pre-filter, not native DLS. T7.1 supplies the
production Kubernetes secret/certificate wiring; no credentials are stored
here.
