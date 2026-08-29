# T6.7 security regression matrix

Questa matrice è l'indice eseguibile della suite avversariale. Ogni riga indica
un test reale; non sostituisce il test con un'affermazione documentale.

| Superficie/minaccia | Oracolo deterministico | Esecuzione CI |
|---|---|---|
| JWT mancante/malformed e claim obbligatori | `services/api-gateway/internal/authn`: `TestMissingOrMalformedBearerFailsBeforeVerification`, `TestRequiredClaimsAndLimitsFailClosed` | `build-lint-test` |
| firma, issuer, audience, expiry, algoritmo JWT | `TestOIDCVerificationRejectsSignatureIssuerAudienceExpiryAndAlgorithm` | `build-lint-test` |
| metadata interno/trace/rate/deadline forgiato | `services/api-gateway/internal/edge`: unit e `TestEnvoyGatewayEndToEnd` | `build-lint-test`, `envoy-gateway-integration` |
| PDP deny/outage/protocollo invalido | `libs/go/eci/authz`: `TestPEPDeniesAndFailsClosed`, `TestClientFailsClosedOnStartupAndRuntimeProtocolErrors`, `TestProductionClientAgainstRealPinnedOPA` | `build-lint-test` |
| azione OPA sconosciuta/scope vuoto | `deploy/compose/opa/policies/eci_authz_test.rego` | `build-lint-test` tramite suite OPA integration |
| graph lookup cross-tenant e not-found uniforme | `TestRetrievalEngineServer/T6_3_CrossTenantGetIsIndistinguishableFromMissing` | `datastore-security-integration` |
| traversal/impact oltre confine ACL | `TestRetrievalEngineServer/Scenario3_ExpandNeighbors`, fixture foreign; test impact/scoped query unit | `datastore-security-integration`, `build-lint-test` |
| vector search cross-tenant | `TestHybridSearchIntegration/Scenarios1to3_FusionEndToEnd` (foreign Qdrant point) | `datastore-security-integration` |
| full-text/hydration cross-tenant | `TestRetrievalEngineServer/Scenario6_AuthenticatedScopeCannotBeExpandedByBody`, `TestHybridGraphVectorSearchContentHydration` (`FOREIGN_SECRET`) | `datastore-security-integration` |
| rerank/GDS-derived stale score | `TestRerankIntegration/T6_7_StaleImpactScopeDefaultsToZero` | `datastore-security-integration` |
| proiezione GDS globale/cross-ACL | `TestGDSImpact/Scenarios1And2_FormulaMatchesWrittenValuesAndImpactKind`, fixture con edge verso `tenant-b` | `datastore-security-integration` |
| cache cross-ACL/repository | `TestSemanticCacheServer/Scenario4_DifferentAclScopeMisses`, `TestMissingOrMismatchedScopeIsDeniedBeforeRedis` | `datastore-security-integration`, `build-lint-test` |
| summary con child non autorizzato | `test_restricted_summary_never_exposes_denied_subtree_to_model_cache_or_result`, `test_authorized_descendant_of_denied_intermediate_is_suppressed` | `build-lint-test` |
| citazione fuori scope/backend leaky | `test_missing_and_out_of_scope_citation_are_indistinguishable`, `test_backend_scope_bypass_is_rejected_without_provenance_leak` | `build-lint-test` |
| audit non scritto/modificabile | `test_audit_failure_is_closed_and_recorded_in_trace`, MinIO COMPLIANCE integration | `build-lint-test`, `worm-audit-integration` |
| prompt injection amplia scope/tool | `test_prompt_injection_cannot_change_scope_or_invoke_unknown_tool`, registry esatto dei sette tool | `build-lint-test` |
| query/body prova `allowed_repos`/`acl_groups` | test orchestrator precedente, verification input immutability e server scenario 6 | `build-lint-test`, `datastore-security-integration` |
| baseline evaluation alterata | checksum/versioned evidence T5.6 e regressioni SPEC-054 | `build-lint-test` |

Comandi locali completi: `task build`, `task lint`, `task test`,
`task test:integration`, `task guard`. Le integrazioni richiedono Docker ma sono
CPU-only; nessuna GPU, LLM judge o servizio SaaS è usato.
