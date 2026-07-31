### Template `CLAUDE.md` di root (personalizzato sull'ADD)

```markdown
# ECI — Enterprise Code Intelligence

GraphRAG system over multi-million-LoC codebases. Microservices, hybrid storage
(Neo4j + Qdrant + OpenSearch) coordinated via CDC. Full architecture in
`docs/add/` (read-only). Contracts in `contracts/` (read-only).

## Source-of-truth hierarchy (on conflict: STOP and ask, or open an ADR)
docs/add/ (ADD) > contracts/ > docs/specs/ > existing code > your judgement.

## Non-negotiable architectural invariants (from the ADD)
- PostgreSQL is the ONLY source of truth. Neo4j/Qdrant/OpenSearch are
  rebuildable materialized views. Never write to them except via sink consumers.
- Every canonical mutation = ONE ACID tx: entity upsert + outbox row. No direct
  publishing to Kafka from services.
- Delivery is at-least-once: every sink MUST be idempotent (MERGE / upsert /
  doc-id index) and record event_id in processed_events.
- CPG is statement-level; detailed AST is a node attribute, never first-class nodes.
- Cache key is ast_hash + logic_fingerprint (+ acl_scope from Phase 6). A model
  or prompt upgrade MUST change logic_fingerprint.
- Retrieval fusion is RRF with k=60. Do not "improve" it.
- Reverse reachability is bounded: depth K (default 4 CALLS / 2 EXTENDS-IMPLEMENTS),
  DISTINCT semantics, fan-out cap. Never unbounded var-length expansion.
- SecurityContext {tenant_id, user_id, allowed_repos, acl_groups, trace_id}
  comes ONLY from authenticated metadata. NEVER build security filters from
  LLM output or user text. Pre-retrieval filters on ALL three legs.
- Final answers pass the deterministic Verification gate (symbol/relation/
  citation checks + Tree-sitter re-parse). Probabilistic checks are pre-filters only.
- gRPC deadline budget ~30s end-to-end: Retrieval 8s, Rerank 3s, LLM 15s
  (streaming), Verification 4s. Propagate deadlines; retry only idempotent RPCs.

## Stack per service (do not change languages)
ingestion=Rust(tree-sitter,stack-graphs) · sinks/retrieval-engine/llm-gateway/
semantic-cache=Go · orchestrator(LangGraph+PydanticAI)/verification/
summarization=Python · reranker=TEI(bge-reranker-v2-m3) · embeddings=
jina-code-embeddings-1.5b, 1536-dim.

## Commands (the ONLY way to build/test — never invent alternatives)
task up / task down      # local stack (compose)
task build | lint | test # all languages
task test:integration    # testcontainers (requires docker)
task proto:gen           # buf codegen after proto changes (needs ADR)
task db:migrate          # PG + Neo4j migrations
task guard               # verifies contracts/ and docs/add/ untouched

## Workflow rules
1. Implement exactly ONE SPEC (docs/specs/SPEC-NNN-*.md) per session/PR.
2. TDD: write the tests from SPEC §3/§7 FIRST, watch them fail, then implement.
3. Touch only the service dir named in the SPEC + its tests. Nothing else.
4. When done: `task lint && task test` green, set SPEC status to `implemented`,
   summarize deviations (if any) at the bottom of the SPEC.
5. Never commit secrets; use .env.example. Never bypass task guard.
6. If the SPEC conflicts with the ADD or a contract: STOP, report, do not code.

## Definition of Done (every task)
lint green · unit+integration tests green · idempotency proven where relevant
(run twice, same state) · OTel span added for new operations · SPEC updated.
```

Lo stesso file funziona identico come `.cursorrules` (symlink). In inglese perché è un artefatto operativo per il tool ed è portabile; le SPEC restano in italiano.

