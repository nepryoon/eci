# Strategia di Sviluppo AI-First — dall'ADD ECI al codice
**Setup: 1 developer + AI · Versione 1.0 · 31 luglio 2026**

**Decisioni di tooling (fisse, non riaperte oltre):**
1. **Claude Code** (CLI/VS Code) come *executor* — implementa, testa, rifattorizza dentro il repo. Una sessione = un task atomico del task-tree.
2. **Questa chat** (con l'ADD in contesto) come *architetto* — scrive e rifinisce le SPEC, non il codice. La separazione spec/implementazione è il guardrail principale con un solo dev.
3. **Monorepo** unico: contratti condivisi (D2/D3/D7), refactoring cross-service atomici, un solo contesto per l'AI. Multi-repo si giustifica solo con più team.
4. Interfaccia comandi unica: **Taskfile** (`task build|test|up|proto:gen|…`) — l'AI non deve mai inventare comandi.
5. Codegen contratti: **buf** (protobuf → Go/Python), **datamodel-code-generator** (JSON Schema → Pydantic). Ambiente locale: **docker compose** + **testcontainers**. K8s solo in Fase 7.

---

## 1. Struttura del contesto e dei repository

```
eci/
├── CLAUDE.md                      # contesto principale AI (template sotto); symlink .cursorrules
├── Taskfile.yml                   # interfaccia comandi unica
├── docs/
│   ├── add/                       # ADD consolidato + diagramma master — SOLA LETTURA
│   ├── specs/                     # SPEC-NNN-slug.md (una per task) + _TEMPLATE.md
│   ├── decisions/                 # ADR-NNN.md — ogni deviazione dall'ADD passa da qui
│   └── runbooks/
├── contracts/                     # SOLA LETTURA per l'AI (modifiche solo via ADR)
│   ├── proto/eci/retrieval/v1/retrieval.proto     # Deliverable D7
│   ├── jsonschema/hybrid-graph.json               # Deliverable D2
│   ├── cypher/schema.cypher                       # Deliverable D3
│   ├── sql/                       # DDL code_node/code_relation/outbox/processed_events
│   ├── events/                    # schema eventi outbox→Kafka
│   ├── buf.yaml / buf.gen.yaml
├── services/
│   ├── ingestion/                 # Rust  (+ CLAUDE.md locale con regole Rust)
│   ├── sink-graph/  sink-vector/  sink-search/    # Go
│   ├── retrieval-engine/          # Go
│   ├── orchestrator/              # Python — LangGraph + PydanticAI
│   ├── verification/              # Python
│   ├── llm-gateway/  semantic-cache/              # Go
│   └── summarization/             # Python
├── libs/
│   ├── go/   (securitycontext, otelinit, kafkautil)
│   ├── py/   (eci_core: tipi generati, client gRPC, otel)
│   └── rust/ (cpg_model, merkle)
├── fakes/                         # vllm-fake, embedder-fake, retrieval-stub
├── deploy/
│   ├── compose/                   # dev locale (PG17, Kafka KRaft, Debezium, Neo4j, Qdrant, OpenSearch, MinIO)
│   └── k8s/                       # Fase 7
├── tests/
│   ├── integration/               # testcontainers
│   ├── e2e/                       # pipeline completa su fixture
│   ├── fixtures/sample-repo/      # mini-repo di codice target per tutti i test
│   └── golden/                    # golden dataset query→risposta attesa (eval)
└── .github/workflows/ci.yml       # lint+test Go/Py/Rust + buf breaking + guard paths
```

Regole strutturali: (1) `docs/add/` e `contracts/` sono read-only per l'AI — un job CI (`task guard`) fallisce la PR se toccati senza un ADR nello stesso commit; (2) ogni servizio ha un `CLAUDE.md` locale di ≤30 righe con le sole regole specifiche del linguaggio; (3) i fake vivono in `/fakes` e sono cittadini di prima classe: tutto lo sviluppo fino alla Fase 5 gira su LLM/embedder finti e deterministici.

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

---

## 2. Workflow "Spec-to-Code"

**Granularità (regola fissa):** 1 task del task-tree = 1 SPEC = 1 PR = 1 modulo/package + 1 file di test omonimo. Dimensione target: 200–600 righe prodotte, ≤1 giornata. Se una SPEC supera 8 scenari Given/When/Then o tocca più di un servizio → si spezza. Le SPEC sono l'unità di memoria del progetto: il codice si rigenera, le SPEC no.

**Formato esatto della SPEC** (`docs/specs/_TEMPLATE.md`):

```markdown
# SPEC-NNN — <titolo>
Stato: draft | approved | implemented | verified
Task-tree: T<fase>.<n> · Servizio: services/<nome> · ADD: §<riferimenti puntuali>
Contratti: contracts/<file usati>

## 1. Obiettivo — 2-3 frasi, cosa esiste dopo che prima non esisteva.
## 2. Interfaccia — firma ESATTA: RPC del proto / funzione pubblica / CLI /
     schema evento. Nessuna prosa: codice o schema.
## 3. Comportamento — scenari numerati Given/When/Then (diventano i test 1:1).
## 4. Errori & edge case — tabella: condizione → comportamento atteso.
## 5. Non-goals — cosa NON implementare (àncora anti-scope-creep per l'AI).
## 6. Vincoli dall'ADD — citazioni puntuali (es. "sink idempotente, M1 §2.2.4").
## 7. Test plan — unit / integration (testcontainers: quali container) /
     proprietà (es. idempotenza: doppio replay ⇒ stato identico).
## 8. Osservabilità — span e metriche da esporre (nomi esatti).
## 9. Criteri di accettazione — checklist eseguibile, comandi inclusi.
```

**Prompt esatto per chiedermi una SPEC** (in questa chat):

```
Scrivi SPEC-<NNN> per il task T<x.y> del task-tree.
Template: docs/specs/_TEMPLATE.md. Fonti: ADD §<...>, contracts/<...>.
Vincoli aggiuntivi: <...>.
Output: solo il contenuto del file docs/specs/SPEC-<NNN>-<slug>.md. Nessun codice.
```

**Prompt esatto per Claude Code** (implementazione):

```
Implementa docs/specs/SPEC-<NNN>-<slug>.md.
1) Leggi CLAUDE.md e la SPEC. 2) Scrivi PRIMA i test da §3 e §7 e verificane il
fallimento. 3) Implementa fino a `task lint && task test` verdi. 4) Stato SPEC →
implemented, annota le deviazioni. Non toccare file fuori da services/<nome> e tests/.
```

Ciclo completo: (1) SPEC qui → (2) tua review/approvazione (`approved`) → (3) Claude Code implementa → (4) tua review della PR con la SPEC a fianco → (5) merge, stato `verified`. Il punto (2) è dove spendi il giudizio: rivedere 100 righe di SPEC costa un decimo di rivedere 600 righe di codice sbagliato.

---

## 3. Roadmap di esecuzione (task-tree)

Legenda delega: 🟢 = delega ~90% (review leggera) · 🟡 = delega con review strutturata (test prima, lettura riga-per-riga dei punti critici) · 🔴 = human-in-the-loop stringente (tu progetti, l'AI assiste). Dettaglio in §4.

### Fase 0 — Fondamenta: contratti, tipi, mock, test env *(tutto il resto dipende da qui)*

| ID | Task atomico (output verificabile) | Dip. | Delega |
|---|---|---|---|
| T0.1 | Scaffold monorepo §1 + Taskfile + CI (lint/test 3 linguaggi, `task guard`) | — | 🟢 |
| T0.2 | `contracts/proto` = D7 reale + buf codegen Go/Py + breaking-check in CI | T0.1 | 🟢 |
| T0.3 | `contracts/jsonschema` = D2 + codegen Pydantic/Go structs; schema eventi outbox | T0.1 | 🟢 |
| T0.4 | `contracts/cypher` = D3 come migration idempotente + `task db:neo4j:migrate` | T0.1 | 🟡 |
| T0.5 | DDL PG: code_node/code_relation/outbox/processed_events + migration runner | T0.1 | 🟡 |
| T0.6 | Compose dev: PG17+Debezium/Connect(config JSON as code)+Kafka KRaft+Neo4j+Qdrant+OpenSearch+MinIO, healthcheck su `task up` | T0.5 | 🟡 |
| T0.7 | Harness testcontainers Go/Py + smoke test: INSERT outbox → evento sul topic | T0.6 | 🟡 |
| T0.8 | `libs/*`: SecurityContext (tipi dal proto) + interceptor gRPC + header Kafka; bootstrap OTel; config loader | T0.2 | 🔴 |
| T0.9 | Fakes deterministici: vllm-fake (API OpenAI-compatible), embedder-fake (hash→vettore 1536 stabile), retrieval-stub dal proto | T0.2 | 🟢 |
| T0.10 | `tests/fixtures/sample-repo` (mini-progetto nel linguaggio target 1) + golden dataset v0 (10 query→attese) | T0.1 | 🟡 |

### Fase 1 — Walking skeleton verticale *(1 linguaggio, gamba grafo, LLM fake, no security)*

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T1.1 | ingestion v0 (Rust): parse linguaggio 1 con Tree-sitter → File/Class/Method/CallSite + CONTAINS/CALLS intra-file | T0.10 | 🟡 |
| T1.2 | Scrittura PG: tx ACID upsert entità + riga outbox (pattern M1 §2.2.1) | T1.1, T0.5 | 🟡 |
| T1.3 | sink-graph v0: consume → MERGE idempotente Neo4j + processed_events | T0.7 | 🟢 |
| T1.4 | retrieval-engine v0: GetNode, ExpandNeighbors, HybridSearch (sola gamba grafo) — server gRPC dal proto | T0.2, T1.3 | 🟡 |
| T1.5 | orchestrator v0: query→retrieval→prompt→vllm-fake→risposta con provenance; CLI `eci ask` | T1.4, T0.9 | 🟢 |
| T1.6 | Test E2E: commit su fixture → `eci ask "chi chiama X?"` → risposta con citazioni corrette | T1.1–T1.5 | 🟡 |

**Milestone A** — pipeline viva end-to-end. Da qui ogni fase allarga una dimensione alla volta.

### Fase 2 — Ingestion completa (ADD M1)

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T2.1 | Merkle SHA-256 + normalizzazione (whitespace/commenti; identificatori nei 2 regimi) + property test | T1.1 | 🟡 |
| T2.2 | doc_hash separato + chunking cAST configurabile per linguaggio | T2.1 | 🟢 |
| T2.3 | Semantic Cache service (Go): chiave ast_hash+logic_fingerprint, campo acl_scope predisposto | T2.1 | 🟡 |
| T2.4 | Embedding reale: TEI + jina-code-1.5b (profilo compose `gpu`), client con fallback al fake | T0.9 | 🟢 |
| T2.5 | Linguaggi 2-3 + name resolution cross-file (stack-graphs): CALLS/IMPORTS/EXTENDS/IMPLEMENTS cross-file | T1.1 | 🟡 |
| T2.6 | Rename/move (git diff -M, cache content-addressed), lineage, GC, TTL | T2.1 | 🟡 |

### Fase 3 — Consistenza distribuita completa

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T3.1 | sink-vector: upsert Qdrant con payload (node_id, domain, provenance) | T2.4 | 🟢 |
| T3.2 | sink-search: index OpenSearch per doc-id | T0.6 | 🟢 |
| T3.3 | DLQ + retry/backoff + metriche lag Prometheus su tutti i sink | T3.1 | 🟢 |
| T3.4 | Job di riconciliazione fingerprint PG ↔ tre viste | T3.1–3.2 | 🟡 |

### Fase 4 — Retrieval avanzato (ADD M2)

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T4.1 | HybridSearch completo: seed Qdrant → ancoraggio → espansione → RRF k=60 (port in Go della D5, test di parità con la reference Python) | T3.1 | 🟡 |
| T4.2 | ImpactAnalysis streaming: reverse reachability bounded, cap fan-out, impact_kind, ImpactProgress | T4.1 | 🟡 |
| T4.3 | GDS batch: proiezioni + PPR seedato/betweenness campionata/Leiden → write-back impact_score | T3.4 | 🟡 |
| T4.4 | Reranker TEI bge-v2-m3 + final_score = rerank + β·proximity | T4.1 | 🟢 |
| T4.5 | Context packing "a U" + budgeting per sezioni + dedup | T4.4 | 🟡 |

### Fase 5 — Agente, LLM reale, Verification

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T5.1 | Orchestrator LangGraph: grafo di stato, tool tipizzati PydanticAI sui client gRPC, budget 15-30 passi, criteri di stop, stato visita | T4.2 | 🟡 |
| T5.2 | Intent classifier (routing strutturale/concettuale → pesi fusione) | T5.1 | 🟢 |
| T5.3 | LLM Gateway: routing fake/reale, streaming, circuit breaker, deadline | T0.9 | 🟡 |
| T5.4 | Verification service: claim extraction strutturata → symbol/relation/citation check → re-parse Tree-sitter → esiti con loop bounded 2-3 | T4.1 | 🟡 |
| T5.5 | Summarization RAPTOR bottom-up con cache (stessa chiave) | T2.3 | 🟢 |
| T5.6 | vLLM reale (Qwen3-Coder-30B-A3B FP8 su 1 GPU dev) + prima eval sul golden dataset | T5.3 | 🟡 |

**Milestone B** — risposta verificata deterministicamente con LLM reale.

**Follow-up di quality hardening (non modifica retroattivamente T5.6 o Milestone B):**

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T5.7 | Deterministic output canonicalization & eval hardening | T5.6 | 🟡 |

### Fase 6 — Security (ADD M3) — *human-in-the-loop di default*

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T6.1 | IdP dev (Keycloak) + JWT→SecurityContext al gateway | T0.8 | 🔴 |
| T6.2 | PDP (OPA) + PEP negli interceptor di tutti i servizi | T6.1 | 🔴 |
| T6.3 | Enforcement in-query: filtri Cypher obbligatori + RBAC Neo4j (grants); Qdrant is_tenant + must-filter; OpenSearch DLS | T6.2 | 🔴 |
| T6.4 | acl_scope in cache/summary + regola "visibile solo se autorizzato su TUTTI i figli" | T6.3 | 🔴 |
| T6.5 | ACL-check citazioni in Verification + audit log WORM | T6.3 | 🔴 |
| T6.6 | API Gateway Envoy: transcoding gRPC-JSON + SSE, rate limiting | T6.1 | 🟡 |
| T6.7 | Security test suite: cross-tenant leakage, prompt injection vs allow-list tool, GDS per-ACL | T6.1–6.5 | 🔴 |

### Fase 7 — Deploy & Observability (ADD M4)

| ID | Task | Dip. | Delega |
|---|---|---|---|
| T7.1 | Manifests/Helm + operatori (Strimzi, CloudNativePG, Neo4j, Qdrant, OpenSearch) su cluster dev | T6.6 | 🟡 |
| T7.1a | Ingestion worker runtime long-running: ingresso commit autenticato/durabile, readiness e backpressure reali | T7.1 | 🔴 |
| T7.1b | Orchestrator runtime long-running: API autenticata, streaming, readiness e shutdown reali sopra il grafo T5.1 | T7.1 | 🔴 |
| T7.2 | Autoscaling: HPA CPU parsing, KEDA Kafka lag sui sink, KEDA num_requests_waiting su vLLM | T7.1a | 🟡 |
| T7.3 | OTel end-to-end (span links Kafka, gen_ai.*) + dashboard Grafana + alert | T7.1b | 🟢 |
| T7.4 | Pipeline eval: Ragas campionata + tassi deterministici come metriche Prometheus + eval CI sul golden | T5.6 | 🟡 |
| T7.5 | Load test k6 contro le latenze target della matrice D9 | T7.2, T7.3 | 🟡 |

---

## 4. Guardrail & Matrice di Delega

**🟢 Delega ~90% (l'AI genera, tu leggi in diagonale + CI):** scaffolding, Taskfile/CI, compose, codegen e wiring dei contratti, fakes, sink consumers (la loro correttezza è *provata* dal test di idempotenza doppio-replay, non dalla review), client wrapper, chunking cAST, reranker wiring, summarization, dashboard/alert, documentazione. Criterio: logica semplice + oracolo di test meccanico.

**🟡 Delega con review strutturata (l'AI genera con TDD; tu leggi riga-per-riga i punti critici):** parser Rust e name resolution (correttezza semantica del CPG), Merkle+normalizzazione (i property test sono obbligatori: stesso codice riformattato ⇒ stesso hash), traversate Cypher bounded e ImpactAnalysis (performance: EXPLAIN in review), fusione RRF (test di parità con la reference D5), LangGraph/tool (comportamento del loop: verifica budget e stop su trace), Verification (è deterministico, quindi testabile — ma è il gate: review completa), Debezium/migrations, KEDA/HPA. Criterio: logica non banale ma con oracolo verificabile.

**🔴 Human-in-the-loop stringente (tu progetti e decidi; l'AI scrive sotto dettatura; review integrale + test avversari):**
1. **Tutta la Fase 6** e la lib SecurityContext (T0.8): interceptor, costruzione filtri, RBAC grants, is_tenant, DLS, acl_scope, audit. Regola ADD: i filtri derivano SOLO dal contesto autenticato — qualsiasi "ottimizzazione" proposta dall'AI qui è sospetta per definizione.
2. **Modifiche a `contracts/` e deviazioni dall'ADD**: prima l'ADR (scritto da te, l'AI può fare da avvocato del diavolo), poi il codice.
3. **Prompt di sistema dell'agente e allow-list dei tool** (superficie di prompt-injection).
4. **Proiezioni GDS per-ACL** (gap documentato nell'ADD: nessuna garanzia che GDS rispetti il RBAC fine-grained).
5. **Deploy in produzione, segreti, licensing** (Neo4j EE a preventivo).

**Guardrail operativi trasversali (meccanici, non di disciplina):**
1. `task guard` in CI: PR rossa se tocca `contracts/` o `docs/add/` senza ADR nel diff.
2. Test-first obbligatorio: la PR che aggiunge codice senza i test della SPEC §3 non passa la review checklist.
3. Ogni sink/mutazione ha il test di idempotenza "esegui due volte ⇒ stato identico".
4. Checklist fissa in ogni PR 🔴: threat model 5 righe, "cosa succede se questo input è ostile?", secondo passaggio di review a distanza di un giorno (sei un dev solo: il tempo è il tuo secondo revisore).
5. Limite PR ~600 righe: oltre, la SPEC andava spezzata — si spezza retroattivamente.
6. Il golden dataset (T0.10) gira in CI da Fase 1 in poi: le regressioni di qualità si vedono al commit, non in demo.

**Ordine di esecuzione consigliato per un dev solo:** F0 sequenziale (T0.1→T0.10, ~i primi giorni sono solo fondamenta) → F1 completa prima di qualunque ampliamento → poi F2/F3 intrecciabili → F4 → F5 → F6 (a mente fresca, mai in coda a giornate lunghe) → F7. Le SPEC si scrivono a lotti di 3-5 (una mezza giornata qui in chat), si implementano una alla volta in Claude Code.
