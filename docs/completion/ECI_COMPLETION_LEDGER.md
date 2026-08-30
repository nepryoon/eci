# ECI completion ledger

Aggiornato: 2026-08-30 · Branch: `codex/complete-eci-roadmap`

## Preflight e preservazione

| Voce | Evidenza |
|---|---|
| Repository root | `/home/luca/projects/eci`, remote `origin` = `https://github.com/nepryoon/eci.git` |
| Base selezionata | `5193e62bc4a572cb7764a72b3100378fdee0c1d1` (`origin/feat/t7-1a-ingestion-runtime`), discendente diretto di `origin/main` `b5b5ed5e752492a13608e940b484fe763a29b699` |
| Branch di completamento | `codex/complete-eci-roadmap`; nessun merge su `main` |
| Lavoro preesistente preservato | `stash@{0}` / oggetto `b7b04cb2adf4a85b65f8eb9fe522615733a6a49d`, messaggio `pre-codex-completion-2026-08-30`; contiene tre artefatti T5.6 non tracciati |
| Inventario iniziale | 591 file tracciati, 3,955,835 byte; 17 `go.mod`, 2 `Cargo.toml`, 7 `pyproject.toml` |
| Istruzioni lette | `CLAUDE.md`, `PLAYBOOK.md`; nessun `AGENTS.md` o `.cursorrules` tracciato/presente |
| SPEC raggiungibili | 001–062 e 067; 063–066 assenti da ogni ref locale/remoto raggiungibile; prossimo numero 068 |

## Baseline invariata

| Comando | Risultato iniziale | Evidenza sintetica |
|---|---|---|
| `task build` | PASS | moduli enumerati dallo script corrente compilano |
| `task lint` | PASS | vet/clippy/ruff correnti verdi |
| `task test` | FAIL iniziale; PASS dopo SPEC-068 | il baseline tentava Keycloak, OPA e orchestrator/Neo4j senza daemon; ora il gate CPU è separato e verde |
| `task test:integration` | FAIL (ambiente) | daemon non disponibile: socket `/home/luca/.docker/desktop/docker.sock` assente; prima suite Neo4j fallisce nell'acquisizione provider |
| `task guard` | PASS ma incompleto | protegge `contracts/*` e solo `docs/add/*`, non il vero ADD consolidato |
| `task proto:lint` | PASS | Buf 1.72.0; warning deprecazione categoria `DEFAULT` |
| `task proto:breaking` | PASS | confronto con branch locale `main` |
| `task proto:gen` + clean diff | PASS | Go/Python deterministici |
| `task schema:gen` + clean diff | PASS | output deterministico; warning formatter futuro |
| `task envoy:descriptor` + clean diff | PASS | descriptor deterministico |
| `task envoy:validate`, `task test:envoy` | FAIL (ambiente) | entrambi richiedono daemon Docker non disponibile |
| `task k8s:validate` | PASS | Helm lint, 3 render/policy/kubeconform set e 23 test |

## Traceability backlog

| Requirement / finding | Fonte autorevole | Moduli | Evidenza esistente | Comportamento mancante | Test richiesti | Implicazioni security | Dipendenza | Stato | Comando/evidenza di verifica |
|---|---|---|---|---|---|---|---|---|---|
| Superficie build/lint/test completa e ADD reale protetto | ADD registro D1–D9; PLAYBOOK T0.1; harness §6.9 | scripts, Taskfile, CI, tutti i manifest | SPEC-068 implementa inventario dinamico per 13 Go/2 Rust/7 Python, target fake/interop/E2E/security, codegen e guard del vero ADD; gate CPU verdi | manca solo osservare le suite Docker/CI complete su un daemon disponibile | inventario manifest, guard sintetico, target CPU e integration | suite security non può essere accidentalmente omessa | prima riparazione | in_progress | `task build`, `task lint`, `task test`, `task test:fakes`, `task test:security`, interop, codegen, guard e K8s PASS; Docker target falliscono esplicitamente per ambiente |
| Verifica indipendente T7.1a | PLAYBOOK T7.1a; SPEC-067; ADR-0023 | ingestion, chart, migration | implementazione a `5193e62`; 86 unit Rust e 5 test runtime CPU osservati verdi; K8s validate verde | checklist SPEC ancora `implemented`; integration Docker e cluster non rieseguiti | unit, integration due volte, Helm, security, shutdown | ingresso autenticato, scope server-side, TLS, backpressure | SPEC-068 | open | `cargo test`; `task test:integration`; `task k8s:dev:verify` quando disponibile |
| Semantica completa `ImpactAnalysisRequest` | D7; contratto protobuf; ADD M2 §§1.2–1.6; ADR-0024; SPEC-069 | retrieval-engine, generated clients, Envoy descriptor | tutti i campi applicati; fan-out top-N per padre, caps distinti, multi-repo intersect, score, direction/type whitelist, path completo, kind, source batch e status sanitizzati; Go/Python rigenerati | esecuzione integration Neo4j/OpenSearch bloccata dal daemon assente; SPEC resta `implemented` | unit + Neo4j/OpenSearch integration + auth/cancel adversarial | filtri pre-query, intersezione vuota non amplia scope, hydration rifiltrata | SPEC-068; necessario ai runtime query | in_progress | red test osservato; narrow/unit/vet, `task build/lint/test`, Buf, codegen clean, descriptor clean, guard PASS; `task test:integration` fallisce prima delle suite sul socket Docker |
| Tombstone/delete end-to-end | ADD M1 §§1.6.4–1.6.6, 2.2; event schema `DELETE`; ADR-0025; SPEC-070–074 | ingestion, PG/outbox/CDC, shared Go metadata, embedding worker, 3 sink, cache, summary, reconcile | canonical DELETE/CDC/parser; sink-graph scoped node/relation deletion; embedding-worker valida CodeChunk DELETE e registra solo marker senza embedder, embedding o secondo outbox | esecuzione Docker-backed bloccata; Qdrant/OpenSearch DELETE e invalidazioni/reconciliation restano | ACID/replay/idempotenza/failure/cross-tenant integration, CDC header reale, projection consumers | metadata non-disclosing; graph scope/type gates; embedding delete non ricrea dati e valida provenance; marker post-effetto | SPEC-070/071/073/074 implemented, SPEC-072 verified; vector/search SPEC successive | in_progress | embedding red/feature `bbd2e43`/`8a4fd79`; unit/race/vet, integration compile, build/lint/test/security PASS; Docker blocked |
| T7.1b orchestrator runtime | PLAYBOOK T7.1b; ADD M2/M3/M4 | orchestrator, chart | grafo T5.1 e CLI verificati | API long-running autenticata/streaming, probe reali, limiti, shutdown, OTel, workload | unit/integration/security/render | contesto solo metadata autenticati, fail-closed | SPEC-068 + Impact semantics | open | nuovo SPEC |
| T7.1c verification runtime | PLAYBOOK T7.1c; ADD M2 §3/M3 | verification, chart | verifier deterministico e audit WORM | API long-running, auth, deadline, probes, shutdown, workload | unit/integration/adversarial/render | gate fail-closed e ACL citazioni | orchestrator runtime contract decisions | open | nuovo SPEC |
| T7.1c summarization runtime | PLAYBOOK T7.1c; ADD M2 §2.1/M3 | summarization, cache, chart | RAPTOR + ACL scope unitari | API long-running, auth, deadline, probes, shutdown, workload | unit/integration/adversarial/render | summary visibile solo per scope completo figli | verification runtime | open | nuovo SPEC |
| SPEC-062 verifica finale piattaforma | SPEC-062; PLAYBOOK T7.1 | deploy/k8s | Helm static validation verde | resta `implemented`; richiede workload runtime reali e cluster evidence | render + kind quando risorse disponibili | secret enumeration, mTLS/network policy | T7.1a/b/c | open | promuovere solo dopo runtime verificati |
| T7.2 autoscaling | ADD M4 §2.4; PLAYBOOK T7.2 | Helm/KEDA/HPA | operator pins nella piattaforma | HPA ingestion; lag ScaledObject; vLLM/cache triggers; bounds/fallback/stabilization/PDB | schema/policy/render + runtime solo se osservabile | metric auth/TLS e nessun secret in trigger | T7.1a/b/c | open | nuovo SPEC per sottosistema |
| T7.3 observability | ADD M4 §§3.1–3.2; PLAYBOOK T7.3 | tutti i runtime, collector/backends/Grafana | librerie OTel e metriche locali parziali | continuità trace + Kafka links; schema GenAI pin; collector, Tempo/Prometheus/Loki/Grafana, dashboard/alert | unit propagation, integration continuity, promtool/dashboard provisioning | vietati prompt/source/scope/segreti | runtime T7.1 | open | SPEC per collector/backends e per instrumentation |
| T7.4 evaluation continua | ADD M4 §3.3; PLAYBOOK T7.4 | orchestrator/eval, verification, CI | T5.6 evidence e golden deterministic harness | sampled provider-neutral Ragas path, hard metrics, pins, offline CI/optional real model | golden/fake smoke + metric tests | dataset/prompt privacy e judge non-gate | runtime metrics | open | nuovo SPEC |
| T7.5 k6 load/latency | ADD D9; PLAYBOOK T7.5 | tests/load, CI, runbooks | nessun target k6 rilevato | pinned k6 scenarios, D9 profiles/thresholds, SSE/backpressure/failure/recovery, CPU fake smoke, sanitized result format | deterministic smoke + optional scale profile | auth tokens non registrati; risultati sanitizzati | autoscaling + observability | open | nuovo SPEC |
| ADR/SPEC/TODO/documentation reconciliation | hierarchy completa; harness §5.2/§6.9 | docs + tutti i sorgenti | SPEC-001–061 verified, 062/067 implemented | classificare marker storici/fake; correggere commenti/stati solo con evidenza | doc consistency scanner + gate completi | evitare claim/evidence fabbricati | continuo | open | ledger aggiornato per ogni SPEC |

## Blocker esterni correnti

- Il daemon Docker locale non è raggiungibile. Comando di riproduzione: `docker info`; errore: socket Docker Desktop configurato ma assente. Le suite Docker-backed restano obbligatorie e non saranno marcate verificate finché non eseguite in un ambiente disponibile.
- Nessuna evidenza enterprise-scale/GPU o cluster viene dedotta dai test statici. Gli eventuali criteri che richiedono hardware restano aperti con profili deterministici locali completati separatamente.
