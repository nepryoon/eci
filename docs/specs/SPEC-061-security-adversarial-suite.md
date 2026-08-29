# SPEC-061 — Security adversarial suite e GDS per-ACL
Stato: implemented
Task-tree: T6.7 · Servizi: `tools/gds-impact`, `services/retrieval-engine`, `services/orchestrator`, suite security esistente · ADD: Modulo 3 §2.1–§2.6
Contratti: `contracts/proto/eci/retrieval/v1/retrieval.proto` (sola lettura), `contracts/cypher/schema.cypher` (vincolo additivo ADR-0015)

## 1. Obiettivo

Chiudere la Fase 6 con una matrice avversariale eseguibile che dimostri
isolamento tenant/repository/ACL, autenticazione/autorizzazione fail-closed e
resistenza alla prompt injection sui tool. Colmare il gap concreto del job GDS:
discovery, proiezioni e write-back devono operare su una singola partizione
`tenant_id`/`repo`/`acl_group`, e i punteggi diventano inutilizzabili dopo un
cambio di ownership.

T6.7 è intenzionalmente cross-component: non introduce un nuovo servizio, ma
verifica insieme i confini implementati da T6.1–T6.6 e corregge il solo bypass
GDS che può attraversarli. Spezzare la suite eliminerebbe l'oracolo end-to-end
richiesto dal task-tree.

## 2. Interfaccia

```go
package gdsimpact

type ProjectionScope struct {
    TenantID string
    Repo     string
    ACLGroup string
}

type Config struct {
    EntryNodeID string
    Scope       ProjectionScope
    MaxDepth    int
    SamplingSize int
    WPPR, WProx, WBC float64
}

// CLI interna/control-plane; nessun valore proviene da prompt, LLM o RPC.
// Tutti e tre i flag di scope sono obbligatori.
// gds-impact --entry-node-id ID --tenant-id TENANT --repo REPO
//            --acl-group GROUP [flag analitici esistenti]
func ParseConfig(args []string) (Config, error)
func Run(ctx context.Context, driver neo4j.DriverWithContext, cfg Config,
    logf func(string, ...any), hooks Hooks) (Result, error)
```

Ogni `MATCH` di discovery/proiezione/write-back applica, a seed e nodi:

```cypher
n.tenant_id = $tenant_id
AND n.repo = $repo
AND n.acl_group = $acl_group
```

Il write-back salva anche la provenance del calcolo:

```cypher
n.impact_tenant_id = $tenant_id,
n.impact_repo = $repo,
n.impact_acl_group = $acl_group,
n.impact_generation = captured_partition_generation
```

Un nodo metadata `GDSPartition`, unico per le tre label grazie al contratto D3,
mantiene una generation monotona. Il fetch del reranker usa `impact_score`
soltanto se provenance e label coincidono con lo scope autenticato e
`impact_generation` coincide con la generation corrente. Un punteggio
assente/stale vale `0.0`, come già previsto da SPEC-044.

Registry tool autorevole e immutabile:

```python
ALLOWED_TOOL_NAMES: frozenset[str] = frozenset({
    "get_node", "get_callers", "get_callees", "expand_dependencies",
    "semantic_search", "read_source", "summarize_subgraph",
})
```

`RetrievalToolRuntime.execute` accetta esclusivamente azioni appartenenti a
questa allow-list; `Deps.security_context` resta l'unica sorgente dello scope e
non è mai derivato da `query`, argomenti tool, risposta LLM o stato del grafo.

## 3. Comportamento

1. **Scope GDS obbligatorio.** Given flag mancanti, blank, con control char o
   oltre i limiti, When si costruisce `Config` o si invoca `Run` direttamente,
   Then il job fallisce prima di qualunque query e non applica tenant/repo/ACL
   di default.
2. **Proiezione per partizione.** Given due tenant/repository/gruppi e archi
   cross-boundary, When il job gira per la partizione A, Then seed, BFS, due
   proiezioni GDS, algoritmi e write-back contengono solo A; B non riceve
   proprietà e nessuna cardinalità/nome di B compare nel risultato o log.
3. **Revoca/ownership e mutazioni topologiche.** Given punteggi GDS scritti per
   una partizione A, When un nodo cambia tenant/repo/ACL o una relazione viene
   materializzata/aggiornata, Then lo stesso write incrementa in O(1) la
   generation di ogni partizione coinvolta. I vecchi score restano fisicamente
   invariati ma il reranker usa `0.0`. Given una mutazione fra proiezione e
   write-back, Then il fence sotto lock rifiuta l'intero write-back; dopo un
   nuovo run usa soltanto generation e provenance correnti.
4. **Prompt/tool isolation.** Given testo che ordina di usare shell, cambiare
   `allowed_repos`/`acl_groups` o chiamare un tool inventato, When l'agente
   esegue, Then la sequenza contiene soltanto nomi allow-listed e ogni client
   retrieval riceve byte-identico il `SecurityContext` autenticato originale;
   un'azione non allow-listed fallisce prima della rete.
5. **Cross-store/query isolation.** Given dati A e B, When A usa graph lookup,
   full-text, vector search, traversal, impact analysis, hydration/reranking,
   cache, summary o citation verification, Then nessun contenuto/provenance/
   score B viene restituito; body e filtri richiesti possono solo restringere.
6. **Identity/PDP failure matrix.** Given JWT mancante, scaduto, firma/issuer/
   audience errati, metadata interno forgiato, scope vuoto, azione OPA ignota o
   PDP indisponibile, When la richiesta attraversa gateway/PEP, Then viene
   negata prima degli store e non esiste default-allow/fallback anonimo.
7. **Audit e dati derivati.** Given tentativi denied/approved e citazioni, When
   la verification termina, Then audit WORM precede il ritorno, denied e
   not-found non sono un oracle, cache/summary/citazioni/GDS restano nello
   stesso scope e nessun prompt/token/sorgente entra nell'audit.
8. **Suite riproducibile.** Given checkout CPU-only, When CI esegue i job
   security dichiarati, Then unit, OPA, datastore/testcontainers, GDS reale,
   Envoy reale e MinIO Object Lock producono esiti meccanici; nessun test usa
   GPU, LLM judge, fuzzy authorization o rete SaaS.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| scope GDS assente/blank/control char/>256 byte | errore prima di Neo4j; nessun default |
| entry GDS fuori partizione o inesistente | stesso risultato vuoto; nessun existence oracle |
| arco verso nodo fuori partizione | traversal arrestata; nodo/arco non proiettato |
| proiezione/algoritmo/write-back fallisce | errore esplicito; drop delle proiezioni sempre tentato |
| score con provenance mancante o non coincidente | ignorato deterministicamente (`0.0`) |
| generation assente/non coincidente | ignorato deterministicamente (`0.0`) |
| nodo cambia ownership | incrementa nello stesso write le generation vecchia e nuova |
| relazione creata/aggiornata | incrementa nello stesso write le generation endpoint |
| partizione muta durante il calcolo | write-back interamente rifiutato come stale |
| azione tool sconosciuta | `ValueError` prima di qualunque client/store |
| query tenta di serializzare scope | testo trattato solo come query; context invariato |
| JWT/metadata/OPA invalidi o backend auth non disponibile | deny bounded; zero store call |
| dato senza label security | invisibile su ogni path |
| Docker non disponibile localmente | unit test restano eseguibili; integrazioni obbligatorie in CI, mai dichiarate locali |

## 5. Threat model e non-goals

**Attori:** tenant autenticato ostile, client anonimo, prompt/output LLM
compromesso, producer interno mal configurato, operatore batch che omette scope,
PDP/store indisponibile. **Asset:** codice, topologia, embedding, indici,
summary/cache, provenance, score/community GDS e audit. **Confini:** JWT validato
crea `SecurityContext`; il control-plane interno crea `ProjectionScope`; prompt,
body, metadata client e valori expected/eval non sono autorità. **Attacchi:**
cross-tenant/repo, confused deputy, forged metadata, prompt-to-tool escalation,
GDS globale, score stale dopo revoca, not-found oracle, PDP fail-open.
**Mitigazioni:** allow-list chiusa, scope immutabile, predicati positivi su ogni
nodo GDS, provenance del calcolo, generation fencing O(1), lock condiviso fra
sink e write-back, re-check prima del consumo, deny uniforme, test reali e
osservabilità senza label identitarie.

Non-goals: nessuna nuova policy prodotto, modifica ADD/proto, LLM judge,
embedding/fuzzy matching, GPU, nuovo datastore, mTLS/Kubernetes (T7.1),
dashboard/alert completi (T7.3) o modifica della baseline T5.6. La suite non
presenta Neo4j Community come prova delle grant Enterprise; prova il filtro
applicativo e GDS Community con partizionamento esplicito.

## 6. Vincoli dall'ADD

- Modulo 3 §2.1: identità autenticata, PEP/PDP e fail-closed.
- Modulo 3 §2.2: ogni nodo Cypher filtrato; proiezioni GDS mai globali perché
  RBAC fine-grained non è garantito sulle native projections.
- Modulo 3 §2.3–§2.5: Qdrant `must`, OpenSearch DLS, filtro identico sulle tre
  gambe e re-check prima del packing.
- Modulo 3 §2.5(3–5): cache/summary ACL, citation check e audit WORM.
- Modulo 3 §2.5(6): prompt injection non altera scope; sette tool allow-listed.
- CLAUDE.md: SecurityContext solo da metadata autenticato, traversal bounded,
  deadline invariata e verification deterministica.

## 7. Test plan

- Unit Go GDS: validazione `ProjectionScope` e presenza parametri/predicati in
  discovery/projection/write-back; `Run` rifiuta config costruita a mano.
- Integration Neo4j+GDS reale: fixture A/B, edge cross-boundary, write-back e
  catalog cleanup; cambio ACL e mutazione di relazione avanzano solo le
  generation coinvolte senza scansione, una mutazione interveniente fa fallire
  il write-back e il reranker ignora provenance/generation stale.
- Unit Python orchestrator: registry esatto, tool ignoto pre-network, prompt
  injection non modifica azioni ammesse né l'oggetto `SecurityContext`.
- Regression matrix versionata: riferimenti test esistenti per gateway JWT,
  OPA/PEP, Neo4j/Qdrant/OpenSearch, traversal/impact, cache, summary,
  verification/citation e WORM; nessun checkbox senza test nominato.
- CI: aggiunge GDS reale al job datastore-security e mantiene obbligatori
  `build-lint-test`, `guard`, `worm-audit-integration` ed Envoy.
- Byte identity di `tests/golden/queries_v0.json` e baseline T5.6 verificata da
  checksum già versionati; nessuna modifica a tali file.

## 8. Osservabilità

- Log GDS solo con conteggi, fase e outcome; vietati tenant/repo/gruppo/node id.
- Counter `eci_security_regression_total{surface,outcome}` se introdotto, con
  `surface` allow-list chiusa e mai identità; una suite CI può usare soli esiti
  test senza esportare una metrica runtime fittizia.
- Le metriche esistenti restano gli oracoli runtime:
  `eci_retrieval_security_filter_total`,
  `eci_retrieval_acl_recheck_removed_total`,
  `eci_verification_citation_access_total`,
  `eci_verification_audit_append_total`, `eci_gateway_edge_requests_total`.
- Nessun tenant, user, repo, gruppo, query, token, path o node id in label/span.

## 9. Criteri di accettazione

- [x] Scenari §3 scritti red-before-green e verdi.
- [x] Nessuna query store-backed/proiezione GDS senza filtro tenant/repo/ACL
      su seed e ogni endpoint; gli algoritmi operano solo sui graph name già
      scoped.
- [x] Fixture reale prova che edge/nodi B non entrano nelle proiezioni di A.
- [x] Write-back porta provenance e score stale non influenza il reranker.
- [x] Mutazioni di ownership/topologia avanzano generation in O(1); il
      write-back è fenced/atomico e nessuna query sink scansiona la partizione.
- [x] Prompt injection/tool sconosciuto non altera scope e non raggiunge rete.
- [x] Matrice nomina test reali per ogni superficie T6.7, senza skip/xfail.
- [x] JWT/forged metadata/PDP outage e audit WORM restano fail-closed in CI.
- [x] Baseline T5.6 e golden dataset byte-identici; nessuna GPU.
- [ ] `task build`, `task lint`, `task test`, `task test:integration`,
      `task guard` e job security CI verdi.
- [ ] SPEC `verified` dopo review risolta, evidenza CI verde e PR merge-ready.

## 10. Review avversariale di approvazione

Pass eseguito il 2026-08-29. La proposta non cambia ADD/contratti, non usa
input LLM per autorizzare e non aggiunge write dirette a viste materializzate
fuori dal job GDS già approvato da SPEC-043. Il traversal resta BFS bounded e
parametrizzato; nessuna deadline o idempotenza viene indebolita. Scope vuoto,
PDP/store failure e provenance GDS stale falliscono chiusi. La separazione fra
garanzie deterministiche e valutazione probabilistica resta intatta.

L'attacco più pericoloso individuato nel secondo passaggio è la revoca ACL dopo
il write-back: filtrare soltanto il nodo non basta perché un vecchio score può
codificare topologia non più autorizzata. Per questo la SPEC richiede provenance
tenant/repo/ACL del calcolo e verifica al consumo. La prima implementazione di
invalidazione eager è stata poi scartata: è quadratica e non chiude la race con
un job già proiettato. ADR-0015 documenta il registro generation per partizione,
il fencing sotto lock e il vincolo D3 additivo. Sono scartate anche
invalidazione globale, proiezione globale con post-filter, timestamp/TTL, score
senza provenance e scope da prompt: lasciano dati stale o contraddicono l'ADD.

## 11. Evidenza di implementazione

Implementazione CPU-only completata il 2026-08-29. I test red sono nel commit
`78f50ce`; prima della produzione fallivano per assenza di `ProjectionScope` e
`ALLOWED_TOOL_NAMES`. La pipeline finale applica lo scope a discovery, seed,
entrambe le proiezioni e write-back, sostituisce la vecchia stima globale con
le `.stream.estimate` sui graph name autorizzati e verifica la provenance al
consumo del reranker.

La prima review automatizzata ha riprodotto un caso più forte: il punteggio di
un nodo invariato può incorporare la topologia di un altro nodo che cambia
scope. Il test integration aggiunto red nel commit `a655fe3` ha portato a una
prima invalidazione eager. La fresh review dell'head finale ha correttamente
rifiutato quella soluzione: una proiezione già creata poteva riscrivere score
stale dopo l'invalidazione e ogni evento visitava l'intera partizione.

Le regressioni del commit `8f596e2` dimostrano separatamente i tre failure:
generation non avanzata dal sink, score con generation stale ancora consumato
e write-back accettato dopo una mutazione interveniente. ADR-0015 introduce il
fence generation. Il protocollo finale, descritto sotto, usa un ordine globale
dei lock, incrementa O(1) il contatore, rende il write-back all-or-nothing e fa
richiedere al reranker la generation corrente. Le prove mirate sono verdi su
Neo4j reale; nessun rerun GPU e nessuna modifica a baseline/golden.

Una review successiva ha reso più forte il protocollo: leggere lo scope prima
del lock permetteva a due ownership update sovrapposti o a un endpoint spostato
di invalidare la partizione sbagliata; inoltre partizioni acquisite senza
ordine totale potevano produrre deadlock. Le regressioni del commit `1a5a3b4`
richiedono ora l'ordine globale `CodeNode.id` poi scope partizione, rilettura
post-lock nella stessa explicit transaction e lo stesso ordine nel write-back
GDS. I test concorrenti su Neo4j reale coprono sia node move sia endpoint move.

Evidenza locale verde:

- `task build`, `task lint`, `task test`, `task guard`;
- `task test:integration`, inclusi GDS reale, stale-score e MinIO COMPLIANCE;
- `go test -race ./...` nel modulo `tools/gds-impact`;
- `go test -race ./internal/rerank` nel retrieval engine;
- 24 test `orchestrator/test_graph.py`.

Docker Desktop, inizialmente inattivo, è stato avviato per le integrazioni. Due
tentativi preliminari del gate completo hanno incontrato failure di bootstrap
Testcontainers (`PortNotExposed` PostgreSQL e timeout del mapped port Kafka);
entrambi i test sono passati subito isolati senza modifiche. Una terza
esecuzione integrale di `task test:integration` è verde; la verifica CI e la
review successive sono registrate qui sotto.

La run intermedia [33263671444](https://github.com/nepryoon/eci/actions/runs/33263671444)
era verde sull'head eager `9cfbbb7`, ma non è evidenza finale per ADR-0015. I
due nuovi thread P1 restano aperti fino ai gate e alla fresh review del nuovo
head. Baseline T5.6, metriche storiche e `queries_v0.json` restano byte-identici.
