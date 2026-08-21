# SPEC-042 — ImpactAnalysis streaming (T4.2)
Stato: implemented
Task-tree: T4.2 (dip. T4.1, già chiuso) · Servizio: services/retrieval-engine (Go, estende T4.1/SPEC-041) · ADD: Modulo 2, Deliverable D4 (loop agentico, contesto — non un algoritmo eseguibile come D5)

## 1. Obiettivo
Nuova RPC gRPC **streaming** (prima di questo tipo in tutto il progetto — `GetNode`/`ExpandNeighbors`/`HybridSearch`, T1.4/T4.1, sono tutte unarie) che esegue la stessa reverse reachability bounded già costruita in `GraphTraversal` (T4.1), ma: (a) invia i nodi scoperti man mano che la traversata procede, non in un unico batch finale; (b) applica un cap esplicito sul numero totale di nodi esplorati, troncando (non fallendo) quando raggiunto; (c) classifica ciascun nodo con un `impact_kind` derivato dal tipo di arco più vicino al punto di partenza; (d) interpone messaggi di progresso periodici nello stream. Coerente con D4 (§0): questa RPC è pensata per essere chiamata ripetutamente da un agente in un loop (Fase 5, fuori scope qui), con "pruning fan-out, dedup nodi visitati, check criteri stop" esplicitamente previsti dal diagramma di sequenza.

## 2. Interfaccia

**Proto, nuovo messaggio + nuova RPC** (estensione additiva a `contracts/proto/eci/retrieval/v1/retrieval.proto`, stesso principio di ADR-0007 — richiede una propria ADR per lo stesso motivo, modifica a un file già tracciato sotto `contracts/`):
```protobuf
service RetrievalEngine {
  // ... rpc esistenti invariate ...
  rpc ImpactAnalysis(ImpactAnalysisRequest) returns (stream ImpactAnalysisResponse);
}

message ImpactAnalysisRequest {
  string entry_node_id = 1;
  int32 max_depth = 2;
  int32 max_nodes = 3;       // cap sul fan-out totale, default server 500
  Domain domain = 4;
  repeated string repos = 5;
}

message ImpactAnalysisResponse {
  oneof event {
    ImpactNode node = 1;
    ImpactProgress progress = 2;
  }
}

message ImpactNode {
  string node_id = 1;
  string domain = 2;
  int32 hop_distance = 3;
  string impact_kind = 4;    // tipo dell'arco più vicino al nodo di partenza nel path (CALLS/IMPLEMENTS/...)
  Provenance provenance = 5; // riusa il messaggio Provenance già esistente
}

message ImpactProgress {
  int32 nodes_explored = 1;
  int32 current_depth = 2;
  bool truncated = 3;        // true se max_nodes è stato raggiunto, traversata interrotta
}
```

**Package** `services/retrieval-engine/internal/impactanalysis` (nuovo, sorella di `hybridsearch`):
```go
func StreamImpact(ctx, driver neo4j.DriverWithContext, entryNodeID string, maxDepth, maxNodes int, domain, repo *string, emit func(ImpactEvent) error) error
```
`emit` è iniettata dal chiamante (il gRPC handler userà `stream.Send`), non un canale Go — mantiene la funzione testabile senza un vero stream gRPC (una closure che accumula in uno slice, negli unit test).

**Cypher**: stessa struttura di `GraphTraversal` (T4.1) — reverse reachability bounded, STESSO insieme di tipi di arco (`CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS`) — ma eseguita in modo che i risultati possano essere consumati progressivamente livello per livello (BFS esplicito per profondità crescente, non un'unica query che ritorna tutto insieme: necessario sia per lo streaming genuino sia per applicare il cap PRIMA di esplorare oltre, non dopo aver già pagato il costo di una traversata più profonda del necessario).

**`impact_kind`**: il tipo dell'arco Cypher (`type(r)` sull'ultimo hop del path più breve verso quel nodo) — un nodo raggiunto per la prima volta via `CALLS` ha `impact_kind="CALLS"`, e così via per gli altri tipi.

**Cap sul fan-out**: `max_nodes` (default server 500, dichiarato — nessun valore analogo esiste altrove nel progetto, scelta ragionevole non desunta da nient'altro) conta i nodi TOTALI esplorati attraverso l'intera traversata (non per singolo livello). Raggiunto il cap, la traversata si ferma, un `ImpactProgress` finale con `truncated=true` viene inviato, la RPC si chiude senza errore (troncamento è un esito normale e atteso, non un fallimento).

## 3. Comportamento (scenari)

1. **Dato** un `entry_node_id` con una rete di dipendenze nota, **quando** chiamo `ImpactAnalysis`, **allora** ricevo un `ImpactNode` per stream-event per ciascun nodo raggiungibile entro `max_depth`, PRIMA che la RPC si chiuda (streaming genuino, non un batch finale) — verificabile misurando che il client riceva eventi PRIMA che il server abbia terminato l'intera traversata.
2. **Dato** lo stesso scenario, **quando** ispeziono `impact_kind` di un nodo raggiunto via un arco `CALLS`, **allora** è `"CALLS"`; per un nodo raggiunto via `IMPLEMENTS`, `"IMPLEMENTS"`.
3. **Dato** un grafo con più nodi raggiungibili di `max_nodes`, **quando** chiamo `ImpactAnalysis`, **allora** la traversata si ferma al cap, l'ultimo `ImpactProgress` ricevuto ha `truncated=true`, la RPC si chiude senza errore.
4. **Dato** un grafo con MENO nodi raggiungibili di `max_nodes`, **quando** chiamo `ImpactAnalysis`, **allora** `truncated` resta `false` in ogni `ImpactProgress` ricevuto.
5. **Dato** `entry_node_id` inesistente, **quando** chiamo `ImpactAnalysis`, **allora** ricevo zero `ImpactNode`, un `ImpactProgress` con `nodes_explored=0`, `truncated=false` — nessun errore (stesso principio già stabilito in T4.1 per un `entry_node_id` sconosciuto: `MATCH` su un nodo inesistente produce zero risultati, non un errore).
6. **Dato** `max_nodes` <= 0, **quando** chiamo `ImpactAnalysis`, **allora** errore esplicito prima di avviare qualunque traversata.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Neo4j irraggiungibile a metà traversata (dopo aver già inviato alcuni `ImpactNode`) | Lo stream si chiude con un errore gRPC esplicito — i nodi già inviati restano validi presso il client (comportamento naturale dello streaming gRPC: un errore a metà stream non invalida quanto già ricevuto) |
| Un nodo raggiungibile tramite PIÙ percorsi di lunghezza diversa | `impact_kind`/`hop_distance` riflettono il percorso PIÙ BREVE (stesso principio `min(length(path))` già stabilito in `GraphTraversal`, T4.1) |

## 5. Non-goals
Nessun consumo reale da un agente (Fase 5, fuori scope). Nessuna integrazione con `HybridGraphVectorSearch`/RRF (T4.1, rimane invariato, questa è una RPC separata). Nessun GDS/PageRank/betweenness (T4.3, task successivo).

## 6. Vincoli dall'ADD
Modulo 2, D4 — il pruning/cap fan-out/dedup esplicitamente previsti nel diagramma di sequenza per il loop agentico.

## 7. Test plan
Unitari per `impact_kind`/cap-fan-out con un `emit` finto (nessuna infrastruttura reale necessaria per la sola logica). Integrazione con Neo4j reale (testcontainers) per lo streaming genuino (scenario 1, verificabile ricevendo eventi da un client gRPC reale prima che il server termini) e gli scenari 3/5.

## 8. Osservabilità
Uno span per l'intera chiamata, un evento OTel per ogni livello di profondità completato (numero di nodi scoperti a quel livello).

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con evidenza diretta, in particolare lo scenario 1 (streaming genuino, non un batch mascherato). Prova RIGOROSA (non solo osservazionale) in `internal/impactanalysis/stream_test.go`,`TestStreamImpact_EmitErrorStopsTraversalEarly_ProvesGenuineLevelByLevelStreaming`: se `emit` fallisce al PRIMO evento, `fetch` non viene MAI invocata per i livelli successivi — vero solo se ogni livello è recuperato on-demand, non pre-calcolato in blocco. Verifica end-to-end via gRPC reale in `internal/server/impact_integration_test.go` (ordine degli eventi sul wire, `impact_kind` per tipo di arco — scenari 1/2). Scenari 3/4/5 verificati sia con `fetch` finto (unitari puri) sia con Neo4j reale via testcontainers (`internal/impactanalysis/stream_integration_test.go`). Scenario 6 verificato sia a livello di pacchetto sia a livello RPC (`codes.InvalidArgument`).
- [x] Edge case tabella §4 verificati esplicitamente: riga 1 (fallimento a metà traversata, nodi già emessi restano validi) con `fetch` finto che fallisce al secondo livello; riga 2 (percorso più breve vince) sia con `fetch` finto sia con un fixture Neo4j reale a doppio percorso (`EdgeCase_ShortestPathWinsWithRealMultiPathGraph`).
- [x] Estensione proto verificata additiva, propria ADR (`docs/decisions/ADR-0008-impactanalysis-riuso-scaffold-esistente.md`, stesso principio di ADR-0007), bindings rigenerati per ENTRAMBI i linguaggi (`bash scripts/task-proto-gen.sh` completo, verificato esplicitamente col diff che tocca sia `libs/go/eci/retrieval/v1/retrieval.pb.go` sia `libs/py/eci_core/retrieval/v1/retrieval_pb2.py` — lezione diretta da SPEC-041, dove solo Go fu rigenerato).
- [x] Nessuna regressione sui test esistenti di T1.4/T4.1: `TestRetrievalEngineServer` (T1.4, 7 scenari) e `TestHybridSearchGraphVectorDispatch`/`TestHybridSearchIntegration`/`TestHybridSearchParity` (T4.1) rieseguiti invariati, verdi insieme ai nuovi test T4.2 nello stesso pacchetto.

## 10. Deviazioni rispetto alla SPEC

1. **Messaggi proto: riuso dello scaffold D7 esistente, non le forme nuove
   di §2** — vedi `docs/decisions/ADR-0008-impactanalysis-riuso-scaffold-esistente.md`
   per il ragionamento completo (conflitto scoperto e risolto PRIMA di
   scrivere qualunque proto, con conferma esplicita dell'utente). In
   sintesi: `ImpactAnalysisRequest`/`ImpactedNode`/`ImpactProgress`/
   `ImpactAnalysisEvent` erano già nel contratto (mai implementati);
   `ImpactAnalysisRequest`/`ImpactProgress` di SPEC-042 collidevano per
   nome con campi incompatibili, e l'RPC di SPEC-042 aveva un tipo di
   ritorno diverso da quello già dichiarato. Adattati additivamente
   (`max_nodes`/`domain`/`repos` su `ImpactAnalysisRequest`) invece di
   introdurre nomi in conflitto.

2. **`impact_kind` (SPEC-042: stringa tipo-arco) portato da
   `path_edge_types` (repeated EdgeType), non da un nuovo campo**: l'enum
   `ImpactKind` già esistente su `ImpactedNode` è una classificazione di
   SEVERITÀ (SYNTACTIC/BEHAVIORAL/MODULE_BOUNDARY, ADD Modulo 2 §1.5),
   concettualmente diversa — resta sempre `IMPACT_KIND_UNSPECIFIED`
   (richiede GDS, T4.3, esplicitamente fuori scope). `path_edge_types`
   riceve un singolo elemento (il tipo d'arco dell'ultimo hop del percorso
   più breve) — uso legittimo del campo, il cui commento non impone una
   lunghezza minima.

3. **`max_nodes` senza default silenzioso lato server, contrariamente al
   "default server 500" di §2**: §3 scenario 6 testa esplicitamente
   `max_nodes <= 0 -> errore`; con un default silenzioso su 0, lo scenario
   non sarebbe osservabile via RPC (proto3 non distingue "non impostato" da
   "impostato a 0" su uno scalare senza `optional`). Risolto trattando "il
   client deve sempre passare un max_nodes positivo" come il comportamento
   testabile e testato; "default server 500" resta guida per gli autori
   client (es. l'Orchestrator), non applicata nel codice.
   `max_depth` invece USA il default preesistente dichiarato dal commento
   proto originale (`// default server: 4`, D7) quando il client lo lascia
   a 0 — nessuna analoga ambiguità: nessuno scenario testa
   `max_depth <= 0` come errore via RPC (solo `runBFS` lo valida
   difensivamente, stesso principio di `GraphTraversal`/T4.1, mai
   raggiungibile dalla RPC dato il default).

4. **Campi preesistenti di `ImpactAnalysisRequest` accettati ma NON
   implementati da T4.2**: `security_context` (nessun enforcement, stesso
   principio "plumbing non enforcement" di T1.4/T4.1 fino a Fase 6),
   `edge_types` (la traversata usa sempre il set fisso CALLS/IMPLEMENTS/
   EXTENDS/OVERRIDES/DEPENDS_ON/IMPORTS, indipendentemente da cosa il
   client imposta), `direction` (sempre REVERSE, coerente con la semantica
   di impact analysis — FORWARD non implementato), `fanout_cap_per_hop`
   (concetto diverso da `max_nodes`: cap PER LIVELLO invece che totale, non
   implementato in questa SPEC), `min_impact_score` (richiede
   `impact_score` da GDS, T4.3), `include_source_text` (nessuna sorgente
   disponibile in questo scope, `RetrievedNode.source_text` resta
   zero-value). Nessuno di questi è testato da uno scenario di SPEC-042.

5. **`ImpactProgress.truncated_by_depth` sempre `false`**: SPEC-042 ha un
   solo concetto di troncamento (il cap `max_nodes`, mappato su
   `truncated_by_fanout_cap`). Raggiungere `max_depth` naturalmente,
   completando la traversata senza aver esaurito il cap, non è trattato
   come un troncamento in questo modello — nessuno scenario lo richiede.

6. **`ImpactedNode.node` (RetrievedNode) popolato SOLO con `node_id`/
   `domain`/`provenance`/`scores.hop_distance`**: la query Cypher per
   livello (`internal/impactanalysis/fetch.go`) non recupera `name`/
   `node_type`/`ast_hash` — stesso principio già stabilito per
   `GraphTraversal` (T4.1) e `retrievedNodeFromDBNode` (T1.4): nessun campo
   fabbricato indipendentemente dalla fonte dati reale.

7. **Isolamento dei file di test per servizio streaming**: `internal/server/impact_integration_test.go`
   costruisce il proprio harness Neo4j (`startImpactNeo4j`/`seedImpactGraph`/
   `startImpactServer`), separato da `server_integration_test.go` (T1.4) e
   da `hybridsearch_dispatch_integration_test.go` (T4.1) — stesso principio
   di isolamento già stabilito in T4.1: zero rischio di alterare scenari
   esistenti toccando file condivisi.

8. **Flakiness osservata eseguendo `go test -tags=integration ./...` su
   TUTTI i pacchetti insieme (non ripetibile)**: un'unica esecuzione ha
   mostrato `TestRetrievalEngineServer` fallire quando eseguito in
   parallelo (pacchetti Go diversi, quindi binari di test diversi) insieme
   ai nuovi test T4.2 — non riproducibile in esecuzioni successive isolate
   o con `-p 1` (pacchetti sequenziali); consistente con contesa di risorse
   Docker (più container Neo4j/Qdrant avviati simultaneamente da processi
   di test paralleli), non con una regressione di codice. Verificato con
   run ripetuti, package-per-package e con `-p 1`, sempre verdi.
