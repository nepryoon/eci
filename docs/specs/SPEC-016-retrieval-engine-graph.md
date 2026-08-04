# SPEC-016 — retrieval-engine v0: server gRPC sulla sola gamba grafo (T1.4)
Stato: implemented
Task-tree: T1.4 (quarto task di Fase 1) · Servizio: services/retrieval-engine (Go, finora solo scaffold vuoto) · ADD: Modulo 2 (Retrieval), Modulo 3 (API Contracts)
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto (D7, letto — non modificato)

## 1. Obiettivo
Popolare `services/retrieval-engine` con un server gRPC reale che implementa `GetNode`, `ExpandNeighbors`, `HybridSearch` (limitata alla sola gamba grafo — nessun Qdrant/vettoriale ancora) e `Health`, interrogando il Neo4j popolato da sink-graph (T1.3). Prima esposizione di un'API per i dati fin qui costruiti solo internamente alla pipeline CDC.

## 2. Interfaccia

Server gRPC (`google.golang.org/grpc`), stessa coppia di interceptor già costruita e verificata in interoperabilità reale in SPEC-011: `grpc.UnaryInterceptor(secctx.UnaryServerInterceptor())` + `grpc.StatsHandler(otelgrpc.NewServerHandler())`. Client Neo4j: `neo4j-go-driver` (già usato da sink-graph, SPEC-015 — stessa libreria, non introdurne una seconda).

**Scope aggiunto rispetto al testo letterale del task-tree**: `Health` incluso (basso costo, buona prassi per qualunque server gRPC nuovo). `ImpactAnalysis` esplicitamente **fuori scope** — complessità maggiore (GDS, streaming, fanout cap), coerente con una fase successiva.

**Mappatura enum↔stringa**, usata da tutte e tre le RPC: `Domain` (`DOMAIN_CODE` ↔ `"code"`, ecc. — CHECK constraint di SPEC-005) e `EdgeType` (`EDGE_TYPE_CALLS` ↔ `"CALLS"`, ecc. — stesso enum già validato in `sink-graph`, SPEC-015).

**`GetNode`**: `MATCH (n:CodeNode {id: $node_id}) RETURN n, labels(n)`. Popola `RetrievedNode`: `node_id`, `domain` (mappato), `node_type` (dalla label specifica — quella diversa da `CodeNode`), `name`, `ast_hash` da proprietà reali; `provenance.repo`/`provenance.path` da `n.repo`/`n.path`. **Tutti gli altri campi restano al valore di default proto** (`signature`, `summary`, `source_text`, `provenance.start_line/end_line/commit_sha`, l'intero `scores`) — la pipeline attuale non li popola, non vanno fabbricati indipendentemente da `include_source_text`/`include_summary` nella request. Nodo non trovato → risposta gRPC `NotFound`, non un `GetNodeResponse` vuoto silenzioso.

**`ExpandNeighbors`**: `edge_types` vuoto → nessun filtro di tipo (tutti gli archi); `direction = TRAVERSAL_DIRECTION_UNSPECIFIED` → entrambe le direzioni (non documentato esplicitamente dal proto, scelta dichiarata qui); `depth` default 1 (dal commento del proto); `limit` senza default esplicito nel proto → 100 qui (scelta dichiarata, walking skeleton). Traversata Cypher con salto variabile bounded dal parametro `depth` (`*N..M` con `M=depth`, mai illimitato). Stessa proiezione parziale di `RetrievedNode` di `GetNode` per ciascun vicino.

**`HybridSearch`**, sola gamba grafo: `query_text` contro l'indice full-text nativo Neo4j `code_fulltext` (già creato in SPEC-004, copre `Method|Function|Class|File` su `name`/`signature`/`source_text` — qui matcherà di fatto solo su `name`, dato che gli altri due campi non sono ancora popolati: **limite dichiarato, non un bug**) via `CALL db.index.fulltext.queryNodes('code_fulltext', $query_text) YIELD node, score`. `graph_limit` (default 200) bound la query full-text grezza; `top_k` (default 25) bound il risultato finale restituito. `vector_limit`/`intent` non hanno effetto (nessuna gamba vettoriale). `domain`: filtro (in pratica sempre `DOMAIN_CODE` a questo stadio, nient'altro è mai stato ingerito). `repos`: filtro su `n.repo` (effetto limitato oggi, dato che ogni nodo ha lo stesso placeholder `"local"` — comunque implementato correttamente per quando `repo` diventerà reale). Risposta: `graph_candidates` = conteggio grezzo dei match full-text; `vector_candidates` = **sempre 0**; `vector_leg_degraded` = **sempre `true`** — uso diretto e corretto del campo diagnostico esistente per segnalare "gamba vettoriale non disponibile", non un workaround.

**`SecurityContext.allowed_repos` NON viene applicato come filtro di enforcement reale** in nessuna delle tre RPC — stesso principio "plumbing, non enforcement" già stabilito per T0.8: `SecurityContext` viene estratto correttamente (via l'interceptor) e reso disponibile, ma non ancora usato per restringere l'accesso. Enforcement reale: Fase 6.

**`Health`**: `graph_leg_healthy` = `RETURN 1` su Neo4j riesce; `fulltext_leg_healthy` = stessa connessione Neo4j (l'indice vive nello stesso DB — nessun controllo separato sullo stato dell'indice in questa SPEC); `vector_leg_healthy` = **sempre `false`** (nessuna gamba vettoriale esiste). `status` = `SERVING` se `graph_leg_healthy`, altrimenti `NOT_SERVING` (mai `DEGRADED` in questa SPEC: `DEGRADED` implicherebbe che una gamba opzionale è mancante mentre il servizio resta operativo — qui la gamba vettoriale mancante è lo stato NORMALE e permanente di questa fase, non un degrado transitorio).

## 3. Comportamento (scenari)

1. **Dato** il nodo `Method` `Process` già in Neo4j (da SPEC-015 scenario 4), **quando** chiamo `GetNode(node_id=<id di Process>)`, **allora** ricevo un `RetrievedNode` con `node_type="Method"`, `name="Process"`, `ast_hash` corretto, `provenance.repo="local"`, `provenance.path` corretto — e `scores`/`signature`/`summary`/`source_text` ai rispettivi zero-value proto, non valori inventati.
2. **Dato** un `node_id` inesistente, **quando** chiamo `GetNode`, **allora** ricevo un errore gRPC `NotFound`, non una risposta vuota con status OK.
3. **Dato** lo stesso stato Neo4j, **quando** chiamo `ExpandNeighbors(node_id=<id di Process>, direction=UNSPECIFIED)`, **allora** ricevo `Validate` (via `CALLS` uscente) — verifica sia della traversata sia della mappatura `EdgeType` corretta in `traversed_edge_types`.
4. **Dato** lo stesso stato, **quando** chiamo `HybridSearch(query_text="Validate")`, **allora** il nodo `Method` `Validate` compare tra i risultati (match sul nome via `code_fulltext`), `vector_candidates=0`, `vector_leg_degraded=true`.
5. **Dato** lo stesso stato, **quando** chiamo `HybridSearch` con un `query_text` che matcherebbe solo il corpo di una funzione (non il nome) — es. testo letterale presente solo dentro `computeTotal`, mai nel suo nome — **allora** NON viene trovato nulla — verifica diretta ed esplicita del limite dichiarato in §2 (full-text match solo su `name` a questo stadio), non solo teorico.
6. **Dato** un `SecurityContext` con `allowed_repos` che NON include `"local"`, **quando** chiamo una qualunque delle tre RPC, **allora** la richiesta procede comunque senza restrizioni — verifica diretta che l'assenza di enforcement (§2, §5) sia il comportamento reale, non solo dichiarato.
7. **Dato** Neo4j irraggiungibile, **quando** chiamo `Health`, **allora** ricevo `status=NOT_SERVING`, `graph_leg_healthy=false`, `vector_leg_healthy=false`, `fulltext_leg_healthy=false` — non un errore gRPC generico, una risposta strutturata che descrive lo stato.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `ExpandNeighbors` con `depth` molto alto su un grafo denso | Il `limit` (100 di default) resta il bound superiore sul numero di vicini restituiti, indipendentemente da quanti la traversata a profondità `depth` incontrerebbe in astratto — nessun caso di risposte illimitate |
| `edge_types`/`domain` con un valore enum `UNSPECIFIED` esplicito nella request | Trattato come "nessun filtro" per `edge_types` (§2); per `domain` in `HybridSearch`, `DOMAIN_UNSPECIFIED` = nessun filtro di dominio (cerca su tutti, in pratica solo `code` esiste oggi) |
| Query Cypher che fallisce per un errore di connessione Neo4j a metà di una `ExpandNeighbors`/`HybridSearch` | Propagare un errore gRPC esplicito (`Internal` o `Unavailable` secondo il caso), non una risposta parziale silenziosa |

## 5. Non-goals
Nessuna gamba vettoriale/Qdrant (Fase 4). Nessun `ImpactAnalysis` (complessità GDS/streaming, fase successiva). Nessun enforcement reale di `SecurityContext.allowed_repos`/`acl_groups` (Fase 6, stesso principio di T0.8). Nessun RRF/fusion reale in `HybridSearch` (non c'è nulla da fondere con una sola gamba). Nessun rerank cross-encoder (`scores.rerank_score` resta sempre a zero-value).

## 6. Vincoli dall'ADD
Modulo 2: le tre RPC scelte (`GetNode`, `ExpandNeighbors`, `HybridSearch`) sono esattamente i "tool tipizzati dell'agente" e la ricerca ibrida descritti nel Modulo 2 — qui limitati alla gamba grafo come esplicitamente previsto dal task-tree di Fase 1 ("sola gamba grafo").

## 7. Test plan
Test di integrazione con testcontainers Neo4j (stesso pattern SPEC-015), dati di setup inseriti direttamente via Cypher nel test (non serve l'intera pipeline T1.1-T1.3 per testare SOLO il server di lettura) — tranne lo scenario di verifica end-to-end, se incluso, che può riusare lo stato già lasciato da SPEC-015 sullo stack reale.

## 8. Osservabilità
Stessa coppia di interceptor OTel/SecurityContext di SPEC-011, qui lato server per la prima volta con RPC applicative reali (SPEC-011 li aveva verificati solo con un `GetNode` di prova nello scaffold di interoperabilità).

## 9. Criteri di accettazione
- [x] Scenari 1/2: `GetNode` corretto su nodo esistente (con i campi non ancora popolabili correttamente a default) e su nodo assente (`NotFound`).
- [x] Scenario 3: `ExpandNeighbors` traversa correttamente e riporta `traversed_edge_types` accurato.
- [x] Scenari 4/5: `HybridSearch` trova per nome, non trova per contenuto interno — entrambi verificati esplicitamente (`Scenario4_HybridSearchFindsByName`/`Scenario5_HybridSearchMissesInnerContent`, sotto-test distinti), non solo il caso positivo.
- [x] Scenario 6: assenza di enforcement verificata direttamente (richiesta con `SecurityContext.allowed_repos` che esclude `"local"`, sia nel body sia nei metadata gRPC via l'interceptor reale — la richiesta procede comunque), non solo dichiarata.
- [x] Scenario 7: `Health` riporta uno stato strutturato accurato anche a Neo4j irraggiungibile (driver dedicato verso un indirizzo che rifiuta la connessione, nessun errore gRPC).
- [x] Test di integrazione verdi con `-count=1` (rieseguiti dopo un riavvio del daemon Docker a metà sessione — vedi §10 — per un run genuinamente fresco, non dalla cache di `go test`).

## 10. Deviazioni rispetto alla SPEC

1. **`GetNode`: `RETURN n` invece di `RETURN n, labels(n)`**: `dbtype.Node` di
   `neo4j-go-driver` porta già `Labels []string` come campo della struct
   ritornata da `RETURN n` — chiedere `labels(n)` separatamente sarebbe
   stato un secondo valore ridondante nello stesso record. Stesso dato,
   una query più semplice invece di due proiezioni equivalenti.

2. **`ExpandNeighbors`: bound di profondità (`*1..N`) interpolato come
   intero letterale, non legato come parametro Cypher**: verificato che
   Neo4j non supporta un parametro per il bound di un pattern a lunghezza
   variabile (limite del linguaggio Cypher stesso, non implementativo) —
   `depth` è comunque un `uint32` dal messaggio proto, non testo libero,
   quindi l'interpolazione non è un rischio di injection (stesso principio
   di sicurezza usato per label/tipi di relazione, che SONO validati
   contro whitelist prima dell'interpolazione perché arrivano come
   stringhe). `LIMIT $limit`/`LIMIT $graph_limit`, al contrario, sono
   legati come parametri normali — verificato che Neo4j li supporta
   (a differenza del bound del salto), non assunto.

3. **`ExpandNeighbors` con `depth > 1`: deduplica dei vicini lato Go, dopo
   il `LIMIT` Cypher**: con più path distinti verso lo stesso nodo (solo
   possibile a `depth > 1`; a `depth = 1`, il default e l'unico caso
   testato dallo scenario 3, ogni coppia di nodi ha al più una relazione
   per tipo per costruzione di `sink-graph`, SPEC-015, quindi nessuna
   duplicazione può verificarsi), il `LIMIT` Cypher conta le RIGHE grezze
   prima della deduplica per id: un grafo molto denso a `depth` alto
   potrebbe restituire meno vicini DISTINTI di quanti `limit` ne
   permetterebbe in astratto. Limite noto, non corretto in questa SPEC
   (walking skeleton, nessuno scenario lo richiede) — da rivedere se una
   fase successiva userà `depth > 1` con `limit` stretto in modo critico.

4. **Chiave metadata `SecurityContext` duplicata nel test**
   (`"eci-security-context-bin"`): `libs/go/eci/secctx.metadataKey` non è
   esportata dal pacchetto (deliberatamente, SPEC-011 — è un dettaglio di
   wire interno). Il test di scenario 6 la ridichiara come costante
   locale per costruire i metadata gRPC in uscita esattamente come un
   client reale farebbe — stesso valore documentato nei commenti di
   `secctx.go`, non un dettaglio interno fragile.

5. **Porta di default del server**: `RETRIEVAL_ENGINE_ADDR` default `:50053`
   — non specificata da questa SPEC; scelta per non collidere con la
   porta `50052` già usata dallo scaffold di interoperabilità manuale di
   SPEC-012 (`tests/interop/spec-012-secctx-py-go/server`), che resta un
   processo separato e non correlato.

6. **Versioni esatte risolte** (allineate deliberatamente a quelle già
   fissate da `libs/go`/`sink-graph`, non scelte indipendentemente):
   `google.golang.org/grpc v1.83.0`, `google.golang.org/protobuf v1.36.11`,
   `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.69.0`,
   `go.opentelemetry.io/otel v1.44.0`, `github.com/neo4j/neo4j-go-driver/v5 v5.28.4`
   (stessa versione di `tools/migrate-neo4j`/`sink-graph`, SPEC-015 — "non
   introdurne una seconda" rispettato anche sulla versione esatta, non
   solo sulla libreria). Dev-dependency:
   `github.com/testcontainers/testcontainers-go/modules/neo4j v0.43.0`
   (stessa versione di SPEC-015).

7. **Test di integrazione gated col build tag Go nativo `integration`**
   (stesso meccanismo di SPEC-005/008/015), non wired in `Taskfile.yml`:
   perimetro file di questa sessione è `services/retrieval-engine` e
   questa SPEC — `Taskfile.yml` è fuori scope (stesso precedente di
   SPEC-008/014/015).
