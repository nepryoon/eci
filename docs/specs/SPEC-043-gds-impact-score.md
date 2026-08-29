# SPEC-043 — GDS batch: impact_score (T4.3)
Stato: verified
Task-tree: T4.3 (dip. T3.4, già chiuso) · Nuovo: tools/gds-impact (Go), estensione a deploy/compose/docker-compose.yml · ADD: Modulo 2 §1.4-1.5

## 1. Obiettivo
Job batch (non un servizio long-running, invocabile manualmente o da un trigger esterno — stesso principio già stabilito per `tools/reconcile`/`tools/gc-postgres`) che, dato un nodo di ingresso, proietta il sottografo rilevante in-memory via Neo4j GDS, esegue PPR seedata + betweenness campionata + Leiden, combina i risultati nella formula di priorità dell'ADD, e scrive `impact_score` come proprietà sui nodi Neo4j interessati. Chiude direttamente due lacune già segnalate esplicitamente durante T4.1/T4.2: l'enum `ImpactKind` (severità, rimasto `UNSPECIFIED`) e `min_impact_score` (mai implementato in `ImpactAnalysisRequest`).

## 2. Interfaccia

**Compose, modifica dichiarata**: `deploy/compose/docker-compose.yml`, servizio `neo4j`, aggiunta `NEO4J_PLUGINS=["graph-data-science"]` — verificato (non presunto) che GDS non è incluso di default nell'immagine `neo4j:5-community` già in uso da Fase 0; tutti e tre gli algoritmi usati qui sono tier production (namespace `gds.` senza prefisso `beta`/`alpha`), nessuna licenza Enterprise richiesta.

**CLI storica T4.3** `tools/gds-impact --entry-node-id=<id> [--max-depth=4] [--sampling-size=N]`. Da T6.7/SPEC-061 l'interfaccia richiede inoltre `--tenant-id`, `--repo` e `--acl-group`: il requisito di sicurezza successivo restringe il job a una partizione e non reinterpreta l'evidenza T4.3.

**Pipeline** (tutta via Cypher/GDS procedure attraverso il driver Neo4j già stabilito nel progetto — nessun nuovo client, GDS si invoca come procedure Cypher):
1. `gds.graph.project.estimate` (stima memoria, log del risultato — nessun abort automatico in questa SPEC, solo visibilità, coerente con l'assenza di un budget di memoria configurato altrove nel progetto).
2. `gds.graph.project` — proiezione in-memory del sottografo REVERSE-reachable da `entry_node_id` entro `max_depth` (stessi sei tipi di arco di `GraphTraversal`/T4.1), orientamento REVERSE (richiesto per la PPR seedata, ADD §1.4).
3. `gds.pageRank.stream(graphName, {sourceNodes: [entryNodeID], maxIterations: 20, dampingFactor: 0.85})`.
4. `gds.betweenness.stream(graphName, {samplingSize: N})` — `samplingSize` di default = numero di nodi nella proiezione (risultati esatti) se non specificato via flag, coerente con la documentazione GDS citata dall'ADD ("impostare samplingSize al conteggio dei nodi del grafo produce risultati esatti").
5. `gds.leiden.stream(graphName)` — community id per nodo, usato per l'attributo di raggruppamento (non pesato nella formula di `impact_score`, l'ADD non lo include nella combinazione lineare — resta un dato separato scritto come proprietà `community_id`).
6. Normalizzazione (`ppr_norm`, `betweenness_norm`: min-max sul sottografo proiettato) e combinazione: `impact_score = w_ppr*ppr_norm + w_prox*(1/hop_distance) + w_bc*betweenness_norm` (pesi di default dichiarati: `w_ppr=0.5, w_prox=0.3, w_bc=0.2` — nessun valore analogo esiste altrove nel progetto, scelta ragionevole non desunta da nient'altro, configurabile via flag).
7. Write-back: `SET n.impact_score = $score, n.community_id = $community, n.impact_kind = $kind` su ciascun nodo del sottografo.
8. `gds.graph.drop(graphName)` — pulizia della proiezione in-memory, sempre eseguita (anche in caso di errore nei passi precedenti, via `defer`-equivalente) per non perdere memoria tra invocazioni successive.

**`impact_kind`, derivazione dichiarata (Non-goal esplicito su metà della regola dell'ADD)**: solo dal tipo di arco più vicino al nodo di ingresso nel percorso più breve (stesso principio "percorso più corto vince" già stabilito in T4.1/T4.2) — `IMPORTS`/`DEPENDS_ON` → `module-boundary`; `CALLS` → `behavioral`; `IMPLEMENTS`/`EXTENDS`/`OVERRIDES` → `syntactic`. La seconda metà della regola dell'ADD (presenza/assenza di un cambio di firma) richiederebbe un confronto vecchia-firma/nuova-firma che questa pipeline non traccia in nessuna forma oggi (`ast_hash` è sull'intera entità, non isolato sulla sola firma) — dichiarato esplicitamente come Non-goal, non un'omissione silenziosa.

## 3. Comportamento (scenari)

1. **Dato** un piccolo grafo noto con un nodo di ingresso e tre nodi raggiungibili a hop diversi, **quando** eseguo il job, **allora** ciascuno dei tre nodi riceve un `impact_score` scritto su Neo4j, calcolabile a mano con la formula dichiarata e confrontabile con quanto scritto (entro tolleranza float).
2. **Dato** lo stesso grafo, **quando** ispeziono `impact_kind` sui nodi risultanti, **allora** riflette correttamente il tipo di arco più vicino al nodo di ingresso per ciascuno (`CALLS`→`behavioral`, `IMPLEMENTS`→`syntactic`, `IMPORTS`→`module-boundary`).
3. **Dato** il job appena eseguito, **quando** interrogo `gds.graph.list`, **allora** nessuna proiezione in-memory resta residua — pulita sempre, anche in caso di errore a metà pipeline (verificato forzando un fallimento dopo la proiezione, prima del write-back).
4. **Dato** `entry_node_id` inesistente, **quando** eseguo il job, **allora** la proiezione risulta vuota, nessun `impact_score` scritto, nessun errore (stesso principio già stabilito in T4.1/T4.2 per un nodo di ingresso sconosciuto).
5. **Dato** `--sampling-size` non specificato, **quando** eseguo il job, **allora** la chiamata a `gds.betweenness.stream` riceve `samplingSize` pari al conteggio dei nodi della proiezione (risultati esatti per il default) — verificabile ispezionando il parametro passato, non solo il risultato finale.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Neo4j irraggiungibile durante qualunque fase | Errore esplicito, nessun tentativo di scrittura parziale — se la proiezione era già stata creata, un tentativo di `gds.graph.drop` viene comunque eseguito prima di propagare l'errore originale |
| `max_depth` <= 0 | Errore esplicito prima di avviare la proiezione (stesso principio già stabilito in T4.1/T4.2) |

## 5. Non-goals
Derivazione di `impact_kind` dal cambio di firma (§2, dichiarato). Nessun trigger automatico del job (invocazione manuale/da CI esterna, fuori scope). Nessuna integrazione con `ImpactAnalysis`/`HybridSearch` (T4.1/T4.2) per LEGGERE `impact_score`/`impact_kind` scritti qui — questa SPEC li scrive, un consumo reale (es. popolare l'enum `ImpactKind`/`min_impact_score` nelle RPC esistenti) è lavoro futuro esplicitamente fuori da questa SPEC. Nessuna schedulazione periodica automatica su tutti i nodi del grafo — un'invocazione è sempre per un singolo `entry_node_id` alla volta, coerente col fatto che la PPR è intrinsecamente personalizzata su un seed.

## 6. Vincoli dall'ADD
Modulo 2 §1.4 (formula, algoritmi, `.estimate`) e §1.5 (derivazione `impact_kind`, con la semplificazione dichiarata in §2).

## 7. Test plan
Integrazione con Neo4j reale (testcontainers, GDS attivato via `NEO4J_PLUGINS`) — nessun test unitario puro ha senso qui, dato che l'intera pipeline è una sequenza di chiamate GDS reali, non logica isolabile in Go.

## 8. Osservabilità
Log testuale del job (nodi proiettati, tempo per fase, risultato di `.estimate`) — stesso principio già stabilito per `tools/gc-postgres`/`tools/reconcile`.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta, in particolare lo scenario 1 (valore di `impact_score` calcolato a mano e confrontato, non solo "un valore è stato scritto"): il test ricalcola `w_ppr*ppr_norm + w_prox*(1/hop_distance) + w_bc*betweenness_norm` dalle componenti (`ppr_norm`/`betweenness_norm`/`hop_distance`) esposte da `Result.Scores` e verifica che combaci sia col valore calcolato dal job sia con `n.impact_score` letto DIRETTAMENTE da Neo4j dopo l'esecuzione (`internal/gdsimpact/run_integration_test.go`, `Scenarios1And2_FormulaMatchesWrittenValuesAndImpactKind`, fixture a catena di 3 hop, tre `impact_kind` diversi in un solo fixture).
- [x] Edge case tabella §4 verificati esplicitamente: Neo4j irraggiungibile (`EdgeCase_Neo4jUnreachableFailsExplicitly`), `max_depth<=0` (`EdgeCase_MaxDepthZeroFailsExplicitlyBeforeAnyQuery`).
- [x] `NEO4J_PLUGINS=["graph-data-science"]` verificato funzionante con un avvio reale dello stack dev (`task up`, tutti i 9 servizi healthy) — `CALL gds.version()` eseguito via `cypher-shell` nel container reale PRIMA di scrivere qualunque codice Go (`gdsVersion=2.13.11`), come richiesto esplicitamente dalle istruzioni di sessione.
- [x] Nessuna regressione sui test esistenti (in particolare T1.3/sink-graph, che scrive sugli stessi nodi Neo4j): `TestSinkGraphConsumer`/`TestSinkGraphResilienceWrappingGenuinelyWired`/`TestSinkGraphMetricsExposedViaRealHTTP` rieseguiti, verdi (un fallimento isolato di uno scenario in una prima esecuzione si è rivelato flaky preesistente — vedi §10 — non riprodotto in due riesecuzioni successive).

## 10. Deviazioni rispetto alla SPEC

1. **DUE proiezioni GDS per esecuzione (REVERSE + UNDIRECTED), non una
   sola**: verificato empiricamente PRIMA di scrivere codice (contro GDS
   2.13.11 reale, via `cypher-shell`) che `gds.leiden.stream` rifiuta un
   grafo orientato ("The Leiden algorithm works only with undirected
   graphs"), mentre PPR seedata richiede REVERSE (ADD §1.4: "Su proiezione
   a orientamento REVERSE, il PPR misura quanto ciascun dipendente è
   'esposto' al target"). Le due esigenze sono incompatibili su una singola
   proiezione. Risolto proiettando lo STESSO sottografo scoperto due volte,
   con nomi derivati dallo stesso suffisso casuale (`gds-impact-rev-<hex>`,
   `gds-impact-undir-<hex>`) — `gds.graph.drop` (§2 punto 8) chiamato per
   ENTRAMBE nella pulizia, sempre, stesso principio "sempre eseguita" della
   SPEC applicato a due proiezioni invece di una.

2. **Proiezione tramite la forma "subquery" di `gds.graph.project`
   (GDS 2.x, non deprecata), non la forma nativa label-based né
   `gds.graph.project.cypher`**: la forma nativa (`gds.graph.project(name,
   label, relTypes, config)`) proietta l'INTERA label, non un sottografo
   filtrato — incompatibile con "proietta il sottografo RILEVANTE" (§2).
   `gds.graph.project.cypher` (la forma esplicitamente citata come
   possibile alternativa nella nota dell'ADD) supporta filtri Cypher
   arbitrari ma — verificato empiricamente, non presunto —
   **non** supporta `undirectedRelationshipTypes` in configurazione
   (rifiutato con "Unexpected configuration key"), quindi non può produrre
   la proiezione UNDIRECTED richiesta da Leiden (deviazione 1). La forma
   "subquery" (`gds.graph.project(name, sourceNode, targetNode, dataConfig,
   configuration)`, introdotta in GDS 2.x) supporta ENTRAMBI i requisiti
   contemporaneamente: filtro Cypher arbitrario (via `MATCH ... WHERE
   n.id IN $node_ids AND m.id IN $node_ids`) e
   `undirectedRelationshipTypes` — verificata funzionante per entrambe le
   forme (REVERSE via sorgente/target scambiati, UNDIRECTED via
   configurazione) prima di scriverci sopra qualunque codice Go.

3. **Deviazione storica T4.3, superseded da T6.7/SPEC-061 —
   `gds.graph.project.estimate` (§2 punto 1) chiamato sull'INTERA label
   `:CodeNode`, non sul sottografo filtrato**: la procedura `.estimate`
   accetta solo la forma nativa label-based (nessun equivalente per la
   forma "subquery" usata per la proiezione reale, deviazione 2) —
   verificato che non esiste una `.estimate` per un filtro Cypher
   arbitrario. La stima loggata è quindi un limite SUPERIORE sull'intera
   label, non sul sottografo effettivamente proiettato (più piccolo) —
   era dichiarato esplicitamente nel log stesso (`logProjectionEstimate`),
   mai usata per un abort automatico (§2: "nessun abort automatico ...
   solo visibilità", requisito comunque rispettato). T6.7 rimuove questa
   lettura globale: dopo le proiezioni Cypher per-ACL esegue
   `gds.*.stream.estimate` sui graph name autorizzati prima degli algoritmi.
   Il requisito ADD della stima resta soddisfatto senza osservare cardinalità
   cross-tenant.

4. **Discovery del sottografo: BFS livello-per-livello duplicata da
   T4.1/T4.2, non importata**: `tools/gds-impact` è un modulo Go separato
   da `services/retrieval-engine` (`internal/` non attraversa il confine di
   modulo) — stesso principio già accettato per `embedclient` (T4.1) e per
   la duplicazione dello stesso pattern BFS tra `hybridsearch`/
   `impactanalysis` (T4.1/T4.2). La query per-livello è STESSA struttura
   (stessi sei tipi di arco, stesso principio "percorso più corto vince"),
   non un algoritmo diverso.

5. **Ordine effettivo delle attività di sessione: verifica GDS via
   `cypher-shell` diretto INTRECCIATA con l'implementazione, non
   strettamente "verifica GDS, POI scrivi tutti i test, POI implementa"**:
   la sintassi Cypher/GDS esatta necessaria (proiezione filtrata +
   doppio orientamento, validata nelle deviazioni 1/2) non è documentata
   da nessuna parte nel progetto né interamente nell'ADD (che documenta la
   forma nativa e accenna alla forma Cypher, ma non la forma "subquery" né
   il vincolo di Leiden su UNDIRECTED) — non derivabile senza sperimentare
   contro un'istanza GDS reale. La verifica "GDS è davvero attivo" (istruzione
   2, `CALL gds.version()`) è stata eseguita PRIMA di scrivere qualunque
   codice, come richiesto; la successiva esplorazione empirica dell'API
   GDS (istruzioni 2/3 di fatto intrecciate) ha preceduto la scrittura del
   file di test finale, che riflette comunque scenari scritti guardando
   SOLO alla SPEC (§3), verificati falliti per assenza del pacchetto prima
   di implementare `run.go`/`algorithms.go`/`project.go`.

6. **`TestSinkGraphConsumer/Scenario5.../rel_type_non_valido` flaky,
   preesistente, non causato da questa SPEC**: una prima esecuzione
   dell'intera suite `sink-graph` (dopo l'implementazione di questa SPEC)
   ha mostrato quello scenario fallire con `outcome=OutcomeDuplicate`
   invece di `OutcomeInvalidSkipped` — non riprodotto in due riesecuzioni
   immediatamente successive (stesso codice, nessuna modifica). Questa
   SPEC non tocca `services/sink-graph` in alcun modo (fuori dal perimetro
   dichiarato) né il suo Kafka/Postgres/Neo4j di test (container
   testcontainers propri, indipendenti dallo stack `docker compose`
   modificato qui) — nessun meccanismo causale plausibile individuato;
   coerente con un carico di sistema elevato (molte suite testcontainers
   eseguite in sequenza/parallelo in questa sessione) che ha alterato il
   timing della redelivery Kafka nel test, stesso genere di flakiness da
   contesa di risorse già osservato e documentato in SPEC-042 §10.
