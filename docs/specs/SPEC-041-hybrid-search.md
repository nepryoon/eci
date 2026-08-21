# SPEC-041 — HybridSearch completo: port Go di D5 (T4.1)
Stato: implemented
Task-tree: T4.1 (primo task di Fase 4, dip. T3.1 già chiuso) · Servizio: services/retrieval-engine (Go, estende T1.4/SPEC-016) · ADD: Modulo 2, Deliverable D5

## 1. Obiettivo
Portare fedelmente in Go l'algoritmo `hybrid_graph_vector_search` (ADD, Deliverable D5, Python completo) dentro `retrieval-engine`, sostituendo l'attuale `HybridSearch` (T1.4, sola gamba grafo, `vector_leg_degraded` sempre `true`) con l'implementazione completa: seed vettoriale Qdrant, traversata grafo Neo4j bounded, fusione RRF (k=60), scoring di prossimità topologica. Non è ricerca semantica generale — richiede un `entry_node_id` noto (validato obbligatorio nella firma stessa di D5), orientata all'impact analysis da un punto di partenza dato (coerente con T4.2, che dipende da questo task). Verificato con test di parità diretti contro la reference Python reale (eseguita come sottoprocesso, non un fixture statico potenzialmente stantio).

## 2. Interfaccia

**Contratto proto, estensione additiva**: `HybridSearchRequest` (T1.4, `contracts/proto/`) esteso con `entry_node_id` (string, obbligatorio) e `max_depth` (int32) — campi assenti nella versione originale (T1.4 non aveva bisogno di un punto di partenza, essendo solo full-text sul nome). Nessun campo rimosso. **Correzione in fase di implementazione** (vedi §10 deviazione 1 in fondo alla SPEC): il precedente SPEC-026 §10 riguardava una migrazione SQL additiva — sempre un file NUOVO (status git "A"), libero da `task guard`/ADR-0002. Un'estensione di un `.proto` ESISTENTE è invece una modifica (status "M") sullo stesso file, che `task guard` protegge indipendentemente dalla natura additiva del contenuto: ADR necessaria, aggiunta come ADR-0007.

**Nuovo package** `services/retrieval-engine/internal/hybridsearch`, porting 1:1 delle cinque funzioni di D5:
```go
type Provenance struct {
    Repo, Path, Commit         string
    StartLine, EndLine         int
}

type RetrievedNode struct {
    NodeID                       string
    Domain                       string
    Source                       string // "graph" | "vector" | "fused"
    VectorScore, HopDistance     *float64 // nil = assente (Optional[T] di D5)
    GraphRank, VectorRank        *int
    RRFScore, CombinedScore      float64
    Provenance                   *Provenance
    Payload                      map[string]any
}

func VectorSearch(ctx, qdrantClient, collection string, queryVector []float32, domain, repo *string, limit int) ([]RetrievedNode, error)
func GraphTraversal(ctx, neo4jDriver, entryNodeID string, maxDepth int, domain, repo *string, graphLimit int) ([]RetrievedNode, error)
func RRFFuse(graphNodes, vectorNodes []RetrievedNode, k int) map[string]*RetrievedNode
func ApplyTopologicalProximity(fused map[string]*RetrievedNode, beta float64) []RetrievedNode
func HybridGraphVectorSearch(ctx, deps Deps, query, entryNodeID string, maxDepth int, opts ...Option) ([]RetrievedNode, error)
```

**`VectorSearch`**: query Qdrant per similarità (non per id noto — legge `node_id` dal *payload* di ciascun risultato, esattamente come D5 `p.payload.get("node_id", p.id)` — nessuna derivazione di id necessaria qui, a differenza di sink-vector/T3.1). Collection: `code_embeddings` (nome reale di SPEC-033 — D5 dichiara `"code_nodes"`, stantio, scritto prima che T3.1 esistesse davvero; usare il nome vero, dichiarato come deviazione).

**`GraphTraversal`**: STESSA query Cypher di D5, stessa struttura (`MATCH (seed:CodeNode {id: $entry_node_id}) MATCH path = (seed)<-[r:CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS*1..N]-(dep:CodeNode) ... WITH DISTINCT dep, min(length(path)) AS hop_distance ...`), stesso pattern di sicurezza per `max_depth` (validato come intero prima di essere interpolato letteralmente nella stringa Cypher — Cypher non parametrizza i bound di lunghezza variabile, D5 §note lo dichiara esplicitamente; MAI accettare `max_depth` come stringa non validata). Riusa il driver Neo4j GIÀ esistente in `retrieval-engine` (T1.4), nessuna nuova connessione.

**Client embedding**: nuovo, piccolo — stessa API TEI-nativa già usata da `services/embedding-worker/internal/embedclient` (SPEC-030), replicata (non importabile, modulo Go separato + `internal/`, stesso vincolo già incontrato ripetutamente in T3.4).

**Client Qdrant**: `github.com/qdrant/go-client`, stesso già in uso in `sink-vector` (T3.1) — libreria pubblica, importabile direttamente (a differenza degli helper `internal/` specifici di ciascun servizio).

**`RRFFuse`**: stessa logica di merge di D5 — un nodo che compare in ENTRAMBE le liste (graph e vector) accumula il contributo RRF di ciascuna (`1/(k+rank)` sommato, non sostituito); `hop_distance` prende il minimo tra le due occorrenze se entrambe presenti; i campi vettoriali/grafo mancanti vengono riempiti dalla seconda occorrenza se la prima non li aveva. Ordine di merge identico a D5 (grafo prima, poi vettoriale).

**`ApplyTopologicalProximity`**: `combined_score = rrf_score + beta * (1/(1+hop_distance) se presente, altrimenti 0)`, beta default 0.15, ordinamento decrescente per `combined_score`.

**`HybridGraphVectorSearch`**: orchestratore — validazione query/entry_node_id obbligatori; vector search **degrada senza abortire** (log + continua con lista vuota) se fallisce; graph traversal **obbligatoria** (fallisce l'intera funzione se fallisce); errore esplicito se ENTRAMBE le liste risultano vuote; fusione RRF; scoring+ordinamento; troncamento a `top_k` (default 25).

## 3. Comportamento (scenari)

1. **Dato** un nodo presente sia nei risultati grafo sia in quelli vettoriali (stesso `node_id`), **quando** eseguo la fusione RRF, **allora** il suo `rrf_score` finale è la somma dei due contributi (`1/(k+graph_rank) + 1/(k+vector_rank)`), non solo uno dei due.
2. **Dato** un nodo presente SOLO nei risultati grafo, **quando** eseguo la fusione, **allora** il suo `rrf_score` riflette solo il contributo grafo.
3. **Dato** un nodo presente SOLO nei risultati vettoriali, **quando** eseguo la fusione, **allora** il suo `rrf_score` riflette solo il contributo vettoriale.
4. **Dato** due nodi fusi con lo stesso `rrf_score` ma `hop_distance` diversa, **quando** applico il boost di prossimità topologica, **allora** quello con `hop_distance` minore ottiene un `combined_score` più alto.
5. **Dato** Qdrant irraggiungibile durante la fase di vector search, **quando** eseguo `HybridGraphVectorSearch`, **allora** la funzione NON fallisce — prosegue con la sola gamba grafo (log di degradazione, non un errore fatale).
6. **Dato** Neo4j irraggiungibile durante la fase di graph traversal, **quando** eseguo `HybridGraphVectorSearch`, **allora** la funzione fallisce esplicitamente — questa gamba è obbligatoria, non degrada.
7. **Dato** sia grafo sia vettoriale privi di risultati, **quando** eseguo `HybridGraphVectorSearch`, **allora** ottengo un errore esplicito, non una lista vuota silenziosa.
8. **Dato** lo STESSO fixture (Postgres+Neo4j+Qdrant popolati identicamente) eseguito sia dalla reference Python di D5 (sottoprocesso reale, non un fixture statico) sia dal port Go, **quando** confronto i due output, **allora** l'ordine dei `node_id` risultanti e i punteggi (entro tolleranza float) combaciano.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| `max_depth` <= 0 | Errore esplicito prima di costruire la query Cypher (stesso principio di D5, `ValueError` se `max_depth < 1`) |
| Query o `entry_node_id` vuoti | Errore esplicito immediato, nessuna chiamata a Qdrant/Neo4j |
| `entry_node_id` che non corrisponde a nessun nodo esistente | Graph traversal ritorna zero risultati (non un errore — `MATCH` su un nodo inesistente in Cypher produce semplicemente nessun match), coerente col comportamento reale di Neo4j |

## 5. Non-goals
Nessuna integrazione con GDS batch (T4.3, task separato). Nessun reranking (T4.4). Nessun context packing (T4.5). Nessuna modifica a `ExpandNeighbors`/`GetNode` esistenti (T1.4, invariati).

## 6. Vincoli dall'ADD
Modulo 2, Deliverable D5 — questa SPEC ne è il port diretto, non una reinterpretazione. RRF k=60 invariante dichiarato esplicitamente nel Modulo 1 e ribadito qui.

## 7. Test plan
Unitari puri per `RRFFuse`/`ApplyTopologicalProximity` (nessuna infrastruttura reale necessaria, solo liste di `RetrievedNode` costruite a mano). Integrazione con Neo4j+Qdrant reali (testcontainers) per `VectorSearch`/`GraphTraversal`/`HybridGraphVectorSearch` (scenari 1-7). Scenario 8 (parità): la reference Python di D5 copiata verbatim in `services/retrieval-engine/testdata/hybrid_search_reference/` (stesso principio già stabilito per "port fedele, non reinterpretato"), eseguita come sottoprocesso reale (richiede un venv con `neo4j`+`qdrant-client` installati) contro lo stesso fixture del test Go, output confrontato — non un fixture JSON statico pre-calcolato, per evitare che diverga silenziosamente dalla reference se quest'ultima cambiasse in futuro.

## 8. Osservabilità
Stessa fondazione OTel già stabilita per `retrieval-engine` — uno span per fase (embedding, vector search, graph traversal, fusione).

## 9. Criteri di accettazione
- [x] Scenari 1-8 verificati con evidenza diretta, in particolare lo scenario 8 (parità con la reference Python REALMENTE eseguita, non un fixture statico). Unitari puri per scenari 1-4 (`internal/hybridsearch/hybridsearch_test.go`, scritti PRIMA dell'implementazione — verificato il fallimento per pacchetto assente prima di scrivere `types.go`/`fuse.go`). Integrazione Neo4j+Qdrant reali (testcontainers) + embedder-fake reale (sottoprocesso) per scenari 1-3/5-7 end-to-end (`internal/hybridsearch/hybridsearch_integration_test.go`) e per lo scenario 8 (`internal/hybridsearch/parity_integration_test.go`, `run_reference.py` come sottoprocesso reale contro lo STESSO fixture Neo4j/Qdrant del test Go — ordine dei `node_id` e punteggi entro tolleranza `1e-6` verificati uguali).
- [x] Edge case tabella §4 verificati esplicitamente (`EdgeCase_MaxDepthLessThanOneFailsExplicitly`, `EdgeCase_EmptyQueryOrEntryNodeIdFailsExplicitlyNoIO` — con dipendenze rotte, per distinguere l'errore di validazione da un errore di I/O — `EdgeCase_UnknownEntryNodeIdReturnsEmptyGraphNotError`).
- [x] Nome collection Qdrant verificato essere `code_embeddings` nel codice (`internal/hybridsearch/orchestrate.go`, `defaultOptions()`), non `code_nodes`.
- [x] Estensione proto verificata additiva (nessun campo rimosso/rinominato — `entry_node_id`=11, `max_depth`=12, tag mai riusati), nessuna regressione sui client esistenti di `HybridSearch` (dispatcher su `entry_node_id`, vedi §10 punto 1; verificato esplicitamente da `TestHybridSearchGraphVectorDispatch/EntryNodeIdEmpty_UsesLegacyFullTextPath`).
- [x] Nessuna regressione sui test esistenti di T1.4 (`TestRetrievalEngineServer`, tutti e 7 gli scenari, rieseguiti invariati — `go test -tags=integration ./internal/server/... -run TestRetrievalEngineServer -v -count=1`, verde).

## 10. Deviazioni rispetto alla SPEC

1. **Serve un ADR per l'estensione proto, contrariamente a quanto
   dichiarato in §2**: `task guard`/CI (`.github/workflows/ci.yml`, job
   `guard`) blocca qualunque modifica (M) a un file già tracciato sotto
   `contracts/`, indipendentemente dalla natura additiva del contenuto
   (ADR-0002) — a differenza delle aggiunte di file nuovi (A), libere. Il
   riferimento originale a SPEC-026 §10 riguardava una migrazione SQL
   *additiva* (sempre un file nuovo, status "A"); un'estensione di un
   `.proto` *esistente* è invece uno status "M" sullo stesso file. Risolto
   con `docs/decisions/ADR-0007-hybridsearchrequest-entry-node-id-max-depth.md`
   (unico file toccato fuori dal perimetro dichiarato nelle istruzioni di
   sessione — contracts/proto, services/retrieval-engine, questa SPEC —,
   scelta confermata esplicitamente con l'utente prima di procedere).
   Verificato empiricamente: `BASE_REF=main bash scripts/guard.sh` passa
   solo perché non c'era ancora un commit sul branch al momento del primo
   controllo (il diff è contro HEAD, non contro il working tree) — il
   comportamento reale del guard va verificato DOPO il commit finale.

2. **`HybridSearch` è un dispatcher additivo, non una sostituzione
   incondizionata**: un client che imposta `entry_node_id` usa il nuovo
   percorso grafo+vettoriale completo (`hybridSearchGraphVector`,
   `hybridsearch.HybridGraphVectorSearch`); un client che NON lo imposta
   (stringa vuota — qualunque client scritto prima di questa SPEC, dato che
   il campo non esisteva) riceve ESATTAMENTE il comportamento T1.4
   invariato (`hybridSearchFullTextOnly`, corpo copiato senza modifiche
   dalla vecchia `HybridSearch`). Necessario per soddisfare
   simultaneamente §1 ("richiede un `entry_node_id` noto, validato
   obbligatorio nella firma stessa di D5" — la funzione Go valida
   query/entry_node_id obbligatori esattamente come D5) e §9 ("nessuna
   regressione sui test di T1.4", che chiamano `HybridSearch` senza
   `entry_node_id`): senza il dispatcher le due richieste sarebbero
   contraddittorie.

3. **Bindings rigenerati: solo Go (`buf generate`), non Python
   (`grpc_tools.protoc`)**: `task proto:gen` (script
   `scripts/task-proto-gen.sh`) rigenera entrambi; qui è stato eseguito
   solo il passo Go (`cd contracts && buf generate`, che tocca
   `libs/go/eci/retrieval/v1/retrieval.pb.go`). La reference D5 usa
   `neo4j`/`qdrant-client` direttamente, mai gli stub gRPC generati — non
   c'è alcun consumo Python di `HybridSearchRequest` in questa SPEC, quindi
   rigenerare `libs/py/eci_core/retrieval/v1/` non ha alcun effetto
   osservabile e resta fuori dal perimetro `contracts/proto` +
   `services/retrieval-engine` dichiarato dalle istruzioni. `libs/go` è
   toccato come conseguenza meccanica inevitabile dell'estensione additiva
   (il codice Go compilato di `services/retrieval-engine` non può
   referenziare `EntryNodeId`/`MaxDepth` senza di esso) — non un'estensione
   deliberata del perimetro.

4. **`VectorSearch` legge campi di provenance PIATTI dal payload Qdrant
   (`repo`/`path`/`start_line`/`end_line`/`commit` a livello radice),
   esattamente come D5**: la scrittura reale di `sink-vector` (SPEC-033)
   nidifica la provenance sotto la chiave `"provenance"` (un blob JSON
   opaco). Porto fedele di D5 (§2: "stessa query Cypher, stessa struttura")
   applicato anche qui per coerenza — un port che leggesse la forma nidificata
   reale sarebbe un'interpretazione, non un port 1:1, e romperebbe la
   parità con la reference Python allo scenario 8 (che legge anch'essa i
   campi piatti). Conseguenza pratica nota: con i dati REALI scritti da
   `sink-vector` oggi, `VectorSearch` non popola mai `Provenance` per i
   risultati vettoriali (la condizione `hasProvenanceKeys` non è mai vera).
   Riconciliare le due forme è fuori perimetro di questa SPEC (tocccherebbe
   `sink-vector`/SPEC-033, non elencato tra i file toccabili); da
   affrontare in una SPEC dedicata se necessario.

5. **`GraphCandidates`/`VectorCandidates`/`VectorLegDegraded` (nuovo
   percorso) derivati dal risultato FINALE (fuso, dopo troncamento
   `top_k`), non da conteggi grezzi pre-fusione**: la firma di
   `HybridGraphVectorSearch` è esplicita nell'interfaccia della SPEC (§2:
   `([]RetrievedNode, error)`, nessun conteggio separato) — rispettata
   letteralmente. Il server deriva quindi la diagnostica contando, nel
   risultato finale, quanti nodi hanno `GraphRank`/`VectorRank` non-nil.
   Scelta pragmatica non testata esplicitamente da nessuno degli 8 scenari
   (che validano l'algoritmo, non questi tre campi diagnostici sul nuovo
   percorso).

6. **Distinzione per-tipo-di-eccezione di D5 nella vector search
   collassata a un solo tipo di errore in Go**: D5 cattura solo
   `(UnexpectedResponse, ValueError)` attorno a `_vector_search` — qualunque
   altra eccezione propagherebbe e farebbe fallire l'intera funzione. In Go,
   `VectorSearch` ritorna un solo tipo di errore per qualunque fallimento
   (nessuna gerarchia di eccezioni tipizzate equivalente nel client
   `go-client` di Qdrant), quindi `HybridGraphVectorSearch` degrada su
   QUALUNQUE errore della vector search, non solo un sottoinsieme.
   Semplificazione idiomatica Go, dichiarata: non risulta in nessun
   comportamento diverso osservabile per gli scenari 1-8 (tutti gli errori
   testati — Qdrant irraggiungibile — rientrano comunque nel caso
   degradabile).

7. **Fixture di test con id Qdrant interi (FNV-1a di `node_id`), non
   UUIDv5 come `sink-vector`**: solo per i fixture dei test di integrazione
   di questa SPEC (`internal/hybridsearch/fixture_integration_test.go`) —
   Qdrant accetta sia UUID sia interi come point id; un fixture locale non
   ha bisogno della stessa derivazione deterministica-ma-opaca di
   `sink-vector` (SPEC-033, che deve interoperare con `code_embedding.id`
   reale). Nessun impatto su `VectorSearch`, che legge sempre `node_id` dal
   *payload*, mai dal point id (§2, stesso principio di D5).

8. **Venv della reference Python creato in
   `services/retrieval-engine/testdata/hybrid_search_reference/.venv/`
   (ignorato da git, `.gitignore` root `**/.venv/`)**: `requirements.txt`
   (`neo4j`, `qdrant-client`) committato; il venv stesso si ricostruisce
   lazily al primo `go test -tags=integration` (stesso pattern già
   stabilito per `fakes/embedder-fake/.venv` in
   `embedding-worker`/SPEC-030).

9. **`run_reference.py` (driver del sottoprocesso) NON fa parte della
   reference D5 copiata verbatim**: `hybrid_search_reference.py` (byte-per-
   byte identico al blocco Python dell'ADD, verificato con `diff`) resta
   intoccato; `run_reference.py` è harness di test nuovo (parsing CLI,
   costruzione client reali, embedder HTTP verso `embedder-fake`, stampa
   JSON) scritto per collegare la reference a un sottoprocesso eseguibile —
   necessario perché D5 è una funzione, non uno script, e la SPEC richiede
   di eseguirla "come sottoprocesso reale" (§7).
