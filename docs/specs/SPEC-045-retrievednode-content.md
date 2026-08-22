# SPEC-045 — Idratazione contenuto di RetrievedNode (prerequisito per T4.5)
Stato: implemented
Task-tree: prerequisito non nominato esplicitamente da Fase 4 (concordato in chat) — colma un gap segnalato tre volte durante T4.1 (SPEC-041 §10) e T4.4 (SPEC-044 §10) · Servizio: services/retrieval-engine (Go, estende T4.1/SPEC-041) · ADD: nessuna sezione specifica (conseguenza pratica di §2.4, che presume testo disponibile da impacchettare)

## 1. Obiettivo
`hybridsearch.RetrievedNode` (T4.1) non porta mai `name`/`source_text` — segnalato come limite pratico reale sia in SPEC-041 §10 (provenance sempre nil sui risultati vettoriali) sia in SPEC-044 §10 (il reranker riceve solo `node_id` come testo, un cross-encoder reale non avrebbe nulla di semanticamente utile). T4.5 (context packing) presume testo disponibile da impacchettare — senza questa SPEC non avrebbe nulla da fare. Colma il gap: `name` da Neo4j (già presente come proprietà, mai letto), `source_text` da OpenSearch (condizionale al flag `include_source_text` già esistente nello scaffold proto, mai implementato).

## 2. Interfaccia

**`RetrievedNode`** (`hybridsearch/types.go`, T4.1) esteso con `Name string`, `SourceText string` (`Summary` esplicitamente NON aggiunto — Non-goal, vedi §5: nessun meccanismo nella pipeline produce riassunti oggi).

**`name`, da Neo4j**: già presente come proprietà `n.name` sui nodi `:CodeNode` (scritta da `sink-graph`/T1.3 fin da SPEC-015, derivata dal payload outbox `CodeNode` che la include da sempre — mai letta finora da `retrieval-engine`). Per la gamba grafo (`GraphTraversal`, T4.1): un campo in più nella `RETURN` già esistente, nessuna nuova query. Per la gamba vettoriale (`VectorSearch`, T4.1 — Qdrant non porta `name` nel payload, SPEC-033 non lo scrive): un lookup batch Neo4j aggiuntivo (`MATCH (n:CodeNode) WHERE n.id IN $node_ids RETURN n.id, n.name`, UNA query per l'intero set di risultati vettoriali, non N query separate) — riusa il driver Neo4j già presente in `hybridsearch.Deps`, nessuna nuova dipendenza a livello di struct.

**`source_text`, da OpenSearch, condizionale**: nuovo client OpenSearch in `retrieval-engine` (mai presente finora in questo servizio — le altre due gambe, Neo4j/Qdrant, erano già presenti; stesso client/versione `/v4` già stabilito in `sink-search`/SPEC-034, non Postgres diretto — `retrieval-engine` non ha mai avuto una connessione Postgres, OpenSearch è un read-path già stabilito nell'architettura ibrida, coerente con "le tre viste sono per la lettura, Postgres per la scrittura"). Attivo SOLO quando `HybridSearchRequest.include_source_text` (campo già esistente nello scaffold proto D7, mai implementato) è `true` — quando `false` (default), nessuna chiamata OpenSearch, `SourceText` resta stringa vuota. Query per `entity_id` (filtro sul campo `entity_id` già indicizzato da `sink-search`/SPEC-034), chunk concatenati in ordine di `chunk_index` crescente.

## 3. Comportamento (scenari)

1. **Dato** un risultato della gamba grafo, **quando** eseguo `HybridGraphVectorSearch`, **allora** `Name` è popolato da `n.name` di Neo4j (stesso valore già scritto da `sink-graph`).
2. **Dato** un risultato della SOLA gamba vettoriale (mai visto dal grafo), **quando** eseguo `HybridGraphVectorSearch`, **allora** `Name` è comunque popolato, tramite il lookup batch Neo4j aggiuntivo — UNA sola query per l'intero set vettoriale, non una per nodo (verificabile contando le query eseguite, non solo il risultato).
3. **Dato** `include_source_text=false` (default), **quando** eseguo `HybridGraphVectorSearch`, **allora** `SourceText` resta vuoto per ogni risultato, nessuna chiamata OpenSearch effettuata.
4. **Dato** `include_source_text=true` e un'entità con più chunk, **quando** eseguo `HybridGraphVectorSearch`, **allora** `SourceText` è la concatenazione dei chunk di quell'entità in ordine di `chunk_index` crescente (non l'ordine di arrivo dei risultati OpenSearch, non garantito).
5. **Dato** `include_source_text=true` e un'entità senza alcun chunk indicizzato in OpenSearch (mai passata da `sink-search`), **quando** eseguo `HybridGraphVectorSearch`, **allora** `SourceText` resta vuoto per quel nodo, nessun errore.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| OpenSearch irraggiungibile con `include_source_text=true` | Errore esplicito (il client l'ha richiesto esplicitamente col flag, stesso principio già stabilito per il reranker in T4.4 — un fallimento su qualcosa di esplicitamente richiesto non deve degradare silenziosamente) |
| Il lookup batch `name` su Neo4j fallisce | Errore esplicito — `name` è una proprietà base attesa sempre disponibile per un nodo `:CodeNode` reale, un suo fallimento indica un problema di connessione, non un'assenza di dati normale |

## 5. Non-goals
`Summary` (nessun meccanismo di riassunto esiste ancora nella pipeline — resta un campo assente su `RetrievedNode`, non un campo presente ma sempre vuoto). Nessuna modifica a `Provenance` (limite già dichiarato in SPEC-041 §10, causa diversa — payload Qdrant piatto vs nidificato — non toccata qui). Nessuna modifica a T4.2/T4.3/T4.4 (consumano `RetrievedNode` ma questa SPEC ne allarga solo i campi disponibili, non cambia le loro firme).

## 6. Vincoli dall'ADD
Nessuno specifico — conseguenza pratica necessaria per §2.4 (T4.5), che presume testo disponibile da impacchettare.

## 7. Test plan
Integrazione con Neo4j+Qdrant+OpenSearch reali (testcontainers, tutti e tre già stabiliti singolarmente in T4.1/T3.2 — combinati qui per la prima volta nello stesso test).

## 8. Osservabilità
Nessun requisito nuovo oltre agli span già esistenti in T4.1.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta, in particolare lo scenario 2 (una sola query batch, non N) e lo scenario 4 (ordine di concatenazione corretto): `internal/hybridsearch/content_integration_test.go`, `TestHybridGraphVectorSearchContentHydration` — Neo4j+Qdrant+OpenSearch+embedder-fake reali insieme, via la funzione pubblica `HybridGraphVectorSearch`. Scenario 2 verificato con un driver Neo4j reale avvolto da un contatore di chiamate `session.Run` (entry_node_id isolato + 2 nodi vector-only mancanti di `Name`: totale query <= 2, non 1+2=3 come sarebbe con una query per nodo). Scenario 4 verificato inserendo i chunk in OpenSearch in ordine SCRAMBLED (chunk_index 2,0,1) e controllando la concatenazione risultante ("AAA\nBBB\nCCC").
- [x] Edge case tabella §4 verificati esplicitamente: OpenSearch irraggiungibile con `include_source_text=true` (errore esplicito, via la funzione pubblica); lookup batch `name` su Neo4j fallito, isolato dal fallimento di `GraphTraversal` con un test white-box dedicato (`internal/hybridsearch/hydrate_integration_test.go`, chiama `hydrateNames` direttamente con un driver reale verso un indirizzo irraggiungibile).
- [x] Nessuna regressione sui test esistenti di T1.4/T4.1/T4.2/T4.4 (che consumano `RetrievedNode`): intera suite `services/retrieval-engine` (unitari + integrazione, `-p 1`) rieseguita verde insieme ai nuovi test.

## 10. Deviazioni rispetto alla SPEC

1. **`GraphTraversal` (T4.1): un campo aggiunto alla RETURN esistente,
   deviazione dichiarata rispetto alla stretta fedeltà a D5** — D5 non
   proietta `name`. Questa deviazione è però esplicitamente SANCITA da
   SPEC-045 §2 ("un campo in più nella RETURN già esistente, nessuna nuova
   query"), non introdotta di iniziativa: non tocca `WHERE`/`ORDER BY`/
   `LIMIT`, stesso insieme di righe/ordine di prima — verificato che
   `TestHybridSearchParity` (T4.1, parità con la reference Python D5)
   resta verde: quel test confronta solo `node_id`/`rrf_score`/
   `combined_score`/`hop_distance`, mai `name` (che la reference Python
   non produce comunque, essendo D5 immutata).

2. **Idratazione (Name/SourceText) applicata SOLO ai risultati FINALI
   (dopo il troncamento a `top_k`), non al candidate set grezzo
   pre-troncamento**: non esplicitamente richiesto dalla SPEC in questi
   termini, ma coerente col suo stesso principio "una query batch, non N" —
   idratare meno nodi (quelli scartati dal troncamento non hanno comunque
   bisogno di `Name`/`SourceText`) è strettamente più efficiente e non
   cambia alcun comportamento osservabile (l'idratazione è puramente
   informativa, non influenza fusione/ranking).

3. **Separatore di concatenazione dei chunk: `"\n"` (newline)** — SPEC-045
   §2/§3 scenario 4 specifica l'ORDINE (chunk_index crescente) ma non un
   separatore esplicito. Scelta dichiarata, ragionevole per frammenti di
   codice sorgente, non desunta da nient'altro nel progetto.

4. **`size: 1000` esplicito sulla query OpenSearch batch**: il default
   OpenSearch (10 risultati) troncherebbe silenziosamente un'entità con
   molti chunk o un candidate set con molte entità — limite esplicito
   dichiarato, non presunto dal default implicito del servizio.

5. **`hydrateNames` usa `MATCH`, non `OPTIONAL MATCH`**: un `node_id` che
   non esiste affatto in Neo4j (mai osservato negli scenari — solo
   "name mai scritto per un nodo esistente" non è nemmeno un caso previsto
   dalla SPEC, dato che sink-graph scrive sempre `name`) semplicemente non
   produce una riga — `Name` resta `""` per quel nodo, nessun errore.
   Coerente con l'edge case §4 riga 2, che riguarda fallimenti di
   CONNESSIONE, non l'assenza del dato per un nodo specifico.

6. **Due file di test di integrazione, non uno solo**: `content_integration_test.go`
   (black-box, package `hybridsearch_test`, Neo4j+Qdrant+OpenSearch+
   embedder-fake reali insieme — il test primario, copre tutti gli scenari
   1-5 via l'API pubblica) e `hydrate_integration_test.go` (white-box,
   package `hybridsearch`, SOLO Neo4j — isola l'edge case §4 riga 2 dal
   fallimento più generico di `GraphTraversal`, che condivide lo stesso
   driver e quindi non permetterebbe di distinguere "la traversata grafo è
   fallita" da "l'hydration del nome è fallita" con un driver interamente
   rotto). Nessuna violazione di "solo test di integrazione, real infra" —
   entrambi i file usano Neo4j reale via testcontainers, mai un mock.

7. **`codeChunksIndex = "code_chunks"` ridichiarato localmente in
   `hybridsearch`**: `services/sink-search/internal` non è importabile da
   un altro modulo Go — stesso principio già accettato per `embedclient`
   (T4.1) e per la query BFS duplicata tra `hybridsearch`/
   `impactanalysis`/`tools/gds-impact` (T4.1/T4.2/T4.3).
