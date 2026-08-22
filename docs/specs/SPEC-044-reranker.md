# SPEC-044 — Reranker structure-aware (T4.4)
Stato: verified
Task-tree: T4.4 (dip. dichiarata T4.1, dipendenza reale ANCHE da T4.3 — entrambi già chiusi) · Servizio: services/retrieval-engine (Go, estende T4.1/SPEC-041) · ADD: Modulo 2 §2.3

## 1. Obiettivo
Applicare un reranker cross-encoder (`bge-reranker-v2-m3`, BAAI, self-hosted via TEI — prescritto esplicitamente dall'ADD come default enterprise, il sorgente non lascia il perimetro) ai risultati di `HybridGraphVectorSearch` (T4.1) DOPO la fusione RRF: candidate set di top-50/100 nodi ridotto a top-k (5-10), riordinato per `final_score = rerank_score + β * proximity_boost(node)`, dove `proximity_boost` combina la distanza in hop (T4.1) E l'`impact_score` scritto da GDS (T4.3) — non solo la prossimità topologica semplice.

## 2. Interfaccia

**Client TEI-rerank** (nuovo, `services/retrieval-engine/internal/rerankclient`): endpoint `/rerank` (distinto da `/embed`, già usato per gli embedding, T2.4/T4.1) — `POST {query, texts: [...]}` → `[{index, score}]`. Stesso principio "stesso contratto HTTP contro fake o vero" già stabilito per l'embedding client (T2.4): URL configurabile, nessun failover automatico.

**Nuovo `reranker-fake`** (`fakes/reranker-fake`, stesso pattern di `fakes/embedder-fake`/T2.4): score deterministico — non un giudizio di rilevanza reale, riproducibile per i test (es. funzione hash di `(query, text)` normalizzata in [0,1], stesso principio SHA-256-based di `embedder-fake`).

**`proximity_boost`, formula dichiarata (non specificata a questo livello di dettaglio dall'ADD, che descrive solo le due proprietà — decrescente in hop_distance, crescente in impact_score — senza una formula esatta)**:
```
proximity_boost(node) = w_hop * (1/(1+hop_distance)) + w_impact * impact_score_norm
```
Pesi di default dichiarati (`w_hop=0.5, w_impact=0.5`, nessun precedente diretto nel progetto, configurabili) — `impact_score_norm` letto direttamente dalla proprietà Neo4j scritta da T4.3 (già normalizzata min-max da quella pipeline, nessuna nuova normalizzazione qui). Un nodo per cui `impact_score` non è mai stato scritto (T4.3 mai eseguito per quel sottografo) usa `impact_score_norm=0` — nessun errore, un boost strutturale assente non è un caso eccezionale.

**Package** `services/retrieval-engine/internal/rerank`:
```go
func Rerank(ctx, client *rerankclient.Client, driver neo4j.DriverWithContext, query string, candidates []hybridsearch.RetrievedNode, topK int, beta, wHop, wImpact float64) ([]RankedNode, error)
```
1. Estrae il testo di ciascun candidato (stesso principio "solo i campi che la pipeline può popolare davvero" già stabilito in T1.4/T4.1 — testo disponibile solo se `include_source_text` era impostato a monte, altrimenti il nome/summary disponibile).
2. Chiama `/rerank` con `query` + testi dei candidati.
3. Legge `impact_score` per ciascun `node_id` da Neo4j (batch, una sola query per l'intero candidate set — non N query separate).
4. Calcola `final_score = rerank_score + beta*proximity_boost(node)`.
5. Ordina per `final_score` decrescente, tronca a `topK`.

**Integrazione**: nuovo campo `entry_node_id`-consapevole nel flusso `HybridSearch` esistente (T4.1) — dato che il reranking richiede lo STESSO `entry_node_id` per calcolare `hop_distance`/leggere `impact_score`, non un parametro indipendente. Attivato da un nuovo flag booleano su `HybridSearchRequest` (`bool enable_rerank = 13`, estensione additiva — propria ADR, stesso principio di ADR-0007/0008) — quando `false` (default), comportamento invariato di T4.1.

## 3. Comportamento (scenari)

1. **Dato** un candidate set di nodi con `rerank_score` noti dal fake e `hop_distance`/`impact_score` noti, **quando** eseguo `Rerank`, **allora** il `final_score` di ciascun nodo combacia con la formula dichiarata (calcolabile a mano, non solo "un valore è stato prodotto").
2. **Dato** un nodo con `impact_score` alto ma `rerank_score` basso e un altro con `rerank_score` alto ma `impact_score` basso, **quando** ordino per `final_score`, **allora** l'ordine riflette la combinazione, non uno dei due segnali isolatamente (verifica diretta che `beta`/i pesi abbiano un effetto osservabile sull'ordinamento finale).
3. **Dato** un candidate set più grande di `topK`, **quando** eseguo `Rerank`, **allora** il risultato è troncato esattamente a `topK` elementi, i migliori per `final_score`.
4. **Dato** un nodo il cui `impact_score` non è mai stato scritto da T4.3, **quando** eseguo `Rerank`, **allora** `impact_score_norm=0` per quel nodo, nessun errore.
5. **Dato** il servizio di reranking irraggiungibile, **quando** chiamo `HybridSearch` con `enable_rerank=true`, **allora** l'intera RPC fallisce esplicitamente — a differenza della gamba vettoriale di T4.1 (che degrada), il reranking è stato esplicitamente richiesto dal client con quel flag, quindi un suo fallimento non deve restituire silenziosamente risultati non ri-ordinati.
6. **Dato** `enable_rerank=false` (default), **quando** chiamo `HybridSearch`, **allora** comportamento invariato di T4.1, nessuna chiamata al servizio di reranking.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| `topK` <= 0 | Errore esplicito prima di chiamare il reranker |
| Candidate set vuoto (nessun risultato da `HybridGraphVectorSearch`) | `Rerank` ritorna una lista vuota, nessun errore, nessuna chiamata al servizio di reranking (niente da riordinare) |

## 5. Non-goals
Nessuna integrazione con Cohere Rerank 3.5 (opzione alternativa dell'ADD, non il default prescritto — fuori scope). Nessuna modifica a `HybridGraphVectorSearch`/GDS batch (T4.1/T4.3, entrambi invariati — questa SPEC li consuma, non li modifica). Nessun context packing "a U" (T4.5, task successivo — questa SPEC produce il `final_score` che T4.5 userà per l'ordinamento finale).

## 6. Vincoli dall'ADD
Modulo 2 §2.3 — reranker cross-encoder self-hosted come default enterprise, positioning dopo RRF, boost strutturale decrescente in hop/crescente in impact_score (formula esatta lasciata alla nostra dichiarazione, §2).

## 7. Test plan
Unitari per la formula `proximity_boost`/`final_score` (nessuna infrastruttura reale necessaria). Integrazione con Neo4j reale (testcontainers) per la lettura batch di `impact_score` + `reranker-fake` come processo reale (stesso principio già stabilito per `embedder-fake`, T2.4).

## 8. Osservabilità
Uno span per la chiamata di reranking, un evento con il numero di candidati in ingresso/usciti dopo il troncamento a `topK`.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con evidenza diretta, in particolare lo scenario 2 (l'ordinamento riflette davvero la combinazione dei due segnali): `internal/rerank/rerank_test.go`, `TestRerankCore_OrderReflectsCombinedSignalNotOneAlone` — due nodi il cui ordine per solo `rerank_score` è OPPOSTO a quello per `final_score` combinato, verificato che l'ordine finale sia quello combinato. Scenari 1/3/4 unitari puri (`rerank_test.go`) + integrazione Neo4j+reranker-fake reali (`rerank_integration_test.go`, scenari 1/4). Scenario 5 (reranker irraggiungibile -> RPC fallisce) verificato sia a livello di pacchetto sia a livello RPC reale (`internal/server/rerank_dispatch_integration_test.go`, `codes.FailedPrecondition`/errore esplicito). Scenario 6 (`enable_rerank=false`) verificato via gRPC reale: nessun `RerankScore` popolato, comportamento T4.1 invariato.
- [x] Edge case tabella §4 verificati esplicitamente: `topK<=0` e candidate set vuoto, entrambi con verifica diretta che NESSUNA chiamata (reranker né Neo4j) sia avvenuta (`TestRerankCore_TopKZeroOrNegativeFailsBeforeAnyCall`, `TestRerankCore_EmptyCandidatesReturnsEmptyNoCallsMade`).
- [x] Estensione proto additiva (`bool enable_rerank = 13`), propria ADR (`docs/decisions/ADR-0009-hybridsearchrequest-enable-rerank.md`), bindings rigenerati per ENTRAMBI i linguaggi FIN DALL'INIZIO (`bash scripts/task-proto-gen.sh` completo, diff verificato esplicitamente su `libs/go/eci/retrieval/v1/retrieval.pb.go` E `libs/py/eci_core/retrieval/v1/retrieval_pb2.py` prima di scrivere qualunque codice Go — lezione da SPEC-041/042 applicata dall'inizio, non da correggere dopo).
- [x] Nessuna regressione sui test esistenti di T1.4/T4.1/T4.2: intera suite `services/retrieval-engine` (unitari + integrazione, `-p 1` per evitare la contesa Docker già documentata in SPEC-042/043) rieseguita verde insieme ai nuovi test T4.4.

## 10. Deviazioni rispetto alla SPEC

1. **`candidateText` usa `node_id` come testo passato al reranker, non
   "nome/summary" (§2 punto 1)**: `hybridsearch.RetrievedNode` (T4.1, porto
   fedele di D5) non porta MAI `name`/`summary`/`source_text` — nessuno di
   questi campi è popolato da `HybridGraphVectorSearch` oggi (dichiarato
   esplicitamente in SPEC-041 §10 e mai cambiato da allora; questa SPEC
   §5 vieta esplicitamente di modificare T4.1). `node_id` è quindi l'UNICO
   campo testuale sempre presente sui candidati — usato come fallback
   ultimo, onesto rispetto allo stato reale della pipeline. Nessun impatto
   sulla correttezza dei test (il reranker-fake produce comunque un
   punteggio deterministico per qualunque testo, incluso un id opaco); un
   impatto reale sulla QUALITÀ del reranking in produzione (un cross-encoder
   reale su un id opaco non giudica nulla di semanticamente utile) — segnalato
   qui esplicitamente, non nascosto, in attesa di una SPEC futura che estenda
   `RetrievedNode`/T4.1 con `include_source_text`.

2. **`beta` (SPEC-044 §1, formula `final_score = rerank_score +
   β*proximity_boost`) riusa il default 0.15 già stabilito da
   `ApplyTopologicalProximity` (T4.1, SPEC-041)**: la SPEC dichiara un
   default numerico solo per `w_hop`/`w_impact` (0.5/0.5), non per `beta`
   stesso — che è però lo STESSO simbolo già usato altrove nel progetto
   (commento preesistente su `NodeScores.final_score` nel proto D7: "rerank
   + beta*proximity_boost"). Riusato lo stesso valore per coerenza, nessun
   valore alternativo dichiarato da nessuna parte per questo simbolo.

3. **`beta`/`w_hop`/`w_impact` sono parametri Go passati dal chiamante
   (server.go), non nuovi campi proto**: SPEC-044 §2 li dichiara
   "configurabili" ma non aggiunge campi a `HybridSearchRequest` oltre a
   `enable_rerank` — stessa scelta già fatta per `beta` di T4.1 (mai
   esposto via proto). `server.go` li passa come costanti dichiarate
   (`defaultRerankBeta`/`defaultRerankWHop`/`defaultRerankWImpact`).

4. **Il reranking, quando attivato, SOSTITUISCE `CombinedScore` (RRF+
   prossimità di T4.1) come `final_score`, non lo combina ulteriormente**:
   `retrievedNodeFromRankedNode` scrive `RerankScore`/`FinalScore` (da
   T4.4) sullo stesso campo `NodeScores.final_score` che T4.1 popolava con
   `CombinedScore` — coerente con l'obiettivo dichiarato in SPEC-044 §1
   ("candidate set... riordinato per final_score = rerank_score +
   β*proximity_boost"): quando il reranking è richiesto, è il segnale di
   ordinamento DEFINITIVO, non un boost aggiuntivo sopra un altro
   final_score già scritto da T4.1. `RrfScore`/`HopDistance`/
   `VectorScore`/`Provenance` restano invariati (proiettati dallo stesso
   `hybridsearch.RetrievedNode` sottostante).

5. **`RERANKER_SERVICE_URL` default `http://localhost:8003`**: nessuna
   porta di default nota nel progetto per un servizio TEI-rerank — scelta
   dichiarata (porta successiva a `EMBEDDING_SERVICE_URL`, 8002), non
   desunta da nient'altro. `reranker-fake` nei test usa sempre una porta
   assegnata dinamicamente (`freePort`/`dispatchFreePort`), mai questo
   default.
