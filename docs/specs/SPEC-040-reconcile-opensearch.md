# SPEC-040 — Plugin OpenSearch per la riconciliazione (T3.4, parte 4/4)
Stato: implemented
Task-tree: T3.4 (ultimo dei quattro sotto-task concordati — chiude T3.4 e Fase 3) · Nuovo: tools/reconcile/internal/opensearchtarget (Go) · ADD: Modulo 1 §2.2

## 1. Obiettivo
Implementare `framework.Target` (SPEC-037) per OpenSearch: confronta `code_chunk.text` (Postgres, fonte di verità) con il campo `text` del documento corrispondente in OpenSearch, ripubblica come evento `CodeChunk` in `outbox` ogni entità mancante o divergente — riattivando lo stesso percorso CDC→sink-search→OpenSearch (T3.2) che l'avrebbe scritta correttamente la prima volta. Ultima delle quattro SPEC concordate per T3.4 — con questa, Fase 3 è completa.

## 2. Interfaccia

**Package** `tools/reconcile/internal/opensearchtarget`:
```go
func New(client *opensearchapi.Client, db *sql.DB) framework.Target
```
**Client OpenSearch**: lo STESSO già in uso in `sink-search` (T3.2, SPEC-034) — `github.com/opensearch-project/opensearch-go/v4` (path `/v4` esplicito — verificato in SPEC-034 che `@latest` su un path senza suffisso major risolve silenziosamente alla v1 per la regola Go sul semantic import versioning), forma `opensearchapi.NewClient(Config{...})` con metodi namespaced (`client.Document.X`).

**`Fingerprint` per OpenSearch, scelta dichiarata**: confronto DIRETTO del testo (non un hash) — coerente con lo stile già stabilito da Neo4j (un solo segnale primario: `ast_hash`) e Qdrant (un solo segnale primario: `node_id`, non il vettore) — qui il segnale primario è `text` stesso, dato che OpenSearch (a differenza di Qdrant) non trasforma il valore memorizzato: il testo letto è byte-per-byte quello scritto, nessuna normalizzazione server-side da aggirare.

**`SourceRows`**: `SELECT id, text FROM code_chunk` — `Fingerprint` = `[]byte(text)`.

**`Check`**: `client.Document.Get(ctx, DocumentGetReq{Index: "code_chunks", DocumentID: row.ID})` (DocumentID = `row.ID` DIRETTAMENTE — nessuna derivazione necessaria, a differenza di Qdrant/SPEC-039); documento assente (404, stesso pattern di gestione già stabilito in SPEC-034 — ispezionare `StatusCode`, non fidarsi del solo `error`) → `matches=false`; presente con `text` divergente → `matches=false`; combaciante → `matches=true`.

**`Republish`**: dato un `SourceRow`, interroga di nuovo Postgres (dentro la stessa transazione del framework) per la riga completa di `code_chunk` (`entity_id`, `chunk_index`, `char_count`) + `provenance` dal `code_node` referenziato (stesso principio già stabilito da SPEC-038/039: JOIN esplicito, `code_chunk` non ha una colonna `provenance` propria), inserisce in `outbox` (`aggregate_type='CodeChunk'`) un payload nella STESSA forma esatta che `persist_parsed_file` (T1.2, SPEC-029/032) avrebbe scritto.

## 3. Comportamento (scenari)

1. **Dato** un `code_chunk` il cui documento OpenSearch esiste con `text` combaciante, **quando** eseguo la riconciliazione, **allora** `Check` ritorna `true`, nessuna ripubblicazione.
2. **Dato** un `code_chunk` il cui documento OpenSearch è del tutto assente, **quando** eseguo la riconciliazione, **allora** `Check` ritorna `false`, `Republish` scrive una riga `outbox` corretta.
3. **Dato** lo scenario 2, **quando** lascio che il connector Debezium/Kafka reale instradi quella riga verso `sink-search` (T3.2) come farebbe per qualunque scrittura normale, **allora** il documento OpenSearch mancante viene creato correttamente — stesso principio degli scenari 3 di SPEC-038/039 (ciclo end-to-end reale), qui puntato su `sink-search`.
4. **Dato** un `code_chunk` il cui documento OpenSearch esiste ma con `text` DIVERGENTE, **quando** eseguo la riconciliazione, **allora** stesso comportamento di rilevamento+ripubblicazione dello scenario 2.
5. **Dato** il payload ripubblicato dallo scenario 2, **quando** lo confronto con la forma prodotta da `persist_parsed_file` per la STESSA riga originale, **allora** sono strutturalmente equivalenti.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| OpenSearch irraggiungibile durante `Check` | Errore propagato (non un `matches=false` silenzioso) — il framework lo mette in `Errored` |
| La riga completa non si trova più in Postgres al momento di `Republish` | Errore esplicito, mai un payload parziale/fabbricato |

## 5. Non-goals
Nessuna gestione di documenti OpenSearch orfani. Nessuna modifica al framework di SPEC-037 né ai plugin Neo4j/Qdrant di SPEC-038/039.

## 6. Vincoli dall'ADD
Modulo 1 §2.2 — chiude l'applicazione a tutte e tre le gambe dello storage ibrido.

## 7. Test plan
Integrazione con Postgres+OpenSearch reali (testcontainers) per gli scenari 1/2/4/5; scenario 3 richiede ANCHE Kafka+Debezium+sink-search reali (stesso harness pesante già costruito per SPEC-038/039 §3, adattato al target).

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta, in particolare lo scenario 3 (ciclo completo).
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Client OpenSearch riusato da sink-search (path /v4 corretto), non reintrodotto indipendentemente — verificato esplicitamente, non presunto.

## 10. Deviazioni

1. **Client OpenSearch verificato, non presunto (§2/§9).** Letti
   `services/sink-search/go.mod` (`github.com/opensearch-project/opensearch-go/v4
   v4.7.3`) e `services/sink-search/main.go`/`internal/consumer/consumer.go`
   prima di scrivere codice: path `/v4` esplicito confermato, client
   costruito con `opensearchapi.NewClient(opensearchapi.Config{Client:
   opensearch.Config{Addresses: [...]}})`, metodi namespaced
   (`client.Document.Get`/`client.Indices.Exists`/`client.Index`) — stessa
   identica libreria e forma riusata qui, stessa versione, nessuna nuova
   decisione. Il pacchetto stesso (`opensearch-go/v4`) è una libreria
   pubblica, quindi importabile direttamente (a differenza di
   `consumer.IndexName`, vedi punto 2).

2. **`indexName = "code_chunks"` replicato identico, non riusabile
   direttamente — stesso motivo già dichiarato da qdranttarget
   (SPEC-039 §10 punto 1).** `consumer.IndexName` vive sotto
   `services/sink-search/internal/`, un modulo Go SEPARATO
   (`github.com/eci-project/eci/services/sink-search`, proprio `go.mod`)
   da `tools/reconcile` — nessun `go.work`/`replace` tra i due, e comunque
   le regole di visibilità `internal/` bloccherebbero l'import a
   prescindere. Replicata identica in `opensearchtarget.go`
   (`const indexName = "code_chunks"`).

3. **`Check`: distinzione "404 reale" vs "irraggiungibile" verificata
   leggendo il sorgente del client, non presunta.** `Document.Get`
   (a differenza di `Document.Exists`/`Indices.Exists`, richieste HEAD
   senza corpo) passa un target di decodifica non-nil a `do[T]`
   (`opensearchapi/opensearchapi.go`): per QUALUNQUE `resp.IsError()`
   (status >= 400, 404 incluso) l'helper generico ritorna un errore
   non-nil SENZA MAI decodificare il corpo nel `*DocumentGetResp` — quindi
   `resp.Found`/`resp.Source` restano ai valori zero su un 404, e l'unico
   modo di distinguere "documento assente" (404, comportamento normale) da
   "OpenSearch irraggiungibile" (nessuna risposta HTTP ottenuta) è
   ispezionare `resp.Inspect().Response` (che espone il
   `*opensearch.Response`/`StatusCode` sottostante), MAI il solo `error` —
   stesso principio già stabilito da `consumer.EnsureIndex`/
   `documentExists` in sink-search (SPEC-034 §7) per `Exists`, verificato
   qui valere anche per `Get` leggendo `opensearchapi.go` riga per riga
   (`do[T]`) e `opensearch.go` (`Client.Do`: il corpo viene decodificato
   SOLO quando `!response.IsError()`). Implementato con un guard esplicito
   su `httpResp == nil` (irraggiungibile, propaga errore) prima di
   ispezionare `StatusCode` — mai un `matches=false` silenzioso.

4. **Nessuna scoperta analoga a SPEC-039 §10 punto 3 (payload
   `CodeEmbedding`/array che rompe silenziosamente
   `table.expand.json.payload`) — verificato esplicitamente, non
   presunto.** Lo scenario 3 end-to-end (Postgres+OpenSearch+Kafka+
   Kafka-Connect+sink-search reale come sottoprocesso) è passato al PRIMO
   tentativo, senza alcun intervento correttivo sul generatore di dati di
   test. Coerente con l'assenza strutturale della causa scoperta in
   SPEC-039: il payload `CodeChunk` (`{id, entity_id, chunk_index, text,
   char_count, provenance}`) non contiene NESSUN campo array — solo
   stringhe/interi/un oggetto annidato — quindi l'inferenza dello schema
   JSON di Debezium (che falliva su un array con elementi int/float
   misti nel campo `vector` di `CodeEmbedding`) non ha nulla su cui
   inciampare qui. Nessun workaround necessario nel vettore/testo di test.

5. **`New(client, db)`: `db` accettato ma non referenziato dal corpo del
   pacchetto.** Stessa deviazione già dichiarata da
   `neo4jtarget.New`/`qdranttarget.New` (SPEC-038 §10 punto 2 / SPEC-039
   §10 punto 4): `SourceRows` riceve la propria `*sql.DB` dal framework ad
   ogni chiamata, `Republish` opera sulla `*sql.Tx` già aperta dal
   framework. Il parametro resta per parità con l'interfaccia dichiarata
   da §2 (e con gli altri due plugin); `tools/reconcile/main.go` lo passa
   `nil` esplicitamente, stesso principio delle due SPEC precedenti.

6. **`code_chunk.entity_id/... + provenance` (§2) interpretato come per
   SPEC-039 §10 punto 2: JOIN esplicito su `code_node`.** `code_chunk` non
   ha una colonna `provenance` propria (verificato:
   `contracts/sql/migrations/0003_code_chunk.up.sql`) — `provenance` vive
   solo su `code_node` (`0001_init.up.sql`, JSONB NOT NULL) ed è già
   esattamente nella forma `{"path": ...}` scritta da
   `persist_parsed_file` per quell'entità: `Republish` la incorpora
   invariata (`json.RawMessage(provenance)`) nel payload, senza
   ricostruirla a mano.

7. **`tools/reconcile` resta non cablato in `Taskfile.yml`.** Stato
   preesistente da SPEC-037/038/039 (non causato da questa SPEC) —
   `Taskfile.yml` non è nei file toccabili di questa SPEC. Test eseguiti
   manualmente: `go test -tags=integration ./internal/opensearchtarget/...
   -run TestOpenSearchTarget$` (scenari 1/2/4/5 + edge case, ~30-40s) e
   `go test -tags=integration ./internal/opensearchtarget/... -run
   TestOpenSearchTargetScenario3EndToEndDebeziumSinkSearch` (scenario 3,
   ~85-90s, richiede Docker + `migrate` CLI + toolchain Go per compilare
   `sink-search` come sottoprocesso) — entrambi verdi al primo tentativo
   dopo l'implementazione reale; nessuna regressione sulla suite esistente
   (`go test -tags=integration ./...` da `tools/reconcile`, inclusi
   `neo4jtarget`/`qdranttarget`, verde).

Nessun'altra deviazione rispetto al testo di §1-§9.
