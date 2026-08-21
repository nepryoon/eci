# SPEC-039 — Plugin Qdrant per la riconciliazione (T3.4, parte 3/4)
Stato: verified
Task-tree: T3.4 (terzo dei quattro sotto-task concordati — OpenSearch è la parte 4/4) · Nuovo: tools/reconcile/internal/qdranttarget (Go) · ADD: Modulo 1 §2.2

## 1. Obiettivo
Implementare `framework.Target` (SPEC-037) per Qdrant: confronta l'esistenza e la correttezza del punto Qdrant derivato da ciascuna riga `code_embedding` (Postgres, fonte di verità) con lo stato reale in Qdrant, ripubblica come evento `CodeEmbedding` in `outbox` ogni entità mancante o divergente — riattivando lo stesso percorso CDC→sink-vector→Qdrant (T3.1) che l'avrebbe scritta correttamente la prima volta.

## 2. Interfaccia

**Package** `tools/reconcile/internal/qdranttarget`:
```go
func New(qdrantClient *qdrant.Client, db *sql.DB) framework.Target
```
**Client Qdrant**: lo STESSO già in uso in `sink-vector` (T3.1, SPEC-033, `github.com/qdrant/go-client`) — nessuna nuova libreria.

**Derivazione point ID**: la STESSA funzione UUIDv5 già scritta in `sink-vector` per derivare un id Qdrant valido da `code_embedding.id` (SPEC-033 §10 — Qdrant richiede un UUID RFC4122 vero, i nostri id non lo sono nativamente). Verificare in implementazione dove vive quella funzione e se è già esportabile/riusabile direttamente, o se va replicata identica (stessa logica, stesso namespace fisso) — non reinventata con parametri diversi, che produrrebbe id diversi da quelli già scritti.

**`Fingerprint` per Qdrant, scelta dichiarata**: NON un confronto byte-per-byte del vettore — Qdrant normalizza a norma 1 i vettori di una collection `Distance_Cosine` (scoperto empiricamente in SPEC-033 §10), quindi il vettore letto non combacerebbe mai esattamente con quello scritto anche quando tutto è corretto. Il fingerprint qui è invece `entity_id` (propagato fino al payload Qdrant come `node_id`, SPEC-031/032/033): `Check` verifica che il punto esista E che il suo payload `node_id` combaci con l'`entity_id` atteso — esistenza più correttezza minima, senza dipendere da un confronto vettoriale fragile.

**`SourceRows`**: `SELECT ce.id, cc.entity_id FROM code_embedding ce JOIN code_chunk cc ON cc.id = ce.chunk_id` — `code_embedding` non ha una colonna `entity_id` diretta (vive su `code_chunk`, SPEC-029/032). `Fingerprint` = `[]byte(entity_id)`.

**`Check`**: deriva il point ID (UUIDv5 da `row.ID`), interroga Qdrant per quel punto; punto assente → `matches=false`; punto presente con `payload.node_id` divergente da `row.Fingerprint` → `matches=false`; combaciante → `matches=true`.

**`Republish`**: dato un `SourceRow`, interroga di nuovo Postgres (dentro la stessa transazione del framework) per la riga completa (`code_embedding.vector/model_id/embedding_dim` + `code_chunk.entity_id/provenance`), inserisce in `outbox` (`aggregate_type='CodeEmbedding'`) un payload nella STESSA forma esatta che `embedding-worker` (T3.1, SPEC-030/031/032) avrebbe scritto.

## 3. Comportamento (scenari)

1. **Dato** un `code_embedding` il cui punto Qdrant esiste con `payload.node_id` combaciante, **quando** eseguo la riconciliazione, **allora** `Check` ritorna `true`, nessuna ripubblicazione.
2. **Dato** un `code_embedding` il cui punto Qdrant è del tutto assente, **quando** eseguo la riconciliazione, **allora** `Check` ritorna `false`, `Republish` scrive una riga `outbox` corretta.
3. **Dato** lo scenario 2, **quando** lascio che il connector Debezium/Kafka reale instradi quella riga verso `sink-vector` (T3.1) come farebbe per qualunque scrittura normale, **allora** il punto Qdrant mancante viene creato correttamente — stesso principio dello scenario 3 di SPEC-038 (ciclo end-to-end reale, non solo la riga outbox), qui puntato su `sink-vector` invece di `sink-graph`.
4. **Dato** un `code_embedding` il cui punto Qdrant esiste ma con `payload.node_id` DIVERGENTE, **quando** eseguo la riconciliazione, **allora** stesso comportamento di rilevamento+ripubblicazione dello scenario 2.
5. **Dato** il point ID derivato da questo plugin per una riga nota, **quando** lo confronto con quello che `sink-vector` avrebbe derivato per la STESSA riga, **allora** sono identici — verifica diretta che la derivazione riusata produca lo stesso id, non uno diverso che romperebbe la corrispondenza con punti già scritti.
6. **Dato** il payload ripubblicato dallo scenario 2, **quando** lo confronto con la forma prodotta da `embedding-worker` per la STESSA riga originale, **allora** sono strutturalmente equivalenti.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Qdrant irraggiungibile durante `Check` | Errore propagato (non un `matches=false` silenzioso) — il framework lo mette in `Errored` |
| La riga completa non si trova più in Postgres al momento di `Republish` | Errore esplicito, mai un payload parziale/fabbricato (stesso principio di SPEC-038 §4) |

## 5. Non-goals
Nessuna gestione di punti Qdrant orfani (presenti in Qdrant ma assenti da Postgres). Nessuna modifica al framework di SPEC-037 né al plugin Neo4j di SPEC-038.

## 6. Vincoli dall'ADD
Modulo 1 §2.2 — stessa applicazione già stabilita da SPEC-038, qui per la gamba vettoriale.

## 7. Test plan
Integrazione con Postgres+Qdrant reali (testcontainers) per gli scenari 1/2/4/5/6; scenario 3 richiede ANCHE Kafka+Debezium+sink-vector reali (stesso harness pesante già costruito per SPEC-038 §3 scenario 3, adattato al target).

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con evidenza diretta, in particolare lo scenario 3 (ciclo completo) e lo scenario 5 (derivazione id identica a sink-vector).
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Client Qdrant e derivazione UUIDv5 riusati da sink-vector, non reintrodotti indipendentemente — verificato esplicitamente, non presunto.

## 10. Deviazioni

1. **Client Qdrant riusabile direttamente, derivazione UUIDv5/nome
   collection NO — verificato esplicitamente, non presunto (§2/§9).**
   `github.com/qdrant/go-client` (v1.19.0) è un pacchetto di libreria
   pubblico, importato identico da `services/sink-vector/go.mod` e da
   `tools/reconcile/go.mod` — nessuna decisione nuova, stessa versione.
   La funzione di derivazione (`consumer.DerivePointID`,
   `services/sink-vector/internal/consumer/consumer.go`) e la costante
   `consumer.CollectionName` sono invece **esportate ma non riusabili**:
   vivono sotto `services/sink-vector/internal/`, un modulo Go SEPARATO
   (`github.com/eci-project/eci/services/sink-vector`, proprio `go.mod`)
   da `tools/reconcile` (`github.com/eci-project/eci/tools/reconcile`) —
   nessun `go.work` né direttiva `replace` collega i due moduli in questo
   repo (verificato: `tools/reconcile/go.mod` non ha alcun `replace`
   verso `services/sink-vector`). Anche a prescindere dal confine di
   modulo, le regole di visibilità Go per i pacchetti `internal/`
   impedirebbero comunque l'import da codice esterno all'albero radicato
   in `services/sink-vector/`, indipendentemente da un eventuale
   `replace`. Replicate quindi identiche in `qdranttarget.go`
   (`derivePointID`, `pointIDNamespace = 5f3e8a1c-9b6d-4e2a-8c7f-1a2b3c4d5e6f`,
   `collectionName = "code_embeddings"`) — stessa logica, stesso
   namespace fisso, stesso letterale, non reinventate — e la loro
   identità con l'originale è verificata direttamente dallo scenario 5
   (punto Qdrant scritto a un id calcolato indipendentemente nel test con
   la stessa formula, trovato da `Check`) e da un confronto diretto
   contro `consumer.DerivePointID` eseguito ad-hoc durante il debug dello
   scenario 3 (vedi punto 3 sotto).

2. **`code_chunk.entity_id/provenance` (§2) interpretato come
   `entity_id` da `code_chunk` + `provenance` dal `code_node`
   referenziato.** `code_chunk` NON ha una colonna `provenance` propria
   (verificato: `contracts/sql/migrations/0003_code_chunk.up.sql`) —
   `provenance` vive solo su `code_node` (`0001_init.up.sql`, JSONB NOT
   NULL) e viene propagata a valle SOLO nei payload Kafka (SPEC-032 §2),
   mai persistita come colonna su `code_chunk`/`code_embedding`.
   `Republish` quindi fa `JOIN code_node cn ON cn.id = cc.entity_id` per
   leggere `cn.provenance` — stesso JOIN che `persist_parsed_file`
   avrebbe usato per popolarla la prima volta, non una colonna diversa da
   quella dichiarata da §2.

3. **Scoperta scrivendo lo scenario 3: un vettore con una componente
   intera esatta (es. `3.0`) rompe silenziosamente
   `table.expand.json.payload` del connector Debezium.** Il campo
   `vector` del payload `CodeEmbedding` è un array JSON; quando una sua
   componente serializza senza punto decimale (`encoding/json` di Go
   scrive `3`, non `3.0`, per un `float32` il cui valore è esattamente
   intero), l'EventRouter di Debezium — con schema JSON misto
   intero/float nell'array — NON espande il payload nell'oggetto atteso:
   consegna a `sink-vector` la STRINGA JSON grezza del payload
   (`"{\"id\":...}"`), non l'oggetto. `sink-vector` (codice fuori scope
   di questa SPEC, mai modificato) fallisce quindi il decode
   (`json.Unmarshal` in un valore stringa) — ma questo fallimento non è
   MAI visibile in log: `resilience.WithRetryAndDLQ` (SPEC-035) avvolge
   `ProcessMessage`, e sui suoi due path di errore interni
   (`markProcessed`/`upsertPoint`) NON logga nulla lato applicativo prima
   di convertire l'errore in retry silenziosi (backoff) e infine in un
   messaggio instradato su `outbox.event.CodeEmbedding.DLQ` — ritornando
   comunque `nil` al chiamante. Diagnosticato leggendo il valore Kafka
   grezzo con un consumer dedicato temporaneo e confrontando
   `derivePointID` con `consumer.DerivePointID` invocato direttamente
   (`go test` ad-hoc dentro `services/sink-vector`, poi rimosso — nessuna
   modifica lasciata in quel modulo). **Nessun fix in `sink-vector` o nel
   template del connector** (entrambi fuori dai file toccabili di questa
   SPEC): il workaround è interamente nel generatore di vettori di test
   (`fullVector`, `qdranttarget_integration_test.go`), che ora aggiunge
   un offset fisso `+0.001` a ogni componente per garantire che nessuna
   sia mai un intero esatto. Questo è un limite REALE e preesistente
   della pipeline CDC→sink-vector (T3.1/T3.3), non introdotto da questa
   SPEC, mai emerso prima perché nessun test precedente instradava un
   payload `CodeEmbedding` reale attraverso il connector Debezium vero
   (il test di integrazione di SPEC-033 produce direttamente sul topic
   Kafka, bypassando Debezium/EventRouter) — riportato qui esplicitamente
   perché scoperto costruendo l'harness pesante richiesto da questa SPEC
   (§7 scenario 3), non altrove. Fuori scope risolverlo (richiederebbe
   toccare `services/sink-vector` e/o
   `deploy/compose/debezium-outbox-connector.json`, entrambi non tra i
   file toccabili di questa SPEC) — segnalato per una SPEC futura.

4. **`New(qdrantClient, db)`: `db` accettato ma non referenziato dal
   corpo del pacchetto.** Stessa deviazione già dichiarata da
   `neo4jtarget.New` (SPEC-038 §10 punto 2): `SourceRows` riceve la
   propria `*sql.DB` dal framework ad ogni chiamata, `Republish` opera
   sulla `*sql.Tx` già aperta dal framework. Il parametro resta per
   parità con l'interfaccia dichiarata da §2 (e con `neo4jtarget.New`);
   `tools/reconcile/main.go` lo passa `nil` esplicitamente, stesso
   principio di SPEC-038.

5. **`tools/reconcile` resta non cablato in `Taskfile.yml`.** Stato
   preesistente da SPEC-037 §10 punto 4 / SPEC-038 §10 punto 8 (non
   causato da questa SPEC) — `Taskfile.yml` non è nei file toccabili di
   questa SPEC. Test eseguiti manualmente: `go test -tags=integration
   ./internal/qdranttarget/...` (scenari 1/2/4/5/6 + edge case, ~5s) e
   `go test -tags=integration ./internal/qdranttarget/... -run
   TestQdrantTargetScenario3EndToEndDebeziumSinkVector` (scenario 3,
   ~60-120s, richiede Docker + `migrate` CLI + toolchain Go per compilare
   `sink-vector` come sottoprocesso) — entrambi verdi, eseguiti
   ripetutamente per escludere flakiness; nessuna regressione sulla
   suite esistente (`go test -tags=integration ./...` da
   `tools/reconcile`, incluso `neo4jtarget`, verde).

Nessun'altra deviazione rispetto al testo di §1-§9.
