# SPEC-038 — Plugin Neo4j per la riconciliazione (T3.4, parte 2/4)
Stato: implemented
Task-tree: T3.4 (secondo dei quattro sotto-task concordati — Qdrant/OpenSearch sono le parti 3/4-4/4) · Nuovo: tools/reconcile/internal/neo4jtarget (Go) · ADD: Modulo 1 §2.2

## 1. Obiettivo
Implementare `framework.Target` (SPEC-037) per Neo4j: confronta `code_node.ast_hash` (Postgres, fonte di verità) con la proprietà `ast_hash` del nodo corrispondente in Neo4j, ripubblica come evento `CodeNode` in `outbox` ogni entità mancante o divergente — riattivando lo stesso percorso CDC→sink-graph→Neo4j (T1.3) che l'avrebbe scritta correttamente la prima volta.

## 2. Interfaccia

**Package** `tools/reconcile/internal/neo4jtarget`:
```go
func New(neo4jDriver neo4j.DriverWithContext, db *sql.DB) framework.Target
```
**Driver Neo4j**: lo STESSO già in uso in `sink-graph`/`retrieval-engine` (T1.3/T1.4) — nessuna nuova libreria, nessuna nuova decisione di connessione. Verificare in implementazione l'esatto import/versione da quei due servizi, non ripartire da zero.

**`SourceRows`**: `SELECT id, ast_hash FROM code_node` — un `SourceRow` per riga, `Fingerprint` = `[]byte(ast_hash)`.

**`Check`**: `MATCH (n {id: $id}) RETURN n.ast_hash AS ast_hash` — nodo assente → `matches=false`; nodo presente con `ast_hash` divergente → `matches=false`; nodo presente con `ast_hash` combaciante → `matches=true`.

**`Republish`**: dato un `SourceRow` (solo id+fingerprint), interroga di nuovo Postgres per la riga COMPLETA di `code_node` (`domain`, `node_type`, `name`, `ast_hash`, `file_path`) — `Republish` riceve solo id+fingerprint dal framework, non l'intera riga — poi inserisce in `outbox` (`aggregate_type='CodeNode'`) un payload nella STESSA forma esatta che `persist_parsed_file` (T1.2) avrebbe scritto per quella riga, dentro la stessa transazione già aperta dal framework (SPEC-037 §2).

## 3. Comportamento (scenari)

1. **Dato** un `code_node` il cui nodo Neo4j esiste con lo stesso `ast_hash`, **quando** eseguo la riconciliazione, **allora** `Check` ritorna `true`, nessuna ripubblicazione.
2. **Dato** un `code_node` il cui nodo Neo4j è del tutto assente (simulando un evento perso), **quando** eseguo la riconciliazione, **allora** `Check` ritorna `false`, `Republish` scrive una riga `outbox` corretta.
3. **Dato** lo scenario 2, **quando** lascio che il connector Debezium/Kafka reale instradi quella riga verso `sink-graph` (T1.3) come farebbe per qualunque scrittura normale, **allora** il nodo Neo4j mancante viene creato correttamente — prova diretta che la ripubblicazione chiude davvero il cerchio, non solo che scrive una riga in una tabella.
4. **Dato** un `code_node` il cui nodo Neo4j esiste ma con `ast_hash` DIVERGENTE (simulando drift), **quando** eseguo la riconciliazione, **allora** stesso comportamento di rilevamento+ripubblicazione dello scenario 2.
5. **Dato** il payload ripubblicato dallo scenario 2, **quando** lo confronto con la forma prodotta da `persist_parsed_file` (T1.2) per la STESSA riga originale, **allora** sono strutturalmente equivalenti — verifica diretta che la ricostruzione non introduca una forma diversa che romperebbe il parsing di `sink-graph`.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Neo4j irraggiungibile durante `Check` | Errore propagato a `Check` (non un `matches=false` silenzioso) — il framework (SPEC-037) lo mette in `Errored`, non lo tratta come mismatch reale |
| La riga completa non si trova più in Postgres al momento di `Republish` (rara race: l'entità è stata cancellata tra `SourceRows` e `Republish`) | Errore esplicito da `Republish`, mai un payload parziale/fabbricato |

## 5. Non-goals
Nessuna gestione di nodi Neo4j orfani (presenti in Neo4j ma assenti da Postgres — direzione opposta di riconciliazione, fuori scope qui). Nessuna modifica al framework di SPEC-037.

## 6. Vincoli dall'ADD
Modulo 1 §2.2 — questa SPEC ne è la prima applicazione concreta.

## 7. Test plan
Integrazione con Postgres+Neo4j reali (testcontainers) per gli scenari 1/2/4/5; scenario 3 richiede ANCHE Kafka+Debezium reali (stack più pesante, stesso principio già accettato altrove in questo progetto quando la prova end-to-end lo giustifica — T1.6, T2.5).

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta, in particolare lo scenario 3 (ciclo completo, non solo la riga outbox).
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Driver Neo4j riusato da sink-graph/retrieval-engine, non reintrodotto — verificato esplicitamente, non presunto.

## 10. Deviazioni

1. **Driver Neo4j verificato, non presunto (§2/§9).** `services/sink-graph/go.mod` e `services/retrieval-engine/go.mod` dichiarano entrambi esattamente `github.com/neo4j/neo4j-go-driver/v5 v5.28.4`, import `github.com/neo4j/neo4j-go-driver/v5/neo4j` — stessa dipendenza aggiunta qui, stessa versione, nessuna nuova decisione di connessione.

2. **`New(neo4jDriver, db)`: `db` accettato ma non referenziato dal corpo del pacchetto.** `SourceRows` riceve la propria `*sql.DB` direttamente dal framework ad ogni chiamata (stesso oggetto che `framework.OpenAndRun` apre da `cfg.DSN`), e `Republish` opera interamente sulla `*sql.Tx` già aperta dal framework per quella riga (SPEC-037 §2) — nessuno dei due chiude su `db`. Il parametro resta nella firma di `New` per rispettare l'interfaccia dichiarata da §2 (verosimilmente condivisa con i plugin Qdrant/OpenSearch, parti 3/4-4/4), ma `tools/reconcile/main.go` lo passa `nil` esplicitamente invece di aprire una seconda connessione Postgres solo per riempirlo — annotato nel codice con un commento che spiega il perché, non un dettaglio silenzioso.

3. **`Republish`: "riga completa di `code_node`" interpretata come `domain, node_type, name, ast_hash, provenance->>'path' AS file_path`.** `code_node` non ha una colonna `file_path` scalare — il percorso vive dentro `provenance` (JSONB, stessa forma `{"path": ...}` scritta da `persist_parsed_file`, T1.2). La query estrae `file_path` da lì (`provenance->>'path'`) e il payload ricostruisce `"provenance": {"path": file_path}` — stessa struttura finale, non una nuova forma.

4. **Payload ripubblicato: `"language": "go"` replicato letteralmente, non "corretto".** `persist_parsed_file` (`services/ingestion/src/persist.rs`) scrive oggi `"ext": {"node_type": ..., "language": "go"}` con `"go"` come stringa letterale hardcoded (non `node.language`) — comportamento reale del sistema al momento di questa SPEC (T1.1 estrae solo Go). §2 chiede la forma ESATTA prodotta da `persist_parsed_file` per quella riga: replicata fedelmente, inclusa questa scelta preesistente, non un difetto introdotto o corretto da questa SPEC.

5. **Scenario 5 verificato per confronto strutturale diretto, non contro un'esecuzione reale di `persist_parsed_file` (Rust).** §3 scenario 5 chiede il confronto "con la forma prodotta da `persist_parsed_file` per la STESSA riga originale" — nessuna dipendenza da un ambiente Rust è richiesta altrove da questa SPEC (§7 elenca solo Postgres+Neo4j reali per gli scenari 1/2/4/5). Verificato invece costruendo in Go, nel test, la forma letterale che `persist.rs` produce per gli stessi campi (stesso codice sorgente di `persist.rs` letto e replicato riga per riga nel commento del test) e confrontandola per uguaglianza strutturale (decodifica JSON in `map[string]any`, `reflect.DeepEqual`) con il payload realmente scritto da `Republish` — non un'invocazione del binario Rust.

6. **Ambiente di test: `ast_hash` di test sempre a 64 caratteri esadecimali (`sha256` del seed), non stringhe corte arbitrarie.** `code_node.ast_hash` è `CHAR(64)` (non `VARCHAR`, SPEC-005): un valore più corto di 64 caratteri viene restituito PADDATO con spazi da Postgres alla rilettura — invisibile in produzione (`ast_hash` reale è sempre un digest SHA-256 completo) ma scoperto scrivendo il test stesso (scenari 1 e 5 fallivano per un confronto byte-per-byte rotto dal padding, non per un difetto dell'implementazione). Fix nel solo codice di test (`hash64`), nessun impatto su `neo4jtarget.go`.

7. **Scenario 3 (test): i topic Kafka vanno creati esplicitamente PRIMA che `sink-graph` formi il proprio consumer group.** Scoperto scrivendo il test: un consumer group Kafka con `GroupTopics` che si unisce prima che il topic esista riceve zero partizioni in quell'assegnazione iniziale e non le riscopre da solo quando Debezium lo crea più tardi (auto-create lazy al primo evento CDC) — nessun rebalance automatico lo attiva. Stesso principio già applicato da `services/sink-graph/internal/consumer/consumer_integration_test.go` (SPEC-015 §10, `ensureTopics` prima di qualunque consumer/producer); replicato qui nell'harness dello scenario 3. Nessun impatto sul codice di produzione (`neo4jtarget.go`/`main.go`): `sink-graph` in produzione parte una volta e i topic esistono già dalla prima esecuzione di `deploy/compose`.

8. **`tools/reconcile` resta non cablato in `Taskfile.yml`.** Stato preesistente da SPEC-037 §10 punto 4 (non causato da questa SPEC) — `Taskfile.yml` non è nei file toccabili di questa SPEC. Test eseguiti manualmente: `go test -tags=integration ./internal/neo4jtarget/...` (scenari 1/2/4/5 + edge case, ~30s) e `go test -tags=integration ./internal/neo4jtarget/... -run TestNeo4jTargetScenario3EndToEndDebeziumSinkGraph` (scenario 3, ~80s, richiede Docker + `migrate` CLI + toolchain Go per compilare `sink-graph` come sottoprocesso) — entrambi verdi, eseguiti ripetutamente per escludere flakiness.

Nessun'altra deviazione rispetto al testo di §1-§9.
