# SPEC-015 — sink-graph v0: consumer Kafka + MERGE idempotente Neo4j (T1.3)
Stato: verified
Task-tree: T1.3 (terzo task di Fase 1) · Servizio: services/sink-graph (Go, finora solo scaffold vuoto) · ADD: Modulo 1 §2.2 (CDC), Modulo 1 §1.2-1.3 (CPG, schema D3)
Contratti: deploy/compose/debezium-outbox-connector.json (addendum), contracts/cypher/schema.cypher (D3, letto — vincoli reali verificati, non modificato)

## 1. Obiettivo
Popolare `services/sink-graph` con un consumer Kafka che legge da `outbox.event.CodeNode` e `outbox.event.CodeRelation`, fa `MERGE` idempotente su Neo4j rispettando i vincoli reali di D3 (`code_node_id`, `file_path_unique`, `method_symbol_unique`), e deduplica via `processed_events`. Chiude per la prima volta l'intero percorso end-to-end con dati applicativi reali: parsing (T1.1) → Postgres+outbox (T1.2) → Kafka (già provato in SPEC-008 con dati sintetici) → Neo4j.

## 2. Interfaccia

**Addendum al connector condiviso** (`deploy/compose/debezium-outbox-connector.json`, SPEC-007, già esteso una volta in SPEC-011): il campo `id` della riga outbox non arriva al consumer solo impostando `table.field.event.id` — verificato contro esempi reali di configurazione Debezium che serve comunque un'entry esplicita in `table.fields.additional.placement`. Valore aggiornato (si aggiunge, non sostituisce quello di SPEC-011):
```json
"transforms.outbox.table.fields.additional.placement": "trace_id:header:trace_id,id:header:event_id"
```

**Consumer Go** (`libs/go/eci`: `observability`, `config`, `kafkatrace` di SPEC-011 riusati direttamente): libreria `segmentio/kafka-go`, consumer group `sink-graph`, sottoscritto a entrambi i topic.

Per ogni messaggio:
1. Estrarre `event_id` (header, UUID) e `trace_id` (header, via `kafkatrace.TraceIDFromHeaders` già esistente).
2. Verifica il marker `(event_id, consumer_name)`; se esiste il messaggio è già
   stato completato **da questo consumer** e il MERGE viene saltato. ADR-0021
   definisce lo scope per consumer.
3. Se nuovo: parse del payload JSON (stessa forma prodotta da `persist.rs`,
   T1.2) e MERGE idempotente su Neo4j secondo il topic di origine.
4. Solo dopo il MERGE riuscito inserisce il marker con `INSERT ... ON CONFLICT
   DO NOTHING RETURNING event_id`. ADR-0022 sostituisce esplicitamente
   l'ordinamento storico marker-prima-del-write, che perdeva la
   materializzazione quando Neo4j falliva prima del retry.

**MERGE per `CodeNode`** (topic `outbox.event.CodeNode`): l'etichetta specifica (`node_type` da `payload.ext.node_type`) va validata contro un enum whitelist (`File`, `Class`, `Interface`, `Method`, `Function`) **prima** di essere interpolata nella stringa Cypher — Neo4j non supporta label parametrizzate, ma un valore validato contro un enum noto non è mai un rischio di injection.
```cypher
MERGE (n:CodeNode {id: $id})
SET n:{Label}, n.domain = $domain, n.name = $name, n.ast_hash = $ast_hash,
    n.repo = $repo, n.path = $path
```
```cypher
// SOLO per node_type = "Method" (constraint method_symbol_unique):
SET n.symbol_id = $id
```
```cypher
// SOLO per node_type = "File" (constraint file_path_unique, richiede una SECONDA etichetta :File oltre a :CodeNode):
SET n:File, n.repo = $repo, n.path = $path
```

**Due vincoli reali verificati contro `schema.cypher` che la pipeline attuale non soddisfa ancora, gestiti con semplificazioni dichiarate (non silenziose)**:
- `file_path_unique` richiede `(f.repo, f.path)` — `CodeNode` (T1.1) ha solo `file_path`, nessun concetto di repository. `repo` = valore placeholder fisso (es. `"local"`, da `eci_common::config::env_or_default`) finché la pipeline non avrà un concetto reale di repository (probabilmente insieme alla risoluzione cross-file di Fase 2).
- `method_symbol_unique` richiede `m.symbol_id` — non prodotto da T1.1. `symbol_id` = lo stesso valore di `id` (il hash deterministico già unico per costruzione) — non un vero identificatore di simbolo stabile alla LSP/SCIP, solo una soddisfazione minima del vincolo di unicità per questo stadio.

**MERGE per `CodeRelation`** (topic `outbox.event.CodeRelation`): endpoint creati/riusati con **solo l'etichetta generica `:CodeNode`** (mai una label specifica come `:Method`, dato che a questo punto del consumo non è garantito sapere il tipo reale — la garanzia arriva solo quando/se il messaggio `CodeNode` corrispondente viene processato, prima o dopo in ordine indipendente):
```cypher
MERGE (from:CodeNode {id: $from_id})
MERGE (to:CodeNode {id: $to_id})
MERGE (from)-[r:{RelType}]->(to)
SET r.weight = coalesce($weight, 1)
```
`{RelType}` (`CONTAINS` o `CALLS`) validato contro l'enum di
`code_relation.rel_type` (stessa CHECK constraint di SPEC-005) prima
dell'interpolazione — stesso principio di sicurezza della label.

Il peso del payload è già aggregato dal parser per quella relazione; il `SET`
assoluto, introdotto da ADR-0022, evita che una redelivery sommi di nuovo lo
stesso conteggio dopo un write Neo4j riuscito ma un marker PostgreSQL fallito.

## 3. Comportamento (scenari)

1. **Dato** un messaggio `CodeNode` (tipo `Method`) mai visto, **quando** il consumer lo processa, **allora** in Neo4j esiste un nodo con etichette `:CodeNode:Method`, `id`/`name`/`ast_hash`/`symbol_id` (= id) corretti, e una riga in `processed_events` con quell'`event_id`.
2. **Dato** lo stesso identico messaggio riconsegnato (redelivery at-least-once, es. riavvio del consumer prima del commit dell'offset), **quando** viene riprocessato, **allora** `processed_events` blocca la seconda scrittura (nessuna riga Neo4j duplicata, nessun errore). Se il marker è perso dopo il MERGE, il MERGE ripetuto converge e non incrementa la generation GDS perché nessuna proprietà/topologia cambia — verificabile contando i nodi e leggendo la generation prima/dopo.
3. **Dato** un messaggio `CodeRelation` (`CALLS`, `Process`→`Validate`) che arriva **prima** dei messaggi `CodeNode` corrispondenti, **quando** il consumer lo processa, **allora** vengono creati due nodi placeholder con solo `:CodeNode {id}` (nessun'altra proprietà, nessuna label specifica) e l'arco `CALLS` con `weight=1`; **quando** successivamente arrivano i messaggi `CodeNode` per `Process`/`Validate`, **allora** gli stessi nodi vengono arricchiti (label specifica aggiunta, proprietà impostate) senza duplicare i nodi (stesso `id`, `MERGE`).
4. **Dato** l'intera pipeline reale in esecuzione (stack di SPEC-006/007, `task db:migrate` applicata), **quando** eseguo `parse_file` + `persist_parsed_file` (T1.1/T1.2) su `order_service.go` e poi lascio girare sink-graph, **allora** entro pochi secondi Neo4j riflette esattamente gli stessi 4 nodi e 4 relazioni già verificati in Postgres — prima prova end-to-end reale dell'intera catena.
5. **Dato** una relazione con `rel_type`/`node_type` fuori dall'enum atteso (payload malformato o corrotto), **quando** il consumer la incontra, **allora** logga un errore esplicito e NON scrive nulla su Neo4j per quel messaggio — non un crash, non un'interpolazione Cypher con un valore non validato.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Connessione a Neo4j o Postgres persa a metà elaborazione di un messaggio | Nessun marker prima del successo esterno; il wrapper retry/DLQ gestisce la riconsegna e il MERGE idempotente rende sicura una ripetizione dopo write riuscito ma marker fallito. La generation GDS avanza solo se il grafo cambia realmente (ADR-0022) |
| `node_type`/`rel_type` fuori enum (edge case tabella, scenario 5) | Log esplicito, messaggio scartato senza scrivere Neo4j, offset comunque committato (non bloccare la coda su un messaggio permanentemente malformato — non c'è ancora una DLQ in questa SPEC, vedi §5) |
| Lo stesso `event_id` genuinamente diverso da quello già in `processed_events` ma stesso `aggregate_id` (es. un nodo aggiornato due volte, due righe outbox distinte) | Comportamento corretto per design: due `event_id` distinti, entrambi processati, il secondo `MERGE`/`SET` sovrascrive correttamente le proprietà — non un caso di dedup, un aggiornamento legittimo |

## 5. Non-goals
Nessuna Dead Letter Queue per messaggi permanentemente malformati (scartati con log, non re-instradati altrove — walking skeleton). Nessun vero concetto di `repo`/multi-repository (placeholder fisso, vedi §2). Nessun `symbol_id` semanticamente significativo (riuso di `id`, vedi §2) — la vera risoluzione di simbolo arriva con Fase 2. Nessuna gestione di eventi `DELETE` (l'outbox di T1.2 produce solo `UPSERT` — coerente con SPEC-014 §5).

## 6. Vincoli dall'ADD
Modulo 1 §2.2: consumer CDC idempotente, MERGE non INSERT — esattamente il pattern qui. Vincoli di unicità di D3 (`schema.cypher`, già applicato e verificato in SPEC-004) rispettati per costruzione, incluse le due semplificazioni dichiarate per proprietà non ancora prodotte dalla pipeline a monte.

## 7. Test plan
Test di integrazione con testcontainers: Kafka + Neo4j + Postgres (tre container, pattern analogo a SPEC-008 per l'orchestrazione multi-container, ma qui con Neo4j al posto di Kafka Connect come destinazione finale da verificare). Scenari 1/2/3/5 verificabili con un consumer istanziato direttamente nel test (non via il binario), messaggi Kafka prodotti sinteticamente con gli header giusti. Scenario 4 è la verifica manuale end-to-end contro lo stack reale, con output incollato nel report (non solo "verificato").

## 8. Osservabilità
Uso di `eci_common`... ovvero `libs/go/eci/observability`/`config` (SPEC-011) — uno span per messaggio processato, `kafkatrace` per collegare via span link il consumo al trace del produttore (T1.1/T1.2), stesso pattern già dimostrato in SPEC-011 §3 scenario di interoperabilità.

## 9. Criteri di accettazione
- [x] Scenario 1: nodo Method creato con etichette/proprietà corrette, incluse le due semplificazioni dichiarate (repo placeholder, symbol_id=id). Verificato sia in `TestSinkGraphConsumer/Scenario1_NewCodeNodeMethodMerged` (testcontainers) sia nell'evidenza end-to-end reale di §10.
- [x] Scenario 2: redelivery non duplica nulla, verificato con conteggi diretti prima/dopo (`TestSinkGraphConsumer/Scenario2_RedeliveryDoesNotDuplicate`: 1 nodo prima e dopo la riconsegna, `Outcome` distingue esplicitamente merge vs. duplicate).
- [x] Scenario 3: ordine CodeRelation-prima-di-CodeNode gestito correttamente — placeholder creato (solo `:CodeNode {id}`), poi arricchito senza duplicazione (`TestSinkGraphConsumer/Scenario3_RelationBeforeNodesThenEnriched`).
- [x] Scenario 4: pipeline end-to-end reale (T1.1→T1.2→Kafka→Neo4j) verificata con output diretto — vedi §10, comandi e output incollati per intero, non solo dichiarata.
- [x] Scenario 5: payload con enum non valido scartato con log esplicito, nessuna scrittura Neo4j, nessun crash (`TestSinkGraphConsumer/Scenario5_InvalidEnumDiscardedNoNeo4jWrite`, sia `node_type` sia `rel_type` fuori enum).
- [x] Addendum al connector verificato con evidenza diretta (header `event_id` presente sul messaggio Kafka reale, stesso stile di verifica di SPEC-011 §3 scenario 6) — vedi §10 punto 1, incluso un test di controllo che dimostra che l'header NON compare senza l'addendum.

## 10. Deviazioni rispetto alla SPEC ed evidenza

1. **Addendum al connector verificato con evidenza diretta — e una precisazione non ovvia**: applicato
   `"transforms.outbox.table.fields.additional.placement": "trace_id:header:trace_id,id:header:event_id"`
   a `deploy/compose/debezium-outbox-connector.json`, aggiornato via `PUT
   /connectors/eci-outbox-connector/config` sullo stack SPEC-006/007 già in
   esecuzione (un `POST`/`task up` non basta: il connector era già
   registrato da una sessione precedente, un 409 Conflict non aggiorna la
   config esistente — verificato con `GET .../config` prima e dopo).
   Evidenza diretta (INSERT reale in `outbox`, poi lettura headers dal
   topic Kafka reale con `kafka-console-consumer.sh --property
   print.headers=true`):
   ```
   __debezium.context.connectorLogicalName:eci|__debezium.context.taskId:0|__debezium.context.connectorName:postgresql|__debezium.context.runId:019fc8d2-f553-73ef-b0de-dd04edaf4860|id:228ce9a9-8e82-4631-9238-0449fa9989d4|trace_id:null|event_id:228ce9a9-8e82-4631-9238-0449fa9989d4||"verify-final-45650"||{"symbol_id":"verify.Final"}
   ```
   **Precisazione verificata sperimentalmente** (non ovvia dal testo di
   §2): un header chiamato `id` esiste GIÀ di default in ogni messaggio
   Debezium/EventRouter, indipendentemente da `table.fields.additional.placement`
   (comportamento nativo di `table.field.event.id`, il cui default è
   già `id` — nessuna configurazione esplicita necessaria per QUELLO).
   Rimuovendo temporaneamente l'addendum e ripetendo l'INSERT, l'header
   `id` restava presente ma `event_id` spariva completamente:
   ```
   __debezium.context.connectorLogicalName:eci|__debezium.context.taskId:0|__debezium.context.connectorName:postgresql|__debezium.context.runId:019fc8d2-4edf-7b19-bad2-fb9c1cefc02a|id:df43c72f-ba5e-419c-94a6-50cf64d98916|trace_id:null||"verify-rollback-79357"||{"symbol_id":"verify.Rollback"}
   ```
   L'addendum quindi non "fa arrivare" l'id (che arriva comunque, sotto il
   nome `id`) — crea un ALIAS esplicito con il nome `event_id`, che è
   quello che questo consumer legge (§2 punto 1). La frase di §2 "il campo
   id della riga outbox non arriva al consumer solo impostando
   table.field.event.id" resta corretta nella sostanza (senza l'addendum,
   NESSUN header chiamato `event_id` arriva), ma non nel senso letterale
   che l'id non arrivi affatto — arriva già, solo sotto un altro nome.
   Ripristinato l'addendum dopo la verifica, connector tornato a
   `connector.state=RUNNING`, task RUNNING, verificato di nuovo con un
   terzo INSERT (evidenza sopra).

2. **Tre frammenti Cypher di §2 unificati in una query per messaggio**: la
   query base (`MERGE ... SET n:{Label}, n.domain=..., n.repo=..., n.path=...`)
   e il blocco "SOLO per File" (`SET n:File, n.repo=..., n.path=...`) sono
   meccanicamente la STESSA operazione quando `node_type="File"` (`{Label}`
   già validato risolve a `File`, quindi `SET n:{Label}` già produce
   `SET n:File`): eseguirli come due query separate sarebbe stato un MERGE
   ridondante sullo stesso nodo, non un effetto diverso. Implementato come
   UNA query (`mergeCodeNodeQuery`, `internal/consumer/cypher.go`): il
   blocco base sempre, più `SET n.symbol_id = $id` in coda SOLO per
   `node_type="Method"`. Stesso risultato Cypher, una sola query invece di
   due-tre non necessariamente equivalenti se eseguite come statement
   Cypher realmente distinti.

3. **rel_type validato contro l'INTERO enum CHECK di `code_relation.rel_type`
   (SPEC-005)**, non solo `CONTAINS`/`CALLS` (i due soli valori che T1.1
   produce oggi) — lettura letterale di §2 ("stessa CHECK constraint di
   SPEC-005"), non un'estensione arbitraria: un futuro `rel_type` prodotto
   da una SPEC successiva (es. `IMPORTS`, `EXTENDS`) passa la validazione
   di questo consumer senza bisogno di modificarlo, esattamente perché
   l'enum qui rispecchia il contratto DB, non l'output attuale di T1.1.

4. **Provenienza del placeholder `repo`**: §2 lo descrive via
   `eci_common::config::env_or_default` — quello è il nome Rust (T1.1/T1.2,
   `libs/rust/eci-common`); `services/sink-graph` è Go, quindi
   l'equivalente reale usato è `libs/go/eci/config.EnvOrDefault`
   (`SINK_GRAPH_REPO_PLACEHOLDER`, default `"local"`) — stesso pattern,
   libreria del linguaggio corretto per questo servizio. Correzione minore
   del testo, non un cambio di comportamento.

5. **`CodeRelation` payload: struct locale, non in `libs/go/eci/models`**:
   quel pacchetto (SPEC-003) ha `CodeNode`/`Provenance`/`Versioning`/
   `CodeExtension` ma nessun `CodeRelation` — aggiunto qui come
   `codeRelationPayload` privato di `internal/consumer`, non nel pacchetto
   condiviso (fuori dal perimetro file di questa sessione: `libs/go` non è
   tra i file toccabili). `models.CodeNode`/`ParseExt` sono invece riusati
   direttamente per `CodeNode` (già lì, nessuna estensione necessaria).

6. **Bug reale trovato scrivendo il test di integrazione (non nel test,
   nell'implementazione)**: `codeRelationPayload.Weight` era inizialmente
   `*float64`. `coalesce($weight, 1)` con `$weight` legato come float
   produceva un risultato Neo4j `float64` anche quando il valore
   effettivo era un conteggio intero (`persist.rs` produce sempre
   `Option<u32>`, mai un decimale) — scoperto da
   `TestSinkGraphConsumer/Scenario3...` che falliva su
   `neo4j.GetRecordValue[int64](record, "w")` con "expected int64 but
   found float64". Corretto il tipo in `*int64` (`internal/consumer/consumer.go`):
   esattamente il ciclo rosso→verde della TDD richiesta da questa SPEC,
   non solo teorico.

7. **Test di integrazione: Kafka isolato PER SCENARIO, non un broker
   condiviso** — scoperto un secondo bug reale durante la scrittura del
   test stesso: un consumer group Kafka nuovo (`StartOffset` di default =
   `FirstOffset`, verificato contro il sorgente `segmentio/kafka-go`)
   rilegge un topic dall'inizio, non solo i messaggi prodotti dopo la sua
   creazione. Con un singolo broker/topic condiviso tra scenari,
   `Scenario2` riceveva come "prima consegna" il messaggio già consumato
   da `Scenario1` (`outcome1 = OutcomeDuplicate` invece di `OutcomeMerged`
   — dedup che scattava sull'evento sbagliato). Fix: ogni scenario di
   primo livello avvia il proprio container `confluentinc/confluent-local`
   dedicato (Neo4j/Postgres restano condivisi, nessun problema di replay
   lì); ALL'INTERNO di uno scenario con più messaggi in sequenza sullo
   stesso topic (Scenario 3), un solo reader/group viene riusato per tutti
   i fetch invece di crearne uno nuovo per ciascuno, per lo stesso motivo.

8. **Immagine Kafka nei test: `confluentinc/confluent-local:7.5.0`, non
   `apache/kafka:latest`** (usato altrove nel repo, SPEC-006/007/008): il
   modulo ufficiale `testcontainers-go/modules/kafka` risolve
   l'`advertised.listeners` con la porta host dinamica tramite uno starter
   script iniettato dopo l'avvio del container (necessario perché qui,
   diversamente da SPEC-008, un consumer/produttore kafka-go REALE deve
   connettersi dal processo host, non via `Exec` dentro al container) —
   quel meccanismo è specifico delle immagini Confluent (script
   `/etc/confluent/docker/*`), non applicabile a `apache/kafka:latest`.
   Costruire manualmente l'equivalente per `apache/kafka:latest` era
   possibile ma più rischioso da verificare correttamente nel tempo a
   disposizione; il modulo ufficiale è una soluzione diretta e già
   testata dagli stessi manutentori di `testcontainers-go`.

9. **`confluentinc/confluent-local` non crea topic automaticamente**: a
   differenza di `apache/kafka:latest` (dove l'EventRouter Debezium crea i
   topic lazy alla prima produzione, SPEC-008 §4), qui
   `AllowAutoTopicCreation: true` sul `Writer` non bastava
   ("Unknown Topic Or Partition") — topic creati esplicitamente prima di
   produrre (`ensureTopics`, dial del controller + `CreateTopics`).

10. **`go mod tidy` di questa toolchain (go1.26.5) non supporta più il
    flag `-tags`**: usato `GOFLAGS="-mod=mod -tags=integration" go mod
    tidy` per includere `consumer_integration_test.go` nell'analisi delle
    dipendenze (altrimenti `testcontainers-modules/{kafka,neo4j,postgres}`
    sarebbero stati rimossi da `go.mod` come "non usati", visibili solo
    sotto quel build tag).

11. **Test di integrazione gated col build tag Go nativo `integration`**
    (non un'invenzione di questa SPEC: stesso meccanismo già usato da
    `tests/integration/postgres_ddl`/`outbox_cdc`, SPEC-005/008) — esclusi
    da `task test`/`go test ./...` di default, eseguiti con
    `go test -tags=integration ./...`. **Non wired in `Taskfile.yml`**: il
    perimetro file di questa sessione è `services/sink-graph`,
    `deploy/compose/debezium-outbox-connector.json` e questa SPEC —
    `Taskfile.yml` è fuori scope (stesso precedente di SPEC-008 §10
    deviazione #5 e SPEC-014, poi corretto in una sessione successiva
    esplicitamente dedicata a quello).

### Evidenza scenario 4 — pipeline end-to-end reale

Stack SPEC-006/007 già in esecuzione (`docker compose ps`: tutti healthy),
`task db:migrate` (Postgres, no-op: già applicata) e `task db:neo4j:migrate`
(Neo4j, applicata per questa verifica: il Neo4j dev era vuoto di schema)
eseguite prima. `sink-graph` avviato via `go run .` (brokers/DSN/URI di
default, tutti allineati allo stack compose) contro Postgres/Neo4j/Kafka
reali — non testcontainers.

`cargo run` di `services/ingestion` (parse_file + persist_parsed_file,
T1.1/T1.2) su `order_service.go` contro lo stesso Postgres:
```
parse_file("../../tests/fixtures/sample-repo/order_service.go"): 4 CodeNode, 4 CodeRelation
persist_parsed_file("../../tests/fixtures/sample-repo/order_service.go"): 4 nodi upsert, 4 relazioni sostituite, 8 righe outbox
```
(4 relazioni, non 1: stessa correzione già annotata in SPEC-014 §10 — 3
CONTAINS + 1 CALLS.)

Dopo pochi secondi (Debezium → Kafka → sink-graph), query dirette su Neo4j
(`cypher-shell`):
```
neo4j> MATCH (n:CodeNode) RETURN count(n) AS code_nodes;
code_nodes
4

neo4j> MATCH ()-[r]->() WHERE type(r) IN ['CONTAINS','CALLS'] RETURN count(r) AS code_relations;
code_relations
4

neo4j> MATCH (n:CodeNode) RETURN labels(n) AS labels, n.name AS name, n.repo AS repo, n.path AS path, n.symbol_id AS symbol_id ORDER BY name;
labels, name, repo, path, symbol_id
["CodeNode", "File"], "../../tests/fixtures/sample-repo/order_service.go", "local", "../../tests/fixtures/sample-repo/order_service.go", NULL
["CodeNode", "Class"], "OrderService", "local", "../../tests/fixtures/sample-repo/order_service.go", NULL
["CodeNode", "Method"], "Process", "local", "../../tests/fixtures/sample-repo/order_service.go", "1ca7ef6211d1bca2dc59f4adf29a5f9a1dadfd9f1e31c77cb5c8a442c07613f2"
["CodeNode", "Method"], "Validate", "local", "../../tests/fixtures/sample-repo/order_service.go", "e810275b5dc18d836d9e1684edffffe9419a2889bb7eff0b160ff367e70342ca"

neo4j> MATCH (a:CodeNode)-[r]->(b:CodeNode) RETURN a.name AS from_name, type(r) AS rel_type, b.name AS to_name, r.weight AS weight ORDER BY rel_type, from_name, to_name;
from_name, rel_type, to_name, weight
"Process", "CALLS", "Validate", 1
"../../tests/fixtures/sample-repo/order_service.go", "CONTAINS", "OrderService", 1
"../../tests/fixtures/sample-repo/order_service.go", "CONTAINS", "Process", 1
"../../tests/fixtures/sample-repo/order_service.go", "CONTAINS", "Validate", 1
```
Esattamente gli stessi 4 nodi e 4 relazioni già verificati in Postgres
(SPEC-014): `File`/`Class`/2×`Method`, 1 `CALLS` (Process→Validate,
weight=1) + 3 `CONTAINS` (File→ciascuno, weight=1 di default per tutte —
`coalesce($weight,1)` si applica uniformemente, stesso pattern D3 di
riferimento, non solo alle `CALLS`). `symbol_id` popolato SOLO sui
`Method` (= id, semplificazione dichiarata §2/§5); `repo`/`path` popolati
su tutti i nodi col placeholder `"local"` e il `file_path` di T1.1.

**Nota operativa (non un difetto di sink-graph)**: il primo avvio del
consumer in questa verifica non ha processato messaggi per svariati
secondi — diagnosticato come membership residua di un tentativo precedente
nello stesso consumer group (`sink-graph`), terminato con un semplice
`kill` invece di uno shutdown pulito (nessun `LeaveGroup`): Kafka attende
la scadenza del session timeout del membro "morto" prima di permettere a
un nuovo join di procedere. Riavviato pulito, il consumer si è unito al
gruppo e ha iniziato a processare in ~3 secondi (verificato anche con uno
script Go minimale di debug con logging `kafka-go` verboso). Non richiede
alcun cambiamento a `sink-graph`: un riavvio via un processo gestito
(systemd, container orchestrator) chiama sempre `Close()`/invia SIGTERM
gestito, non un `kill -9` non pulito come nel mio script di verifica
manuale.
