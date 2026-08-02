# SPEC-008 — Smoke test end-to-end: INSERT outbox → evento sul topic Kafka
Stato: verified
Task-tree: T0.7 (ultimo tassello di Fase 0) · Servizio: tests/integration/outbox_cdc · ADD: Modulo 1 §2.2 (CDC & Eventual Consistency, verifica end-to-end)
Contratti: contracts/jsonschema/outbox-event.json (letto come riferimento), contracts/sql/migrations (applicate nel test — NON modificate)

## 1. Obiettivo
Dimostrare, con un harness testcontainers self-contained (Postgres + Kafka + Kafka Connect orchestrati insieme in una rete di test dedicata, indipendente dallo stack compose persistente di SPEC-006/007), che un `INSERT` reale nella tabella `outbox` produce — end-to-end, attraverso Debezium e l'EventRouter SMT — un messaggio Kafka effettivamente leggibile sul topic corretto, con chiave e payload conformi a quanto specificato nell'ADD. Chiude il task-tree originale della Fase 0 (T0.1–T0.7): tutti i pezzi costruiti finora (contratti, migration, stack CDC) vengono provati a funzionare insieme, non solo singolarmente.

## 2. Interfaccia

Nuovo pacchetto `tests/integration/outbox_cdc/` (Go), stesso pattern testcontainers già usato in `tests/integration/postgres_ddl` (SPEC-005) e in `tools/migrate-neo4j` (SPEC-004): build tag `integration`, eseguito con `go test -tags=integration ./...`, NON parte di `task test` di default.

**Orchestrazione testcontainers**, tutti e tre i container su una rete di test dedicata esplicita (non quella di default):
1. **postgres**: `postgres:17` con `command: ["postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4"]` — stessa configurazione di SPEC-007. Applicare `contracts/sql/migrations/0001_init.up.sql` (via il CLI `migrate`, stesso meccanismo di `task db:migrate`) prima di registrare il connector — la tabella `outbox` deve esistere, altrimenti Debezium logga "no changes will be captured" (comportamento già osservato e documentato in SPEC-007) e il test fallirebbe per un motivo di sequencing, non di logica.
2. **kafka**: `apache/kafka:latest`, stessa configurazione KRaft single-node di SPEC-007 (env `KAFKA_PROCESS_ROLES=broker,controller` ecc., i tre `*_REPLICATION_FACTOR`/`MIN_ISR=1` obbligatori su singolo broker).
3. **kafka-connect**: `quay.io/debezium/connect:latest` — **non** `debezium/connect` (quel repository Docker Hub non pubblica immagini da Debezium 3.0 in poi, spostato definitivamente su Quay; vedi SPEC-007 §10 deviazione #1, stessa immagine da riusare qui, non da riscoprire).

**Configurazione del connector**: riusare `deploy/compose/debezium-outbox-connector.json` come **template**, non duplicarlo — leggerlo, sostituire programmaticamente `database.hostname`/`database.port` con l'host/porta assegnati dinamicamente da testcontainers per il container Postgres di questo test (fissi `postgres`/`5432` nel file originale, validi solo nella rete del compose dev). Tutti gli altri campi (`table.include.list`, `plugin.name`, `slot.name`, `publication.name`, `transforms.outbox.*`) restano identici — è la stessa identica configurazione EventRouter di SPEC-007, solo puntata a un'istanza Postgres diversa.

**Registrazione e attesa RUNNING**: riusare la stessa logica a due fasi già implementata in `deploy/compose/register-connector.sh` (POST con retry/backoff, 409 trattato come successo, poi poll di `GET .../status` finché `connector.state=RUNNING` e almeno un task `RUNNING`) — portarla in Go come funzione condivisa o replicarne fedelmente la logica, non reinventarla da zero né saltare la fase di poll (SPEC-007 ha dimostrato empiricamente che il 201/409 da solo non basta, la convergenza richiede secondi).

**Topic Kafka atteso — verificato, non assunto**: il connector NON imposta `transforms.outbox.route.topic.replacement` esplicito (né in SPEC-007 né qui), quindi vale il default documentato da Debezium: **`outbox.event.<valore del campo route.by.field, cioè aggregate_type>`**. Con `aggregate_type="CodeNode"` il topic atteso è **`outbox.event.CodeNode`**; con `aggregate_type="CodeRelation"`, **`outbox.event.CodeRelation`** (i due soli valori validi per l'enum secondo `contracts/jsonschema/outbox-event.json` di SPEC-003). Il messaggio ha come **chiave** il valore di `aggregate_id` (per `table.field.event.key=aggregate_id`) e come **valore** il solo contenuto della colonna `payload` — EventRouter "unwrap-pa" il payload, il messaggio Kafka non è l'intera riga `outbox` né l'envelope Debezium grezzo.

## 3. Comportamento (scenari)

1. **Dato** lo stack testcontainers avviato (Postgres con `outbox` migrata, Kafka, Kafka Connect con il connector registrato e `RUNNING`), **quando** eseguo `INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, trace_id) VALUES ('<uuid>', 'CodeNode', '<aggregate-id-noto>', 'UPSERT', '{"symbol_id":"pkg.Foo#Bar()","language":"go"}'::jsonb, NULL)`, **allora** entro un timeout di 30s un consumer Kafka sottoscritto al topic `outbox.event.CodeNode` riceve esattamente un messaggio.
2. **Dato** il messaggio ricevuto, **quando** ne ispeziono la chiave, **allora** corrisponde esattamente al valore di `aggregate_id` inserito.
3. **Dato** il messaggio ricevuto, **quando** ne deserializzo il valore, **allora** corrisponde esattamente al contenuto JSON della colonna `payload` inserita — non alla riga `outbox` intera, non a un envelope Debezium con campi `before`/`after`/`source`.
4. **Dato** lo stesso test, **quando** inserisco una seconda riga con `aggregate_type='CodeRelation'` (`aggregate_id` diverso), **allora** il messaggio corrispondente appare sul topic **diverso** `outbox.event.CodeRelation` — a conferma che il routing per `aggregate_type` distingue realmente i due casi, non solo che "un messaggio arriva da qualche parte".
5. **Dato** lo stack avviato ma nessun `INSERT` ancora eseguito, **quando** interrogo brevemente entrambi i topic, **allora** non appare nessun messaggio inatteso — conferma che il connector non produce rumore/falsi positivi in assenza di eventi reali.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Il consumer Kafka nel test si sottoscrive prima che il topic esista ancora (creazione lazy alla prima produzione EventRouter) | Il test deve gestire l'attesa/poll sulla sottoscrizione, non assumere che il topic esista già al momento della `subscribe` — un semplice retry/poll con timeout, non una singola lettura secca |
| Il timeout di attesa del messaggio scade senza che nulla arrivi | Fallire con un messaggio esplicito che nomini il topic atteso e il tempo trascorso, non un timeout muto di libreria |
| Il payload JSONB inserito contiene più di una chiave/valori non banali | Usare deliberatamente un payload con almeno 2 campi nel test (non `{}` vuoto) per verificare che la (de)serializzazione JSON attraverso l'intera catena (Postgres → WAL → Debezium → EventRouter → Kafka) non corrompa né tronchi nulla |
| La migration SPEC-005 non è ancora stata applicata quando il connector viene registrato (sequencing sbagliato nel test stesso) | Il test deve applicare esplicitamente la migration PRIMA di registrare il connector — se invertito per errore, il fallimento atteso è lo stesso "no changes will be captured" già documentato in SPEC-007, non qualcosa di nuovo da diagnosticare da zero |

## 5. Non-goals
Nessun sink consumer applicativo reale (quello resta Fase 1+): questo test usa un consumer Kafka "grezzo" (client Go standard) solo per leggere e verificare il messaggio, non la logica di business che lo consumerà davvero in produzione. Nessuna verifica di comportamento su `UPDATE`/`DELETE` della tabella `outbox` (il pattern outbox è insert-only per design — non è un caso d'uso da testare). Nessuna modifica allo stack compose persistente di SPEC-006/007: questo test è completamente self-contained via testcontainers, non lo tocca né dipende da esso.

## 6. Vincoli dall'ADD
Modulo 1 §2.2.2: EventRouter con `route.by.field=aggregate_type`, `table.field.event.key=aggregate_id` — la stessa configurazione già stabilita e verificata in SPEC-007, qui provata end-to-end per la prima volta. Questa SPEC è la conferma diretta che l'architettura CDC descritta nell'ADD (outbox → Debezium → Kafka) funziona realmente dal primo all'ultimo passo, non solo che i singoli componenti esistono e sono configurati correttamente in isolamento.

## 7. Test plan
Questo documento descrive il test stesso — non esiste un piano di test "sopra" il test, dato che SPEC-008 è la verifica end-to-end. Eseguito con `go test -tags=integration ./...` nel nuovo pacchetto, sia in locale sia idealmente in CI se il tempo di esecuzione (tre container orchestrati insieme, più il ciclo register-then-poll) risulta accettabile — stessa valutazione aperta già lasciata in sospeso per SPEC-006/007, da confermare in fase di implementazione.

## 8. Osservabilità
Non applicabile in questa SPEC.

## 9. Criteri di accettazione
- [x] Scenario 1: messaggio ricevuto su `outbox.event.CodeNode` entro il timeout, dopo l'`INSERT` di un evento `CodeNode`. Topic name confermato esatto, verificato prima contro il sorgente Debezium v3.6.0.Final (stessa versione bundlata in `quay.io/debezium/connect:latest`) e poi contro il messaggio reale.
- [x] Scenario 2: chiave del messaggio corrisponde esattamente ad `aggregate_id` — `raw key="\"code-node-aggregate-42\""` (letterale stringa JSON, decodificato a `code-node-aggregate-42`).
- [x] Scenario 3: valore del messaggio corrisponde esattamente al contenuto di `payload` — `raw value="{\"language\":\"go\",\"symbol_id\":\"pkg.Foo#Bar()\"}"` (stesso contenuto dell'INSERT, chiavi riordinate da Postgres/JSONB ma stesso oggetto JSON; non la riga intera, non l'envelope Debezium grezzo).
- [x] Scenario 4: un evento `CodeRelation` produce un messaggio sul topic distinto `outbox.event.CodeRelation` — `raw key="\"code-relation-aggregate-99\""`, `raw value="{\"weight\":0.75,\"rel_type\":\"CALLS\"}"`.
- [x] Scenario 5: nessun messaggio spurio prima di qualunque `INSERT` — verificato su entrambi i topic (`outbox.event.CodeNode`, `outbox.event.CodeRelation`) prima di qualunque insert.
- [x] Il connector JSON di test è generato a partire dal file reale `deploy/compose/debezium-outbox-connector.json` (host/porta sostituiti via `buildConnectorConfig`, tutti gli altri campi letti dal file reale), non una copia duplicata mantenuta a mano.

## 10. Deviazioni rispetto alla SPEC

1. **Fix di configurazione condiviso con SPEC-007: mancavano i converter e
   `table.expand.json.payload`**. La prima esecuzione reale del test (senza
   questi campi, connettore invariato da SPEC-007) mostrava sia la chiave
   sia il valore avvolti in un envelope Kafka Connect
   `{"schema":..., "payload":...}` — non il "solo contenuto della colonna
   payload" descritto in §2. Causa: né il connector né il worker
   `kafka-connect` impostavano `key.converter.schemas.enable`/
   `value.converter.schemas.enable`, quindi valeva il default Kafka
   Connect `schemas.enable=true`. Per documentazione ufficiale Debezium
   sull'Outbox Event Router ("To enable use of the outbox event router
   transformation ... set the table.expand.json.payload to true, and use
   the JsonConverter"), la configurazione corretta richiede esplicitamente:
   `key.converter`/`value.converter` = `org.apache.kafka.connect.json.JsonConverter`,
   `key.converter.schemas.enable`/`value.converter.schemas.enable` = `false`,
   e `transforms.outbox.table.expand.json.payload` = `true`. Aggiunti tutti
   e cinque a **`deploy/compose/debezium-outbox-connector.json`** — file
   condiviso con SPEC-007, quindi il fix si propaga a entrambe le SPEC
   senza duplicazione. Verificato contro il sorgente
   `EventRouterConfigDefinition.java`/`EventRouterDelegate.java` a tag
   `v3.6.0.Final` (stessa versione bundlata nell'immagine) prima di
   applicare il fix, non assunto dalla sola documentazione prosa.

2. **Prefisso binario nella chiave, causa separata e non di
   configurazione**: dopo il fix del punto 1, il valore risultava già
   pulito ma la chiave conservava un prefisso di 8 byte
   (`\x01\x00\x00\x00\x00\x00\x00G...`) prima del JSON. Diagnosticato come
   bug del harness di test, non di Debezium/Kafka Connect: `Container.Exec`
   di testcontainers-go ritorna per default lo stream Docker **grezzo**
   (multiplexato, header di frame da 8 byte: 1 byte stream-type + 3 byte
   padding + 4 byte lunghezza big-endian — esattamente il pattern
   osservato) a meno di passare l'opzione `exec.Multiplexed()`. Aggiunta
   nel test (`tryConsumeOne`, `tests/integration/outbox_cdc/outbox_cdc_test.go`);
   il prefisso è sparito completamente alla riesecuzione. Non è stato
   necessario alcun cambiamento a `deploy/compose/debezium-outbox-connector.json`
   per questo punto — a riprova che i due sintomi avevano cause
   indipendenti (uno di configurazione connector, l'altro di API client
   testcontainers), come sospettato e verificato invece di assunto.

3. **La chiave del messaggio resta un letterale stringa JSON con
   virgolette** (es. `"code-node-aggregate-42"`, non `code-node-aggregate-42`
   nudo) anche con `key.converter.schemas.enable=false`: comportamento
   standard di `JsonConverter` per un valore di schema STRING, non un
   envelope residuo. Il test decodifica esplicitamente questo letterale
   (`decodeJSONString`) prima del confronto, invece di confrontare byte
   grezzi — coerente con come il valore viene già confrontato via
   `assertJSONEqual` (anch'esso un `json.Unmarshal`, non un confronto di
   stringhe grezze).

4. **Nessun listener `EXTERNAL`/porta host per Kafka in questo harness**
   (a differenza dello stack compose dev di SPEC-007): nulla sull'host
   deve raggiungere Kafka direttamente qui — `kafka-connect` lo raggiunge
   via alias di rete interno (`outbox-cdc-kafka:9092`), e il test stesso
   consuma i messaggi via `Exec` dentro il container Kafka
   (`kafka-console-consumer.sh`), non da un client Kafka sull'host.
   Evita la complessità di un listener con porta host dinamica nota solo
   dopo l'avvio del container.

5. **Modulo Go indipendente** in `tests/integration/outbox_cdc` (proprio
   `go.mod`), stesso pattern di `tests/integration/postgres_ddl` (SPEC-005)
   e `tools/migrate-neo4j` (SPEC-004) — non wired in `Taskfile.yml`
   (`task lint`/`task test`/`task test:integration`) in questa sessione:
   il perimetro file assegnato era solo `tests/integration/outbox_cdc` e
   questa SPEC. `go build`/`go vet -tags=integration` e
   `go test -tags=integration -count=1 -v ./...` eseguiti manualmente,
   verdi.
