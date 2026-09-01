# SPEC-007 — Compose di sviluppo: Kafka KRaft + Debezium/Kafka Connect (CDC outbox)
Stato: verified
Task-tree: T0.6b (secondo split di T0.6 — vedi SPEC-006 §5) · Servizio: deploy/compose · ADD: Modulo 1 §2.2 (CDC & Eventual Consistency)
Contratti: nessuno (nessun file sotto contracts/ toccato)

## 1. Obiettivo
Estendere lo STESSO `deploy/compose/docker-compose.yml` di SPEC-006 con Kafka (modalità KRaft, nessun ZooKeeper) e Kafka Connect con il connector Debezium PostgreSQL, configurato per intercettare la tabella `outbox` tramite EventRouter SMT — dimostrando che il connector si registra con successo e resta in stato `RUNNING` contro lo stack reale. Il flusso end-to-end completo (un `INSERT` reale che produce un messaggio effettivamente leggibile sul topic Kafka) è esplicitamente fuori scope: SPEC successiva (il vecchio T0.7, ora SPEC-008).

## 2. Interfaccia

Estensione del file esistente `deploy/compose/docker-compose.yml` — non un file compose separato (coerente con SPEC-006 §5: un solo `task up` avvia tutto lo stack di sviluppo).

**Modifica al servizio `postgres` già esistente** (unica modifica a un servizio di SPEC-006 — tutti gli altri sono nuovi): aggiungere
```yaml
command: ["postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4"]
```
Senza `wal_level=logical`, Debezium non può fare logical decoding — fallisce con un errore Postgres/Debezium esplicito, non un timeout criptico. `max_wal_senders`/`max_replication_slots` a 4 sono ampiamente sufficienti per un solo connector dev (default Postgres è già 10, ma esplicitarli documenta l'intento).

**kafka** — immagine `apache/kafka:latest` (immagine ufficiale Apache Kafka, KRaft nativo, un solo container che copre sia il ruolo broker sia controller — adatta per singolo nodo dev; verificare al momento dell'implementazione la versione risolta, stesso trattamento già riservato a Qdrant in SPEC-006). Variabili d'ambiente con prefisso `KAFKA_` (non `KAFKA_CFG_`, che è la convenzione Bitnami — immagine diversa, non usare doc/esempi Bitnami come riferimento):
```yaml
environment:
  KAFKA_NODE_ID: 1
  KAFKA_PROCESS_ROLES: broker,controller
  KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```
I tre `*_REPLICATION_FACTOR`/`MIN_ISR` a 1 sono **obbligatori su singolo broker**: senza, Kafka si aspetta un replication factor ≥3 di default sui topic interni e fallisce ad avviarsi o resta in uno stato inconsistente — errore comune e ben documentato di setup single-node. `KAFKA_ADVERTISED_LISTENERS` distingue esplicitamente il listener interno (`kafka:9092`, usato da `kafka-connect` sulla rete Docker) da quello esterno (`localhost:9094`, per client dall'host) — un mismatch qui è la causa più comune di client che si connettono e poi falliscono sulle richieste successive di metadata.
Porta host `9094:9094` (solo il listener esterno). Volume nominato `eci_kafka_data` su `/var/lib/kafka/data`. Healthcheck: `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` (script nativo incluso nell'immagine).

**kafka-connect** — immagine ufficiale `quay.io/debezium/connect` bloccata al digest `sha256:698f0559e667a242f962221079e75917b2b7a3ad4de62661e977628da0e33b45` (Kafka Connect con i plugin Debezium — incluso il connector PostgreSQL — già preinstallati; nessuna installazione manuale di plugin necessaria). Env:
```yaml
environment:
  BOOTSTRAP_SERVERS: kafka:9092
  GROUP_ID: eci-connect
  CONFIG_STORAGE_TOPIC: eci_connect_configs
  OFFSET_STORAGE_TOPIC: eci_connect_offsets
  STATUS_STORAGE_TOPIC: eci_connect_status
```
Porta host `8083:8083` (REST API). `depends_on: [kafka, postgres]`. Healthcheck: richiesta HTTP a `/connectors` (200, anche con lista vuota, indica che il REST API è pronto).

**Configurazione del connector Debezium**, versionata nel repo (non generata a runtime) in `deploy/compose/debezium-outbox-connector.json`:
```json
{
  "name": "eci-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "eci",
    "database.password": "eci-dev-only",
    "database.dbname": "eci",
    "topic.prefix": "eci",
    "table.include.list": "public.outbox",
    "plugin.name": "pgoutput",
    "slot.name": "eci_outbox_slot",
    "publication.name": "eci_outbox_publication",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field": "aggregate_type",
    "transforms.outbox.table.field.event.key": "aggregate_id"
  }
}
```
`plugin.name=pgoutput` usa il plugin di decodifica logica nativo di PostgreSQL (disponibile dalla 9.4+, nessuna estensione aggiuntiva richiesta). `transforms.outbox.*` sono l'EventRouter SMT esattamente come specificato nell'ADD Modulo 1 §2.2.2 (`route.by.field=aggregate_type`, `table.field.event.key=aggregate_id`). Le credenziali coincidono con quelle del servizio `postgres` di SPEC-006.

**Script `deploy/compose/register-connector.sh`**: POST del JSON sopra a `http://localhost:8083/connectors`, con retry/backoff se Kafka Connect non è ancora pronto (non un singolo tentativo secco). Un **409 Conflict** (connector già registrato) è trattato come successo, non come errore — idempotenza coerente con lo stile già adottato in `up.sh`/`verify.sh` di SPEC-006.

Target Taskfile: `task up` (esteso) avvia anche `kafka`/`kafka-connect`, attende che siano `healthy`, poi esegue `register-connector.sh` — un solo comando continua ad avviare tutto lo stack, connector incluso. `task up:verify` (esteso, non un nuovo target separato — stessa filosofia "un comando verifica tutto") aggiunge un controllo: `GET /connectors/eci-outbox-connector/status` deve riportare `connector.state=RUNNING` e lo stato di ogni task in `tasks[]` deve essere `RUNNING`.

## 3. Comportamento (scenari)

1. **Dato** nessuno stack in esecuzione, **quando** eseguo `task up`, **allora** anche `kafka` e `kafka-connect` diventano `healthy`, e `register-connector.sh` completa senza errori (il connector risulta registrato).
2. **Dato** lo stack in esecuzione, **quando** eseguo `task up:verify`, **allora** `GET /connectors/eci-outbox-connector/status` risponde con `connector.state=RUNNING` e tutti i task del connector in stato `RUNNING` (nessun `FAILED`).
3. **Dato** il connector già registrato, **quando** rilancio `task up` una seconda volta, **allora** `register-connector.sh` non fallisce (il 409 Conflict è gestito come non-errore) e lo stato resta `RUNNING`.
4. **Dato** lo stack in esecuzione, **quando** interrogo `SHOW wal_level;` su Postgres, **allora** il valore restituito è `logical` — conferma diretta del prerequisito CDC, non dedotta indirettamente dal solo fatto che il connector si sia registrato.
5. **Dato** uno stack già usato in precedenza (`task down` seguito da `task up`, volumi persistenti), **quando** il connector viene ri-registrato, **allora** non fallisce per via di uno slot di replica residuo dal run precedente — o perché lo slot con lo stesso nome viene riusato correttamente, o (se questo non risultasse possibile) l'edge case è documentato esplicitamente in §4 con il fix manuale.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `wal_level` non impostato a `logical` (dimenticato nella config Postgres) | Il connector fallisce alla registrazione o subito dopo con un errore Debezium esplicito che nomina il requisito mancante — verificabile nei log di `kafka-connect` (`docker compose logs kafka-connect`), non un fallimento muto |
| Kafka Connect non ancora pronto quando `register-connector.sh` tenta il POST | Retry con backoff entro un timeout complessivo esplicito; se il timeout scade, fallire con messaggio chiaro, non restare appeso |
| Connector già registrato (409 Conflict) | Trattato come successo — coerente con la necessità che `task up` resti rilanciabile senza errori (scenario 3) |
| `KAFKA_ADVERTISED_LISTENERS` configurato in modo errato (listener interno/esterno confusi) | `kafka-connect`, che gira sulla rete Docker interna e usa `kafka:9092`, deve continuare a funzionare indipendentemente dal listener esterno `9094` — i due listener sono indipendenti; verificare che la configurazione non li scambi |
| Replication slot residuo da un run precedente dopo `task down`/`task up` (slot dal nome fisso `eci_outbox_slot`, volume Postgres persistente) | Documentare nel README: se il connector fallisce alla ri-registrazione per slot esistente ma inconsistente, il fix manuale è `SELECT pg_drop_replication_slot('eci_outbox_slot');` su Postgres prima di ri-registrare — non un fix automatico in questa SPEC, solo documentato esplicitamente |

## 5. Non-goals
Il flusso end-to-end completo — un `INSERT` reale nella tabella `outbox` che produce un messaggio effettivamente leggibile sul topic Kafka atteso, con il payload corretto dopo la trasformazione EventRouter — è la SPEC successiva (SPEC-008, il vecchio T0.7: "harness testcontainers Go/Py + smoke test INSERT outbox → evento sul topic"). Qui ci si ferma allo stato `RUNNING` del connector, verificato via REST API. Nessun consumer applicativo (sink graph/vector/search): lavoro di Fase 1+. Nessuna gestione avanzata di errori/DLQ del connector Debezium stesso (da affrontare quando i sink consumer esisteranno davvero).

## 6. Vincoli dall'ADD
Modulo 1 §2.2.2: Debezium legge il WAL (non polling, nessun carico di query sul DB), EventRouter SMT con `route.by.field=aggregate_type` e `table.field.event.key=aggregate_id`. Partitioning per `aggregate_id` è implicito nella chiave del messaggio prodotta da EventRouter (la configurazione esplicita del partitioning a livello di topic Kafka, se necessaria oltre al default, è demandata alla SPEC che introdurrà i topic/sink reali). Kafka KRaft — Modulo 4 §1.3.

## 7. Test plan
Nessun testcontainers qui (come SPEC-006: lo stack stesso è l'oggetto sotto verifica, non qualcosa che si spinge su/giù in un test automatizzato). `task up:verify` esteso è il test primario (scenario 2). Job CI dedicato: probabilmente non conveniente per questa SPEC — Kafka+Connect aggiungono tempo di avvio significativo sopra quello già non trascurabile di SPEC-006; raccomandazione di default: verifica solo-locale, documentata nel README, non bloccante per la CI (da confermare/rivalutare in fase di implementazione se il tempo risultasse comunque accettabile).

## 8. Osservabilità
Non applicabile in questa SPEC (osservabilità vera è Fase 7 / Modulo 4). I log di `kafka-connect` (`docker compose logs kafka-connect`) sono la fonte primaria per diagnosticare problemi del connector durante lo sviluppo.

## 9. Criteri di accettazione
- [x] `task up` porta anche `kafka`/`kafka-connect` a stato `healthy` e registra il connector senza errori — stack reale, tutti i 7 servizi healthy in 36-40s, connector registrato (201 Created) al primo run. Riverificato anche con `public.outbox` presente (dopo `task db:migrate`, eseguito dopo `task up` in questo test): `table.include.list=public.outbox` risulta l'unica tabella catturata su 5 nello schema `public` (`code_node`, `code_relation`, `outbox`, `processed_events`, `schema_migrations` — quest'ultima è la tabella di bookkeeping di `golang-migrate`), confermato via `GET /connectors/eci-outbox-connector/config`. La pubblicazione Postgres `eci_outbox_publication` è invece `FOR ALL TABLES` (`puballtables=t` in `pg_publication`) e quindi traccia tutte e 5 le tabelle a livello WAL (`pg_publication_tables` le elenca tutte e 5, con gli `attnames` reali di ciascuna — per `outbox`: `{id,aggregate_type,aggregate_id,event_type,payload,trace_id,created_at}`, che coincidono esattamente con le colonne di SPEC-005) — è `table.include.list` lato Debezium, non la pubblicazione Postgres, a restringere la cattura effettiva alla sola `outbox`.
- [x] `task up:verify` conferma `connector.state=RUNNING` e tutti i task `RUNNING` (scenario 2) — confermato via REST reale (`GET /connectors/eci-outbox-connector/status`).
- [x] `task up` rilanciato una seconda volta non fallisce sulla registrazione del connector (scenario 3) — rilanciato più volte di seguito (worker Kafka Connect ancora attivo): `register-connector.sh` riceve 409, lo tratta come successo, `task up` esce con rc=0, connector resta RUNNING.
- [x] `SHOW wal_level;` su Postgres restituisce `logical` (scenario 4) — interrogato direttamente via `psql`, risposta `logical`.
- [x] Comportamento su slot di replica residuo dopo `down`/`up` verificato (scenario 5) — vedi §10 per il meccanismo osservato esattamente (non quello ipotizzato nel testo originale della SPEC).
- [x] README aggiornato con: prerequisito `wal_level=logical` (motivato), nuove porte (`9094` Kafka esterno, `8083` Kafka Connect REST), fix manuale per lo slot di replica residuo, e la nota su stack fresco/`task db:migrate` prima di considerare il connector operativo (vedi sotto).

## 10. Deviazioni rispetto alla SPEC

1. **`debezium/connect:latest` non esiste**: `docker pull debezium/connect:latest`
   fallisce (`not found`) — quel repository su Docker Hub pubblica solo tag
   di versione esplicita (es. `2.7.3.Final`), mai `latest`. Usata invece
   `quay.io/debezium/connect` bloccata al digest `sha256:698f0559e667a242f962221079e75917b2b7a3ad4de62661e977628da0e33b45`, l'immagine ufficiale equivalente con
   immagine mantenuta da Debezium stesso su Quay e bloccata per digest — stessi plugin
   preinstallati (verificato: `debezium-connector-postgres-3.6.0.Final.jar`
   presente in `/kafka/connect/debezium-connector-postgres/`), `curl`
   disponibile nell'immagine (usato per l'healthcheck).

2. **`apache/kafka:latest` non pinnata**: verificata al momento
   dell'implementazione, risolve a Kafka **4.3.1** — KRaft-only dalla 4.0
   (nessuna modalità ZooKeeper da poter scegliere per errore). Stesso
   trattamento già riservato a Qdrant in SPEC-006: nessun pin esplicito
   necessario.

3. **Scenario 5, meccanismo osservato diverso da quello ipotizzato nel
   testo della SPEC**: il testo prevedeva o un riuso del connector via 409
   o un fallimento da documentare. Il comportamento realmente osservato
   (riprodotto su più cicli `task down` + `task up`, volumi Postgres/Kafka
   persistenti, container `kafka-connect` ricreato da zero) è un terzo
   caso: `register-connector.sh` riceve **201 Created** (non 409) a ogni
   riavvio completo dello stack, perché il worker Kafka Connect ricreato
   non ha lo stato del connector precedente in memoria al momento del POST.

   Nonostante questo, non si verifica mai un fallimento — ma la
   convergenza a uno stato consistente **non è immediata, verificato
   empiricamente**: subito dopo il 201/409 (in entrambi i casi, non solo
   al 201), `GET .../status` può mostrare temporaneamente `tasks: []`
   (nessun task ancora assegnato) e lo slot di replica Postgres con
   `active=false`, perché in modalità distribuita Kafka Connect assegna il
   task al worker solo dopo un rebalance che richiede alcuni secondi (~15s
   osservati). Solo dopo quell'intervallo il task passa a `RUNNING` e lo
   slot diventa `active=true`. Per questo `register-connector.sh` non
   dichiara più successo al solo 201/409: dopo la registrazione fa il poll
   di `GET .../status` (timeout separato, `RUNNING_TIMEOUT_SECONDS`,
   default 60s) finché `connector.state=RUNNING` **e** almeno un elemento
   di `tasks[]` è `RUNNING`, prima di ritornare. Verificato via `SELECT
   slot_name, active, plugin FROM pg_replication_slots;`: un solo slot
   `eci_outbox_slot`, nessun duplicato, `active=t` una volta che il poll
   ha confermato successo. Il fix manuale per lo scenario realmente
   problematico (slot esistente ma inconsistente) resta documentato nel
   README come da SPEC, anche se non è mai stato necessario innescarlo nei
   test di questa sessione.

4. **`up.sh`/`verify.sh` estesi, non nuovi script**: coerente con
   l'indicazione della SPEC di estendere `task up`/`task up:verify` senza
   nuovi target — la registrazione del connector è un passo aggiuntivo in
   fondo a `deploy/compose/up.sh` (dopo che tutti i servizi, `kafka`/
   `kafka-connect` inclusi, sono `healthy`), e il controllo dello stato
   connector è un blocco aggiuntivo in fondo a `deploy/compose/verify.sh`.
