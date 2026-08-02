# deploy/compose — stack di sviluppo locale (SPEC-006 + SPEC-007)

Docker Compose per i cinque datastore di sviluppo (PostgreSQL 17, Neo4j 5
Community, Qdrant, OpenSearch, MinIO — SPEC-006) più Kafka (KRaft) e Kafka
Connect con il connector Debezium sulla tabella `outbox` (SPEC-007).

## Prerequisiti

- Docker con Compose v2 (`docker compose version`, non il vecchio binario
  `docker-compose` v1 separato).
- **`vm.max_map_count` >= 262144 su host Linux** (limite kernel richiesto da
  OpenSearch/Elasticsearch — senza, il container OpenSearch non diventa mai
  healthy e `task up` fallisce con timeout esplicito elencandolo come
  servizio non healthy). Verifica:

  ```
  sysctl vm.max_map_count
  ```

  Se sotto 262144, alzalo temporaneamente:

  ```
  sudo sysctl -w vm.max_map_count=262144
  ```

  o in modo persistente aggiungendo `vm.max_map_count=262144` a
  `/etc/sysctl.conf` (o un file in `/etc/sysctl.d/`).
- **`wal_level=logical` su Postgres** (SPEC-007): il servizio `postgres`
  parte già con `-c wal_level=logical -c max_wal_senders=4 -c
  max_replication_slots=4` — nessuna azione manuale richiesta, è
  configurato nel `command:` del servizio. Documentato qui perché è un
  prerequisito hard per Debezium: senza logical decoding abilitato, il
  connector non può leggere il WAL e fallisce alla registrazione (o subito
  dopo) con un errore Debezium esplicito visibile in `docker compose logs
  kafka-connect`, non un timeout criptico. Verifica diretta: `docker
  compose -f deploy/compose/docker-compose.yml exec postgres psql -U eci
  -d eci -c "SHOW wal_level;"` deve rispondere `logical`.

## Comandi

- `task up` — avvia lo stack e attende (timeout di default 90s
  complessivi, configurabile con `UP_TIMEOUT_SECONDS`) che tutti i servizi
  risultino `healthy`, poi registra il connector Debezium outbox
  (`deploy/compose/register-connector.sh`, con retry/backoff se Kafka
  Connect non è ancora pronto; un 409 Conflict, connector già registrato,
  è trattato come successo — `task up` resta rilanciabile senza errori).
  Se il timeout scade, elenca esplicitamente quali servizi non ce l'hanno
  fatta.
- `task up:verify` — verifica connettività reale (non solo "container up")
  a ciascuno dei 5 datastore: `SELECT 1` su Postgres, `RETURN 1` su Neo4j
  via bolt, `GET /collections` su Qdrant, `/_cluster/health` su OpenSearch
  (accetta `green`/`yellow`, mai `red`), `/minio/health/live` su MinIO;
  più `GET /connectors/eci-outbox-connector/status` su Kafka Connect,
  atteso `connector.state=RUNNING` con tutti i task in `tasks[]` `RUNNING`.
  Ogni servizio è riportato singolarmente (`OK`/`FAIL` + motivo).
- `task down` — ferma e rimuove i container. **I volumi nominati NON
  vengono rimossi**: i dati di sviluppo (incluso il replication slot
  Postgres e i topic Kafka) sopravvivono a un ciclo `down`/`up`. Per
  ripartire da zero: `docker compose -f deploy/compose/docker-compose.yml
  down -v`.

## Porte esposte sull'host

| Servizio | Porta | Uso |
|---|---|---|
| postgres | 5432 | Postgres wire protocol |
| neo4j | 7474 | HTTP/Browser |
| neo4j | 7687 | Bolt |
| qdrant | 6333 | REST |
| qdrant | 6334 | gRPC |
| opensearch | 9200 | REST |
| minio | 9000 | API S3 |
| minio | 9001 | Console web |
| kafka | 9094 | Listener esterno (`EXTERNAL`), per client dall'host. Il listener interno `9092` (`PLAINTEXT://kafka:9092`, usato da `kafka-connect` sulla rete Docker) non è esposto sull'host |
| kafka-connect | 8083 | REST API (registrazione/stato connector) |

Se una porta è già occupata da un altro processo (es. un Postgres locale
su 5432), `docker compose up` fallisce con l'errore nativo di Docker sulla
porta in conflitto. Cambia il mapping nella sezione `ports:` del servizio
in `docker-compose.yml`.

## Credenziali (SOLO sviluppo locale)

`POSTGRES_PASSWORD`, `NEO4J_AUTH`, `MINIO_ROOT_PASSWORD` nel file compose
sono valori di sviluppo fissi e pubblici in questo repo. **Non riusarli in
nessun altro ambiente** (staging, produzione, o qualunque istanza
raggiungibile fuori da questa macchina).

## Nota: OpenSearch Security plugin disattivato

Il servizio `opensearch` gira con `DISABLE_SECURITY_PLUGIN=true`. È una
semplificazione deliberata per ridurre l'attrito in sviluppo locale. Il
Modulo 3 dell'ADD richiede il Security plugin attivo in produzione per la
Document-Level Security — questa disattivazione non va riprodotta in
nessun ambiente non-dev.

## Neo4j Community Edition

Il servizio `neo4j` usa `neo4j:5-community`, non Enterprise — vincolante
per via di [ADR-0004](../../docs/decisions/ADR-0004-rimuovi-existence-constraint-neo4j.md)
e [ADR-0005](../../docs/decisions/ADR-0005-rimuovi-emb-vector-type-constraint.md)
(SPEC-004).

## Kafka + Debezium (CDC outbox, SPEC-007)

`kafka` gira in modalità KRaft nativa (nessun ZooKeeper). `kafka-connect`
usa l'immagine `quay.io/debezium/connect:latest` (non `debezium/connect`
su Docker Hub — quel repository non pubblica un tag `latest`, solo tag di
versione esplicita; vedi deviazione in SPEC-007 §10) con i plugin Debezium
preinstallati, incluso il connector PostgreSQL. La configurazione del
connector è versionata in `deploy/compose/debezium-outbox-connector.json`
(EventRouter SMT su `aggregate_type`/`aggregate_id`, come da ADD Modulo 1
§2.2.2) e registrata automaticamente da `task up`.

Il flusso end-to-end completo (INSERT reale → messaggio leggibile sul
topic Kafka) non è verificato qui: questa SPEC si ferma allo stato
`connector.state=RUNNING`, confermato via REST API. Il test end-to-end è
SPEC-008.

### Replication slot residuo dopo `down`/`up`

Il connector usa uno slot di replica Postgres con nome fisso
(`eci_outbox_slot`). Nei test di questa SPEC, un ciclo `task down` (senza
`-v`, volumi persistenti) seguito da `task up` non ha mai causato un
fallimento: Debezium riusa correttamente lo slot esistente sul volume
Postgres persistito. Se in futuro la registrazione dovesse fallire per uno
slot esistente ma in stato inconsistente (es. dopo un arresto non pulito),
il fix manuale è:

```sql
SELECT pg_drop_replication_slot('eci_outbox_slot');
```

da eseguire su Postgres prima di ri-registrare il connector — non
automatizzato in questa SPEC.
