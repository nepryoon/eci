# deploy/compose — stack di sviluppo locale (SPEC-006)

Docker Compose per i cinque datastore di sviluppo: PostgreSQL 17, Neo4j 5
Community, Qdrant, OpenSearch, MinIO. Kafka/Debezium sono fuori scope
(T0.6b) e verranno aggiunti come nuovi servizi in questo stesso file.

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

## Comandi

- `task up` — avvia lo stack e attende (timeout di default 90s
  complessivi, configurabile con `UP_TIMEOUT_SECONDS`) che tutti i servizi
  risultino `healthy`. Se il timeout scade, elenca esplicitamente quali
  servizi non ce l'hanno fatta.
- `task up:verify` — verifica connettività reale (non solo "container up")
  a ciascuno dei 5 servizi: `SELECT 1` su Postgres, `RETURN 1` su Neo4j via
  bolt, `GET /collections` su Qdrant, `/_cluster/health` su OpenSearch
  (accetta `green`/`yellow`, mai `red`), `/minio/health/live` su MinIO.
  Ogni servizio è riportato singolarmente (`OK`/`FAIL` + motivo).
- `task down` — ferma e rimuove i container. **I volumi nominati NON
  vengono rimossi**: i dati di sviluppo sopravvivono a un ciclo
  `down`/`up`. Per ripartire da zero: `docker compose -f
  deploy/compose/docker-compose.yml down -v`.

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
