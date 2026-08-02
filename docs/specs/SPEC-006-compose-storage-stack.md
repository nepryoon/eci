# SPEC-006 — Compose di sviluppo: datastore storage (Postgres, Neo4j, Qdrant, OpenSearch, MinIO)
Stato: implemented
Task-tree: T0.6 (split — vedi §5 Non-goals) · Servizio: deploy/compose · ADD: Modulo 1 (datastore), Modulo 4 §2 (deployment strategy, qui in versione dev locale)
Contratti: nessuno (nessun file sotto contracts/ toccato)

## 1. Obiettivo
Sostituire i placeholder `task up`/`task down` di SPEC-001 con uno stack Docker Compose reale per i cinque datastore di sviluppo (PostgreSQL 17, Neo4j 5 Community, Qdrant, OpenSearch, MinIO), con healthcheck nativi, volumi persistenti tra riavvii, e un target di verifica che confermi connettività reale a ciascun servizio — non solo che i container siano "up". Kafka KRaft e Debezium/Kafka Connect sono esplicitamente esclusi (SPEC successiva, T0.6b).

## 2. Interfaccia

File: `deploy/compose/docker-compose.yml`. Rete dedicata (`eci-dev`). Volumi Docker nominati (non bind-mount su path host, per evitare problemi di permessi cross-OS): `eci_postgres_data`, `eci_neo4j_data`, `eci_neo4j_logs`, `eci_qdrant_data`, `eci_opensearch_data`, `eci_minio_data`.

**postgres** — immagine `postgres:17` (stessa versione già validata in SPEC-005 via testcontainers). Env: `POSTGRES_USER=eci`, `POSTGRES_PASSWORD=eci-dev-only` (credenziali di sviluppo, mai da riusare in altri contesti — annotarlo nel commento del file), `POSTGRES_DB=eci`. Porta host `5432:5432`. Healthcheck: `pg_isready -U eci`.

**neo4j** — immagine `neo4j:5-community` (stessa già validata via testcontainers in SPEC-004; vincolante per via di ADR-0004/ADR-0005 — mai Enterprise in questo stack). Env: `NEO4J_AUTH=neo4j/eci-dev-only`. Porte host `7474:7474` (HTTP/Browser), `7687:7687` (Bolt). Volumi su `/data` e `/logs`. Healthcheck: `wget -O /dev/null -q http://localhost:7474 || exit 1` (o equivalente verificabile senza dipendenze extra nell'immagine).

**qdrant** — immagine `qdrant/qdrant:latest` (vincolo ADD: ≥1.11 per `is_tenant`; verificare al momento dell'implementazione che `:latest` risolva a una versione ≥1.11, altrimenti pinnare esplicitamente). Porta host `6333:6333` (REST), `6334:6334` (gRPC). Volume su `/qdrant/storage`. Healthcheck: richiesta HTTP a `/healthz` o `/collections`.

**opensearch** — immagine `opensearchproject/opensearch:latest`. Env: `discovery.type=single-node`, `DISABLE_SECURITY_PLUGIN=true` (deliberata semplificazione da dev: il Modulo 3 dell'ADD richiede il Security plugin attivo in produzione per la Document-Level Security — qui disattivato solo per ridurre l'attrito locale; annotarlo nel commento del file come nota, non da riprodurre in nessun ambiente non-dev). Porta host `9200:9200`. Volume su `/usr/share/opensearch/data`. Healthcheck: richiesta HTTP a `/_cluster/health`, accettando status `green` o `yellow` (mai `red`).

**minio** — immagine `minio/minio:latest`. Comando `server /data --console-address ":9001"`. Env: `MINIO_ROOT_USER=eci`, `MINIO_ROOT_PASSWORD=eci-dev-only-minio` (MinIO richiede minimo 8 caratteri per la root password — verificare che il valore scelto rispetti il vincolo). Porte host `9000:9000` (API S3), `9001:9001` (console). Volume su `/data`. Healthcheck: richiesta HTTP a `/minio/health/live`.

Target Taskfile (sostituiscono i placeholder di SPEC-001):
- `task up` → `docker compose -f deploy/compose/docker-compose.yml up -d`, poi attende (poll con timeout, default 90s complessivi) che tutti i servizi con healthcheck risultino `healthy`; se il timeout scade, fallisce elencando quali servizi non sono diventati healthy.
- `task down` → `docker compose -f deploy/compose/docker-compose.yml down` (i volumi nominati NON vengono rimossi di default — i dati di sviluppo sopravvivono).
- `task up:verify` (nuovo target) → esegue i controlli di connettività reale dello scenario 2 (§3) contro lo stack già in esecuzione.

## 3. Comportamento (scenari)

1. **Dato** nessuno stack in esecuzione, **quando** eseguo `task up`, **allora** entro il timeout tutti e 5 i container riportano stato `healthy` (verificabile con `docker compose ps`).
2. **Dato** lo stack in esecuzione, **quando** eseguo `task up:verify`, **allora**: una query `SELECT 1` su Postgres riesce; una query Cypher `RETURN 1` su Neo4j riesce (via bolt, credenziali dell'env); una richiesta REST a Qdrant risponde 200; `/_cluster/health` di OpenSearch risponde con status diverso da `red`; `/minio/health/live` risponde 200. Ogni fallimento riporta esplicitamente quale servizio e perché, non un errore aggregato generico.
3. **Dato** lo stack in esecuzione con dati scritti (es. una riga inserita in Postgres durante un test manuale), **quando** eseguo `task down` seguito da `task up`, **allora** i dati sono ancora presenti (i volumi nominati sopravvivono al ciclo down/up).
4. **Dato** nessuno stack mai avviato, **quando** eseguo `task down`, **allora** il comando termina senza errore (no-op pulito, non deve fallire per l'assenza di container da fermare).
5. **Dato** lo stack in esecuzione, **quando** eseguo `task db:migrate` e `task db:neo4j:migrate` (i migration runner già costruiti in SPEC-004/SPEC-005) puntandoli a questo stack invece che a un container testcontainers effimero, **allora** entrambe le migration completano con successo — è la prova che l'intero lavoro di migration di questa Fase 0 funziona anche contro lo stack di sviluppo persistente, non solo contro container di test usa-e-getta.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Porta host già occupata (es. un altro Postgres locale su 5432) | `docker compose up` fallisce con l'errore nativo di Docker sulla porta; documentare nel README di `deploy/compose` che le porte sono nella sezione `ports:` e vanno modificate lì se in conflitto |
| OpenSearch non diventa mai healthy per `vm.max_map_count` insufficiente sull'host Linux (limite kernel comune per Elasticsearch/OpenSearch in Docker) | **Prerequisito da documentare esplicitamente nel README**: `sudo sysctl -w vm.max_map_count=262144` (temporaneo) o persistente in `/etc/sysctl.conf`. `task up` deve comunque fallire con timeout esplicito, non restare appeso, elencando OpenSearch come il servizio non healthy |
| `task up:verify` eseguito mentre lo stack non è in esecuzione | fallisce esplicitamente indicando che i container non sono attivi, non con un timeout di connessione criptico |
| MinIO root password sotto la soglia minima di 8 caratteri | il container MinIO si rifiuta di avviarsi; verificare a monte nel file compose stesso, non scoprirlo solo a runtime |

## 5. Non-goals
Kafka (KRaft), Debezium/Kafka Connect, il connector CDC sull'outbox: SPEC separata (T0.6b), che estenderà lo STESSO file `docker-compose.yml` con i nuovi servizi (non un file compose separato — un solo `task up` deve continuare ad avviare tutto lo stack di sviluppo). Nessuna esecuzione automatica delle migration da `task up` (restano comandi espliciti separati). Nessun caricamento di dati seed/fixture.

## 6. Vincoli dall'ADD
Neo4j Community Edition, vincolante per via di ADR-0004/ADR-0005 (Modulo 1, deviazioni SPEC-004). PostgreSQL 17 (Modulo 1 §2.2.1). Qdrant ≥1.11 (Modulo 4 §1.3, requisito `is_tenant`). OpenSearch con Security plugin — **attivo in produzione per il Modulo 3** (Document-Level Security), qui disattivato solo per lo sviluppo locale: deviazione esplicita da annotare, non silenziosa.

## 7. Test plan
- `task up:verify` è di per sé il test primario (connettività reale, non simulata) — vedi scenario 2.
- Verifica manuale/scriptata degli scenari 3 e 4 (persistenza tra down/up, idempotenza di `task down`).
- Scenario 5 collega esplicitamente questa SPEC al lavoro già fatto: rilancia i migration runner di SPEC-004/SPEC-005 contro questo stack.
- Job CI dedicato (separato da `guard`/`build-lint-test`, dato il tempo di avvio di 5 container): valutare in fase di implementazione se il tempo di esecuzione resta accettabile; se troppo lento, documentarlo come verifica solo-locale con una nota nel README invece che bloccare la CI.

## 8. Osservabilità
Non applicabile in questa SPEC (lo stack di observability vero è Fase 7 / Modulo 4). I log dei container restano accessibili via `docker compose logs <servizio>` per il debug.

## 9. Criteri di accettazione
- [x] `task up` porta tutti e 5 i servizi a stato `healthy` entro il timeout — verificato ripetutamente con stack reale, ~21-27s (ben sotto il timeout di 90s), incluso un run a freddo (volumi appena creati, immagini già pull-ate).
- [x] `task up:verify` conferma connettività reale a ciascuno dei 5 servizi (scenario 2), con fallimento esplicito e specifico per servizio in caso di problema — verificato con stack up (tutti `OK`) e con stack fermo (fallimento esplicito "nessun container ... in esecuzione", non un timeout criptico).
- [x] Persistenza dei dati tra `task down`/`task up` verificata (scenario 3) — riga inserita in Postgres, `task down` + `task up`, riga ancora presente.
- [x] `task down` su stack mai avviato non fallisce (scenario 4) — verificato: `task down` due volte di fila, entrambe rc=0.
- [x] `task db:migrate` e `task db:neo4j:migrate` completano con successo puntando a questo stack (scenario 5) — entrambi eseguiti contro lo stack compose reale (non testcontainers): 4 tabelle create in Postgres, 10 statement Cypher creati in Neo4j; ri-eseguiti una seconda volta per confermare l'idempotenza anche qui (`no change` / `10 già esistenti, 0 creati`).
- [x] README in `deploy/compose/` documenta il prerequisito `vm.max_map_count` per OpenSearch e la nota su porte/credenziali di sviluppo.

## 10. Deviazioni rispetto alla SPEC

1. **Verifiche pre-implementazione richieste dall'utente**: `docker compose`
   v2 presente (`v5.3.0`, nessun `docker-compose` v1 standalone separato
   installato); `vm.max_map_count` sull'host = **1048576**, ampiamente sopra
   la soglia minima 262144 richiesta da OpenSearch — nessuna azione
   necessaria su questa macchina, ma il prerequisito resta documentato nel
   README per chi userà lo stack su un host diverso.

2. **`name:` esplicito sui 6 volumi e sul progetto compose**: senza,
   Compose v2 prefissa i nomi risorsa col nome del progetto (di default la
   directory, es. `compose_eci_postgres_data`), diverso dai nomi letterali
   `eci_postgres_data` ecc. richiesti da §2. Aggiunto `name: <nome
   esatto>` a ogni volume e `name: eci-dev` a livello di progetto —
   verificato con `docker volume ls` che i nomi risultanti sono esatti.

3. **Qdrant, immagine `:latest` non pinnata**: verificato al momento
   dell'implementazione che `qdrant/qdrant:latest` risolve a **v1.18.3**
   (label `org.opencontainers.image.version`), ampiamente sopra il vincolo
   `>= 1.11` dell'ADD — nessun pin esplicito necessario, come previsto
   dalla condizionale di §2.

4. **Healthcheck Qdrant degradato a controllo TCP**: l'immagine
   `qdrant/qdrant` non include né `curl` né `wget` (solo `bash`/`sh`),
   quindi l'healthcheck Docker-level del container non può fare la
   richiesta HTTP a `/healthz`/`/collections` suggerita da §2. Usa invece
   un controllo TCP via `/dev/tcp` di bash sulla porta 6333. Il vero
   controllo HTTP applicativo (`GET /collections`, atteso 200) è in
   `task up:verify` (scenario 2), eseguito dall'host con `curl` — quindi la
   verifica HTTP richiesta dalla SPEC esiste comunque, solo non nel
   container healthcheck nativo.

5. **Script `up.sh`/`verify.sh` invece di comandi inline nel Taskfile**: la
   logica di poll-con-timeout (`task up`) e i controlli per-servizio con
   report esplicito (`task up:verify`) sono script bash dedicati in
   `deploy/compose/` (non file toccati fuori dal perimetro assegnato:
   `deploy/compose`, `Taskfile.yml`, questa SPEC), richiamati da `Taskfile.yml`
   con `bash deploy/compose/<script>.sh` — stessa scelta già fatta per
   `scripts/task-*.sh` nel resto del repo, qui dentro `deploy/compose/`
   per restare nel perimetro.

6. **Credenziali usate nelle verifiche di `task db:migrate`/`task
   db:neo4j:migrate` contro questo stack (scenario 5)**: i default
   hardcoded nel Taskfile (`POSTGRES_URL` di SPEC-005) non coincidono con
   le credenziali dev di questo compose (`eci-dev-only` vs `eci`) — testato
   passando esplicitamente `POSTGRES_URL=postgres://eci:eci-dev-only@localhost:5432/eci?sslmode=disable`
   e `NEO4J_USER=neo4j NEO4J_PASSWORD=eci-dev-only` come override, nessuna
   modifica ai default di SPEC-005 (restano corretti per l'uso con
   testcontainers, che non hanno queste credenziali). Da tenere a mente
   quando si scriverà il README/onboarding di uso quotidiano dello stack.
