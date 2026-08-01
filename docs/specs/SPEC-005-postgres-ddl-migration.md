# SPEC-005 — DDL PostgreSQL: code_node, code_relation, outbox, processed_events
Stato: implemented
Task-tree: T0.5 · Servizio: contracts/sql + migration runner (golang-migrate) · ADD: Modulo 1 — §2.2.1 (outbox pattern), §2.2.4 (processed_events), D2 (entità)
Contratti: contracts/jsonschema/hybrid-graph.json, contracts/jsonschema/outbox-event.json (letti come riferimento, non modificati)

## 1. Obiettivo
Definire lo schema PostgreSQL 17 delle quattro tabelle canoniche (source of truth `code_node`/`code_relation`, più `outbox` e `processed_events` del pattern CDC), gestite da migration versionate con **golang-migrate** (CLI standard, file `.up.sql`/`.down.sql` numerati — scelto perché language-agnostic all'esecuzione, coerente con un monorepo multi-linguaggio dove Go non è l'unico consumer di Postgres).

## 2. Interfaccia

Struttura: `contracts/sql/migrations/0001_init.up.sql` + `0001_init.down.sql`. Target Taskfile: `task db:migrate` (up), `task db:migrate:down` (rollback ultima migration), `task db:migrate:new NAME=<slug>` (crea coppia di file numerata vuota).

DDL di `0001_init.up.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- per gen_random_uuid()

CREATE TABLE code_node (
  id              TEXT PRIMARY KEY,
  domain          TEXT NOT NULL CHECK (domain IN ('code','doc','legal','compliance','contract')),
  node_type       TEXT NOT NULL,
  name            TEXT NOT NULL,
  ast_hash        CHAR(64) NOT NULL,
  content_hash    CHAR(64),
  embedding_ref   JSONB,
  provenance      JSONB NOT NULL,
  version         INTEGER NOT NULL DEFAULT 1,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  valid_from      TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to        TIMESTAMPTZ,
  supersedes      TEXT REFERENCES code_node(id),
  ext             JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_code_node_ast_hash ON code_node(ast_hash);
CREATE INDEX idx_code_node_domain   ON code_node(domain);

CREATE TABLE code_relation (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  domain      TEXT NOT NULL CHECK (domain IN ('code','doc','legal','compliance','contract')),
  rel_type    TEXT NOT NULL CHECK (rel_type IN (
                'CALLS','IMPORTS','EXTENDS','IMPLEMENTS','CONTAINS','DEPENDS_ON',
                'REFERENCES','OVERRIDES','DERIVED_FROM','GOVERNED_BY','CITES')),
  from_id     TEXT NOT NULL REFERENCES code_node(id),
  to_id       TEXT NOT NULL REFERENCES code_node(id),
  weight      NUMERIC,
  provenance  JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_code_relation_from ON code_relation(from_id);
CREATE INDEX idx_code_relation_to   ON code_relation(to_id);

CREATE TABLE outbox (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type  TEXT NOT NULL CHECK (aggregate_type IN ('CodeNode','CodeRelation')),
  aggregate_id    TEXT NOT NULL,
  event_type      TEXT NOT NULL CHECK (event_type IN ('UPSERT','DELETE')),
  payload         JSONB NOT NULL,
  trace_id        TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_created_at ON outbox(created_at);

CREATE TABLE processed_events (
  event_id       UUID PRIMARY KEY,
  consumer_name  TEXT NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`0001_init.down.sql`: `DROP TABLE` nell'ordine inverso (`processed_events`, `outbox`, `code_relation`, `code_node`), poi `DROP EXTENSION IF EXISTS pgcrypto`.

## 3. Comportamento (scenari)

1. **Dato** un Postgres 17 vuoto, **quando** eseguo `task db:migrate`, **allora** le 4 tabelle esistono con i vincoli sopra (verifica via `\d+ code_node` ecc. o query su `information_schema`).
2. **Dato** il DB migrato, **quando** eseguo `task db:migrate:down`, **allora** tutte le tabelle vengono rimosse senza errori.
3. **Dato** il DB migrato, **quando** inserisco un `code_node` con `domain='invalid'`, **allora** l'INSERT fallisce per violazione del CHECK constraint.
4. **Dato** il DB migrato, **quando** eseguo in un'unica transazione un INSERT su `code_node` seguito da un INSERT su `outbox` con lo stesso `aggregate_id`, poi forzo un errore prima del COMMIT (es. un secondo INSERT che viola un vincolo), **allora** dopo il ROLLBACK nessuna delle due righe è presente (atomicità verificata).
5. **Dato** un `code_relation` che referenzia un `from_id` inesistente in `code_node`, **quando** tento l'INSERT, **allora** fallisce per violazione della foreign key.
6. **Dato** un evento con `event_id` già presente in `processed_events`, **quando** un consumer tenta di re-inserirlo, **allora** l'INSERT fallisce per violazione della PK (questo è il meccanismo di dedup che i sink delle fasi successive useranno: check-then-skip, non gestito qui oltre al vincolo).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `pgcrypto` non disponibile nell'immagine Postgres usata | la migration fallisce con errore esplicito all'`CREATE EXTENSION`, non un errore criptico più a valle su `gen_random_uuid()` |
| Migration `0001` già applicata, si tenta di riapplicarla | `golang-migrate` la marca come no-op tramite la tabella di versioning che crea automaticamente (`schema_migrations`) — comportamento di libreria, da verificare in test, non da reimplementare |
| `code_node.supersedes` punta a un id che verrà inserito solo dopo (ordine di inserimento) | documentare che l'applicazione deve inserire in ordine (il nodo precedente prima del successore) o rimandare il collegamento con un UPDATE — nessuna azione richiesta nello schema oltre alla FK |

## 5. Non-goals
Non implementare in questo task la logica applicativa che scrive nella stessa transazione entità+outbox (arriva in T1.2, servizio Ingestion). Non implementare ancora il consumo/dedup lato sink (T1.3+). Nessun indice oltre a quelli elencati: altri si aggiungono quando un caso d'uso concreto li richiede (evitare over-indexing prematuro).

## 6. Vincoli dall'ADD
Campi e vincoli derivano da D2 (entità `CodeNode`/`CodeRelation`) e dal pattern outbox del Modulo 1 §2.2.1 (l'`INSERT INTO outbox` di riferimento nell'ADD ha esattamente i campi `id, aggregate_type, aggregate_id, event_type, payload, created_at` — `trace_id` è un'aggiunta di coerenza con l'envelope evento di SPEC-003, da annotare nel commit). `processed_events` implementa la dedup idempotente richiesta in §2.2.4.

## 7. Test plan
- Integration (testcontainers, immagine `postgres:17`): applica `up`, esegue gli scenari 1/3/4/5/6, applica `down`, verifica tabelle assenti.
- Test di parità: uno script valida che i campi di `code_node`/`code_relation` coprano tutti i campi "common" di `hybrid-graph.json` (nessun campo dello schema JSON orfano nello schema SQL).

## 8. Osservabilità
N/A per lo schema puro. Riservare per una fase successiva (T3.3) una vista `outbox_unprocessed_count` per il lag monitoring — non crearla qui (scope creep rispetto a questo task).

## 9. Criteri di accettazione
- [x] `task db:migrate` crea le 4 tabelle con i vincoli esatti di §2 — verificato in `TestPostgresDDLMigration` contro `postgres:17` reale (testcontainers), invocando il vero CLI `migrate` (stesso comando di `task db:migrate`).
- [x] `task db:migrate:down` rimuove tutto senza errori — scenario 2, verificato nello stesso test (`assertTablesAbsent` dopo `migrate ... down 1`).
- [x] Scenari 3, 4, 5, 6 (constraint, atomicità, FK, dedup PK) verdi in test di integrazione — tutti passano come subtest di `TestPostgresDDLMigration` (`tests/integration/postgres_ddl/migration_test.go`).
- [x] Nessun campo di D2 "common" privo di colonna corrispondente in `code_node`/`code_relation` — `TestSQLColumnsCoverJSONSchemaFields` (`tests/integration/postgres_ddl/parity_test.go`), verde. Vedi §10.2 per come "campi comuni" è stato interpretato per `code_relation`.

## 10. Deviazioni rispetto alla SPEC

1. **`POSTGRES_URL` non specificato dalla SPEC**: §2 non definisce come il
   Taskfile debba ricevere la connection string. Ho introdotto la variabile
   d'ambiente/Task `POSTGRES_URL`, default
   `postgres://eci:eci@localhost:5432/eci?sslmode=disable`, sullo stesso
   pattern di `NEO4J_URI` in `tools/migrate-neo4j` (SPEC-004). Usata da
   `db:migrate` e `db:migrate:down`.

2. **Interpretazione di "campi comuni" per il test di parità (§7)**:
   `hybrid-graph.json` include `versioning` come proprietà opzionale (non
   `required`) anche su `CodeRelation`, ma il DDL letterale di §2 non ha
   colonne di versioning su `code_relation` (solo `created_at`) — le
   relazioni non sono versionate in D2, solo i nodi lo sono. Il test di
   parità (`parity_test.go`) tratta questo come un'esclusione esplicita e
   documentata (`codeRelationSkippedFields["versioning"] = true`), non come
   un'omissione silenziosa: se in futuro `hybrid-graph.json` aggiunge un
   nuovo campo a `CodeNode`/`CodeRelation`, il test fallisce finché la
   mappa campo→colonna non viene aggiornata esplicitamente. Per tutti gli
   altri campi (inclusi quelli opzionali come `content_hash`, `weight`), la
   copertura è verificata 1:1, con `versioning` flattato in
   `version/is_current/valid_from/valid_to/supersedes` per `code_node` ed
   `embedding` mappato a `embedding_ref`.

3. **Test di integrazione invoca il CLI `migrate` reale via `exec.Command`**,
   non la libreria `golang-migrate` in-process: stessa scelta già fatta per
   coerenza con `task db:migrate`/`task db:migrate:down`, che sono wrapper
   diretti del CLI — il test verifica quindi esattamente il comando che un
   operatore eseguirebbe, non un path alternativo. Richiede il binario
   `migrate` sul PATH (verificato a inizio sessione, vedi richiesta
   dell'utente).

4. **Modulo Go indipendente in `tests/integration/postgres_ddl`** (proprio
   `go.mod`), non dentro `tools/` — il perimetro file assegnato per questa
   sessione è `contracts/sql`, `Taskfile.yml`, `scripts/`, `tests/`, quindi
   il codice di test vive sotto `tests/integration/` seguendo lo stesso
   pattern di modulo autonomo di `tools/migrate-neo4j` (SPEC-004), wired a
   mano in `Taskfile.yml` (`lint`, `test`, `test:integration`) sullo stesso
   modello già in uso per `tools/migrate-neo4j`.

5. **`0001_init.down.sql`**: la SPEC ne descrive il contenuto in prosa (§2,
   ultima riga) ma non lo dà come blocco SQL letterale come per `up.sql`.
   Scritto come descritto: `DROP TABLE` in ordine inverso
   (`processed_events, outbox, code_relation, code_node`), poi
   `DROP EXTENSION IF EXISTS pgcrypto`.

6. **`task db:migrate:new`**: implementato (`migrate create -ext sql -dir
   contracts/sql/migrations -seq {{.NAME}}`) anche se non compare
   esplicitamente nei criteri di accettazione §9 — è nell'interfaccia §2 e
   non richiede stato/scenari da testare (comando di scaffolding, non
   comportamento del DDL).
