# SPEC-014 — Scrittura PostgreSQL: transazione ACID + outbox (T1.2)
Stato: verified
Task-tree: T1.2 (secondo task di Fase 1) · Servizio: services/ingestion (Rust, estende T1.1) · ADD: Modulo 1 §2.2.1 (pattern outbox, Postgres come sorgente di verità)
Contratti: contracts/sql/migrations/0001_init.up.sql (schema reale, letto — non modificato), contracts/jsonschema/hybrid-graph.json (D2, forma del payload outbox)

## 1. Obiettivo
Estendere `services/ingestion` (T1.1 già popolato) con una funzione che prende l'output di `parse_file` — `Vec<CodeNode>`, `Vec<CodeRelation>` in memoria — e lo persiste su PostgreSQL dentro un'unica transazione ACID: upsert idempotente dei nodi, sostituzione (delete+insert) delle relazioni emananti dai nodi di questo file, e una riga `outbox` per ciascuna entità toccata. Prima applicazione reale con dati concreti del pattern outbox descritto nell'ADD e già verificato end-to-end (schema vuoto) fino a SPEC-008.

## 2. Interfaccia

Nuova funzione in `services/ingestion/src/`:
```rust
pub fn persist_parsed_file(
    client: &mut postgres::Client,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
) -> Result<PersistSummary, PersistError>;

pub struct PersistSummary {
    pub nodes_upserted: usize,
    pub relations_replaced: usize,
    pub outbox_rows_written: usize,
}
```
Dipendenza: crate `postgres` (client sincrono — **scelta dichiarata**: `services/ingestion` a questo stadio è un processo batch/CLI che elabora un file alla volta, non un servizio che necessita di concorrenza async; introdurre `tokio-postgres`/`sqlx` e un intero runtime tokio non è giustificato per questo caso d'uso). Connessione via DSN da `eci-common::config::env_or_default` (SPEC-010), stesso pattern già stabilito.

**Upsert `code_node`** (`id` deterministico da T1.1 rende `ON CONFLICT` diretto):
```sql
INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
VALUES ($1, 'code', $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE SET
  node_type = EXCLUDED.node_type, name = EXCLUDED.name,
  ast_hash = EXCLUDED.ast_hash, updated_at = now();
```

**Sostituzione `code_relation`** — **nessun `ON CONFLICT`**: verificato contro `contracts/sql/migrations/0001_init.up.sql` che `code_relation` ha solo un `id UUID` surrogato, nessun vincolo di unicità su `(domain, rel_type, from_id, to_id)`. Un `INSERT` ripetuto duplicherebbe righe; un `ON CONFLICT` non ha un target su cui agganciarsi. Pattern invece:
```sql
DELETE FROM code_relation WHERE domain = 'code' AND from_id = ANY($file_node_ids);
-- poi, per ogni CodeRelation:
INSERT INTO code_relation (domain, rel_type, from_id, to_id, weight)
VALUES ('code', $1, $2, $3, $4);
```
`$file_node_ids` = gli id di TUTTI i nodi prodotti da questo parsing (non solo i Method/Function — anche il `File` stesso, dato che le sue `CONTAINS` hanno `from_id` = id del File). Lo scope del DELETE è deliberatamente solo `from_id` (mai `to_id`): una relazione con `to_id` in questo file ma `from_id` altrove non è mai stata prodotta da T1.1 (intra-file only, SPEC-013 §5) e non deve essere toccata da questa operazione — preserva la correttezza anche quando in futuro (Fase 2) esisteranno relazioni cross-file.

**Riga `outbox` per ogni entità toccata** (upsert di nodo, o insert di relazione): `aggregate_type` = `'CodeNode'`/`'CodeRelation'` secondo lo schema di `contracts/sql/migrations`, `aggregate_id` = l'`id` del nodo o l'`id` UUID generato per la relazione, `event_type` = `'UPSERT'` sempre in questa SPEC (nessuna `DELETE` — vedi §5), `payload` = rappresentazione JSON dell'entità conforme a D2 (`CodeNode`/`CodeRelation` di `hybrid-graph.json`), `trace_id` = `eci_common::observability::current_trace_id_hex()` (SPEC-010 — prima applicazione reale di questa funzione, finora usata solo nei suoi stessi test).

Tutto — upsert nodi, delete+insert relazioni, insert righe outbox — dentro **una singola transazione**: `client.transaction()?` all'inizio, `tx.commit()?` solo se tutte le operazioni riescono; qualunque errore intermedio propaga e la transazione va in rollback automaticamente (drop implicito senza commit).

## 3. Comportamento (scenari)

1. **Dato** l'output di `parse_file("order_service.go", ...)` (4 nodi, 1 relazione, da SPEC-013), **quando** chiamo `persist_parsed_file` la prima volta su un DB vuoto, **allora** `PersistSummary{nodes_upserted: 4, relations_replaced: 1, outbox_rows_written: 5}`, verificabile anche con query dirette (`SELECT count(*) FROM code_node`, `code_relation`, `outbox`).
2. **Dato** lo stesso file, **quando** chiamo `persist_parsed_file` una SECONDA volta con lo STESSO output (nessuna modifica al sorgente), **allora** `code_node` resta a 4 righe (stessi `id`, `updated_at` cambiato — nessun duplicato); `code_relation` resta a 1 riga logicamente identica ma fisicamente nuova (nuovo `id` UUID, dato che non c'è un vincolo naturale — comportamento accettato, non un difetto, vedi §4); 5 nuove righe `outbox` (ogni run produce eventi, nessun rilevamento "nessun cambiamento" a questo stadio — quello è T2.1, vedi §5).
3. **Dato** una seconda versione del sorgente in cui `Process` non chiama più `Validate` (chiamata rimossa), **quando** riparso e persisto, **allora** l'arco `CALLS` precedente non esiste più in `code_relation` — nessuna riga orfana, grazie al DELETE prima dell'INSERT.
4. **Dato** un errore forzato a metà transazione (es. violazione FK simulata), **quando** l'operazione fallisce, **allora** né `code_node` né `code_relation` né `outbox` riportano modifiche parziali — rollback completo verificato con query dirette dopo il fallimento.
5. **Dato** `persist_parsed_file` chiamato dentro uno span OTel attivo (aperto da T1.1 attorno a `parse_file`, o un nuovo span sibling — da decidere in implementazione), **quando** ispeziono le righe `outbox` prodotte, **allora** tutte condividono lo stesso `trace_id` non-NULL di quello span.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `code_relation` non ha vincolo di unicità naturale — righe fisicamente duplicate a livello di `id` UUID per la stessa relazione logica su run ripetuti | Comportamento accettato per questa SPEC (vedi scenario 2): il `DELETE` prima dell'`INSERT` garantisce che al termine di OGNI transazione esista esattamente una riga per relazione logica corrente — la storia di `id` UUID passati non è recuperabile, ma non è un requisito di questa SPEC |
| Nessuno span OTel attivo al momento della chiamata (`current_trace_id_hex()` ritorna `None`) | `outbox.trace_id` = `NULL` — comportamento esplicitamente previsto dallo schema (`trace_id TEXT`, nullable), non un errore |
| Connessione al DB fallita o persa a metà transazione | Propagare l'errore esplicitamente (`PersistError`), fail-fast — nessun retry automatico in questa SPEC (walking skeleton, vedi §5) |
| `$file_node_ids` vuoto (caso limite: un file che T1.1 riduce a solo il nodo `File` stesso, zero altre entità) | Il `DELETE ... WHERE from_id = ANY($file_node_ids)` con un solo id (quello del File) è comunque corretto — cancella solo eventuali `CONTAINS` residue dal File verso entità non più presenti, non richiede un caso speciale |

## 5. Non-goals
Nessun batch multi-file in questa SPEC — un file alla volta; l'orchestrazione su un intero commit (più file) è oltre lo scope di T1.2. Nessuna gestione di file rimossi dal repository (tombstone su un intero File scomparso — richiede sapere che un file non esiste più, non solo riparsarlo: probabilmente Fase 2). Nessun rilevamento "nessun cambiamento reale" per evitare outbox ridondanti su run ripetuti senza modifiche al sorgente — quella è esplicitamente la responsabilità del delta indexing basato su Merkle hashing di T2.1, non anticipata qui. Nessun retry/backoff su errori di connessione DB.

## 6. Vincoli dall'ADD
Modulo 1 §2.2.1: Postgres come sola sorgente di verità, scrittura ACID che include la riga outbox nella STESSA transazione dei dati di dominio (garanzia dual-write risolta strutturalmente, non con un pattern 2PC) — questa SPEC è la prima implementazione concreta di quella garanzia con un vero cammino di dati (parsing → persistenza), non solo lo schema DDL già verificato in SPEC-005.

## 7. Test plan
Test di integrazione con testcontainers (`postgres:17`, stesso pattern di SPEC-005/SPEC-008), migration reali applicate prima del test. Scenari 1-5 verificati con query dirette sul DB dopo ogni operazione (conteggi esatti, non solo "non vuoto"). Scenario 4 richiede un modo deliberato di forzare un fallimento a metà transazione (es. un batch di relazioni che include un `to_id` inesistente, violando la FK) per verificare il rollback.

## 8. Osservabilità
Uso di `eci_common::observability::current_trace_id_hex()` (SPEC-010) per popolare `outbox.trace_id` — prima applicazione reale di questa funzione al di fuori dei suoi stessi test. Uno span (nuovo o esteso da quello di `parse_file`, da decidere in implementazione) attorno all'intera operazione di persistenza.

## 9. Criteri di accettazione
- [x] Scenario 1: conteggi esatti su DB vuoto (4 nodi, **4 relazioni** — non 1, vedi §10 deviazione #1 —, 8 righe outbox, non 5) verificati con query dirette.
- [x] Scenario 2: nessun duplicato in `code_node` su run ripetuto, `code_relation` sostituita correttamente (non accumulata — stesso insieme logico `(rel_type, from_id, to_id)`, id UUID fisicamente nuovi verificato via query diretta).
- [x] Scenario 3: rimozione di una relazione dal sorgente si riflette come rimozione reale in `code_relation`, non un arco orfano (verificato: 3 righe CONTAINS restano, zero CALLS).
- [x] Scenario 4: rollback completo verificato su un fallimento forzato a metà transazione (FK reale su `to_id`, non simulata) — nessuna riga residua in nessuna delle tre tabelle.
- [x] Scenario 5: `trace_id` propagato correttamente nelle righe outbox prodotte (stesso valore non-NULL su tutte le righe di una stessa chiamata).
- [x] Test di integrazione verdi, rieseguiti due volte di seguito senza flakiness (equivalente Rust di `-count=1`: `cargo` non cachea l'esito dei test come `go test`, ma ogni container Postgres è nuovo ad ogni esecuzione — nessuno stato residuo possibile tra run).

## 10. Deviazioni rispetto alla SPEC

1. **§3 scenario 1 dichiara conteggi sbagliati per `parse_file("order_service.go", ...)`**:
   il testo diceva "4 nodi, 1 relazione" (quindi `PersistSummary{relations_replaced: 1}`,
   5 righe outbox). Verificato eseguendo `parse_file` reale che l'output è
   **4 nodi E 4 relazioni** (3 `CONTAINS` + 1 `CALLS`), coerente con
   `services/ingestion/src/lib.rs` (`scenario1_order_service_nodes_and_contains`,
   già "verified" in SPEC-013, attesta esplicitamente `contains.len() == 3`;
   `scenario2_order_service_calls_process_to_validate` attesta 1 `CALLS`). "1
   relazione" era quindi un refuso di questa SPEC (con ogni probabilità un
   'eco' della frase "1 arco CALLS" di SPEC-013 §3 scenario 2, letta perdendo
   di vista le 3 `CONTAINS`), non un comportamento da riprodurre: implementare
   `persist_parsed_file` per produrre `relations_replaced: 1` a fronte di un
   input di 4 `CodeRelation` reali sarebbe stato un bug (conteggio falso),
   non fedeltà alla SPEC. Corretto nei test/criteri di accettazione qui sopra
   con i valori reali (4 relazioni, 8 righe outbox = 4 CodeNode + 4
   CodeRelation). Non è un conflitto con l'ADD o un contratto (CLAUDE.md
   regola 6): è un'inconsistenza interna della SPEC contro un comportamento
   già verificato a monte, risolta usando il comportamento verificato,
   annotata qui invece di corretta silenziosamente.

2. **Provenance (`code_node.provenance`, JSONB NOT NULL) minimale, non conforme
   ai campi `required` di D2** (`repo`, `commit_sha`, `path`, `ingested_at`
   in `contracts/jsonschema/hybrid-graph.json`, definizione `provenance`):
   popolato con **solo `path`** (`{"path": file_path}`). `repo`/`commit_sha`
   non sono ricavabili dall'interfaccia di `persist_parsed_file` così come
   specificata in §2 (prende `Vec<CodeNode>`/`Vec<CodeRelation>`, nessun
   parametro di contesto repo/commit) — e da `CodeNode` stesso (SPEC-013),
   che porta solo `file_path`. `ingested_at` omesso per lo stesso motivo di
   onestà: fabbricare un timestamp client-side avrebbe richiesto una
   dipendenza aggiuntiva (`chrono`/`time`) solo per un valore che
   `outbox.created_at` (colonna `TIMESTAMPTZ DEFAULT now()`, già presente)
   cattura comunque in modo autoritativo per le righe outbox. La tabella SQL
   non impone il JSON Schema di D2 a livello di CHECK constraint (solo
   `NOT NULL`), quindi questa scelta non causa errori a runtime — ma il
   payload prodotto NON supererebbe una validazione stretta contro
   `hybrid-graph.json`. Provenance completa (repo/commit awareness) è
   presumibilmente materia di un'orchestrazione a livello di commit in Fase
   2, non di questo T1.2 file-per-file.

3. **Dipendenze aggiunte, versioni esatte risolte** (nessuna era già
   presente in `services/ingestion/Cargo.toml`):
   - `postgres = "0.19.14"` (feature `with-serde_json-1`) — client sincrono,
     come richiesto esplicitamente da §2. Trascina `tokio-postgres = "0.7.18"`,
     `postgres-types = "0.2.14"`.
   - `serde_json = "1.0.151"` — costruzione dei payload JSONB (`provenance`,
     `outbox.payload`).
   - `rust_decimal = "1.42.1"` (feature `db-postgres`) — necessario per
     legare `CodeRelation.weight` (`Option<u32>`) alla colonna
     `code_relation.weight NUMERIC`: verificato contro il sorgente di
     `postgres-types`/`tokio-postgres` che un intero Rust (`i32`/`i64`) NON
     si lega direttamente a un parametro NUMERIC (mismatch di OID rilevato
     da `ToSql::accepts` a runtime — nessun cast implicito lato client);
     `Decimal` implementa `ToSql` per l'OID numeric esatto. Alternativa
     scartata: cast SQL `$N::numeric` su un parametro intero — stesso
     problema di risoluzione del tipo lato server per un parametro non
     tipato esplicitamente dal client, non affidabile senza verificarlo
     contro il protocollo esteso, mentre `rust_decimal` è una soluzione
     diretta e già usata in produzione dall'ecosistema `postgres`/
     `tokio-postgres`.
   - `testcontainers = "0.27.3"` (feature `blocking`, dev-dependency) e
     `testcontainers-modules = "0.15.0"` (feature `postgres`,
     dev-dependency) — `blocking` necessaria esplicitamente per
     `testcontainers::runners::SyncRunner` (verificato contro il sorgente:
     il modulo `runners::sync_runner` è dietro `#[cfg(feature = "blocking")]`,
     non abilitato di default). Il modulo `postgres` di
     `testcontainers-modules` usa il tag immagine `11-alpine` di default —
     sovrascritto esplicitamente a `17` con `ImageExt::with_tag("17")` per
     restare coerenti con `postgres:17` di SPEC-005/SPEC-008 (nessun
     parametro dedicato nel modulo per la versione, solo l'override generico
     di `ImageExt`).

4. **Test di integrazione gated con `#[ignore]`, non con un cargo feature
   dedicato**: Go usa un build tag (`-tags=integration`) per escludere questi
   test da una compilazione/esecuzione di default; Cargo non ha un analogo
   diretto applicabile qui, perché i `[dev-dependencies]` non supportano
   `optional`/feature-gating (solo le dipendenze normali in `[dependencies]`
   possono esserlo) — non è possibile evitare che `testcontainers`/
   `testcontainers-modules` vengano COMPILATI da un `cargo test` semplice
   senza spostare l'intero file di test dietro un cargo feature complesso e
   non idiomatico. La proprietà che conta per `task test`/SPEC-001 §4 è che
   NON vengano ESEGUITI (quindi nessuna dipendenza da Docker per `task
   test`) — `#[ignore]` la garantisce, verificato (`cargo test` semplice:
   "2 ignored", nessun tentativo di connessione Docker). Eseguiti
   manualmente con `cargo test --test persist_integration_test -- --ignored
   --test-threads=1`, verdi, rieseguiti due volte di seguito.

5. **Nuovo modulo `src/persist.rs`** (non aggiunto direttamente a `lib.rs`
   accanto a `parse_file`): `persist_parsed_file`/`PersistSummary`/
   `PersistError` vivono in un modulo dedicato, ri-esportati a livello di
   crate (`pub use persist::{persist_parsed_file, PersistError,
   PersistSummary};`) così l'interfaccia pubblica resta esattamente quella
   di §2 (`ingestion::persist_parsed_file(...)`) nonostante la separazione
   fisica del file.

6. **Wired in `Taskfile.yml`** (aggiunto in una sessione successiva,
   esplicitamente richiesta dall'utente — il perimetro file iniziale non
   includeva `Taskfile.yml`, poi ampliato): `task lint` esegue
   `cd services/ingestion && cargo clippy --test persist_integration_test`
   (riga dedicata, stesso pattern di `go vet -tags=integration ./...` per
   `tests/integration/postgres_ddl`/`outbox_cdc` — anche se qui è in
   parte ridondante con `cargo clippy --all-targets` già eseguito da
   `scripts/task-lint.sh` per `services/ingestion`, dato che i test di
   integrazione Rust vivono nello STESSO crate, non in un modulo Go
   separato come `tests/integration/*`: `--all-targets` li copre già
   implicitamente. La riga dedicata resta comunque utile per la visibilità
   esplicita in `task lint`, come richiesto). `task test:integration`
   esegue `cd services/ingestion && cargo test --test
   persist_integration_test -- --ignored --test-threads=1` — entrambe
   verificate con un'esecuzione reale di `task lint`/`task test:integration`
   (quest'ultima con Docker disponibile: 2 test eseguiti realmente, non
   solo compilati, entrambi verdi).
