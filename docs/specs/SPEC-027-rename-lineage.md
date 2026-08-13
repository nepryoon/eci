# SPEC-027 — Rename/move detection + lineage (T2.6, parte 1/2)
Stato: implemented
Task-tree: T2.6 (primo dei due sotto-task concordati — GC di outbox/processed_events è la parte 2/2, separata) · Servizio: services/ingestion (Rust, nuovo modulo + nuova migrazione) · ADD: Modulo 1 §1.6.4-1.6.6
Contratti: nuova migrazione `contracts/sql/migrations/000N_lineage.up.sql` (nuova tabella, additiva — nessun ADR richiesto per un'aggiunta, stesso principio già confermato in SPEC-026 §10 per `rel_type`)

## 0. Nota architetturale (discussa e concordata con l'utente prima di questa SPEC)
Il nostro `id` (T1.1, `sha256_hex(file_path:qualified_name)`) include il path — un rename cambia l'id di ogni entità nel file anche se il contenuto, e quindi `ast_hash`, resta identico. Questo è in tensione diretta con la proprietà che l'ADD promette per il rename ("cache hit totale... la chiave di cache primaria è l'hash del sottoalbero", §1.6.5): il NOSTRO schema di id non è puramente content-addressed, lo è solo `ast_hash`. Decisione: non toccare la formula dell'id (romperebbe T1.2/T1.3/T1.4/T1.5, che la usano già in produzione) — costruire invece una tabella di lineage separata, popolata quando un rename viene rilevato, che collega esplicitamente vecchio id → nuovo id via un match su `ast_hash` condiviso. La proprietà "cache hit su rename" dell'ADD si ottiene così a un livello logico sopra l'id, non cambiando l'id stesso.

## 1. Obiettivo
Rilevare, tra due commit git, i file rinominati/spostati (`git diff -M`) e, per ciascun rename, riconoscere quali entità della nuova versione del file sono logicamente le STESSE di quelle della vecchia versione (stesso `ast_hash`, path diverso) — registrando il collegamento in una tabella di lineage dedicata, senza toccare lo schema di id esistente.

## 2. Interfaccia

**Nuova tabella** (`contracts/sql/migrations/000N_lineage.up.sql`):
```sql
CREATE TABLE lineage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    old_node_id CHAR(64) NOT NULL,
    new_node_id CHAR(64) NOT NULL,
    ast_hash CHAR(64) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason TEXT NOT NULL DEFAULT 'rename'
);
CREATE INDEX idx_lineage_old_node_id ON lineage(old_node_id);
CREATE INDEX idx_lineage_new_node_id ON lineage(new_node_id);
```
`reason` con default esplicito, non un enum vincolato: questa SPEC produce solo `'rename'`, ma il campo resta aperto per estensioni future (es. move-tra-file, fuori scope qui — vedi §5) senza richiedere un'altra migrazione.

**Rilevamento rename** (`services/ingestion/src/lineage.rs`, nuovo modulo):
```rust
pub struct RenamedFile {
    pub old_path: String,
    pub new_path: String,
    pub similarity_percent: u8,
}

pub fn detect_renames(old_ref: &str, new_ref: &str, repo_dir: &Path) -> Result<Vec<RenamedFile>, LineageError>;
```
Shell-out a `git diff -M --name-status {old_ref} {new_ref}` (non una libreria Rust per git — l'algoritmo di rilevamento rename di git stesso è già maturo e testato, reimplementarlo non aggiunge valore). Parsing dell'output: righe `R{score}\t{old_path}\t{new_path}` (es. `R100\told.go\tnew.go`) diventano un `RenamedFile`; righe con altri prefissi (`M`, `A`, `D`, ...) ignorate — questa SPEC si occupa solo di rename, non di altri tipi di change.

**Riconciliazione lineage** (stessa funzione o una gemella diretta):
```rust
pub fn reconcile_renamed_file(
    renamed: &RenamedFile,
    old_entities: &[(String, String)],  // (id, ast_hash) esistenti a old_path, da query su code_node
    new_entities: &[CodeNode],          // già estratti da parse_X_file su new_path
) -> Vec<LineageLink>;  // { old_node_id, new_node_id, ast_hash }
```
Match **greedy per `ast_hash` uguale**, non per nome/posizione — un'entità nuova il cui hash combacia con un'entità vecchia (non ancora rivendicata da un match precedente nello stesso rename) produce un `LineageLink`. **Ambiguità dichiarata** (§4): se più entità vecchie condividono lo stesso `ast_hash` (es. due funzioni genuinamente identiche) e più nuove pure, l'accoppiamento è arbitrario tra i candidati non ancora rivendicati — nessun tentativo di disambiguare per nome o posizione in questa SPEC.

**Persistenza**: righe `lineage` inserite in una singola transazione per rename (stesso principio transazionale di `persist_parsed_file`, T1.2) — o tutte le righe di un rename sono scritte, o nessuna.

## 3. Comportamento (scenari)

1. **Dato** un repo git con un file rinominato SENZA modifiche di contenuto tra due commit, **quando** eseguo `detect_renames`, **allora** ottengo un `RenamedFile` con `similarity_percent: 100`.
2. **Dato** lo stesso rename, **quando** eseguo `reconcile_renamed_file` con le entità vecchie (da una precedente estrazione simulata) e quelle nuove (da `parse_go_file`/`parse_js_file`/`parse_ts_file` sul nuovo path), **allora** ottengo un `LineageLink` per OGNI entità del file — tutte, dato che nessun contenuto è cambiato.
3. **Dato** un file rinominato CON una piccola modifica di contenuto (una funzione il cui corpo è cambiato), **quando** riconcilio, **allora** ottengo un `LineageLink` per le entità INVARIATE, ma NESSUNO per quella modificata — il suo `ast_hash` non combacia con nessuna entità vecchia, correttamente trattata come nuovo contenuto, non collegata.
4. **Dato** un file modificato SUL POSTO (nessun rename, stesso path, `git diff -M` lo riporta come `M`), **quando** eseguo `detect_renames`, **allora** quel file NON compare nell'elenco dei `RenamedFile` — questa SPEC non tocca file non rinominati.
5. **Dato** due entità vecchie con lo stesso `ast_hash` (contenuto genuinamente identico) in un rename dove esistono anche due nuove entità con quello stesso hash, **quando** riconcilio, **allora** ottengo comunque due `LineageLink` (un accoppiamento valido, anche se l'esatto assegnamento tra le due coppie è arbitrario) — non un errore, non un collegamento perso.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `old_ref`/`new_ref` non validi (commit inesistente) | Errore esplicito da `detect_renames` (propagato dall'exit code non-zero di `git diff`), non un elenco vuoto silenzioso |
| Un'entità nuova il cui `ast_hash` non combacia con NESSUNA entità vecchia (contenuto genuinamente nuovo, non solo rinominato) | Nessun `LineageLink` prodotto per lei — comportamento corretto, non un caso da segnalare come errore |
| Ambiguità di match (§2, più coppie con lo stesso hash) | Accoppiamento arbitrario tra i candidati, mai un panic, mai un collegamento duplicato verso la stessa entità vecchia due volte |
| Spostamento di codice TRA due file diversi (una funzione si sposta da A a B, nessuno dei due file è "rinominato" nel senso di git diff -M) | Fuori scope dichiarato di questa SPEC (§5) — `git diff -M` è per rename di file interi, non rileva in modo affidabile un move parziale di contenuto tra file distinti |

## 5. Non-goals
Move di codice TRA file diversi (solo rename di file interi, rilevato da `git diff -M` — §4). Nessun aggancio automatico a un webhook/commit-hook (§1.6.6 dell'ADD lo menziona come possibilità futura — questa SPEC espone `detect_renames`/`reconcile_renamed_file` come funzioni chiamabili, l'invocazione automatica su ogni push è fuori scope). Nessuna garbage collection delle voci di cache orfane (dipende da questa lineage ma è la parte 2/2 concordata, o oltre — nessun consumatore reale della Semantic Cache ancora esistente). Nessuna disambiguazione sofisticata per hash duplicati (§2/§4, dichiarata).

## 6. Vincoli dall'ADD
§1.6.5: `git diff -M` per il rilevamento, content-addressing per il cache hit — realizzato qui tramite la tabella di lineage separata (vedi §0), non modificando l'id. §1.6.4: la lineage tracking come base per l'invalidazione selettiva — questa SPEC costruisce la tabella e la logica di popolamento, non ancora un consumatore che la usi per invalidare (nessun consumatore reale del cache esiste ancora, stesso principio di T2.2/T2.3).

## 7. Test plan
`detect_renames`: test contro un repo git reale creato al volo nel test (init in una directory temporanea, commit, rename, commit) — non solo parsing di stringhe `git diff -M` scritte a mano, per provare l'integrazione vera con git, non solo la logica di parsing isolata. `reconcile_renamed_file`: test unitari puri (nessun bisogno di git reale, solo liste di entità costruite a mano) per gli scenari 2/3/5.

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta.
- [x] Edge case tabella §4 tutti verificati esplicitamente.
- [x] Il test di `detect_renames` usa un repo git reale, non solo output `git diff -M` simulato a mano.
- [x] Migrazione applicata e verificata con un `task db:migrate` (o equivalente) reale, non solo scritta.

## 10. Implementazione — deviazioni

**Numerazione migrazione**: la SPEC lasciava `000N` come placeholder
esplicito da non presumere (§4 note del prompt di lavoro). Verificato
`contracts/sql/migrations/`: un'unica migrazione preesistente,
`0001_init` (SPEC-005). Usato quindi `0002_lineage.up.sql` /
`0002_lineage.down.sql` — nessun'altra numerazione era coerente con la
sequenza reale.

**Persistenza (§2 "Persistenza")**: la SPEC descrive il requisito
transazionale ma non fissa una firma per la funzione di persistenza.
Aggiunta `persist_lineage(client, links) -> Result<usize,
PersistLineageError>` in `services/ingestion/src/lineage.rs`, stesso
principio "singola transazione, rollback automatico su errore" di
`persist_parsed_file` (SPEC-014). Non richiesta esplicitamente dal test
plan §7 (che elenca solo `detect_renames`/`reconcile_renamed_file`), ma
coperta comunque da due test di integrazione reali (`postgres:17` via
testcontainers + CLI `migrate`, stesso pattern di
`tests/persist_integration_test.rs`) in
`services/ingestion/tests/lineage_persist_integration_test.rs`, gated
`#[ignore]` come gli altri test di integrazione Rust del progetto — non
wired in `Taskfile.yml` (`task test:integration` invoca solo
`persist_integration_test` per questo crate) perché la modifica del
Taskfile è fuori dai file toccabili di questa sessione e non è richiesta
dai criteri di accettazione; eseguibile manualmente con `cargo test
--test lineage_persist_integration_test -- --ignored --test-threads=1`.

**Regressione scoperta ed corretta, fuori dai file toccabili dichiarati
(autorizzata esplicitamente dall'utente durante l'implementazione)**:
`tests/integration/postgres_ddl/migration_test.go` (SPEC-005) invocava
`migrate ... down 1` assumendo implicitamente un'unica migrazione in
`contracts/sql/migrations` — con `0002_lineage` aggiunta da questa SPEC,
`down 1` annulla solo l'ultima migrazione (la tabella `lineage`) e lascia
intatte le 4 tabelle di SPEC-005, facendo fallire
`assertTablesAbsent`. Cambiato in `migrate ... down -all` (annulla
sempre tutto, indipendentemente da quante migrazioni si accumuleranno in
futuro) — unica riga toccata in quel file, verificato con `task
test:integration` reale (Docker + CLI `migrate`) che il test torni
verde e che nessun altro suite regredisca.

**Verifica reale eseguita** (non solo scritta, §9): `docker run
postgres:17` + CLI `migrate up`/`down` manuale (schema `lineage`
ispezionato con `\d lineage`); `cargo test --lib` (73 test, incluso il
nuovo modulo `lineage`, tutti verdi); `cargo clippy --all-targets`
pulito; `task lint` e `task test` completi puliti; `task
test:integration` completo pulito (Go `postgres_ddl`/`outbox_cdc`/
`migrate-neo4j` + `persist_integration_test` Rust); i due test
`lineage_persist_integration_test` eseguiti manualmente con `--ignored`
contro `postgres:17` reale (happy path + rollback su fallimento forzato
a metà transazione).
