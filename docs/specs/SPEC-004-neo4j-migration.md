# SPEC-004 — Migration Neo4j idempotente da D3
Stato: verified
Task-tree: T0.4 · Servizio: contracts/cypher + tools/migrate-neo4j · ADD: Modulo 1 — Deliverable D3
Contratti: contracts/cypher/schema.cypher (già committato, Step 2 — NON modificarlo in questo task)

## 1. Obiettivo
Rendere `schema.cypher` (D3) eseguibile come migration idempotente contro un'istanza Neo4j via `task db:neo4j:migrate`, separando nettamente il DDL (constraint/indici, da eseguire) dalle query di esempio (MERGE di upsert nodo/relazione, da NON eseguire in migration — sono reference per i servizi delle fasi successive).

## 2. Interfaccia

**Split del file sorgente (senza modificare il contenuto, solo la struttura fisica):** `schema.cypher` contiene sia il DDL sia esempi di query applicativa (MERGE con parametri `$id`, `$name`, ecc.). Questo task crea:
- `contracts/cypher/schema.cypher` — SOLO i blocchi DDL: `CREATE CONSTRAINT ...`, `CREATE RANGE INDEX ...`, `CREATE FULLTEXT INDEX ...`, `CREATE VECTOR INDEX ...` (dal file originale, sezioni "CONSTRAINTS", "RANGE INDEXES", "FULL-TEXT INDEXES", "NATIVE VECTOR INDEX").
- `contracts/cypher/examples.cypher` — i blocchi MERGE parametrici (upsert nodo/relazioni, cross-domain, query vettoriale commentata) spostati qui verbatim, con l'intestazione `-- QUERY DI REFERENCE PER I SERVIZI APPLICATIVI (T1.3, T4.1). NON eseguito dalla migration.`
Questo split è una riorganizzazione fisica del contratto esistente, non una modifica di contenuto: non richiede ADR (nessuna riga viene alterata, solo redistribuita tra due file); annotarlo comunque nel messaggio di commit per tracciabilità.

**Runner:** script (linguaggio Go, riusando `libs/go`) in `tools/migrate-neo4j/main.go` che:
1. Legge `NEO4J_URI` (default `bolt://localhost:7687`), `NEO4J_USER`, `NEO4J_PASSWORD` da env.
2. Parsa `schema.cypher` in singoli statement (separatore: linea vuota tra blocchi, o `;` di fine statement — scegliere `;` come separatore esplicito, più robusto).
3. Esegue ogni statement in una sessione separata (Neo4j richiede spesso che `CREATE CONSTRAINT`/`CREATE INDEX` non condividano transazione con altri comandi in alcune versioni) usando il driver ufficiale `neo4j-go-driver`.
4. Logga ogni statement eseguito e il suo esito (creato / già esistente per via di `IF NOT EXISTS` / errore).

Target Taskfile: `task db:neo4j:migrate` (sostituisce il placeholder di SPEC-001).

## 3. Comportamento (scenari)

1. **Dato** un'istanza Neo4j vuota, **quando** eseguo `task db:neo4j:migrate`, **allora** tutti i constraint e gli indici elencati in D3 vengono creati (verifica via `SHOW CONSTRAINTS` e `SHOW INDEXES`).
2. **Dato** lo stesso DB già migrato, **quando** rieseguo `task db:neo4j:migrate`, **allora** il comando termina con successo senza errori (idempotenza garantita da `IF NOT EXISTS` nel DDL).
3. **Dato** il DB migrato, **quando** interrogo `SHOW CONSTRAINTS`, **allora** trovo esattamente: `code_node_id`, `file_path_unique`, `method_symbol_unique` (i property existence constraint `code_node_id_exists`/`code_node_domain_exists` sono stati rimossi, vedi [ADR-0004](../decisions/ADR-0004-rimuovi-existence-constraint-neo4j.md) e §10.7; il property type constraint `emb_vector_type` è stato rimosso a sua volta, vedi [ADR-0005](../decisions/ADR-0005-rimuovi-emb-vector-type-constraint.md) e §10.8).
4. **Dato** il DB migrato, **quando** interrogo `SHOW INDEXES`, **allora** trovo gli indici range (`code_node_ast_hash`, `code_node_domain`, `method_name`, `rel_commit`), full-text (`code_fulltext`, `doc_fulltext`) e l'indice vettoriale nativo (`code_embeddings`, 1536 dimensioni, cosine).
5. **Dato** `examples.cypher`, **quando** eseguo `task db:neo4j:migrate`, **allora** NESSUno dei blocchi MERGE al suo interno viene eseguito (verifica: il DB post-migrazione ha zero nodi `Method`/`Class`/ecc., solo lo schema).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Neo4j non raggiungibile (connessione rifiutata) | il runner fallisce con messaggio esplicito (host/porta/causa), non un timeout silenzioso |
| Credenziali errate | errore di autenticazione esplicito, distinto dall'errore di connessione |
| Uno statement DDL fallisce a metà lista (es. sintassi non supportata dalla versione Neo4j in uso) | il runner si ferma, riporta quale statement e l'errore esatto, non prosegue silenziosamente con i successivi |
| Il vector index richiede una versione Neo4j che non lo supporta (< 5.x con quella feature) | errore esplicito che nomina la versione minima richiesta, non un errore Cypher generico da decifrare |

## 5. Non-goals
Nessun framework di migration versionato con rollback per Neo4j in questo task (il DDL è forward-only e idempotente per costruzione via `IF NOT EXISTS` — sufficiente per Fase 0-5). Nessuna esecuzione degli esempi in `examples.cypher` da parte del runner.

## 6. Vincoli dall'ADD
Il DDL deve restare fedele 1:1 a D3. Nota pratica di ambiente (da annotare nel README di `deploy/compose`): **Neo4j Community è sufficiente per lo sviluppo fino alla Fase 5** (RBAC fine-grained e multi-database richiedono Enterprise, servono solo dalla Fase 6 — Modulo 3 §2.2). Non procurare licenza Enterprise prima di allora.

## 7. Test plan
- Integration (testcontainers, immagine `neo4j:5-community`): applica la migration, verifica scenari 1/3/4; riapplica, verifica scenario 2 (nessun errore, stesso stato); verifica scenario 5 (zero nodi applicativi dopo la migration).
- Unit: parser dello statement-splitter di `main.go` su un file `.cypher` fixture con casi limite (statement multi-riga, commenti `//`, righe vuote).

## 8. Osservabilità
Il runner stampa un riepilogo finale: N statement eseguiti, M già esistenti, 0 errori (o l'elenco degli errori). Non serve integrazione OTel in questo task (è un comando one-shot, non un servizio long-running).

## 9. Criteri di accettazione
- [x] `schema.cypher` contiene solo DDL; `examples.cypher` contiene i MERGE spostati verbatim con l'intestazione di reference.
- [ ] `task db:neo4j:migrate` verde su Neo4j Community vuoto (scenario 1, 3, 4) — **non verificato in questa sessione**, nessun daemon Docker disponibile nella sandbox (vedi §10.3). Runner e integration test sono scritti e compilano (`go build`/`go vet -tags=integration`); vanno eseguiti contro un Neo4j reale prima del merge.
- [ ] Riesecuzione idempotente verificata (scenario 2) — coperta da `TestRunAllCountsCreatedAndAlreadyExists`/`TestMigrationAgainstRealNeo4j` a livello di logica (`runAll`, classificazione `Counters()`), ma non end-to-end contro un DB reale (stesso motivo di cui sopra).
- [ ] Test che conferma zero nodi applicativi post-migrazione (scenario 5) — `assertNodeCountZero` scritto in `runner_integration_test.go`, non eseguito (nessun Docker).
- [ ] README `deploy/compose` annota la nota Community/Enterprise di §6 — **non fatto**, `deploy/` è fuori dal perimetro di file assegnato per questa sessione (solo `contracts/cypher`, `tools/migrate-neo4j`, `Taskfile.yml`, `tests/`).

## 10. Deviazioni rispetto alla SPEC

1. **Header del contratto vs §2**: l'intestazione del file SPEC dice
   "`contracts/cypher/schema.cypher` (già committato ... NON modificarlo in
   questo task)", ma il §2 istruisce esplicitamente a splittare quel file
   (fisicamente, non nel contenuto). Ho seguito §2, più specifico, e trattato
   l'intestazione come boilerplate generico ripreso dal template SPEC.

2. **`task guard` fallisce sullo split di `schema.cypher`**: verificato
   creando un commit locale temporaneo (poi disfatto con `reset --soft`, mai
   pushato) — `scripts/guard.sh` vede `contracts/cypher/schema.cypher` come
   `M` e richiede un ADR in `docs/decisions/` nello stesso commit,
   indipendentemente dal fatto che il contenuto sia solo redistribuito (non
   alterato). La SPEC afferma che questo split "non richiede ADR" per la
   natura puramente strutturale della modifica, ma il guard meccanico non fa
   questa distinzione. `docs/decisions/` non è tra i path assegnati per
   questa sessione (`contracts/cypher`, `tools/migrate-neo4j`, `Taskfile.yml`,
   `tests/`), quindi non ho aggiunto un ADR di mia iniziativa: **prima del
   commit definitivo serve un ADR breve** (stesso schema di ADR-0001/0002) o
   un adeguamento di `guard.sh`, a discrezione di chi revisiona.

3. **Nessun Docker disponibile in questa sandbox**: `docker info` risponde ma
   `docker ps` fallisce (`dial unix /var/run/docker.sock: no such file or
   directory` — nessun daemon in esecuzione). Di conseguenza:
   - Il test di integrazione (`runner_integration_test.go`, testcontainers +
     `neo4j:5-community`) è scritto secondo il test plan §7 (scenari 1-5) ma
     **non è mai stato eseguito**; è isolato dietro `//go:build integration`
     così che non impatti `task test` di default. Compila correttamente sia
     senza tag (`go vet ./...`) sia con `-tags=integration`.
   - `task db:neo4j:migrate` è stato validato solo contro un endpoint
     irraggiungibile (`bolt://localhost:19999`), per verificare l'edge case
     "connessione rifiutata" (§4) — produce un errore esplicito con
     host/porta/causa, non un timeout silenzioso. Non è stato possibile
     verificare una migrazione reale (scenari 1-5) né la creazione effettiva
     di constraint/indici.
   - `task test:integration` è stato collegato a
     `go test -tags=integration ./...` in `tools/migrate-neo4j` (era un
     placeholder `echo "not implemented yet — see T0.7"`): l'ho sostituito
     solo per la parte che ho effettivamente implementato, senza introdurre
     script fuori da `Taskfile.yml`.

4. **`ParseExt`-equivalente per il vector index / classificazione errori**:
   `classifyStatementError` distingue l'errore di creazione del vector index
   solo euristicamente (statement contiene la stringa `"VECTOR INDEX"`), non
   tramite il codice di errore Neo4j specifico — la versione minima citata
   nel messaggio (`>= 5.13`) è una stima ragionevole (introduzione del native
   vector index in Neo4j 5.x) ma non verificata contro un'istanza reale di
   quella versione.

5. **Struttura del runner**: la logica di controllo del loop (`runAll`:
   stop-on-first-error, conteggi Created/AlreadyExists) è isolata dal driver
   Neo4j dietro un `stepFunc`, per essere unit-testabile senza Docker. `Run()`
   (la funzione pubblica che apre davvero le sessioni Bolt) resta comunque
   sottile e coperta solo dal test di integrazione non eseguibile qui — non
   è una scelta della SPEC, ma necessaria per poter scrivere *qualcosa* di
   verificabile in questa sandbox oltre al parser.

6. **`tools/migrate-neo4j` è un modulo Go indipendente** (proprio `go.mod`,
   non dentro `libs/go`), per non dover toccare `libs/go/go.mod`/`go.sum`
   (fuori dal perimetro assegnato) e per aggiungere `neo4j-go-driver` /
   `testcontainers-go` solo dove servono. "Riusando `libs/go`" (§2) è stato
   interpretato come "stesso linguaggio/convenzioni Go del resto del repo",
   non come dipendenza di import diretta — il runner non ha bisogno di
   nessun tipo definito in `libs/go/eci/models`.

7. **Deviazione da D3: rimossi `code_node_id_exists` e
   `code_node_domain_exists`**. `TestMigrationAgainstRealNeo4j`, eseguito per
   la prima volta contro `neo4j:5-community` (§5, questa sessione), ha
   dimostrato che i property existence constraint richiedono Neo4j
   Enterprise Edition (errore Neo4j: "Property existence constraint requires
   Neo4j Enterprise Edition") — non disponibili in Community. I due
   constraint sono stati rimossi da `contracts/cypher/schema.cypher`;
   l'enforcement di `id`/`domain` non-null resta garantito a monte da
   PostgreSQL e dalla validazione Pydantic/Go di SPEC-003. Dettagli e
   motivazione completa in
   [ADR-0004](../decisions/ADR-0004-rimuovi-existence-constraint-neo4j.md).
   Lo scenario 3 (§3) e i test di integrazione sono stati aggiornati di
   conseguenza. Nessun impatto sulla nota Community/Enterprise di §6, che
   anzi risulta rinforzata: Community basta ora anche per questi due
   constraint, non solo per RBAC/multi-database.

8. **Deviazione da D3: rimosso `emb_vector_type`**. Proseguendo l'esecuzione
   di `TestMigrationAgainstRealNeo4j` oltre il punto raggiunto dalla
   deviazione #7, lo statement `CREATE CONSTRAINT emb_vector_type ...
   REQUIRE n.embedding IS :: VECTOR<FLOAT32>(1536)` fallisce con un errore
   di sintassi (`Invalid input 'VECTOR': expected 'ARRAY', 'LIST', ...`) su
   `neo4j:5-community` (Neo4j 5.26.28): `VECTOR<TYPE>(DIMENSION)` come tipo
   di proprietà è sintassi Cypher 25-only, introdotta in Neo4j 2025.10, non
   disponibile sulla linea 5.x usata nei test (`CYPHER 25` non è un valore
   di versione valido su quella build). Inoltre, per documentazione
   ufficiale Neo4j, memorizzare valori come tipo `VECTOR` nativo è
   supportato solo in Enterprise Edition o su Aura, mai in Community,
   indipendentemente dalla versione — il problema non si risolverebbe con
   una semplice immagine più recente restando su Community. Il constraint è
   stato rimosso da `contracts/cypher/schema.cypher`; `CREATE VECTOR INDEX
   code_embeddings` resta invariato e verificato funzionante su Community
   (indice vettoriale nativo, 1536 dimensioni, cosine) — la ricerca per
   similarità non è impattata, si perde solo l'enforcement schema-level del
   tipo esatto della proprietà `embedding`, responsabilità che ricade
   sull'applicazione (sink writer), coerente con la deviazione #7. Dettagli
   in [ADR-0005](../decisions/ADR-0005-rimuovi-emb-vector-type-constraint.md).
   Lo scenario 3 (§3) e i test di integrazione sono stati aggiornati di
   conseguenza (3 constraint attesi, 10 statement totali).
