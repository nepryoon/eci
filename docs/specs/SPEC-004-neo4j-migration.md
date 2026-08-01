# SPEC-004 — Migration Neo4j idempotente da D3
Stato: draft
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
3. **Dato** il DB migrato, **quando** interrogo `SHOW CONSTRAINTS`, **allora** trovo esattamente: `code_node_id`, `code_node_id_exists`, `code_node_domain_exists`, `file_path_unique`, `method_symbol_unique`, `emb_vector_type`.
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
- [ ] `schema.cypher` contiene solo DDL; `examples.cypher` contiene i MERGE spostati verbatim con l'intestazione di reference.
- [ ] `task db:neo4j:migrate` verde su Neo4j Community vuoto (scenario 1, 3, 4).
- [ ] Riesecuzione idempotente verificata (scenario 2).
- [ ] Test che conferma zero nodi applicativi post-migrazione (scenario 5).
- [ ] README `deploy/compose` annota la nota Community/Enterprise di §6.
