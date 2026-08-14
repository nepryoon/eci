# SPEC-029 — Chunking cablato nella persistenza (pre-T3.1, 1/3)
Stato: verified
Task-tree: prerequisito non nominato esplicitamente da Fase 3 (concordato in chat) — primo dei tre pezzi prima di T3.1 · Servizio: services/ingestion (Rust, estende T1.2/T2.2) · ADD: Modulo 1 §2.1 (chunk come payload derivato del CPG)
Contratti: nuova migrazione `contracts/sql/migrations/0003_code_chunk.up.sql` (nuova tabella, additiva — nessun ADR, stesso principio già confermato per `lineage`/SPEC-027)

## 1. Obiettivo
`chunk_entity` (T2.2) esiste, è testato, ma non è mai stato invocato da nessun percorso reale della pipeline. Cablarlo in `parse_go_file`/`parse_js_file`/`parse_ts_file` (dove il nodo Tree-sitter di ciascuna entità è già in mano — `persist_parsed_file` non lo riceve mai, quindi non può chiamare `chunk_entity` da sola) e in `persist_parsed_file` (T1.2), che scrive i chunk risultanti in una nuova tabella `code_chunk` più una riga outbox per ciascuno, nella stessa transazione di `code_node`/`code_relation`. Primo dei tre pezzi concordati prima di T3.1 (sink-vector) — nessun worker di embedding né sink in questa SPEC.

## 2. Interfaccia

**Nuova tabella**:
```sql
CREATE TABLE code_chunk (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain TEXT NOT NULL DEFAULT 'code',
    entity_id CHAR(64) NOT NULL REFERENCES code_node(id),
    chunk_index INT NOT NULL,
    text TEXT NOT NULL,
    char_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_id, chunk_index)
);
CREATE INDEX idx_code_chunk_entity_id ON code_chunk(entity_id);
```

**Modifica a `parse_go_file`/`parse_js_file`/`parse_ts_file`** (`services/ingestion/src/lib.rs`): nello stesso punto in cui ciascun `CodeNode` viene costruito (dove il nodo Tree-sitter dell'entità è già disponibile), chiamata a `chunking::chunk_entity(node, source, budget)` — risultato accumulato in un nuovo `Vec<CodeChunk>` restituito come QUARTO elemento dalle varianti `_full` (`parse_go_file_full` — nuova, non esisteva; `parse_js_file_full`/`parse_ts_file_full` — estese). `entity_id` del `CodeChunk` popolato con l'id reale dell'entità appena costruita (T2.2 §10 lo lasciava vuoto esplicitamente per questo, "da un futuro chiamante" — questa SPEC è quel chiamante).

**Budget per linguaggio, generalizzato**: `chunking::chunk_budget_chars_go()` (T2.2, specifica a Go) sostituita da `chunking::chunk_budget_chars(language: &str) -> usize`, che legge `CHUNK_BUDGET_CHARS_{LINGUAGGIO}` (maiuscolo) con lo stesso default 1500 — un solo punto invece di tre funzioni quasi identiche.

**`persist_parsed_file`** (`services/ingestion/src/persist.rs`): firma estesa con un nuovo parametro `chunks: &[CodeChunk]`. Nella stessa transazione di `code_node`/`code_relation`: per ciascuna entità toccata da questo parse, **DELETE dei suoi chunk esistenti per `entity_id`** seguito da INSERT dei nuovi (stesso pattern già stabilito per `code_relation` in T1.2 — non un ON CONFLICT, dato che il numero di chunk di un'entità può cambiare tra un parse e l'altro), più una riga outbox (`aggregate_type = 'CodeChunk'`) per ciascun chunk scritto.

## 3. Comportamento (scenari)

1. **Dato** `Validate` (`order_service.go`, abbastanza piccolo da stare in un solo chunk col budget di default), **quando** eseguo `parse_go_file_full` e poi `persist_parsed_file`, **allora** ottengo esattamente una riga `code_chunk` con `entity_id` uguale all'id del `CodeNode` di `Validate`.
2. **Dato** lo stesso file già persistito, **quando** lo ri-parso e ri-persisto SENZA modifiche, **allora** il conteggio di righe `code_chunk` per quell'entità resta lo stesso (vecchie cancellate, nuove identiche inserite — non duplicate).
3. **Dato** un'entità il cui contenuto cambia tra un parse e l'altro in modo da produrre un NUMERO diverso di chunk (es. un budget artificialmente basso nel test), **quando** ri-persisto, **allora** il conteggio di `code_chunk` per quell'entità riflette il nuovo numero, non la somma dei due.
4. **Dato** lo stesso scenario 1, **quando** ispeziono `outbox`, **allora** trovo una riga con `aggregate_type = 'CodeChunk'` per il chunk scritto, con lo stesso `trace_id` della transazione.
5. **Dato** un'entità JS/TS (non solo Go), **quando** eseguo lo stesso ciclo parse+persist, **allora** il comportamento è identico — verifica diretta che il cablaggio non sia specifico a un solo linguaggio.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Un'entità con zero chunk (caso limite già gestito da T2.2 — corpo vuoto produce comunque un chunk con `char_count: 0`) | Una riga `code_chunk` viene comunque scritta, coerente con T2.2 — non un caso speciale qui |
| Il `DELETE` dei vecchi chunk di un'entità fallisce a metà transazione (es. vincolo violato) | Rollback dell'intera transazione (stesso principio già stabilito per `persist_parsed_file`, T1.2) — nessuna scrittura parziale di `code_node`/`code_relation`/`code_chunk` |

## 5. Non-goals
Nessun worker di embedding (pezzo 2/3, prossima SPEC). Nessun sink-vector (T3.1, dopo). Nessuna modifica al formato del payload outbox oltre a includere il nuovo `aggregate_type`. Nessuna verifica che il connector Debezium instradi correttamente il nuovo tipo verso un topic dedicato — presunto funzionante per lo stesso meccanismo generico già in uso per `CodeNode`/`CodeRelation` (EventRouter instrada per `aggregate_type`), ma **da verificare esplicitamente durante l'implementazione**, non presunto silenziosamente.

## 6. Vincoli dall'ADD
Modulo 1 §2.1: "chunk AST-aware come payload derivato del CPG, size configurabile" — questa SPEC è il primo punto in cui tale payload viene effettivamente scritto da qualche parte, non solo calcolato in memoria.

## 7. Test plan
Test di integrazione con Postgres reale (testcontainers, stesso principio di `persist_integration_test.rs`, T1.2) — non solo unit test in memoria, dato che il comportamento DELETE+INSERT transazionale è esattamente il tipo di cosa che un test puramente unitario non verificherebbe realisticamente.

## 8. Osservabilità
Nessun requisito nuovo oltre allo span `persist_parsed_file` già esistente.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con conteggi/confronti espliciti.
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Verificato esplicitamente (non presunto) che il connector Debezium instrada `CodeChunk` verso un topic proprio, avviando lo stack dev reale e ispezionando i topic Kafka risultanti.
- [x] Nessuna regressione sui test esistenti di T1.1/T1.2/T2.1-T2.6.

## 10. Deviazioni dall'implementazione

1. **`entity_id` TEXT, non `CHAR(64)`** — il DDL proposto in §2 dichiarava
   `entity_id CHAR(64) NOT NULL REFERENCES code_node(id)`, ma
   `code_node.id` (DDL SPEC-005, `0001_init.up.sql`) è `TEXT`, non
   `CHAR(64)`. Usata `TEXT` per `entity_id`, stesso tipo già usato da
   `code_relation.from_id`/`to_id` per lo stesso riferimento — coerente
   con la colonna reale referenziata, non con l'assunzione (errata) del
   DDL abbozzato in §2.
2. **CHECK constraint `outbox.aggregate_type` esteso nella stessa
   migrazione** — non menzionato esplicitamente dal DDL di §2, ma
   necessario: `0001_init.up.sql` vincola
   `aggregate_type IN ('CodeNode','CodeRelation')`, quindi l'INSERT
   outbox per un chunk (§2, `aggregate_type = 'CodeChunk'`) avrebbe
   violato il CHECK esistente. `0003_code_chunk.up.sql` fa
   `DROP CONSTRAINT` + `ADD CONSTRAINT` per includere `'CodeChunk'`
   (down.sql lo ripristina all'insieme originale).
3. **`parse_go_file_full` ritorna una 3-tupla
   `(Vec<CodeNode>, Vec<CodeRelation>, Vec<CodeChunk>)`, non una 4-tupla
   con `CodeChunk` come "quarto elemento"** come indicato letteralmente
   da §2. A differenza di JS/TS, Go non ha un concetto di chiamate
   "unresolved" (SPEC-013 §2 — risoluzione CALLS interamente intra-file,
   nessun resolver cross-file per questo linguaggio): un quarto elemento
   preceduto da un `UnresolvedCalls` sempre vuoto e senza consumatore
   sarebbe stato peso morto, non giustificato dal task. `CodeChunk` resta
   comunque l'ultimo elemento aggiunto in tutte e tre le varianti `_full`,
   solo la posizione ordinale cambia per Go.
4. **Nuova funzione pubblica `parse_file_full`** (non nominata da §2):
   dispatcher per estensione analogo a `parse_file`, che ritorna
   `(Vec<CodeNode>, Vec<CodeRelation>, Vec<CodeChunk>)`. Necessaria perché
   `main.rs` (entrypoint reale) doveva ottenere i chunk per passarli a
   `persist_parsed_file` senza duplicare la logica di dispatch per
   estensione già esistente in `parse_file`.
5. **Verifica Debezium (§9)**: eseguita realmente, non presunta.
   `task up` (stack dev locale) → migrazione `0003` applicata al Postgres
   dello stack dev → `cargo run --bin ingestion` su
   `order_service.go` → `SELECT aggregate_type, count(*) FROM outbox
   GROUP BY aggregate_type` conferma 4 righe `CodeChunk` scritte →
   `kafka-topics.sh --list` conferma l'esistenza del topic
   `outbox.event.CodeChunk` (distinto da `outbox.event.CodeNode`/
   `outbox.event.CodeRelation`, stesso EventRouter generico instradato
   per `aggregate_type`, nessuna configurazione connector aggiuntiva
   necessaria) → `kafka-console-consumer.sh` su quel topic ha consumato
   un messaggio reale con payload `{id, entity_id, chunk_index, text,
   char_count}` coerente con quanto scritto da `persist_parsed_file`.
