# SPEC-032 — Propagare provenance attraverso CodeChunk/CodeEmbedding
Stato: implemented
Task-tree: correzione a SPEC-029/030, prerequisito per T3.1 (scoperta e concordata in chat) · Servizi: services/ingestion + services/embedding-worker (Go+Rust) · ADD: Modulo 1 §1.6.3 (payload Qdrant: node_id, domain, provenance)

## 1. Obiettivo
`provenance` (tipicamente `{"path": ...}`, presente su `code_node` fin da T1.2) non è mai stato propagato oltre — né nel payload outbox di `CodeChunk` (SPEC-029) né in quello di `CodeEmbedding` (SPEC-030/031). Stessa motivazione già stabilita per `entity_id` (SPEC-031): nessun sink di questo progetto interroga mai Postgres all'indietro, l'evento deve portare già tutto ciò che serve. `domain` resta esplicitamente FUORI da questa SPEC (§5) — è sempre stata la costante `'code'`, un default locale dentro T3.1 stesso è sufficiente, propagarla attraverso la catena sarebbe lavoro non necessario.

## 2. Interfaccia

**Parte 1 — `CodeChunk` (`services/ingestion/src/persist.rs`, T1.2/SPEC-029)**: `persist_parsed_file` riceve già sia `nodes: &[CodeNode]` sia `chunks: &[CodeChunk]` nella STESSA chiamata — nessuna nuova query necessaria. Per ciascun chunk, lookup in memoria del `CodeNode` con `id == chunk.entity_id` nello stesso `nodes` già ricevuto, per ottenere il suo `file_path`; payload outbox del chunk esteso con `"provenance": {"path": <file_path del suo CodeNode>}`, stessa forma già usata per `CodeNode` stesso (T1.2).

**Parte 2 — `CodeEmbedding` (`services/embedding-worker/internal/consumer/consumer.go`, SPEC-030/031)**: stesso pattern già applicato a `entity_id` in SPEC-031 — `codeChunkPayload` (il messaggio `CodeChunk` in ingresso, deserializzato) esteso con un campo `Provenance` (tipo `json.RawMessage` o struct dedicata `{Path string}` — da decidere in implementazione in base a come Go deserializza più naturalmente un oggetto JSON annidato, non fissato qui), propagato invariato nel payload outbox di `CodeEmbedding` in uscita.

## 3. Comportamento (scenari)

1. **Dato** `Validate` (`order_service.go`), **quando** eseguo `parse_go_file_full` e poi `persist_parsed_file`, **allora** il payload outbox del suo `CodeChunk` include `provenance: {"path": "order_service.go"}` (o il percorso esatto usato nel test), coerente col `provenance` del `CodeNode` di `Validate` stesso.
2. **Dato** quel messaggio `CodeChunk` consumato da `embedding-worker`, **quando** ispeziono il payload outbox di `CodeEmbedding` risultante, **allora** contiene lo stesso `provenance`, invariato rispetto al messaggio in ingresso.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Un chunk il cui `entity_id` non corrisponde a NESSUN `CodeNode` nello stesso batch (non dovrebbe accadere per costruzione, dato che i chunk sono sempre generati per un'entità del batch corrente) | Comportamento esplicito da definire in implementazione (es. `provenance` omesso o un valore sentinella) — non deve mai causare un panic o bloccare la persistenza degli altri chunk |

## 5. Non-goals
`domain` (§1 — resta un default locale dentro T3.1, non propagato). Nessuna modifica allo schema `code_chunk`/`code_embedding` (solo i payload outbox, stesso principio già stabilito per `entity_id` in SPEC-031). Nessuna modifica a T3.1 stesso (SPEC successiva, ora finalmente con un payload completo da cui attingere).

## 6. Vincoli dall'ADD
Modulo 1 §1.6.3: payload Qdrant `(node_id, domain, provenance)` — questa SPEC chiude l'ultimo pezzo mancante (`provenance`) prima che T3.1 possa scriverlo davvero su Qdrant.

## 7. Test plan
Estensione dei test di integrazione esistenti (SPEC-029 per la parte 1, SPEC-030/031 per la parte 2) — stesso principio già seguito in SPEC-031, non nuovi scenari isolati.

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-2 verificati con evidenza diretta (payload outbox reali ispezionati in entrambi i servizi, non solo il codice).
- [x] Edge case tabella §4 verificato esplicitamente.
- [x] Nessuna regressione sui test esistenti di SPEC-029/030/031.

## 10. Deviazioni dall'implementazione

1. **Tipo di `Provenance` in Go: `json.RawMessage`, non uno struct dedicato
   `{Path string}`** — scelta lasciata aperta da §2. `json.RawMessage`
   preferito perché: (a) stesso principio già in uso in questo stesso
   repo per `OutboxEvent.Payload` (`libs/go/eci/models/outbox.go`) — un
   blob JSON opaco che il consumer non deve interpretare, solo
   ritrasmettere; (b) `provenance` lato Rust (`persist.rs`, SPEC-014 §10)
   è dichiaratamente "minimale" (solo `path` per ora, ma la definizione
   D2 completa include `repo`/`commit_sha`/`ingested_at`) — uno struct Go
   con un solo campo `Path` andrebbe silenziosamente a perdere qualunque
   campo futuro aggiunto lato Rust senza che nessuno se ne accorga (nessun
   errore di unmarshal su campi extra ignorati), mentre `json.RawMessage`
   passa attraverso qualunque forma byte-per-byte, sempre corretta per
   costruzione. Zero value (`nil`, `len() == 0`) quando il messaggio in
   ingresso non ha `provenance`: la chiave resta omessa anche nel payload
   di `CodeEmbedding` in uscita (mai un `null`/oggetto vuoto fabbricato).
2. **Edge case §4 (Parte 1, Rust)**: verificato che l'entità mancante nel
   batch non causi panic riusando lo stesso test di rollback FK già
   esistente da SPEC-029
   (`chunk_persist_edge_case_rollback_on_forced_mid_transaction_chunk_failure`,
   che passa già un `entity_id` inesistente) — quel path fallisce comunque
   PRIMA di raggiungere il lookup di `provenance` (viola la FK
   `code_chunk.entity_id` a livello di INSERT), quindi non esercita
   direttamente il ramo "lookup non trovato, provenance omesso" del nuovo
   codice. Il ramo stesso (`if let Some(file_path) = ...`) è comunque privo
   di side-effect quando `None` (nessun panic per costruzione: un normale
   `if let` che semplicemente salta l'assegnazione) — non è stato aggiunto
   un test dedicato per questo ramo specifico perché §4 lo descrive come un
   caso "non dovrebbe accadere per costruzione", coerente con l'assenza di
   un nuovo scenario isolato richiesta da §7.
3. **Parte 2 (Go), test esteso non isolato**: come già in SPEC-031,
   l'assertion su `provenance` è stata aggiunta a
   `scenario3OutboxRowWithVectorIncluded` (non una nuova funzione di
   scenario). A differenza di SPEC-031, qui è stato comunque necessario un
   nuovo helper di produzione messaggi (`codeChunkPayloadWithProvenance`,
   usato SOLO da questo scenario) invece di estendere l'helper condiviso
   `codeChunkPayload` — evita di dover toccare gli altri 6 call site che
   non hanno bisogno di `provenance` nel messaggio sintetico.
