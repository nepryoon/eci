# SPEC-021 — doc_hash separato + chunking cAST configurabile (T2.2)
Stato: implemented
Task-tree: T2.2 (secondo task di Fase 2) · Servizio: services/ingestion (Rust, estende T1.1/T2.1) · ADD: Modulo 1 §1.1 (cAST), §1.6.2 (doc_hash)
Contratti: nessuno (nessun file sotto contracts/ toccato)

## 1. Obiettivo
Due estensioni indipendenti a `parse_file`, entrambe derivate direttamente dal testo dell'ADD: (a) `doc_hash`, un fingerprint separato del commento di documentazione di ciascuna entità (il contenuto che T2.1 esclude deliberatamente da `ast_hash`), a supporto di un futuro embedding docstring-aware separato; (b) chunking strutturale cAST (Zhang et al. 2025, EMNLP Findings) — split ricorsivo di sottoalberi troppo grandi, fusione di fratelli piccoli, budget in caratteri non-whitespace configurabile per linguaggio.

## 2. Interfaccia

**`doc_hash`** — nuova funzione in `services/ingestion/src/hashing.rs`:
```rust
pub fn doc_hash(node: tree_sitter::Node, source: &[u8]) -> Option<String>;
```
**Decisione dichiarata** (l'ADD non lo specifica a questo livello di dettaglio): `doc_hash` cattura **solo il commento di documentazione immediatamente precedente** la dichiarazione (convenzione Go: uno o più nodi `comment` consecutivi immediatamente prima del nodo, senza riga vuota tra l'ultimo commento e la dichiarazione) — non i commenti inline sparsi dentro il corpo, che restano semplicemente esclusi da `ast_hash` senza contribuire a nessun fingerprint separato. `None` se l'entità non ha un commento di documentazione immediatamente precedente. SHA-256 del testo dei commenti trovati, concatenato nell'ordine in cui appaiono, whitespace tra loro preservato (il testo di un commento di documentazione è già prosa naturale — normalizzarlo come per `ast_hash` non ha senso, la formattazione stessa fa parte del contenuto).

**Chunking cAST** — nuovo modulo `services/ingestion/src/chunking.rs`:
```rust
pub struct CodeChunk {
    pub entity_id: String,     // id del CodeNode padre (stesso schema di T1.1)
    pub chunk_index: u32,      // posizione tra i chunk della stessa entità, da 0
    pub text: String,          // testo sorgente grezzo del chunk (non normalizzato — payload per embedding, non per hashing)
    pub char_count: usize,     // conteggio caratteri NON-whitespace, la metrica di budget (ADD §1.1)
}

pub fn chunk_entity(node: tree_sitter::Node, source: &[u8], budget_chars: usize) -> Vec<CodeChunk>;
```
**Algoritmo** (fedele al testo dell'ADD, non una sua reinterpretazione): se il conteggio di caratteri non-whitespace dell'intero `node` è ≤ `budget_chars`, l'intera entità è UN chunk. Altrimenti, split ricorsivo sui figli diretti di `node`: per ciascun figlio, se il suo conteggio supera ancora il budget, ricorri su di lui; altrimenti accumula i figli consecutivi (da sinistra a destra) in un chunk corrente finché il PROSSIMO figlio non farebbe superare il budget — a quel punto il chunk corrente si chiude e ne inizia uno nuovo (fusione greedy di fratelli piccoli, esattamente la strategia descritta dall'ADD: "fonde nodi fratelli rispettando un budget dimensionale").

**Budget configurabile per linguaggio**: `eci_common::config::env_or_default("CHUNK_BUDGET_CHARS_GO", "1500")` — valore di default dichiarato qui (1500 caratteri non-whitespace), non specificato dall'ADD; scelto come ordine di grandezza ragionevole per un metodo/funzione tipico, regolabile senza toccare il codice. Solo Go popolato per ora (stesso principio già applicato in T2.1: il meccanismo è pronto per più linguaggi, un solo valore esiste finché T2.5 non ne aggiunge altri).

## 3. Comportamento (scenari)

Verificati contro il fixture di SPEC-009 dove possibile, con un budget deliberatamente basso in alcuni scenari per esercitare lo split senza dover costruire un file sintetico grande:

1. **Dato** `notifier.go` (`EmailNotifier.Notify`, che ha un commento Go-doc immediatamente precedente nel fixture, se presente — altrimenti un caso dedicato inline), **quando** chiamo `doc_hash`, **allora** ottengo `Some(...)` con l'hash del solo testo del commento.
2. **Dato** un'entità senza nessun commento immediatamente precedente (es. `Process` in `order_service.go`, se non ne ha uno), **quando** chiamo `doc_hash`, **allora** ottengo `None`.
3. **Dato** la stessa entità di scenario 1, **quando** modifico SOLO il testo del commento di documentazione (non il codice), **allora** `doc_hash` cambia ma `merkle_hash` (T2.1) resta identico — verifica diretta dell'indipendenza dei due fingerprint.
4. **Dato** `Validate` (`order_service.go`) con un budget artificialmente basso (es. 20 caratteri non-whitespace, inferiore all'intero corpo del metodo), **quando** chiamo `chunk_entity`, **allora** ottengo più di un `CodeChunk`, ciascuno con `char_count <= 20` (salvo un singolo nodo foglia atomico più grande del budget, che non può essere ulteriormente diviso — vedi §4).
5. **Dato** lo stesso metodo con un budget generoso (es. 10000), **quando** chiamo `chunk_entity`, **allora** ottengo esattamente un `CodeChunk` che copre l'intera entità.
6. **Dato** un caso con tre dichiarazioni fratelli piccole consecutive (es. tre variabili locali dichiarate separatamente) e un budget che le contiene tutte e tre ma non una quarta, **quando** chiamo `chunk_entity`, **allora** le prime tre finiscono nello STESSO chunk (fusione), non in tre chunk separati — verifica diretta della fusione, non solo dello split.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Un singolo nodo foglia (es. una stringa letterale molto lunga) il cui conteggio di caratteri supera da solo il budget | Diventa comunque un chunk a sé (non ulteriormente divisibile — un token non si spezza a metà), anche se supera nominalmente `budget_chars`: il budget è un obiettivo per il caso comune, non un limite superiore rigido garantito su ogni singolo token atomico |
| Un'entità con zero figli e conteggio zero (es. un corpo vuoto) | Un chunk con `text` vuoto e `char_count: 0`, non un caso speciale/panico |
| Budget configurato a 0 o un valore non parsabile come intero | Errore esplicito alla lettura della configurazione (fail-fast), non un budget silenzioso di default diverso da quello dichiarato |

## 5. Non-goals
Nessuna persistenza dei chunk su Postgres (nessuna tabella `code_chunk` in questa SPEC — nessun task di Fase 2 la richiede ancora esplicitamente; i chunk restano un valore di ritorno in memoria da `parse_file`, consumabile da chi ne avrà bisogno più avanti, T2.4 o successivo). Nessun secondo linguaggio (stesso principio di T2.1). Nessun embedding reale (T2.4). Nessuna normalizzazione del testo dei chunk (il payload per l'embedding è il testo grezzo, non quello normalizzato usato per `ast_hash` — l'embedding deve vedere il codice come lo vedrebbe un umano, whitespace e commenti inclusi).

## 6. Vincoli dall'ADD
Modulo 1 §1.1: chunking strutturale AST-aware come default, budget in caratteri non-whitespace, configurabile per linguaggio/modello — non hard-coded, esattamente come motivato dal controstudio citato nello stesso paragrafo. §1.6.2: `doc_hash` come "secondo fingerprint" separato da quello semantico, per un futuro embedding docstring-aware.

## 7. Test plan
Test unitari (`cargo test`, stesso pattern di T2.1), fixture reale dove disponibile, casi dedicati inline dove il fixture non offre un esempio utilizzabile (dichiarato esplicitamente, stesso principio già applicato in SPEC-020 §7).

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con conteggi/confronti espliciti, non approssimativi.
- [x] Edge case tabella §4 tutti verificati esplicitamente, incluso il caso del token singolo più grande del budget.
- [x] Nessuna regressione sui test esistenti di T1.1/T2.1.

## 10. Implementazione — deviazioni

`doc_hash` implementato in `services/ingestion/src/hashing.rs` esattamente
con la firma dichiarata. `chunk_entity`/`CodeChunk`/`chunk_budget_chars_go`
implementati nel nuovo modulo `services/ingestion/src/chunking.rs`, cablato
in `lib.rs` (`pub mod chunking;`). **Nessuna delle due funzioni è stata
cablata dentro `parse_file`**: a differenza di T2.1 (SPEC-020 §2, che
apriva esplicitamente con "Modifica diretta a `parse_file`"), l'interfaccia
di questa SPEC non lo richiede — sono funzioni pure aggiuntive, coerenti
con §5 Non-goals ("i chunk restano un valore di ritorno in memoria da
`parse_file`, consumabile da chi ne avrà bisogno più avanti"), che descrive
un'intenzione futura (T2.4+), non un requisito di questa SPEC.

**Deviazione 1 — esempio di scenario 2 non valido nel fixture reale.** Il
testo di §3 scenario 2 suggerisce "Process in `order_service.go`, se non
ne ha uno" come esempio di entità senza commento immediatamente
precedente. Verificato leggendo il fixture: **ogni** entità top-level di
`order_service.go`/`notifier.go`/`util.go`/`main.go` (SPEC-009) ha un
commento Go-doc immediatamente precedente — non esiste nel fixture reale
un'entità che soddisfi lo scenario. Usato un caso dedicato inline
(`spec021_scenario2_doc_hash_none_for_entity_without_preceding_comment`),
dichiarato esplicitamente come tale, stesso principio già ammesso da
SPEC-020 §7 e da questa SPEC §7.

**Deviazione 2 — `entity_id` sempre stringa vuota.** La firma dichiarata
`chunk_entity(node, source, budget_chars) -> Vec<CodeChunk>` non riceve un
id come parametro, ma `CodeChunk.entity_id` esiste nello struct. Non
essendoci wiring con `parse_file` in questa SPEC (vedi sopra), `entity_id`
resta `String::new()` su ogni `CodeChunk` prodotto: il campo è popolato
correttamente da un futuro chiamante che assembla i chunk insieme al
`CodeNode` (id già disponibile lì via `make_id`, T1.1), non da
`chunk_entity` stessa, che non ha come derivarlo dalla sola firma data.

**Nota tecnica emersa durante il test dell'edge case "token singolo più
grande del budget"**: il nodo Tree-sitter `interpreted_string_literal` di
Go NON è esso stesso un nodo foglia (ha 3 figli: virgolette di
apertura/chiusura + `interpreted_string_literal_content`) — il vero nodo
foglia atomico è quest'ultimo. Il test edge-case usa
`interpreted_string_literal_content` (verificato `child_count() == 0`
esplicitamente nel test), non il nodo letterale che lo racchiude. Nessun
impatto sull'algoritmo di `chunk_entity`, che gestisce comunque
correttamente entrambi i casi (la ricorsione scenderebbe fino al vero nodo
foglia in ogni caso) — solo una correzione al caso di test perché
esercitasse davvero il ramo "nodo foglia atomico" e non il ramo "nodo
interno con figli piccoli e uno grande".

Nessun'altra deviazione. Nessuna nuova dipendenza (Cargo.toml invariato —
`eci_common::config::env_or_default` già presente dal T1.1). Nessuna
modifica allo schema DB, nessuna persistenza dei chunk, nessun secondo
linguaggio, nessuna normalizzazione del testo dei chunk — tutto come da §5
Non-goals.
