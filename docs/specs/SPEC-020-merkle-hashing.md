# SPEC-020 — Hashing Merkle SHA-256 + normalizzazione (T2.1)
Stato: verified
Task-tree: T2.1 (primo task di Fase 2 — tutti gli altri task di Fase 2 tranne T2.4/T2.5 dipendono da questo) · Servizio: services/ingestion (Rust, estende T1.1) · ADD: Modulo 1 §1.6.1-1.6.2
Contratti: nessuno (nessun file sotto contracts/ toccato — `ast_hash` resta `CHAR(64)` come già in SPEC-005, cambia solo come viene calcolato)

## 1. Obiettivo
Sostituire il placeholder di `ast_hash` introdotto in T1.1 (`sha256_hex(source)`, hash piatto del testo grezzo dell'intera entità — dichiarato esplicitamente come tale in SPEC-013 §5 Non-goals) con l'hashing Merkle bottom-up specificato nell'ADD (Modulo 1 §1.6.1): un hash che si propaga dai token foglia fino alla radice di ciascuna entità, insensibile a whitespace/commenti/rinomina di variabili locali, ma sensibile a qualunque cambiamento semantico reale. Fondamenta per tutto il resto di Fase 2 (T2.2 doc_hash/chunking, T2.3 Semantic Cache, T2.6 rename/move) — nessuno di questi è costruibile senza un `ast_hash` che si comporti correttamente.

## 2. Interfaccia

Modifica diretta a `parse_file` (`services/ingestion/src/lib.rs`), non un modulo separato invocato dopo: per ciascuna entità già visitata durante l'attraversamento esistente (File/Class/Interface/Method/Function), il nodo Tree-sitter già in mano viene passato a una nuova funzione invece che allo `sha256_hex(source)` attuale:

```rust
// services/ingestion/src/hashing.rs (nuovo modulo)
pub fn merkle_hash(node: tree_sitter::Node, source: &[u8]) -> String;
```

**Formula, esattamente come da ADD §1.6.1** (nessuna variazione):
```
H(node) = SHA256( normalize(node.kind())
                 || normalize(node.text()) [solo se nodo foglia]
                 || H(child_1) || H(child_2) || ... || H(child_n) [in ordine, solo se nodo interno] )
```
Un nodo è foglia se `node.child_count() == 0` (nessun figlio con nome secondo la grammatica Tree-sitter di Go — coerente con come Tree-sitter stesso distingue token terminali da nodi di produzione).

**Normalizzazione `normalize(...)`, tre regole indipendenti**:
1. **Whitespace**: mai incluso nell'hash — un nodo "foglia" nella grammatica Tree-sitter di Go non include whitespace circostante nel proprio testo; nessuna azione esplicita necessaria oltre a non attraversare nodi `extra` (commenti, vedi punto 2) che potrebbero portarlo.
2. **Commenti**: i nodi di kind `comment` vengono saltati per intero durante l'attraversamento — non contribuiscono né come foglia né propagando un hash verso l'alto (l'entità genitore si comporta come se il commento non esistesse affatto in questo hash). **Nessun `doc_hash` prodotto qui** — quello è esplicitamente T2.2, fuori scope per questa SPEC (vedi §5).
3. **Identificatori, due regimi per tipo di nodo** (ADD §1.6.2, criterio esplicito):
   - **Identity-preserving** (default): il nome DICHIARATO di un'entità File/Class/Interface/Method/Function (il nodo `identifier`/`field_identifier` che è il nome della dichiarazione stessa, non un suo uso interno) — hashato dal proprio testo letterale, invariato.
   - **Alpha-renaming-normalizzato**: identificatori che referenziano variabili locali dichiarate con `:=` o `var` DENTRO il corpo di una Method/Function — normalizzati a un placeholder posizionale deterministico (`LOCAL_0`, `LOCAL_1`, ... nell'ordine di prima dichiarazione all'interno di quello specifico corpo), non al proprio nome letterale. **Ogni altro identificatore** (nomi di funzione/metodo/tipo chiamati, campi, package-qualified, parametri di funzione) resta identity-preserving per default — la vera risoluzione di ambito arriva con la name resolution di T2.5 (stack-graphs); questa è un'euristica dichiaratamente più stretta (solo variabili locali dichiarate con `:=`/`var` nello stesso corpo), non simbolica completa.

## 3. Comportamento (scenari)

Verificati contro `order_service.go` del fixture di SPEC-009 (metodo `Validate`), non un caso ad-hoc:

1. **Dato** `order_service.go`, **quando** aggiungo/rimuovo righe vuote o cambio indentazione senza toccare il codice, **allora** `merkle_hash` per TUTTE le entità (File, Class, entrambi i Method) resta identico.
2. **Dato** lo stesso file, **quando** aggiungo un commento dentro il corpo di `Validate`, **allora** `merkle_hash(Validate)` resta identico.
3. **Dato** lo stesso file, **quando** rinomino una variabile locale dichiarata con `:=` dentro `Validate` (se presente nel fixture reale — altrimenti costruire un caso di test dedicato con una variabile locale esplicita), **allora** `merkle_hash(Validate)` resta identico.
4. **Dato** lo stesso file, **quando** rinomino il METODO `Validate` stesso (il suo nome dichiarato), **allora** `merkle_hash(Validate)` CAMBIA — identity-preserving per il nome dichiarato, non alpha-renamed.
5. **Dato** lo stesso file, **quando** cambio un operatore di confronto reale dentro il corpo di `Validate` (es. `>` in `>=`), **allora** `merkle_hash(Validate)` cambia — e, per la proprietà di propagazione Merkle, anche `merkle_hash` di `OrderService` (la Class che lo contiene) e del `File` cambiano, mentre `merkle_hash(Process)` (l'altro metodo, non toccato) resta identico.
6. **Dato** un caso con due variabili locali distinte nello stesso corpo, **quando** le rinomino SCAMBIANDO i loro nomi tra loro (non solo rinominando una), **allora** l'hash resta identico solo se l'ordine di PRIMA DICHIARAZIONE resta lo stesso — verifica esplicita che la normalizzazione sia posizionale, non un semplice "ignora i nomi delle variabili".

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Una variabile locale con lo stesso nome di un identificatore identity-preserving altrove nel file (es. un parametro chiamato come un metodo) | Nessuna ambiguità: la classificazione locale-vs-identity-preserving è per POSIZIONE nell'albero (dentro il corpo dove è dichiarata con `:=`/`var`), non per il nome — stesso nome in scope diversi non interferisce |
| Un corpo di funzione senza nessuna variabile locale dichiarata con `:=`/`var` (es. `computeTotal`, se non ne ha) | Nessun placeholder `LOCAL_N` generato — comportamento normale, non un caso speciale |
| Un identificatore che appare sia come nome dichiarato SIA come variabile locale in un contesto diverso (ombreggiamento) | Fuori scope di questa euristica dichiaratamente ristretta (§2 punto 3) — la vera risoluzione di scope arriva con T2.5; se osservato come problema concreto su codice reale, va trattato come limite noto da quella SPEC, non qui |

## 5. Non-goals
Nessun `doc_hash` (T2.2). Nessun `logic_fingerprint` (proprietà della pipeline — modello di embedding, versione prompt — non del codice; T2.3). Nessuna vera risoluzione di scope/simboli oltre l'euristica dichiarata al punto 3 di §2 (T2.5, stack-graphs). Nessuna modifica allo schema DB (`ast_hash` resta `CHAR(64)`, cambia solo il valore prodotto). Nessun secondo linguaggio (resta Go, come T1.1 — l'algoritmo è a livello di principio language-agnostic, ma la classificazione "variabile locale" al punto 3 di §2 è specifica alla grammatica Go di Tree-sitter).

## 6. Vincoli dall'ADD
Modulo 1 §1.6.1: formula Merkle esatta, riportata testualmente in §2 — nessuna variazione. §1.6.2: le tre regole di normalizzazione (whitespace, commenti, i due regimi di identificatori "selezionabili per tipo di nodo") — implementate esattamente come descritte, non come una loro reinterpretazione.

## 7. Test plan
Test unitari (`cargo test`, stesso pattern di SPEC-013) per gli scenari di §3, usando `order_service.go` reale da `tests/fixtures/sample-repo/` dove possibile (scenari 1, 2, 4, 5), un caso di test dedicato inline dove il fixture non offre una variabile locale utilizzabile (scenari 3, 6). **Property test** (nuova dipendenza `proptest` o equivalente, versione esatta da annotare in deviazioni): genera varianti casuali di whitespace/nomi di variabili locali su un corpo di funzione fisso e verifica l'invariante "l'hash non cambia" su un campione ampio di varianti generate, non solo i pochi casi manuali di §3 — la classe di test che la SPEC nomina esplicitamente nel titolo del task, non opzionale.

## 8. Osservabilità
Nessun requisito nuovo oltre quanto già stabilito in T1.1 (span `parse_file` già esistente).

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con conteggi/confronti espliciti di hash (uguale/diverso), non solo "il test passa".
- [x] Property test implementato e riportato nel report con il numero di casi generati verificati, non solo dichiarato presente.
- [x] Edge case tabella §4 tutti verificati esplicitamente.
- [x] Nessuna regressione sui test già esistenti di T1.1 (SPEC-013, gli 8 test originali continuano a passare — adattati se necessario per il nuovo valore di `ast_hash`, ma senza cambiare cosa verificano strutturalmente).

## 10. Implementazione — deviazioni

Implementato in `services/ingestion/src/hashing.rs` (nuovo modulo, `pub fn
merkle_hash(node: tree_sitter::Node, source: &[u8]) -> String`), cablato in
`parse_file` (`services/ingestion/src/lib.rs`) sostituendo i tre call-site
`sha256_hex(...)` (File: nodo `root`; Class/Interface: nodo `spec`
type_spec; Method/Function: nodo `child` method/function_declaration).

**Deviazione da §3 scenario 5** (unica deviazione sostanziale, concordata
esplicitamente con l'utente prima dell'implementazione, non silenziosa):
lo scenario 5 afferma che modificare un operatore di confronto dentro il
corpo di `Validate` debba cambiare anche `merkle_hash(OrderService)` (la
Class contenente) "per la proprietà di propagazione Merkle". Questo non è
strutturalmente possibile con la formula esatta di §2 ("il nodo
Tree-sitter già in mano" — per una Class è il suo `type_spec`, che in Go
contiene solo i campi dello struct, MAI i Method, dichiarati altrove come
`method_declaration` top-level separati e sintatticamente disgiunti). L'ADD
stesso (Modulo 1 §1.6.1, riga 100) conferma che la propagazione Merkle
arriva "fino alla radice della funzione/file", mai fino a un'entità Class
sintatticamente distinta. Implementato quindi fedelmente a §2/ADD: il test
`scenario5_changed_comparison_operator_propagates_to_validate_and_file_not_class_or_process`
verifica che `merkle_hash(Validate)` e `merkle_hash(File)` CAMBINO e che
`merkle_hash(Process)` e `merkle_hash(OrderService)` (Class) NON cambino.
Nessuna modifica al comportamento di File/Method/Function rispetto al testo
della SPEC — la deviazione riguarda solo l'aspettativa sulla Class in
questo singolo scenario.

**Nuova dipendenza**: `proptest = "1.11.0"` (dev-dependency,
`services/ingestion/Cargo.toml`) — versione esatta risolta in `Cargo.lock`.
Property test in `services/ingestion/src/hashing.rs`, modulo
`hashing::tests::proptest_suite::merkle_hash_invariant_under_whitespace_and_local_var_renaming`:
genera varianti casuali di whitespace di fine riga (spazi/tab, 0-5
caratteri, 4 punti di inserimento indipendenti) e di nomi di due variabili
locali dichiarate con `:=` su un corpo di funzione fisso (`Compute`),
verificando che l'hash resti identico alla forma canonica. **512 casi
generati e verificati** (`ProptestConfig::with_cases(512)`), tutti passati.

Nessun'altra deviazione: normalizzazione whitespace/commenti/identificatori
implementata esattamente come da §2 punto 3 (verificato dal grammar dump
di `tree-sitter-go` — nomi dichiarati di Method usano `field_identifier`,
nomi di Function/parametri/variabili locali usano `identifier`, accessi a
campo usano `field_identifier`: la sostituzione `LOCAL_N` tocca solo nodi
`identifier` il cui testo compare come LHS di `short_var_declaration`/
`var_declaration` dentro il campo `body` dell'entità, mai `field_identifier`
o `type_identifier`, che restano sempre identity-preserving per costruzione
senza bisogno di un caso speciale esplicito). Nessuna modifica allo schema
DB, nessun secondo linguaggio, nessuna vera risoluzione di scope — tutto
come da §5 Non-goals.
