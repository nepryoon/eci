# SPEC-025 — Risoluzione cross-file JavaScript via import (T2.5, parte 2/3)
Stato: implemented
Task-tree: T2.5 (secondo dei tre sotto-task concordati — sostituisce "stack-graphs cablato per il solo JavaScript" con un resolver su misura, vedi ADR-0006) · Servizio: services/ingestion (Rust, nuovo modulo, estende SPEC-024) · ADD: Modulo 1 §1.4 (nome resolution), ma con deviazione dichiarata sulla tecnologia
Contratti: nessuno sotto contracts/

## 0. Deviazione dalla tecnologia prescritta dall'ADD (vedi ADR-0006)
L'ADD prescrive stack-graphs come meccanismo di name resolution incrementale primario. Verificato con ricerca diretta (non presunto): nessun supporto Go esistente; le regole TypeScript/JavaScript esistono ma sono descritte da un praticante con esperienza diretta come "6.000 righe di DSL", il progetto come "appassito sulla vite" da quando GitHub ha dismesso Precise Code Nav; un issue aperto sul repository ufficiale mostra risoluzione cross-modulo non funzionante per Python (il linguaggio meglio supportato); un secondo issue mostra un manutentore descrivere un problema di correttezza ("un riferimento che si risolve diversamente a seconda del contesto") come strutturalmente non risolvibile con l'approccio attuale. Il codice reale da analizzare in questo progetto è prevalentemente C/C++/C#/Java/JavaScript/TypeScript — Go è solo il linguaggio-ponte della pipeline. Decisione: per JavaScript/TypeScript, i cui moduli sono espliciti e basati su percorso file (a differenza della visibilità implicita di Go), un resolver su misura è più semplice da costruire correttamente, capire per intero, e verificare con certezza — rispetto a dipendere dal comportamento, a tratti documentato come inconsistente, di una libreria esterna il cui stato di manutenzione è incerto.

## 1. Obiettivo
Risolvere gli archi CALLS cross-file per JavaScript quando la chiamata coinvolge un nome importato esplicitamente via `import { nome } from './percorso.js'` — coprendo il caso comune e dichiarato, non ogni forma possibile di modulo JS. Nuovo passaggio a livello di **insieme di file**, non di singolo file: la prima operazione del progetto che richiede vedere più file estratti insieme, non uno alla volta.

## 2. Interfaccia

**Estrazione import** (`services/ingestion/src/imports.rs`, nuovo): per ciascun file, un nuovo passaggio (parallelo a `parse_js_file`, non sostitutivo) che estrae i binding di import:
```rust
pub struct ImportBinding {
    pub local_name: String,      // nome usato nel corpo del file che importa
    pub imported_name: String,   // nome nel file di origine (== local_name se nessun alias)
    pub module_specifier: String, // stringa letterale dopo `from`, es. "./util.js"
}

pub fn extract_imports(tree: &tree_sitter::Tree, source: &[u8]) -> Vec<ImportBinding>;
```
**Scope dichiarato**: solo `import { nome }` e `import { nome as alias }` (named import, con o senza alias) — trovati tramite il nodo `import_statement`/`import_clause` di `tree-sitter-javascript` (kind esatto da confermare empiricamente durante l'implementazione, stesso principio di verifica già seguito in SPEC-024, non presunto dal nome). **Non-goal dichiarati, non gestiti in silenzio**: default import (`import X from ...`), namespace import (`import * as X from ...`) — un `call_expression` che referenzia un binding di questo tipo resta semplicemente non risolto, stesso comportamento di un nome genuinamente non importato.

**Risoluzione percorso modulo**: `module_specifier` relativo (`./` o `../`) risolto rispetto alla directory del file che importa; se non termina con un'estensione riconosciuta (`.js`), prova prima il percorso letterale, poi con `.js` appeso — stesso principio delle convenzioni Node.js comuni, senza reimplementarle per intero (niente risoluzione `node_modules`, niente `package.json` `exports` — un modulo che non risolve a un file del nostro stesso set resta semplicemente non trovato, non un errore).

**Risoluzione simbolo, semplificazione dichiarata**: dato un file target risolto, il resolver cerca tra i suoi nodi CONTAINS di primo livello (Function/Class, dallo stesso output già prodotto da SPEC-024) un'entità con `name == imported_name`. **Nessuna verifica che l'entità sia effettivamente esportata** (il costrutto `export` non è tracciato da SPEC-024) — un nome dichiarato ma non esportato risolverebbe comunque per costruzione. Semplificazione dichiarata, non un bug nascosto: coerente con cosa l'estrazione attuale sa rappresentare.

**Passaggio di risoluzione cross-file** (`services/ingestion/src/resolve.rs`, nuovo — non dentro `parse_js_file`, vedi Obiettivo):
```rust
pub fn resolve_cross_file_calls(
    files: &[(PathBuf, Vec<CodeNode>, Vec<ImportBinding>, UnresolvedCalls)],
) -> Vec<CodeRelation>;
```
Per ciascuna chiamata rimasta irrisolta dopo l'estrazione intra-file (stesso meccanismo già esistente in SPEC-013/SPEC-024 per le chiamate a un nome non dichiarato localmente — qui riusato, non duplicato): se il nome chiamato compare tra gli `ImportBinding` del file, prova a risolvere come sopra; altrimenti resta irrisolto, stesso comportamento di oggi (cross-file non catturato).

## 3. Comportamento (scenari)

Il fixture di SPEC-024 va esteso con un vero import — attualmente `order_service.js` chiama `computeTotal` senza nessuna dichiarazione `import` (non serviva per quella SPEC): aggiungere `import { computeTotal } from './util.js';` in testa al file, ed **esportare** `computeTotal` in `util.js` (`export function computeTotal(...)`) per coerenza sintattica, anche se l'estrazione non verifica l'export (vedi §2).

1. **Dato** `order_service.js` con l'import aggiunto e `util.js`, **quando** eseguo la risoluzione cross-file sull'insieme dei due file, **allora** ottengo un arco CALLS `process`→`computeTotal`, con l'id di `computeTotal` che combacia esattamente con quello prodotto dall'estrazione di `util.js` (stessa formula deterministica di T1.1/SPEC-024, nessuna scorciatoia).
2. **Dato** lo stesso scenario ma con `import { computeTotal as total } from './util.js';` e la chiamata riscritta come `total(prices)`, **quando** risolvo, **allora** ottengo comunque l'arco verso `computeTotal` (l'id reale nel file target), non verso un nodo inesistente chiamato `total` — prova diretta che l'alias viene gestito, non solo il caso senza alias.
3. **Dato** un file con `import { qualcosa } from 'libreria-esterna';` (uno specificatore che non risolve a nessun file del nostro set), **quando** risolvo, **allora** una chiamata a `qualcosa` resta non risolta — nessun crash, nessuna relazione fabbricata verso un file inesistente.
4. **Dato** una chiamata a un nome che non è né dichiarato localmente né importato (es. una funzione globale del browser/Node come `setTimeout`), **quando** risolvo, **allora** resta non risolta — stesso comportamento di oggi.
5. **Dato** un file con `import Foo from './qualcosa.js';` (default import, Non-goal dichiarato), **quando** una chiamata a `Foo(...)` viene valutata, **allora** resta non risolta, senza errori — verifica diretta che il Non-goal sia gestito correttamente come "non gestito", non come un crash o un comportamento indefinito.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `module_specifier` che risolve a un percorso file valido ma quel file non è tra quelli passati a `resolve_cross_file_calls` (es. mai processato in questa esecuzione) | Non risolto — stesso trattamento di un modulo esterno, il resolver non tenta di leggere file non forniti esplicitamente |
| Due `ImportBinding` nello stesso file con lo stesso `local_name` (import duplicato o rinominato due volte, codice sorgente insolito ma sintatticamente valido) | L'ultimo vince (comportamento di un semplice inserimento in mappa per `local_name`) — non un caso speciale, non un errore |
| Un file che importa se stesso o crea un ciclo di import (`a.js` importa da `b.js` che importa da `a.js`) | Nessun problema per questa SPEC: la risoluzione è per singola chiamata, non richiede attraversare l'intero grafo di import — un ciclo non causa loop infinito perché non c'è ricorsione, solo un lookup diretto per file |

## 5. Non-goals
Default import, namespace import (§2, §3 scenario 5). Verifica di visibilità `export` (§2). Risoluzione `node_modules`/pacchetti npm esterni. Import dinamici (`import()`). Re-export (`export { x } from './y.js'`). Nessuna modifica a TypeScript (parte 3/3, separata). Nessuna persistenza diversa da quella già stabilita — gli archi CALLS cross-file prodotti qui si aggiungono allo stesso `Vec<CodeRelation>` già gestito da T1.2.

## 6. Vincoli dall'ADD
Deviazione dichiarata in §0/ADR-0006 sulla tecnologia; l'obiettivo di fondo (name resolution cross-file per CALLS) resta coerente con l'intento dell'ADD, cambia solo il meccanismo.

## 7. Test plan
Test unitari (`cargo test`), fixture esteso di SPEC-024 per gli scenari possibili, casi dedicati inline per gli scenari che il fixture non copre naturalmente (es. import da libreria esterna, default import).

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con conteggi/confronti espliciti — in particolare lo scenario 2 (alias) e lo scenario 5 (Non-goal gestito esplicitamente, non per omissione).
- [x] Edge case tabella §4 tutti verificati.
- [x] Nessuna regressione sui test esistenti di SPEC-024 (l'estrazione per-file non deve cambiare).
- [x] ADR-0006 scritta e committata insieme a questa SPEC, non successivamente.

## 10. Implementazione — deviazioni

ADR-0006 scritta in `docs/decisions/`, stesso formato esatto di
ADR-0001..0005 (`Stato: accettata · Data:`, `## Contesto`/`## Decisione`/
`## Conseguenze`), committata nello stesso commit di questa SPEC.

**Node kind reali di `tree-sitter-javascript` per gli import, verificati
con `to_sexp()` prima di scrivere `extract_imports`** (stesso principio
di SPEC-024, mai presunti dal nome):
- `import_statement` (top-level), campo `source` di kind `string`
  contenente un `string_fragment` (il testo SENZA virgolette — non serve
  strippare manualmente le virgolette, il nodo giusto le esclude già).
- Figlio diretto (non un campo con nome) `import_clause`, che contiene
  UNO tra: `named_imports` (il caso in scope: una lista di
  `import_specifier`, ciascuno con campo `name` obbligatorio e campo
  `alias` opzionale, presente solo con `as`); un `identifier` nudo
  (default import); `namespace_import` (`import * as X`). Questi ultimi
  due non hanno un `named_imports` figlio: il filtro
  `.find(|c| c.kind() == "named_imports")` fallisce naturalmente su di
  loro, `None` → nessun `ImportBinding` prodotto. Non un caso speciale
  scritto apposta — conseguenza diretta della ricerca per kind.

**Regressione reale scoperta estendendo il fixture per §3 (non
ipotetica)**: `export function computeTotal(...)` in `util.js` avvolge la
`function_declaration` in un `export_statement` (campo `declaration`) —
verificato con `to_sexp()` PRIMA di aggiungere `export` al fixture, non
durante il debug di un test fallito dopo. Senza gestirlo, `computeTotal`
sarebbe sparita del tutto dall'estrazione (il top-level loop di
`parse_js_file` cercava solo `class_declaration`/`function_declaration`
diretti), rompendo lo scenario 4 di SPEC-024 stessa. Corretto in
`services/ingestion/src/lib.rs`: il loop top-level ora spacchetta un
`export_statement` al proprio campo `declaration` prima del match — il
costrutto `export` in sé resta comunque non tracciato (nessun campo
`is_exported` sul `CodeNode`, coerente con SPEC-025 §2 "il costrutto
export non è tracciato"), cambia solo che la dichiarazione avvolta non
scompare più.

**`UnresolvedCalls`, non specificato a livello di struct dalla SPEC**
(solo usato come tipo nella firma di `resolve_cross_file_calls`):
definito in `resolve.rs` come `Vec<UnresolvedCall>`, con
`UnresolvedCall { caller_id, callee_name, weight }`. Prodotto SENZA
duplicare il meccanismo di attraversamento (SPEC-025 §2 "stesso
meccanismo... qui riusato, non duplicato"): `collect_calls_js`
(SPEC-013/SPEC-024, già esistente) esteso con un secondo accumulatore
per i nomi non risolti invece di scartarli, in un solo passaggio.
`parse_js_file` (contratto pubblico di SPEC-024, invariato) resta un
thin wrapper su una nuova `parse_js_file_full` (`pub(crate)`, non
pubblica: consumata solo da `resolve`/dai test, non è un'estensione
dichiarata dell'API pubblica di SPEC-024) che espone anche il terzo
elemento.

**Risoluzione del percorso modulo**: implementata con una normalizzazione
manuale dei componenti `.`/`..` (`normalize_relative_path`) invece di
`Path::canonicalize`, che richiederebbe che il file esista realmente sul
filesystem — qui `files` è un insieme logico passato dal chiamante (in
memoria nei test), non necessariamente presente su disco con quei path
esatti.

Nessun'altra deviazione. Nessuna risoluzione `node_modules`, nessun
import dinamico, nessun re-export, nessuna verifica di `export`, nessuna
modifica a TypeScript — tutto come da §5 Non-goals.
