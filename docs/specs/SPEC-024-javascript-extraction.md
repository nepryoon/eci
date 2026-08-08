# SPEC-024 — Estrazione CPG JavaScript (T2.5, parte 1/3)
Stato: implemented
Task-tree: T2.5 (primo di tre sotto-task concordati: estrazione, poi stack-graphs JS, poi TypeScript) · Servizio: services/ingestion (Rust, estende T1.1) · ADD: Modulo 1 §1.2-1.4
Contratti: nessuno sotto contracts/ (stesso schema CodeNode/CodeRelation di T1.1, nessuna estensione di schema richiesta)

## 1. Obiettivo
Applicare a JavaScript lo stesso pattern di estrazione CPG già stabilito per Go in T1.1 — non un meccanismo nuovo, lo stesso con adattamenti dove la struttura reale di JavaScript differisce genuinamente da quella di Go. **Nessuna risoluzione cross-file in questa SPEC** (stack-graphs è la parte 2/3, deliberatamente separata per isolare il rischio — vedi la scomposizione concordata in chat). Introduce per la prima volta un dispatch per linguaggio in `services/ingestion`, che fino a T2.4 era implicitamente solo-Go.

## 2. Interfaccia

**Crate**: `tree-sitter-javascript` (grammatica standard, stesso ruolo di `tree-sitter-go` in T1.1).

**Dispatch per linguaggio** (nuovo — decisione dichiarata, prima volta che serve): `parse_file` (T1.1) rinominato internamente in `parse_go_file`; nuova `parse_js_file` con la stessa firma/contratto di output (`Vec<CodeNode>`, `Vec<CodeRelation>`); un `parse_file` sottile in cima dispatcha in base all'estensione del path (`.go` → Go, `.js` → JavaScript) — nessun altro meccanismo di rilevamento linguaggio (nessuno sniffing del contenuto): l'estensione è già come SPEC-009/il fixture reale identificano i file, coerente con l'intero progetto finora.

**Mappatura entità CPG, differenze dichiarate rispetto a Go**:
- `File` — invariato.
- `Class` — `class_declaration`. **A differenza di Go**: in JavaScript un `class_declaration` CONTIENE realmente i propri metodi come figli diretti dell'albero Tree-sitter (`class_body` → `method_definition`) — non serve nessun escamotage: CONTAINS Class→Method è direttamente derivabile qui, cosa che SPEC-021 §10 aveva dichiarato esplicitamente IMPOSSIBILE per Go (dove i metodi sono `method_declaration` separati a livello di file, collegati solo per nome del receiver).
- `Method` — `method_definition` dentro un `class_body`, **incluso `constructor`** (strutturalmente identico agli altri metodi in Tree-sitter, nessun caso speciale).
- `Function` — `function_declaration` a livello top-level SOLO. **Non-goal dichiarato** (non un'omissione silenziosa): function expression assegnate a variabile (`const f = function() {}`) e arrow function (`const f = () => {}`) NON sono estratte in questa SPEC — non hanno un nodo "nome" diretto nello stesso modo, richiederebbero inferire il nome dal target dell'assegnazione, una complessità aggiuntiva reale che si rimanda deliberatamente.
- `Interface` — **nessun analogo in JavaScript puro** (è un costrutto TypeScript, `interface` keyword) — Non-goal per questa SPEC, dichiarato esplicitamente, non un'assenza silenziosa. Tornerà rilevante nella parte 3/3 (TypeScript).

**CONTAINS**: File→{Class, Function} di livello top-level (stesso principio di Go); **Class→Method** (nuovo, vedi sopra — derivabile direttamente dall'albero, non un'aggiunta artificiale).

**CALLS**: stessa euristica intra-file a due passaggi di T1.1 (SPEC-013 §2), adattata alla sintassi di chiamata JS (`call_expression`), stesso limite dichiarato — nessuna risoluzione cross-file (arriva con la parte 2/3 di questa scomposizione).

**Id deterministico**: stessa formula di T1.1 (`sha256_hex(file_path:qualified_name)`), stessa convenzione di nome qualificato `ClassName.methodName` per i Method.

**Fixture**: nuovo `tests/fixtures/sample-repo-js/` — stessa STRUTTURA concettuale del fixture Go esistente (SPEC-009), non una coincidenza: rende i due direttamente comparabili scenario per scenario.
```javascript
// order_service.js
class OrderService {
  constructor(minItems) { this.minItems = minItems; }
  process(prices) {
    this.validate(prices);
    return computeTotal(prices); // cross-file, deliberatamente non catturato (stesso principio del fixture Go)
  }
  validate(prices) {
    if (prices.length < this.minItems) { throw new Error("too few items"); }
  }
}
```
```javascript
// util.js
function computeTotal(prices) {
  return prices.reduce((sum, p) => sum + p, 0);
}
```
```javascript
// main.js
function main() {
  const service = new OrderService(2);
  service.process([10, 20, 30]);
}
```

## 3. Comportamento (scenari)

Mirror diretto degli scenari di T1.1 (SPEC-013 §3), stessa numerazione concettuale, per comparabilità:

1. **Dato** `order_service.js`, **quando** parso il file, **allora** ottengo nodi File/Class(`OrderService`)/Method(`constructor`,`process`,`validate`) e archi CONTAINS File→Class, **Class→ciascuno dei tre Method** (nuovo rispetto a Go, verificato esplicitamente qui).
2. **Dato** lo stesso file, **quando** ispeziono gli archi CALLS, **allora** trovo `process`→`validate` (intra-classe, intra-file).
3. **Dato** lo stesso file, **quando** cerco un arco CALLS verso `computeTotal`, **allora** non lo trovo — cross-file, stesso limite dichiarato di Go, non un bug.
4. **Dato** `util.js`, **quando** parso il file, **allora** ottengo un nodo File + un nodo Function(`computeTotal`), CONTAINS File→Function, nessun CALLS (nessuna chiamata in uscita nel corpo).
5. **Dato** `main.js`, **quando** parso il file, **allora** ottengo File + Function(`main`), nessun arco CALLS in uscita catturato (la chiamata a `service.process` è su un'istanza, non risolvibile senza analisi di tipo — comunque cross-file anche se lo fosse).
6. **Dato** JavaScript sintatticamente invalido (es. una parentesi graffa non chiusa), **quando** parso il file, **allora** l'error recovery di Tree-sitter produce comunque i nodi validi estraibili dalle porzioni non affette, stesso principio di T1.1 (SPEC-013 §4).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Una funzione dichiarata con `function` ma nidificata dentro un'altra funzione (non a livello top-level né dentro una classe) | Non estratta come `Function` di questa SPEC (che si limita esplicitamente al livello top-level) — nessun crash, semplicemente non genera un nodo proprio |
| Un metodo statico (`static metodo() {}`) | Estratto come `Method` allo stesso modo di uno d'istanza — nessuna distinzione statico/istanza in questa SPEC (non richiesta da nessuno scenario, il campo `node_type` resta `Method` per entrambi) |
| Getter/setter (`get x() {}` / `set x(v) {}`) | Stesso trattamento di un `Method` ordinario — Tree-sitter li rappresenta comunque come `method_definition`, nessun caso speciale necessario |

## 5. Non-goals
Nessuna risoluzione cross-file (parte 2/3). Nessuna estrazione di function expression/arrow function assegnate a variabile (dichiarato in §2). Nessun `Interface` (non esiste in JS puro — parte 3/3, TypeScript). Nessuna estensione di `merkle_hash`/`doc_hash`/`chunk_entity` (T2.1/T2.2) per riconoscere le forme di dichiarazione locale JS (`let`/`const`/`var`) — continuano a funzionare su un nodo JS (la formula è già language-agnostic), ma senza il beneficio di alpha-renaming per le variabili locali JS: tutti gli identificatori JS restano identity-preserving per ora, corretto ma non ottimizzato. Nessuna modifica allo schema DB.

## 6. Vincoli dall'ADD
Modulo 1 §1.2: Tree-sitter come front-end di parsing per qualunque linguaggio con una grammatica disponibile — `tree-sitter-javascript` è esattamente questo caso. §1.3 (CPG a livello di statement, non trattato in dettaglio qui: stessa granularità già stabilita da T1.1, nessuna variazione).

## 7. Test plan
Test unitari (`cargo test`), stesso pattern di T1.1 (SPEC-013 §7) — fixture reale per tutti gli scenari possibili, nessun caso dedicato inline necessario dato che il fixture è stato progettato apposta per coprirli tutti.

## 8. Osservabilità
Nessun requisito nuovo — stesso span `parse_file` già esistente, con un nuovo attributo `language` (valore `"go"` o `"javascript"`) per distinguere le due strade nel dispatch.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con conteggi/confronti espliciti.
- [x] Edge case tabella §4 tutti verificati esplicitamente.
- [x] Nessuna regressione sui test esistenti di T1.1 (il dispatch non deve alterare il comportamento Go).

## 10. Implementazione — deviazioni

Dispatch implementato esattamente come da §2: `parse_file` (pubblica)
apre lo span (con l'attributo `language` aggiunto, §8) e smista su
`parse_go_file`/`parse_js_file` (entrambe rese private — "rinominato
**internamente**", §2 — verificato che nessun altro punto del crate
referenziasse `parse_file` se non tramite l'API pubblica, quindi il
downgrade a privato non rompe nulla). Estensione non riconosciuta (né
`.go` né `.js`): `panic!` esplicito con messaggio che nomina l'estensione
ricevuta — non specificato dalla SPEC, ma coerente col principio
generale del progetto (fail-fast, mai un fallback silenzioso su
un'estensione a caso). Test dedicato
(`dispatch_panics_on_unsupported_extension`).

**Differenze empiriche tra i node kind di `tree-sitter-javascript` attesi
e quelli reali** (stesso principio di verifica di T1.1/T2.1, mai assunti
dal nome): verificate con dump `to_sexp()` prima di scrivere qualunque
logica di estrazione, non durante il debug di un test fallito.
- Root: `program`, non `source_file` (diverso da Go).
- Nome di un metodo (`method_definition`): campo `name` di kind
  `property_identifier`, NON `identifier` — diverso dal nome di una
  `function_declaration` top-level, che resta `identifier` come in Go.
  Questa distinzione non richiede alcun codice esplicito nell'estrazione
  (si usa sempre `child_by_field_name("name")`, indipendente dal kind
  specifico del nodo restituito), ma è rilevante per chi in futuro
  toccherà `merkle_hash`/l'euristica di alpha-renaming (§5 Non-goals): un
  filtro `node.kind() == "identifier"` per riconoscere variabili locali,
  se mai esteso a JS, NON intercetterebbe i nomi di metodo per costruzione
  (sono `property_identifier`), coerentemente identity-preserving anche
  senza logica dedicata.
- Chiamata a metodo (`this.validate(...)`, `obj.metodo(...)`):
  `call_expression` con campo `function` di kind `member_expression`
  (campi `object`/`property`) — equivalente diretto della
  `selector_expression` di Go (campo `field`), stesso ruolo,
  nome diverso.
- **`new X(...)` produce un nodo `new_expression`, MAI un
  `call_expression`** — non previsto esplicitamente dal testo della SPEC,
  scoperto durante la verifica empirica del fixture. Conseguenza diretta:
  `new OrderService(2)` in `main.js` non entra nemmeno nel percorso di
  risoluzione CALLS (che cerca solo nodi `call_expression`), non per un
  filtro scritto apposta — comportamento corretto per lo scenario 5,
  ottenuto "gratis" dalla struttura della grammatica.
- `static`/getter (`get x`)/setter (`set x`): tutti `method_definition`
  strutturalmente identici a un metodo ordinario (nessun kind separato,
  nessun campo che li distingua da un metodo qualunque) — confermato
  l'edge case §4 tabella, nessun caso speciale necessario nel codice.
- `class_body` espone i propri metodi con **campo ripetuto `member`**
  (non named children generiche senza field name) — nella pratica
  irrilevante per l'estrazione (si itera comunque `class_body.children()`
  filtrando per `kind() == "method_definition"`, lo stesso pattern già
  usato per `type_declaration`→`type_spec` in Go), ma diverso da come
  Go espone i field di un `type_spec` (`name`/`type` singoli, non
  ripetuti).
- **Scenario 6 (error recovery)**: comportamento REALMENTE diverso da Go,
  non solo nominalmente. In Go, una dichiarazione successiva a un errore
  di sintassi viene inghiottita SENZA restare un proprio kind
  riconoscibile a livello di root (SPEC-013 §4). In JavaScript, verificato
  che una `class` con corpo non chiuso (`class Broken { function
  alsoValid() {...}` senza `}` finale) resta comunque un `class_declaration`
  di primo livello proprio e riconoscibile — è la porzione INTERNA al
  corpo malformato a diventare un `method_definition` spurio (con un nodo
  `ERROR` innestato), non la dichiarazione esterna a sparire. Il test
  `scenario6_syntax_error_recovers_valid_declarations` verifica solo
  l'affermazione POSITIVA letterale di §3 scenario 6 ("le porzioni non
  affette restano estraibili" — qui, la funzione `valid` dichiarata prima
  dell'errore), senza assumere un'equivalenza comportamentale con Go sulla
  porzione malformata, che non esiste.

`ast_hash` della Class JS calcolato sull'intera `class_declaration`
(nome + `class_body`, quindi metodi inclusi come discendenti reali) — a
differenza di Go, dove SPEC-020 §10 aveva dichiarato esplicitamente
impossibile la propagazione da un Method al Class hash perché
strutturalmente disgiunti. Qui non è una scelta ulteriore ma la
conseguenza diretta della struttura reale dell'albero: nessuna deviazione
dalla formula Merkle di T2.1, che resta language-agnostic e invariata.

**Nuova dipendenza Rust, versione esatta**: `tree-sitter-javascript =
"0.25.0"` (stessa versione di `tree-sitter-go`, stessa versione core
`tree-sitter` 0.26.11 già presente, nessun conflitto).

Nessun'altra deviazione. Nessuna risoluzione cross-file, nessuna
estrazione di function expression/arrow function assegnate a variabile,
nessun `Interface`, nessuna estensione di `merkle_hash`/`doc_hash`/
`chunk_entity`, nessuna modifica allo schema DB — tutto come da §5
Non-goals.
