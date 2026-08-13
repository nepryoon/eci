# SPEC-026 — TypeScript: estrazione + risoluzione (T2.5, parte 3/3)
Stato: verified
Task-tree: T2.5 (terzo e ultimo dei tre sotto-task concordati — chiude T2.5 e Fase 2) · Servizio: services/ingestion (Rust, estende SPEC-024/025) · ADD: Modulo 1 §1.2-1.4, con deviazione dichiarata su resolution (ADR-0006, invariata — vale anche qui)
Contratti: possibile estensione del vincolo `rel_type` su `code_relation` se IMPLEMENTS/EXTENDS non sono già valori ammessi — **da verificare durante l'implementazione**, non presunto; se serve, ADR dedicata (stesso principio di ADR-0006)

## 1. Obiettivo
Terzo linguaggio della pipeline. Due parti: (a) estrazione, che introduce per la prima volta da Go **Interface** come entità reale (SPEC-024 §2 l'aveva dichiarata Non-goal per JS puro — "tornerà rilevante nella parte 3/3") e, novità assoluta, **IMPLEMENTS/EXTENDS** come relazioni esplicite — TypeScript le dichiara con sintassi diretta (`class X implements Y`), risolvibile dal solo albero sintattico senza inferenza di tipo, a differenza del limite dichiarato per Go in SPEC-019 (g05, mai risolto: "nessun arco IMPLEMENTS esiste nella pipeline... analisi semantica dei tipi, non parsing sintattico"); (b) risoluzione cross-file, che **riusa** (non reimplementa) `extract_imports`/`resolve_cross_file_calls` di SPEC-025 — la sintassi `import`/`export` di TypeScript è la stessa di JavaScript per costruzione grammaticale.

## 2. Interfaccia

**Crate**: `tree-sitter-typescript`, grammatica `LANGUAGE_TYPESCRIPT` (non `LANGUAGE_TSX` — dialetto realmente diverso, verificato non presunto: `.tsx`/JSX resta Non-goal esplicito, vedi §5).

**Dispatch**: estende quello di SPEC-024 — `.ts` → nuova `parse_ts_file`, stesso ruolo di `parse_js_file`.

**Estrazione, riuso e novità rispetto a SPEC-024**:
- File/Class/Method/Function/CONTAINS/CALLS: stesso pattern di `parse_js_file`, verificato empiricamente (non presunto) che i kind dei nodi Tree-sitter coincidano tra le due grammatiche prima di scrivere il codice — se divergono anche solo in parte, documentare la divergenza invece di forzare un riuso che non regge.
- **`Interface`** — `interface_declaration`, campo `name`. Il corpo contiene method signature senza implementazione (`method_signature`, kind esatto da confermare empiricamente): estratte come nodi `Method` **anche senza corpo** — mirror diretto del precedente Go (`Notifier`, SPEC-013 §3 scenario 4), stessa convenzione di id qualificato `InterfaceName.methodName`.
- **IMPLEMENTS** — dalla clausola `implements` di un `class_declaration` (campo dedicato, kind esatto da confermare empiricamente): un arco `IMPLEMENTS` da Class a Interface, **solo quando l'Interface referenziata è definita nello stesso file** (risoluzione cross-file di IMPLEMENTS è fuori scope qui, vedi §5 — non un oversight, un confine dichiarato per tenere questa SPEC trattabile).
- **EXTENDS** — dalla clausola `extends` di un `class_declaration` (Class→Class) o di un `interface_declaration` (Interface→Interface), stesso principio di IMPLEMENTS: solo intra-file.

**Risoluzione cross-file CALLS**: nessun codice nuovo — `extract_imports`/`resolve_cross_file_calls` (SPEC-025) applicate invariate all'output di `parse_ts_file`. Verificare empiricamente che i kind `import_statement`/`import_clause`/`named_imports`/`import_specifier` coincidano; se sì, nessuna modifica a `imports.rs`/`resolve.rs` è necessaria — se no, documentare la divergenza come deviazione, non adattare silenziosamente i due moduli in modi che potrebbero rompere il comportamento già verificato per JS.

**Fixture**: nuovo `tests/fixtures/sample-repo-ts/`, stessa struttura concettuale di Go/JS:
```typescript
// notifier.ts
interface Notifier {
  notify(message: string): void;
}
class EmailNotifier implements Notifier {
  notify(message: string): void { console.log(message); }
}
```
```typescript
// order_service.ts
import { computeTotal } from './util';
class OrderService {
  minItems: number;
  constructor(minItems: number) { this.minItems = minItems; }
  process(prices: number[]): number {
    this.validate(prices);
    return computeTotal(prices);
  }
  validate(prices: number[]): void {
    if (prices.length < this.minItems) { throw new Error("too few items"); }
  }
}
```
```typescript
// util.ts
export function computeTotal(prices: number[]): number {
  return prices.reduce((sum, p) => sum + p, 0);
}
```

## 3. Comportamento (scenari)

1. **Dato** `notifier.ts`, **quando** parso il file, **allora** ottengo Interface(`Notifier`) con Method(`notify`, senza corpo), Class(`EmailNotifier`) con Method(`notify`, con corpo) — due nodi Method distinti con lo stesso nome semplice ma id diverso (qualificati `Notifier.notify` vs `EmailNotifier.notify`).
2. **Dato** lo stesso file, **quando** ispeziono le relazioni, **allora** trovo un arco IMPLEMENTS `EmailNotifier`→`Notifier`.
3. **Dato** `order_service.ts` e `util.ts`, **quando** applico `resolve_cross_file_calls` (riusata da SPEC-025, non reimplementata), **allora** ottengo l'arco CALLS cross-file `process`→`computeTotal`, stesso comportamento già verificato per JS.
4. **Dato** lo stesso file, **quando** ispeziono le relazioni intra-file, **allora** trovo `process`→`validate`.
5. **Dato** `order_service.ts`, dove ogni parametro/proprietà ha un'annotazione di tipo esplicita (`prices: number[]`, `minItems: number`), **quando** estraggo i nodi, **allora** i nomi di Method/Function restano corretti — le annotazioni di tipo non contaminano l'estrazione del nome (verifica diretta che il codice riusato da JS non si comporti diversamente in presenza di sintassi che JS non ha mai).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Una clausola `implements`/`extends` che referenzia un nome NON definito nello stesso file (es. un'interfaccia importata da un altro modulo) | Nessun arco prodotto — fuori scope dichiarato (§2, §5), non un errore |
| Un'interfaccia con una property signature (non un metodo, es. `readonly id: string;`) nel corpo | Non estratta come `Method` (non lo è) — nessun nodo prodotto per lei, nessun crash |
| File `.tsx` passato al dispatch | Comportamento esplicitamente non definito da questa SPEC — se il dispatch lo instrada comunque a `parse_ts_file` (stessa estensione-base `.ts` non combacia con `.tsx`, quindi in realtà cadrebbe nel ramo "estensione non riconosciuta" già esistente da SPEC-024) verificarlo esplicitamente con un test, non lasciarlo implicito |

## 5. Non-goals
`.tsx`/JSX (grammatica realmente diversa, `LANGUAGE_TSX` non usata qui). IMPLEMENTS/EXTENDS cross-file (solo intra-file in questa SPEC — un'interfaccia importata da un altro modulo e implementata localmente non produce l'arco). Generici (`class Box<T>`) — nessuna gestione speciale dichiarata, il nome della Class/Interface si assume estraibile allo stesso modo indipendentemente dai parametri di tipo, ma non testato esplicitamente. Namespace/moduli TypeScript (`namespace X {}`) — costrutto raro nel codice TS moderno (preferito ES module), non trattato.

## 6. Vincoli dall'ADD
Stesso principio di SPEC-024/025 — pattern di estrazione Tree-sitter coerente, deviazione sulla tecnologia di resolution già coperta da ADR-0006 (che si applica a TypeScript tanto quanto a JavaScript, stessa motivazione, nessuna ADR aggiuntiva necessaria per questo aspetto specifico).

## 7. Test plan
Test unitari (`cargo test`), fixture reale per gli scenari possibili, casi dedicati inline dove necessario (edge case tabella §4).

## 8. Osservabilità
Nessun requisito nuovo — stesso span `parse_file` con `language="typescript"`.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con conteggi/confronti espliciti.
- [x] Edge case tabella §4 tutti verificati esplicitamente, incluso il comportamento reale su `.tsx`.
- [x] Verificato esplicitamente (non presunto) se `extract_imports`/`resolve_cross_file_calls` funzionano invariati su TS — riportato nel report anche se il risultato è "sì, invariati", non solo in caso di divergenza.
- [x] Verificato se il vincolo `rel_type` esistente ammette già IMPLEMENTS/EXTENDS o se serve un'estensione dichiarata (con ADR se necessario).
- [x] Nessuna regressione sui test esistenti di SPEC-024/025 (Go e JS invariati).

## 10. Implementazione — deviazioni

**Punto 3 delle istruzioni (vincolo `rel_type`)**: verificato leggendo
`contracts/sql/migrations/0001_init.up.sql` riga 27-29 — `IMPLEMENTS` ed
`EXTENDS` sono **già** tra i valori ammessi dal `CHECK` su
`code_relation.rel_type` (elenco completo: `CALLS, IMPORTS, EXTENDS,
IMPLEMENTS, CONTAINS, DEPENDS_ON, REFERENCES, OVERRIDES, DERIVED_FROM,
GOVERNED_BY, CITES`), presumibilmente riservati fin da SPEC-005 in
previsione di un linguaggio che li avrebbe usati. **Nessuna migrazione,
nessuna ADR necessaria per questo punto** — `contracts/sql/migrations`
non è stato toccato.

**Punto 2 delle istruzioni (confronto kind Tree-sitter JS vs TS)**,
verificato con `to_sexp()` reale PRIMA di scrivere `parse_ts_file`, non
presunto — riportato esplicitamente come richiesto, incluse le parti
"invariate":
- **Invariati rispetto a JS** (SPEC-024/025): `program` (root),
  `function_declaration` (nome: `identifier`), `method_definition`
  (nome: `property_identifier`, campo `body` opzionale), `class_body`
  (figli `method_definition` diretti), `call_expression`/
  `member_expression` (campi `function`/`object`/`property` identici),
  `new_expression` (mai `call_expression`, stesso comportamento),
  `import_statement`/`import_clause`/`named_imports`/`import_specifier`/
  `export_statement` (identici byte per byte nella struttura, campi
  inclusi) — **`extract_imports`/`resolve_cross_file_calls` riusati
  SENZA modifiche alla logica di estrazione import né ai loro test
  esistenti** (unica modifica a `resolve.rs` è nel fallback di
  risoluzione percorso, vedi sotto — non nell'estrazione import).
- **Divergenti, richiesti da questa SPEC** (novità assolute rispetto a
  Go/JS, non presenti prima): `class_declaration`/`interface_declaration`
  hanno campo `name` di kind **`type_identifier`**, non `identifier`
  come in JS puro — irrilevante in pratica perché l'estrazione accede
  sempre per NOME di campo (`child_by_field_name("name")`), mai per kind
  del nodo restituito, quindi nessun codice diverso necessario, ma la
  divergenza di kind è reale e va segnalata come richiesto. `interface_body`
  contiene `method_signature` (senza campo `body`: nessuna
  implementazione) e `property_signature` (mai un Method). `class_heritage`
  (figlio diretto di `class_declaration`, non un campo) contiene
  `extends_clause` (campo `value`, kind **`identifier`** — asimmetria
  reale: diverso da `implements_clause`, i cui figli `type_identifier`
  ripetuti non hanno field name proprio) e/o `implements_clause`.
  `interface_declaration`'s `extends_type_clause` (figlio diretto) ha
  campo `type` RIPETUTO (uno per ciascun target, per `interface X extends
  Y, Z`).

**Deviazione reale scoperta con la verifica empirica del punto 2, non
ipotetica — unica modifica di codice a `resolve.rs`**: il fixture
dichiarato da SPEC-026 §2 usa `import { computeTotal } from './util';`
— uno specifier SENZA estensione (diverso dal fixture JS di SPEC-025,
che usava `'./util.js'`). `resolve_module_path` (SPEC-025) tentava solo
il fallback `.js` per specifier privi di estensione: verificato con un
test fallito PRIMA di qualunque correzione (non assunto) che questo
lascia `process`→`computeTotal` non risolto contro `util.ts`. Esteso il
fallback a provare `.js` **e** `.ts` (non solo `.js`) — unica riga di
comportamento nuovo in `resolve.rs`, il resto (estrazione import,
lookup per nome, gestione alias/duplicati/cicli) resta esattamente
quello di SPEC-025. Questa è la deviazione dal "nessun codice nuovo"
atteso da §2 per la risoluzione cross-file, motivata dal fallimento
empirico osservato, non da un'estensione preventiva non richiesta.

**`.tsx`**: confermato con un test dedicato
(`edge_case_tsx_file_falls_into_unsupported_extension_panic`) che
`parse_file("component.tsx", ...)` cade nel ramo "estensione non
supportata" già esistente da SPEC-024 (`.tsx` non combacia con `.ts`) —
comportamento verificato esplicitamente, non lasciato implicito, come
richiesto da §4.

**Nuova dipendenza Rust, versione esatta**: `tree-sitter-typescript =
"0.23.2"` — usa la costante `LANGUAGE_TYPESCRIPT` (non `LANGUAGE_TSX`,
grammatica JSX realmente diversa, mai usata in questa SPEC per costruzione
del dispatch — `.tsx` non raggiunge mai `parse_ts_file`).

Nessun'altra deviazione. Nessun supporto `.tsx`/JSX, nessuna risoluzione
cross-file di IMPLEMENTS/EXTENDS, nessuna gestione speciale dei generici,
nessun supporto per `namespace` — tutto come da §5 Non-goals.
