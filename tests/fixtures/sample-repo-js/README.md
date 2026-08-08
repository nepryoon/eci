# sample-repo-js — fixture di ingestion JavaScript (SPEC-024)

Stessa STRUTTURA concettuale del fixture Go (`tests/fixtures/sample-repo`,
SPEC-009) — non una coincidenza: rende i due direttamente comparabili
scenario per scenario. Dato in pasto al parser Tree-sitter (`tree-sitter-javascript`),
non eseguito/lintato come progetto Node reale (nessun `package.json`
necessario per questo scopo).

## Cosa esercita

| File | Nodi CPG attesi | Note |
|---|---|---|
| `main.js` | 1 `Function` (`main`) | Istanzia `OrderService` e chiama `.process` — su un'istanza, non risolvibile per nome, comunque cross-file |
| `order_service.js` | 1 `Class` (`OrderService`), 3 `Method` (`constructor`, `process`, `validate`) | `process` chiama `validate` **intra-file/intra-classe** e `computeTotal` **cross-file** |
| `util.js` | 1 `Function` (`computeTotal`) | Nessuna dipendenza da altri file |

Ogni file è anche un nodo `File`, con un arco `CONTAINS` verso ciascuna
entità di primo livello che dichiara — **e**, a differenza del fixture Go,
un arco `CONTAINS` anche da `Class` verso ciascuno dei suoi `Method`
(derivabile direttamente dall'albero Tree-sitter di JavaScript, dove
`class_body` contiene realmente i `method_definition` come figli — vedi
SPEC-024 §2).

## Relazione CALLS intra-file (T2.5/SPEC-024 DEVE catturarla)

`OrderService.process` → `OrderService.validate`, entrambe dichiarate in
`order_service.js` (chiamata `this.validate(prices)`, risolta per nome
sulla `property` di una `member_expression`, stesso principio della
`selector_expression` di Go).

## Relazioni CALLS cross-file/non risolvibili (NON catturate, per design)

1. `OrderService.process` (in `order_service.js`) → `computeTotal` (in
   `util.js`) — cross-file, stesso limite dichiarato del fixture Go.
2. `main` (in `main.js`) → `service.process(...)` — chiamata su
   un'istanza (`member_expression` la cui `property` è `process`), non
   risolvibile senza analisi di tipo; anche se lo fosse, sarebbe comunque
   cross-file (`process` è dichiarato in `order_service.js`).
3. `new OrderService(2)` (in `main.js`) — produce un nodo Tree-sitter
   `new_expression`, non `call_expression`: non entra nemmeno nel
   percorso di risoluzione CALLS (che cerca solo `call_expression`), non
   per un filtro esplicito ma perché è strutturalmente un nodo diverso.

## Mappatura linguaggio → CPG

`class_declaration` → `Class` · `method_definition` (dentro una classe,
incluso `constructor`/metodi statici/getter/setter) → `Method` ·
`function_declaration` di primo livello → `Function` · file → `File`.
Nessun `Interface` (non esiste in JavaScript puro — arriverà con la parte
3/3, TypeScript). Nessuna function expression/arrow function assegnata a
variabile viene estratta (SPEC-024 §2, Non-goal dichiarato).
