# sample-repo — fixture di ingestion (SPEC-009)

Mini-progetto Go autonomo (proprio `go.mod`), **mai buildato/testato come
parte del resto del repo `eci`** — è dato in pasto al parser Tree-sitter
(T1.1), non compilato come dipendenza di `eci`. `go build ./...`/`go vet
./...` qui dentro servono solo a garantire che sia Go valido, non
pseudocodice.

## Cosa esercita

| File | Nodi CPG attesi | Note |
|---|---|---|
| `main.go` | 1 `Function` (`main`) | Chiama `OrderService.Process` — **cross-file** |
| `order_service.go` | 1 `Class` (`OrderService`), 2 `Method` (`Process`, `Validate`) | `Process` chiama `Validate` **intra-file** e `computeTotal` **cross-file** |
| `notifier.go` | 1 `Interface` (`Notifier`), 1 `Class` (`EmailNotifier`), 1 `Method` (`Notify`) | Nessuna chiamata in uscita |
| `util.go` | 1 `Function` (`computeTotal`) | Nessuna dipendenza da altri file |

Ogni file è anche un nodo `File`, con un arco `CONTAINS` verso ciascuna
entità che dichiara.

## Relazione CALLS intra-file (T1.1 DEVE catturarla)

`OrderService.Process` → `OrderService.Validate`, entrambe dichiarate in
`order_service.go`. È l'unico caso nel fixture pensato per dimostrare che
T1.1 cattura correttamente `CALLS` quando chiamante e chiamato sono nello
stesso file.

## Relazioni CALLS cross-file (T1.1 NON deve catturarle, per design)

Il fixture contiene deliberatamente due chiamate che attraversano un
confine di file:

1. `main` (in `main.go`) → `OrderService.Process` (in `order_service.go`)
2. `OrderService.Process` (in `order_service.go`) → `computeTotal` (in
   `util.go`)

Il codice sorgente le contiene normalmente (è normale che codice reale
abbia dipendenze cross-file), ma l'output di T1.1 (walking skeleton) **non
deve produrre un arco `CALLS`** per nessuna delle due — la risoluzione
cross-file è rimandata a Fase 2 (T2.5). Questo è un limite di scope
verificato esplicitamente da questo fixture, non un buco silenzioso: vedi
`tests/golden/queries_v0.json` (query `g02`, `g03`) per le assertion
corrispondenti.

## Mappatura linguaggio → CPG

`struct` → `Class` · `interface` → `Interface` · funzione con receiver →
`Method` · funzione standalone → `Function` · file → `File` (con archi
`CONTAINS`).

Nota per chi estenderà il fixture: uno `struct` **senza** metodi resta
comunque un nodo `Class` (coerente con D3 — `Class` non richiede metodi).
Non è un caso presente in questo fixture (`OrderService` ed
`EmailNotifier` hanno entrambi almeno un metodo), ma la regola vale a
prescindere.
