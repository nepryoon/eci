# SPEC-009 — Fixture di ingestion (sample-repo) + golden dataset v0
Stato: verified
Task-tree: T0.10 (ultimo prerequisito di Fase 0, sblocca T1.1) · Servizio: tests/fixtures, tests/golden · ADD: Modulo 1 (CPG: File/Class/Interface/Method/Function/CallSite, archi CONTAINS/CALLS)
Contratti: nessuno (nessun file sotto contracts/ toccato)

## 1. Obiettivo
Costruire i due artefatti di test condivisi che sbloccano Fase 1: (a) un mini-progetto Go sotto `tests/fixtures/sample-repo/`, piccolo e mantenibile a mano, ma costruito deliberatamente per esercitare in modo verificabile i nodi File/Class/Interface/Method/Function/CallSite e gli archi CONTAINS/CALLS che T1.1 (ingestion v0, walking skeleton) deve produrre — includendo sia relazioni intra-file (che T1.1 DEVE catturare) sia una relazione cross-file (che T1.1, per design, NON deve ancora catturare, essendo la risoluzione cross-file rimandata a Fase 2); (b) un golden dataset v0 — 10 query in linguaggio naturale abbinate ai fatti strutturali attesi dal recupero, non a testo di risposta libero — usato come regressione automatica da T1.6 in poi.

## 2. Interfaccia

**Fixture** — `tests/fixtures/sample-repo/`, modulo Go autonomo (`go.mod` proprio, mai buildato/testato dal resto del repo — è dato in pasto al parser, non compilato come parte di `eci`):

- `main.go`: `func main()` che istanzia `OrderService` e chiama `Process` — **cross-file** rispetto a dove `OrderService`/`Process` sono definiti (in `order_service.go`). Deliberato: verifica che T1.1 NON produca un arco `CALLS` per questa chiamata (fuori scope walking-skeleton).
- `order_service.go`: `type OrderService struct{...}`; `func (s *OrderService) Process(...) error` che chiama **nello stesso file** `s.Validate(...)` e la funzione standalone `computeTotal(...)` (quest'ultima definita in `util.go` — **anche questa cross-file**, stesso motivo: deve restare senza arco `CALLS` a questo stadio); `func (s *OrderService) Validate(...) error`.
- `notifier.go`: `type Notifier interface { Notify(...) error }`; `type EmailNotifier struct{...}`; `func (e *EmailNotifier) Notify(...) error` — nessuna chiamata in uscita, serve a esercitare il nodo `Interface` e la relazione di implementazione.
- `util.go`: `func computeTotal(items []Item) float64` — funzione standalone, nessuna dipendenza da altri file.

Requisito esplicito, **non negoziabile per l'utilità del fixture**: il fixture deve contenere **almeno una relazione CALLS genuinamente intra-file** (`Process` → `Validate`, stesso file `order_service.go`) — è l'unico caso che T1.1 deve dimostrare di catturare correttamente. Le chiamate cross-file (`main`→`Process`, `Process`→`computeTotal`) restano nel codice sorgente (è normale, il codice reale ha dipendenze cross-file) ma **non devono generare un arco CALLS** nell'output di T1.1 — è il confine di scope da verificare esplicitamente, non un'omissione da correggere.

Mappatura linguaggio→CPG (Go non ha "classi" in senso stretto): `struct` → nodo `Class`; `interface` → nodo `Interface`; funzione con receiver → nodo `Method`; funzione standalone → nodo `Function`; file → nodo `File` con archi `CONTAINS` verso le entità che dichiara.

**Golden dataset** — `tests/golden/queries_v0.json`, array di 10 oggetti, ciascuno con questa forma:
```json
{
  "id": "g01",
  "query": "Chi chiama Validate?",
  "expected_facts": {
    "callers": ["OrderService.Validate <- OrderService.Process"]
  },
  "scope_note": "intra-file, deve essere trovato da T1.1"
}
```
Copertura richiesta tra le 10 query (non tutte devono avere `scope_note` uguale — mix deliberato):
- ≥1 query che interroga la relazione CALLS intra-file attesa (`Process`→`Validate`) — deve risultare TROVATA.
- ≥1 query che interroga esplicitamente una delle due relazioni cross-file (`main`→`Process` o `Process`→`computeTotal`) — deve risultare **assente** dal risultato a questo stadio, con `scope_note` che lo dichiara esplicitamente (non un buco silenzioso, un limite noto e verificato).
- ≥1 query sui membri di una `Class` (es. "quali metodi ha OrderService?").
- ≥1 query su un'`Interface` e la sua implementazione.
- ≥1 query sul contenuto di un `File` (es. "cosa contiene order_service.go?").
- Le restanti, a discrezione, purché ciascuna sia verificabile contro fatti strutturali del fixture (nodi/archi attesi), non contro testo libero — a questo stadio non c'è un LLM reale a generare risposte da confrontare, solo il layer di recupero grafo.

## 3. Comportamento (scenari)

1. **Dato** il fixture, **quando** un parser Tree-sitter Go lo processa, **allora** produce esattamente: 4 nodi `File`, 2 nodi `Class` (`OrderService`, `EmailNotifier`), 1 nodo `Interface` (`Notifier`), i nodi `Method`/`Function` corrispondenti a ogni dichiarazione, e un arco `CONTAINS` da ciascun `File` verso ciò che dichiara.
2. **Dato** lo stesso output, **quando** cerco l'arco `CALLS` da `Process` a `Validate`, **allora** esiste (stesso file, walking-skeleton lo cattura).
3. **Dato** lo stesso output, **quando** cerco un arco `CALLS` da `main` a `Process` o da `Process` a `computeTotal`, **allora** NON esiste — conferma che il limite "solo intra-file" è rispettato, non superato né mancato per difetto.
4. **Dato** il golden dataset, **quando** ne valido la struttura (schema JSON minimo: `id`, `query`, `expected_facts`, `scope_note` tutti presenti), **allora** tutte e 10 le voci sono conformi e coprono i cinque casi elencati in §2.
5. **Dato** il fixture, **quando** eseguo un secondo parsing identico (idempotenza, stesso principio di Merkle hashing del Modulo 1), **allora** produco lo stesso identico insieme di nodi/archi — nessuna differenza tra due run sullo stesso input.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Il fixture viene accidentalmente reso più complesso in futuro (es. aggiunta di altri file) senza aggiornare il golden dataset | Un test di coerenza (non in questa SPEC, ma da tenere a mente per T1.6) dovrebbe fallire se il golden dataset assume fatti non più veri — qui ci si limita a costruire i due artefatti coerenti tra loro al momento della creazione |
| Ambiguità nella mappatura Go struct-senza-metodi → `Class` vs qualcos'altro | Un `struct` senza metodi resta comunque un nodo `Class` (coerente con D3: `Class` non richiede metodi) — non è un caso presente in questo fixture (entrambi gli struct hanno almeno un metodo), ma la regola va dichiarata esplicitamente per chi estenderà il fixture in futuro |

## 5. Non-goals
Nessuna risoluzione cross-file reale (Fase 2, T2.5) — le relazioni cross-file nel fixture esistono apposta per verificare che NON vengano catturate ora, non per essere risolte. Nessuna valutazione di qualità delle risposte in linguaggio naturale (richiede un LLM reale o un giudice, non disponibile a questo stadio — il golden dataset verifica fatti strutturali di recupero, non testo generato). Nessuna integrazione con `task test`/CI in questa SPEC: i due artefatti sono dati statici, consumati dai test di Fase 1 (T1.1, T1.6), non hanno un proprio test runner.

## 6. Vincoli dall'ADD
Modulo 1: nodi `File`/`Class`/`Interface`/`Method`/`Function`/`CallSite`, archi `CONTAINS`/`CALLS`, esattamente lo schema già codificato in D3 (SPEC-004) — questo fixture è il primo caso reale che li esercita con dati concreti invece che con lo schema astratto.

## 7. Test plan
Nessun test automatico in questa SPEC (i due artefatti sono fixture statiche). La verifica è manuale/ispezione diretta: contare i file/struct/interface/metodi/funzioni nel fixture e confermare che corrispondano esattamente a quanto dichiarato in §3 scenario 1; validare lo schema JSON del golden dataset con un controllo diretto (`jq` o equivalente) sulle 10 voci.

## 8. Osservabilità
Non applicabile.

## 9. Criteri di accettazione
- [x] Il fixture Go compila da solo (`go build ./...` dentro `tests/fixtures/sample-repo/`) — verificato, `go build ./...` e `go vet ./...` entrambi puliti. Il binario prodotto (`sample-repo`) è escluso via `.gitignore` locale al fixture (non deve finire tracciato — non è codice sorgente).
- [x] Conteggio esatto: 4 file, 2 struct, 1 interface, corrispondenza 1:1 tra dichiarazioni e nodi attesi (scenario 1) — verificato per ispezione diretta (`grep -n "^type .* struct"` / `"^type .* interface"` su tutti i `.go`): `OrderService`/`EmailNotifier` (struct), `Notifier` (interface), nessun altro. 3 `Method` (`Process`, `Validate`, `Notify`), 2 `Function` standalone (`main`, `computeTotal`).
- [x] La relazione CALLS intra-file (`Process`→`Validate`) è verificabile per ispezione diretta del sorgente (scenario 2) — `order_service.go:18`, `s.Validate(prices)` dentro `Process`, stesso file.
- [x] Le due relazioni cross-file esistono nel sorgente ma sono esplicitamente annotate come "non catturate a questo stadio" nel commento del codice o in un README della fixture (scenario 3) — entrambe presenti (`main.go:12` chiama `Process`; `order_service.go:21` chiama `computeTotal`), annotate sia inline (commenti sopra ogni funzione coinvolta) sia in `tests/fixtures/sample-repo/README.md`.
- [x] `tests/golden/queries_v0.json` contiene esattamente 10 voci, tutte conformi allo schema minimo, con la copertura dei cinque casi richiesti in §2 — validato con `jq` (array di lunghezza 10, tutti i campi `id`/`query`/`expected_facts`/`scope_note` presenti e non nulli, `id` tutti univoci). Copertura: g01 (CALLS intra-file trovato), g02+g03 (entrambe le relazioni cross-file, non solo una), g04 (membri Class), g05 (Interface + implementazione), g06-g09 (contenuto di tutti e 4 i File), g10 (mappatura struct→Class).

## 10. Deviazioni rispetto alla SPEC

1. **`computeTotal` prende `[]float64`, non uno struct `Item` dedicato**:
   la SPEC (§1) parla di "computeTotal(items []Item) float64" solo
   nell'obiettivo generale, senza fissare la firma esatta in §2. Uno
   struct `Item` avrebbe introdotto un **terzo** nodo `Class` nel fixture,
   in conflitto diretto con lo scenario 1 (§3), che richiede **esattamente
   2** nodi `Class` (`OrderService`, `EmailNotifier`). Usato `[]float64`
   per evitare il conflitto, mantenendo la logica di calcolo (somma dei
   prezzi) semanticamente equivalente.

2. **`.gitignore` locale in `tests/fixtures/sample-repo/`**: `go build
   ./...` (richiesto esplicitamente come criterio di accettazione) produce
   un binario `sample-repo` nella stessa directory. Ignorato esplicitamente
   per non tracciarlo per errore in un commit futuro — non previsto dalla
   SPEC ma necessario conseguenza del criterio di accettazione stesso.

3. **`tests/golden/README.md`** (placeholder preesistente da SPEC-001,
   `# golden`) lasciato invariato — la SPEC non richiede di espanderlo, e
   il file `queries_v0.json` è autodescrittivo (ogni voce ha `scope_note`).

4. **Scenario 5 (§3, idempotenza del parsing) non eseguito**: richiede un
   parser Tree-sitter reale, che è T1.1 (non ancora costruito) — coerente
   con l'istruzione esplicita di questa sessione ("non serve un parser
   vero, quello è T1.1"). Il fixture è comunque deterministico per
   costruzione (nessuna generazione a runtime, file statici), quindi non
   c'è motivo strutturale per cui un futuro parser Tree-sitter produrrebbe
   output diverso tra due run — ma non è stato verificato con un parser
   reale in questa sessione, resta da fare quando T1.1 esiste.

5. **Copertura del golden dataset oltre il minimo richiesto**: §2 chiede
   "≥1" query per ciascuno dei primi cinque casi. Implementate 2 query per
   le relazioni cross-file (g02 per `main`→`Process`, g03 per
   `Process`→`computeTotal`, invece di una sola) e 4 query di contenuto
   File (g06-g09, una per ciascuno dei 4 file, invece di una sola) — scelta
   deliberata per esercitare l'intero fixture nel golden dataset, entro il
   budget di 10 voci totali richiesto.
