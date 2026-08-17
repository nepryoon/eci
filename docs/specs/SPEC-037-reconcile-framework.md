# SPEC-037 — Framework di riconciliazione (T3.4, parte 1/4)
Stato: implemented
Task-tree: T3.4 (primo dei quattro sotto-task concordati — i tre plugin per vista sono le parti 2/4, 3/4, 4/4) · Nuovo: tools/reconcile (Go, stesso pattern di tools/gc-postgres/migrate-neo4j) · ADD: Modulo 1 §2.2 (riconciliazione periodica)

## 1. Obiettivo
Un job periodico (non un servizio long-running) che enumera i fingerprint reali da Postgres (fonte di verità), li confronta con lo stato di una vista downstream tramite una funzione di controllo specifica per quella vista, e per ogni mismatch ripubblica la riga `outbox` corrispondente — riattivando lo stesso meccanismo CDC che l'avrebbe scritta correttamente la prima volta, non un percorso di scrittura diretto verso la vista. Questa SPEC costruisce solo l'ORCHESTRAZIONE condivisa (enumera→confronta→ripubblica→report); i tre plugin specifici per Neo4j/Qdrant/OpenSearch sono SPEC separate (parti 2/4-4/4), innestati qui senza modificare questo framework.

## 2. Interfaccia

**Package** `tools/reconcile/internal/framework`:
```go
// SourceRow è UNA riga da riconciliare — id dell'entità Postgres +
// fingerprint atteso (forma dipendente dal plugin: ast_hash per Neo4j,
// esistenza+vettore per Qdrant, esistenza+testo per OpenSearch — questo
// framework non ne conosce la semantica, la tratta come byte opachi).
type SourceRow struct {
    ID          string
    Fingerprint []byte
}

// Target incapsula tutto ciò che serve per riconciliare UNA vista
// contro Postgres — le tre funzioni sono fornite dai plugin (parti 2/4-4/4),
// questo framework le orchestra soltanto.
type Target struct {
    Name       string
    SourceRows func(ctx context.Context, db *sql.DB) ([]SourceRow, error)
    Check      func(ctx context.Context, row SourceRow) (matches bool, err error)
    Republish  func(ctx context.Context, tx *sql.Tx, row SourceRow) error
}

type Report struct {
    Checked     int
    Matched     int
    Republished int
    Errored     []RowError // {RowID string, Err error} — mai un panic, un mismatch/errore su UNA riga non deve fermare le altre
}

func Reconcile(ctx context.Context, db *sql.DB, target Target) (Report, error)
```

**Comportamento di `Reconcile`**: per ciascuna `SourceRow` ottenuta da `SourceRows`, chiama `Check`; se `matches == true`, incrementa `Matched`; se `false`, apre una transazione, chiama `Republish`, la committa, incrementa `Republished`; se `Check` o `Republish` ritornano un errore, la riga finisce in `Errored` (con l'errore specifico), il processo CONTINUA con la riga successiva — mai un errore su una riga che interrompe l'intera riconciliazione.

**CLI** `tools/reconcile`: `reconcile --target=<nome>` — il binding tra `--target` e un `Target` concreto avviene nei tre plugin (parti 2/4-4/4), non in questa SPEC (nessun `Target` reale implementato qui, solo il framework e la CLI che lo invoca con un target FINTO nei propri test).

## 3. Comportamento (scenari)

1. **Dato** un insieme di righe dove tutte combaciano (`Check` sempre `true`), **quando** eseguo `Reconcile`, **allora** `Matched` uguaglia il totale, `Republished` è 0.
2. **Dato** un insieme dove alcune righe non combaciano, **quando** eseguo `Reconcile`, **allora** `Republish` viene chiamata SOLO per quelle, `Republished` ne riflette il conteggio esatto.
3. **Dato** una riga il cui `Check` ritorna un errore (vista temporaneamente irraggiungibile, simulato), **quando** eseguo `Reconcile`, **allora** quella riga finisce in `Errored`, le righe successive vengono comunque processate.
4. **Dato** una riga il cui `Republish` fallisce (simulato), **quando** eseguo `Reconcile`, **allora** stesso comportamento dello scenario 3 — nessuna riga blocca le altre.
5. **Dato** una riga dove `Republish` viene chiamata, **quando** ispeziono la transazione, **allora** o la riga `outbox` è scritta per intero o (in caso di fallimento) NULLA è scritto — nessuna scrittura parziale (stesso principio transazionale già stabilito ovunque in questo progetto).

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| `SourceRows` stessa fallisce (Postgres irraggiungibile) | Errore esplicito ritornato da `Reconcile` stessa (non un `Report` parziale) — senza l'elenco delle righe non c'è nulla da riconciliare |
| Zero righe da `SourceRows` | `Report` con tutti i contatori a zero, nessun errore — comportamento normale, non un caso speciale |

## 5. Non-goals
Nessun plugin reale per Neo4j/Qdrant/OpenSearch (parti 2/4-4/4). Nessuno scheduling automatico (job invocabile manualmente o da cron esterno, stesso principio già stabilito per `tools/gc-postgres`, SPEC-028). Nessuna deduplicazione di ripubblicazioni multiple nello stesso run (se una riga fallisce la riconciliazione di PIÙ viste in run separati, ciascun run la ripubblica indipendentemente — comportamento accettabile, un evento outbox ripubblicato più volte è idempotente per costruzione lungo tutta la pipeline).

## 6. Vincoli dall'ADD
Modulo 1 §2.2: riconciliazione periodica che ricalcola i fingerprint dalla fonte di verità e ripubblica gli eventi mancanti/divergenti — questa SPEC costruisce esattamente questo meccanismo, generico rispetto alla vista specifica.

## 7. Test plan
Test unitari con un `Target` FINTO (funzioni Go semplici, nessun bisogno di Postgres/Neo4j/Qdrant/OpenSearch reali per verificare la sola ORCHESTRAZIONE) — Postgres reale solo per verificare che `Republish` scriva davvero dentro una transazione vera (scenario 5, via testcontainers).

## 8. Osservabilità
Nessun requisito nuovo oltre all'output testuale del `Report`, stesso principio di `tools/gc-postgres`.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta.
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Nessuna regressione sui test esistenti.

## 10. Deviazioni

1. **Dettagli della CLI non dettati letteralmente da §2.** §2 dichiara solo l'uso (`reconcile --target=<nome>`) e che il binding `--target`→`Target` concreto avviene nei plugin (parti 2/4-4/4). Il come esatto non è specificato: implementato con `Config`/`ParseConfig` (flag `--dsn`, `--target`, entrambi obbligatori — fail-fast, stesso principio di `tools/gc-postgres` SPEC-028 §2) e un `OpenAndRun(ctx, cfg, targets map[string]Target, logf)` che risolve il nome contro una mappa passata esplicitamente da `main.go`. `main.go` dichiara `var targets = map[string]framework.Target{}`, vuota in questa SPEC (§5: nessun plugin reale ancora) — le parti 2/4-4/4 la popoleranno aggiungendo la propria voce, senza toccare `internal/framework` (coerente con "innestati qui senza modificare questo framework", §1). Nessuna registrazione globale a effetto collaterale (niente `init()`/registry package-level): scelta deliberata per mantenere `OpenAndRun` testabile con un target finto senza stato globale condiviso tra i test.

2. **Driver `database/sql` finto in-package per i test di orchestrazione (scenari 1-4, edge case).** §7 richiede esplicitamente "nessun bisogno di Postgres... reali per verificare la sola orchestrazione", ma §2 dichiara `Republish func(ctx, tx *sql.Tx, row SourceRow) error` — una `*sql.Tx` reale richiede comunque una connessione `database/sql` funzionante per `BeginTx`/`Commit`/`Rollback`, anche se il Target finto non vi esegue mai query. Risolto con un driver minimale (`fakedriver_test.go`, ~40 righe: `Open`/`Prepare`/`Close`/`Begin`/`Commit`/`Rollback`, nessun supporto Exec/Query perché mai esercitato dai test di orchestrazione) registrato una sola volta per processo di test — nessuna nuova dipendenza esterna (niente sqlmock/sqlite), coerente col vincolo di §7.

3. **Exit code della CLI non specificato esplicitamente da §8/§9.** Scelto lo stesso principio di `tools/gc-postgres` (SPEC-028): `main.go` esce con status 1 se `len(Report.Errored) > 0` (almeno una riga in errore), status 2 per un errore di parsing dei flag, status 1 per un errore di `OpenAndRun` stessa (target sconosciuto, DSN irraggiungibile, `SourceRows` fallita).

4. **`tools/reconcile` non è cablato in `Taskfile.yml`.** Stesso stato di `tools/gc-postgres` (mai aggiunto a `lint`/`test`/`test:integration` nonostante esista da SPEC-028) — `Taskfile.yml` non è nei file toccabili di questa SPEC, quindi non modificato; nessuna regressione introdotta, solo continuità con uno stato preesistente non causato da questa SPEC.

Nessun'altra deviazione rispetto al testo di §1-§9.
