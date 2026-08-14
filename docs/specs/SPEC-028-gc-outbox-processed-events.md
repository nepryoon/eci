# SPEC-028 — GC periodica di outbox/processed_events (T2.6, parte 2/2)
Stato: verified
Task-tree: T2.6 (secondo dei due sotto-task concordati — chiude T2.6 e Fase 2) · Nuovo: tools/gc-postgres (Go, stesso pattern di tools/migrate-neo4j già esistente) · ADD: Modulo 1 §1.6.6, §2.2
Contratti: nessuno sotto contracts/ (nessuna modifica di schema — solo `DELETE` su tabelle esistenti)

## 1. Obiettivo
Le tabelle `outbox` (T1.2) e `processed_events` (T1.3) crescono senza limite — nessun meccanismo di purge esiste oggi. L'ADD lo segnala esplicitamente come rischio (§1.6.6: "vanno anch'esse purgate periodicamente per evitare crescita illimitata"; §2.2 lo ripete per `outbox` in un contesto di monitoraggio/riconciliazione). Un tool standalone che elimina le righe più vecchie di una soglia configurabile, per entrambe le tabelle indipendentemente.

## 2. Interfaccia

**Nuovo tool** `tools/gc-postgres/` (Go, stesso scheletro di `tools/migrate-neo4j` — un piccolo binario standalone, non un servizio long-running):
```
gc-postgres --dsn <postgres-dsn> [--outbox-retention 168h] [--processed-events-retention 168h] [--dry-run]
```
Default dichiarato per entrambe le soglie: **168h (7 giorni)** — non specificato dall'ADD, scelto come corrispondenza ragionevole con la retention di default comune di Kafka (rilevante soprattutto per `processed_events`, la cui funzione è protezione da ri-consegna entro la finestra di retention del topic). Configurabile via flag o `env_or_default` (stesso principio già usato altrove nel progetto — `GC_OUTBOX_RETENTION`/`GC_PROCESSED_EVENTS_RETENTION`), non hardcoded.

**Comportamento**: `DELETE FROM outbox WHERE created_at < now() - $1::interval` e `DELETE FROM processed_events WHERE <colonna timestamp reale, da verificare contro la DDL esistente — non presumere il nome> < now() - $2::interval`, ciascuna in una propria transazione breve. `--dry-run`: stampa il conteggio di righe che SAREBBERO eliminate per ciascuna tabella, senza eseguire il `DELETE`.

**Nessuna infrastruttura di scheduling**: questo tool è invocabile manualmente o da un cron/systemd timer esterno — configurare quello scheduling è fuori scope (stesso principio già dichiarato in SPEC-027 §5 per il trigger automatico della lineage).

## 3. Comportamento (scenari)

1. **Dato** righe in `outbox` più vecchie della soglia di retention e righe più recenti, **quando** eseguo il tool, **allora** solo le righe più vecchie vengono eliminate, quelle recenti restano.
2. **Dato** lo stesso stato per `processed_events`, **quando** eseguo il tool, **allora** stesso comportamento — le due tabelle sono purgate indipendentemente, ciascuna con la propria soglia.
3. **Dato** l'opzione `--dry-run`, **quando** eseguo il tool su uno stato con righe vecchie note, **allora** ottengo il conteggio corretto di righe che sarebbero eliminate, ma le righe restano tutte presenti dopo l'esecuzione.
4. **Dato** una tabella vuota, **quando** eseguo il tool, **allora** nessun errore — zero righe eliminate, comportamento normale non un caso speciale.
5. **Dato** soglie di retention diverse per le due tabelle (es. `--outbox-retention 720h --processed-events-retention 24h`), **quando** eseguo il tool, **allora** ciascuna tabella rispetta la propria soglia indipendentemente — verifica diretta che le due soglie non si confondano tra loro.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| DSN Postgres non raggiungibile | Errore esplicito, exit code non-zero, nessun tentativo silenzioso di continuare |
| Valore di retention non parsabile come durata (es. `"abc"`) | Errore esplicito alla lettura della configurazione (fail-fast), stesso principio già stabilito per la configurazione di budget in T2.2 |
| Il `DELETE` su `outbox` fallisce ma quello su `processed_events` avrebbe successo (es. lock/timeout su una sola tabella) | Le due operazioni sono indipendenti (transazioni separate, §2) — un fallimento sull'una non deve impedire il tentativo sull'altra; l'exit code finale riflette se ALMENO UNA delle due è fallita |

## 5. Non-goals
Nessuna infrastruttura di scheduling automatico (§2). Nessuna GC delle voci della Semantic Cache (dipende da un consumatore reale non ancora esistente, stesso principio già dichiarato in SPEC-027 §5). Nessuna modifica allo schema. Nessuna metrica/osservabilità dedicata oltre l'output testuale del tool stesso.

## 6. Vincoli dall'ADD
§1.6.6/§2.2: purge periodica di `outbox`/`processed_events` per evitare crescita illimitata — esattamente l'obiettivo di questa SPEC, nessuna reinterpretazione.

## 7. Test plan
Test di integrazione con Postgres reale (testcontainers, stesso principio già stabilito in tutto il progetto — non un DB in memoria), popolando righe con timestamp noti (alcuni prima, alcuni dopo la soglia) per verificare gli scenari con precisione, non approssimativamente.

## 8. Osservabilità
Output testuale con il conteggio di righe eliminate (o che sarebbero eliminate, in `--dry-run`) per ciascuna tabella — sufficiente per un tool invocato da cron, nessun requisito OTel dedicato.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con conteggi espliciti.
- [x] Edge case tabella §4 tutti verificati esplicitamente.
- [x] Nome reale della colonna timestamp di `processed_events` verificato contro la DDL esistente, non presunto (vedi §2).

## 10. Implementazione — deviazioni

**Nomi colonna verificati contro la DDL reale** (§2, punto esplicito del
prompt di lavoro): letto `contracts/sql/migrations/0001_init.up.sql`
righe 39-54. `outbox.created_at` — nome presunto corretto. `processed_events`
usa **`processed_at`**, non `created_at` (nome esplicitamente da non
presumere, verificato riga 53) — usato correttamente in tutta
l'implementazione e nei test.

**`make_interval(secs => $1)` invece di `$1::interval`**: lo schizzo SQL
di §2 passava la soglia come stringa castata a `interval`. Implementato
invece passando `retention.Seconds()` (float8) a `make_interval(secs =>
$1)` — stesso identico comportamento (`DELETE ... WHERE colonna < now() -
<intervallo>`), ma senza dipendere dal formato testuale prodotto da
`time.Duration.String()` di Go (es. `"168h0m0s"`), la cui compatibilità
con il parser di interval literal di Postgres non era garantita senza
verifica diretta — `make_interval` accetta un numero, elimina
l'ambiguità alla radice.

**`lock_timeout` per-transazione (2s), non richiesto esplicitamente da
§2**: aggiunto (`SET LOCAL lock_timeout` dentro la transazione di
ciascuna tabella) per due motivi — (a) rende deterministico e testabile
l'edge case §4 "il DELETE su outbox fallisce ma quello su
processed_events avrebbe successo (es. lock/timeout)", verificato con un
lock `ACCESS EXCLUSIVE` reale tenuto da una connessione separata in
`TestRunWithDBIndependentFailureDoesNotBlockTheOtherTable`; (b) un tool
di GC invocato da cron non dovrebbe poter restare bloccato a tempo
indeterminato su un lock contentato — coerente con il principio generale
del progetto contro le attese non limitate (deadline budget gRPC, ADD).

**Wiring in `Taskfile.yml`/`task lint`/`task test`/`task test:integration`
non incluso**: `tools/gc-postgres` non è agganciato all'automazione
Taskfile (che elenca esplicitamente `tools/migrate-neo4j` come comando a
parte) — `Taskfile.yml` non è tra i file toccabili di questa sessione
(`tools/gc-postgres`, questa SPEC) e nessun criterio di accettazione lo
richiede. Verificato manualmente con `go build ./...`, `go vet ./...`,
`go vet -tags=integration ./...`, `gofmt -l .` (pulito), `go test ./...`
(7 test, unit, Postgres non richiesto) e `go test -tags=integration
-v ./...` (12 test totali, Postgres reale via testcontainers — 5
scenari di integrazione, tutti verdi). Eseguibile manualmente con:
`cd tools/gc-postgres && go test -tags=integration ./...`.

**Verifica reale del binario, non solo dei test** (§9): oltre alla suite
di test, il binario compilato è stato eseguito manualmente
(`go run .`) contro un `postgres:17` reale con righe di età nota,
verificando `--dry-run` (conteggio corretto, nessuna riga eliminata),
l'esecuzione reale (righe vecchie eliminate, recenti conservate) e i tre
percorsi di errore (`--dsn` mancante -> exit 2, retention non parsabile
-> exit 2, DSN irraggiungibile -> exit 1) con i messaggi e gli exit code
attesi.
