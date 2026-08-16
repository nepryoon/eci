# SPEC-035 — Retry/backoff + DLQ (T3.3, parte 1/2)
Stato: implemented
Task-tree: T3.3 (primo dei due sotto-task concordati — metriche Prometheus è la parte 2/2, separata) · Nuovo: libs/go/eci/resilience (Go) + integrazione in 4 servizi esistenti · ADD: nessuna sezione specifica — resilienza operativa standard per consumer Kafka at-least-once

## 1. Obiettivo
Tutti e quattro i sink Kafka del progetto (`sink-graph`, `embedding-worker`, `sink-vector`, `sink-search`) condividono oggi lo stesso comportamento su un fallimento: non commit dell'offset, redelivery indefinita. Un messaggio che fallisce SEMPRE (contenuto genuinamente malformato, un bug del downstream) blocca l'intera partizione Kafka per sempre — il classico problema "poison pill". Libreria condivisa che aggiunge retry con backoff esponenziale (via header sul messaggio stesso, non stato in Postgres) fino a un limite, poi instrada su una coda DLQ dedicata invece di bloccare la partizione.

## 2. Interfaccia

**Nuovo package** `libs/go/eci/resilience` (stesso principio delle librerie condivise già esistenti — config/observability/kafkatrace/secctx):
```go
type ProcessFunc func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (Outcome, error)

type Config struct {
    MaxRetries  int           // default 5
    BackoffBase time.Duration // default 1s — raddoppia a ogni tentativo (1s,2s,4s,8s,16s)
}

// WithRetryAndDLQ avvolge la ProcessFunc di un sink (invariata,
// nessuna riscrittura della logica applicativa): su errore, legge
// l'header di retry-count (assente = 0) dal messaggio, applica il
// backoff, ripubblica sullo STESSO topic con l'header incrementato se
// sotto MaxRetries, altrimenti pubblica su "{topic}.DLQ" — in entrambi
// i casi l'offset ORIGINALE viene comunque committato (il chiamante
// avanza, il messaggio riappare più avanti nel topic con contatore
// incrementato, o finisce nella coda morta).
func WithRetryAndDLQ(cfg Config, producer *kafka.Writer, inner ProcessFunc) ProcessFunc
```

**Header di retry-count**: nome dichiarato `x-eci-retry-count`, valore decimale ASCII (stessa convenzione già in uso per `event_id`/`trace_id` — header Kafka semplici, non un formato binario). Assente = tentativo 0.

**Topic DLQ**: `{topic}.DLQ` (es. `outbox.event.CodeNode.DLQ`) — stessa convenzione a punti già in uso (`outbox.event.CodeChunk`), creato implicitamente da Kafka al primo publish (nessuna configurazione esplicita necessaria, stesso comportamento degli altri topic in questo stack dev).

**Integrazione nei quattro sink**: ciascun `main.go`/punto di avvio del consume-loop avvolge la propria `ProcessMessage` esistente con `resilience.WithRetryAndDLQ` prima di passarla al ciclo di consumo — nessuna modifica alla logica applicativa di `ProcessMessage` stessa in nessuno dei quattro servizi.

## 3. Comportamento (scenari)

1. **Dato** un messaggio che fallisce al primo tentativo ma la `ProcessFunc` interna avrebbe successo a un secondo tentativo (simulato), **quando** il wrapper lo gestisce, **allora** il messaggio viene infine processato con successo, nessun DLQ.
2. **Dato** un messaggio che fallisce SEMPRE, **quando** il wrapper lo gestisce, **allora** dopo esattamente `MaxRetries` tentativi il messaggio compare sul topic `{topic}.DLQ`, con l'header di retry-count al valore massimo.
3. **Dato** lo scenario 2, **quando** misuro i tempi tra i tentativi successivi, **allora** crescono in modo coerente con backoff esponenziale (non costanti) — verifica diretta dei tempi, non solo del conteggio finale.
4. **Dato** un messaggio ripubblicato per retry, **quando** ispeziono il suo header di retry-count, **allora** è incrementato esattamente di 1 rispetto al tentativo precedente — mai un doppio incremento, mai un reset.
5. **Dato** UNO dei quattro sink reali (rappresentativo — non tutti e quattro devono ripetere l'intera suite di scenari, la libreria condivisa già li copre esaustivamente), **quando** eseguo un test di integrazione con quel servizio reale, **allora** confermo che il wrapping è genuinamente cablato nel suo consume-loop — non presunto dal solo fatto che compili.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Un header di retry-count con un valore non numerico (corruzione improbabile ma possibile) | Trattato come tentativo 0 (fail-safe: non blocca il messaggio, ricomincia il conteggio) — non un crash |
| Il publish verso il topic DLQ stesso fallisce (Kafka irraggiungibile) | Errore propagato esplicitamente, offset NON committato in questo caso specifico — a differenza del percorso normale, qui non c'è un modo sicuro di "avanzare comunque" senza perdere il messaggio |

## 5. Non-goals
Nessuna metrica Prometheus (parte 2/2, prossima SPEC). Nessuna interfaccia di ripubblicazione/ispezione manuale dei messaggi in DLQ (fuori scope — un consumatore Kafka standard può già leggerli). Nessuna applicazione a `retrieval-engine`/`orchestrator`/`semantic-cache` (non sono consumer Kafka nel senso di questa SPEC).

## 6. Vincoli dall'ADD
Nessuno specifico — resilienza operativa standard per un'architettura a consumer Kafka at-least-once, coerente con l'intero principio "mai perdere un messaggio in silenzio" già seguito ovunque in questo progetto.

## 7. Test plan
Libreria condivisa: test con Kafka reale (testcontainers), non simulato — backoff/DLQ sono comportamenti temporali e di instradamento reali, non verificabili con un mock. Integrazione: un test rappresentativo su UNO dei quattro sink (scenario 5), non una ripetizione su tutti e quattro.

## 8. Osservabilità
Nessun requisito nuovo oltre al logging già esistente in ciascun sink — le metriche sono la parte 2/2.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta, in particolare lo scenario 3 (tempi reali, non solo conteggio).
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Wrapping applicato e verificato compilare/testare in tutti e quattro i sink (anche se solo uno riceve il test di integrazione completo, gli altri tre vanno comunque toccati e verificati con `go build`/`go test` reali).
- [x] Nessuna regressione sui test esistenti dei quattro servizi.

## 10. Deviazioni dall'implementazione

1. **Tipo `Outcome` (§2 dichiara solo la firma `(Outcome, error)`, senza
   enumerarne i valori)**: definito come `{OutcomeProcessed, OutcomeRetried,
   OutcomeDeadLettered}` — descrive cosa ha fatto IL WRAPPER (non l'Outcome
   specifico del sink applicativo, che ciascun servizio definisce
   localmente e che `resilience` non può conoscere). Pensato come punto di
   aggancio naturale per le metriche di T3.3 parte 2/2 (quante
   elaborazioni dirette/ritentate/finite in DLQ).
2. **`FetchAndProcess` (nei 4 sink) cambia firma**: da
   `(ctx, reader, deps Deps) (Outcome, error)` (Outcome specifico del
   servizio) a `(ctx, reader, process resilience.ProcessFunc)
   (resilience.Outcome, error)` — necessario per accettare la ProcessFunc
   già avvolta da `main.go` (§2: "prima di passarla al ciclo di
   consumo"), che per costruzione non può più portare l'Outcome specifico
   del servizio attraverso il confine generico di `resilience.ProcessFunc`.
   **Conseguenza per i test di integrazione ESISTENTI dei 4 sink** (tutti
   scritti prima di questa SPEC, con assert su `consumer.OutcomeMerged`/
   `OutcomeDuplicate`/`OutcomeStored`/ecc.): l'helper condiviso di ciascun
   file (`fetchAndProcessWithReader`/`fetchAndProcessOnce`, più due call
   site diretti in embedding-worker/scenario4) ora chiama `ProcessMessage`
   DIRETTAMENTE dentro un adapter locale (nessun `WithRetryAndDLQ` — quegli
   scenari testano la logica applicativa del sink, non retry/DLQ, già
   coperto esaustivamente da `libs/go/eci/resilience`) e cattura l'Outcome
   specifico del servizio tramite una variabile catturata per closure,
   ripristinando l'esatto comportamento di assert pre-esistente. Nessuna
   modifica alla logica applicativa di `ProcessMessage` stessa in nessuno
   dei quattro servizi, come richiesto da §2.
3. **`producer *kafka.Writer` con `BatchTimeout` basso (10ms)** — scoperto
   scrivendo lo scenario 3 del test di `libs/go/eci/resilience`: il
   default di kafka-go (1s) fa sì che ogni `WriteMessages` resti in attesa
   del flush del batch fino a un secondo intero anche per un solo
   messaggio, sommandosi SILENZIOSAMENTE al backoff configurato (osservato
   empiricamente: gap di ~1.08s/1.16s/1.32s invece degli ~80/160/320ms
   attesi). Applicato lo stesso `BatchTimeout` basso ai producer reali dei
   quattro sink (non solo nel test), altrimenti il backoff configurato in
   produzione sarebbe stato sistematicamente più lento del dichiarato.
4. **Bump di dipendenze transitive in `libs/go/eci/resilience`** (per
   `testcontainers-go/modules/kafka`, necessario al test §7) ha reso
   `services/retrieval-engine` (che referenzia `libs/go` via `replace`, ma
   NON è uno dei quattro sink né è nell'elenco dei file toccabili di
   questa SPEC) temporaneamente non compilabile (`go build`/`go test`
   fallivano con "updates to go.mod needed"). Corretto con un
   `go mod tidy` mirato in quel modulo — modifica meccanica di
   go.mod/go.sum, nessun cambiamento applicativo — perché lasciare una
   build realmente rotta nel repo è una regressione più grave del
   restare strettamente dentro l'elenco dichiarato. Verificato dopo la
   correzione: `go build`/`go vet`/`go test` di retrieval-engine di nuovo
   verdi, così come `task lint`/`task test` sull'intero repo.
5. **Sink rappresentativo per lo scenario 5 (§3/§7): `sink-graph`** —
   motivato dal fatto che è il PRIMO sink costruito in questo progetto
   (SPEC-015, T1.3) e il modello esplicito che gli altri tre dichiarano di
   replicare ("stesso scheletro di sink-graph" nei commenti di
   embedding-worker/sink-vector/sink-search) — la scelta più naturale per
   rappresentare il pattern comune ai quattro. Il test forza un
   fallimento DETERMINISTICO e sempre riproducibile puntando `Deps.DB` a
   un indirizzo Postgres irraggiungibile (il dedup via `processed_events`
   è il PRIMO passo di `ProcessMessage` per costruzione, quindi fallisce
   già lì per qualunque messaggio, prima di toccare Neo4j — `Deps.Neo4j`
   resta `nil`, mai dereferenziato) — nessun container Postgres/Neo4j
   necessario in questo test, solo Kafka reale, mantenendolo rapido
   (~40s) pur esercitando il consume-loop REALE (`consumer.FetchAndProcess`
   + `consumer.ProcessMessage` reali, non una reimplementazione) attraverso
   l'intero percorso retry (retryCount 0→1) poi DLQ (retryCount 1==MaxRetries).
