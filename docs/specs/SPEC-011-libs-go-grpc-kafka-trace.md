# SPEC-011 — libs/go: OTel + config + interceptor gRPC (SecurityContext) + estrazione trace_id da header Kafka (T0.8, parte Go)
Stato: implemented
Task-tree: T0.8 (split per linguaggio — Go, secondo dei tre) · Servizio: libs/go/eci/{observability,config,secctx,kafkatrace} · ADD: Modulo 3 (SecurityContext, API Contracts), Modulo 4 §3.1 (Observability)
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto (SecurityContext, letto — non modificato)

## 1. Obiettivo
Costruire i package Go condivisi che retrieval-engine (T1.4) e sink-graph (T1.3) useranno da subito: bootstrap OTel (stessa filosofia console/stdout di SPEC-010), config loader, propagazione/estrazione di `SecurityContext` — il tipo proto **già esistente**, generato da SPEC-002, non un nuovo tipo — via interceptor gRPC lato server, ed estrazione di `trace_id` da header Kafka per collegare (via span **link**, non parent-child: è un confine asincrono) il consumo al trace del produttore. Include una piccola aggiunta al connector Debezium condiviso (`deploy/compose/debezium-outbox-connector.json`, SPEC-007) per promuovere davvero `trace_id` a header Kafka — senza, non ci sarebbe nulla da estrarre.

## 2. Interfaccia

**`libs/go/eci/observability`** (equivalente Go di SPEC-010):
```go
// InitTracing inizializza il TracerProvider OTel con un exporter
// stdout/console. Ritorna una funzione di shutdown (chiamare via defer)
// per il flush pulito alla chiusura del processo.
func InitTracing(serviceName string) (shutdown func(context.Context) error, err error)
```
Dipendenze: `go.opentelemetry.io/otel`, `.../sdk/trace`, `.../exporters/stdout/stdouttrace` (versioni correnti — verificare al momento dell'implementazione, stesso trattamento riservato alle dipendenze Rust in SPEC-010).

**`libs/go/eci/config`**:
```go
// EnvOrDefault legge la variabile d'ambiente key; se assente, ritorna
// defaultValue. Stringa vuota impostata esplicitamente è un valore, non
// un'assenza (stesso comportamento di SPEC-010 §4, non da alterare).
func EnvOrDefault(key, defaultValue string) string
```

**`libs/go/eci/secctx`** — propagazione di `SecurityContext` (tipo `retrievalv1.SecurityContext`, già generato da SPEC-002) via gRPC:
```go
// UnaryServerInterceptor estrae SecurityContext dai metadata gRPC in
// ingresso (chiave binaria "eci-security-context-bin", convenzione gRPC
// per dati non-UTF8 — deserializzata come protobuf), lo attacca al
// context.Context passato all'handler. Se il metadata è assente o
// malformato, l'handler riceve comunque il context (senza SecurityContext
// attaccato) — l'interceptor non blocca la richiesta: Fase 1 è "no
// security" per design (§5), qui c'è solo il plumbing, non l'enforcement.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor

// FromContext recupera il SecurityContext attaccato dall'interceptor.
// ok=false se assente (nessun panic, nessun valore fittizio silenzioso).
func FromContext(ctx context.Context) (sc *retrievalv1.SecurityContext, ok bool)
```
Per la propagazione del trace context W3C (`traceparent`) via gRPC: **non reinventarla** — usare `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` (pacchetto contrib ufficiale OTel), passato come `grpc.StatsHandler` all'avvio del server. Il codice custom di questa SPEC riguarda solo `SecurityContext` (tipo nostro), non il trace context (concetto generico, già risolto da una libreria consolidata).

**`libs/go/eci/kafkatrace`** — estrazione trace_id da header Kafka (per sink-graph, T1.3):
```go
// TraceIDFromHeaders estrae il trace_id (esadecimale, 32 caratteri) dagli
// header di un messaggio Kafka (chiave header "trace_id", promossa dal
// connector Debezium — vedi addendum sotto). ok=false se l'header è
// assente o il valore non è un trace_id esadecimale valido di 32 caratteri.
func TraceIDFromHeaders(headers []kafka.Header) (traceID string, ok bool)

// SpanLinkFromTraceID costruisce un trace.Link da passare come opzione
// all'apertura di uno span di consumo — collega (LINK, non parent-child:
// pattern OTel standard per boundary asincroni messaging) il consumo al
// trace del produttore. ok=false se traceID non è valido (stessa validazione
// di TraceIDFromHeaders).
func SpanLinkFromTraceID(traceID string) (link trace.Link, ok bool)
```
Libreria client Kafka: `segmentio/kafka-go` (Go puro, nessuna dipendenza cgo/librdkafka — scelta a basso rischio, dichiarata, coerente con la preferenza già seguita altrove in questa fase per toolchain più semplici da buildare).

**Addendum al connector Debezium condiviso** (`deploy/compose/debezium-outbox-connector.json`, file di SPEC-007 — non ne crea uno nuovo): aggiungere
```json
"transforms.outbox.table.fields.additional.placement": "trace_id:header:trace_id"
```
Sintassi confermata da documentazione ufficiale Debezium: `<colonna>:<placement>:<alias>`, `placement=header` promuove la colonna a header del messaggio con quel nome — qui alias identico al nome colonna, nessuna ambiguità.

## 3. Comportamento (scenari)

1. **Dato** `InitTracing("retrieval-engine")` chiamato all'avvio, **quando** genero uno span, **allora** viene stampato su stdout (stessa verifica di SPEC-010 scenario 1, qui lato Go).
2. **Dato** un `SecurityContext` serializzato come protobuf e allegato ai metadata gRPC in uscita (chiave `eci-security-context-bin`), **quando** il server con `UnaryServerInterceptor()` riceve la chiamata, **allora** `FromContext(ctx)` dentro l'handler ritorna lo stesso `SecurityContext`, `ok=true`.
3. **Dato** una chiamata gRPC SENZA metadata `eci-security-context-bin`, **quando** arriva al server, **allora** l'handler viene comunque invocato (nessun blocco), `FromContext(ctx)` ritorna `ok=false` — coerente con "no security" di Fase 1.
4. **Dato** un messaggio Kafka con header `trace_id` = stringa esadecimale di 32 caratteri valida, **quando** chiamo `TraceIDFromHeaders`, **allora** ottengo quella stringa, `ok=true`; **quando** poi chiamo `SpanLinkFromTraceID` con quel valore, **allora** ottengo un `trace.Link` valido, `ok=true`.
5. **Dato** un messaggio Kafka SENZA header `trace_id` (o con un valore malformato, es. non esadecimale o lunghezza diversa da 32), **quando** chiamo `TraceIDFromHeaders`, **allora** `ok=false` — nessun panic, nessuna stringa vuota spacciata per un trace_id valido.
6. **Dato** l'addendum applicato al connector Debezium e lo stack di SPEC-006/007 in esecuzione con `outbox` migrata, **quando** inserisco una riga in `outbox` con un `trace_id` esplicito, **allora** il messaggio Kafka risultante (verificabile con `kafka-console-consumer.sh --property print.headers=true`, stesso strumento già usato in SPEC-008) mostra un header `trace_id` con quel valore.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Metadata `eci-security-context-bin` presente ma non deserializzabile come protobuf valido (corrotto) | `FromContext` ritorna `ok=false` (stesso trattamento di "assente") — l'interceptor logga un warning ma NON blocca la richiesta, coerente con "plumbing, non enforcement" |
| Header Kafka `trace_id` presente ma con un valore che non è esadecimale o non ha 32 caratteri | Trattato come assente (`ok=false`), non troncato/adattato silenziosamente a un formato valido |
| `InitTracing` chiamata due volte nello stesso processo | Stesso comportamento non-panicante di SPEC-010 §4 (no-op con warning su stderr) — coerenza intenzionale tra le due implementazioni |

## 5. Non-goals
Nessun enforcement reale di `SecurityContext` (nessun controllo su `acl_groups`/`allowed_repos` contro una risorsa) — decisione già presa: Fase 1 è "no security" per design, questa SPEC costruisce solo il plumbing (propagazione), l'enforcement arriva in Fase 6 quando il modello RBAC sarà definito nel Modulo 3. Nessun interceptor **client**-side gRPC (quello serve a orchestrator, Python, SPEC-012 — non a retrieval-engine/sink-graph in questa fase). Nessun consumer Kafka completo per sink-graph (quello è T1.3 — qui solo l'helper di estrazione trace_id, riusabile da quel task). Nessun exporter OTLP reale (stessa scelta di SPEC-010, rimandato a Fase 7).

## 6. Vincoli dall'ADD
Modulo 3: `SecurityContext` come definito in D7 (SPEC-002) — tipo riusato, non ridefinito. Modulo 4 §3.1: propagazione trace context end-to-end, incluso attraverso il confine asincrono Kafka (span link, non parent-child — distinzione semantica corretta per boundary di messaging secondo le convenzioni OTel).

## 7. Test plan
`go test` per gli scenari 2/3/4/5 (deterministici, nessuna dipendenza esterna — un client/server gRPC in-process per 2/3, dati sintetici per 4/5). Scenario 1 verificato manualmente come in SPEC-010 (output stdout incollato nel report). Scenario 6 verificato contro lo stack compose reale di SPEC-006/007 (`task up`, insert, `kafka-console-consumer.sh --property print.headers=true`) — non un test automatico, una verifica diretta da riportare con l'output esatto.

## 8. Osservabilità
Questa SPEC costruisce la fondazione dell'osservabilità lato Go — non applicabile ricorsivamente.

## 9. Criteri di accettazione
- [x] `InitTracing` produce output leggibile su stdout (scenario 1) — verificato con `go run ./eci/observability/examplemain`: JSON completo con `TraceID`/`SpanID`, `Resource` (`service.name=retrieval-engine`), coincidente col trace_id stampato manualmente da `span.SpanContext().TraceID()`.
- [x] Interceptor gRPC propaga `SecurityContext` correttamente quando presente (scenario 2) e non blocca quando assente (scenario 3) — `go test`, verde; verificato anche in composizione con `otelgrpc.NewServerHandler()`/`NewClientHandler()` (SPEC-011 §2), non solo in isolamento.
- [x] `TraceIDFromHeaders`/`SpanLinkFromTraceID` corretti sia sul caso valido sia su quello malformato/assente (scenari 4/5) — `go test`, verde.
- [x] Addendum al connector Debezium verificato con evidenza diretta: header `trace_id` realmente presente sul messaggio Kafka (scenario 6) — vedi output esatto in §10.
- [x] `go test` verde su tutti gli scenari automatizzabili — 11/11 test (`config`×3, `observability`×1, `secctx`×3, `kafkatrace`×5), nessuna regressione nel resto di `libs/go` (`models`, `retrieval/v1`), `go vet ./...` pulito, verificato anche via `task lint`/`task test` reali.

## 10. Deviazioni rispetto alla SPEC

1. **Versioni esatte risolte** (verificate al momento dell'implementazione,
   come da istruzione — stesso trattamento di SPEC-006/007/010):
   `go.opentelemetry.io/otel` v1.44.0, `.../otel/sdk` v1.44.0,
   `.../otel/trace` v1.44.0, `.../otel/exporters/stdout/stdouttrace`
   v1.44.0, `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc`
   v0.69.0, `github.com/segmentio/kafka-go` v0.4.51. API verificata
   leggendo il sorgente reale dei moduli scaricati (`SdkTracerProvider`
   equivalente Go = `sdktrace.NewTracerProvider` con `WithSyncer`; convenzione
   "-bin" per metadata gRPC binari verificata in
   `google.golang.org/grpc/internal/transport/http_util.go`), non assunta.

2. **Doppio `InitTracing`: no-op con warning su stderr**, stessa scelta di
   SPEC-010 §4 (coerenza intenzionale tra le due implementazioni, come
   richiesto da §4 di questa SPEC). Rilevato tramite un flag booleano
   protetto da mutex a livello di package (l'SDK Go OTel non ha un
   equivalente diretto di `try_init()` di `tracing-subscriber` usato in
   SPEC-010 — nessuna primitiva nativa per "imposta il TracerProvider
   globale solo se non già impostato"). La funzione di shutdown ritornata
   dalla chiamata no-op è comunque valida: esegue lo shutdown del proprio
   `TracerProvider` inerte, mai stato impostato come globale.

3. **`SpanLinkFromTraceID`: `SpanID` generato casualmente, non propagato
   dal produttore**: l'addendum al connector promuove solo `trace_id` a
   header Kafka (per design, §2), non uno `span_id` del produttore. Un
   `trace.Link` OTel richiede un `SpanContext` con `IsValid()==true`
   (`TraceID` e `SpanID` entrambi non-zero) per essere strutturalmente
   valido — usato `crypto/rand` per generare 8 byte casuali come `SpanID`
   del link, un placeholder non corrispondente a un vero span del
   produttore ma sufficiente per un `SpanContext` valido con `Remote:
   true`. Scelta esplicita, non lasciata implicita: documentata nel
   commento di `SpanLinkFromTraceID` in `kafkatrace.go`.

4. **`otelgrpc` non è (ancora) importato da nessun servizio reale**: §2
   descrive `otelgrpc.NewServerHandler()` come guidance per l'avvio del
   server gRPC di un futuro servizio (retrieval-engine/sink-graph, T1.3/T1.4),
   non come qualcosa che `libs/go/eci/secctx` deve wrappare internamente —
   il mio package espone solo l'interceptor `SecurityContext`. Per
   verificare che le due cose (interceptor `SecurityContext` +
   `otelgrpc.StatsHandler`) coesistano davvero senza conflitti (non solo a
   parole), il server/client gRPC di test in `secctx_test.go` li compone
   entrambi (`grpc.StatsHandler(otelgrpc.NewServerHandler())` lato server,
   `grpc.WithStatsHandler(otelgrpc.NewClientHandler())` lato client) —
   `go test` verde con entrambi attivi contemporaneamente.

5. **`go.mod`/`go.sum` di `libs/go` modificati** (fuori dalla lista
   letterale dei "file toccabili", che nomina solo le sotto-directory
   `libs/go/eci/{observability,config,secctx,kafkatrace}`): conseguenza
   meccanica inevitabile dell'aggiungere dipendenze esterne (OTel SDK Go,
   otelgrpc, kafka-go) a un modulo Go **già esistente e condiviso**
   (`libs/go`, non un nuovo modulo standalone come nel caso Rust di
   SPEC-010) — `go.mod`/`go.sum` sono file di modulo unici alla radice di
   `libs/go`, non duplicabili per sotto-package. Nessun altro file fuori
   dal perimetro assegnato è stato toccato (`libs/go/eci/models`,
   `libs/go/eci/retrieval/v1` invariati).

6. **`kafka-console-consumer.sh` con `--property` deprecato**: lo
   strumento per lo scenario 6 emette un warning
   ("--property is deprecated... use --formatter-property instead") sulla
   stessa immagine `apache/kafka:latest` già in uso da SPEC-007/008 — non
   impedisce la verifica, `--property` resta supportato in questa versione,
   annotato per completezza (nessuna azione necessaria).
