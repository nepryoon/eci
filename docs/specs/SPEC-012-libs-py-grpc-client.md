# SPEC-012 — libs/py: OTel + config + interceptor gRPC client (SecurityContext) (T0.8, parte Python — ultima delle tre)
Stato: implemented
Task-tree: T0.8 (split per linguaggio — Python, terzo e ultimo dei tre) · Servizio: libs/py/eci_core (moduli aggiunti al package già esistente da SPEC-003) · ADD: Modulo 3 (SecurityContext, API Contracts), Modulo 4 §3.1 (Observability)
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto (SecurityContext, letto — non modificato)

## 1. Obiettivo
Aggiungere al package Python condiviso già esistente (`libs/py/eci_core`, creato in SPEC-003 per i modelli Pydantic da D2) tre moduli: bootstrap OTel (stdout exporter, stessa filosofia di SPEC-010/011), config loader, e un interceptor gRPC **client** che allega `SecurityContext` — tipo proto già generato da SPEC-002 per Python, non ridefinito qui — ai metadata di ogni chiamata uscente, riusando la stessa convenzione di chiave metadata (`eci-security-context-bin`) già stabilita lato server in SPEC-011. Nessun coinvolgimento di Kafka: orchestrator (T1.5), il consumatore di questi moduli, non consuma né produce messaggi Kafka in Fase 1 — è un client gRPC verso retrieval-engine/llm-gateway, non un consumer del piano CDC.

## 2. Interfaccia

**`libs/py/eci_core/observability.py`**:
```python
def init_tracing(service_name: str) -> TracerProvider:
    """Inizializza il TracerProvider OTel con un ConsoleSpanExporter.
    Il chiamante è responsabile di invocare .shutdown() sul provider
    ritornato alla chiusura del processo (blocco finally o
    atexit.register), per il flush pulito degli span in buffer —
    stesso pattern di guard/shutdown già usato in Rust (SPEC-010) e
    Go (SPEC-011)."""
```
Dipendenze: `opentelemetry-api`, `opentelemetry-sdk` (versioni correnti — verificare al momento dell'implementazione, stesso trattamento riservato alle altre due lingue).

**`libs/py/eci_core/config.py`**:
```python
def env_or_default(key: str, default: str) -> str:
    """os.environ.get(key, default) — stringa vuota impostata
    esplicitamente è un valore, non un'assenza (comportamento nativo
    di os.environ.get, stesso principio di Rust/Go SPEC-010/011)."""
```

**`libs/py/eci_core/grpc_client.py`** — interceptor client che allega `SecurityContext`:
```python
class SecurityContextInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Allega un SecurityContext (tipo proto già generato da SPEC-002)
    serializzato come protobuf ai metadata di ogni chiamata
    unary-unary in uscita, sotto la chiave binaria
    'eci-security-context-bin' — stessa convenzione di SPEC-011 (lato
    server, Go). Costruito con un SecurityContext fisso al momento
    della creazione (walking skeleton "no security": nessuna logica
    di autenticazione reale, solo plumbing — vedi §5)."""

    def __init__(self, security_context: SecurityContext): ...
    def intercept_unary_unary(self, continuation, client_call_details, request): ...
```
Per la propagazione del trace context W3C via gRPC: **non reinventarla**, usare `opentelemetry-instrumentation-grpc` (pacchetto contrib ufficiale OTel per Python, equivalente Python di `otelgrpc` già usato in SPEC-011) applicato al canale client. Il codice custom di questa SPEC riguarda solo `SecurityContext`, non il trace context — stessa divisione di responsabilità già seguita in SPEC-011.

## 3. Comportamento (scenari)

1. **Dato** `init_tracing("orchestrator")` chiamato, **quando** genero uno span, **allora** viene stampato su stdout (verifica manuale, stesso trattamento di SPEC-010/011 scenario 1).
2. **Dato** `env_or_default` chiamato con/senza la variabile d'ambiente impostata, **allora** rispetta default/override, incluso il caso di stringa vuota impostata esplicitamente (trattata come valore, non come assenza).
3. **Dato** un canale gRPC Python con `SecurityContextInterceptor(sc)` applicato, **quando** faccio una chiamata unary verso un **server Go reale** che usa l'interceptor di SPEC-011 (`secctx.UnaryServerInterceptor()`), **allora** il server riceve e decodifica correttamente lo stesso `SecurityContext` — **verifica di interoperabilità cross-linguaggio reale**, non solo che le due implementazioni compilino separatamente ciascuna per conto proprio.
4. **Dato** lo stesso canale con anche `opentelemetry-instrumentation-grpc` applicato, **quando** faccio la stessa chiamata, **allora** il server (con `otelgrpc` lato Go, SPEC-011) riceve un trace context valido e collegato — stessa verifica di interoperabilità, sul lato della propagazione trace standard invece che sul `SecurityContext` custom.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Il server Go (SPEC-011) riceve metadata `eci-security-context-bin` ma non riesce a deserializzarli (formato incompatibile tra le due implementazioni) | Deve manifestarsi nello scenario 3 come fallimento esplicito (il server tratta il metadata come assente, secondo SPEC-011 §4) — se questo accade, è un segnale che le due implementazioni non concordano davvero sul formato, da correggere qui, non da ignorare come "va bene comunque" |
| `SecurityContextInterceptor` costruito con un `SecurityContext` che fallisce la serializzazione protobuf (caso teorico, dato che è un tipo generato e valido per costruzione) | Propagare l'eccezione, non ingoiarla silenziosamente — un `SecurityContext` non serializzabile è un bug altrove, non qualcosa da mascherare qui |

## 5. Non-goals
Nessun enforcement reale di `SecurityContext` — stessa decisione già presa per SPEC-011: Fase 1 è "no security" per design, qui solo il plumbing lato client. Nessun coinvolgimento di Kafka (orchestrator non consuma/produce messaggi — quello è sink-graph, già coperto da SPEC-011). Nessun test automatizzato in CI per lo scenario di interoperabilità cross-linguaggio (3/4): orchestrare due processi in due linguaggi diversi dentro un singolo test automatico aggiunge complessità significativa per un valore incrementale limitato rispetto a una verifica manuale una tantum, ben documentata con l'output esatto di entrambi i lati.

## 6. Vincoli dall'ADD
Modulo 3: `SecurityContext` come definito in D7 (SPEC-002, generato anche per Python) — tipo riusato, non ridefinito. Modulo 4 §3.1: propagazione trace context end-to-end — qui la prima verifica reale che la convenzione concordata tra Go e Python funzioni effettivamente insieme, chiudendo il cerchio aperto da SPEC-010/011.

## 7. Test plan
`pytest` per gli scenari 2 (deterministico, nessuna dipendenza esterna). Scenario 1 verificato manualmente come nelle altre due SPEC (output stdout incollato nel report). Scenari 3/4 (interoperabilità cross-linguaggio) verificati manualmente: avviare un piccolo server Go di prova con l'interceptor di SPEC-011, un client Python di prova con l'interceptor di questa SPEC, ed effettuare una chiamata reale — incollare i log di entrambi i lati nel report, non solo "verificato".

## 8. Osservabilità
Questa SPEC costruisce la fondazione dell'osservabilità lato Python — non applicabile ricorsivamente.

## 9. Criteri di accettazione
- [x] `init_tracing` produce output leggibile su stdout (scenario 1) — vedi §10 output esatto: JSON completo con `trace_id`, `resource.attributes["service.name"]="orchestrator"`.
- [x] `env_or_default` rispetta default/override, incluso il caso stringa-vuota-esplicita (scenario 2) — `pytest` verde, 3/3 test in `test_config.py`.
- [x] Interoperabilità cross-linguaggio verificata con evidenza diretta (log di entrambi i lati): un client Python con `SecurityContextInterceptor` comunica correttamente con un server Go reale che usa l'interceptor di SPEC-011 — sia per `SecurityContext` (scenario 3) sia per il trace context W3C via `otelgrpc`/`opentelemetry-instrumentation-grpc` (scenario 4, con una deviazione — vedi §10 punto 4).
- [x] `pytest` verde sugli scenari automatizzabili — 16/16 in `libs/py` (13 preesistenti + 3 nuovi di `test_config.py`), nessuna regressione. `task lint`/`task test` reali verificati verdi.

## 10. Deviazioni rispetto alla SPEC

1. **Versioni esatte risolte** (verificate al momento dell'implementazione,
   stesso trattamento di SPEC-006/007/010/011):
   `opentelemetry-api` 1.44.0, `opentelemetry-sdk` 1.44.0 (stessa versione
   major.minor.patch del Go SDK di SPEC-011, coincidenza non pianificata ma
   comoda per il confronto), `opentelemetry-instrumentation-grpc` 0.65b0
   (pacchetto contrib ufficiale, ancora in beta upstream — nessuna versione
   1.x stabile disponibile su PyPI al momento dell'implementazione, stessa
   situazione di prassi per i pacchetti `opentelemetry-instrumentation-*`),
   `grpcio` 1.83.0 (già presente, coerente con `google.golang.org/grpc`
   v1.83.0 usato da libs/go), `protobuf` 7.35.1 (Python runtime, generato
   da SPEC-002). Aggiunte a `pyproject.toml` come dipendenze dirette (non
   extra), stesso motivo già documentato per `jsonschema` in SPEC-003 —
   `task-test.sh`/`task-lint.sh` non installano extra prima di eseguire
   pytest/ruff.

2. **`init_tracing`: `SimpleSpanProcessor` (non `BatchSpanProcessor`)**,
   per ottenere stdout sincrono/immediato — stessa filosofia di
   `WithSyncer` in Go (SPEC-011 §10 punto 1) e degli export sincroni già
   scelti in Rust (SPEC-010). Nessuna gestione esplicita del doppio
   `init_tracing`: l'SDK Python ha una protezione nativa equivalente
   (`opentelemetry.trace.set_tracer_provider` logga un warning e NON
   sovrascrive il provider globale se già impostato — verificato
   direttamente leggendo il comportamento a runtime), a differenza di Go
   che ha richiesto un mutex/flag manuale (SPEC-011 §10 punto 2): qui non
   serve codice custom, il comportamento coerente con le altre due lingue
   viene gratis dalla libreria.

3. **`opentelemetry-instrumentation-grpc`: due famiglie di interceptor
   client non componibili nella stessa chiamata.** Il pacchetto contrib
   espone un proprio `intercept_channel(channel, *interceptors)` che
   accetta solo i propri interceptor "grpcext" (`client_interceptor()`),
   distinti dal protocollo standard `grpc.UnaryUnaryClientInterceptor`
   usato da `SecurityContextInterceptor` (questa SPEC) — `grpc.intercept_channel`
   standard rifiuta l'interceptor otel con `TypeError` se passato insieme
   (scoperto empiricamente, non documentato esplicitamente nei docstring
   del pacchetto). Risolto impilando i due wrapper di canale in sequenza
   (`otel_intercept_channel` prima, poi `grpc.intercept_channel` per
   `SecurityContextInterceptor`) — entrambe le librerie restano
   disaccoppiate, nessuna delle due sa dell'altra, come nel caso Go
   (SPEC-011 §10 punto 4: coesistenza verificata, non fusione). Vedi
   `eci_core/examples/interop_client.py`.

4. **Scenario 4 (trace context W3C via `otelgrpc`): fallito al primo
   tentativo, causa reale identificata e isolata — non un problema di
   formato tra le due implementazioni.** Il client Python (verificato con
   un interceptor di debug temporaneo) invia correttamente un header
   `traceparent` W3C valido nei metadata gRPC in uscita. Il server Go
   scaffold (SPEC-011, `otelgrpc.NewServerHandler()`), alla prima esecuzione,
   riportava sempre `trace context: ASSENTE/non valido` — causa: il
   `TextMapPropagator` globale di `go.opentelemetry.io/otel` è **no-op per
   default** finché non viene impostato esplicitamente con
   `otel.SetTextMapPropagator(...)`, e **nessun package di `libs/go/eci`
   (SPEC-010/011) lo imposta mai** (verificato con
   `grep -rn SetTextMapPropagator libs/go/eci/` — zero risultati). Il
   Python SDK invece imposta questo default automaticamente (composite
   tracecontext+baggage), quindi il gap non era visibile lato Python.
   Impostato `otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(...))`
   **solo nello scaffold Go di questa SPEC**
   (`tests/interop/spec-012-secctx-py-go/server/main.go`, fuori dal
   perimetro file di `libs/go` assegnato a SPEC-011, quindi non toccato
   lì) per completare la verifica: con questa riga, `trace_id`/`span_id`
   ricevuti lato Go **coincidono esattamente** con quelli generati lato
   Python (vedi log in fondo a questa sezione) — conferma che il formato
   W3C è compatibile tra le due implementazioni, il gap era puramente di
   configurazione mancante, non di incompatibilità. **Segnalato per una
   futura SPEC/ADR**: `libs/go/eci/observability.InitTracing` (SPEC-011)
   dovrebbe probabilmente impostare il propagator globale di default,
   dato che nessun servizio Go può oggi propagare/estrarre trace context
   via `otelgrpc` senza che ogni singolo `main()` ricordi di farlo
   esplicitamente — non corretto qui perché fuori dal perimetro file di
   questa SPEC (solo `libs/py/eci_core` + lo scaffold Go di test).

5. **Scaffold Go di interoperabilità**: creato come modulo Go
   **separato e autonomo** in
   `tests/interop/spec-012-secctx-py-go/` (proprio `go.mod`, modulo
   `github.com/eci-project/eci/tests/interop/spec-012-secctx-py-go`, con
   `replace github.com/eci-project/eci/libs/go => ../../../libs/go` per
   importare `libs/go/eci/secctx` e `libs/go/eci/retrieval/v1` senza
   modificare `libs/go` stesso) — stessa convenzione già in uso in
   `tests/integration/postgres_ddl` e `tools/migrate-neo4j` (moduli Go
   indipendenti fuori da `libs/go`, non un nuovo sotto-package del modulo
   condiviso). Contiene un solo file, `server/main.go`: un server gRPC che
   registra `retrievalv1.RetrievalEngineServer` (riusando il proto
   `GetNode` già esistente, nessun nuovo contratto), applica
   `secctx.UnaryServerInterceptor()` + `otelgrpc.NewServerHandler()`
   (esattamente come in `secctx_test.go` di SPEC-011), e stampa su stdout
   ciò che riceve. Non incluso in `task lint`/`task test` (script non lo
   scansionano, per design — è uno scaffold di verifica manuale, non un
   servizio applicativo né una suite CI, coerente con SPEC-012 §5/§7);
   verificato manualmente con `go vet ./...` e `go build ./...`, entrambi
   puliti.

6. **Client Python di interoperabilità**: `libs/py/eci_core/examples/interop_client.py`
   — non parte della suite pytest (nessun file `test_*.py`), script
   eseguibile con `python -m eci_core.examples.interop_client` mentre lo
   scaffold Go gira su `127.0.0.1:50052`. Tenuto dentro `libs/py/eci_core`
   (perimetro file assegnato) invece che come file usa-e-getta, per
   rendere la verifica di interoperabilità riproducibile in futuro senza
   dover ricostruire lo script da zero.

7. **Fix retroattivo applicato a `libs/go/eci/observability` (SPEC-011,
   già su `main`), successivo al report iniziale di questa SPEC.** Il
   punto 4 sopra documentava il gap (propagator globale mai impostato) e
   il workaround temporaneo applicato solo nello scaffold di test. Su
   richiesta esplicita, la riga
   `otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))`
   è stata spostata dentro `InitTracing()` stesso
   (`libs/go/eci/observability/observability.go`), impostata una sola
   volta insieme a `otel.SetTracerProvider(tp)` nello stesso blocco
   guardato da `initMu`/`initDone` — coerente con il pattern
   no-op-alla-seconda-chiamata già esistente (SPEC-011 §10 punto 2), non
   un meccanismo nuovo. La riga equivalente nello scaffold Go
   (`tests/interop/spec-012-secctx-py-go/server/main.go`) è stata
   **rimossa** e sostituita da una chiamata reale a
   `observability.InitTracing("interop-go-server")` — la correzione ora
   arriva esclusivamente dal pacchetto condiviso, non duplicata nello
   scaffold. Verificato che il fix nel pacchetto risolve davvero il
   problema (non solo "dovrebbe"): stessa identica verifica di
   interoperabilità ri-eseguita da capo (server Go ricompilato e
   rilanciato, client Python rilanciato) — vedi log in "Evidenza fix
   retroattivo" sotto — `trace_id`/`span_id` del parent remoto ricevuti
   lato Go coincidono di nuovo esattamente con quelli generati lato
   Python, stavolta con la correzione proveniente da `InitTracing`.
   `go build ./...`, `go vet ./...`, `go test ./...` di `libs/go`
   verificati verdi dopo la modifica (nessuna regressione sugli altri
   package: `config`, `kafkatrace`, `models`, `retrieval/v1`, `secctx`).

### Evidenza scenario 1 (init_tracing, stdout)

```
$ .venv/bin/python -c "
from eci_core.observability import init_tracing

provider = init_tracing('orchestrator')
tracer = provider.get_tracer(__name__)
with tracer.start_as_current_span('example_span') as span:
    span.set_attribute('example.attr', 'value')
provider.shutdown()
"
{
    "name": "example_span",
    "context": {
        "trace_id": "0x96c9fe24d65ab999729b9f746cb61897",
        "span_id": "0xaaaafc2c50868829",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": null,
    "start_time": "2026-08-02T21:04:01.842474Z",
    "end_time": "2026-08-02T21:04:01.842519Z",
    "status": {"status_code": "UNSET"},
    "attributes": {"example.attr": "value"},
    "events": [],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.44.0",
            "service.instance.id": "12207b90-d6f2-4308-a0a9-09d281838c87",
            "service.name": "orchestrator"
        },
        "schema_url": ""
    }
}
```

### Evidenza scenari 3/4 (interoperabilità cross-linguaggio reale)

Setup: `go build -o /tmp/interop-server ./server` dentro
`tests/interop/spec-012-secctx-py-go`, eseguito in background su
`127.0.0.1:50052`; poi `python -m eci_core.examples.interop_client` da
`libs/py` (venv del progetto). Run finale, con la correzione del
propagator applicata nello scaffold (punto 4 sopra):

**Log lato client Python:**
```
[py-client] invio SecurityContext: tenant_id='tenant-py-42' user_id='user-py-7' allowed_repos=['repo-a', 'repo-b'] acl_groups=['group-x']
[py-client] trace_id inviato: 53eff65a233fe46abdf17ae2c541f129
{
    "name": "/eci.retrieval.v1.RetrievalEngine/GetNode",
    "context": {
        "trace_id": "0x53eff65a233fe46abdf17ae2c541f129",
        "span_id": "0x0da419bf9120bb51",
        "trace_state": "[]"
    },
    "kind": "SpanKind.CLIENT",
    "parent_id": "0x85d8b25f3a0c24d4",
    "attributes": {
        "rpc.system": "grpc",
        "rpc.grpc.status_code": 0,
        "rpc.method": "GetNode",
        "rpc.service": "eci.retrieval.v1.RetrievalEngine"
    },
    "resource": {"attributes": {"service.name": "orchestrator-interop-client", ...}}
}
{
    "name": "interop_get_node",
    "context": {
        "trace_id": "0x53eff65a233fe46abdf17ae2c541f129",
        "span_id": "0x85d8b25f3a0c24d4",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL"
}
[py-client] risposta ricevuta: node_id='interop-node-1'
```

**Log lato server Go (stesso run, in parallelo):**
```
[go-server] in ascolto su 127.0.0.1:50052
[go-server] SecurityContext ricevuto: tenant_id="tenant-py-42" user_id="user-py-7" allowed_repos=[repo-a repo-b] acl_groups=[group-x]
[go-server] trace context ricevuto: trace_id=53eff65a233fe46abdf17ae2c541f129 span_id=0da419bf9120bb51 remote=true
```

Verifica incrociata: `trace_id` (`53eff65a233fe46abdf17ae2c541f129`) e lo
`span_id` del CLIENT span Python (`0da419bf9120bb51`) coincidono
esattamente con quanto estratto lato Go — propagazione W3C reale, non
solo formati compatibili sulla carta. `tenant_id`/`user_id`/`allowed_repos`/
`acl_groups` del `SecurityContext` ricevuto lato Go coincidono
esattamente con quanto costruito lato Python.

**Log lato server Go, primo tentativo (senza la correzione del punto 4,
per completezza — mostra il fallimento reale prima della diagnosi):**
```
[go-server] SecurityContext ricevuto: tenant_id="tenant-py-42" user_id="user-py-7" allowed_repos=[repo-a repo-b] acl_groups=[group-x]
[go-server] trace context: ASSENTE/non valido
```

### Evidenza fix retroattivo in libs/go/eci/observability (punto 7)

Stessa identica verifica, ri-eseguita dopo aver spostato il fix in
`InitTracing()` e rimosso la riga duplicata dallo scaffold: server Go
ricompilato (`go build -o /tmp/interop-server ./server`), rilanciato in
background su `127.0.0.1:50052`; client Python rilanciato da
`libs/py` (`python -m eci_core.examples.interop_client`).

**Log lato client Python:**
```
[py-client] invio SecurityContext: tenant_id='tenant-py-42' user_id='user-py-7' allowed_repos=['repo-a', 'repo-b'] acl_groups=['group-x']
[py-client] trace_id inviato: b52163a21990532ab6a52481d23bc94a
{
    "name": "/eci.retrieval.v1.RetrievalEngine/GetNode",
    "context": {
        "trace_id": "0xb52163a21990532ab6a52481d23bc94a",
        "span_id": "0x69feb35736568eac",
        "trace_state": "[]"
    },
    "kind": "SpanKind.CLIENT",
    "parent_id": "0x6b6ca387a1c16d67",
    "attributes": {
        "rpc.system": "grpc",
        "rpc.grpc.status_code": 0,
        "rpc.method": "GetNode",
        "rpc.service": "eci.retrieval.v1.RetrievalEngine"
    },
    "resource": {"attributes": {"service.name": "orchestrator-interop-client", ...}}
}
{
    "name": "interop_get_node",
    "context": {
        "trace_id": "0xb52163a21990532ab6a52481d23bc94a",
        "span_id": "0x6b6ca387a1c16d67",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL"
}
[py-client] risposta ricevuta: node_id='interop-node-1'
```

**Log lato server Go (stesso run, in parallelo — stavolta con
`observability.InitTracing("interop-go-server")` chiamato realmente
all'avvio, nessuna riga duplicata nello scaffold):**
```
[go-server] in ascolto su 127.0.0.1:50052
[go-server] SecurityContext ricevuto: tenant_id="tenant-py-42" user_id="user-py-7" allowed_repos=[repo-a repo-b] acl_groups=[group-x]
[go-server] trace context ricevuto: trace_id=b52163a21990532ab6a52481d23bc94a span_id=34789fc8d5d242e6 remote=false
{
    "Name": "eci.retrieval.v1.RetrievalEngine/GetNode",
    "SpanContext": {
        "TraceID": "b52163a21990532ab6a52481d23bc94a",
        "SpanID": "34789fc8d5d242e6",
        "TraceFlags": "03",
        "Remote": false
    },
    "Parent": {
        "TraceID": "b52163a21990532ab6a52481d23bc94a",
        "SpanID": "69feb35736568eac",
        "TraceFlags": "03",
        "Remote": true
    },
    "SpanKind": 2,
    "Resource": [{"Key": "service.name", "Value": {"Type": "STRING", "Value": "interop-go-server"}}, ...],
    "InstrumentationScope": {
        "Name": "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        "Version": "0.69.0"
    }
}
```

Verifica incrociata: `trace_id` (`b52163a21990532ab6a52481d23bc94a`)
coincide su entrambi i lati; il campo `Parent` dello span lato server
Go — lo span che `otelgrpc` crea per la RPC in ingresso — ha
`SpanID: "69feb35736568eac"` e `Remote: true`, che coincide esattamente
con lo `span_id` del CLIENT span Python (`0x69feb35736568eac`):
prova diretta che il server ha estratto correttamente il `traceparent`
in ingresso e lo ha collegato come genitore remoto, con la correzione
proveniente stavolta unicamente da `InitTracing` (nessuna riga
`otel.SetTextMapPropagator` nello scaffold). Il `SecurityContext`
ricevuto coincide di nuovo esattamente con quanto inviato.
