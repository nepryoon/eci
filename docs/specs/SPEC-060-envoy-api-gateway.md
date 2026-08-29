# SPEC-060 — Envoy API Gateway: JSON/gRPC, SSE, rate limit e SecurityContext
Stato: implemented
Task-tree: T6.6 · Servizio: `services/api-gateway` + `deploy/envoy` · ADD: Modulo 3 §1.1–§1.4, §2.1; Modulo 4 §1.3, §2.3; D7 `SecurityContext`
Contratti: `contracts/proto/eci/retrieval/v1/retrieval.proto` (immutato)

## 1. Obiettivo

Esiste un edge Envoy reale che accetta gRPC e JSON esclusivamente su TLS,
autentica prima del routing, applica rate limit isolato per caller autenticato
e propaga il solo `SecurityContext` derivato dal JWT. Lo
stream `ImpactAnalysis` è esposto ai browser come SSE genuino tramite un helper
interno, senza cambiare il contratto gRPC condiviso.

## 2. Interfaccia

```go
type Authenticator interface {
    Authenticate(context.Context, string, string) (*retrievalv1.SecurityContext, error)
}

type ImpactClient interface {
    ImpactAnalysis(
        context.Context, *retrievalv1.ImpactAnalysisRequest,
        ...grpc.CallOption,
    ) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error)
}

type EdgeConfig struct {
    MaxJSONBodyBytes int64        // default 1 MiB
    RequestTimeout   time.Duration // default 30s
    Random           io.Reader     // default crypto/rand.Reader
    Registerer       prometheus.Registerer // required
    Tracer           trace.Tracer // default OTel provider tracer
}

func NewEdgeHandler(auth Authenticator, impact ImpactClient, cfg EdgeConfig) (http.Handler, error)
// internal routes:
// ANY  /authorize/*                  ext_authz only
// POST /v1/impact-analysis:stream    Envoy-routed SSE only
// GET  /healthz                      internal readiness
```

```text
Public Envoy:
POST /eci.retrieval.v1.RetrievalEngine/HybridSearch       application/json
POST /eci.retrieval.v1.RetrievalEngine/GetNode            application/json
POST /eci.retrieval.v1.RetrievalEngine/ExpandNeighbors    application/json
POST /v1/impact-analysis:stream                            text/event-stream
gRPC /eci.retrieval.v1.RetrievalEngine/*                   application/grpc
GET  /healthz                                              200 (no identity data)
```

```yaml
trusted upstream metadata:
  eci-security-context-bin: base64(protobuf SecurityContext)
  traceparent: 00-<gateway-generated-trace-id>-<gateway-generated-span-id>-01
  x-eci-request-deadline-unix-ms: <trusted absolute deadline; SSE route only>
```

## 3. Comportamento

1. **JSON→gRPC autenticato.** Given JWT valido, When il client invia JSON a un
   metodo auto-mapped, Then Envoy chiama ext_authz, sovrascrive metadata
   riservati, rimuove il bearer prima dell'hop interno, transcodifica e il
   backend riceve body corretto più `SecurityContext` autenticato.
2. **Header/body forgiati.** Given `eci-security-context-bin`, `traceparent` o
   `securityContext` controllati dal client, When la richiesta attraversa il
   gateway, Then gli header vengono rimossi/sostituiti e lo scope backend
   deriva solo dal JWT; il body non espande mai i permessi.
3. **Auth fail-closed.** Given token mancante/invalido/scaduto oppure helper
   auth indisponibile, When si chiama una route protetta, Then 401/403 oppure
   503 bounded e zero chiamate upstream; `failure_mode_allow=false`.
4. **SSE genuino.** Given stream gRPC di almeno due eventi, When si chiama la
   route SSE, Then status/headers precedono la fine dello stream e ogni evento
   è un frame `data:` JSON seguito da blank line e flush osservabile.
5. **SSE cancellation/error.** Given client cancellato, deadline o errore gRPC,
   When lo stream è aperto, Then il context downstream viene cancellato; prima
   degli header ritorna status HTTP bounded, dopo gli header chiude con evento
   `error` privo di detail interno e non continua a leggere.
6. **Rate limiting.** Given bucket esaurito, When arriva una richiesta protetta,
   Then Envoy risponde 429 con `Retry-After`, non chiama l'upstream ed espone
   contatori bounded; il limite si ricarica deterministicamente e non consuma
   il bucket di un caller autenticato differente.
   Given un flood di token invalidi, When supera il ceiling pre-auth condiviso,
   Then Envoy restituisce 429 prima di ext_authz e riduce deterministicamente
   le invocazioni del validatore, senza chiamare alcun backend.
7. **Transcoder/config stretti.** Given metodo/query JSON sconosciuto, body
   malformato o >1 MiB, When passa dall'edge, Then 400/404/413 senza passthrough;
   descriptor e config Envoy validano con immagine pinned. Anche un metodo gRPC
   nativo non presente nell'allow-list esatta dei quattro RPC dichiarati viene
   respinto all'edge e non raggiunge l'unknown-service handler del backend.
8. **Compatibilità gRPC e deadline.** Given client gRPC diretto all'edge, When
   invoca unary/server-stream con JWT metadata, Then Envoy preserva HTTP/2,
   autenticazione, status/cancellazione e deadline massima 30s. Per SSE il
   helper possiede la deadline assoluta di 30s; Envoy disabilita il route
   timeout dello stream ma applica un idle timeout di 35s, lasciando margine
   per il frame terminale senza consentire stream bloccati indefinitamente.
9. **TLS obbligatorio.** Given bearer valido inviato in cleartext, When raggiunge
   il listener pubblico, Then il TLS handshake manca e la richiesta non arriva
   a ext_authz/upstream. HTTPS e gRPC TLS richiedono almeno TLS 1.2; certificato
   e chiave sono file montati da Secret e non sono versionati.
10. **Header e trace bounded.** Given un client TLS che invia header incompleti,
    When non li completa entro 5s, Then Envoy risponde/chiude la connessione
    prima di ext_authz. Given una richiesta autenticata, Then il trace id
    generato dal gateway è il parent trusted dello span
    `eci.gateway.authorize` e il `traceparent` propagato identifica quello span,
    senza segmenti orfani o contesto controllato dal client.
11. **Deadline SSE assoluta.** Given un body SSE caricato lentamente, When il
    tempo da ext-auth alla fine del buffering supera 30s, Then l'adapter riceve
    la deadline trusted già scaduta, risponde 504 e non chiama gRPC. Header
    deadline forgiati sono rimossi; il metadata è conservato solo sulla route
    SSE e rimosso prima delle route gRPC dirette.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| helper auth non configurabile/OIDC discovery fallisce | processo non ready/non parte; nessun default issuer/audience |
| header auth >16 KiB o malformed | 401 bounded; non eco del token |
| impossibile generare randomness per trace/span | 503, nessun metadata debole/fisso |
| metadata binario assente/malformed nell'adapter SSE | 401/403, zero RPC |
| JSON SSE contiene `securityContext` privilegiato | campo azzerato; metadata autenticato unico scope |
| backend gRPC non raggiungibile prima del primo frame | 502/503 bounded |
| errore dopo almeno un frame | `event: error` + payload bounded, flush e close |
| ext_authz timeout | deny 503; mai failure-mode allow |
| header bucket forgiato | rimosso prima di ext_authz; chiave nuova solo da identità validata |
| header HTTP incompleti/Slowloris | 408/close entro 5s; zero chiamate ext_authz/upstream |
| scope autenticato con `SecurityContext` base64 >12 KiB | `invalid_claims`/deny bounded prima del trasporto |
| deadline SSE assente/malformata | 401; nessuna deadline client-controlled |
| deadline SSE già trascorsa durante buffering | 504; zero RPC |
| certificato/chiave TLS assente o invalido | Envoy non parte; nessun fallback cleartext |
| route/servizio non allow-listed | 404; transcoder non passthrough |

## 5. Non-goals

- Nessuna modifica proto/ADD, gRPC-Web, WebSocket o endpoint LLM OpenAI.
- Nessuna quota distribuita user/team/model (bounded context LLM Gateway) e
  nessuna configurazione Kubernetes/HPA (T7.1/T7.2).
- Nessun mTLS/service mesh (T7.1) o telemetry end-to-end completa (T7.3).
- Nessuna fiducia in body/header esterni e nessun filtro ACL post-retrieval.

## 6. Vincoli dall'ADD e threat model

- Modulo 3 §1.1–§1.4: edge auth, `SecurityContext`, REST/JSON+SSE→gRPC,
  rate limit; Modulo 3 §2: scope soltanto da identità autenticata.
- Modulo 4 §1.3: Envoy API Gateway; §1.4: deadline assolute/decrescenti;
  §2.3: W3C trace context e attributi bounded.
- D7: `SecurityContext` esistente e RPC `ImpactAnalysis` server-streaming.

**Minacce:** forged metadata/body, token replay/scaduto, header smuggling,
ext_auth bypass/failure o resource exhaustion JWKS tramite token invalidi,
intercettazione/replay del bearer in cleartext,
replay/esposizione del bearer sull'hop interno, starvation cross-tenant, route
passthrough, oversized body, Slowloris/header trickle, slow stream, rate limit bypass, direct
helper/backend exposure, prompt injection che tenta scope, error leakage.
Controlli: TLS obbligatorio al listener, rimozione metadata forgiabili e ceiling
coarse prima di auth, stripping del bearer subito dopo ext_authz, bucket opaco
derivato dal caller autenticato e poi rimosso, replace da validator T6.1, porte interne, strict
route/transcoder, size/deadline/header-time cap, fail-closed, scope metadata-only T6.2/T6.3,
error body allow-listed e test Envoy reale.

## 7. Test plan

- Unit Go `internal/edge`: auth success/failure, header overwrite, randomness
  failure, metadata decode, JSON size/forgery, SSE multi-frame/flush,
  cancellation ed errori pre/post-header (scenari 2–5).
- Static/deterministic: genera `deploy/envoy/retrieval.pb` dal proto con Buf e
  verifica byte identity; controlla filtri/ordine/fail-closed/no-passthrough.
- Integration CPU-only con `envoyproxy/envoy:v1.39.0` pinned, fake OIDC/auth e
  backend gRPC reale: TLS/cleartext rejection, JSON→gRPC metadata, SSE
  incremental, gRPC passthrough, 401/503, forged headers, 429 isolato tra due
  caller, header parziali chiusi prima dell'auth, continuità tra auth span e
  `traceparent`, upload SSE lento incluso nella deadline assoluta, scope fuori
  dal budget trasporto, e route unknown. `envoy --mode validate` sulla
  config effettiva prima del test.

## 8. Osservabilità

- span `eci.gateway.authorize` e `eci.gateway.sse` con soli attributi bounded
  `gateway.route`, `gateway.outcome`, `rpc.grpc.status_code`.
- `SecurityContext.trace_id`, span `eci.gateway.authorize` e `traceparent`
  downstream condividono lo stesso trace id; lo span id propagato è quello
  dello span authorize realmente registrato.
- counter `eci_gateway_edge_requests_total{route,outcome}` dove `route` è
  allow-list `{auth,sse,health}` e outcome chiuso.
- Envoy stats: `http.<listener>.downstream_rq_*`,
  `http.<listener>.ext_authz.{ok,denied,error}`,
  `http.<listener>.local_rate_limit.{enabled,ok,rate_limited}`.
- vietati tenant/user/repo/gruppo/token/query/body/node/path in log, span o label.

## 9. Criteri di accettazione

- [x] Scenari §3 red-before-green e verdi CPU-only.
- [x] Envoy pinned è unico listener pubblico; helper/backend non esposti.
- [x] JWT SPEC-055 produce il solo SecurityContext upstream; forged header/body
  non modifica tenant/repo/gruppi/trace.
- [x] JSON unary e gRPC passthrough reali attraversano ext_authz+backend.
- [x] SSE reale prova frame/flush incrementali, cancellation e failure path.
- [x] 429/Retry-After e zero-upstream provati; auth/rate failures fail-closed.
- [ ] Descriptor deterministico; `envoy --mode validate` e integration verdi.
- [ ] `task build`, `task lint`, `task test`, test gateway, `task guard` verdi;
  SPEC verified, ADR-0014 accepted, CI verde e PR merged.

## 10. Evidenza di implementazione

Red phase osservata prima dei file di produzione: `go test ./...` falliva per
`loadConfig` assente e per `deploy/envoy/envoy.yaml`/`retrieval.pb` assenti;
la suite edge iniziale falliva per i simboli handler non definiti.

Green locale CPU-only: unit/race/vet gateway verdi; descriptor rigenerato
byte-identico (SHA-256
`9e05e2a69c0aaddbd8541c60083cfca2b15ec4263228bf9b4e23b7cbac56b62e`);
bootstrap validato dal binario ufficiale Envoy 1.39.0; integrazione end-to-end
e relativo race detector verdi usando lo stesso binario. `task build`,
`task lint`, `task guard`, lint/breaking/codegen proto sono verdi. Il daemon
Docker locale non è avviabile senza credenziali sudo: `task test` locale ha
quindi completato le suite pure ma ha fallito esclusivamente nei testcontainers
Keycloak/OPA/Neo4j preesistenti. La prova dell’immagine pinned, il `task test`
completo e lo stato `verified` restano intenzionalmente subordinati alla CI.

## 11. Review avversariale di approvazione

Pass eseguito il 2026-08-29. La soluzione non cambia ADD/proto e non scrive
viste; l'adapter preserva bounded traversal/deadline e non post-filtra ACL.
Header/body/LLM non costruiscono autorità; ext_auth/rate limit falliscono chiusi.
Non esiste default tenant, direct exposure o route pass-through. SSE resta una
rappresentazione edge separata dal contratto deterministico e non introduce
judge/garanzie probabilistiche. La decisione non ovvia Envoy+helper e il limite
locale sono espliciti in ADR-0014, con impatto replica documentato. Nessuna
contraddizione, escalation o decisione architetturale nascosta rilevata.
