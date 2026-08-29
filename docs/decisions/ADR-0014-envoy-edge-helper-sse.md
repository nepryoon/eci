# ADR-0014 — Envoy edge con helper interno per autenticazione e SSE

Stato: accepted
Data: 2026-08-29
Decisione collegata: T6.6 / SPEC-060

## Contesto

L'ADD Modulo 3 §1.4 e Modulo 4 §1.3 assegna a Envoy il boundary esterno:
REST/JSON e SSE verso browser, gRPC interno, autenticazione e rate limiting.
T6.1 ha già implementato in Go la validazione OIDC rigorosa e la derivazione
canonica di `SecurityContext`; duplicarla nel filtro JWT di Envoy produrrebbe
due implementazioni dei claim e non risolverebbe la serializzazione del
protobuf binario nei metadata gRPC.

Il filtro `grpc_json_transcoder` di Envoy restituisce gli stream server-side
come array JSON. Envoy supporta contenuto arbitrario/streaming con
`google.api.HttpBody`, ma il contratto congelato `RetrievalEngine.ImpactAnalysis`
restituisce `ImpactAnalysisEvent`. Cambiarlo per ottenere SSE sarebbe una
rottura wire/semantica non richiesta dal task.

## Opzioni considerate

1. **Solo filtri Envoy JWT + transcoder.** Non riusa SPEC-055, non costruisce il
   metadata protobuf canonico e non produce frame SSE. Scartata.
2. **Cambiare `ImpactAnalysis` per restituire `google.api.HttpBody`.** Permette
   content type arbitrario dal transcoder, ma rompe il contratto gRPC interno e
   mescola una rappresentazione browser nel bounded context Retrieval.
   Scartata.
3. **Sostituire Envoy con grpc-gateway.** Può generare REST e custom SSE, ma
   contraddice la scelta vincolante API Gateway=Envoy e perde i filtri edge.
   Scartata.
4. **Envoy pubblico + helper Go interno.** Envoy resta il listener esterno,
   transcoder e rate limiter. Un helper non esposto implementa ext-auth HTTP
   riusando SPEC-055 e un adapter SSE che chiama il client gRPC esistente. È
   l'opzione scelta.

## Decisione

Envoy è l'unico listener pubblico e accetta solo TLS (minimo TLS 1.2, ALPN
HTTP/2/HTTP/1.1); certificato e chiave arrivano da Secret montato, mai dal
repository. La catena HTTP è:

```text
header sanitization → ext_authz (fail closed) → bearer stripping
→ caller-partitioned local rate limit → limiter-key stripping
→ grpc_json_transcoder (route JSON/gRPC) → router
```

Il filtro di sanitizzazione rimuove ogni `eci-security-context-bin` e trace
context in ingresso. L'helper genera un trace id crittograficamente casuale,
valida il bearer token con `authn.Authenticator`, serializza il solo
`SecurityContext` derivato dai claim e restituisce header autorizzati con
semantica replace. Envoy fallisce chiuso se l'helper è indisponibile. Le porte
dell'helper e dei servizi gRPC sono cluster-internal e non pubblicate.

Il transcoder usa un descriptor set deterministico derivato dal proto
esistente, `auto_mapping=true`, `reject_unknown_method=true` e request
validation stretta. Non vengono aggiunte annotations o modifiche al contratto:
gli endpoint JSON unari usano il mapping canonico
`POST /eci.retrieval.v1.RetrievalEngine/<Method>`.

`POST /v1/impact-analysis:stream` è escluso dal transcoder e instradato
all'adapter Go. L'adapter rifiuta metadata assenti/malformati, ignora/azzera
qualsiasi `securityContext` nel JSON, propaga soltanto il metadata autenticato
al client gRPC e converte ogni `ImpactAnalysisEvent` con `protojson` in un
frame `data: <json>\n\n`, con `Content-Type: text/event-stream`, flush per
evento e cancellazione/deadline propagate.

Il rate limit T6.6 usa bucket dinamici locali per caller autenticato, derivati
da SHA-256 della coppia canonica `(tenant_id, user_id)` prodotta dal validator:
il valore opaco non proviene dal client, ha cardinalità massima bounded ed è
rimosso prima dell'upstream. Un bucket condiviso più ampio resta come
load-shedding per istanza. I limiti producono 429 e `Retry-After`. È un limite
edge di protezione;
quota globale per user/team/model del LLM Gateway resta responsabilità del
bounded context LLM. Il limite effettivo aggregato scala con le repliche e sarà
dimensionato insieme all'HPA in T7.1/T7.2.

Fonti primarie:

- Envoy gRPC-JSON Transcoder (stream JSON e `HttpBody`):
  https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/grpc_json_transcoder_filter.html
- Envoy ext_authz e considerazioni di sicurezza:
  https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_authz_filter.html

## Conseguenze

- Un solo validatore OIDC/claim produce l'autorità; prompt, body e header
  esterni non possono costruire lo scope.
- Bearer e chiave bucket non attraversano il boundary verso i servizi interni;
  il listener pubblico rifiuta il cleartext prima dell'autenticazione.
- JSON unary e SSE hanno due data path edge, ma condividono autenticazione,
  deadline, metadata e test end-to-end.
- L'API JSON auto-mapped è stabile ma non usa URL REST cosmetici; annotations
  potranno essere aggiunte solo con ADR/compatibility review separata.
- Il helper è stateless; una sua indisponibilità nega traffico invece di
  bypassare auth. L'adapter non filtra risultati: l'enforcement rimane pre-query
  nei servizi T6.2/T6.3.

## Rollback e sicurezza

Il rollback rimuove il listener Envoy senza cambiare servizi/contratti interni.
Non è consentito esporre direttamente helper o porte gRPC come workaround.
Config e immagini sono pinnate; secret OIDC e chiavi TLS provengono da
env/Secret o volume Secret. Header
forgiati vengono rimossi prima di ext_authz e mai concatenati. Nessun body
viene inviato al servizio auth, evitando prompt/body exfiltration.
