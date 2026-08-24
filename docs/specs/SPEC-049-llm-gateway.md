# SPEC-049 — LLM Gateway: routing, streaming, deadline e circuit breaker (T5.3)
Stato: implemented
Task-tree: T5.3 · Servizio: services/llm-gateway · ADD: Modulo 3 §1.1-1.2, Modulo 4 §1.5
Contratti: API OpenAI-compatible `/v1/chat/completions` del `vllm-fake`; `contracts/` invariati

## 1. Obiettivo
Implementare un reverse proxy HTTP OpenAI-compatible stateless che instrada alias verso vLLM fake/on-prem o endpoint reali configurati, preserva streaming, deadline e contiene guasti con circuit breaker.

## 2. Interfaccia
```go
type Route struct { Upstream *url.URL; Model string }
type Config struct { Routes map[string]Route; DefaultRoute Route; Timeout time.Duration; FailureThreshold int; OpenDuration time.Duration }
func NewHandler(Config, *http.Client) (http.Handler, error)
```
Endpoint `POST /v1/chat/completions`, health `GET /healthz`. Env: `LLM_GATEWAY_ADDR`, `LLM_GATEWAY_ROUTES`, `LLM_GATEWAY_DEFAULT`, `LLM_GATEWAY_TIMEOUT`.

## 3. Comportamento
1. Alias noto: route e model rewrite. 2. Alias ignoto: default o 400. 3. Status/header/body preservati. 4. `stream=true`: flush incrementale. 5. Timeout/cancellazione propagati. 6. N errori rete/5xx aprono breaker, half-open dopo durata, successo richiude. 7. Input/metodo invalidi non raggiungono upstream. 8. Health sempre 200.

## 4. Errori & edge case
| Condizione | Comportamento |
|---|---|
| Body >4MiB | 413 |
| model vuoto | 400 |
| upstream 4xx | propagato, non conta nel breaker |
| upstream 5xx/rete | conta nel breaker |

## 5. Non-goals
Quota/rate limit/cache, nuovo proto, auth, retry/hedging della generazione non idempotente.

## 6. Vincoli dall'ADD
Stateless; vLLM on-prem default; streaming; budget ~15s; circuit breaker; nessun retry generazione.

## 7. Test plan
`httptest.Server` upstream reale per scenari 1-8, flush osservabile, context cancellation e transizioni breaker.

## 8. Osservabilità
Header `X-ECI-Upstream-Model`; nessun prompt nei log. OTel HTTP completo demandato a T7.3.

## 9. Criteri di accettazione
- [x] Scenari 1-8 verdi.
- [ ] `task build`, `task lint`, `task test`, `task guard` verdi.
- [ ] ADD/contratti invariati.

## 10. Deviazioni
1. Il gateway espone HTTP OpenAI-compatible, non gRPC: non esiste un contratto
   LLM protobuf in `contracts/` e il fake canonico è HTTP. Introdurre un proto
   autonomamente violerebbe la gerarchia delle fonti. Il timeout HTTP propaga
   comunque il context e implementa il budget prescritto.
2. OTel HTTP completo resta T7.3 come previsto dal task-tree; questa SPEC non
   modifica `libs/go`. L'header diagnostico non contiene prompt o dati utente.
3. Nessun retry: Chat Completions può produrre token/side effect di accounting e
   non è dichiarata idempotente. Il breaker conta errori rete/5xx, ignora 4xx e
   consente una sola probe half-open.
