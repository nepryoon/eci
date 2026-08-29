# SPEC-056 — OPA PDP e PEP fail-closed negli interceptor gRPC
Stato: verified
Task-tree: T6.2 · Servizio: `libs/go/eci/authz`, `services/retrieval-engine`, `services/semantic-cache` · ADD: Modulo 3 §2.1
Contratti: `contracts/proto/eci/retrieval/v1/retrieval.proto`, `contracts/proto/eci/semanticcache/v1/semanticcache.proto` (sola lettura)

## 1. Obiettivo

Introdurre un Policy Decision Point OPA centralizzato per l'ABAC statico e un
Policy Enforcement Point gRPC riusabile, installato su tutti i server gRPC
applicativi oggi esistenti (Retrieval Engine e Semantic Cache). Ogni chiamata
protetta è autorizzata usando esclusivamente il `SecurityContext` autenticato
nei metadata e il nome RPC fornito dal runtime gRPC; assenza, diniego, errore o
indisponibilità del PDP producono un diniego fail-closed.

T6.2 autorizza l'azione di servizio. L'enforcement pre-retrieval nei datastore,
la derivazione di `acl_scope` e la verifica delle citation restano
rispettivamente T6.3, T6.4 e T6.5.

## 2. Interfaccia

```go
package authz

type Config struct {
    Endpoint          string
    DecisionPath      string
    Service           string
    Timeout           time.Duration
    AllowInsecureHTTP bool
}

type Decision struct {
    Allow  bool
    Reason string
}

type DecisionClient interface {
    Decide(ctx context.Context, securityContext *retrievalv1.SecurityContext, fullMethod string) (Decision, error)
}

func New(ctx context.Context, cfg Config, registerer prometheus.Registerer) (*Client, error)
func UnaryServerInterceptor(client DecisionClient) grpc.UnaryServerInterceptor
func StreamServerInterceptor(client DecisionClient) grpc.StreamServerInterceptor
```

```python
def run_ask(query_text, retrieval_addr, vllm_url, security_context, tracer_provider=None) -> AskResult: ...
```

`security_context` è obbligatorio e già autenticato: viene propagato al PEP del
servizio. Il precedente tenant dev implicito di SPEC-018 viene rimosso; il CLI
diretto fallisce chiuso fino all'entrypoint autenticato T6.6.

Richiesta OPA, costruita internamente e non esposta come input del chiamante:

```json
{
  "input": {
    "subject": {
      "tenant_id": "tenant-a",
      "user_id": "user-a",
      "allowed_repos": ["repo-a"],
      "acl_groups": ["engineering"]
    },
    "action": "/eci.retrieval.v1.RetrievalEngine/GetNode"
  }
}
```

Risposta obbligatoria dall'entrypoint fisso
`/v1/data/eci/authz/decision`:

```json
{"result":{"allow":true,"reason":"allow"},"decision_id":"opaque"}
```

La policy Rego `deploy/compose/opa/policies/eci_authz.rego`:

```rego
package eci.authz

default decision := {"allow": false, "reason": "policy_denied"}
```

e restituisce esclusivamente una reason dalla tassonomia chiusa:
`allow`, `unknown_action`, `missing_tenant`, `missing_user`,
`empty_repo_scope`, `empty_acl_scope`, `policy_denied`.

Configurazione processi:

```text
OPA_URL                  URL base del PDP, obbligatorio in produzione
OPA_DECISION_PATH        default /v1/data/eci/authz/decision
OPA_TIMEOUT              default 100ms, >0 e <=2s
OPA_ALLOW_INSECURE_HTTP  default false; true solo nello stack dev/test
METRICS_PORT             9105 Retrieval Engine; 9106 Semantic Cache
```

## 3. Comportamento

1. **Dato** un `SecurityContext` autenticato completo e una RPC registrata,
   **quando** OPA risponde `allow=true`, **allora** il PEP invoca l'handler una
   volta e non modifica request o context.
2. **Dato** un contesto assente o metadata protobuf malformato, **quando** arriva
   una RPC protetta, **allora** l'handler e OPA non sono invocati e il client
   riceve `Unauthenticated`.
3. **Dato** un contesto autenticato e OPA `allow=false`, **quando** arriva la
   chiamata, **allora** l'handler non è invocato e il client riceve
   `PermissionDenied`, senza dettagli di policy o scope.
4. **Dato** OPA irraggiungibile, in timeout, HTTP non-2xx o con risposta
   malformata/oversize, **quando** il servizio si inizializza o decide una
   richiesta, **allora** l'inizializzazione fallisce oppure la RPC termina
   `Unavailable`; non esiste fallback allow né cache stale.
5. **Dato** testo utente, prompt, body RPC o output LLM che contiene tenant,
   repository, gruppi o un'azione diversa, **quando** il PEP costruisce l'input
   OPA, **allora** questi valori non compaiono nell'input: subject viene solo dal
   metadata autenticato e action solo da `grpc.UnaryServerInfo.FullMethod`.
6. **Dato** una RPC non dichiarata, oppure una RPC dati con `allowed_repos` o
   `acl_groups` vuoti, **quando** la policy è valutata, **allora** il risultato è
   deterministico `allow=false` con la reason chiusa appropriata; nessun default
   tenant/repository/gruppo è inventato.
7. **Dato** Retrieval Engine o Semantic Cache avviato, **quando** serve una RPC
   unary o server-streaming, **allora** la catena esegue prima l'estrazione
   `secctx`, poi il PEP OPA, poi l'handler; entrambi i server hanno lo stesso
   comportamento fail-closed e `ImpactAnalysis` non può bypassare il PEP.
8. **Dato** il file Rego reale, **quando** viene eseguito il corpus di test OPA,
   **allora** allow, deny, scope vuoto, azione sconosciuta, confusione tra tenant
   e input controllato e ordinamento irrilevante degli scope sono verificati
   deterministicamente con OPA reale e CPU-only.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `SecurityContext` assente/malformato | `Unauthenticated`; zero chiamate PDP/handler |
| tenant o user vuoto | OPA nega; `PermissionDenied` |
| scope repo/ACL vuoto su RPC dati | OPA nega; nessun significato wildcard |
| RPC sconosciuta | default deny `unknown_action` |
| health RPC autenticata | consentita con tenant/user validi; gli scope dati non sono richiesti |
| endpoint/path/timeout/config non validi | errore startup, prima di `Serve` |
| HTTP in produzione | rifiutato; ammesso solo con flag dev esplicito |
| redirect PDP | rifiutato, non seguito |
| PDP lento oltre il minor deadline | richiesta cancellata/`Unavailable`, mai allow |
| HTTP non-200, body >64 KiB, JSON/schema/reason non validi | errore PDP fail-closed |
| `decision_id` assente | decisione valida; il campo è opzionale e non diventa autorizzativo |
| reason OPA non riconosciuta | normalizzata a `policy_denied`, cardinalità bounded |
| registrazione metriche duplicata | errore startup esplicito |

## 5. Threat model e non-goals

### Threat model

Attori ostili: client non autenticato; tenant autenticato malevolo; prompt/output
LLM compromesso; servizio interno che forgia body o metadata; rete/PDP guasto o
compromesso; risposta PDP costruita per esaurire memoria o metric cardinality.

Asset: isolamento tenant/repository/gruppo, identità autenticata, disponibilità
controllata e auditabilità della decisione. Confini di fiducia: solo il gateway
T6.1 autentica JWT e crea `SecurityContext`; il runtime gRPC fornisce il metodo;
request/body/query/prompt/LLM sono sempre non fidati. Mitigazioni: default deny,
input costruito internamente, allow-list RPC in Rego, timeout e body bound,
redirect vietati, schema stretto, reason normalizzata, nessun secret nei log,
nessuna cache decisionale, enforcement server-side su ogni server gRPC.

### Non-goals

- Nessun filtro Cypher/Qdrant/OpenSearch o RBAC datastore (T6.3).
- Nessuna autorizzazione di `acl_scope`, summary o cache key (T6.4).
- Nessuna verifica citation/WORM audit (T6.5).
- Nessun Envoy/JWT gateway/mTLS o transcoding (T6.6/T7.1).
- Nessun ruolo nuovo, modifica proto, ADD, contratto o ADR.
- Nessun SpiceDB/ReBAC, decision cache, default allow o policy generata da LLM.
- Nessun dato di request/body come attributo autorizzativo in T6.2.

## 6. Vincoli dall'ADD

- Modulo 3 §2.1: PEP distribuito negli interceptor dei servizi; PDP
  centralizzato OPA per ABAC su policy relativamente statiche.
- Modulo 3 §2.1 e D7: claims autenticati propagati nel `SecurityContext`; nessun
  contesto implicito o caller-provided sostitutivo.
- Modulo 3 §2.2/§2.4: i filtri futuri derivano dallo stesso context e mai
  dall'LLM. Questa SPEC preserva il confine, ma non anticipa i filtri T6.3.
- SPEC-011 resta invariata: `secctx.UnaryServerInterceptor` continua a essere
  plumbing e composabile; T6.2 aggiunge un PEP separato dopo l'estrazione.
- SPEC-055: subject proviene esclusivamente da JWT verificato al gateway.

## 7. Test plan

- Unit Go `libs/go/eci/authz`: scenari 1–7 unary e stream con fake PDP
  osservabile, server HTTP controllato, deadline, redirect, response
  bound/schema e metriche.
- Policy `deploy/compose/opa/policies/eci_authz_test.rego`: scenario 8 e tabella
  completa allow/deny; eseguita con immagine ufficiale pin
  `openpolicyagent/opa:1.20.1`.
- Integration Go: container OPA reale con la policy versionata, chiamata REST
  attraverso il client production, allow e deny verificati.
- Wiring: test/static assertion che entrambi i main compongano
  `secctx -> authz` e che l'assenza PDP impedisca di entrare in `Serve`.
- CPU-only; nessuna GPU, modello, expected golden o dato T5.6.

Red phase richiesta: i test dell'interceptor e della policy devono fallire per
package/file/wiring assente prima dell'implementazione.

## 8. Osservabilità

- Span client: `eci.authz.check`.
- Attributi bounded: `eci.authz.service`, `eci.authz.outcome`,
  `eci.authz.reason`; mai tenant, user, repo, group, token, prompt o policy input.
- Counter:
  `eci_authz_decisions_total{service,outcome,reason}` dove `service` è config
  deploy bounded, `outcome ∈ {allow,deny,error}` e `reason` usa la tassonomia
  chiusa più `pdp_error`.
- Histogram:
  `eci_authz_pdp_duration_seconds{service,outcome}` senza action/identity.
- Entrambi i servizi espongono il default registry su `/metrics`; Prometheus
  dev li scrapa come `eci-query-services` sulle porte host 9105/9106.
- Log OPA JSON in dev; decision log remoto/WORM è T6.5. Il PEP non stampa
  subject, scope, body o errori crittografici/rete nel messaggio gRPC.

## 9. Criteri di accettazione

- [x] `deploy/compose/opa/policies/eci_authz.rego` è default-deny e testato con
  OPA reale pin 1.20.1.
- [x] Input OPA contiene solo subject autenticato e full method runtime; test di
  forged body/prompt prova l'assenza di influenza.
- [x] Orchestrator non costruisce tenant/scope impliciti e rifiuta un
  `SecurityContext` assente prima di invocare tool o backend.
- [x] Missing/malformed context, deny, timeout, outage e malformed PDP response
  sono fail-closed e non invocano l'handler.
- [x] Retrieval Engine e Semantic Cache installano `secctx -> authz`.
- [x] `ImpactAnalysis` server-streaming attraversa il PEP; non esiste un path
  stream non protetto.
- [x] Lo stack dev include OPA pin, healthcheck e policy read-only; nessun secret.
- [x] Metriche/span hanno nomi e cardinalità conformi a §8.
- [x] Nessun file in `docs/add/**` o `contracts/**` cambia.
- [x] `task build`, `task lint`, `task test`, `task guard` verdi in CI.
- [x] SPEC passa il secondo review avversariale, poi è `implemented`; diventa
  `verified` solo con evidenza CI/PR reale.

## 10. Review avversariale di approvazione

Pass eseguito il 2026-08-29 contro ADD, D7, SPEC-011 e SPEC-055.

- **Contraddizione ADD / isolamento:** nessuna: OPA è il PDP prescritto; policy e
  PEP sono default-deny e non sostituiscono i filtri T6.3.
- **Input LLM/non fidato:** escluso per costruzione dall'interfaccia; il request
  body non è passato a `Decide`.
- **Write/materialized views/idempotenza:** nessuna write introdotta; Semantic
  Cache Put è solo action-gated e l'idempotenza esistente resta invariata.
- **Fail-closed:** startup, timeout, outage, schema e reason ignote non producono
  mai allow; non esiste cache stale.
- **Deterministico/probabilistico:** policy Rego e comparator booleani; nessun
  LLM, embedding, fuzzy matching o judge.
- **Contratti/ADR:** proto e ADD sono letti, non modificati; interceptor separato
  preserva la semantica storica di SPEC-011.
- **Traversal/deadline:** nessuna traversal; il PDP usa il deadline chiamante e
  un timeout massimo configurato.
- **Decisione architetturale nascosta:** nessuna. OPA centralizzato, PEP
  distribuito e separazione T6.2/T6.3 sono già prescritti dall'ADD/task-tree.

Esito: `approved`.

## 11. Evidenza di implementazione

Implementato il 2026-08-29 nel commit `684ed4950694cb98cc280678b6489da893e30f2a`.

- Red phase Go: `go test ./eci/authz` falliva su `Decision`,
  `DecisionClient`, `New` e interceptor assenti; il test stream aggiunto dopo
  l'audit falliva su entrambi gli interceptor stream assenti.
- Red phase Rego: OPA ufficiale 1.20.1 riportava `FAIL: 10/10` senza policy.
- Green policy: `/tmp/opa_linux_amd64_static test
  deploy/compose/opa/policies` → `PASS: 11/11` dopo l'hardening degli scope
  blank/non-string.
- Green locale: `task build`, `task lint`, `task guard`; unit authz/secctx e 23
  test Orchestrator CPU-only verdi. `docker compose config --quiet` verde con
  credenziali temporanee ignorate e rimosse subito dopo la validazione.
- `task test` locale: tutti i test non-container eseguiti sono verdi; fallisce
  esclusivamente nelle suite Testcontainers preesistenti e nella nuova OPA
  integration perché Docker Desktop non è in esecuzione
  (`Cannot connect to ... docker.sock`). Nessun test è stato skipped o
  indebolito; la verifica container resta obbligatoria in CI.

Deviazione controllata rispetto all'interfaccia approvata iniziale: durante il
wiring è stato rilevato che `ImpactAnalysis` è server-streaming. La SPEC e i
test sono stati corretti prima del merge aggiungendo gli interceptor stream
`secctx -> authz`; lasciare solo unary avrebbe creato un bypass reale.

Verifica finale PR [#69](https://github.com/nepryoon/eci/pull/69), commit
funzionale `8370f7e3b12dc9c1ae1b86590a886dc26e8fb247`:

- GitHub Actions run
  [33241977832](https://github.com/nepryoon/eci/actions/runs/33241977832):
  `build-lint-test` verde in 8m17s, `guard` verde in 1m50s.
- Il run include `TestProductionClientAgainstRealPinnedOPA`, che esegue OPA
  1.20.1 reale, `opa test` sul corpus versionato e decisioni allow/deny via il
  client production; `libs/go/eci/authz` verde.
- La suite Orchestrator completa è verde con processo Retrieval reale e prova
  che la telemetria authz non può bloccare il fixture tramite pipe non drenati.
- I due finding P2 (metriche registrate ma non esposte; cleanup container
  registrato troppo tardi) sono coperti da regressioni, risposti e risolti.
- `/metrics` è verificato per entrambi i servizi e Prometheus scrapa 9105/9106.

Esito T6.2: `verified` senza modifica di ADD o contratti condivisi.
