# SPEC-077 — Boundary gRPC Python autenticato e fail-closed
Stato: verified
Task-tree: T7.1b/T7.1c · Libreria: libs/py/eci_core · ADD: Modulo 3 §§2.1, 3.1, Modulo 4 §1
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto, ADR-0012, ADR-0017, ADR-0018

## 1. Obiettivo

I runtime Python condividono un solo interceptor gRPC server che estrae il
SecurityContext esclusivamente dai metadata binari autenticati, lo valida,
chiede una decisione OPA fail-closed e lo rende disponibile al solo handler
della chiamata. Una client OPA stretta usa configurazione di processo fidata,
deadline bounded e risposte limitate.

## 2. Interfaccia

```python
METADATA_KEY = "eci-security-context-bin"

class AuthenticatedServerInterceptor(grpc.ServerInterceptor):
    def __init__(self, authorizer: DecisionClient) -> None: ...

def security_context() -> SecurityContext: ...

class OPAClient:
    @classmethod
    def from_environment(cls, service: str) -> "OPAClient": ...
    def preflight(self) -> None: ...
    def decide(self, context, subject: SecurityContext, method: str) -> Decision: ...
```

## 3. Comportamento

1. **Dato** metadata unico e valido e OPA allow, **quando** una RPC unary o
   server-streaming entra, **allora** il handler legge lo stesso contesto.
2. **Dato** metadata assente, duplicato, malformato, sovradimensionato o scope
   non canonico, **quando** entra, **allora** fallisce Unauthenticated prima di
   OPA e handler.
3. **Dato** OPA deny, **quando** entra, **allora** fallisce PermissionDenied e
   il handler non parte.
4. **Dato** OPA indisponibile, timeout o risposta invalida/incoerente, **quando**
   entra, **allora** fallisce Unavailable senza dettagli PDP.
5. **Dato** una RPC streaming, **quando** termina o viene cancellata, **allora**
   il ContextVar viene ripristinato e non trapela alla chiamata successiva.
6. **Dato** configurazione OPA HTTP non esplicitamente ammessa, redirect, URL
   con userinfo/query/fragment o timeout fuori limite, **quando** inizializza,
   **allora** il processo fallisce chiuso.
7. **Dato** una decisione, **quando** serializzata, **allora** OPA riceve solo
   subject autenticato e full method; mai request, prompt, trace ID o payload.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| handler chiama `security_context()` fuori da RPC | errore runtime generico |
| lista repo/gruppi vuota, duplicata o oltre 128 | Unauthenticated |
| valore blank/control/>256 byte | Unauthenticated |
| risposta OPA oltre 64 KiB | Unavailable |
| allow=true con reason diverso da `allow` | Unavailable |
| reason ignota su deny | normalizzata `policy_denied` |

## 5. Non-goals

- Autenticare JWT o derivare scope dal body: resta responsabilita' gateway.
- Fidarsi di SecurityContext presente nella request.
- Implementare policy business nel client: OPA resta PDP centrale.
- Aggiungere runtime orchestrator/verification/summarization in questa SPEC.
- Registrare prompt, scope, valori metadata o decision payload.

## 6. Vincoli dall'ADD

- Modulo 3 §2.1: PEP prima dell'accesso e default deny.
- Modulo 3 §3.1: contesto autenticato propagato, non costruito dal testo.
- Modulo 4 §1: deadline e servizio Python stateless.
- ADR-0012/0017/0018: metadata-only, OPA fail-closed e runtime Python futuri.

## 7. Test plan

- Unit con handler gRPC reali in-process per unary e server-streaming.
- Fake DecisionClient per allow/deny/error e registrazione input.
- HTTP server locale per schema OPA, limite, redirect e timeout.
- Ruff e pytest `libs/py`, aggregate build/lint/test/security.

## 8. Osservabilita'

Nessun dato subject nei log. Il chiamante runtime misura solo outcome/reason
bounded; l'interceptor espone eccezioni gRPC generiche.

## 9. Criteri di accettazione

- [x] Test nuovi osservati rossi contro libreria assente.
- [x] `ruff check eci_core`
- [x] `pytest -q`
- [x] `task build && task lint && task test && task test:security`
- [x] Review duplicate metadata/context leak/OPA SSRF/redirect/size/deadline.

## 10. Review avversariale pre-implementazione

Il body non partecipa mai all'autorita'. L'interceptor richiede esattamente un
metadata binario, con limiti prima del parse, e conserva il subject in un
ContextVar soltanto per la durata effettiva del handler/generatore. Il client
OPA costruisce URL solo da configurazione startup validata, vieta redirect e
normalizza gli errori prima del wire gRPC.

## 11. Evidenza di verifica

- Test red `99974bb`; implementazione `3bfe0ee`; suite/redirect
  `16b6b42`/`e98dc0d`; gate security `dcee4c0`.
- `ruff check eci_core` e `pytest -q`: 38 test verdi, inclusi 18 scenari
  dedicati al boundary autenticato.
- `task build`, `task lint`, `task test` e `task test:security` verdi; il gate
  security esegue esplicitamente `test_grpc_server.py`.
- Review conferma metadata unico/limitato, nessuna autorita' dal body,
  ContextVar ripristinato nel `finally`, OPA URL di processo validato, redirect
  vietati, risposta limitata a 64 KiB e timeout 0–2 secondi.
