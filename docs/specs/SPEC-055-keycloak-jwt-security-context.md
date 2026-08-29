# SPEC-055 — IdP dev Keycloak e JWT → SecurityContext al gateway
Stato: verified
Task-tree: T6.1 · Servizio: services/api-gateway + deploy/compose · ADD: Modulo 3 §1.1, §2.1 e D7 `SecurityContext`
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto (letto, non modificato)

## 1. Obiettivo

Fornire un Identity Provider OIDC di sviluppo riproducibile basato su
Keycloak e il componente di autenticazione del gateway che trasforma un
access token JWT verificato nel `SecurityContext` protobuf esistente. Il
confine autentica issuer, audience, firma, algoritmo, scadenza e claim
obbligatori prima di produrre il contesto; ogni errore fallisce chiuso.

T6.1 autentica e costruisce il contesto. La decisione di autorizzazione PDP,
l'enforcement negli store e il proxy Envoy completo appartengono
rispettivamente a T6.2, T6.3 e T6.6.

## 2. Interfaccia

Package interno `services/api-gateway/internal/authn`:

```go
type Config struct {
    Issuer                   string
    Audience                 string
    AllowHTTPForDevelopment  bool
}

type ErrorCode string

const (
    ErrorMissingToken  ErrorCode = "missing_token"
    ErrorInvalidToken  ErrorCode = "invalid_token"
    ErrorInvalidClaims ErrorCode = "invalid_claims"
    ErrorInvalidTrace  ErrorCode = "invalid_trace_id"
)

type AuthError struct { Code ErrorCode }

func New(ctx context.Context, cfg Config, registerer prometheus.Registerer) (*Authenticator, error)

func (a *Authenticator) Authenticate(
    ctx context.Context,
    authorizationHeader string,
    trustedTraceID string,
) (*retrievalv1.SecurityContext, error)
```

`New` usa OIDC discovery e JWKS dell'issuer configurato. Gli unici JWT
accettati sono access token con claim `typ=Bearer` firmati `RS256`;
l'algoritmo non è configurabile dall'ambiente. `Issuer` e `Audience` sono
valori operativi trusted, mai letti da request, prompt o output LLM. HTTP è
rifiutato per default ed è consentito soltanto con l'opt-in esplicito di
sviluppo e per issuer loopback (`localhost`, `127.0.0.0/8` o `::1`).

`Authenticate` accetta esattamente `Authorization: Bearer <token>`, verifica
il token e costruisce il tipo già generato da D7:

```text
JWT sub           -> SecurityContext.user_id
JWT tenant_id     -> SecurityContext.tenant_id
JWT allowed_repos -> SecurityContext.allowed_repos
JWT acl_groups    -> SecurityContext.acl_groups
trustedTraceID    -> SecurityContext.trace_id
```

`sub` e `tenant_id` devono essere non vuoti e lunghi al massimo 256 byte.
`allowed_repos` e `acl_groups` devono esistere come array JSON di stringhe;
possono essere vuoti (utente autenticato ma privo di accesso), non possono
contenere stringhe vuote o duplicati. Il risultato è deduplicato e ordinato
lessicograficamente per propagazione deterministica. Oltre ai limiti per campo,
il `SecurityContext` protobuf codificato base64 deve restare entro 12 KiB, con
4 KiB riservati agli altri header del budget interno da 16 KiB; un insieme di
claim che supera questo limite cumulativo è `invalid_claims`. `trustedTraceID` proviene
dal tracing del gateway, non da JWT, query text o header applicativi arbitrari;
deve essere un W3C trace-id non-zero di 32 caratteri esadecimali minuscoli.

Il realm dev è importato da
`deploy/compose/keycloak/eci-realm.json` su Keycloak `26.7.2` pin-nato. Espone:

```text
realm:       eci
issuer dev:  http://localhost:8081/realms/eci
audience:    eci-gateway
client:      eci-dev-cli (public, direct grant solo per sviluppo)
```

Il realm contiene un solo utente di sviluppo con attributi non sensibili;
la password e le credenziali bootstrap sono placeholder risolti da variabili
d'ambiente al runtime. Nessuna password o client secret è versionata.

## 3. Comportamento

1. **Dato** un access token Keycloak `RS256` valido per issuer e audience
   configurati, con `sub`, `tenant_id`, `allowed_repos` e `acl_groups`,
   **quando** `Authenticate` riceve il Bearer token e un trace-id trusted
   valido, **allora** restituisce il `SecurityContext` canonico con gli stessi
   confini di autorizzazione, liste ordinate/deduplicate e trace-id fornito.
2. **Dato** un header assente, schema diverso da Bearer, token vuoto o più di
   una credenziale, **quando** autentico, **allora** ricevo
   `AuthError{Code: missing_token}` senza chiamare il verifier.
3. **Dati** token con firma/algoritmo non ammesso, issuer errato, audience
   errata o `exp` scaduto, **quando** autentico, **allora** ricevo sempre
   `invalid_token`, nessun `SecurityContext` e nessun dettaglio crittografico
   esposto al chiamante.
4. **Dato** un token crittograficamente valido ma privo di un claim richiesto,
   con claim del tipo JSON sbagliato, tenant/user vuoto, lista con elemento
   vuoto o cardinalità/dimensioni oltre i limiti, **quando** autentico,
   **allora** ricevo `invalid_claims` e il sistema fallisce chiuso.
5. **Dato** un token valido e testo utente/header aggiuntivi che dichiarano
   tenant, repository o gruppi diversi, **quando** autentico, **allora** il
   contesto deriva esclusivamente dai claim verificati e nessun input esterno
   può allargarne lo scope.
6. **Dato** un trace-id assente, zero, maiuscolo o malformato, **quando**
   autentico, **allora** ricevo `invalid_trace_id`; il valore non viene creato
   da input non trusted né normalizzato silenziosamente.
7. **Dato** il JWKS che ruota a una nuova chiave con `kid` distinto, **quando**
   arriva un token valido firmato dalla nuova chiave, **allora** il verifier
   aggiorna il key set remoto e autentica senza accettare chiavi rimosse dalla
   configurazione corrente oltre il caching previsto dalla libreria OIDC.
8. **Dato** il compose dev con variabili segrete fornite fuori Git, **quando**
   Keycloak importa il realm e viene richiesto un token reale per l'utente dev,
   **allora** discovery/JWKS/token endpoint sono disponibili e quel token
   attraversa con successo lo stesso `Authenticator` di produzione.

## 4. Errori ed edge case

| Condizione | Comportamento atteso |
|---|---|
| issuer/audience vuoti o URL issuer non assoluto | `New` fallisce prima di servire traffico |
| issuer HTTP senza opt-in dev, oppure HTTP non-loopback anche con opt-in | configurazione rifiutata |
| discovery/JWKS indisponibile all'avvio | `New` fallisce; nessun default allow |
| header Bearer oltre 16 KiB | `invalid_token`, nessuna allocazione non limitata |
| `sub`/tenant oltre 256 byte, più di 256 repo o gruppi, singolo valore oltre 256 byte | `invalid_claims` |
| `SecurityContext` base64 oltre 12 KiB | `invalid_claims`; nessun header parziale o errore di trasporto a valle |
| `allowed_repos`/`acl_groups` assenti | `invalid_claims`; assente non equivale a lista vuota |
| array presenti ma vuoti | autenticazione valida, scope vuoto |
| errore interno del recorder metriche | non trasforma un rifiuto in successo |

## 5. Threat model

**Asset:** isolamento tenant/repository/ACL, identità utente, chiave pubblica
trusted dell'issuer e metadata gRPC prodotti a valle.

**Avversari:** client anonimo, utente autenticato che modifica token o claim,
attaccante con token emesso per un'altra audience/issuer, prompt injection che
tenta di cambiare scope, replay di token scaduti e input sovradimensionati.

**Trust boundary:** solo configurazione del processo, discovery HTTPS/JWKS
dell'issuer configurato e claim di un JWT verificato possono influire sul
`SecurityContext`. Corpo, query string, prompt, output LLM, header tenant/repo e
campi request non autenticati sono fuori dal boundary e non vengono letti.

**Contromisure:** `RS256` e `typ=Bearer` fissi, verifica
issuer/audience/firma/expiry, required claims tipizzati, limiti di
cardinalità/dimensione, trace-id trusted separato, errori fail-closed, metriche
a cardinalità chiusa e nessun log di token/claim.
La compromissione dell'IdP è fuori dal perimetro; la protezione delle
credenziali avviene tramite env/Secret e non tramite file versionati.

## 6. Non-goals

- Nessuna policy OPA/PDP o decisione allow/deny su una risorsa (T6.2).
- Nessun filtro Cypher/Qdrant/OpenSearch o RBAC store (T6.3).
- Nessuna modifica a `SecurityContext` o ad altro contratto condiviso.
- Nessun token introspection fallback, token opaco, algoritmo simmetrico,
  client secret versionato o implicit default tenant.
- Nessun Envoy transcoding, SSE o rate limiting (T6.6).
- Nessun mTLS/service mesh o configurazione Keycloak production-ready (T7.1).

## 7. Vincoli dall'ADD

- Modulo 3 §1.1: componente Identity Provider/AuthZ OIDC/OAuth2.
- Modulo 3 §2.1: JWT con claims propagati come `SecurityContext`; interceptor
  gRPC a valle; mTLS separato.
- D7: riuso esatto di `SecurityContext {tenant_id, user_id, allowed_repos,
  acl_groups, trace_id}` e regola “mai costruito dall'LLM”.
- PLAYBOOK T6.1 dipende da T0.8 e deve precedere T6.2/T6.6.

## 8. Test plan

- Unit test table-driven per parsing Bearer, claim obbligatori/tipi/limiti,
  deduplica deterministica, trace-id e impossibilità di override esterno.
- Test OIDC con server `httptest`: chiavi RSA reali, discovery e JWKS; casi
  firma, `alg`, issuer, audience, expiry e rotazione `kid`.
- Test metriche con registry Prometheus isolato: serie ammesse soltanto
  `success|failure` e i quattro `reason` enumerati.
- Integration test testcontainers contro
  `quay.io/keycloak/keycloak:26.7.2`: import del realm senza secret versionati,
  token reale e mapping end-to-end.
- Scansione repository per password/token/client secret accidentalmente
  versionati; render `docker compose config` con env sintetico.
- Gate: `task build`, `task lint`, `task test`, `task guard`; il test Keycloak
  è CPU-only ma richiede Docker, come gli integration test già presenti.

## 9. Osservabilità

Span `eci.gateway.authenticate` con attributi a cardinalità chiusa:

```text
auth.outcome = success|failure
auth.reason  = success|missing_token|invalid_token|invalid_claims|invalid_trace_id
```

Counter:

```text
eci_gateway_authentication_total{outcome,reason}
```

Non sono ammessi come label/attributi: token, `sub`, tenant, repo, gruppi,
issuer arbitrario, testo utente o dettagli degli errori crittografici.

## 10. Criteri di accettazione

- [x] Keycloak dev pin-nato importa un realm riproducibile e rilascia un token
  reale con audience e claim richiesti.
- [x] Nessun secret o password è versionato; compose richiede env esplicite.
- [x] JWT access token validato per `typ=Bearer`, `RS256`, issuer, audience,
  firma ed expiry.
- [x] Claim obbligatori e limiti falliscono chiuso senza default tenant/scope.
- [x] Solo claim verificati possono costruire tenant/repo/gruppi del contesto.
- [x] Il protobuf D7 è riusato senza modificare `contracts/`.
- [x] Metriche/span non contengono dati sensibili o cardinalità non limitata.
- [x] Unit, OIDC crypto/JWKS rotation e Keycloak integration test verdi.
- [x] `task build`, `task lint`, `task test`, `task guard` verdi.

## 11. Review avversaria per approvazione

Review completata il 2026-08-29 tentando di invalidare la proposta rispetto
ad ADD, contratti e threat model:

- **Tenant isolation / input LLM:** nessun campo del contesto deriva da body,
  query, prompt, output LLM o header di scope; solo claim verificati. Liste
  vuote restano vuote e non attivano alcun default tenant/repo/gruppo.
- **Token confusion / downgrade:** aggiunto `typ=Bearer`; algoritmo fissato a
  `RS256`; issuer, audience, firma ed expiry sono tutti obbligatori. Nessun
  introspection fallback o symmetric key.
- **HTTP dev:** l'opt-in iniziale avrebbe potuto essere attivato per un issuer
  remoto in chiaro; ristretto a loopback, rendendo l'eccezione non riusabile
  silenziosamente in produzione.
- **Resource exhaustion:** header, identità, cardinalità e singoli valori sono
  limitati; metric labels provengono da enum chiusi.
- **Shared contracts:** D7 viene soltanto importato; nessun campo o semantica
  del protobuf cambia e quindi non serve ADR.
- **Fail closed / dependencies:** discovery o verifica indisponibili falliscono;
  T6.2/T6.3 restano responsabili di PDP e enforcement, senza default allow qui.
- **Secrets:** realm e compose contengono solo riferimenti env obbligatori; il
  test usa credenziali effimere generate nel processo.

Esito: nessuna contraddizione con l'ADD, nessun indebolimento dell'isolamento,
nessun input controllato dall'LLM nel trust boundary e nessuna decisione
architetturale nuova non documentata. SPEC approvata per implementazione.

## 12. Implementazione e deviazioni

Implementazione in `services/api-gateway/internal/authn`, con
`github.com/coreos/go-oidc/v3` v3.20.0 per discovery, JWKS e verifica JWT,
Prometheus client v1.24.1 e OTel v1.44.0. La libreria OIDC viene configurata
esplicitamente con il solo `RS256`; i claim applicativi vengono decodificati
soltanto dopo la verifica crittografica. In risposta al finding P2 della
review, `New` effettua inoltre un preflight reale e bounded del `jwks_uri`:
rifiuta redirect, status non-200, payload oltre 1 MiB, set vuoti/malformati e
set senza una chiave pubblica RSA utilizzabile per `RS256`. Discovery sana ma
JWKS inutilizzabile non può quindi produrre una readiness falsa.

Il realm dev è importato dal compose con Keycloak `26.7.2`. La password
bootstrap e quella dell'utente `eci-dev` sono env obbligatorie; l'integration
test genera valori casuali in memoria. `docker compose config` è risultato
verde con env locali temporanee non versionate; JSON e script shell sono stati
validati staticamente.

Test locali verdi: unit, required claims/limiti, metriche/span, RSA/JWKS,
issuer/audience/expiry/algoritmo e rotazione `kid`; inoltre `task build`,
`task lint` e `task guard`. `task test` locale arriva ai test Docker ma fallisce
perché il daemon Docker Desktop non è raggiungibile, incluso il nuovo test
Keycloak; la stessa limitazione produce i 10 errori testcontainers orchestrator
preesistenti. I criteri Keycloak end-to-end e gate completi restano aperti fino
all'esecuzione CI su runner Docker-capable.

Nessuna deviazione da ADD o contratti. L'HTTP dev è più restrittivo della sola
flag configurabile: è accettato esclusivamente per issuer loopback. Il servizio
non espone ancora un listener, intenzionalmente: Envoy/transcoding/SSE/rate
limiting appartengono a T6.6; T6.1 consegna il boundary interno autenticante che
T6.6 comporrà.

## 13. Evidenza di verifica

Il commit funzionale `7450cf1` è stato verificato dal run GitHub Actions
[`33238726705`](https://github.com/nepryoon/eci/actions/runs/33238726705):
`build-lint-test` PASS in 7m51s e `guard`/proto checks PASS in 1m24s. Nel job
completo `go test` su `services/api-gateway/internal/authn` è PASS in 34.479s;
questo include il container `quay.io/keycloak/keycloak:26.7.2`, import reale
del realm, emissione di un access token con password effimere generate a
runtime e autenticazione tramite lo stesso `Authenticator` di produzione.

Il finding JWKS è coperto da regressioni che fallivano sul comportamento
precedente e ora dimostrano il rifiuto all'avvio sia per HTTP 503 sia per un
key set vuoto. Il thread di review è stato risposto e risolto soltanto dopo il
push del fix. Nessun file in `contracts/` o nell'ADD è stato modificato e la
scansione dei file T6.1 non rileva chiavi private, token o client secret.
