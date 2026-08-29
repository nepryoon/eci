# SPEC-059 — Citation ACL verification e audit trail WORM
Stato: verified
Task-tree: T6.5 · Servizio: `services/verification` · ADD: Modulo 2 §3; Modulo 3 §2.6.4–§2.6.5; Modulo 4 §2.3
Contratti: `contracts/proto/eci/retrieval/v1/retrieval.proto` (`SecurityContext`), `contracts/cypher/schema.cypher` (`tenant_id`, `repo`, `acl_group`)

## 1. Obiettivo

Il gate deterministico verifica simboli, relazioni e citazioni esclusivamente
nello scope autenticato e non rivela se un'entità fuori scope esista. Ogni
decisione viene registrata prima del ritorno in un audit trail realmente WORM,
con S3 Object Lock COMPLIANCE verificato su MinIO e failure mode fail-closed.

## 2. Interfaccia

```python
@dataclass(frozen=True)
class AuthorizationScope:
    tenant_id: str
    user_id: str
    allowed_repos: tuple[str, ...]
    acl_groups: tuple[str, ...]
    trace_id: str

class EvidenceStore(Protocol):
    def symbol(self, node_id: str, scope: AuthorizationScope) -> SymbolEvidence | None: ...
    def relation_exists(
        self, source_id: str, target_id: str, edge_type: str,
        max_depth: int, scope: AuthorizationScope,
    ) -> bool: ...

class AuditSink(Protocol):
    def append(self, event: VerificationAuditEvent) -> AuditReceipt: ...

class Verifier:
    def verify(
        self, candidate: CandidateAnswer,
        security_context: retrieval_pb2.SecurityContext, *, attempt: int = 0,
    ) -> VerificationResult: ...

class MinioWormAuditSink:
    def __init__(
        self, client: minio.Minio, bucket: str,
        retention_days: int = 365,
        now: Callable[[], datetime] = ...,
    ) -> None: ...
    def initialize(self) -> None: ...
    def append(self, event: VerificationAuditEvent) -> AuditReceipt: ...
```

```json
{
  "schema_version": "eci.verification.audit.v1",
  "event_id": "UUID",
  "recorded_at": "RFC3339 UTC",
  "trace_id": "authenticated trace id",
  "tenant_id": "authenticated tenant",
  "user_id": "authenticated user",
  "allowed_repos": ["sorted", "unique"],
  "acl_groups": ["sorted", "unique"],
  "attempt": 0,
  "outcome": "approved|corrected|regenerated|degraded",
  "issue_codes": ["stable order"],
  "citation_access": [{"node_id": "...", "decision": "authorized|inaccessible"}]
}
```

## 3. Comportamento

1. **Scope valido e citazione autorizzata.** Given un `SecurityContext`
   autenticato e una evidence con tenant/repo/gruppo inclusi, When `verify`
   esegue tutti i check, Then passa lo scope normalizzato a ogni query, valida
   nuovamente le label restituite e approva/corregge la citazione corrente.
2. **Citazione fuori scope o inesistente.** Given un node id inesistente oppure
   esistente solo fuori scope, When viene citato, Then entrambi producono lo
   stesso `citation-inaccessible`, nessuna provenance viene restituita e
   l'esito richiede rigenerazione senza existence oracle.
3. **Scope mancante, malformato o forgiato nel contenuto.** Given metadata senza
   tenant/user/repo/gruppo/trace validi oppure testo/claim che tenta di
   cambiare scope, When si verifica, Then il gate rifiuta prima dello store e
   usa soltanto il `SecurityContext` autenticato.
4. **Difesa in profondità sul backend.** Given uno store difettoso che restituisce
   evidence con label non incluse nello scope, When il verifier la riceve,
   Then la tratta come inaccessibile e non espone path, commit o label.
5. **Audit prima del ritorno.** Given qualunque outcome deterministico, When la
   decisione è pronta, Then viene scritto un solo evento canonico privo di
   query/answer/source/token e soltanto dopo viene restituito il risultato.
6. **Audit indisponibile.** Given errore di append o retention non verificabile,
   When `verify` termina i check, Then solleva `VerificationAuditError`, non
   restituisce l'answer e registra nel trace il failure bounded.
7. **WORM reale.** Given MinIO reale e bucket object-lock-enabled, When un evento
   viene appeso con COMPLIANCE, Then retention e version id sono leggibili e
   overwrite/delete della versione non possono alterare o eliminare il payload
   prima della scadenza.
8. **Determinismo e osservabilità.** Given input/scope equivalenti con ordine o
   duplicati differenti, When vengono verificati, Then scope, issue ordering e
   audit schema sono stabili; metriche/span non contengono identità, node id,
   repo, path o testo.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| campo scope vuoto, whitespace/control char, >128 valori o valore >256 byte | `VerificationAuthorizationError`, zero query allo store |
| evidence `tenant_id`/`repo`/`acl_group` fuori scope | indistinguibile da not-found; nessuna provenance |
| errore symbol/relation store | `VerificationBackendError`, fail-closed |
| relazione coinvolge entità fuori scope | `relation-nonexistent`, senza distinguerla da relazione assente |
| bucket esistente senza Object Lock | bootstrap fallisce; non tenta conversione o fallback |
| retention <1 giorno, timestamp naive/non UTC | configurazione rifiutata |
| append, get-retention o COMPLIANCE mismatch | `VerificationAuditError`, nessun risultato |
| delete marker senza version id | non è prova di cancellazione; verifica sempre la versione WORM esplicita |

## 5. Non-goals

- Nessun LLM judge, filtro ACL costruito da prompt o post-filter sostitutivo.
- Nessuna modifica ADD/proto/Cypher, nessuna UI di audit e nessuna retention
  legale definitiva per produzione.
- Nessun audit di query/risposte/sorgente in chiaro e nessuna GPU.
- Nessuna promessa di HA/replica MinIO (T7.1); questa SPEC prova semantica WORM.

## 6. Vincoli dall'ADD e threat model

- Modulo 2 §3: symbol/relation/citation/syntax check deterministico e loop 2–3.
- Modulo 3 §2.6.4: ogni citazione deve essere autorizzata per l'utente.
- Modulo 3 §2.6.5: audit di query/accessi immutabile append-only/WORM.
- Modulo 3 §2.3–§2.5: tenant/repository/ACL prima dell'accesso e fail-closed.
- Modulo 4 §2.3: outcome e issue type osservabili senza cardinalità non bounded.

**Minacce:** caller/LLM forgia repo o node id; store omette filtro; attaccante usa
not-found vs denied come oracle; processo omette/modifica audit; operatore prova
delete/overwrite; audit indisponibile; log injection; secret leakage. Controlli:
scope solo da metadata autenticati, query scoped + label re-check, errore uniforme,
JSON tipizzato canonico, COMPLIANCE/versioning, verifica retention, fail-closed,
allow-list dei campi e metriche bounded.

## 7. Test plan

- Unit: scenari 1–6 e 8 in `verification/test_verifier.py` e
  `verification/test_audit.py`, con store/sink osservabili e fault injection.
- Integration CPU-only: MinIO
  `minio/minio:RELEASE.2025-04-22T22-12-26Z`; crea bucket con lock, append,
  verifica payload/digest/retention/versioning, nega delete esplicita della
  versione e prova che il payload originale resta leggibile.
- Regressione: check esistenti symbol/relation/stale/syntax e bounded loop
  restano invariati per evidence autorizzata.

## 8. Osservabilità

- span `verification.verify`: attributi bounded `verification.outcome`,
  `verification.issue_count`, `verification.audit_status=written|failed`;
  eventi `verification.issue` con `verification.issue_code`/stage e
  `verification.audit_failure` senza detail backend.
- counter `eci_verification_citation_access_total{decision=authorized|inaccessible}`.
- counter `eci_verification_audit_append_total{outcome=written|failed}`.
- vietati label/attributi con tenant, user, repo, path, node id, trace id o testo.

## 9. Criteri di accettazione

- [x] Tutti gli scenari §3 hanno test red-before-green e passano CPU-only.
- [x] Store riceve lo scope su symbol e relation; re-check delle label fail-closed.
- [x] Fuori-scope e not-found sono indistinguibili e non restituiscono provenance.
- [x] Ogni risultato richiede append audit riuscito; nessun no-op di produzione.
- [x] MinIO reale prova COMPLIANCE, retention, version id e delete negato.
- [x] Audit payload non contiene query, answer, source, snippet, token/JWT.
- [x] `task build`, `task lint`, `task test`, test MinIO, `task guard` verdi.
- [x] SPEC `verified`, ADR-0013 `accepted`, CI verde e PR pronta al merge.

## 10. Review avversariale di approvazione

Pass eseguito il 2026-08-29. La SPEC non cambia ADD/contratti né autorizza
scritture alle viste materializzate; scope autenticato è l'unico input ACL e
le query restano pre-filtered. Denied/not-found uniforme evita leakage;
backend e audit falliscono chiusi. Nessun input LLM decide autorizzazione,
nessun traversal diventa unbounded, il loop/deadline esistente non cambia e
deterministic verification resta separata da metriche probabilistiche. La
scelta Object Lock è esplicita in ADR-0013; retention configurabile non è
controllabile dalla request. Non sono emerse contraddizioni o indebolimenti.

## 11. Evidenza di verifica

Run GitHub Actions `33249327331` sul commit `e4b3481`: `build-lint-test`
PASS (7m56s), `guard` PASS (1m47s), `datastore-security-integration` PASS
(12m27s) e `worm-audit-integration` PASS (28s). La prova MinIO reale ha
confermato modalità COMPLIANCE, retention/version id, preservazione della
versione originale dopo una nuova versione con payload diverso e rifiuto del
delete sulla versione bloccata. Suite Verification: 31 unit test passati; il
solo test marcato integration viene eseguito, senza skip, dal job dedicato.

Due finding review sono stati risolti con regressioni: endpoint di relazione
ricontrollati nello scope prima della query e limite di 1024 caratteri coerente
per tutti i node id accettati/auditati. Nessuna deviazione dall'ADD o dai
contratti condivisi.
