# SPEC-057 — Enforcement in-query su Neo4j, Qdrant e OpenSearch
Stato: approved
Task-tree: T6.3 · Servizio: `services/ingestion`, sink materializzati, `services/retrieval-engine` · ADD: Modulo 3 §2.1–2.5
Contratti: `contracts/jsonschema/hybrid-graph.json` (estensione additiva, ADR-0010), `contracts/proto/eci/retrieval/v1/retrieval.proto` (sola lettura)

## 1. Obiettivo

Applicare lo stesso scope autenticato prima di ogni lettura in Neo4j, Qdrant e
OpenSearch e materializzare su ogni store le etichette necessarie. Nessun
parametro RPC, testo utente o output LLM può ampliare `tenant_id`, repository o
gruppi ACL; dati non etichettati, scope vuoti e store security non configurati
falliscono chiusi.

T6.3 è un'unica unità cross-store perché l'invariante ADD §2.5 è l'identità del
filtro sulle tre gambe: spezzarlo consentirebbe finestre di leakage tra store.

## 2. Interfaccia

```rust
pub struct IngestionScope {
    pub tenant_id: String,
    pub repo: String,
    pub acl_group: String,
}

impl IngestionScope {
    pub fn new(tenant_id: String, repo: String, acl_group: String)
        -> Result<Self, ScopeError>;
}

pub fn persist_parsed_file(
    client: &mut postgres::Client,
    scope: &IngestionScope,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[CodeChunk],
) -> Result<PersistSummary, PersistError>;
```

```go
package accessscope

type Scope struct {
    TenantID    string
    AllowedRepos []string
    ACLGroups    []string
}

func FromContext(ctx context.Context) (Scope, error)
func (s Scope) Neo4jParams() map[string]any
func (s Scope) QdrantMust() []*qdrant.Condition
func (s Scope) OpenSearchFilter() []map[string]any
func (s Scope) OpenSearchHeaders() http.Header
```

`FromContext` legge soltanto `secctx.FromContext(ctx)`, valida valori non vuoti,
limiti e duplicati, e restituisce copie ordinate deterministicamente. Le
request RPC non sono parametri dell'interfaccia.

Materialized security labels:

```json
{"tenant_id":"tenant-a","repo":"repo-a","acl_group":"engineering"}
```

Predicato Neo4j obbligatorio per ogni nodo letto o attraversato:

```cypher
n.tenant_id = $tenant_id
AND n.repo IN $allowed_repos
AND n.acl_group IN $acl_groups
```

Filtro Qdrant obbligatorio:

```json
{"must":[
  {"key":"tenant_id","match":{"value":"tenant-a"}},
  {"key":"repo","match":{"any":["repo-a"]}},
  {"key":"acl_group","match":{"any":["engineering"]}}
]}
```

OpenSearch usa sia un `bool.filter` applicativo equivalente sia DLS su ruolo
read-only con sostituzioni da attributi autenticati extended-proxy. Il client
invia `x-proxy-user`, `x-proxy-roles` e attributi `x-proxy-ext-*` esclusivamente
dallo `Scope`; il backend OpenSearch è raggiungibile solo dal servizio.

## 3. Comportamento

1. **Dato** un ingestion scope valido, **quando** nodi/chunk/embedding vengono
   materializzati, **allora** tutti e tre gli store contengono le stesse
   etichette piatte e indicizzate; un evento privo di etichette non riceve
   placeholder o default.
2. **Dato** un `SecurityContext` autenticato, **quando** si esegue `GetNode`,
   full-text o hydration Neo4j, **allora** la query contiene i tre predicati
   positivi prima di `LIMIT` e usa soltanto parametri derivati dal context.
3. **Dato** una traversata o impact analysis, **quando** il path attraversa un
   nodo fuori scope, **allora** seed, ogni nodo del path e risultato sono
   filtrati e il blast radius si arresta senza rivelare il nodo.
4. **Dato** vector search, **quando** Qdrant riceve `QueryPoints`, **allora** il
   filtro non è mai nil e contiene nel `must` tenant, repo e ACL; i payload
   index includono `tenant_id` keyword `is_tenant=true`, repo e ACL keyword,
   con HNSW `payload_m=16,m=0`.
5. **Dato** full-text/source hydration, **quando** Retrieval Engine chiama
   OpenSearch, **allora** applica filtro pre-retrieval e identità extended-
   proxy derivati dal context; la role DLS read-only replica tenant/repo/ACL e
   una variabile assente produce errore, non fallback.
6. **Dato** un candidate set fuso, **quando** viene idratato o inviato a
   reranker/packing, **allora** un re-check Neo4j autorizzato elimina prima
   qualunque id non più visibile; zero candidate autorizzati è un esito vuoto,
   non un fallback non filtrato.
7. **Dato** body RPC con `repos`, prompt o metadata applicativo forgiati,
   **quando** differiscono dal metadata autenticato, **allora** possono solo
   restringere i risultati funzionali e non ampliano mai lo scope.
8. **Dato** configurazione nativa, **quando** vengono validate policy/grant,
   **allora** Neo4j Enterprise parte da privilegi minimi property-based,
   OpenSearch usa un ruolo read-only DLS, e Community/security-plugin-off sono
   dichiarati dev-only senza essere presentati come difesa nativa verificata.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Context assente/malformato | `Unauthenticated`, normalmente intercettato prima dell'handler |
| tenant/repo/ACL vuoto o blank | `PermissionDenied`; zero query store |
| scope oltre i limiti o valore con CR/LF | `PermissionDenied`; nessun header injection |
| campo security assente nel record | confronto positivo non soddisfatto; invisibile |
| request repo fuori `allowed_repos` | intersezione vuota; zero risultati, mai ampliamento |
| seed Neo4j fuori scope | `NotFound`/zero traversal senza distinguerlo da assente |
| Qdrant filter non costruibile | errore fail-closed; nessuna query senza filter |
| OpenSearch DLS attribute assente | 4xx/security error propagato; nessun retry anonimo |
| re-check Neo4j fallisce | intera richiesta fallisce prima del packing |
| record cambia ACL tra retrieval e re-check | rimosso dal candidate set |
| GDS globale | proibito; solo proiezioni per tenant/ACL, fuori dal runtime corrente |
| Neo4j Community | solo test del filtro applicativo; nessuna dichiarazione di RBAC nativo |

## 5. Threat model e non-goals

### Threat model

Attori: tenant autenticato malevolo, prompt/output LLM compromesso, client che
forgia request/body, producer o consumer interno mal configurato, record legacy
non etichettato, store raggiunto con credenziali eccessive, cambio ACL TOCTOU.
Asset: isolamento tenant/repo/ACL, contenuto sorgente, topologia del grafo,
embedding e full-text. Confini: solo JWT validato crea `SecurityContext`; solo
ingestion system-to-system crea la provenance; body/prompt/LLM non sono fidati.
Mitigazioni: filtri positivi pre-retrieval identici, query parametrizzate,
must non-nil, DLS/RBAC least-privilege, header sanitizzati, re-check prima del
packing, dati unlabeled invisibili, zero wildcard/default e metriche bounded.

### Non-goals

- Nessun cache/summary `acl_scope` (T6.4), citation/WORM (T6.5), gateway/mTLS
  production (T6.6/T7.1) o security matrix completa (T6.7).
- Nessun post-filter come sostituto dei tre filtri pre-retrieval.
- Nessuna collection/database per tenant o GDS globale.
- Nessun filtro generato da LLM, fuzzy authorization o default tenant.
- Nessun backfill automatico di eventi storici e nessuna modifica T5.6.

## 6. Vincoli dall'ADD

- Modulo 3 §2.1: scope soltanto dal `SecurityContext` autenticato.
- Modulo 3 §2.2(a): predicati Cypher obbligatori in ogni query, mai dall'LLM.
- Modulo 3 §2.2(b): RBAC Neo4j property-based come secondo livello; Enterprise
  è vincolo di licenza e non può essere simulato con Community.
- Modulo 3 §2.2 GDS: proiezioni native globali non sono security-safe.
- Modulo 3 §2.3: Qdrant `must`, `is_tenant=true`, `payload_m:16,m:0`.
- Modulo 3 §2.4: OpenSearch DLS derivato dallo stesso contesto.
- Modulo 3 §2.5: filtri identici sulle tre gambe e re-check prima del packing.
- ADR-0010: etichette additive nella provenance; legacy unlabeled invisibile.

## 7. Test plan

- Unit Rust: validazione `IngestionScope`, transazione/payload con etichette,
  missing scope fallisce prima delle write.
- Unit Go `accessscope`: origine esclusiva dal context, ordinamento/copie,
  limiti, CR/LF, Qdrant must, OpenSearch filter/header.
- Unit query: fake Neo4j verifica testo/parametri per lookup, full-text,
  traversal, impact, hydration e re-check; request forgiata non amplia scope.
- Integration Qdrant: due tenant/repo/gruppi nella stessa collection, query A
  non restituisce B; verifica payload indexes/config.
- Integration Neo4j Community: due scope e path con nodo intermedio proibito;
  test applicativo arresta traversal. Grant Enterprise validati staticamente e
  in ambiente Enterprise solo quando una licenza autorizzata è disponibile.
- Integration OpenSearch security-enabled: due documenti e due identità,
  DLS A non restituisce B; in CI senza certificati/licenza, parser + mock HTTP
  verificano policy, header e filtro senza dichiarare il plugin reale testato.
- Regression: tutti i precedenti test retrieval con metadata autenticato;
  baseline/query T5.6 byte-identiche. CPU-only, nessuna GPU.

Red phase: test di scope e query obbligatorie devono fallire perché package,
etichette e parametri non esistono; il fallimento viene registrato nel commit.

## 8. Osservabilità

- Span `eci.retrieval.security_filter` con attributi bounded
  `eci.store ∈ {neo4j,qdrant,opensearch}`, `eci.outcome ∈ {allow,empty,error}`;
  mai tenant, user, repo, gruppo, query o token.
- Counter `eci_retrieval_security_filter_total{store,outcome}`.
- Counter `eci_retrieval_acl_recheck_removed_total{store="neo4j"}` senza id.
- Counter sink `eci_sink_security_labels_total{sink,outcome}` con outcome
  `accepted|missing|invalid`; cardinalità chiusa.
- Log di errore senza valori dello scope. Decision/audit WORM è T6.5.

## 9. Criteri di accettazione

- [ ] ADR-0010 e contratto additivo documentano migrazione fail-closed.
- [ ] Nuova ingestion richiede scope esplicito e lo propaga a ogni evento.
- [ ] Neo4j, Qdrant e OpenSearch memorizzano etichette piatte indicizzate.
- [ ] Ogni query Neo4j filtra seed, risultati e nodi intermedi.
- [ ] Ogni query Qdrant contiene i tre `must`; config multitenant conforme.
- [ ] OpenSearch applica filtro identico e role DLS read-only versionata.
- [ ] Re-check ACL avviene prima di hydration/reranker/packing.
- [ ] Test cross-scope e forged request sono fail-closed e CPU-only.
- [ ] Grant Neo4j Enterprise sono least-privilege e non dichiarati verificati
      senza un runtime Enterprise autorizzato.
- [ ] T5.6 baseline e `queries_v0.json` restano byte-identici.
- [ ] `task build`, `task lint`, `task test`, `task guard` verdi.
- [ ] Stato passa a `implemented`, poi `verified` solo con CI/PR reale.

## 10. Review avversariale di approvazione

- **Contraddizione ADD:** nessuna; i tre filtri e il re-check sono separati e
  obbligatori. L'uso di etichette additive risolve, non aggira, D2 incompleto.
- **Tenant isolation:** record legacy e scope vuoti sono invisibili; nessun
  default o wildcard. Tutti i nodi di path sono filtrati.
- **Input LLM/body:** l'unica origine query-plane è `secctx.FromContext`; le
  request possono solo restringere per intersezione.
- **Materialized views/idempotenza:** scrivono solo i sink esistenti; scope e
  dati condividono transazione/evento e gli upsert restano per id.
- **Fail closed:** errori DLS/RBAC/re-check non degradano verso query anonime.
- **Contratto condiviso:** modifica additiva coperta da ADR-0010, con legacy
  leggibile ma non autorizzato.
- **Traversal/deadline:** depth/limit restano bounded e lo stesso context/deadline
  viene passato a ogni store.
- **Decisioni nuove:** extended-proxy verso DLS e proprietà piatte sono
  esplicitate qui/ADR, non introdotte implicitamente.

Esito: approvata dopo seconda passata avversariale; nessun criterio indebolito.
