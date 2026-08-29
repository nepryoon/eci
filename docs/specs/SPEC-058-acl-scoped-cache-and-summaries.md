# SPEC-058 — Cache ACL-scoped e summary chiusi sui figli autorizzati
Stato: implemented
Task-tree: T6.4 · Servizio: `services/semantic-cache`, `services/summarization`, `libs/go/eci/accessscope` · ADD: Modulo 3 §2.6.3
Contratti: `contracts/proto/eci/semanticcache/v1/semanticcache.proto` (semantica del campo esistente, ADR-0012)

## 1. Obiettivo

Derivare `acl_scope` esclusivamente dal `SecurityContext` autenticato e usarlo
in ogni lookup/write della Semantic Cache. Un summary RAPTOR aggregato viene
restituito o riusato soltanto se il chiamante è autorizzato su tutti i suoi
contributori; altrimenti viene generata e cacheata una vista distinta costruita
dal solo sottoinsieme autorizzato.

T6.4 attraversa due servizi perché la garanzia ADD è end-to-end: il producer
deve generare una vista autorizzata e il cache backend deve impedire al client
di scegliere una partizione diversa.

## 2. Interfaccia

```go
package accessscope

func Fingerprint(scope Scope) string
```

```go
// Firma RPC invariata. Get/Put derivano acl_scope da ctx e richiedono che il
// campo ricevuto coincida con accessscope.Fingerprint(authenticatedScope).
func (s *Server) Get(
    ctx context.Context,
    key *semanticcachev1.CacheKey,
) (*semanticcachev1.GetResponse, error)

func (s *Server) Put(
    ctx context.Context,
    req *semanticcachev1.PutRequest,
) (*semanticcachev1.PutResponse, error)
```

```python
class SummaryNode(BaseModel):
    node_id: str
    level: SummaryLevel
    ast_hash: str
    source: str = ""
    child_ids: tuple[str, ...] = ()
    tenant_id: str
    repo: str
    acl_group: str

class SummaryCacheKey(BaseModel):
    ast_hash: str
    logic_fingerprint: str
    acl_scope: str

class SummaryResult(BaseModel):
    root_id: str
    summaries: dict[str, str]
    cache_hits: int
    cache_misses: int

class RaptorSummarizer:
    def summarize(
        self,
        nodes: Sequence[SummaryNode],
        root_id: str,
        security_context: SecurityContext,
    ) -> SummaryResult: ...
```

Fingerprint canonico (ADR-0012): SHA-256 di versione + tenant + liste
repository/gruppi deduplicate e ordinate, tutte length-prefixed `u32be` UTF-8.
`user_id`, trace, prompt, body RPC e output LLM sono esclusi. Go e Python
devono produrre lo stesso digest per lo stesso contesto.

Per un aggregate ristretto, la chiave usa un `effective_ast_hash` SHA-256
versionato che include l'hash del nodo e, nell'ordine dichiarato, per ogni
figlio il suo id, bit visible/hidden e l'effective hash se visibile. Quando
l'intera closure è autorizzata resta l'`ast_hash` originale. Il valore
`acl_scope` resta sempre il fingerprint autenticato.

## 3. Comportamento

1. **Dato** lo stesso tenant e gli stessi repository/gruppi in ordini o con
   duplicati diversi, **quando** Go e Python calcolano `acl_scope`, **allora**
   producono lo stesso SHA-256; user, trace e input applicativo non lo cambiano.
2. **Dato** un `Get/Put`, **quando** metadata autenticati sono assenti oppure
   `key.acl_scope` è vuoto/diverso, **allora** il servizio risponde
   `PermissionDenied` prima di Redis; un body non può selezionare un altro
   scope.
3. **Dato** la stessa tupla logica sotto due scope autenticati, **quando** viene
   cacheata, **allora** le chiavi fisiche v2 sono distinte; tuple contenenti
   `:` non collidono e le entry storiche `scache:*` non vengono riusate.
4. **Data** una gerarchia in cui root e tutta la closure sono autorizzate,
   **quando** viene riassunta, **allora** il modello vede tutti e soli i nodi,
   ogni cache key contiene `acl_scope` e il comportamento bottom-up/hit
   preesistente resta invariato.
5. **Data** una root autorizzata con almeno un discendente non autorizzato,
   **quando** viene riassunta, **allora** nessun source/summary/id in chiaro del
   subtree negato raggiunge modello, cache o risultato; gli aggregate
   interessati sono rigenerati con source vuoto e chiave di vista ristretta
   distinta. Il risultato non espone conteggi dei nodi negati.
6. **Data** una root non autorizzata o un `SecurityContext` invalido/vuoto,
   **quando** parte la summarization, **allora** fallisce chiusa prima di
   cache/model calls e non restituisce l'esistenza della root.
7. **Dato** codice invariato ma un cambio ACL che modifica la closure visibile,
   **quando** lo stesso scope riassume di nuovo, **allora** cambia
   `effective_ast_hash` e un precedente summary aggregato non produce hit.
8. **Dato** un errore cache/modello o una gerarchia invalida, **quando** avviene
   su una vista autorizzata, **allora** resta un errore tipizzato fail-closed;
   eventi/metriche riportano solo livello, operazione ed esito bounded.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| metadata/security context assente o malformato | `PermissionDenied`; zero Redis/model/cache I/O |
| tenant, allowed_repos o acl_groups vuoti/blank/oversized | diniego fail-closed, nessun default tenant/scope |
| `acl_scope` caller diverso dal derivato | `PermissionDenied`; non miss e non override silenzioso |
| key/PutRequest/value nil | `InvalidArgument`; nessun panic o Redis I/O |
| root autorizzata, tutti i figli negati | summary ristretto della root, source aggregate vuoto, nessun dato figlio |
| label security mancante su un nodo | nodo/subtree negato; root unlabeled produce diniego indistinguibile |
| ACL cambia senza cambiare AST | view hash diverso; cache full non riusata |
| Redis irraggiungibile dopo autorizzazione | `Unavailable`, non miss |
| cache/model failure Python | errore tipizzato; nessun fallback a summary più ampio |

## 5. Threat model e non-goals

### Threat model

Attori: client autenticato malevolo, servizio interno compromesso che forgia
`CacheKey`, prompt/output LLM ostile, record legacy senza label, ACL revocata
dopo una precedente generazione. Asset: sorgente e summary di tenant/repo/ACL
non autorizzati, membership/identità dei nodi e integrità delle partizioni
cache. Confini: solo JWT validato crea il `SecurityContext`; il backend deriva
lo scope e il summarizer riceve quel messaggio dal percorso autenticato.
Mitigazioni: fingerprint canonico, confronto esatto, chiave fisica hashata,
closure authorization prima del modello, view hash sensibile alla visibilità,
nessun default/fuzzy/LLM authorization e dati unlabeled negati.

### Non-goals

- Nessuna citation verification/WORM (T6.5), Envoy (T6.6) o matrice security
  completa (T6.7).
- Nessun nuovo RPC di summarization, scheduler batch o write diretta a
  PostgreSQL/Qdrant/Neo4j.
- Nessuna wildcard ACL, LLM judge, embedding/fuzzy match o filtro derivato dal
  prompt.
- Nessuna migrazione delle entry Redis v1: la cache è ricostruibile e v2 le
  invalida per namespace.
- Nessuna modifica all'ADD o alle metriche/baseline T5.6.

## 6. Vincoli dall'ADD

- Modulo 3 §2.1: `SecurityContext` soltanto da JWT validato; input utente/LLM
  non può costruire autorizzazione.
- Modulo 3 §2.6.3: summary aggregate visibile solo se autorizzato su **tutti**
  i figli; altrimenti generazione/cache separata del sottoinsieme autorizzato.
- Modulo 3 §2.6.3: chiave
  `ast_hash + logic_fingerprint + acl_scope`, mai riuso cross-ACL.
- Modulo 1 §1.6.3: cache content-addressed e invalidazione quando input/logica
  cambiano; Redis resta cache, non source of truth.
- SPEC-055/056: metadata JWT validati e PEP precedono handler; assenza/diniego
  è fail-closed.
- ADR-0012: derivazione canonica server-side, confronto e namespace v2.

## 7. Test plan

- Unit Go `accessscope`: vettore canonico condiviso, ordine/duplicati, user
  escluso, scope diversi e input invalido.
- Unit Go semantic-cache: metadata mancanti/mismatch/nil non invocano Redis;
  chiave v2 collision-safe e scope derivato non sostituibile.
- Integration Go gRPC + Redis testcontainer: interceptor `secctx`, Put/Get
  same-scope, cross-scope deny/miss, TTL preesistente e backend unavailable.
- Unit Python: full closure, restricted closure, denied root, source secret non
  osservabile dal model, ACL-only invalidation, key scope, stable child order,
  error paths e vettore cross-language.
- Regression: scenari SPEC-022/051 aggiornati al contesto autenticato senza
  skip o fallback; baseline T5.6 e `queries_v0.json` byte-identiche. CPU-only,
  Redis container solo in CI; nessuna GPU.

Red phase richiesta: i test devono inizialmente fallire perché
`accessscope.Fingerprint`, enforcement handler, `acl_scope` Python e label dei
nodi non esistono.

## 8. Osservabilità

- Span `eci.semantic_cache.scope` con attributi bounded
  `eci.operation ∈ {get,put}`, `eci.outcome ∈ {allow,deny,error}`.
- Counter `eci_semantic_cache_scope_decisions_total{operation,outcome}`;
  nessun tenant/user/repo/gruppo/entity nella label.
- Evento span `summarization.visibility` e counter
  `eci_summarization_visibility_total{level,outcome}`, con
  `level ∈ {method,class,module,repo}` e
  `outcome ∈ {full,restricted,denied}`.
- Log/errori non includono fingerprint, valori dello scope, source, summary o
  id dei nodi negati.

## 9. Criteri di accettazione

- [x] ADR-0012 e commenti proto descrivono la nuova semantica senza cambiare
      tag o forma wire.
- [x] Fingerprint Go/Python byte-identico, versionato e collision-safe.
- [x] Semantic Cache deriva/verifica lo scope e rifiuta mismatch prima Redis.
- [x] Redis key v2 non collide per separatori e non legge entry legacy.
- [x] Summary full/restricted rispettano la closure di tutti i figli.
- [x] ACL-only change invalida deterministicamente l'aggregate ristretto.
- [x] Source/id/summary negati non raggiungono modello, cache, output o log.
- [x] Telemetria bounded senza identità o contenuto.
- [ ] Test SPEC-022/051 e nuove regressioni verdi CPU-only/container CI.
- [x] T5.6 baseline e `queries_v0.json` byte-identici.
- [ ] `task build`, `task lint`, `task test`, `task guard` verdi.
- [ ] Stato `implemented`, poi `verified` soltanto con CI/PR reale.

## 10. Review avversariale di approvazione

- **Contraddizione ADD:** nessuna; restricted summaries implementano
  letteralmente la regola all-children e non reinterpretano il summary full.
- **Tenant isolation:** tenant parte dal digest; root/record unlabeled sono
  negati; nessun default o wildcard; il risultato non espone conteggi o id dei
  subtree negati.
- **Input LLM/body:** esclusi dal fingerprint e dalle decisioni; il modello
  riceve solo il projection set già autorizzato.
- **Leak via aggregate/cache:** una closure incompleta usa source vuoto e view
  hash diverso; il backend non accetta scope scelti dal client.
- **ACL TOCTOU:** un cambio della visibilità cambia il view hash anche con AST
  invariato; T6.5 aggiungerà il re-check citation, non viene anticipato qui.
- **Materialized views/idempotenza:** nessuna write canonica; Redis è
  ricostruibile e le put restano idempotenti per tupla v2.
- **Fail closed:** context, label, key, cache o modello invalidi non degradano
  verso scope più ampi.
- **Contratto/decisione:** nessun tag proto cambia; la rottura semantica Phase
  2→6 è esplicita in ADR-0012, con invalidazione e rollback documentati.
- **Traversal/deadline:** visita bounded alla gerarchia fornita, cycle-check
  preesistente e stesso context/deadline Redis; nessuna graph traversal nuova.
- **Deterministico/probabilistico:** autorizzazione, projection e cache key sono
  deterministiche; il modello genera testo solo dopo il gate.

Esito: approvata dopo seconda passata avversariale; nessun invariante ADD,
security o contratto è stato indebolito.

## 11. Evidenza di implementazione e deviazioni

- Red phase commit `df8958b`: Go non compilava per
  `accessscope.Fingerprint` assente, il server panicava prima del diniego e
  Python non esponeva `SummaryAuthorizationError`/fingerprint.
- Green core commit `310ea92`: vettore cross-language
  `c94ba9d3ff0d97a5fd8414abbcbad8c01bdc54c35436a9a69496e0b0d184eafa`,
  key Redis `scache:v2:<sha256>`, enforcement gRPC e projection RAPTOR
  full/restricted.
- La review d'implementazione ha eliminato due canali laterali non esplicitati
  nel draft: gli errori di gerarchia non includono id dei figli e il nodo
  ristretto passa al modello `effective_ast_hash`, source vuoto e soli child id
  autorizzati, non l'hash/source/roster completo.
- `task build`, `task lint`, `task guard`, `task proto:lint`,
  `task proto:breaking` e rigenerazione proto deterministica sono verdi
  localmente. I 14 test Summarization, i test Semantic Cache e il package
  `accessscope` sono verdi.
- `task test` locale esegue verdi tutti i test T6.4 e fallisce soltanto nelle
  suite preesistenti Keycloak, OPA e Orchestrator/Neo4j perché Docker Desktop
  non è in esecuzione. Nessun test è stato skipped o indebolito; la CI aggiunge
  esplicitamente il test gRPC + Redis reale prima di `verified`.
- Hash storici confermati invariati:
  `results.jsonl.summary.json` =
  `3f3d7053480a7cb6f5db2ffa9f995129aa1bfef95b64155ba3c1d1a7145cf3ac`,
  `queries_v0.json` =
  `67b6bca856e7bfce733be2cab38cb10e210ce831e41339393decb3f793c0f06b`.

Nessuna deviazione dall'ADD. Il cambio comportamentale del proto esistente è
quello approvato da ADR-0012; forma wire e tag restano invariati.
