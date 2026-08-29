# SPEC-054 — Deterministic output canonicalization & eval hardening
Stato: implemented
Task-tree: T5.7 · Servizio: services/orchestrator · ADD: Modulo 1 §1.4, Modulo 2 §3.1-3.4, Modulo 4 §3.3
Contratti: nessun nuovo contratto condiviso; formato interno dell'harness golden; `contracts/` invariati

## 1. Obiettivo
Rendere l'evaluation capace di distinguere un errore semantico da un simbolo
corretto ma non qualificato, da una risposta verbosa e da una deviazione di
rappresentazione/canonicalizzazione. La soluzione è deterministica, usa soltanto
repository/CPG/symbol table come fonti semantiche e mantiene separati exact
verification e recupero deterministico dell'entità.

La SPEC nasce dalla failure analysis di `baseline-v1` documentata in SPEC-053.
Non ricalcola né sostituisce `pass_rate=0.50` o `fact_recall=0.40`: ogni metrica
T5.7 sarà un nuovo campo e un nuovo risultato di evaluation.

## 2. Interfaccia

### 2.1 Strong structured-output contract

Il modello deve restituire soltanto JSON valido con un envelope fisso,
indipendente da `expected_facts` della singola query:

```json
{
  "facts": [
    {"kind": "contains", "value": "OrderService.Process"}
  ],
  "citations": ["tests/fixtures/sample-repo/order_service.go"]
}
```

```python
class FactKind(StrEnum):
    callers = "callers"
    methods = "methods"
    implementations = "implementations"
    contains = "contains"
    node_type = "node_type"

class StructuredFact(BaseModel):
    kind: FactKind
    value: str

class StructuredReply(BaseModel):
    facts: tuple[StructuredFact, ...]
    citations: tuple[str, ...] = ()
```

`value` accetta soltanto la grammatica canonica del `kind`: identificatore
fully-qualified, valore enum canonico, sentinel di insieme vuoto o relazione tra
identificatori canonici. Esempi validi:

- `OrderService.Process`
- `OrderService.Validate`
- `EmailNotifier.Notify`
- `main`
- `OrderService.Validate <- OrderService.Process`

Esempi non validi:

- `Process`
- `Notify è un metodo...`
- `main istanzia OrderService...`
- `tests/fixtures/sample-repo/order_service.go` in un fact slot

Le citations hanno un campo separato e non possono soddisfare un fact.

### 2.2 Canonicalizzazione deterministica

```python
class SymbolResolver(Protocol):
    def resolve(self, identifier: str, *, scope: ResolutionScope) -> Resolution: ...

class DeterministicCanonicalizer:
    def canonicalize(
        self,
        fact: StructuredFact,
        *,
        symbols: SymbolResolver,
        scope: ResolutionScope,
    ) -> CanonicalizationResult: ...

class EvaluationResult(BaseModel):
    exact_canonical_fact_recall: float
    semantic_entity_recall: float | None
    issues: tuple[EvaluationIssue, ...]
```

`Resolution` è uno tra `exact`, `unique`, `ambiguous`, `not_found`. La mappatura
di un simbolo corto, per esempio `Process → OrderService.Process`, è ammessa
soltanto per `unique`. In caso di più candidati non si sceglie: il risultato è
`ambiguous_canonicalization`.

Il resolver usa esclusivamente simboli e relazioni strutturate ottenuti dal
repository, dal CPG o dalla symbol table. Non usa similarity, embedding, edit
distance, frequenza, ordine del golden o valore atteso.

Il parser di output legacy può riconoscere come `verbose_fact` soltanto una
forma trasparente e deterministica: un identificatore lessicale esatto in testa
al valore seguito da testo non ammesso. L'eventuale identificatore iniziale è
risolto con la stessa symbol table e le stesse regole `exact|unique|ambiguous`;
il testo restante non partecipa alla risoluzione. Non sono ammessi fuzzy matching
o estrazione semantica da prosa.

### 2.3 Separazione dall'oracolo golden

Il data flow obbligatorio è:

```text
model actual ──> canonicalizer(repository/CPG/symbol table) ──> canonical actual
                                                                    │
golden expected ────────────────────────────────────────────────────> comparator
```

`expected_facts` entra solo nel comparator, dopo che l'actual è stato
canonicalizzato. Non entra nel prompt, nel resolver, nello scope di risoluzione,
nel ranking dei candidati o nella classificazione della forma dell'actual. Anche
le chiavi/categorie dell'envelope del modello sono un enum fisso e non sono
derivate dal record golden corrente.

Qualunque modifica al prompt incrementa la sua versione e il
`logic_fingerprint`, come richiesto dall'ADD. Versione e fingerprint sono
registrati nell'output dell'eval.

## 3. Comportamento

1. **Given** un output fully-qualified conforme, **when** viene valutato, **then**
   conserva l'identificatore e contribuisce a `exact_canonical_fact_recall`.
2. **Given** `Process` e una symbol table con il solo
   `OrderService.Process`, **when** viene canonicalizzato, **then** risolve in
   modo univoco e produce issue `unqualified_symbol`; non diventa un exact raw
   match.
3. **Given** `Process` e più simboli omonimi, **when** viene canonicalizzato,
   **then** non sceglie alcun candidato e produce
   `ambiguous_canonicalization`.
4. **Given** `main istanzia OrderService...`, **when** il parser legacy trova
   l'identificatore canonico iniziale seguito da prosa, **then** produce
   `verbose_fact`; l'eventuale recupero di `main` è tracciato separatamente.
5. **Given** una citation path in un fact slot `callers`, **when** la grammatica
   della relazione fallisce e nessun simbolo è risolvibile, **then** produce
   `semantic_error`, senza promuovere la citation a fact.
6. **Given** actual canonici in eccesso o expected canonici non coperti, **when**
   il comparator termina, **then** produce rispettivamente `unexpected_fact` e
   `missing_fact` in ordine stabile.
7. **Given** un actual canonicalizzato senza consultare il golden, **when** viene
   calcolato `semantic_entity_recall`, **then** contano soltanto risoluzioni
   `exact` o `unique` confrontate successivamente con l'expected.
8. **Given** lo stesso output, repository snapshot e symbol table, **when** la
   pipeline viene eseguita due volte, **then** metriche, issue e ordine sono
   identici.

## 4. Failure taxonomy ed edge case

Ogni mismatch espone almeno una delle classi seguenti; una priorità stabile
determina la classe primaria e le condizioni secondarie restano campi espliciti,
non inferenze testuali.

| Classe | Condizione deterministica |
|---|---|
| `semantic_error` | fact slot contiene un valore di tipo semantico errato o una relazione/entity non risolvibile come richiesta dal `kind` |
| `unqualified_symbol` | token identificatore valido, non canonico, risolto a un solo simbolo |
| `verbose_fact` | identificatore lessicale in testa seguito da testo non ammesso dal contract |
| `unexpected_fact` | actual canonico senza corrispondente expected dopo il confronto exact |
| `missing_fact` | expected canonico non coperto dopo il confronto |
| `ambiguous_canonicalization` | token non canonico con più candidati strutturali possibili |

| Condizione | Comportamento |
|---|---|
| symbol table assente o backend fallisce | errore tipizzato/fail closed; nessun semantic credit |
| identificatore non trovato | `semantic_error` o `unexpected_fact` secondo il `kind`, mai best guess |
| più candidati | `ambiguous_canonicalization`, canonical value assente |
| citation corretta ma fact errato | citation e fact valutati indipendentemente |
| output non JSON/schema invalido | record error esistente; nessuna canonicalizzazione da testo libero |
| insieme vuoto | sentinel canonico stabile, non omissione silenziosa |
| categoria non supportata | validazione fallisce prima del confronto |

Precedenza primaria per un singolo actual: schema/semantic type error →
`verbose_fact` → `ambiguous_canonicalization` → `unqualified_symbol` →
`unexpected_fact`. `missing_fact` è emesso separatamente per ogni expected non
coperto. Questa precedenza non altera il set di issue secondari né le metriche.

## 5. Metriche

### `exact_canonical_fact_recall`

```text
raw actual facts già canonici che corrispondono exact agli expected canonici
────────────────────────────────────────────────────────────────────────────
                         expected facts canonici
```

La metrica non accredita output verboso o simboli non fully-qualified, anche se
risolvibili. Sul corpus storico il vecchio `fact_recall` resta archiviato e non
viene rinominato o sovrascritto.

### `semantic_entity_recall`

```text
actual facts risolti exact|unique che corrispondono agli expected dopo la risoluzione
──────────────────────────────────────────────────────────────────────────────────
                              expected facts canonici
```

La metrica è calcolata solo quando tutti i `kind` nello scope hanno una
grammatica e una risoluzione strutturale completa. Se una categoria o relazione
non può essere risolta deterministicamente, il summary riporta
`semantic_entity_recall: null` per quello scope e una limitazione strutturata;
non applica euristiche opache né esclude silenziosamente casi dal denominatore.

Le due metriche sono sempre riportate insieme alla coverage del resolver e ai
conteggi della failure taxonomy. Nessuna delle due sostituisce retroattivamente
`baseline-v1`.

## 6. Regression corpus T5.6

Fixture sorgente immutabile:
`artifacts/t5.6/20260828T211053Z/results.jsonl`. L'implementazione T5.7 aggiunge
un file separato di aspettative di classificazione sotto `tests/golden/`; non
modifica l'artefatto né `queries_v0.json`.

| Query | Regressione obbligatoria |
|---|---|
| `g01` | classe primaria `semantic_error`; la citation path non è un caller fact |
| `g04` | i due simboli corti sono `unqualified_symbol` e risolvono univocamente |
| `g06` | `OrderService` resta exact; `Process` e `Validate` sono `unqualified_symbol` |
| `g07` | i tre valori sono `verbose_fact`; ogni eventuale entity recovery usa solo il token iniziale e il resolver strutturale |
| `g09` | `verbose_fact`; `main` è recuperabile solo perché identificatore canonico iniziale |

Il test deve inoltre provare che nessun valore di `expected_facts` viene passato
al canonicalizer, per esempio con un resolver spy e con expected alternativi che
non cambiano il risultato della canonicalizzazione dell'actual.

## 7. Non-goals

- Modificare le attese di `tests/golden/queries_v0.json`.
- Ricalcolare o aumentare manualmente le metriche T5.6.
- Fuzzy matching, edit distance, stemming, embedding similarity o ranking
  probabilistico degli omonimi.
- LLM judge o seconda chiamata LLM per interpretare l'output.
- Usare expected facts, query ID o ordine del golden come feature.
- Unificare fact correctness e citation correctness.
- Cambiare ADD, contratti condivisi, modello o provisioning GPU.
- Eseguire una nuova eval GPU durante l'implementazione T5.7.

## 8. Vincoli dall'ADD

- Modulo 1 §1.4: gli identificatori canonici e la name resolution provengono da
  stack-graphs/SCIP/symbol table, non da similarità semantica.
- Modulo 2 §3.1-3.4: il gate finale usa oracoli deterministici; LLM-as-judge e
  altri segnali probabilistici non forniscono garanzie.
- Modulo 4 §3.3: i tassi deterministici specifici del dominio devono restare
  distinguibili dalle metriche probabilistiche di evaluation.
- Modulo 1 §1.6.3: ogni modifica al prompt cambia `logic_fingerprint`.

## 9. Test plan

Unit test 1:1 per gli otto scenari con symbol table in-memory costruita dai
simboli del fixture, più test di backend failure, output invalido, insieme vuoto,
ordine stabile e separazione citation/fact. Regression CPU-only sui record reali
T5.6 per `g01`, `g04`, `g06`, `g07`, `g09` e invariant test che dimostri
l'indipendenza del canonicalizer dagli expected.

Nessun test richiede Docker, GPU, vLLM, Runpod o un servizio esterno.

## 10. Osservabilità

Il summary aggiunge, senza rimuovere i campi storici:

- `exact_canonical_fact_recall`;
- `semantic_entity_recall` oppure `null` con `semantic_metric_limitations`;
- `canonicalization_coverage`;
- `failure_taxonomy_counts` per le sei classi;
- `prompt_contract_version` e `logic_fingerprint`.

I record per query espongono raw fact, canonical fact opzionale, resolution
status e issue; non includono symbol table completa, prompt, secret o source non
già previsto dall'harness.

## 11. Criteri di accettazione

- [x] Strong structured-output contract indipendente dal golden.
- [x] Canonicalizzazione solo tramite repository/CPG/symbol table e solo se
  univoca.
- [x] Metriche exact e semantic separate, con limitation esplicita.
- [x] Sei classi di failure prodotte deterministicamente.
- [x] Regression corpus T5.6 verde con le cinque classificazioni richieste.
- [x] `baseline-v1` e `tests/golden/queries_v0.json` byte-identici.
- [x] Nessun LLM judge, fuzzy matching, embedding similarity o GPU.
- [x] Prompt versionato e `logic_fingerprint` aggiornato.
- [ ] `task build`, `task lint`, `task test`, `task guard` verdi.

## 12. Implementazione e deviazioni

Implementazione in `services/orchestrator/orchestrator/golden_canonicalization.py`
e integrazione nell'harness `golden_eval.py`. Il resolver pubblico resta un
`Protocol`: il run sul fixture costruisce la symbol table esclusivamente dalle
dichiarazioni Go presenti nel repository (`type`, funzioni e metodi con
receiver), mentre un backend CPG può implementare la stessa interfaccia senza
toccare comparator o prompt. Il loader Go è intenzionalmente limitato al corpus
golden corrente e non pretende di sostituire il name resolver di ingestion.

Il prompt `golden-structured-facts-v2` non riceve `expected_facts`, categorie
derivate dall'expected o `scope_note`; richiede un envelope fisso e separa
facts/citations. Il fingerprint della logica è
`576a17cd3e98c43bd230bb8da3add6f2d3fd22405498cb293b355e6db1c52689`.

Il replay CPU-only degli output storici produce, come **nuove metriche T5.7**,
`exact_canonical_fact_recall=0.40`, `semantic_entity_recall=14/15` e coverage
resolver `14/15`. Questi valori sono evidenza di regressione deterministica,
non sostituiscono né modificano la baseline ufficiale T5.6 (`pass_rate=0.50`,
`fact_recall=0.40`). Taxonomy del replay: un `semantic_error`, quattro
`unqualified_symbol`, quattro `verbose_fact` e un `missing_fact` conseguente al
failure g01; nessun match fuzzy o probabilistico.

Nessuna deviazione da ADD o contratti condivisi. Il formato resta interno
all'harness; non è stato modificato alcun file sotto `contracts/`.
