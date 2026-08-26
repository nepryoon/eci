# SPEC-051 — Summarization RAPTOR bottom-up con Semantic Cache (T5.5)
Stato: verified
Task-tree: T5.5 · Servizio: services/summarization · ADD: Modulo 2 §2.2, Modulo 3 §1.2
Contratti: nessun nuovo contratto; `contracts/` invariati

## 1. Obiettivo
Implementare il core stateless che genera summary lungo la gerarchia CPG method→class→module→repo. Ogni nodo riusa la Semantic Cache con chiave `ast_hash + logic_fingerprint`, evitando chiamate LLM per sottoalberi invariati.

## 2. Interfaccia
```python
class SummaryCache(Protocol):
    def get(self, key: SummaryCacheKey) -> str | None: ...
    def put(self, key: SummaryCacheKey, summary: str) -> None: ...
class SummaryModel(Protocol):
    def summarize(self, node: SummaryNode, child_summaries: tuple[ChildSummary, ...]) -> str: ...
class RaptorSummarizer:
    def summarize(self, nodes: Sequence[SummaryNode], root_id: str) -> SummaryResult: ...
```

## 3. Comportamento
1. Una gerarchia completa è visitata post-order, dal method fino al repo.
2. Un cache hit restituisce il summary senza invocare il modello né riscrivere la cache.
3. Un miss invoca il modello una volta, valida output non vuoto e scrive la cache.
4. Due esecuzioni identiche producono lo stesso risultato; la seconda fa zero chiamate modello.
5. Cambiando un hash foglia e gli hash dei suoi antenati, solo quel cammino viene rigenerato.
6. L'ordine dei figli passato al modello è quello dichiarato dal parent ed è stabile.
7. Cicli, figli mancanti, root mancante e gerarchie di livello invalide falliscono prima di chiamate modello/cache write.
8. Un errore cache/modello fallisce chiuso e non viene presentato come cache hit.

## 4. Errori & edge case
| Condizione | Comportamento |
|---|---|
| hash/fingerprint non SHA-256 | validazione Pydantic |
| summary vuoto | `SummaryModelError` e nessun put |
| nodo duplicato | `InvalidHierarchyError` |
| cache backend failure | `SummaryCacheError` |

## 5. Non-goals
Clustering GMM/UMAP, persistenza concreta, API gRPC, scheduling batch, ACL scope (T6), scrittura diretta su PostgreSQL/Qdrant.

## 6. Vincoli dall'ADD
Gerarchia imposta dal CPG lungo CONTAINS; cache `ast_hash + logic_fingerprint`; propagazione bottom-up; servizio Python stateless; nessuna chiamata LLM ridondante.

## 7. Test plan
Unit test 1:1 per gli otto scenari con cache e modello in-memory deterministici; test invalidazione selettiva, errori backend e output vuoto.

## 8. Osservabilità
Span `summarization.raptor` con hit/miss e nodi elaborati; evento `summarization.cache_hit|cache_miss` contenente solo livello, mai sorgente o summary.

## 9. Criteri di accettazione
- [x] Scenari unitari verdi.
- [x] `task build`, `task lint`, `task test`, `task guard` verdi.
- [x] ADD e contratti invariati.

## 10. Deviazioni
1. Cache, modello e persistenza sono porte tipizzate: non esistono contratti di rete canonici per Summarization/Semantic Cache e questa SPEC non ne inventa.
2. Il core implementa la gerarchia strutturale prescritta, non il clustering RAPTOR statistico, esplicitamente sostituito dall'ADD con gli archi CONTAINS.
3. `acl_scope` sarà aggiunto dalla fase Security; anticiparlo ora senza SecurityContext autenticato rischierebbe filtri non autorevoli.
