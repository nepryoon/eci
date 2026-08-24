# SPEC-047 — Orchestrator agentico: LangGraph + tool tipizzati (T5.1)
Stato: implemented
Task-tree: T5.1 (dip. T4.2, già chiuso) · Servizio: services/orchestrator (Python, estende T1.5/SPEC-018) · ADD: Modulo 2 §2.2

## 1. Obiettivo
Sostituire il core della CLI `eci ask` esistente (T1.5, un solo passaggio deterministico) con un grafo di stato LangGraph che naviga il CPG tramite sei tool tipizzati PydanticAI, seguendo il pattern ibrido plan-then-react prescritto dall'ADD (§2.2): Plan-and-Solve per query multi-hop a struttura nota, ReAct puro per query esplorative. Loop bounded a quattro componenti (budget passi, budget token, criteri di stop, gestione vicoli ciechi), stato di visita mantenuto lato orchestratore non nel prompt.

## 2. Interfaccia

**Verifica preliminare obbligatoria (prima di scrivere qualunque codice)**: PydanticAI ha avuto un bump di versione maggiore (v1→v2) dopo il mio addestramento — verificare la sintassi esatta di decoratori/dependency injection/costruttore Agent contro la versione REALMENTE installata (`pip show pydantic-ai`), non presumerla da un ricordo di addestramento potenzialmente stantio.

**Sei tool PydanticAI-tipizzati** (`services/orchestrator/tools.py`, nuovo), ciascuno un wrapper sottile su un client gRPC già esistente — nessuna nuova logica di backend:
```python
async def get_node(ctx: RunContext[Deps], node_id: str) -> NodeResult          # GetNode, T1.4
async def get_callers(ctx: RunContext[Deps], node_id: str, depth: int) -> list[NodeResult]     # ExpandNeighbors/ImpactAnalysis REVERSE su CALLS, T1.4/T4.2
async def get_callees(ctx: RunContext[Deps], node_id: str, depth: int) -> list[NodeResult]      # ExpandNeighbors FORWARD su CALLS, T1.4
async def expand_dependencies(ctx: RunContext[Deps], node_id: str, edge_types: list[str], depth: int) -> list[NodeResult]  # ImpactAnalysis, T4.2
async def semantic_search(ctx: RunContext[Deps], query: str, filters: dict) -> list[NodeResult]  # gamba vettoriale di HybridSearch, T4.1
async def read_source(ctx: RunContext[Deps], node_id: str) -> str              # hydrateSourceText già in HybridSearch, SPEC-045
```
**`summarize_subgraph(node_ids)` NON implementato in questa SPEC** (dip. T5.5/RAPTOR, non ancora costruito) — dichiarato esplicitamente nella firma del tool: chiamarlo ritorna un errore tipizzato `SummarizationNotYetAvailable`, mai un riassunto fabbricato.

**Grafo di stato LangGraph** (`services/orchestrator/graph.py`, nuovo):
```python
class AgentState(TypedDict):
    query: str
    pattern: Literal["react", "plan_and_solve"]
    plan: list[str] | None       # solo per plan_and_solve
    visited: set[str]            # nodi già esplorati — vive QUI, mai nel prompt (ADD §2.2)
    candidates: list[NodeResult] # nodi scoperti finora, con impact_score se disponibile
    step_count: int
    token_count: int
    stop_reason: str | None
```
Nodi: `classify_pattern` (euristica semplice temporanea — parole chiave tipo "chi chiama"/"impatto"/"dipendenze" → `plan_and_solve`, altrimenti `react`; **placeholder dichiarato**, sostituito dal vero intent classifier in T5.2), `make_plan` (solo ramo plan_and_solve — genera una sequenza di fasi come da esempio ADD: trova X → implementazioni → chiamanti → prioritizza → verifica), `react_step` (un'iterazione: ragiona via LLM, sceglie un tool, osserva il risultato, aggiorna `visited`/`candidates`), `check_stop` (valuta i quattro criteri di stop, §3).

**LLM per il ragionamento**: chiama `vllm-fake` (T0.9) direttamente via il suo endpoint OpenAI-compatibile — NON attraverso T5.3 (LLM Gateway, non ancora costruito). Nessun routing fake/reale, nessun circuit breaker, nessuna deadline propagation qui — tutti espliciti Non-goal, oggetto di T5.3.

## 3. Comportamento (scenari)

1. **Dato** un `node_id` con chiamanti diretti noti, **quando** chiamo il tool `get_callers`, **allora** ritorna esattamente i nodi raggiungibili REVERSE su CALLS a `depth=1` — stesso risultato che `ImpactAnalysis` produrrebbe con gli stessi parametri, non una logica diversa.
2. **Dato** un nodo già presente in `visited`, **quando** un tool lo riscopre durante l'esplorazione, **allora** non viene ri-processato (nessuna nuova chiamata tool su di esso), il grafo prosegue su un ramo diverso.
3. **Dato** il budget di passi esaurito (default 15-30, configurabile), **quando** il grafo continua a iterare, **allora** si ferma con `stop_reason="step_budget_exhausted"`, non un errore.
4. **Dato** nessun nuovo nodo con `impact_score` sopra soglia scoperto in un'iterazione, **quando** valuto i criteri di stop, **allora** il grafo si ferma con `stop_reason="blast_radius_stabilized"` (stesso principio "nessun nuovo segnale" già usato altrove nel progetto per condizioni di terminazione).
5. **Dato** una query contenente "chi chiama" o "impatto", **quando** eseguo `classify_pattern`, **allora** il pattern scelto è `plan_and_solve`, un piano viene generato PRIMA di qualunque chiamata tool.
6. **Dato** una query esplorativa generica ("capisci come funziona X"), **quando** eseguo `classify_pattern`, **allora** il pattern scelto è `react`, nessuna fase di pianificazione upfront.
7. **Dato** `summarize_subgraph` invocato, **quando** eseguo il grafo, **allora** ritorna esplicitamente `SummarizationNotYetAvailable`, mai un riassunto fabbricato o un errore generico non tipizzato.
8. **Dato** il grafo in esecuzione, **quando** ispeziono il testo effettivamente inviato all'LLM (via il fake), **allora** l'elenco completo dei `node_id` visitati NON appare come testo nel prompt — vive solo in `AgentState.visited` (verifica diretta del vincolo architetturale dell'ADD §2.2, non solo dichiarata).

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Un tool ritorna vuoto (nessun risultato) | Trattato come vicolo cieco (§2.2 dell'ADD) — backtrack alla frontiera precedente, non un errore che interrompe il grafo |
| `vllm-fake` irraggiungibile | Errore esplicito, il grafo non prosegue con un ragionamento fabbricato |
| Query vuota | Errore esplicito prima di avviare qualunque nodo del grafo |

## 5. Non-goals
`summarize_subgraph` reale (dip. T5.5). Vero intent classifier (T5.2 — qui solo un'euristica placeholder dichiarata). LLM Gateway/routing fake-reale/circuit breaker/deadline (T5.3). Verification layer (T5.4). Nessuna modifica ai servizi Go di Fase 4 — tutti i tool sono wrapper Python attorno a RPC già esistenti, invariate.

## 6. Vincoli dall'ADD
Modulo 2 §2.2 — sette tool tipizzati (sei implementati qui), pattern ReAct/Plan-and-Solve/ibrido plan-then-react, le quattro componenti di controllo del loop, stato di visita lato orchestratore.

## 7. Test plan
Unitari per `classify_pattern`/i criteri di stop (nessuna infrastruttura reale necessaria). Integrazione con `retrieval-engine` reale (i servizi Go già testati in Fase 4, invocati via gRPC reale) + `vllm-fake` reale per gli scenari end-to-end (5-8).

## 8. Osservabilità
Uno span per nodo del grafo attraversato, un evento per ciascuna decisione di stop con la motivazione.

## 9. Criteri di accettazione
- [ ] Scenari 1-8 verificati con evidenza diretta, in particolare lo scenario 8 (stato di visita mai nel prompt, verificato ispezionando il testo reale inviato all'LLM).
- [ ] Edge case tabella §4 verificati esplicitamente.
- [ ] Sintassi PydanticAI verificata contro la versione installata, non presunta.
- [ ] Nessuna regressione sui test esistenti di T1.5 (CLI `eci ask` deve continuare a funzionare, anche se il suo core interno cambia).

## 10. Deviazioni rispetto alla SPEC

1. **Il fake non implementa tool calling strutturato.** L'API reale di
   `fakes/vllm-fake` valida solo `model` e `messages`, poi restituisce l'eco
   dell'ultimo messaggio utente; non interpreta `tools`, `tool_choice` né
   produce `tool_calls`. I sette contratti sono quindi registrati e validati
   realmente da un `pydantic_ai.Agent`, mentre LangGraph mantiene il routing
   deterministico. Non viene dichiarata una selezione LLM dei tool che il
   backend corrente non può effettuare; quella integrazione resta dipendente
   da T5.3.

2. **`semantic_search` usa `HybridSearch`, non una gamba vector-only.** Il
   contratto retrieval espone solo `HybridSearch`; non esiste una RPC
   vettoriale separata. Il nome logico del tool dell'ADD è mantenuto senza
   inventare un'API backend.

3. **`read_source` fallisce esplicitamente con `SourceNotAvailable`.** Sebbene
   `GetNodeRequest` contenga `include_source_text`, l'implementazione corrente
   di `GetNode` ignora il flag e proietta soltanto Neo4j. L'hydration di
   SPEC-045 esiste nel percorso `HybridSearch(include_source_text=true)`, non
   nel lookup puntuale; il tool non finge quindi di aver recuperato sorgente.

4. **Budget token come stima dichiarata.** Prima di T5.3 il fake riporta un
   conteggio per parole e non espone il tokenizer del futuro modello reale.
   L'orchestrator usa una stima deterministica `ceil(UTF-8 bytes / 4)`, chiamata
   esplicitamente stima e testata; non presenta un word count come token reali.

5. **Dipendenza slim deliberata.** È installato
   `pydantic-ai-slim[openai]` anziché il metapacchetto `pydantic-ai`, che
   installerebbe provider cloud e integrazioni non usati. Entrambi espongono
   lo stesso package Python `pydantic_ai`; la sintassi `Tool(function)`,
   `RunContext[Deps]` e `Agent(..., tools=...)` è stata verificata con la
   versione risolta 1.107.5. LangGraph risolto: 1.0.10.

6. **Limite della verifica locale.** Gli unit test reali usano le dipendenze
   installate, senza stub. I test testcontainers/retrieval end-to-end non sono
   eseguibili nell'ambiente di verifica privo del daemon Docker; le checkbox di
   accettazione restano pertanto non marcate come verificate.

7. **Correzione post-review dello skeleton iniziale.** Il primo commit
   terminava il grafo subito dopo classificazione/piano. È stato sostituito da
   un loop LangGraph eseguibile `classify_pattern → make_plan (condizionale) →
   react_step → check_stop → react_step|END`: il piano viene consumato tramite
   `plan_index`, i tool sono chiamati dal `RetrievalToolRuntime`, la
   deduplicazione avviene prima della RPC, e una frontiera LIFO conserva rami
   alternativi per il backtrack. Test unitari dedicati verificano consumo del
   piano, selezione ReAct, dedup preventiva, backtrack, budget, stabilizzazione,
   payload LLM senza `visited`, propagazione dell'indisponibilità LLM e
   span/eventi OTel in-memory. La regressione CLI/infrastrutturale resta
   soggetta al limite Docker dichiarato al punto 6.

8. **Correzioni della review sulla PR #56 e conflitti con `main`.** Integrato
   `origin/main` preservando la SPEC e i contratti già confluiti con PR #55.
   I conflitti add/add sui tre moduli agentici sono stati risolti mantenendo il
   loop completo del branch e non lo skeleton di `main`. I quattro rilievi
   inline sono coperti così: `eci ask` passa ora da `run_agent`; ogni fase del
   piano conserva i seed originali invece di consumare una frontiera DFS
   condivisa; `run_agent` imposta `recursion_limit = 2 * max_steps + 10`; il
   piano non invoca più incondizionatamente `read_source`, che resta un tool
   esplicito con errore tipizzato finché `GetNode` non supporta hydration.

9. **Correzione della regressione CI SPEC-018 scenario 5.** Il primo wiring di
   `run_ask` invocava il reasoner subito dopo il seed search: con vLLM
   irraggiungibile l'eccezione conteneva soltanto il target, non anche i suoi
   chiamanti già richiesti dal contratto SPEC-018. Per i piani strutturati il
   reasoning è ora differito all'ultima fase deterministica di retrieval; un
   fallimento LLM conserva quindi tutte le fonti (`Validate` e `Process`) senza
   eseguire ulteriori tool dopo il fallimento. Un test unitario riproduce
   esattamente la regressione e verifica le fonti nell'errore tipizzato.
