# SPEC-019 — Test E2E: golden dataset contro la pipeline reale (T1.6, Milestone A)
Stato: implemented
Task-tree: T1.6 (ultimo task di Fase 1) · Nuovo: tests/e2e/ (Python) · ADD: Milestone A del piano originale ("pipeline viva end-to-end")
Contratti: tests/golden/queries_v0.json (SPEC-009, letto — non modificato)

## 1. Obiettivo
Un test end-to-end che ingerisce il fixture di SPEC-009 attraverso la pipeline reale per intero — `parse_file`/`persist_parsed_file` (T1.1/T1.2, Rust) → Postgres+outbox → Debezium/Kafka (CDC) → `sink-graph` (T1.3, Go) → Neo4j → `retrieval-engine` (T1.4, Go) → `orchestrator` (T1.5, Python) — e verifica ciascuna delle 10 query del golden dataset contro il risultato reale, non un grafo di test ridotto costruito ad hoc. È la dimostrazione diretta di Milestone A ("pipeline viva end-to-end") nominata fin dal piano originale, non solo l'ultimo task di Fase 1.

## 2. Interfaccia

Nuovo package `tests/e2e/` (Python, pytest — stesso linguaggio del test più sofisticato già esistente, SPEC-018 `conftest.py`, di cui questo test riusa ed estende i pattern anziché reinventarli): testcontainers per Postgres/Neo4j/Kafka+Debezium-Connect (quest'ultimo, stesso pattern multi-container e stessa registrazione del connector già verificati in SPEC-007/008), sottoprocessi reali per `ingestion` (Rust, un'invocazione per ciascuno dei 4 file del fixture), `sink-graph` e `retrieval-engine` (Go, compilati ed eseguiti come binari), e riuso diretto di `vllm-fake` via lo stesso bootstrap già costruito in SPEC-018 `_ensure_vllm_fake_venv`.

**Setup, una sola volta per l'intera sessione di test** (fixture pytest `session`-scoped): stack testcontainers su, connector Debezium registrato, i 4 file del fixture ingeriti in sequenza (`main.go`, `order_service.go`, `notifier.go`, `util.go` — stesso ordine indifferente, l'ordine reale non ha effetto sul risultato finale), `sink-graph`/`retrieval-engine`/`vllm-fake` avviati. **Attesa propagazione CDC**: polling di `GetNode` su un piccolo insieme di id noti (i 4 nodi File) fino a comparsa in Neo4j, con timeout esplicito — mai un semplice `sleep` fisso (stesso principio già applicato in tutti i test con readiness reale di questo progetto).

**Caricamento del golden dataset**: `tests/golden/queries_v0.json` letto direttamente da disco a runtime (non duplicato come dati inline nel test) — un cambiamento futuro al golden dataset si riflette automaticamente qui.

**Strategia di verifica, diversa per categoria di `expected_facts`** (vocabolario confermato leggendo il file reale):
- **`callers`** (g01, g02, g03): via `eci ask` — l'unica categoria per cui l'orchestrator ha un'euristica di comprensione della domanda ("chi chiama", SPEC-018 §2). Insieme atteso di nomi in Fonti = {entità cercata} ∪ {nome del chiamante per ciascuna stringa `"X <- Y"` in `callers`, prendendo solo la parte dopo l'ultimo punto}. Per g02/g03 (`callers: []`) l'insieme atteso si riduce alla sola entità cercata — verifica diretta che Fonti non contenga NESSUN chiamante aggiuntivo, non solo che non sia vuota.
- **`contains`** (g06, g07, g08, g09): chiamata diretta a `ExpandNeighbors(FORWARD, CONTAINS)` sul nodo File corrispondente (riuso del client gRPC già costruito in `orchestrator.retrieval_client`, non passando per `eci ask` — l'orchestrator non ha un'euristica per "cosa contiene X").
- **`node_type`** (g10): chiamata diretta a `GetNode`, confronto sul campo `node_type`.
- **`methods`** (g04): **deviazione dichiarata rispetto a una traversata diretta** — non esiste un arco Class→Method nello schema attuale (CONTAINS va solo da File verso le entità che dichiara, verificato contro il codice reale di T1.1). Verificato con lo stesso `ExpandNeighbors(FORWARD, CONTAINS)` del File, filtrato lato test per `node_type == "Method"`. Funziona correttamente su QUESTO fixture perché `order_service.go` dichiara una sola Class — un file con più classi produrrebbe risultati ambigui con questo filtro; non generalizzato oltre il caso presente.
- **`implementations`** (g05): **non verificato in questa SPEC, esplicitamente segnalato come tale nel report del test** (skip con motivazione, non un'assenza silenziosa) — nessun arco IMPLEMENTS esiste nella pipeline: T1.1 non implementa il rilevamento della soddisfazione implicita di interfaccia di Go (analisi semantica dei tipi, non parsing sintattico). Limite noto, da riaprire quando la risoluzione di tipo arriverà (probabilmente Fase 2).

Confronto sempre per **nome semplice** (`"Process"`, non `"OrderService.Process"`) — coerente con la forma che sia `eci ask`/Fonti sia `ExpandNeighborsResponse.neighbors[].name` espongono realmente.

## 3. Comportamento (scenari)

1. **Dato** il fixture ingerito per intero attraverso la pipeline reale, **quando** eseguo il test per g01, **allora** l'insieme dei nomi in Fonti è esattamente `{Validate, Process}`.
2. **Dato** lo stesso stato, **quando** eseguo il test per g02, **allora** l'insieme dei nomi in Fonti è esattamente `{Process}` — nessun chiamante, verifica diretta non solo "non fallisce".
3. **Dato** lo stesso stato, **quando** eseguo il test per g03, **allora** l'insieme dei nomi in Fonti è esattamente `{computeTotal}`.
4. **Dato** lo stesso stato, **quando** eseguo il test per g04, **allora** l'insieme filtrato per `node_type=Method` dal CONTAINS di `order_service.go` è esattamente `{Process, Validate}`.
5. **Dato** lo stesso stato, **quando** eseguo il test per g06/g07/g08/g09, **allora** l'insieme dei vicini CONTAINS di ciascun File corrisponde esattamente all'elenco `contains` atteso (confronto per nome semplice).
6. **Dato** lo stesso stato, **quando** eseguo il test per g10, **allora** `GetNode` su `OrderService` riporta `node_type=Class`.
7. **Dato** lo stesso stato, **quando** il test raggiunge g05, **allora** il risultato del test lo segnala esplicitamente come non verificato con la motivazione (nessun arco IMPLEMENTS), non un'omissione silenziosa dal report finale.
8. **Dato** l'intera suite eseguita, **quando** conto quante delle 10 query del golden dataset sono state effettivamente verificate con un'asserzione reale, **allora** il numero è 9 su 10, esplicito nel report — non "10 su 10" ottenuto facendo passare g05 con un'asserzione vuota.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Timeout nell'attesa di propagazione CDC (setup) | Fallimento esplicito del setup con messaggio chiaro su quale nodo non è comparso, non un timeout generico o un test che parte comunque su uno stato parziale |
| Un'entry del golden dataset con una categoria di `expected_facts` non riconosciuta (nessuna delle cinque note) | Fallimento esplicito del test per quell'entry con messaggio chiaro, non uno skip silenzioso — un caso non gestito è un errore da correggere, diverso da g05 (gestito e dichiarato) |

## 5. Non-goals
Nessuna verifica di g05 (dichiarata esplicitamente, vedi §2/§7). Nessuna verifica di performance/latenza della pipeline. Nessuna esecuzione in CI in questa SPEC (perimetro analogo a `persist_integration_test`/`server_integration_test`/`conftest.py` di SPEC-018 — richiede Docker + build multi-linguaggio, non `task test` di default; se opportuno wire in `Taskfile.yml`, valutare separatamente).

## 6. Vincoli dall'ADD
Milestone A del piano originale: "pipeline viva end-to-end" — questa SPEC è la sua dimostrazione diretta e misurabile, non un'interpretazione. Il fatto che 9 delle 10 query siano verificabili e 1 esplicitamente no è la fotografia onesta dello stato reale della pipeline a fine Fase 1, coerente con l'intera disciplina di questo progetto (documentare i limiti, non nasconderli).

## 7. Test plan
Il test STESSO è il test plan — non c'è un livello di test "sopra" un test end-to-end. Eseguito manualmente (richiede Docker, build multi-linguaggio) prima della chiusura di questa SPEC, con l'output completo (9 asserzioni verdi, g05 esplicitamente segnalato) incollato nel report.

### Output reale (esecuzione ripetuta due volte a stack fresco, per dimostrare determinismo dopo la deviazione di §10 punto 2)

```
$ .venv/bin/python -m pytest . -v -rs
============================= test session starts ==============================
platform linux -- Python 3.14.4, pytest-9.1.1, pluggy-1.6.0 -- .../tests/e2e/.venv/bin/python
rootdir: /home/luca/projects/eci/tests/e2e
plugins: anyio-4.14.2
collecting ... collected 13 items

test_golden.py::test_golden_entry[g01] PASSED                            [  7%]
test_golden.py::test_golden_entry[g02] PASSED                            [ 15%]
test_golden.py::test_golden_entry[g03] PASSED                            [ 23%]
test_golden.py::test_golden_entry[g04] PASSED                            [ 30%]
test_golden.py::test_golden_entry[g06] PASSED                            [ 38%]
test_golden.py::test_golden_entry[g07] PASSED                            [ 46%]
test_golden.py::test_golden_entry[g08] PASSED                            [ 53%]
test_golden.py::test_golden_entry[g09] PASSED                            [ 61%]
test_golden.py::test_golden_entry[g10] PASSED                            [ 69%]
test_golden.py::test_g05_implementations_explicitly_not_verified SKIPPED [ 76%]
test_golden.py::test_golden_dataset_verification_coverage PASSED         [ 84%]
test_golden.py::test_edge_case_cdc_timeout_explicit_failure PASSED       [ 92%]
test_golden.py::test_edge_case_unrecognized_expected_facts_category_hard_fails PASSED [100%]

=========================== short test summary info ============================
SKIPPED [1] test_golden.py:136: g05 (implementations) non verificato in questa SPEC (SPEC-019 §2/§5):
nessun arco IMPLEMENTS esiste nella pipeline reale — T1.1 (services/ingestion) non implementa il
rilevamento della soddisfazione implicita di interfaccia di Go (richiede analisi semantica dei tipi,
non parsing sintattico). Limite noto e dichiarato, da riaprire quando la risoluzione di tipo arriverà
(probabilmente Fase 2).
======================== 12 passed, 1 skipped in 54.38s ========================
```

Rilanciata immediatamente dopo, a stack completamente fresco (nuovi container Postgres/Neo4j/Kafka/Kafka-Connect, nuova ingestione dei 4 file), stesso esito identico:

```
======================== 12 passed, 1 skipped in 58.84s ========================
```

13 test raccolti = 9 query verificabili (`test_golden_entry[g01..g10]` esclusa g05) + 1 skip esplicito (g05) + 1 verifica del conteggio 9/10 (scenario 8) + 2 edge case di §4. Tutti verdi/skip-come-atteso in entrambe le esecuzioni.

## 8. Osservabilità
Nessun requisito nuovo — la pipeline già propaga `trace_id` end-to-end (verificato manualmente in questa stessa conversazione, catena HybridSearch→ExpandNeighbors→orchestrator.ask su un solo trace_id condiviso); il test non aggiunge strumentazione propria.

## 9. Criteri di accettazione
- [x] Scenari 1-6: le 9 query verificabili (g01-g04, g06-g10) tutte verdi con asserzioni esatte, non approssimative.
- [x] Scenario 7: g05 segnalata esplicitamente nel report come non verificata, con motivazione.
- [x] Scenario 8: il conteggio 9/10 esplicito nell'output del test.
- [x] Edge case tabella §4 (timeout CDC, categoria non riconosciuta) entrambi verificati con un caso concreto, non solo dichiarati.
- [x] Output completo del test reale incollato nel report — non un riepilogo.

## 10. Deviazioni dal testo della SPEC

1. **`tests/e2e/harness.py` come modulo separato, non solo `conftest.py`.**
   Non menzionato esplicitamente da §2. Necessario perché i due edge case
   di §4 devono richiamare la STESSA funzione usata dal setup/dalla
   verifica reale (`harness.wait_for_node`, `harness.classify_category`),
   non una copia semplificata — pytest non rende comodamente importabili
   funzioni da `conftest.py` in un test file dello stesso pacchetto senza
   ambiguità di collezione, un modulo dedicato è la soluzione più diretta.

2. **Attesa di propagazione CDC ampliata oltre i soli 4 id di File
   (§2/§3, deviazione più significativa).** Il testo di §2 specifica
   "polling di `GetNode` su un piccolo insieme di id noti (i 4 nodi
   File)". Implementato prima esattamente così; **verificato
   empiricamente insufficiente**: due esecuzioni consecutive dell'intera
   suite contro stack freschi hanno prodotto risultati diversi a parità
   di codice (prima esecuzione: 9/9 verdi; seconda, con l'unica modifica
   di un bug di confronto nomi: 4 fallimenti sparsi). Cause reali,
   entrambe osservate nei log:
   - **Nodo CONTAINS "nudo".** `services/sink-graph/internal/consumer/cypher.go`
     (`mergeCodeRelationQuery`) fa `MERGE (from:CodeNode {id:$from_id})
     MERGE (to:CodeNode {id:$to_id})` senza impostare proprietà.
     `CodeNode` e `CodeRelation` viaggiano su topic Kafka distinti senza
     garanzia d'ordine tra topic: un messaggio `CodeRelation` può essere
     processato prima del `CodeNode` del proprio endpoint, creando
     temporaneamente un nodo con solo `id` (nessun `name`, nessuna
     label di tipo). Osservato concretamente: `CONTAINS di util.go =
     {''}` invece di `{'computeTotal'}`.
   - **Indice full-text eventually-consistent.** L'indice `code_fulltext`
     di Neo4j 5 si popola in modo asincrono rispetto al commit della
     transazione che scrive il nodo — un `GetNode` già positivo non
     implica che `HybridSearch` trovi lo stesso nodo. Osservato
     concretamente: Fonti vuote per g01/g02/g03 in un'esecuzione in cui
     lo stesso identico codice, al run precedente, le trovava
     correttamente.

   Corretto ampliando la fixture `stack` (`conftest.py`) con due attese
   aggiuntive, sempre polling con timeout esplicito (mai uno sleep
   fisso, stesso principio di §2): `harness.wait_for_populated_contains`
   (ciascun File raggiunge il numero noto di figli CONTAINS con `name`
   non vuoto) e `harness.wait_for_fulltext_match` (le 3 entità cercate
   dalle query `callers` — Validate/Process/computeTotal — sono trovabili
   via full-text). Verificato deterministico su due esecuzioni
   consecutive a stack fresco dopo la correzione (§7).

3. **`packaging` aggiunto esplicitamente alle dipendenze del venv
   (README).** `testcontainers.community.kafka` importa `packaging`
   (`testcontainers/core/version.py`) ma il pacchetto `testcontainers`
   4.15.0 non lo dichiara nei propri metadata (`pip show testcontainers`
   non lo elenca tra `Requires`) — verificato empiricamente in un venv
   scratch con solo `testcontainers[kafka,postgres,neo4j]` installato:
   `ModuleNotFoundError: No module named 'packaging'` all'import. Gap
   reale del pacchetto upstream, non un errore di installazione locale.

4. **Nessuna dipendenza PEP 508 a path locale in `tests/e2e/`** (nessun
   `pyproject.toml` con `-e .`): stessa conclusione già raggiunta e
   documentata in SPEC-017/018 (`fakes/vllm-fake/README.md`,
   `services/orchestrator/README.md`) — non ripetuta qui, `tests/e2e`
   non è un pacchetto installabile (solo test + `conftest.py`/`harness.py`),
   il venv installa `eci_core`/`orchestrator` editable direttamente da
   `libs/py`/`services/orchestrator`.

5. **Id dei nodi calcolati deterministicamente invece che scoperti via
   ricerca full-text**, per le categorie `contains`/`methods`/
   `node_type` (non per `callers`, che usa `eci ask` reale come da §2).
   Non esplicitato da §2, ma coerente con esso: `harness.node_id`
   ricalcola lo stesso `sha256_hex(f"{file_path}:{qualified_name}")` di
   `services/ingestion/src/lib.rs::make_id` — evita di dipendere dalla
   tokenizzazione Lucene di un nome file con un punto interno (es.
   `order_service.go`), un caso non banale per un match full-text esatto
   e comunque non necessario dato che il valore di `file_path` passato a
   `ingestion` è scelto deterministicamente da questo stesso test
   (`conftest.py::_run_ingestion`, nome file bare, non un path assoluto).
