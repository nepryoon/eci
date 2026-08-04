# SPEC-018 — orchestrator v0: CLI `eci ask`, query→retrieval→prompt→vllm-fake→risposta con provenance (T1.5)
Stato: verified
Task-tree: T1.5 (quinto task di Fase 1, penultimo) · Servizio: services/orchestrator (Python, finora solo scaffold vuoto) · ADD: Modulo 2 (Agentic Reasoning, qui nella sua forma più semplice — nessun agente reale ancora)
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto (D7, client — non modificato)

## 1. Obiettivo
Popolare `services/orchestrator` con una CLI (`eci ask "<query>"`) che collega per la prima volta, in un solo percorso utente reale, tutto ciò che Fase 1 ha costruito finora: chiama `HybridSearch` su retrieval-engine (T1.4, gRPC), costruisce un prompt dal contesto recuperato, chiama `vllm-fake` (T0.9/SPEC-017) per la risposta, e stampa un risultato finale con **provenance esplicita e deterministica** — costruita dal codice dell'orchestrator dai nodi realmente recuperati, non dedotta fidandosi di cosa l'LLM (fake, qui) dice di aver citato.

## 2. Interfaccia

**CLI**: `eci ask "<query>"`, installata come `[project.scripts]` in `pyproject.toml` di `services/orchestrator` (comando reale dopo `pip install -e .`, non solo un `python -m` da ricordare).

**Client gRPC verso retrieval-engine**: riuso diretto di `libs/py/eci_core.grpc_client.SecurityContextInterceptor` + `opentelemetry.instrumentation.grpc` — esattamente la combinazione già verificata funzionante end-to-end contro un server Go reale nello scaffold di interoperabilità di SPEC-012 (che era, di fatto, una prova generale di questa stessa integrazione). `SecurityContext` costruito con valori placeholder di sviluppo (`tenant_id`/`user_id` fissi da config, `allowed_repos` vuoto) — propagato correttamente ma non enforced da nessuna parte della catena, stesso principio "plumbing, non enforcement" già stabilito per T0.8/T1.4.

`HybridSearchRequest` costruita minimale: `security_context`, `query_text` = argomento CLI. Tutti gli altri campi (`graph_limit`, `top_k`, `intent`, `domain`, ecc.) lasciati al valore zero del proto — retrieval-engine (T1.4/SPEC-016 §2) applica i propri default documentati (`graph_limit=200`, `top_k=25`, ecc.) quando il campo è zero-value: l'orchestrator non li duplica.

**Costruzione del prompt** per `vllm-fake`: dato che il fake echeggia letteralmente solo l'ULTIMO messaggio `role="user"` (SPEC-017 §2/§3 scenario 4), l'intero contesto va nel messaggio utente, non nel system:
```
Domanda: {query_text}

Contesto recuperato dal grafo:
- {node.name} ({node.node_type}, id={node.node_id})
- ... (una riga per ciascun RetrievedNode in HybridSearchResponse.nodes)

Rispondi basandoti solo sul contesto sopra, citando i nomi dei nodi usati.
```
Un messaggio `system` separato può precedere quello utente nella richiesta (istruzioni generali) — ignorato da `vllm-fake` per design, incluso comunque per coerenza con un futuro backend reale (Fase 5).

**Euristica "chi chiama" (query strutturali)**: se `query_text` (case-insensitive) contiene una delle frasi `["chi chiama", "who calls", "chiamanti di"]` — elenco chiuso, non un classificatore NLP — dopo `HybridSearch`, per ciascun nodo trovato nella risposta, chiama ANCHE `ExpandNeighbors(node_id=<id>, direction=TRAVERSAL_DIRECTION_REVERSE, edge_types=[EDGE_TYPE_CALLS])` e unisci i vicini risultanti (deduplicati per `node_id`) all'insieme usato per il prompt e per "Fonti". Motivazione: `HybridSearch` (full-text sul nome) non può rispondere a una domanda sui chiamanti — il proto stesso distingue questo caso (`TRAVERSAL_DIRECTION_REVERSE`, commento "impact"; `QUERY_INTENT_STRUCTURAL`, commento "chiamanti") e T1.4 ha già costruito l'RPC che serve.

**Chiamata a `vllm-fake`**: `POST /v1/chat/completions`, stesso shape OpenAI-compatible di SPEC-017. URL da config (`ORCHESTRATOR_VLLM_URL`, default coerente con l'esempio del README di SPEC-017).

**Output finale al terminale**, due parti distinte e sempre entrambe presenti:
1. La risposta del fake (`choices[0].message.content`) — nella walking skeleton è l'eco marcato di SPEC-017, non un vero ragionamento.
2. **Una sezione "Fonti" costruita direttamente dall'orchestrator** dai `RetrievedNode` ricevuti da retrieval-engine (nome, tipo, id, `provenance.repo`/`path`) — **indipendente da cosa il testo della risposta effettivamente menziona**. La provenance non è "quello che l'LLM dice di aver usato": è l'elenco deterministico di ciò che è stato davvero recuperato e passato nel prompt, verificabile a prescindere dal contenuto della risposta.

**`ORCHESTRATOR_RETRIEVAL_ADDR`** (default `localhost:50053`, la porta di T1.4/SPEC-016) e **`ORCHESTRATOR_VLLM_URL`** via `eci_core.config.env_or_default` — stesso pattern già usato ovunque.

**`eci_core` come dipendenza**: stesso problema, stessa soluzione già documentata e verificata in SPEC-017 — README con l'installazione in due passi (`pip install -e ../../libs/py -e .`), non un tentativo nuovo di risolverlo diversamente.

## 3. Comportamento (scenari)

1. **Dato** lo stack reale in esecuzione (Neo4j popolato da T1.1-T1.3, retrieval-engine e vllm-fake avviati), **quando** eseguo `eci ask "Chi chiama Validate?"`, **allora** l'output contiene sia la risposta del fake sia una sezione "Fonti" che elenca almeno `Process` (il nodo che effettivamente chiama `Validate`, coerente col golden dataset g01 di SPEC-009) — trovato via l'euristica "chi chiama" di §2 (`HybridSearch` matcha `Validate` per nome, poi `ExpandNeighbors` in direzione REVERSE su `CALLS` trova `Process`), non dalla sola `HybridSearch` (che da sola non può: full-text solo sul nome, verificato empiricamente che non trova `Process` per questa query).
2. **Dato** lo stesso stato, **quando** eseguo la STESSA query due volte, **allora** entrambe le risposte del fake sono identiche (determinismo end-to-end: stessa query → stesso HybridSearch → stesso contesto → stesso prompt → stessa risposta) — la sezione "Fonti" è anch'essa identica.
3. **Dato** una query che non trova nessun risultato in `HybridSearch` (`nodes` vuoto), **quando** eseguo `eci ask`, **allora** l'output lo segnala esplicitamente ("nessun risultato trovato nel grafo"), non invia un prompt con un contesto vuoto a `vllm-fake` né stampa una sezione "Fonti" vuota senza spiegazione.
4. **Dato** retrieval-engine irraggiungibile, **quando** eseguo `eci ask`, **allora** l'errore è esplicito e comprensibile (host/porta nel messaggio), non una traceback Python grezza.
5. **Dato** `vllm-fake` irraggiungibile (retrieval-engine invece raggiungibile e con risultati), **quando** eseguo `eci ask`, **allora** l'errore è ugualmente esplicito — e la sezione "Fonti" (che non dipende da `vllm-fake`) può comunque essere costruita e mostrata prima di segnalare l'errore sulla generazione, dato che la chiamata a retrieval-engine è già completata con successo a quel punto.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Query CLI vuota o assente (`eci ask` senza argomento) | Errore d'uso esplicito (`eci ask "<query>"`), non una chiamata con `query_text=""` a retrieval-engine |
| `HybridSearchResponse.vector_leg_degraded=true` (sempre vero a questo stadio, T1.4 §2) | Non è un errore da segnalare come tale all'utente — è lo stato normale e permanente di questa fase (sola gamba grafo), non un degrado transitorio: nessun messaggio di avviso allarmante, al più una nota informativa se si vuole |
| Un `RetrievedNode` con `provenance` parzialmente vuota (campi non ancora popolabili dalla pipeline, T1.4 §2) | La sezione "Fonti" mostra solo i campi realmente disponibili (nome, tipo, id, repo/path) — non stampa placeholder tipo "N/A" per ogni campo mancante, semplicemente li omette |

## 5. Non-goals
Nessun vero agente/reasoning multi-step (LangGraph o simili — Fase 5). Nessuna gestione di conversazioni multi-turno (`eci ask` è query singola, stateless). Nessun vero LLM (resta `vllm-fake` fino a Fase 5). Nessun enforcement di `SecurityContext` (stesso principio di T0.8/T1.4, Fase 6). Nessuna cache semantica (Fase 2+).

## 6. Vincoli dall'ADD
Modulo 2: il flusso query→retrieval→prompt→risposta-con-provenance è la forma più semplice del pattern agentic reasoning descritto nel Modulo 2 — qui senza ancora reasoning multi-step, senza verification layer (entrambi fasi successive), ma con la garanzia di provenance già strutturalmente presente fin da questa prima versione, non aggiunta dopo.

## 7. Test plan
Test di integrazione: un vllm-fake e un retrieval-engine reali (quest'ultimo con testcontainers Neo4j, stesso pattern SPEC-016) avviati per il test, orchestrator chiamato direttamente (funzione Python, non necessariamente il processo CLI) per gli scenari 1-3/5. Scenario 4 verificabile puntando la config a un indirizzo che non risponde. Scenario 2 (determinismo end-to-end) è la verifica più importante — due invocazioni complete, confronto diretto dell'output.

## 8. Osservabilità
Stessa fondazione OTel di `libs/py/eci_core` — uno span per l'intera operazione `ask`, che include (via gli interceptor già esistenti) sia la chiamata gRPC a retrieval-engine sia quella HTTP a `vllm-fake`.

## 9. Criteri di accettazione
- [x] Scenario 1: query reale contro lo stack vero produce risposta + Fonti corrette, verificate con l'output esatto nel report (non solo "funziona") — vedi §10 evidenza.
- [x] Scenario 2: determinismo end-to-end verificato con due invocazioni confrontate direttamente (`run_ask` chiamata due volte, `AskResult` completo confrontato con `==`, non solo il testo della risposta).
- [x] Scenario 3: nessun risultato gestito esplicitamente (`nodes == []`, `answer is None` — `run_ask` ritorna PRIMA di chiamare vllm-fake, per costruzione), nessun prompt vuoto inviato al fake.
- [x] Scenari 4/5: errori su retrieval-engine/vllm-fake irraggiungibili entrambi verificati esplicitamente con test dedicati, non solo il primo — scenario 5 verifica anche che le Fonti siano già disponibili nell'eccezione.
- [x] `eci ask` funziona come comando reale installato (`[project.scripts]`) — verificato con `subprocess.run` sul binario reale in `.venv/bin/eci` (non richiamando `run_ask`/`main()` in-process), sia per i casi d'errore CLI sia per un `ask` end-to-end completo.

## 10. Deviazioni rispetto alla SPEC

1. **Correzione di uno scarto reale tra §2 e §3 scenario 1, scoperto empiricamente prima di scrivere il codice**: il testo originale di §2 descriveva un flusso a una sola chiamata `HybridSearch`, ma §3 scenario 1 richiedeva che la query `"Chi chiama Validate?"` producesse `Process` in Fonti — irraggiungibile con la sola full-text search (che fa match solo sul nome). Verificato contro un Neo4j reale prima di implementare: zero match per la query letterale, e anche ripulita trova solo `Validate` per nome, mai i suoi chiamanti. Non risolto in autonomia: segnalato esplicitamente, la SPEC è stata corretta con testo fornito dall'utente (§2 "Euristica 'chi chiama'", §3 scenario 1) PRIMA di procedere con l'implementazione — non un'estensione decisa unilateralmente in corso d'opera.

2. **Bug meccanico scoperto durante l'implementazione dell'euristica appena corretta**: anche con l'euristica "chi chiama" implementata, `HybridSearch("Chi chiama Validate?")` (testo grezzo, `?` finale) non trova NEMMENO `Validate` — zero hit — perché in sintassi Lucene un `?` finale è un wildcard a un carattere (`Validate?` cerca un termine di 9 caratteri, non gli 8 di `Validate`). Senza un nodo di partenza, `ExpandNeighbors` non ha nulla da espandere: scenario 1 falliva ancora (`nodes == []`) dopo aver implementato esattamente il testo appena corretto. Fix: `retrieval_client._sanitize_for_fulltext` ripulisce la punteggiatura finale (`?!.,;:`) da `query_text` prima di ogni `HybridSearch` — necessario perché l'asserzione già scritta nel testo corretto ("`HybridSearch` matcha `Validate` per nome") fosse vera, non una nuova decisione di design. Verificato con un test dedicato (`test_hybrid_search_alone_does_not_find_process_for_who_calls_query`) che documenta il comportamento pre-fix.

3. **Verifica del meccanismo, non solo del risultato finale**: oltre a verificare che `Process` compaia in Fonti per la query "chi chiama", due test dedicati provano che `find_callers` usa davvero `TRAVERSAL_DIRECTION_REVERSE` e non un'altra direzione che darebbe lo stesso risultato per caso — sfruttando un'asimmetria deliberata nel grafo di test (`Validate` non ha archi `CALLS` USCENTI): `test_find_callers_uses_reverse_direction_on_calls` conferma che REVERSE da `Validate` trova `Process`; `test_forward_expand_from_validate_finds_no_callers` conferma che FORWARD dallo stesso nodo non trova nulla — se la direzione fosse sbagliata, il secondo test fallirebbe (troverebbe comunque zero risultati per un motivo diverso, ma la coppia dei due test insieme dimostra che il risultato di `find_callers` dipende realmente dalla direzione, non da un artefatto del grafo).

4. **Struttura del pacchetto**: lo scaffold di SPEC-001 aveva `services/orchestrator/__init__.py` direttamente nella root del servizio (nessuna sottodirectory pacchetto), un layout che avrebbe richiesto una configurazione setuptools non standard per funzionare con `[tool.setuptools.packages.find]`. Ristrutturato in `services/orchestrator/orchestrator/` (stesso pattern di `fakes/vllm-fake/vllm_fake/`, SPEC-017): il vecchio `__init__.py` vuoto è stato rimosso da git e ricreato nella nuova posizione, nessun contenuto perso (era vuoto).

5. **`eci_core` come dipendenza**: stesso problema, stessa soluzione già verificata SPERIMENTALMENTE in SPEC-017 (non solo "stessa soluzione" per analogia) — nessuna sintassi PEP 508 per path locale ritentata. README con l'installazione in due passi, identica nello spirito a quella di `fakes/vllm-fake/README.md`.

6. **Test cross-linguaggio**: a differenza di SPEC-015/016 (harness nello stesso linguaggio del servizio sotto test), qui i test Python avviano DAVVERO un binario Go (`go build` in una directory temporanea + sottoprocesso, `services/retrieval-engine` non modificato) e un processo `uvicorn` da un venv Python SEPARATO (`fakes/vllm-fake/.venv`, con bootstrap automatico se assente) — nessuno dei due è un mock. `conftest.py` gestisce tre risorse esterne (container Neo4j, sottoprocesso Go, sottoprocesso Python) con fixture `session`-scoped e attesa di readiness esplicita per ciascuna (poll gRPC `Health` per retrieval-engine, poll HTTP per vllm-fake — non un semplice `sleep`).

7. **Rumore OTel su stdout della CLI, osservato e non corretto (fuori scope)**: `eci_core.observability.init_tracing` usa `ConsoleSpanExporter`, che scrive il dump JSON di ogni span su stdout in modo sincrono — stesso comportamento già stabilito per gli altri servizi Python/Go/Rust (SPEC-010/011/012), ma per un comando CLI il cui output è pensato per un utente (non un server il cui stdout è già "log"), questo interfoglia il dump degli span (`HybridSearch`, `ExpandNeighbors`, `orchestrator.ask`) PRIMA del testo di risposta/Fonti — verificato nell'evidenza sotto. Non corretto qui: richiederebbe modificare `libs/py/eci_core/observability.py` per instradare il `ConsoleSpanExporter` su stderr (o un flag per disabilitarlo), fuori dal perimetro file di questa sessione (`services/orchestrator`, non `libs/py`).

8. **Versioni esatte risolte**: `grpcio==1.83.0`, `protobuf==7.35.1` (più recente del minimo `>=5` dichiarato — nessun vincolo superiore imposto, coerente con `libs/py`), `opentelemetry-api`/`opentelemetry-sdk==1.44.0`, `opentelemetry-instrumentation-grpc==0.65b0` (stesse versioni di `libs/py`/SPEC-012), `httpx==0.28.1`, `testcontainers==4.15.0` (con `testcontainers.community.neo4j.Neo4jContainer` — l'import `testcontainers.neo4j` originale è deprecato in questa versione, migrato al modulo `community` fin dall'inizio), `neo4j==6.2.0` (driver, indiretta via `testcontainers[neo4j]`), `pytest==9.1.1`.

### Evidenza scenario 1/2/3/4/5 — output esatto

Stack reale per il test manuale: Neo4j via testcontainers (stesso grafo di
`conftest.py`: `File`/`Class`/2×`Method`/`Function`, `Process` `CALLS`
`Validate`), `retrieval-engine` compilato ed eseguito come sottoprocesso
Go puntato a quel Neo4j, `vllm-fake` eseguito dal suo venv — `eci`
invocato come **binario reale** (`.venv/bin/eci`), non `run_ask` in
Python. Comando completo di cattura in
`/tmp/.../scratchpad/manual_ask_report.py` (script di verifica, non
parte del repo).

```
$ eci ask "Chi chiama Validate?"
exit code: 0
--- stdout ---
{... dump JSON dello span client "HybridSearch" (ConsoleSpanExporter, §10 punto 7) ...}
{... dump JSON dello span client "ExpandNeighbors" ...}
{... dump JSON dello span "orchestrator.ask" ...}
[vllm-fake risposta deterministica] Domanda: Chi chiama Validate?

Contesto recuperato dal grafo:
- Validate (Method, id=orch-test-method-validate)
- Process (Method, id=orch-test-method-process)

Rispondi basandoti solo sul contesto sopra, citando i nomi dei nodi usati.

Fonti:
- Validate, tipo=Method, id=orch-test-method-validate, repo=local, path=order_service.go
- Process, tipo=Method, id=orch-test-method-process, repo=local, path=order_service.go
```

Scenario 2 (determinismo): la STESSA invocazione ripetuta produce
byte-per-byte lo stesso blocco `[vllm-fake risposta deterministica] ...`
e la stessa sezione Fonti (id, ordine, contenuto identici) — verificato
nel test automatico `test_scenario2_end_to_end_determinism` con
`AskResult` intero confrontato via `==`, non solo a occhio sull'output
testuale.

```
$ eci ask "computeTotal"   (query SENZA l'euristica "chi chiama")
exit code: 0
--- stdout ---
[vllm-fake risposta deterministica] Domanda: computeTotal

Contesto recuperato dal grafo:
- computeTotal (Function, id=orch-test-func-compute-total)

Rispondi basandoti solo sul contesto sopra, citando i nomi dei nodi usati.

Fonti:
- computeTotal, tipo=Function, id=orch-test-func-compute-total, repo=local, path=util.go
```

```
$ eci ask "zzzznonexistentqueryterm12345"   (scenario 3, nessun risultato)
exit code: 0
--- stdout ---
nessun risultato trovato nel grafo per: 'zzzznonexistentqueryterm12345'
```

```
$ eci ask   (nessun argomento — edge case §4)
exit code: 2
--- stderr ---
usage: eci [-h] {ask} ...
eci: error: argomento "query" mancante — uso: eci ask "<query>"
```

```
$ eci ask "Validate"   (scenario 4, retrieval-engine irraggiungibile: ORCHESTRATOR_RETRIEVAL_ADDR=127.0.0.1:1)
exit code: 1
--- stderr ---
errore: impossibile raggiungere retrieval-engine (127.0.0.1:1): <_InactiveRpcError of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:1: Failed to connect to remote host: Connection refused"
>
```

```
$ eci ask "Chi chiama Validate?"   (scenario 5, vllm-fake irraggiungibile: ORCHESTRATOR_VLLM_URL=http://127.0.0.1:1)
exit code: 1
--- stdout ---
{... dump JSON degli span, come sopra ...}
Fonti:
- Validate, tipo=Method, id=orch-test-method-validate, repo=local, path=order_service.go
- Process, tipo=Method, id=orch-test-method-process, repo=local, path=order_service.go
--- stderr ---
errore: impossibile raggiungere vllm-fake (http://127.0.0.1:1): [Errno 111] Connection refused
```

Scenario 5 conferma esattamente quanto richiesto: le Fonti (`Validate`,
`Process` — costruite da retrieval-engine, già raggiunto con successo)
sono stampate PRIMA dell'errore sulla generazione, non scartate insieme
al fallimento della chiamata a vllm-fake.
