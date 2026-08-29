# orchestrator

CLI `eci ask "<query>"` — query → `HybridSearch` (retrieval-engine) →
prompt → `vllm-fake` → risposta con provenance (SPEC-018, T1.5). Vedi
`docs/specs/SPEC-018-orchestrator-cli.md` per il comportamento completo.

## Setup

Stesso problema e stessa soluzione già verificati e documentati in
`fakes/vllm-fake/README.md` (SPEC-017): `eci_core` (`libs/py`) non è
dichiarato come dipendenza in `pyproject.toml` — nessuna sintassi PEP 508
per un path locale si è rivelata affidabile per un file committato in
questo monorepo (path relativo si risolve rispetto alla CWD del comando
`pip install`, non alla posizione di `pyproject.toml`; path assoluto
hardcoda una singola macchina). Non ritentato qui.

```bash
cd services/orchestrator
python3 -m venv .venv   # o: python3 -m virtualenv .venv, se python3-venv non è installato
.venv/bin/pip install -e ../../libs/py -e ".[test]"
```

`eci` diventa un comando reale nel venv dopo l'install (`[project.scripts]`):

```bash
.venv/bin/eci ask "Chi chiama Validate?"
```

Config via env (`eci_core.config.env_or_default`): `ORCHESTRATOR_RETRIEVAL_ADDR`
(default `localhost:50053`), `ORCHESTRATOR_VLLM_URL` (default `http://localhost:8001`).

## Golden evaluation

`eci eval-golden` usa il contratto interno
`golden-structured-facts-v2`: il modello restituisce esclusivamente una lista di
`{"kind": ..., "value": ...}` con identificatori canonici e citations
separate. Il prompt non osserva `expected_facts` o `scope_note`; la
canonicalizzazione usa la symbol table ricavata dal repository e accredita un
simbolo corto soltanto quando la risoluzione è univoca. Il summary mantiene le
metriche storiche e aggiunge recall exact/semantic, coverage, taxonomy,
`prompt_contract_version` e `logic_fingerprint` (SPEC-054).

## Test

Il core T5.1 espone `orchestrator.graph.run_agent`: costruisce ed esegue
realmente il loop LangGraph bounded. In produzione va passato un
`RetrievalToolRuntime(Deps(...))`; il parametro opzionale `reasoner` riceve il
payload effettivo di ogni thought e può essere collegato a `chat_completion`.
`visited` resta nello stato del grafo e non è accettato dalla funzione che
costruisce il prompt.

Test di integrazione: avviano per davvero un Neo4j via testcontainers,
un `retrieval-engine` reale (sottoprocesso Go, `go run .` — richiede
il toolchain Go sul PATH) e un `vllm-fake` reale (sottoprocesso
`uvicorn` dal suo venv, `fakes/vllm-fake/.venv` — bootstrap automatico
se assente, stessi comandi del suo README). Richiede Docker.

```bash
.venv/bin/python -m pytest orchestrator/ -v
```
