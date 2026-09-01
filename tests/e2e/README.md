# e2e

Test end-to-end (SPEC-019, T1.6): ingerisce il fixture di SPEC-009
attraverso la pipeline reale per intero — `ingestion` (Rust) →
Postgres+outbox → Debezium/Kafka (CDC) → `sink-graph` (Go) → Neo4j →
`retrieval-engine` (Go) → `orchestrator` (Python) — e verifica le query
del golden dataset (`tests/golden/queries_v0.json`) contro il risultato
reale. Vedi `docs/specs/SPEC-019-e2e-golden-dataset.md` per il
comportamento completo.

Wired esplicitamente in `task test:e2e` (SPEC-068), non in `task test`: richiede Docker, build
multi-linguaggio (Go/Rust) e i toolchain relativi sul PATH — stesso
perimetro di `persist_integration_test`/`server_integration_test`/
`services/orchestrator/orchestrator/conftest.py` (SPEC-014/016/018).

## Requisiti

- Docker in esecuzione.
- `cargo`/`go` sul PATH (compilano `ingestion`/`sink-graph`/
  `retrieval-engine` come sottoprocessi reali).
- Il binario `migrate` sul PATH (applica `contracts/sql/migrations` —
  stesso strumento di `task db:migrate` e di
  `tests/integration/outbox_cdc`, SPEC-008).

## Setup

`eci_core` (`libs/py`) e `orchestrator` (`services/orchestrator`) non
sono dichiarati come dipendenze PEP 508 a path locale in nessun
`pyproject.toml` di questo repo: nessuna delle due sintassi (relativa o
assoluta) è risultata affidabile per un file committato (stessa
conclusione già documentata in `fakes/vllm-fake/README.md` e
`services/orchestrator/README.md`). Il venv installa quindi entrambi i
pacchetti editable esplicitamente, più le dipendenze specifiche di
questo test (testcontainers per Postgres/Neo4j/Kafka, `packaging` —
dipendenza reale di `testcontainers.community.kafka` non dichiarata nei
suoi stessi metadata, verificato empiricamente, vedi SPEC-019 §10):

```bash
cd tests/e2e
python3 -m venv .venv   # o: python3 -m virtualenv .venv, se python3-venv non è installato
.venv/bin/pip install -e ../../libs/py -e ../../services/orchestrator \
    pytest "testcontainers[neo4j,postgres,kafka]" packaging
```

## Esecuzione

```bash
.venv/bin/python -m pytest . -v
```

Sessione lunga (build Go/Rust, avvio di ~5 container Docker, attesa
propagazione CDC reale): tipicamente alcuni minuti alla prima
esecuzione, più veloce alle successive grazie alle cache di
build/immagini Docker.
