# vllm-fake

LLM fake OpenAI-compatible con risposte deterministiche (SPEC-017, T0.9
scope rivisto). Vedi `docs/specs/SPEC-017-vllm-fake.md` per il
comportamento completo.

## Setup

`eci_core` (`libs/py`) non è dichiarato come dipendenza in
`pyproject.toml`: non esiste in questo monorepo un meccanismo di
path-dependency Python affidabile. Le due sintassi PEP 508 per un path
locale sono state verificate entrambe fragili per un file committato:

- `eci-core @ file:../../libs/py` (relativo) si risolve rispetto alla
  CWD del comando `pip install`, non rispetto alla posizione di
  `pyproject.toml` — funziona solo lanciando il comando da dentro
  questa directory, si rompe con `pip install -e fakes/vllm-fake` da
  root del repo.
- `eci-core @ file:///home/utente/...` (assoluto) funziona da
  qualunque CWD ma hardcoda un path specifico di una singola macchina
  — rotto per chiunque altro clona il repo o in CI.

Il venv di sviluppo/test installa quindi entrambi i pacchetti editable
esplicitamente, nello stesso comando:

```bash
cd fakes/vllm-fake
python3 -m venv .venv   # o: python3 -m virtualenv .venv, se python3-venv non è installato
.venv/bin/pip install -e ../../libs/py -e . pytest ruff
```

## Test

```bash
.venv/bin/python -m pytest vllm_fake/ -v
```

## Avvio locale

```bash
.venv/bin/uvicorn vllm_fake.main:app --port 8001
```
