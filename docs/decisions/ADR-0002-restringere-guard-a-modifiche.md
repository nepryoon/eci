# ADR-0002 — Restringere l'ambito di task guard alle sole modifiche

Stato: accettata · Data: 2026-08-01

## Contesto
guard.sh richiedeva un ADR per ogni tocco a contracts/, incluse le aggiunte
di nuovi file di tooling che non alterano il contenuto dei contratti
(es. buf.yaml/buf.gen.yaml per SPEC-002). Con un solo sviluppatore e review
per-PR già in vigore, questo produceva ADR ripetitivi a basso valore.

## Decisione
guard.sh richiede ADR solo su modifica (M) o cancellazione (D) di file già
tracciati sotto contracts/ o docs/add/. Le aggiunte (A) restano libere.

## Conseguenze
Le SPEC che aggiungono file di supporto sotto contracts/ non richiedono più
un ADR dedicato; la review per-PR resta il controllo sulle aggiunte.
