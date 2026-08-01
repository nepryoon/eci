# ADR-0001 — Commit mancante di contracts/proto/retrieval.proto (D7)
Stato: accettata · Data: 2026-08-01

## Contesto
`contracts/proto/eci/retrieval/v1/retrieval.proto` (Deliverable D7 dell'ADD)
non è mai stato committato durante il setup iniziale del progetto, nonostante
fosse previsto. Durante l'implementazione di SPEC-002 è stato individuato un
file di lavoro non tracciato (`retrieval/v1/retrieval.proto`), verificato via
diff identico al contenuto D7 dell'ADD consolidato.

## Decisione
Si colma il gap spostando il file verificato nel path canonico. Nessuna
modifica di contenuto rispetto a D7.

## Conseguenze
`contracts/buf.gen.yaml` genera d'ora in poi esclusivamente da questo path.
