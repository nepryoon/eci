# ADR-0007 — Estensione additiva di HybridSearchRequest (entry_node_id, max_depth)

Stato: accettata · Data: 2026-08-21

## Contesto
SPEC-041 (T4.1) porta in Go l'algoritmo `hybrid_graph_vector_search`
dell'ADD (Modulo 2, Deliverable D5), che richiede un punto di ancoraggio
noto (`entry_node_id`) e un bound di profondità (`max_depth`) per la
traversata grafo tipizzata — campi assenti da `HybridSearchRequest`
(T1.4/SPEC-016, quando la ricerca ibrida copriva solo full-text sul nome,
senza bisogno di un punto di partenza).

`contracts/proto/eci/retrieval/v1/retrieval.proto` è sotto `contracts/`,
protetto da `task guard` (ADR-0002): guard.sh richiede un ADR per qualunque
modifica (M) a un file già tracciato sotto `contracts/`, indipendentemente
dalla natura additiva o meno del cambiamento — a differenza delle aggiunte
di nuovi file (A), libere. Una migrazione SQL additiva (nuova migrazione
numerata, precedente citato in SPEC-026 §10/SPEC-027) è sempre uno status
"A" e non richiede ADR per questo motivo; un'estensione di un `.proto`
esistente è per costruzione uno status "M" sullo stesso file, e quindi
richiede questo ADR anche quando additiva.

## Decisione
Si estende `HybridSearchRequest` con due nuovi campi, tag mai riusati prima
(nessun campo rimosso o rinominato):
- `string entry_node_id = 11;`
- `int32 max_depth = 12;`

Nessun impatto sui client esistenti: `entry_node_id` non impostato (default
proto3, stringa vuota) preserva esattamente il comportamento T1.4
(`HybridSearch` full-text-only, `vector_leg_degraded` sempre `true`) —
verificato esplicitamente da un test di integrazione dedicato
(`TestHybridSearchGraphVectorDispatch/EntryNodeIdEmpty_UsesLegacyFullTextPath`,
`services/retrieval-engine/internal/server`).

## Conseguenze
`libs/go/eci/retrieval/v1/retrieval.pb.go` rigenerato (`buf generate`) per
riflettere i due nuovi campi — nessuna rigenerazione dei binding Python
(`libs/py/eci_core/retrieval/v1/`), che questa SPEC non consuma (la
reference D5 usa `neo4j`/`qdrant-client` direttamente, mai gli stub gRPC).
Un client che imposta `entry_node_id` riceve il nuovo percorso
grafo+vettoriale completo (`hybridsearch.HybridGraphVectorSearch`,
SPEC-041); un client che non lo imposta continua a ricevere il
comportamento T1.4 invariato.
