# ADR-0009 — Estensione additiva di HybridSearchRequest (enable_rerank)

Stato: accettata · Data: 2026-08-22

## Contesto
SPEC-044 (T4.4) applica un reranker cross-encoder (`bge-reranker-v2-m3` via
TEI) ai risultati fusi RRF di `HybridGraphVectorSearch` (T4.1), riordinati
per `final_score = rerank_score + beta*proximity_boost` — dove
`proximity_boost` combina `hop_distance` (T4.1) e `impact_score` (T4.3,
SPEC-043). Il reranking richiede lo STESSO `entry_node_id` già usato dalla
gamba grafo (per calcolare `hop_distance` e leggere `impact_score`), quindi
è naturalmente un flag sullo stesso `HybridSearchRequest` (T4.1/ADR-0007),
non un parametro indipendente o un'RPC separata.

`contracts/proto/eci/retrieval/v1/retrieval.proto` è sotto `contracts/`,
protetto da `task guard` (ADR-0002): qualunque modifica (M) a un file già
tracciato richiede un ADR nello stesso diff, indipendentemente dalla natura
additiva del contenuto — stesso principio già applicato in ADR-0007/0008,
qui riapplicato per lo stesso motivo (estensione di un messaggio esistente,
non un file nuovo).

## Decisione
Si estende `HybridSearchRequest` con un solo campo additivo, tag mai
riusato prima (11/12 già occupati da T4.1/ADR-0007):
- `bool enable_rerank = 13;`

Default `false` (zero-value proto3): comportamento T4.1 invariato, nessuna
chiamata al servizio di reranking, nessuna regressione sui client
esistenti — stesso principio già stabilito per `entry_node_id`/`max_depth`
in ADR-0007. Quando `true`, il reranking è ESPLICITAMENTE richiesto dal
client: un fallimento del servizio di reranking fa fallire l'intera RPC
(SPEC-044 §3 scenario 5), a differenza della gamba vettoriale di T4.1 (che
degrada silenziosamente) — la differenza è intenzionale, non un'
incoerenza: qui il client ha chiesto esplicitamente il reranking, quindi
un risultato non ri-ordinato spacciato per ri-ordinato sarebbe peggio di un
errore esplicito.

## Conseguenze
`libs/go/eci/retrieval/v1/retrieval.pb.go` E
`libs/py/eci_core/retrieval/v1/retrieval_pb2.py` rigenerati per ENTRAMBI i
linguaggi (`task proto:gen` completo) FIN DALL'INIZIO — lezione diretta da
SPEC-041 (dove solo Go fu rigenerato inizialmente, richiedendo una
correzione in SPEC-042), non da riscoprire qui. Un client che imposta
`enable_rerank=true` senza `entry_node_id` riceve lo stesso errore di
validazione già stabilito da T4.1 per `entry_node_id` mancante (nessuna
nuova validazione richiesta: il reranking dipende dal percorso
grafo+vettoriale completo, che già richiede `entry_node_id`).
