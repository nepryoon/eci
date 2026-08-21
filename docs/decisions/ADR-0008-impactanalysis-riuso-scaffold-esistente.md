# ADR-0008 — ImpactAnalysis (T4.2): riuso dello scaffold proto esistente invece delle nuove forme di SPEC-042

Stato: accettata · Data: 2026-08-21

## Contesto
`contracts/proto/eci/retrieval/v1/retrieval.proto` contiene già, dal
contratto D7 originale (mai implementato — `ImpactAnalysis` cade su
`UnimplementedRetrievalEngineServer`, `codes.Unimplemented`):
`rpc ImpactAnalysis(ImpactAnalysisRequest) returns (stream ImpactAnalysisEvent);`,
`message ImpactAnalysisRequest` (`security_context`, `entry_node_id`,
`max_depth`, `edge_types`, `direction`, `fanout_cap_per_hop`,
`min_impact_score`, `include_source_text`), `message ImpactedNode`
(`RetrievedNode node`, `ImpactKind impact_kind` — un ENUM di
classificazione severità: `SYNTACTIC`/`BEHAVIORAL`/`MODULE_BOUNDARY`, ADD
Modulo 2 §1.5 — `repeated EdgeType path_edge_types`, `uint32 depth`),
`message ImpactProgress` (`nodes_emitted`, `frontier_size`,
`current_depth`, `truncated_by_fanout_cap`, `truncated_by_depth`),
`message ImpactAnalysisEvent` (oneof `node`/`progress`).

SPEC-042 §2 propone invece messaggi NUOVI che riusano DUE di quegli stessi
nomi (`ImpactAnalysisRequest`, `ImpactProgress`) con campi INCOMPATIBILI, e
un'RPC omonima (`ImpactAnalysis`) con un tipo di ritorno diverso
(`stream ImpactAnalysisResponse` invece di `stream ImpactAnalysisEvent`).
Questo non è un semplice conflitto di naming: `protoc`/`buf` rifiuta
messaggi duplicati nello stesso package, e non possono esistere due RPC
omonime con firme diverse nello stesso `service`. Inoltre `impact_kind`
in SPEC-042 (stringa che rispecchia il TIPO DI ARCO più vicino al punto di
partenza, es. `"CALLS"`) è concettualmente diverso dall'enum `ImpactKind`
già esistente (classificazione di SEVERITÀ dell'impatto).

Verificato (`grep` su tutto il repo, Go/Python/doc) che NESSUN codice reale
consuma lo scaffold esistente oltre ai binding generati — solo
documentazione (PLAYBOOK.md, l'ADD, questa stessa SPEC). Nessun rischio di
rompere consumer reali in nessuna delle due direzioni.

## Decisione
Si adatta SPEC-042 alle forme GIÀ esistenti nel contratto, invece di
introdurre nomi nuovi in conflitto (opzione scelta esplicitamente
dall'utente tra le due presentate in sessione):

- **RPC**: invariata — `rpc ImpactAnalysis(ImpactAnalysisRequest) returns (stream ImpactAnalysisEvent);`.
- **`ImpactAnalysisRequest`**: estesa additivamente con `int32 max_nodes = 9`
  (cap SPEC-042, concetto NUOVO — TOTALE sull'intera traversata, diverso da
  `fanout_cap_per_hop` esistente, che resta non implementato in questa SPEC),
  `Domain domain = 10`, `repeated string repos = 11` (SPEC-042 li voleva sul
  nuovo messaggio; qui aggiunti al messaggio riusato). I campi esistenti
  `edge_types`/`direction`/`fanout_cap_per_hop`/`min_impact_score`/
  `include_source_text`/`security_context` restano accettati ma NON
  implementati da T4.2 (stesso principio "plumbing, non enforcement" già
  stabilito ripetutamente nel progetto per campi non ancora rilevanti alla
  fase corrente) — traversata sempre REVERSE, sempre sui sei tipi di arco
  fissi di D5/GraphTraversal.
- **`ImpactedNode`**: riusato senza modifiche di schema. `node` (RetrievedNode)
  popolato solo con `node_id`/`domain`/`provenance`/`scores.hop_distance`
  (stesso principio "solo i campi che la pipeline può popolare davvero",
  T1.4/T4.1). `impact_kind` (l'enum) resta `UNSPECIFIED`: la classificazione
  di severità richiede GDS (T4.3, esplicitamente fuori scope). Il concetto
  di SPEC-042 chiamato "impact_kind" (tipo d'arco dell'ultimo hop) è
  portato invece da `path_edge_types`, popolato con un singolo elemento
  (il tipo d'arco del percorso più breve) — uso legittimo del campo
  esistente, il cui commento originale ("catena di archi... per
  spiegabilità") non impone una lunghezza minima.
- **`ImpactProgress`**: riusato senza modifiche di schema.
  `nodes_emitted`←`nodes_explored` di SPEC-042; `frontier_size`←nodi
  scoperti nel livello corrente; `current_depth` invariato;
  `truncated_by_fanout_cap`←`truncated` di SPEC-042 (il cap `max_nodes` è,
  concettualmente, un cap sul fan-out — solo globale invece che per-hop);
  `truncated_by_depth` resta sempre `false` (raggiungere `max_depth`
  naturalmente non è un troncamento in questo modello, è terminazione
  normale — SPEC-042 non ha un concetto distinto per questo caso).
- **`ImpactAnalysisEvent`**: riusato senza modifiche.

## Conseguenze
Diff proto minimo (solo 3 campi additivi su un messaggio esistente, nessun
nuovo messaggio, nessuna modifica di RPC) — stesso principio "estensione
additiva" già applicato in ADR-0007, stessa necessità di questa ADR per lo
stesso motivo (`task guard`/ADR-0002: qualunque modifica-M a un file sotto
`contracts/` richiede un ADR nello stesso diff, additiva o meno). Bindings
rigenerati per ENTRAMBI i linguaggi (`task proto:gen` completo, non solo
`buf generate` — lezione diretta da SPEC-041/ADR-0007, dove solo i binding
Go furono rigenerati perché nessun consumer Python esisteva; qui
`ImpactAnalysisRequest` è un messaggio pubblico del contratto, generarne
solo metà lascerebbe `libs/py/eci_core` silenziosamente disallineato).
