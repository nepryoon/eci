# SPEC-069 — Completamento del contratto ImpactAnalysis
Stato: implemented
Task-tree: T4.2 · Servizio: services/retrieval-engine · ADD: Modulo 2 §1.2–§1.5, D7
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto

## 1. Obiettivo

`ImpactAnalysis` applica ogni controllo pubblico finora rinviato da
ADR-0008: tipi/direzione, fan-out, score, repository multipli, source text,
percorso completo, classificazione e troncamenti distinti. La traversata
resta BFS streaming, bounded, deterministica e filtrata prima del retrieval
esclusivamente dallo scope autenticato.

## 2. Interfaccia

```protobuf
rpc ImpactAnalysis(ImpactAnalysisRequest) returns (stream ImpactAnalysisEvent);

message ImpactProgress {
  uint32 nodes_emitted = 1;
  uint32 frontier_size = 2;
  uint32 current_depth = 3;
  bool truncated_by_fanout_cap = 4;
  bool truncated_by_depth = 5;
  bool truncated_by_node_cap = 6;
}
```

Interfaccia interna:

```go
type Options struct {
    MaxDepth, MaxNodes, FanoutCap int
    EdgeTypes, Repos []string
    Direction string
    MinImpactScore float64
    Domain *string
    HydrateLevel func(context.Context, []*ImpactNode) error
}

func StreamImpact(context.Context, neo4j.DriverWithContext, string, Options,
    func(ImpactEvent) error) error
```

## 3. Comportamento

1. **Dato** un grafo con tipi diversi, **quando** il client seleziona tipi e
   direzione, **allora** vengono attraversati solo quei tipi nella direzione
   richiesta; lista vuota e direzione unspecified usano i default D7.
2. **Dato** un super-nodo, **quando** il fan-out supera il cap, **allora** per
   ogni padre passano i top-N per `impact_score DESC, node_id ASC` e progress
   segnala soltanto `truncated_by_fanout_cap`.
3. **Dato** un cap totale o una profondita' insufficienti, **quando** esiste
   ulteriore lavoro autorizzato, **allora** progress segnala rispettivamente
   `truncated_by_node_cap` o `truncated_by_depth`, senza confonderli.
4. **Dato** un nodo raggiungibile per percorsi diversi, **quando** viene
   emesso, **allora** vince deterministicamente il percorso minimo e
   `path_edge_types` contiene l'intera catena.
5. **Dato** `min_impact_score` e piu' repository richiesti, **quando** si
   espande, **allora** score e repository filtrano prima del retrieval e i
   repository non possono ampliare lo scope autenticato.
6. **Dato** `include_source_text=true`, **quando** un livello e' pronto,
   **allora** i chunk autorizzati OpenSearch sono idratati in un'unica query
   batch e ordinati per indice; false non chiama OpenSearch.
7. **Dato** un percorso, **quando** il nodo viene convertito, **allora** i
   campi base Neo4j, provenance, score e `ImpactKind` seguono ADR-0024.
8. **Dato** input fuori bound, dipendenza fallita, deadline o cancellazione,
   **quando** la RPC li incontra, **allora** fallisce chiusa col codice gRPC
   specifico e non continua fetch o send.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `entry_node_id` vuoto; `max_nodes <= 0`; enum ignoto/unspecified esplicito | `InvalidArgument` prima del fetch |
| Depth, node cap, fan-out o numero repository oltre massimi server | `InvalidArgument`, nessun clamp silenzioso |
| Score NaN/Inf/fuori `[0,1]`; repository vuoto/malformato | `InvalidArgument` |
| Scope autenticato assente | `PermissionDenied`; il body non e' fallback |
| OpenSearch assente con source richiesto | `FailedPrecondition` |
| Neo4j/OpenSearch irraggiungibile | errore esplicito fail-closed |
| Context cancellato/deadline scaduta | `Canceled` / `DeadlineExceeded` |
| Entry o chunk assente ma dipendenze sane | stream/proprieta' vuoti, nessuna rivelazione |

## 5. Non-goals

- Calcolare nuovamente PageRank/betweenness durante la query.
- Predire se una modifica ipotetica cambia firma; `ImpactKind` classifica il
  meccanismo di propagazione secondo ADR-0024.
- Scrivere Neo4j/OpenSearch o modificare dati canonici.
- Dichiarare evidenza enterprise-scale senza infrastruttura reale.

## 6. Vincoli dall'ADD

- Modulo 2 §1.2–§1.3: reverse reachability, dedup e bounds espliciti; top-N
  per nodo a ogni hop.
- Modulo 2 §1.4: priorita' `impact_score` e pruning.
- Modulo 2 §1.5: propagazione tipizzata e classificazione impatto.
- Modulo 3 §2.3: security metadata server-side, fail-closed prima del
  retrieval; nessun path puo' attraversare nodi non autorizzati.
- D7: RPC streaming, source text condizionale, provenance e path spiegabile.

## 7. Test plan

- Unitari puri del BFS: caps indipendenti, percorso, dedup, ordine, probe
  profondita', cancellazione ed errore emit.
- Unitari query/validation/conversion: whitelist edge, direzione, score,
  multi-repo, enum e `ImpactKind`.
- Integrazione Neo4j: filtri prima dell'espansione, top-N per padre, direzioni,
  isolamento tenant/repository/ACL e path multi-hop.
- Integrazione OpenSearch + gRPC: idratazione batch autorizzata e codici errore.
- Buf + rigenerazione Go/Python con clean diff.

## 8. Osservabilita'

Conservare gli span dependency `eci.security.filter` per Neo4j/OpenSearch e
le metriche outcome a cardinalita' limitata. Non aggiungere node ID, repository,
tenant, source text, query Cypher, ACL o path a log/attributi.

## 9. Criteri di accettazione

- [x] I test nuovi falliscono contro l'implementazione precedente per i motivi attesi.
- [x] `go test ./internal/impactanalysis ./internal/server ./internal/hybridsearch`
- [ ] `go test -tags=integration ./internal/impactanalysis ./internal/server ./internal/hybridsearch`
- [x] `task proto:lint && task proto:breaking && task proto:gen`
- [x] `git diff --exit-code -- libs/go libs/py`
- [x] `task build && task lint && task test && task guard`
- [x] Review avversariale di auth, enum injection, bounds, cancellation e PII completata.

Il test integration e' compilato con `-tags=integration` ma non eseguito:
`task test:integration` termina prima delle suite perche' il socket Docker
Desktop configurato non esiste. Lo stato resta quindi `implemented`, non
`verified`; nessuna evidenza Neo4j/OpenSearch runtime viene dedotta dai test
unitari.

## 10. Review avversariale pre-implementazione

La query dinamica puo' interpolare soltanto simboli derivati dalla whitelist
enum; tutti i valori restano parametri. Il filtro repository del body e'
congiunto, non alternativo, allo scope metadata. Fan-out viene applicato nel
database prima che un super-nodo materializzi risultati illimitati; il probe
di profondita' riusa gli stessi filtri. L'idratazione e' successiva alla
selezione ACL-safe e applica nuovamente lo scope. Bounds massimi e context
impediscono abuso numerico o lavoro superstite alla disconnessione.

## 11. Review avversariale post-implementazione

La seconda passata ha verificato che tipo e direzione interpolati arrivino
soltanto da mappe enum chiuse; un test tenta esplicitamente una relationship
injection. Tenant/repository/ACL sono applicati sia al nodo frontiera sia a
ogni vicino prima dell'espansione e di nuovo durante l'idratazione. Una
intersezione repository vuota produce uno stream vuoto senza trasformarsi nel
significato "nessun filtro". Score non finiti e bounds protobuf eccessivi sono
rifiutati. Cancellazione/deadline preservano i codici gRPC; gli altri errori
dependency sono `Unavailable` sanitizzati. La query OpenSearch rifiuta timeout,
errori e risultati parziali invece di restituire source incompleto. Non sono
stati aggiunti log o attributi con scope, ID, path o sorgente.
