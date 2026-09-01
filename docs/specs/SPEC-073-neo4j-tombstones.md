# SPEC-073 — Tombstone idempotenti nel sink Neo4j
Stato: implemented
Task-tree: T7.1a · Servizio: services/sink-graph · ADD: Modulo 1 §2.2.3–§2.2.4, Modulo 2 §1.2
Contratti: contracts/jsonschema/outbox-event.json, contracts/cypher/schema.cypher

## 1. Obiettivo

Il sink graph applica DELETE canoniche a nodi e relazioni Neo4j, mantenendo
idempotenza, marker-after-effect e invalidazione GDS. Ogni messaggio passa prima
dal parser metadata condiviso e nessun operation/header ambiguo produce effetti.

## 2. Interfaccia

```go
func ProcessMessage(ctx context.Context, deps Deps, topic string,
    value []byte, headers []kafka.Header) (Outcome, error)
```

Tombstone nodo: `{"id":"...","ext":{"node_type":"..."},"provenance":{...}}`.
Tombstone relazione: `{"id":"...","rel_type":"CALLS","from_id":"...","to_id":"...","provenance":{...}}`.

## 3. Comportamento

1. **Dato** un UPSERT con metadata canonici, **quando** viene processato,
   **allora** conserva il MERGE verificato esistente.
2. **Dato** un DELETE CodeRelation valido, **quando** l'arco esiste, **allora**
   elimina l'arco whitelisted e incrementa una volta le generation degli scope
   endpoint interessati prima del marker.
3. **Dato** un DELETE CodeNode con scope uguale al nodo proiettato, **quando**
   viene processato, **allora** incrementa la partition generation e applica
   `DETACH DELETE` prima del marker PostgreSQL.
4. **Dato** un tombstone riconsegnato o arrivato dopo la delete del nodo,
   **quando** viene processato, **allora** e' no-op esterno idempotente ma viene
   completato il marker senza incremento generation duplicato.
5. **Dato** un tombstone node con scope diverso, rel type ignoto o payload
   incompleto, **quando** viene processato, **allora** nessun effetto/marker.
6. **Dato** metadata assenti, duplicati o operation ignota, **quando** viene
   processato, **allora** fallisce chiuso prima di PostgreSQL e Neo4j.
7. **Dato** Neo4j fallisce, **quando** la delete ritorna errore, **allora** il
   marker non esiste e la redelivery puo' riprovare.
8. **Dato** marker PostgreSQL fallisce dopo l'effetto, **quando** avviene replay,
   **allora** la delete esterna no-op non incrementa di nuovo generation e il
   marker viene poi scritto.
9. **Dato** un vecchio UPSERT dal retry topic dopo una DELETE completata,
   **quando** la sua sequence non supera il watermark CodeNode/CodeRelation,
   **allora** viene marcato processed senza MERGE o modifica generation.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| nodo gia' assente | successo idempotente, marker post-effetto |
| relazione gia' assente | successo idempotente, marker post-effetto |
| node tombstone scope mismatch | invalid skipped, nessun marker |
| rel type fuori whitelist | invalid skipped, nessuna query interpolata |
| header invalido | invalid skipped, nessuna dipendenza contattata |
| Neo4j timeout/cancel | errore, niente marker/offset |

## 5. Non-goals

- Eliminare Qdrant/OpenSearch o invalidare cache/summary.
- Usare l'UUID SQL relazione come property Neo4j retroattiva.
- Inferire operation dalla forma del payload.
- Modificare il contratto pubblico.

## 6. Vincoli dall'ADD

- PostgreSQL resta canonico; Neo4j e' una vista ricostruibile.
- Modulo 1 §2.2.3–§2.2.4: at-least-once, idempotenza, marker post-effetto.
- Modulo 2 §1.2: reachability/GDS non usa una projection stale inosservata.
- ADR-0015/0022/0025: generation, lock order e tombstone canoniche.

## 7. Test plan

- Unit: parser metadata prima delle dipendenze; query delete whitelist/scope,
  assenza di interpolazione e generation condizionale.
- Integration Neo4j+PostgreSQL: relazione e nodo, scope mismatch, replay,
  marker failure, dependency failure e DELETE-newer/UPSERT-older.
- `go test`, vet, race CPU dove possibile; integration via aggregate Docker.

## 8. Osservabilita'

Riusa outcome metric esistente. Log e errori non includono payload, path, scope
o header; operation e outcome sono valori chiusi. Nessun tombstone viene loggato.

## 9. Criteri di accettazione

- [x] Test nuovi osservati rossi contro consumer UPSERT-only.
- [x] `go test ./internal/consumer/...`
- [x] `go vet ./...`
- [ ] integration test compilato e verde con Docker, ripetuto due volte.
- [x] `task build && task lint && task test && task test:security`
- [ ] `task test:integration`
- [x] Review scope/replay/marker/lock/injection/cancel completata.

Il binario integration con i nuovi scenari compila con tag `integration`. La
suite reale Neo4j/PostgreSQL resta non eseguita per assenza del daemon Docker;
la SPEC non viene quindi promossa a `verified`.

## 10. Review avversariale pre-implementazione

Node delete confronta simultaneamente ID e provenance con le property della
vista, quindi un tombstone cross-scope non elimina un omonimo. Relation type e'
interpolato soltanto dopo whitelist; endpoint sono parametri. Generation cambia
solo quando esiste realmente un'entita' eliminata, rendendo sicuro il replay
dopo marker failure. `DETACH DELETE` gestisce l'ordering indipendente dei topic:
un tombstone relazione successivo resta un no-op valido.
