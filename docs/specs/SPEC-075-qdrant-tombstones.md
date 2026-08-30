# SPEC-075 — Tombstone scope-safe nel sink Qdrant
Stato: implemented
Task-tree: T7.1a · Servizio: services/sink-vector · ADD: Modulo 1 §2.2.3–§2.2.4, Modulo 2 §1.3
Contratti: contracts/jsonschema/outbox-event.json, ADR-0025

## 1. Obiettivo

Il sink vector elimina in modo durevole e idempotente il punto Qdrant di una
CodeEmbedding DELETE. L'operazione usa insieme point ID deterministico e scope
canonico, attende `Completed`, poi registra il marker PostgreSQL.

## 2. Interfaccia

```go
const OutcomeDeleted Outcome
func ProcessMessage(ctx context.Context, deps Deps, topic string,
    value []byte, headers []kafka.Header) (Outcome, error)
func buildDeleteRequest(msg codeEmbeddingTombstone) *qdrant.DeletePoints
```

## 3. Comportamento

1. **Dato** UPSERT canonico, **quando** processato, **allora** conserva l'upsert
   Qdrant verificato.
2. **Dato** DELETE valido, **quando** punto ID e tenant/repo/ACL combaciano,
   **allora** Delete(wait=true) completa prima del marker.
3. **Dato** tombstone cross-scope, **quando** l'ID esiste in altro scope,
   **allora** il filtro congiunto non elimina il punto.
4. **Dato** replay o punto assente, **quando** DELETE viene riconsegnato,
   **allora** e' idempotente e il marker puo' completarsi.
5. **Dato** metadata/provenance/payload malformati, **quando** processati,
   **allora** nessun Qdrant/marker viene toccato.
6. **Dato** Qdrant errore o status non Completed, **quando** DELETE avviene,
   **allora** nessun marker/offset viene completato.
7. **Dato** marker fallito dopo delete, **quando** replay avviene, **allora**
   delete assente no-op e marker viene ritentato.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| point ID assente | delete Completed/no-op, poi marker |
| scope label vuota | invalid skipped, nessun effetto |
| event_type duplicato/ignoto | invalid prima delle dipendenze |
| deadline/cancel Qdrant | errore, niente marker |
| ACK non Completed | errore, niente marker |

## 5. Non-goals

- Eliminare tutti i punti per entity ID: ogni tombstone embedding e' specifica.
- Eliminare OpenSearch/Neo4j o cache.
- Inferire scope dal point esistente o operation dal payload.
- Cambiare collection/vector schema.

## 6. Vincoli dall'ADD

- Qdrant e' vista ricostruibile; PostgreSQL resta canonico.
- Modulo 1 §2.2.3–§2.2.4: idempotenza e marker post-effetto.
- Modulo 2 §1.3: filtri ACL prima dell'accesso vettoriale.
- ADR-0022/0025: wait durable e tombstone canonicali.

## 7. Test plan

- Unit: metadata fail-closed e DeletePoints con ID+tre label, wait=true.
- Integration Qdrant+PostgreSQL: delete, cross-scope, replay, assente,
  Qdrant failure e marker failure window.
- Go test/vet/race, integration compile e aggregate gates.

## 8. Osservabilita'

Outcome/operation chiusi; nessun ID, scope, payload, vettore o header nei nuovi
log/error. Le metriche esistenti distinguono processed/retry/DLQ.

## 9. Criteri di accettazione

- [x] Test nuovi osservati rossi contro sink UPSERT-only.
- [x] `go test ./internal/consumer/...`
- [x] `go vet ./...`
- [ ] integration Docker verde e ripetuta due volte.
- [x] `task build && task lint && task test && task test:security`
- [ ] `task test:integration`
- [x] Review cross-scope/replay/wait/marker/cancel completata.

## 10. Review avversariale pre-implementazione

Il point ID da solo identifica globalmente l'embedding ma non costituisce
autorizzazione. Il filtro DELETE combina `has_id` con tenant, repo e ACL
indicizzati, evitando confused-deputy cross-scope. `wait=true` e status
Completed mantengono effect-before-marker anche nella failure window.

## 11. Evidenza di implementazione

- Test red iniziale: `8bf7435`; failure-window integration: `2f2a328`;
  implementazione: `dc6dcbe`.
- `go test ./internal/consumer/...`, `go test -race
  ./internal/consumer/...`, `go vet ./...` e compilazione con
  `-tags=integration -run '^$'` verdi.
- Gli aggregate `task build`, `task lint`, `task test` e
  `task test:security` sono verdi. `task test:integration` termina
  esplicitamente prima delle suite per daemon Docker non raggiungibile;
  pertanto lo stato non viene promosso a `verified`.
