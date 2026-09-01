# SPEC-076 — Tombstone scope-safe nel sink OpenSearch
Stato: implemented
Task-tree: T7.1a · Servizio: services/sink-search · ADD: Modulo 1 §§2.2.3–2.2.4, Modulo 2 §1.4
Contratti: contracts/jsonschema/outbox-event.json, ADR-0022, ADR-0025

## 1. Obiettivo

Il sink search elimina in modo durevole e idempotente il documento OpenSearch
di una CodeChunk DELETE. La cancellazione congiunge document ID e scope
canonico in una singola delete-by-query sincrona, poi registra il marker
PostgreSQL.

## 2. Interfaccia

```go
const OutcomeDeleted Outcome
func ProcessMessage(ctx context.Context, deps Deps, topic string,
    value []byte, headers []kafka.Header) (Outcome, error)
func buildDeleteRequest(msg codeChunkTombstone) (opensearchapi.DocumentDeleteByQueryReq, error)
func validateDeleteResponse(resp *opensearchapi.DocumentDeleteByQueryResp) error
```

## 3. Comportamento

1. **Dato** UPSERT canonico, **quando** processato, **allora** conserva
   l'indicizzazione verificata esistente.
2. **Dato** DELETE valido, **quando** ID e tenant/repo/ACL combaciano,
   **allora** una delete-by-query sincrona e con refresh elimina un solo
   documento prima del marker.
3. **Dato** tombstone cross-scope, **quando** l'ID appartiene a un altro scope,
   **allora** il filtro congiunto elimina zero documenti e completa il marker.
4. **Dato** documento assente o crash post-delete/pre-marker, **quando** DELETE
   viene riconsegnato, **allora** zero match e' successo idempotente e il
   marker viene ritentato.
5. **Dato** metadata, payload o provenance malformati, **quando** processati,
   **allora** nessun accesso OpenSearch/PostgreSQL avviene.
6. **Dato** timeout, failure, conflitto o risposta parziale OpenSearch,
   **quando** DELETE avviene, **allora** nessun marker/offset viene completato.
7. **Dato** marker gia' presente, **quando** l'evento viene riconsegnato,
   **allora** il backend non viene richiamato e l'esito e' duplicate.
8. **Dato** un UPSERT vecchio dal retry topic dopo una DELETE piu' recente,
   **quando** la sequence e' sotto watermark, **allora** non ricrea il documento.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| documento assente | delete 0/0 riuscita, poi marker |
| scope label o path vuoti | invalid skipped, nessun effetto |
| event_type assente, duplicato o ignoto | invalid prima delle dipendenze |
| `timed_out`, version conflict o failures non vuote | errore, niente marker |
| total/deleted incoerenti o oltre un documento | errore, niente marker |
| deadline/cancel | errore propagato, niente marker |

## 5. Non-goals

- Eliminare tutti i chunk per entity ID.
- Eliminare Qdrant/Neo4j o invalidare cache e summary.
- Fare GET seguito da DELETE, soggetto a TOCTOU.
- Inferire scope dal documento o operation dal payload.
- Cambiare mapping, analyzer o contratto evento.

## 6. Vincoli dall'ADD

- PostgreSQL resta canonico; OpenSearch e' una vista ricostruibile.
- Modulo 1 §2.2.3–2.2.4: consumer idempotente e marker post-effetto.
- Modulo 2 §1.4: filtri tenant/repo/ACL prima dell'accesso full-text.
- ADR-0022/0025: operation autenticata ed effect-before-marker.

## 7. Test plan

- Unit HTTP/request: query bool con ID e tre label, refresh e completion
  sincrona; risposta parziale/timed-out rifiutata; metadata fail-closed.
- Integration OpenSearch+PostgreSQL: delete, cross-scope, replay assente,
  marker failure window, backend irraggiungibile e DELETE-newer/UPSERT-older.
- Go test/vet/race, integration compile e aggregate gates.

## 8. Osservabilita'

Outcome/operation chiusi; nessun ID, scope, path, testo, payload o header nei
nuovi log/error. Le metriche retry/DLQ esistenti restano il segnale operativo.

## 9. Criteri di accettazione

- [x] Test nuovi osservati rossi contro sink UPSERT-only.
- [x] `go test ./internal/consumer/...`
- [x] `go vet ./...`
- [ ] integration Docker verde e ripetuta due volte.
- [x] `task build && task lint && task test && task test:security`
- [ ] `task test:integration`
- [x] Review TOCTOU/cross-scope/replay/partial-response/cancel completata.

## 10. Review avversariale pre-implementazione

Un GET autorizzante seguito da DELETE per ID permetterebbe un cambio di
documento tra le due chiamate. La delete-by-query combina `_id`, tenant, repo e
ACL nello stesso effetto server-side. `wait_for_completion=true`, refresh e la
validazione di timeout/failure/conflitti impediscono che un risultato parziale
diventi completion marker; 0/0 rende sicuro il replay dopo crash.

## 11. Evidenza di implementazione

- Test red: `07e90b7`; failure-window integration: `861c966`;
  implementazione: `66a7f83`.
- `go test ./internal/consumer/...`, `go test -race
  ./internal/consumer/...`, `go vet ./...` e compilazione con
  `-tags=integration -run '^$'` verdi.
- Gli aggregate `task build`, `task lint`, `task test` e
  `task test:security` sono verdi. `task test:integration` fallisce
  esplicitamente al preflight per daemon Docker non raggiungibile; lo stato
  resta quindi `implemented`, non `verified`.
