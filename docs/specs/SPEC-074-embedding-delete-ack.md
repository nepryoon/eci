# SPEC-074 — Acknowledgement delete nel worker embedding
Stato: implemented
Task-tree: T7.1a · Servizio: services/embedding-worker · ADD: Modulo 1 §2.2.3–§2.2.4
Contratti: contracts/jsonschema/outbox-event.json, ADR-0025

## 1. Obiettivo

Il worker embedding distingue DELETE CodeChunk da UPSERT tramite metadata CDC
canonici. Una DELETE valida non chiama l'embedder e non ricrea righe/outbox;
registra soltanto il completamento consumer-scoped idempotente.

## 2. Interfaccia

```go
const OutcomeTombstoneAcknowledged Outcome
func ProcessMessage(ctx context.Context, deps Deps, topic string,
    value []byte, headers []kafka.Header) (Outcome, error)
```

## 3. Comportamento

1. **Dato** UPSERT con metadata canonici, **quando** viene processato, **allora**
   conserva embedding e transazione outbox esistenti.
2. **Dato** DELETE CodeChunk valido, **quando** viene processato, **allora** non
   contatta embedder e inserisce soltanto il marker `embedding-worker`.
3. **Dato** replay della stessa DELETE, **quando** viene processato, **allora**
   ritorna Duplicate senza nuove righe/outbox/chiamate.
4. **Dato** metadata assenti/duplicati/ignoti o topic errato, **quando** viene
   processato, **allora** fallisce chiuso prima delle dipendenze.
5. **Dato** tombstone senza ID/provenance valida, **quando** viene processato,
   **allora** viene scartato senza marker.
6. **Dato** PostgreSQL indisponibile durante acknowledgement, **quando** avviene
   la DELETE, **allora** ritorna errore e offset non viene committato.
7. **Dato** un vecchio CodeChunk UPSERT riconsegnato dopo una DELETE con
   sequence maggiore, **quando** viene processato, **allora** non contatta
   l'embedder e non crea CodeEmbedding/outbox.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| embedder irraggiungibile durante DELETE | nessun impatto: non viene chiamato |
| canonical embedding gia' eliminato | acknowledgement valido |
| marker gia' presente | Duplicate |
| payload contiene testo/vector extra | ignorato, mai loggato o inoltrato |

## 5. Non-goals

- Emettere un secondo CodeEmbedding DELETE: ingestion lo ha gia' emesso ACID.
- Eliminare Qdrant/OpenSearch.
- Cambiare il comportamento embedding UPSERT esistente.
- Leggere operation dal payload.

## 6. Vincoli dall'ADD

- PostgreSQL canonico e outbox unico percorso di mutazione.
- Modulo 1 §2.2.3–§2.2.4: at-least-once, dedup consumer-scoped.
- ADR-0021/0025: marker per consumer e tombstone dirette CodeEmbedding.

## 7. Test plan

- Unit: metadata invalida prima di dipendenze e outcome DELETE esplicito.
- Integration PostgreSQL con embedder spento: acknowledgement, replay, nessuna
  embedding/outbox aggiunta e failure DB.
- Compile integration, Go test/vet/race e aggregate gates.

## 8. Osservabilita'

Riusa metriche outcome; operation/outcome sono label chiuse. Log di delete non
contengono ID, provenance, testo, vettore, header o payload.

## 9. Criteri di accettazione

- [x] Test nuovi osservati rossi contro worker UPSERT-only.
- [x] `go test ./internal/consumer/...`
- [x] `go vet ./...`
- [ ] integration Docker verde e ripetuta due volte.
- [x] `task build && task lint && task test && task test:security`
- [ ] `task test:integration`
- [x] Review no-recreate/replay/privacy/offset completata.

L'integration test usa intenzionalmente un endpoint embedder irraggiungibile e
verifica zero nuove embedding/outbox, acknowledgement e replay. Il binario con
tag integration compila; l'esecuzione resta bloccata dal daemon Docker assente.

## 10. Review avversariale pre-implementazione

La DELETE CodeEmbedding canonica viene prodotta direttamente dalla transazione
ingestion: farla rigenerare qui creerebbe ordering e dual-writer ambiguo. Il
worker quindi completa soltanto la propria subscription CodeChunk. Metadata e
provenance sono validati prima del DB; nessun campo del payload autorizza scope.
