# SPEC-070 — Canonical file delete e tombstone outbox
Stato: implemented
Task-tree: T7.1a · Servizio: services/ingestion · ADD: Modulo 1 §1.6.4–§1.6.6, §2.2
Contratti: contracts/jsonschema/ingestion-file-command.json, contracts/sql/migrations/0008_ingestion_file_delete.up.sql

## 1. Obiettivo

Il worker ingestion accetta una DELETE autenticata sullo stesso stream ordinato
per file e rimuove l'intero file canonico senza contattare object storage o
parser. PostgreSQL registra atomicamente tombstone outbox per ogni vista e una
receipt idempotente, rendendo possibile la successiva pulizia dei sink.

## 2. Interfaccia

```json
{
  "schema_version": "1",
  "operation": "DELETE",
  "command_id": "uuid",
  "commit_sha": "40 lower hex",
  "path": "relative/supported.go"
}
```

```rust
pub enum FileOperation { Upsert, Delete }
pub fn persist_ingestion_delete_command(
    client: &mut postgres::Client,
    scope: &AuthenticatedCommitScope,
    command: &IngestionFileCommand,
) -> Result<CommandOutcome, PersistError>;
```

Tombstone payload:

```json
{"id":"aggregate-id","entity_id":"optional","rel_type":"optional",
 "from_id":"optional","to_id":"optional",
 "provenance":{"tenant_id":"...","repo":"...","acl_group":"...","path":"...","commit_sha":"..."}}
```

## 3. Comportamento

1. **Dato** un payload v1 storico senza operation, **quando** viene validato,
   **allora** resta UPSERT e richiede digest/size come prima.
2. **Dato** operation DELETE, **quando** il comando contiene campi sorgente o
   path/scope non validi, **allora** viene rifiutato prima di DB/MinIO/parser.
3. **Dato** un file con nodi, relazioni entranti/uscenti, chunk ed embedding,
   **quando** la DELETE e' applicata, **allora** ogni entita' riceve un outbox
   DELETE e le righe sono rimosse in ordine FK nella stessa transazione.
4. **Dato** un path omonimo in un altro tenant o repository, **quando** applico
   DELETE, **allora** nessuna sua riga o tombstone viene toccata/prodotta.
5. **Dato** un command ID gia' completato con fingerprint uguale, **quando**
   viene riconsegnato, **allora** e' Duplicate senza nuove delete/outbox; con
   fingerprint diverso e' conflitto permanente.
6. **Dato** un file gia' assente, **quando** arriva una nuova DELETE valida,
   **allora** viene registrata una receipt Applied con zero tombstone.
7. **Dato** un errore dopo almeno un tombstone/delete ma prima della receipt,
   **quando** la transazione termina, **allora** tutto e' rollbackato.
8. **Dato** DELETE nel runtime, **quando** viene processata, **allora** non
   avviene alcuna chiamata MinIO/parser e l'offset e' committato solo dopo il
   commit DB; errori transient non committano.
9. **Dato** UPSERT e DELETE con command ID distinti sullo stesso file,
   **quando** si sovrappongono, **allora** il lock scope/path li serializza e
   ogni tombstone DELETE ha `event_sequence` maggiore degli UPSERT precedenti.
10. **Dato** un secondo UPSERT dello stesso file dopo che esistono embedding e
    viste per i chunk precedenti, **quando** sostituisce relazioni e chunk,
    **allora** emette atomicamente tombstone CodeRelation, CodeEmbedding e
    CodeChunk prima dei nuovi UPSERT, elimina i dipendenti in ordine FK e non
    lascia UUID storici ricercabili.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| operation assente con campi UPSERT validi | compatibilita' legacy UPSERT |
| DELETE con source digest/size | payload permanente invalido, DLQ sanitizzata |
| DELETE senza righe correnti | Applied, receipt atomica, zero outbox |
| relazione da altro file verso nodo eliminato | tombstone relazione e delete |
| receipt schema/constraint non disponibile | retry PostgreSQL, nessun offset |
| errore DB/lock/statement timeout | rollback e retry, nessun marker parziale |
| comando DELETE sul topic con key errata | DLQ `invalid_message_key` |
| rollback verso schema UPSERT-only | elimina receipt DELETE; non inventa digest e non ripristina dati |

## 5. Non-goals

- Applicare i tombstone a Neo4j/Qdrant/OpenSearch (SPEC successive).
- Invalidare cache/summary o completare reconciliation.
- Eliminare il blob MinIO sorgente, che resta durevole/immutabile.
- Rendere atomico un commit multi-file.

## 6. Vincoli dall'ADD

- Modulo 1 §1.6.4–§1.6.6: delta, lineage e rimozione controllata.
- Modulo 1 §2.2.1–§2.2.4: entita' canonica e outbox nella stessa transazione,
  delivery at-least-once e sink idempotenti.
- Modulo 3 §2.3: scope soltanto da metadata autenticati e fail-closed.
- ADR-0011: identita' persistite tenant/repository-owned.
- ADR-0023/0025: command boundary durevole, receipt e tombstone.

## 7. Test plan

- Unit Rust: schema/serde legacy+DELETE, validation, key/fingerprint e ramo
  runtime senza source fetch.
- Integration PostgreSQL: grafo completo e cross-scope, tombstone payload,
  FK order, duplicate/conflict/absent, rollback forzato; suite due volte sullo
  stesso stato per replay.
- Regressione re-ingestion con embedding reale gia' referenziante il vecchio
  chunk: successo transazionale e un tombstone per embedding/chunk/relazione.
- Migration up/down e JSON Schema fixture/codegen consistency.
- Concurrency PostgreSQL con failpoint: DELETE osservata in attesa sul lock
  same-file e boundary `max(UPSERT sequence) < min(DELETE sequence)`.
- Security: payload/log/metric non contengono path, scope o source values.

## 8. Osservabilita'

Riusa `ingestion.command.persist` con attributi chiusi
`ingestion.operation=delete`, `ingestion.outcome=applied|duplicate|failed` e
metriche command outcome esistenti. Non registrare path, commit, ID, tenant,
repository, ACL o tombstone payload.

## 9. Criteri di accettazione

- [x] Test nuovi rossi contro il runtime UPSERT-only.
- [x] `cargo test --manifest-path services/ingestion/Cargo.toml`
- [ ] test PostgreSQL `persist_ingestion_delete_command` due volte sullo stesso stato.
- [x] `task schema:gen` e output clean.
- [x] `task build && task lint && task test && task guard`
- [ ] `task test:integration` verde con Docker.
- [x] Review avversariale scope/replay/rollback/offset/log completata.

L'evidenza CPU e di compilazione e' verde al commit di implementazione. I test
PostgreSQL coprono scope omonimo cross-tenant, relazioni entranti, quattro tipi
di tombstone, replay, assenza e rollback tramite failpoint; sono compilati e
inclusi in `task test:integration`, ma non possono essere dichiarati eseguiti
senza il daemon Docker richiesto dalla suite.

## 10. Review avversariale pre-implementazione

DELETE conserva la stessa partition key di UPSERT e non introduce URL, bucket
o key. Il lookup non puo' usare soltanto path: richiede i tre label autenticati
nella provenance. I payload tombstone vengono costruiti prima delle delete ma
diventano visibili soltanto allo stesso commit. Relazioni entranti sono incluse
per evitare FK e proiezioni orfane. Receipt e mutazioni condividono transazione;
nessun consumer viene marcato qui e nessun write diretto a Kafka e' aggiunto.
