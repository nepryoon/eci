# SPEC-003 — JSON Schema (D2): codegen Pydantic/Go + schema evento outbox
Stato: draft
Task-tree: T0.3 · Servizio: contracts/jsonschema (genera in libs/py, libs/go) · ADD: Modulo 1 — Deliverable D2, §2.2.2 (outbox)
Contratti: contracts/jsonschema/hybrid-graph.json (già committato, Step 2 — NON modificarlo in questo task)

## 1. Obiettivo
Generare i tipi Pydantic (Python) dal JSON Schema D2 (`CodeNode`/`CodeRelation` con le estensioni `code`/`doc`/`legal`), scrivere lo struct Go equivalente (validato a runtime, dato che Go non ha discriminated union native), e definire come nuovo artefatto lo schema dell'**envelope evento outbox→Kafka**, coerente con l'INSERT su `outbox` del Modulo 1 §2.2.1.

## 2. Interfaccia

**Codegen Python:** `task schema:gen` esegue `datamodel-code-generator` su `contracts/jsonschema/hybrid-graph.json` → output `libs/py/eci_core/models.py`. Deve usare `--input-file-type jsonschema --output-model-type pydantic_v2.BaseModel` e mappare il discriminatore `domain` con `oneOf`/`if-then`/`const` su un **discriminated union Pydantic v2** (`Field(discriminator=...)`) se il generator lo supporta nativamente; altrimenti generare i tre modelli (`CodeNodeCode`, `CodeNodeDoc`, `CodeNodeLegal`) più una funzione `parse_code_node(payload: dict) -> CodeNode` che dispaccia manualmente su `payload["domain"]`.

**Go (manuale, non generato):** `libs/go/eci/models/codenode.go` con struct `CodeNode` (campi comuni + `Ext json.RawMessage`) e funzione `func (n *CodeNode) ParseExt() (any, error)` che, in base a `n.Domain`, fa `json.Unmarshal` di `Ext` nel tipo concreto (`CodeExtension`/`DocExtension`/`LegalExtension`). Motivazione esplicita: Go non ha discriminated union native, quindi qui non si genera da schema ma si scrive a mano con test di validazione contro esempi del JSON Schema.

**Nuovo schema evento outbox** — `contracts/jsonschema/outbox-event.json` (Draft-07, da scrivere in questo task, non presente nell'ADD con questo livello di dettaglio — deriva da Modulo 1 §2.2.1/§2.2.2):
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Outbox Event Envelope",
  "type": "object",
  "required": ["id", "aggregate_type", "aggregate_id", "event_type", "payload", "created_at"],
  "properties": {
    "id": { "type": "string", "format": "uuid" },
    "aggregate_type": { "type": "string", "enum": ["CodeNode", "CodeRelation"] },
    "aggregate_id": { "type": "string" },
    "event_type": { "type": "string", "enum": ["UPSERT", "DELETE"] },
    "payload": { "type": "object" },
    "created_at": { "type": "string", "format": "date-time" },
    "trace_id": { "type": ["string", "null"] }
  },
  "additionalProperties": false
}
```
(`trace_id` opzionale, previsto per coerenza con la propagazione OTel del Modulo 4 §3.1 — non usato finché l'observability non è implementata, ma il campo va riservato ora per evitare una migration futura dell'envelope.) Genera anche il modello Pydantic corrispondente (`OutboxEvent`) e lo struct Go (`OutboxEvent`).

## 3. Comportamento (scenari)

1. **Dato** `hybrid-graph.json`, **quando** eseguo `task schema:gen`, **allora** `libs/py/eci_core/models.py` viene generato senza errori e importabile.
2. **Dato** un payload JSON valido con `domain="code"` e `ext.node_type="Method"`, **quando** lo valido col modello Pydantic generato, **allora** la validazione passa e il campo `ext.symbol_id` è accessibile tipizzato.
3. **Dato** lo stesso payload ma con `domain="legal"` mentre `ext` contiene campi di `codeExtension` (es. `symbol_id`), **quando** lo valido, **allora** la validazione fallisce (il discriminatore rifiuta l'estensione non coerente col dominio).
4. **Dato** il payload di scenario 2, **quando** chiamo `ParseExt()` sullo struct Go equivalente, **allora** ottengo un valore del tipo `CodeExtension` con `NodeType == "Method"`.
5. **Dato** un payload di evento outbox con `aggregate_type="CodeNode"`, `event_type="UPSERT"`, **quando** lo valido contro `outbox-event.json`, **allora** passa; se manca `aggregate_id` la validazione fallisce con l'errore di campo mancante.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `datamodel-code-generator` non gestisce nativamente `if-then`/`const` come discriminated union | fallback esplicito ai 3 modelli + funzione dispatcher manuale (documentato sopra) — non forzare un pattern che il tool non supporta |
| Payload con `domain` fuori dall'enum (`"finance"`) | validazione fallisce con errore di enum, sia Pydantic che Go (check esplicito prima del dispatch) |
| `ext` assente per un `CodeNode` di dominio `code` | fallisce (il campo `ext` è richiesto dallo schema quando il discriminatore lo implica) |

## 5. Non-goals
Non implementare Debezium/EventRouter (arriva in Fase 2-3): qui solo lo schema dell'evento e i tipi. Non generare client Kafka.

## 6. Vincoli dall'ADD
Fedeltà 1:1 a D2 per `CodeNode`/`CodeRelation`. L'envelope evento riprende esattamente i campi dell'`INSERT INTO outbox` del Modulo 1 §2.2.1 (`id, aggregate_type, aggregate_id, event_type, payload, created_at`) — nessun campo in più salvo `trace_id` (giustificato sopra e da annotare come tale nel commit).

## 7. Test plan
- Python: `pytest` con fixture di payload validi/invalidi per ciascun dominio (code/doc/legal) e per l'evento outbox; verifica che la validazione fallisca esattamente sui casi attesi (scenario 3).
- Go: test table-driven su `ParseExt()` con gli stessi fixture (condivisi via file JSON in `tests/fixtures/`, non duplicati per linguaggio).
- Round-trip: serializza un modello Pydantic a JSON e rivalida contro lo schema originale con una libreria di JSON Schema validation (es. `jsonschema`) per garantire che il modello generato non abbia deviato dallo schema sorgente.

## 8. Osservabilità
N/A per questo task (tipi e validazione, non runtime).

## 9. Criteri di accettazione
- [ ] `task schema:gen` produce `models.py` importabile senza errori.
- [ ] Scenario 2 e 3 (validazione discriminata) verdi in Python.
- [ ] `ParseExt()` Go verde sugli stessi fixture (scenario 4).
- [ ] `outbox-event.json` creato, validato dal test Python e Go (scenario 5).
- [ ] I fixture di test sono condivisi (un solo set in `tests/fixtures/`, non duplicati).
