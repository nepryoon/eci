# SPEC-031 — CodeEmbedding: aggiungere entity_id al payload (fix a SPEC-030)
Stato: verified
Task-tree: correzione a SPEC-030, prerequisito per T3.1 (scoperta e concordata in chat) · Servizio: services/embedding-worker (Go, modifica minima) · ADD: Modulo 1 §1.6.3 (payload Qdrant: node_id, domain, provenance)

## 1. Obiettivo
Il payload outbox di `CodeEmbedding` (SPEC-030) contiene `{id, chunk_id, vector, model_id, embedding_dim}` — manca `entity_id`, necessario a Qdrant (T3.1) per il payload `(node_id, domain, provenance)` che l'ADD richiede. Nessun sink di questo progetto interroga mai Postgres all'indietro (principio tenuto da `sink-graph`, T1.3): l'evento deve portare già tutto ciò che serve. Il messaggio `CodeChunk` in ingresso che `embedding-worker` consuma include già `entity_id` (verificato nel messaggio Kafka reale ispezionato durante la chiusura di SPEC-029) — questa SPEC è solo propagazione di un campo già disponibile, nessuna nuova query.

## 2. Interfaccia
`services/embedding-worker/internal/consumer/consumer.go`: dove il payload outbox di `CodeEmbedding` viene costruito, aggiunta `entity_id` letta direttamente dal messaggio `CodeChunk` in ingresso già deserializzato (lo stesso valore già usato per popolare `code_embedding.chunk_id` indirettamente tramite il join a `code_chunk` — qui si tratta del campo `entity_id` del messaggio Kafka in ingresso, non di una nuova colonna in `code_embedding`).

## 3. Comportamento (scenari)
1. **Dato** un messaggio `CodeChunk` reale con un `entity_id` noto, **quando** il worker lo processa, **allora** il payload outbox di `CodeEmbedding` risultante include quello stesso `entity_id`, invariato.

## 4. Errori & edge case
Nessuno oltre a quanto già coperto da SPEC-030 — questa è un'estensione additiva del payload, non una modifica di comportamento.

## 5. Non-goals
Nessuna modifica allo schema `code_embedding` (l'aggiunta è solo nel payload outbox, non in una colonna Postgres — `entity_id` è già derivabile da `code_chunk.entity_id` via il join su `chunk_id` per chi legge da Postgres; il payload Kafka lo include solo per evitare a T3.1 di dover fare quel join lui stesso). Nessuna modifica a T3.1 stesso (SPEC successiva).

## 6. Vincoli dall'ADD
Modulo 1 §1.6.3: payload Qdrant richiede `node_id` — questa SPEC è il prerequisito diretto per soddisfarlo.

## 7. Test plan
Estensione del test di integrazione esistente di SPEC-030 (stesso principio, Kafka+Postgres reali) — verifica che il payload risultante includa `entity_id`, non un nuovo scenario isolato.

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenario 1 verificato con evidenza diretta (payload outbox reale ispezionato, non solo il codice).
- [x] Nessuna regressione sui test esistenti di SPEC-030.

## 10. Deviazioni dall'implementazione

1. **Premessa verificata prima di modificare (§0 del task)**: `entity_id`
   è confermato presente nel messaggio `CodeChunk` reale prodotto da
   `persist.rs` (`services/ingestion/src/persist.rs`, SPEC-029) — ma NON
   era ancora deserializzato lato Go: `codeChunkPayload` in
   `consumer.go` mappava solo `id`/`text`, quindi `entity_id` veniva
   scartato silenziosamente da `json.Unmarshal` (campo non mappato, non
   un errore). Il fix è quindi esattamente quello descritto da §1/§2:
   propagazione di un campo già sul wire, nessuna nuova query — confermato
   empiricamente, non presunto.
2. **Test esteso, non isolato**: come richiesto da §7, l'estensione ha
   modificato `scenario3OutboxRowWithVectorIncluded` (il test esistente di
   SPEC-030 che già ispeziona il payload outbox reale di `CodeEmbedding`)
   aggiungendo l'asserzione su `entity_id`, invece di introdurre una nuova
   funzione di scenario isolata — coerente con "non un nuovo scenario
   isolato" di §7. Verificato il fallimento (`payload['entity_id'] = <nil>`)
   prima del fix, verde dopo.
3. Nessuna modifica allo schema `code_embedding` né a `main.go`/
   `embedclient`: unica modifica di produzione in
   `services/embedding-worker/internal/consumer/consumer.go` (campo
   `EntityID` su `codeChunkPayload`, propagato fino al payload outbox),
   come dichiarato da §2/§5.
