# SPEC-033 — sink-vector: CodeEmbedding → Qdrant (T3.1)
Stato: verified
Task-tree: T3.1 (primo task di Fase 3, ora con tutti i prerequisiti chiusi — SPEC-029/030/031/032) · Nuovo servizio: services/sink-vector (Go, scaffold vuoto da SPEC-001) · ADD: Modulo 1 §1.6.3 (payload Qdrant)
Contratti: nessuno sotto contracts/ (nessun proto/schema nuovo — Qdrant non è descritto da un JSON Schema/proto in questo progetto)

## 1. Obiettivo
Consumare `outbox.event.CodeEmbedding` (ora completo di `node_id`/`domain`(implicito)/`provenance`, SPEC-029→032) e scrivere ciascun embedding come punto Qdrant — stesso pattern Kafka-consumer+dedup già stabilito da `sink-graph` (T1.3) ed `embedding-worker` (T3.0 informale), applicato per la prima volta a un downstream diverso da Postgres/Neo4j.

## 2. Interfaccia

**Client**: `github.com/qdrant/go-client` (ufficiale — verificato, non `henomis/qdrant-go`, un client non ufficiale con lo stesso ambito che compare nelle stesse ricerche, da non confondere). gRPC, porta 6334 — **da verificare esplicitamente durante l'implementazione** se già esposta in `deploy/compose/docker-compose.yml` (Qdrant è nello stack dal 2026-08-04/SPEC-006, ma solo la porta 6333/REST è stata verificata finora); se manca, aggiungerla è una modifica dichiarata e minima a un file condiviso, stesso principio già seguito per Redis/T2.3 e il profilo `gpu`/T2.4.

**Setup collection, idempotente**: al via del servizio, verifica se la collection (nome dichiarato: `code_embeddings`) esiste già; se no, la crea con `VectorsConfig{Size: 1536, Distance: Cosine}` — dimensione che combacia con `jina-code-embeddings-1.5b` (verificata in T2.4), `Cosine` come metrica di distanza standard per embedding testuali/di codice, nessuna giustificazione più specifica disponibile dall'ADD. Nessuna migrazione nel senso Postgres — Qdrant non ne ha, l'idempotenza è ottenuta a livello applicativo.

**ID punto Qdrant**: i nostri id sono SHA-256 esadecimali (stringhe), non numerici — **da verificare esplicitamente** se il client espone un costruttore per id stringa/UUID (parallelo a `qdrant.NewIDNum` per id numerici, visto solo quello negli esempi ufficiali consultati) prima di procedere; se non esiste, ricadere su un UUID deterministico derivato dall'id esistente (es. UUIDv5 con namespace fisso) — decisione da annotare esplicitamente in implementazione, non presunta qui.

**Consumer**: stesso scheletro di `sink-graph`/`embedding-worker` — consuma `outbox.event.CodeEmbedding`, dedup via `processed_events` (stessa tabella condivisa, stesso principio), per ciascun messaggio upsert di UN punto Qdrant con payload `{node_id: <entity_id del messaggio>, domain: "code", provenance: <provenance del messaggio, propagato invariato>}` — `domain` hardcoded qui (mai propagato attraverso la catena, per decisione già presa prima di SPEC-032: è sempre stata la costante `'code'`).

## 3. Comportamento (scenari)

1. **Dato** un messaggio `CodeEmbedding` reale (prodotto dalla catena SPEC-029→032), **quando** il servizio lo consuma, **allora** un punto esiste in Qdrant con lo stesso vettore, e payload `node_id`/`domain`/`provenance` corretti.
2. **Dato** lo stesso messaggio consumato una seconda volta (ridelivery), **quando** il servizio lo riprocessa, **allora** nessun nuovo punto duplicato — dedup via `processed_events`, stesso principio già stabilito.
3. **Dato** il servizio avviato per la prima volta contro un'istanza Qdrant senza la collection `code_embeddings`, **quando** parte, **allora** la crea con `Size: 1536`/`Distance: Cosine` prima di iniziare a consumare.
4. **Dato** il servizio riavviato con la collection già esistente, **quando** parte, **allora** non tenta di ricrearla (nessun errore "collection già esistente" che blocchi l'avvio).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Qdrant irraggiungibile all'avvio (setup collection) | Errore esplicito, il servizio non parte silenziosamente in uno stato inconsistente |
| Un messaggio `CodeEmbedding` senza `provenance` (caso limite già ammesso da SPEC-032 §4 — entità non nel batch al momento della scrittura) | Punto Qdrant scritto comunque, senza la chiave `provenance` nel payload — nessun crash, nessun valore fabbricato |

## 5. Non-goals
Nessuna query di ricerca/similarità (sola scrittura in questa SPEC — la ricerca è Fase 4, retrieval avanzato). Nessun DLQ/retry sofisticato/metriche (T3.3, task successivo dichiarato dal task-tree originale). Nessuna gestione di aggiornamento/cancellazione di punti quando un'entità viene rimossa (stesso limite pre-esistente già noto per `code_node` fin da T1.2 — non nuovo qui).

## 6. Vincoli dall'ADD
Modulo 1 §1.6.3: payload Qdrant `(node_id, domain, provenance)` — ora finalmente completo grazie a SPEC-029→032, questa SPEC lo scrive per la prima volta davvero su Qdrant.

## 7. Test plan
Test di integrazione con Kafka+Postgres+Qdrant reali (testcontainers per tutti e tre, stesso principio già stabilito ovunque in questo progetto) — verifica diretta del punto scritto in Qdrant (query di lettura del punto per id), non solo che il consumer non sia andato in errore.

## 8. Osservabilità
Stessa fondazione OTel già stabilita per i servizi Go del progetto.

## 9. Criteri di accettazione
- [x] Scenari 1-4 verificati con evidenza diretta (punto Qdrant reale ispezionato via query, non presunto dall'assenza di errori).
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Porta gRPC 6334 verificata esplicitamente contro `docker-compose.yml` — se assente, aggiunta e verificata con `task up` reale.
- [x] Tipo di ID Qdrant (stringa nativa vs UUID derivato) verificato empiricamente, non presunto — riportato esplicitamente nel report qualunque sia l'esito.

## 10. Deviazioni dall'implementazione

1. **Porta gRPC 6334: già presente, nessuna modifica a `docker-compose.yml`**
   — verificato per primo (prima di scrivere codice), come richiesto: il
   blocco `qdrant:` in `deploy/compose/docker-compose.yml` espone già
   `"6334:6334"` insieme a `"6333:6333"` (probabilmente da SPEC-006
   stessa, mai smentito). Confermato anche runtime: lo stack dev già
   attivo aveva `eci-dev-qdrant-1` con `0.0.0.0:6333-6334->6333-6334/tcp`
   pubblicato. Nessuna riga toccata in questo file.
2. **ID punto Qdrant — entrambe le "due strade" di §2, non una sola**:
   verificato empiricamente con un probe reale contro Qdrant (throwaway,
   rimosso a fine verifica) che `qdrant.NewID`/`NewIDUUID` ESISTONO nel
   client ufficiale (`github.com/qdrant/go-client` v1.19.0,
   `qdrant/oneof_factory.go:319`) — quindi la "prima strada" di §2 non è
   assente. Il problema è un livello più in basso: quei costruttori
   accettano una stringa arbitraria lato client, ma il SERVER Qdrant
   valida che sia un UUID RFC4122 vero — un id SHA-256 esadecimale grezzo
   (64 caratteri, formato di `code_node.id`/`entity_id` in questo
   progetto) viene rifiutato con `rpc error: ... Unable to parse UUID`.
   Verificata quindi anche la "seconda strada" (fallback dichiarato):
   `uuid.NewSHA1(namespace_fisso, []byte(id))` (UUIDv5, `google/uuid`,
   già dipendenza del progetto) produce un UUID valido, accettato da
   Qdrant, deterministico (stesso input → stesso output, verificato) e il
   punto risultante è recuperabile con `Get` per quell'id. Implementato
   `consumer.DerivePointID`, che applica SEMPRE la derivazione UUIDv5
   (non condizionalmente "solo se non è già un UUID") — un solo percorso
   di codice, corretto sia che l'id sorgente sia già UUID-formattato sia
   che non lo sia.
3. **Sorgente del point id: `code_embedding.id` (campo `id` del messaggio
   CodeEmbedding), non `entity_id`** — non fissato esplicitamente da §2
   ("i nostri id sono SHA-256 esadecimali" si riferisce a `entity_id`,
   l'unico campo del messaggio con quel formato; `id`/`chunk_id` sono UUID
   Postgres già validi). Scelto `id` (l'identità specifica di QUESTO
   embedding/chunk) e non `entity_id` (l'identità dell'entità sorgente)
   perché **un'entità può produrre PIÙ chunk/embedding** (SPEC-021/029,
   budget di chunking): usare `entity_id` come point id avrebbe fatto sì
   che l'upsert di un chunk successivo della STESSA entità sovrascrivesse
   silenziosamente il punto Qdrant del chunk precedente — perdita di dati
   reale, non un dettaglio di formato. `entity_id` resta comunque nel
   payload come `node_id` (esattamente come richiesto da §2/dall'ADD),
   solo non come identità del punto.
4. **Normalizzazione lato server per collection `Distance_Cosine`** —
   scoperta durante la scrittura del test (SPEC-033 §7, non presunta):
   Qdrant memorizza i vettori di una collection `Distance_Cosine` già
   normalizzati a norma 1 (verificato: il vettore letto via `Get` NON
   combacia byte-per-byte con quello scritto, ma `letto[i] == scritto[i] /
   ‖scritto‖` entro la precisione float32). Non è un bug
   dell'implementazione: il test (scenario 1) verifica quindi la
   DIREZIONE del vettore (cosine similarity ≳ 1.0 tra atteso e ottenuto),
   non l'uguaglianza esatta dei componenti — l'unica invariante che
   sopravvive alla normalizzazione lato server ed è comunque tutto ciò
   che serve a chi consuma il vettore per ricerche di similarità (Fase 4,
   fuori scope qui).
5. **`sink-vector` era già in `scripts/task-lint.sh`/`task-build.sh`/
   `task-test.sh` (`GO_SERVICES`)** dallo scaffold vuoto di SPEC-001 — a
   differenza di `embedding-worker` (SPEC-030), qui NON è stata necessaria
   nessuna deviazione riguardo `task lint`/`task test`: entrambi coprono
   già `services/sink-vector` automaticamente, verificato con l'esecuzione
   reale di entrambi (`task lint`/`task test` dalla radice del repo).
