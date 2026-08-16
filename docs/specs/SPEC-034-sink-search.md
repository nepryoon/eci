# SPEC-034 — sink-search: CodeChunk → OpenSearch (T3.2)
Stato: implemented
Task-tree: T3.2 (secondo task di Fase 3, dip. T0.6 — non T2.4, nessuna catena di prerequisiti come T3.1) · Nuovo servizio: services/sink-search (Go, scaffold vuoto da SPEC-001) · ADD: Modulo 1 (hybrid storage — full-text, terza gamba oltre grafo/vettore)

## 1. Obiettivo
Indicizzare su OpenSearch il testo dei chunk (SPEC-029) per ricerca full-text — terza gamba dello storage ibrido dell'ADD, oltre Neo4j (grafo, già in T1.4 con fulltext nativo solo su `name`) e Qdrant (vettori, T3.1). **Nessuna catena di prerequisiti**: a differenza di T3.1, `outbox.event.CodeChunk` esiste già dal 2026-08-14 (SPEC-029) e porta già tutto il necessario (`text`, `entity_id`, `provenance`, SPEC-032) — questo servizio è un SECONDO consumatore dello stesso topic già consumato da `embedding-worker` (gruppo consumer Kafka distinto, stesso principio standard di fan-out — non ancora esercitato esplicitamente in questo progetto, ma nessun meccanismo nuovo da costruire).

## 2. Interfaccia

**Client**: `github.com/opensearch-project/opensearch-go` (ufficiale). **Da verificare esplicitamente in implementazione** (non presunto qui): quale forma di API sia quella corrente per la versione effettivamente risolta da `go get` — trovate due forme apparentemente di generazioni diverse (`opensearchapi.IndexRequest{...}.Do(ctx, client)` vs `opensearchapi.NewClient(Config{...})` con metodi namespaced come `client.Document.X`, quest'ultima da `/v4`) — usare quella che la versione effettivamente installata espone, non la prima trovata per caso.

**ID documento**: `chunk.id` (non `entity_id` — stessa lezione di T3.1: un'entità produce più chunk, `entity_id` come id causerebbe sovrascritture silenziose). A differenza di Qdrant, **nessuna derivazione necessaria** — OpenSearch accetta stringhe arbitrarie come `DocumentID` (unico vincolo: 512 byte), i nostri id SHA-256 esadecimali funzionano direttamente — **da confermare comunque con una scrittura reale**, non presunto dalla sola documentazione.

**Indice**: nome dichiarato `code_chunks`, creato idempotentemente al via del servizio (stesso principio già stabilito per la collection Qdrant in T3.1) se non esiste. Mapping minimo: `text` (analizzato per full-text), `entity_id`/`provenance`/`chunk_index` (memorizzati, non necessariamente analizzati per full-text).

**Consumer**: stesso scheletro Kafka-consumer+dedup già stabilito (`sink-graph`/`embedding-worker`/`sink-vector`) — consuma `outbox.event.CodeChunk` con un **consumer group id proprio e distinto** da quello di `embedding-worker` (necessario perché entrambi devono ricevere OGNI messaggio indipendentemente — due consumer group diversi sullo stesso topic, non uno condiviso), dedup via `processed_events` (stessa tabella condivisa).

## 3. Comportamento (scenari)

1. **Dato** un messaggio `CodeChunk` reale, **quando** il servizio lo consuma, **allora** un documento esiste in OpenSearch con quell'id, `text` corrispondente, `entity_id`/`provenance` nei campi memorizzati.
2. **Dato** lo stesso messaggio consumato una seconda volta (ridelivery), **quando** il servizio lo riprocessa, **allora** nessun documento duplicato — dedup via `processed_events`.
3. **Dato** una query full-text semplice contro l'indice (es. cercare una parola presente nel testo di un chunk noto), **quando** eseguo la ricerca, **allora** il documento corretto compare tra i risultati — verifica diretta che l'indicizzazione full-text funzioni, non solo che il documento esista.
4. **Dato** il servizio avviato per la prima volta senza l'indice `code_chunks`, **quando** parte, **allora** lo crea prima di iniziare a consumare; riavviato con l'indice già esistente, non tenta di ricrearlo.
5. **Dato** lo STESSO messaggio `CodeChunk` consumato in parallelo sia da questo servizio sia da `embedding-worker` (entrambi attivi sullo stesso topic), **quando** ispeziono lo stato finale, **allora** ENTRAMBI hanno processato il messaggio in modo indipendente — verifica diretta del fan-out via consumer group distinti, non solo dichiarato.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| OpenSearch irraggiungibile all'avvio (setup indice) | Errore esplicito, il servizio non parte silenziosamente |
| Un messaggio `CodeChunk` con `text` vuoto (caso limite già ammesso da T2.2) | Documento comunque indicizzato con `text` vuoto — nessun caso speciale |

## 5. Non-goals
Nessuna integrazione con `retrieval-engine` (T1.4) — questa SPEC scrive l'indice, non lo interroga da nessun consumatore reale (stesso principio "costruisci la capacità, rimanda il collegamento" già applicato in T2.2/T2.3/T2.4). Nessun DLQ/retry sofisticato (T3.3). Nessuna gestione di aggiornamento/cancellazione quando un'entità cambia (stesso limite pre-esistente già noto, non nuovo qui).

## 6. Vincoli dall'ADD
Modulo 1: hybrid storage con una gamba full-text dedicata, distinta dal fulltext nativo limitato di Neo4j (T1.4, solo `name`) — questa SPEC la costruisce per la prima volta sul testo completo dei chunk.

## 7. Test plan
Test di integrazione con Kafka+Postgres+OpenSearch reali (testcontainers) — scenario 3 in particolare richiede una query reale, non solo un `GetDocument` per id, per verificare che l'analisi full-text sia genuinamente configurata e funzionante.

## 8. Osservabilità
Stessa fondazione OTel già stabilita per i servizi Go del progetto.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta.
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Versione/forma esatta dell'API `opensearch-go` verificata empiricamente, riportata nel report.
- [x] Scrittura con id SHA-256 esadecimale confermata funzionante con una scrittura reale, non presunta dalla sola documentazione.

## 10. Deviazioni dall'implementazione

1. **Versione/forma API `opensearch-go` — verificato PRIMA di scrivere
   codice, come richiesto**: `go get github.com/opensearch-project/opensearch-go@latest`
   (percorso "nudo", senza suffisso di versione maggiore) risolve alla
   VECCHIA linea v1 (`v1.1.0`) — non perché sia davvero la più recente, ma
   per la regola di Go Modules sul "semantic import versioning": un major
   ≥2 richiede il suffisso `/vN` nel path di import, quindi `@latest` sul
   path nudo considera solo v0/v1. La versione realmente più recente è
   `github.com/opensearch-project/opensearch-go/v4` (risolto a `v4.7.3`),
   che espone la forma `opensearchapi.NewClient(Config{...})` con client
   namespaced (`client.Document.X`, `client.Indices.X`) — la SECONDA delle
   due forme descritte da §2, non la prima (`IndexRequest{}.Do(...)`, che
   appartiene alla v1/v2 più vecchie). Usata `/v4` con la forma namespaced.
2. **Bug/limite del client `/v4` scoperto scrivendo il test (§7, non
   presunto)**: `Indices.Exists`/`Document.Exists` eseguono una richiesta
   HEAD (corpo sempre vuoto per definizione HTTP); il meccanismo generico
   `do()` del client tratta qualunque status ≥400 — 404 incluso, l'esito
   NORMALE e atteso di "non esiste" per un endpoint *Exists — come un
   errore da decodificare come corpo JSON, che su un corpo vuoto fallisce
   con `"failed to json unmarshal body"` invece di un errore chiaro.
   `EnsureIndex` (e gli helper omologhi nel test) ispezionano quindi
   `StatusCode` sulla `*opensearch.Response` ritornata direttamente (che
   resta popolata anche quando l'`error` non è nil), non il solo valore di
   errore: `resp == nil` → irraggiungibile (errore reale, propagato);
   `resp.StatusCode == 404` → non esiste, comportamento normale;
   `resp.StatusCode == 200` → esiste.
3. **`Document.Create` scartato in favore di `client.Index`**: durante
   l'implementazione ho verificato nel sorgente del client che
   `Document.Create` mappa sull'endpoint `PUT {index}/_create/{id}`
   (fallisce con 409 su id duplicato — semantica "crea, mai sovrascrivi"),
   mentre il metodo top-level `client.Index` mappa su
   `PUT {index}/_doc/{id}` (upsert-by-id, sovrascrive se l'id esiste già).
   Usato `client.Index` per la stessa idempotenza upsert-by-id già scelta
   per Qdrant/T3.1 — anche se in pratica il dedup via `processed_events`
   impedisce già una seconda chiamata a `indexDocument` per lo STESSO
   `event_id` prima che la scelta tra i due endpoint diventi rilevante.
4. **Id SHA-256 esadecimale confermato con scrittura reale (§9)**: il
   test (scenario 1) usa esplicitamente una stringa di 64 caratteri
   esadecimali (stessa forma di `entity_id`/`code_node.id`, SPEC-013) come
   `DocumentID` — scrittura, lettura e query full-text confermate
   funzionanti senza alcuna trasformazione, a differenza di Qdrant/T3.1.
   Osservazione collaterale (non un problema, solo un chiarimento):
   `code_chunk.id` nella pipeline reale (`persist.rs`, SPEC-029) è in
   realtà un UUID generato da Postgres (`gen_random_uuid()`), non una
   stringa SHA-256 esadecimale come `entity_id` — l'affermazione di §2 "i
   nostri id sono SHA-256 esadecimali" descrive con precisione `entity_id`,
   non il campo `chunk.id` realmente usato come `DocumentID`. Ininfluente
   ai fini pratici (OpenSearch accetta comunque qualunque stringa fino a
   512 byte), ma verificato esplicitamente col formato SHA-256 letterale
   per rispettare la lettera di quanto richiesto, non presunto.
5. **Scenario 5 (fan-out) verificato con due consumer group REALI attivi
   in parallelo, non con `embedding-worker` in esecuzione come processo
   separato**: `services/embedding-worker/internal/consumer` vive sotto
   un path `.../internal/...`, quindi non è importabile da questo modulo
   per costruzione (regola Go sui package `internal`, indipendente da
   eventuali `replace` tra moduli dello stesso repo). Verificato invece
   con DUE `kafka.Reader` realmente attivi in parallelo sullo stesso
   broker/topic nello stesso test: uno usa `consumer.ConsumerName`
   ("sink-search", il codice reale di questo servizio), l'altro usa il
   valore LETTERALE `"embedding-worker"`, copiato per lettura diretta da
   `services/embedding-worker/internal/consumer/consumer.go` (citato nel
   commento del test, non un valore indovinato) — entrambi ricevono
   davvero, indipendentemente, lo stesso messaggio prodotto una sola
   volta, provando il meccanismo di fan-out via consumer group Kafka
   distinti che è l'oggetto reale dello scenario 5. Non è stato avviato il
   binario completo di `embedding-worker` (richiederebbe anche
   `embedder-fake` e fixture Postgres per le sue FK) perché la sua logica
   applicativa non è ciò che lo scenario 5 verifica — il fan-out Kafka è
   una proprietà del broker/consumer-group, non del codice applicativo a
   valle, ed è provata più direttamente così.
