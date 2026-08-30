# SPEC-030 — Worker asincrono di embedding (pre-T3.1, 2/3)
Stato: verified
Task-tree: prerequisito non nominato esplicitamente da Fase 3 (concordato in chat) — secondo dei tre pezzi prima di T3.1 · Nuovo servizio: services/embedding-worker (Go) · ADD: Modulo 1 §1.6.3/§2.1 (embedding come payload derivato, cache-aware)
Contratti: nuova migrazione `contracts/sql/migrations/0004_code_embedding.up.sql` (nuova tabella, additiva — nessun ADR)

## 1. Obiettivo
Consumare `outbox.event.CodeChunk` (SPEC-029), chiamare il client di embedding (T2.4 — fake o reale, stesso URL configurabile) per ciascun chunk, scrivere il vettore risultante in una nuova tabella `code_embedding` più una riga outbox (`aggregate_type = 'CodeEmbedding'`), che T3.1 (sink-vector) consumerà per scrivere su Qdrant. Nessun sink-vector in questa SPEC — solo il worker che produce l'evento che T3.1 leggerà.

## 2. Interfaccia

**Nuovo servizio Go** `services/embedding-worker/`, stesso scheletro di `sink-graph` (T1.3, SPEC-015) — consumer Kafka del topic `outbox.event.CodeChunk`, dedup via la tabella `processed_events` GIÀ ESISTENTE (stesso meccanismo, `INSERT...ON CONFLICT (event_id, consumer_name) DO NOTHING RETURNING` secondo ADR-0021 — non una nuova tabella di dedup, il meccanismo è consumer-scoped per costruzione).

**Client di embedding in Go** (nuovo, piccolo — non un riuso del client Rust di T2.4, che vive in un crate diverso e non è chiamabile da Go): stessa API nativa TEI (`POST {base_url}/embed`, `{"inputs": text}` → `[[float]]`), stesso principio di URL configurabile (fake in sviluppo/test, vero TEI quando disponibile) di T2.4 — nessuna logica nuova, la stessa scelta di design replicata nel linguaggio di questo servizio.

**Nuova tabella**:
```sql
CREATE TABLE code_embedding (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain TEXT NOT NULL DEFAULT 'code',
    chunk_id UUID NOT NULL REFERENCES code_chunk(id),
    vector REAL[] NOT NULL,
    model_id TEXT NOT NULL,
    embedding_dim INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chunk_id, model_id)
);
```
**Decisione dichiarata**: `REAL[]` nativo di Postgres, non l'estensione `pgvector` — non installata/configurata da nessuna parte in questo progetto finora (verificato: nessun'immagine `pgvector/pgvector` in `docker-compose.yml`, il Postgres in uso è quello standard). Nessuna query di similarità vettoriale avviene MAI in Postgres in questa architettura (quello è il ruolo di Qdrant) — `pgvector` aggiungerebbe una dipendenza infrastrutturale non necessaria per il solo scopo di "conservare il vettore come sorgente di verità". Rivalutabile in futuro se emergesse un bisogno reale di query vettoriali dirette in Postgres.

**Payload outbox**: il vettore stesso incluso nel payload JSON scritto in outbox (non solo un riferimento) — stesso principio già osservato in SPEC-029, dove il testo COMPLETO del chunk viaggiava nel payload Kafka, non solo il suo id: evita a `sink-vector` (T3.1) di dover interrogare Postgres separatamente per ottenere il vettore.

**Gestione errori, scope dichiarato**: nessun retry/backoff sofisticato o DLQ in questa SPEC — esplicitamente T3.3 nel task-tree originale ("DLQ + retry/backoff + metriche lag Prometheus su tutti i sink"), un task successivo dedicato. Qui: un fallimento della chiamata di embedding (servizio irraggiungibile) logga l'errore esplicitamente e NON conferma l'offset Kafka del messaggio (ridelivery naturale al prossimo poll, comportamento at-least-once già stabilito ovunque in questo progetto) — non un crash del processo, non un messaggio perso in silenzio.

## 3. Comportamento (scenari)

1. **Dato** un messaggio `CodeChunk` reale in Kafka (prodotto da SPEC-029), **quando** il worker lo consuma, **allora** ottengo una riga `code_embedding` con `chunk_id` corretto, `vector` di lunghezza coerente col modello configurato, `model_id` popolato.
2. **Dato** lo stesso messaggio consumato una seconda volta (ridelivery simulata), **quando** il worker lo processa di nuovo, **allora** nessuna riga duplicata in `code_embedding` — dedup via `processed_events` funziona come già stabilito per `sink-graph`.
3. **Dato** lo scenario 1, **quando** ispeziono `outbox`, **allora** trovo una riga `aggregate_type = 'CodeEmbedding'` col vettore incluso nel payload, non solo un riferimento.
4. **Dato** il servizio di embedding irraggiungibile (fake fermato deliberatamente), **quando** il worker tenta di processare un messaggio, **allora** l'errore è loggato esplicitamente e l'offset Kafka NON viene confermato — verificabile riavviando il fake e osservando che il messaggio viene ri-processato con successo al prossimo poll, non perso.
5. **Dato** due chunk diversi dello stesso file (es. il File-level chunk e un chunk di Method, se il fixture li produce entrambi), **quando** il worker li processa, **allora** ciascuno riceve il proprio vettore indipendente, correttamente distinto per `chunk_id`.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Un messaggio `CodeChunk` con `text` vuoto (caso limite già ammesso da T2.2 — corpo vuoto produce un chunk con `char_count: 0`) | Comunque inviato all'embedding (`embedder-fake` gestisce già testo vuoto in modo deterministico, SPEC-023 §4) — nessun caso speciale |
| Il worker si riavvia a metà di un batch di messaggi non ancora confermati | Nessun problema per costruzione — la dedup via `processed_events` rende il riprocessamento sicuro, stesso principio già stabilito per `sink-graph` |

## 5. Non-goals
Nessun sink-vector/Qdrant (T3.1, dopo). Nessun DLQ/retry sofisticato/metriche Prometheus (T3.3). Nessuna estensione `pgvector` (dichiarato in §2). Nessuna gestione di modelli multipli/versioning degli embedding oltre a registrare `model_id` per riga.

## 6. Vincoli dall'ADD
Modulo 1 §1.6.3: embedding come payload derivato del CPG, con `model_id`/`embedding_dim` tracciati — questa SPEC è il primo punto in cui questi campi vengono effettivamente popolati da un embedding realmente calcolato (fake o vero), non solo descritti nello schema della Semantic Cache.

## 7. Test plan
Test di integrazione con Kafka+Postgres reali (testcontainers, stesso principio già stabilito per `sink-graph`/T1.3) più `embedder-fake` avviato come processo reale (stesso pattern già usato in SPEC-023) — non mock su nessuno dei tre fronti.

## 8. Osservabilità
Stessa fondazione OTel già stabilita per i servizi Go del progetto — uno span per messaggio processato.

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta.
- [x] Edge case tabella §4 verificati esplicitamente.
- [x] Verificato che l'assenza di `pgvector` sia un fatto reale (non presunto) — un controllo diretto su `docker-compose.yml`/l'immagine Postgres in uso.
- [x] Nessuna regressione sui test esistenti.

## 10. Deviazioni dall'implementazione

1. **Dedup + scrittura `code_embedding` + riga outbox in un'UNICA
   transazione Postgres**, non tre passi separati come nel pattern
   letterale di `sink-graph` (§2 dice solo "stesso meccanismo, stesso
   `INSERT...ON CONFLICT DO NOTHING RETURNING`"). Possibile qui perché,
   a differenza di `sink-graph` (Postgres -> Neo4j, due sistemi diversi,
   nessuna transazione condivisa possibile), TUTTE le scritture di questo
   worker (dedup, `code_embedding`, `outbox`) hanno come destinazione la
   STESSA Postgres. Bundlarle in una transazione elimina un rischio reale
   presente nel pattern letterale di `sink-graph`: se il dedup marca
   l'evento come processato ma la scrittura successiva fallisce
   (separatamente, non transazionale), l'evento risulterebbe "processato"
   per sempre senza che la riga corrispondente sia mai stata scritta. Qui
   la chiamata HTTP di embedding avviene PRIMA di aprire la transazione
   (§2: un suo fallimento non deve toccare il database affatto); dedup +
   scritture avvengono POI, atomicamente — se il dedup rileva un duplicato,
   l'intera transazione va in ROLLBACK (nessuna scrittura), altrimenti
   viene committata per intero.
2. **`task lint`/`task build`/`task test` non includono
   `services/embedding-worker`**: `scripts/task-lint.sh`/`task-build.sh`/
   `task-test.sh` enumerano i servizi Go in un array `GO_SERVICES`
   hardcoded che non è stato esteso — quegli script sono fuori dai file
   toccabili di questa SPEC (`contracts/sql/migrations`,
   `services/embedding-worker`, questa SPEC). Verificato invece
   direttamente con `go build ./...`/`go vet ./...`/`go vet -tags=integration
   ./...`/`go test ./...`/`go test -tags=integration ./...` dentro
   `services/embedding-worker`, tutti verdi. Un futuro PR che tocchi
   `scripts/` dovrebbe aggiungere `embedding-worker` a quegli array.
3. **`EMBEDDING_SERVICE_URL`** riusato LETTERALMENTE come nome della env
   var (stessa di `EmbeddingClient` Rust, SPEC-023 §2), non una variante
   Go-specifica — coerente con "stesso URL configurabile" di §2. Aggiunta
   `EMBEDDING_MODEL_ID` (default `jina-code-embeddings-1.5b`, stack
   dichiarato in CLAUDE.md), non specificata da §2 ma necessaria per
   popolare `code_embedding.model_id`.
4. **Assenza di `pgvector` verificata direttamente** (non presunta):
   `deploy/compose/docker-compose.yml` usa `image: postgres:17` (immagine
   standard, non `pgvector/pgvector`); `grep -rni pgvector` sull'intero
   repo non trova nessuna configurazione; le uniche `CREATE EXTENSION` in
   `contracts/sql/migrations/` sono `pgcrypto` (0001, per
   `gen_random_uuid()`). Conferma diretta della decisione dichiarata in §2.
