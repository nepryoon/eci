# ADR-0025 — Comandi file-delete autenticati e tombstone outbox

Stato: accepted
Data: 2026-08-30
Decisioni collegate: D1, D2, T7.1a, ADR-0011, ADR-0021–0023

## Contesto

Il contratto outbox ammette `DELETE`, ma l'ingestion produce soltanto UPSERT e
i sink interpretano il payload senza conoscere il tipo evento. Una nuova
versione di repository puo' quindi rimuovere un file da PostgreSQL soltanto
lasciando copie indefinitamente leggibili in Neo4j, Qdrant e OpenSearch. ADR-0023
ha dichiarato esplicitamente questo limite senza simularne la soluzione.

Il comando file corrente contiene obbligatoriamente digest e dimensione di un
oggetto MinIO. Una delete non ha sorgente da scaricare: inventare un blob vuoto,
un digest sentinella o una URL allargherebbe il trust boundary e confonderebbe
assenza con contenuto valido.

## Decisione

Il topic e la chiave partizionata `eci.ingestion.file.v1` restano invariati.
Il contratto JSON viene esteso additivamente con `operation`:

- assente o `UPSERT`: conserva esattamente il payload v1 esistente e richiede
  `source_sha256`/`source_size_bytes`;
- `DELETE`: richiede soltanto i campi comuni `schema_version`, `command_id`,
  `commit_sha`, `path` e vieta i campi sorgente.

Tenant, repository e ACL continuano ad arrivare esclusivamente dagli header
Kafka autenticati; `operation`, path o body non sono fonti di autorita'. La
chiave Kafka resta il digest length-prefixed di tenant/repository/path, cosi'
UPSERT e DELETE dello stesso file hanno ordering nella stessa partizione.

Una DELETE valida non contatta MinIO e non invoca il parser. In una singola
transazione PostgreSQL il worker:

1. acquisisce il receipt idempotente e individua i nodi usando
   `tenant_id`/`repo`/`path` nella provenance canonica;
2. materializza tombstone minimi e sanitizzati per ogni CodeEmbedding,
   CodeChunk, CodeRelation e CodeNode interessato;
3. inserisce righe outbox `event_type='DELETE'` prima di rimuovere, in ordine
   referenziale, embedding, chunk, relazioni, lineage e nodi;
4. registra la receipt solo dopo tutte le mutazioni e committa tutto insieme.

I tombstone portano l'identita' necessaria alla vista e la provenance di scope,
ma mai testo sorgente, vettori, prompt o secret. Le relazioni portano anche
tipo/from/to per eliminare la relazione Neo4j, che non conserva l'UUID SQL.

La migration receipt aggiunge `operation` con default `UPSERT` e rende
`source_sha256` nullable soltanto per DELETE tramite CHECK. Il fingerprint
include sempre l'operazione; riusare un command ID con operazione o coordinate
diverse resta conflitto permanente.

Il CDC deve promuovere `event_type` nell'header Kafka omonimo. Ogni sink valida
l'header contro `UPSERT|DELETE`; header assente/duplicato/ignoto fallisce chiuso
come payload permanente. Il marker `processed_events` viene scritto soltanto
dopo che l'effetto esterno idempotente e' durably applied, anche per DELETE.

## Conseguenze

- Nessun nuovo writer canonico o topic: PostgreSQL+outbox/CDC restano l'unico
  percorso di mutazione.
- Un file gia' assente produce una receipt applicata e zero tombstone; la
  redelivery e' un no-op deterministico.
- Una relazione cross-file che punta a un nodo eliminato viene anch'essa
  rimossa e tombstonata, evitando FK e archi orfani.
- Canonical delete, ciascun sink, invalidazioni e reconciliation sono SPEC
  sequenziali indipendenti; nessuna SPEC puo' dichiarare end-to-end verified
  prima dell'intera catena.

## Sicurezza e rollback

La selezione usa congiuntamente tenant, repository e path, non il solo path.
Gli ID scope-owned impediscono collisioni cross-repository; la provenance
viene comunque ricontrollata. DELETE non puo' indurre fetch/SSRF e non riceve
bucket/key. Metriche usano solo operation/outcome a cardinalita' chiusa.

Il rollback del runtime smette di accettare nuove DELETE ma non reinserisce
dati gia' rimossi. Migration e campi/header sono additive e devono restare
leggibili. Il recupero di una cancellazione applicata e' un nuovo UPSERT dalla
sorgente durevole autorizzata, non una ricostruzione dai tombstone.

## Review avversariale

Il design evita offset-before-effect, marker-before-effect e dual write verso
Kafka. Un crash prima del commit non pubblica tombstone ne' cancella righe; un
crash dopo il commit viene assorbito dalla receipt e dai sink idempotenti.
Ordering per path evita che un vecchio UPSERT superi una DELETE sullo stesso
file. La delete di relazioni entranti impedisce riferimenti cross-file orfani,
mentre i predicati scope impediscono che un path omonimo elimini un altro
tenant/repository. Nessun payload di delete contiene contenuto da esporre.
