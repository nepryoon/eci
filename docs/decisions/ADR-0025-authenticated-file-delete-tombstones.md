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
UPSERT e DELETE dello stesso file hanno ordering nella stessa partizione
primaria. Poiche' il runtime usa topic di retry, questo ordering non e'
sufficiente da solo: un UPSERT spostato sul retry topic puo' essere consegnato
dopo una DELETE piu' recente.

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

La stessa migration aggiunge a `outbox` un `event_sequence BIGINT GENERATED
ALWAYS AS IDENTITY`, univoco e positivo, e la tabella
`consumer_projection_watermark`, indicizzata per consumer, aggregate type e
aggregate ID. Ogni writer ingestion acquisisce, oltre al lock sul command ID,
un advisory transaction lock length-prefixed su tenant/repository/ACL/path.
Le mutazioni dello stesso file sono quindi serializzate prima delle scritture
canoniche e dell'allocazione delle sequenze. I republisher reconciliation
bloccano `FOR UPDATE` le righe canoniche idratate: una repair UPSERT o precede
la DELETE e riceve una sequenza inferiore, oppure osserva la riga gia' assente.

Il CDC promuove `event_type` e `event_sequence` negli header Kafka omonimi.
Ogni sink richiede una singola sequenza decimale canonica positiva oltre a
operation ed event ID; header assente, duplicato o invalido fallisce chiuso.
Prima dell'effetto il consumer acquisisce un advisory transaction lock per
consumer+aggregate e confronta la sequenza col watermark. Un evento con
sequenza minore o uguale viene marcato processed senza applicare l'effetto.
Per un evento piu' recente il lock resta detenuto durante l'effetto esterno
idempotente; watermark e `processed_events` vengono poi committati insieme.
Un crash post-effetto/pre-commit puo' ripetere l'effetto, mai riordinare due
effetti concorrenti. Un nuovo UPSERT autorizzato con sequenza maggiore puo'
ricreare legittimamente la vista dopo una DELETE.

L'identita' di ordering e' quella dell'oggetto realmente materializzato, non
necessariamente la primary key canonica. Per CodeNode, CodeChunk e
CodeEmbedding coincide con l'ID della riga. Neo4j, invece, materializza una
CodeRelation per la tripla `(rel_type, from_id, to_id)` e non conserva l'UUID
SQL: il watermark di sink-graph per una relazione usa quindi una codifica
length-prefixed della stessa tripla logica. In questo modo un tombstone tardivo
per una vecchia riga UUID non puo' eliminare una relazione reingerita con UUID
diverso e sequenza piu' recente.

La stessa regola vale per la sostituzione durante un UPSERT dello stesso file.
Prima di cancellare le vecchie relazioni, embedding e chunk canonici, la
transazione enumera le righe sotto lock ed emette i rispettivi tombstone;
elimina poi embedding prima dei chunk per rispettare la FK e inserisce le
nuove relazioni/chunk. I DELETE ricevono sequenze inferiori agli UPSERT di
sostituzione nella medesima transazione. In questo modo una re-ingestion non
lascia documenti o archi storici nelle viste e non fallisce quando il worker
embedding ha gia' materializzato un dipendente.

## Conseguenze

- Nessun nuovo writer canonico o topic: PostgreSQL+outbox/CDC restano l'unico
  percorso di mutazione.
- Un file gia' assente produce una receipt applicata e zero tombstone; la
  redelivery e' un no-op deterministico.
- Una relazione cross-file che punta a un nodo eliminato viene anch'essa
  rimossa e tombstonata, evitando FK e archi orfani.
- La normale re-ingestion produce tombstone per ogni proiezione sostituita;
  non si affida a una successiva DELETE file per ripulire UUID storici.
- La sequenza e' globale per semplicita' operativa, mentre il watermark resta
  per-consumer/per-aggregate: nessun consumer dipende dalla continuita' della
  sequenza o da eventi di altri aggregate.
- Per le relazioni Neo4j, `aggregate_id` e' l'identita' logica tipizzata degli
  endpoint, non l'UUID della riga PostgreSQL; questo rispecchia esattamente la
  chiave del `MERGE` della vista.
- Il lock PostgreSQL viene mantenuto durante l'effetto esterno bounded. Questo
  sacrifica throughput sul singolo aggregate, non tra aggregate indipendenti,
  e rende esplicita la backpressure necessaria per l'ordine.
- Canonical delete, ciascun sink, invalidazioni e reconciliation sono SPEC
  sequenziali indipendenti; nessuna SPEC puo' dichiarare end-to-end verified
  prima dell'intera catena.

## Sicurezza e rollback

La selezione usa congiuntamente tenant, repository e path, non il solo path.
Gli ID scope-owned impediscono collisioni cross-repository; la provenance
viene comunque ricontrollata. DELETE non puo' indurre fetch/SSRF e non riceve
bucket/key. Metriche usano solo operation/outcome a cardinalita' chiusa.

Il rollback del runtime smette di accettare nuove DELETE ma non reinserisce
dati gia' rimossi. La down migration elimina watermark/sequenza e scarta
soltanto le receipt DELETE, perche' lo schema UPSERT-only non puo' rappresentare
onestamente un digest sorgente nullo; non fabbrica digest e non ripristina dati.
Il recupero di una cancellazione applicata e' un nuovo UPSERT dalla sorgente
durevole autorizzata, non una ricostruzione dai tombstone.

Il rollout e' migration-first e connector-first: la colonna e la placement
`event_sequence` devono essere attive e osservate end-to-end prima di avviare i
consumer che la richiedono. Il rollback dei consumer precede quello del
connector e della migration. La sequenza non contiene tenant, repository,
path, payload o credenziali e non viene usata come autorita' ACL.

OpenSearch richiede inoltre una migrazione applicativa esplicita: aggiungere i
campi al mapping non modifica `_source` dei documenti esistenti. Prima di
pubblicare il marker mapping `eci_chunk_cursor_schema=1`, sink-search esegue un
`update_by_query` sincrono e bounded che assegna a ogni documento storico
`chunk_id=_id` ed `event_sequence=0`. Zero e' riservato alla vista storica;
ogni sequenza outbox canonica e' positiva e la supersede. Timeout, conflitti o
failure parziali impediscono il marker. Retrieval-engine richiede il marker
prima di aprire il listener e valida comunque su ogni hit identita', presenza
e non negativita' del cursore, fallendo chiuso. Il rollback conserva i campi
aggiunti e il marker innocui; rimuoverli richiederebbe ricostruire la vista da
PostgreSQL, mai una down migration distruttiva in-place.

## Review avversariale

Il design evita offset-before-effect, marker-before-effect e dual write verso
Kafka. Un crash prima del commit non pubblica tombstone ne' cancella righe; un
crash dopo il commit viene assorbito dalla receipt e dai sink idempotenti.
Il solo partition ordering non protegge dai retry topic; sequenza canonica,
watermark e lock per aggregate coprono esplicitamente quel failure mode. Test
reali su Neo4j, Qdrant e OpenSearch, oltre al worker embedding, consegnano una
DELETE con sequenza maggiore seguita da un vecchio UPSERT e verificano che la
vista non venga ricreata. La delete di relazioni entranti impedisce riferimenti
cross-file orfani, mentre i predicati scope impediscono che un path omonimo
elimini un altro tenant/repository. Nessun payload di delete contiene
contenuto da esporre.

Una regressione distinta usa due UUID canonici per la medesima tripla
relazionale: applica il nuovo UPSERT e poi il vecchio tombstone. Il lock e il
watermark sulla tripla logica classificano il tombstone come stale e
mantengono l'arco nuovo; usare gli UUID separati riprodurrebbe invece la delete
errata osservata durante la review.
