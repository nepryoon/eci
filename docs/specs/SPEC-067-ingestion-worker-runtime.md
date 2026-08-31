# SPEC-067 — Runtime worker ingestion durabile e autenticato
Stato: implemented
Task-tree: T7.1a · Servizio: `services/ingestion` · ADD: Modulo 3 §1.1–§1.2, §2.1, D8
Contratti: nuovo `contracts/jsonschema/ingestion-file-command.json`; `contracts/jsonschema/hybrid-graph.json`; nuova migration SQL receipt
ADR: ADR-0023

## 1. Obiettivo

Trasformare ingestion dal CLI one-shot in un worker long-running 4–40 che
consuma comandi file Kafka autenticati, legge sorgenti immutabili da MinIO e
scrive entita' canoniche, outbox e receipt con semantica at-least-once
idempotente. Sostituire nel chart il CronJob transitorio con un Deployment la
cui readiness verifica dipendenze reali e il cui consume-loop applica
backpressure tramite offset manuali.

## 2. Interfaccia

```json
{
  "$id": "https://eci.internal/contracts/ingestion-file-command.json",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "command_id", "commit_sha", "path", "source_sha256", "source_size_bytes"],
  "properties": {
    "schema_version": {"const": "1"},
    "command_id": {"type": "string", "format": "uuid"},
    "commit_sha": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
    "path": {"type": "string", "minLength": 1, "maxLength": 1024},
    "source_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    "source_size_bytes": {"type": "integer", "minimum": 0, "maximum": 16777216}
  }
}
```

```text
topic: eci.ingestion.file.v1
message key: sha256(length-prefix(tenant_id, repository, path))
required headers: eci-tenant-id, eci-repository, eci-acl-group
optional header: traceparent (W3C, invalid value is ignored rather than trusted)
consumer group: ingestion
DLQ: eci.ingestion.file.v1.DLQ

GET /live    -> 204 | 503
GET /ready   -> 204 | 503 (empty body)
GET /metrics -> 200 text/plain; version=0.0.4
```

```rust
pub struct IngestionFileCommand {
    pub schema_version: String,
    pub command_id: uuid::Uuid,
    pub commit_sha: String,
    pub path: String,
    pub source_sha256: String,
    pub source_size_bytes: u64,
}

pub struct AuthenticatedCommitScope { /* private validated fields */ }

pub fn parse_authenticated_command(
    payload: &[u8],
    headers: &[(String, Vec<u8>)],
    max_source_bytes: u64,
) -> Result<(AuthenticatedCommitScope, IngestionFileCommand), CommandError>;

pub fn source_object_key(
    scope: &AuthenticatedCommitScope,
    command: &IngestionFileCommand,
) -> String;

pub fn persist_ingestion_command(
    client: &mut postgres::Client,
    scope: &AuthenticatedCommitScope,
    command: &IngestionFileCommand,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[CodeChunk],
) -> Result<CommandOutcome, PersistError>; // Applied | Duplicate
```

## 3. Comportamento

1. **Consume autenticato.** Given broker mTLS e i tre header completi da un
   producer ACL dedicato, When arriva un comando valido, Then il worker deriva
   object key e scope senza leggere autorita' dal body, verifica il blob,
   parsa e committa l'offset solo dopo il commit PostgreSQL.
2. **Replay idempotente.** Given lo stesso `command_id` e fingerprint consegnato
   due volte, When entrambe le delivery sono elaborate, Then la seconda e'
   `Duplicate`: code tables, outbox e receipt restano byte/conteggio invariati
   e l'offset puo' avanzare. Il receipt preflight avviene prima del fetch
   MinIO/parser: un comando gia' completato non dipende piu' dalla presenza del
   blob; il lock transazionale resta il controllo autoritativo per concorrenza.
3. **Conflitto fail-closed.** Given un `command_id` gia' completato, When cambia
   scope, commit, path o digest, Then nessuna mutazione canonica avviene, un
   reason code `command_id_conflict` raggiunge la DLQ e solo dopo si committa
   l'originale.
4. **Sorgente vincolata.** Given key/path/size/digest inatteso, symlink/URL o
   blob non UTF-8, When il comando e' validato/scaricato, Then nessun parser o
   PostgreSQL write parte; l'errore permanente sanitizzato va in DLQ senza
   contenuto, scope o path. `maxLength=1024` e il runtime misurano entrambi code
   point Unicode; la key MinIO usa un digest fisso del path UTF-8 e non puo'
   superare il limite S3 per un path valido. `command_id` accetta soltanto la
   rappresentazione UUID RFC 4122 con gruppi `8-4-4-4-12` richiesta dal format
   JSON Schema (hex case-insensitive), non le forme compact, braced o URN che
   il parser UUID permissivo saprebbe altrimenti normalizzare.
5. **Errore transitorio/backpressure.** Given Kafka, PostgreSQL o MinIO
   indisponibile, When un comando e' in-flight, Then nessun offset viene
   committato, la partizione non avanza, il consumer continua a pollare mentre
   l'assignment è in pausa per conservare membership, il retry usa backoff
   bounded con jitter e `/ready` resta 503 fino al recupero. Ogni callback di
   rebalance invalida l'epoch locale: prima di elaborare e prima di committare
   l'offset il worker verifica epoch e ownership topic/partition, scartando i
   record revocati dal buffer senza nuovo effetto o commit. Se la revoca avviene
   durante una transazione gia' iniziata, la receipt rende l'eventuale replay
   del nuovo owner idempotente e il vecchio owner non committa l'offset. Le
   nuove assignment osservate durante il retry sono immediatamente pausate; il
   buffer applicativo e' limitato a 64 record. A capacita' piena il worker
   continua a servire `StreamConsumer::recv()` per mantenere viva la membership:
   qualunque record prefetched esposto dopo il pause viene riavvolto allo stesso
   offset, mai aggiunto oltre il limite, scartato o acknowledged.
   Lo stesso poll concorrente resta attivo durante fetch, parse e persistenza
   di un comando valido, quindi un input grande non puo' far scadere
   `max.poll.interval.ms` soltanto per il tempo di elaborazione.
6. **Provenance completa.** Given un comando applicato, When si leggono
   CodeNode/CodeRelation/CodeChunk e relativi eventi, Then provenance contiene
   scope trusted, `repo`, `commit_sha`, `path`, un solo `ingested_at` DB e non
   contiene credenziali, URL o payload Kafka.
7. **Lifecycle reale.** Given dipendenze sane e consumer assegnato, When si
   chiamano i probe, Then `/live=204`, `/ready=204` e `/metrics` espone dati;
   dipendenza/assignment assente rende solo readiness 503 e non causa restart
   storm tramite liveness. La readiness PostgreSQL verifica anche presenza
   delle tabelle migrate e tutti i privilegi richiesti dalla transazione;
   raggiungibilita' o `SELECT 1` da soli non rendono il pod ready.
8. **Deployment worker pool.** Given render standard con applicazioni abilitate,
   When si ispeziona Helm, Then esiste `Deployment/ingestion` a 4 repliche con
   Service metrics, PDB/probe/resources, identita' Kafka propria, Secret
   enumerato e NetworkPolicy solo verso Kafka/PostgreSQL/MinIO; il CronJob
   transitorio non e' renderizzato e dev riduce onestamente le repliche.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| header scope mancante, duplicato, non UTF-8, blank/control/>256 byte | permanent deny prima di MinIO/parser/DB; DLQ sanitizzata |
| message key assente o diversa dalla key derivata | permanent `invalid_message_key`; impedisce di eludere l'ordinamento per path |
| scope presente anche nel body/additional property | schema reject; il body non diventa autorita' |
| payload >64 KiB o JSON/schema/versione invalida | permanent deny/DLQ; nessun echo del payload |
| path assoluto, `.`/`..`, backslash, control o >1024 code point Unicode | permanent `invalid_path` prima di costruire la key |
| path multibyte valido entro 1024 code point | accettato; key S3 ASCII fissa di 181 byte derivata dal digest del path |
| commit/digest/UUID/size invalido o size oltre config | permanent deny; UUID compact/braced/URN rifiutato prima di fetch/write |
| object key configurabile dal messaggio o endpoint non trusted | impossibile per tipo/schema; key derivata e endpoint solo env |
| GET object 404/digest/size/UTF-8 mismatch o sorgente UTF-8 con NUL | permanent failure/DLQ (`source_contains_nul` per NUL); nessun parse/write |
| MinIO header/body timeout o 5xx, Kafka transport, PostgreSQL unavailable | transient; no commit, readiness 503, retry bounded |
| endpoint MinIO HTTP, CA PostgreSQL/MinIO assente o non valida | startup fail-closed; HTTPS/TLS con CA e hostname runtime completo `minio.data-plane.svc.cluster.local` verificati obbligatori |
| rebalance durante fetch/retry/persist/DLQ publish | record invalidato dall'epoch; ownership riverificata dopo ogni await e prima dell'offset commit |
| parse/persist di un comando valido supera un ciclo di poll | elaborazione isolata in task; il consume-loop continua a pollare, pausa e bufferizza/riavvolge deterministicamente |
| nuova assignment durante backoff | assignment pausata; buffer applicativo massimo 64 record; a cap piena il poll continua e ogni record prefetched viene riavvolto allo stesso offset |
| migration receipt/tabella canonica assente o ruolo senza un privilegio runtime | readiness PostgreSQL 503 anche se la connessione e `SELECT 1` funzionano |
| PostgreSQL auth/connect/query/lock/commit stall | connect 5s, statement/TCP 10s e lock 5s; transient senza offset commit |
| parse panic per input ostile | catturato al command boundary, permanent `parse_failed`; il processo resta vivo |
| receipt esistente con fingerprint uguale | `Duplicate` prima di MinIO/parser, zero nuove righe outbox |
| receipt esistente con fingerprint diverso | `command_id_conflict` prima di MinIO/parser, zero write canonici |
| DLQ publish fallisce | original offset non committato |
| TLS/CA/cert/key/Secret assente | startup fail-closed; nessun PLAINTEXT/default credential |
| shutdown SIGTERM durante comando | stato shutdown durevole; drain massimo 20s, poi task abort e teardown blocking-pool massimo 5s entro la grace pod 30s; offset solo se commit completato |

## 5. Threat model e non-goals

**Attori:** producer esterno ostile senza ACL, producer di sistema compromesso,
tenant/client che controlla sorgente, payload/path malevolo, LLM compromesso,
operatore con Secret errato. **Asset:** scope tenant/repo/ACL, codice sorgente,
PostgreSQL/outbox, credenziali Kafka/MinIO, disponibilita' del worker. **Confini:**
la mTLS Kafka + ACL autentica il producer; i tre header costituiscono commit
context system-to-system; body e sorgente sono dati non fidati; endpoint,
bucket, CA pubbliche, limiti e credenziali sono config server-side. PostgreSQL
e MinIO sono TLS-only con hostname e CA verificati; al worker sono montate solo
le CA pubbliche, mai chiavi CA o chiavi server. **Attacchi:** confused
deputy, forged body scope, SSRF/object-key traversal, decompression/size bomb,
poison pill, replay, command-id collision, rebalance/offset-after-revocation,
MITM verso datastore, offset-before-commit, secret leak in metric/log/DLQ.
**Mitigazioni:** topic/identity literal, schema chiuso, scope
header-only, key derivata, GET bounded+digest, receipt atomica, DLQ sanitizzata,
offset manuale, metriche low-cardinality e fail-closed.

Non-goals: delete/tombstone, full commit atomicity, clone Git/URL, webhook/API
pubblica, stack-graphs/SCIP cross-file, linguaggi nuovi, HPA (T7.2), dashboard
(T7.3), service mesh, GPU, modifica ADD o scrittura diretta a viste
materializzate. La SPEC non dichiara risolte le eliminazioni: saranno un task
separato emerso dall'audit di completezza.

## 6. Vincoli dall'ADD

- Modulo 3 §1.1: ingestion stateless CPU worker pool, PostgreSQL+outbox nella
  stessa transazione, sorgenti su object storage, backlog senza downtime query.
- Modulo 3 §1.2: piano ingestion Kafka asincrono, at-least-once, DLQ e
  backpressure via lag; partizionamento per identita' aggregato.
- Modulo 3 §2.1: Kafka porta commit context system-to-system negli header;
  mTLS strict/fail-closed, mai scope da testo o LLM.
- D1/D2: PostgreSQL unica source of truth; outbox e provenance canonica.
- D8: `Ingestion and AST Parsing` e' Deployment Rust 4–40 nel piano CPU.
- ADR-0016: readiness/lifecycle non possono essere simulati; il CronJob e'
  eliminabile solo dopo ingresso durabile, backpressure e concorrenza reali.

## 7. Test plan

- Unit Rust: schema/header/path/key/fingerprint; UUID hyphenated lowercase e
  uppercase positivo, compact/braced/URN negativi; path ASCII e multibyte ai
  boundary 1024/1025 code point con key S3 bounded; scope body rifiutato; ordering
  stabile; DLQ reason allow-list; payload/failure non compaiono in log/metric;
  endpoint MinIO plaintext e CA PostgreSQL invalida sono rifiutati.
- Unit runtime con fake Kafka/object fetch/persistence: offset success,
  transient no-commit, DLQ-before-commit, shutdown e readiness transitions.
- Integration PostgreSQL testcontainers + migration reale: first apply,
  doppio replay stato identico, conflitto rollback, provenance completa e
  receipt/outbox nella stessa transazione.
- Integration MinIO/Kafka testcontainers: mTLS/ACL ove supportato dal harness,
  GET/digest/size, redelivery dopo fault e commit offset. Se Docker manca,
  resta obbligatorio in CI e non viene dichiarato eseguito localmente.
- Helm/schema/policy: topic/users ACL literal, Deployment 4–40-ready, dev
  ridotto, probe/Service/Secret/NetworkPolicy e assenza CronJob.
- Red-before-green: conservare nel log PR i nomi dei test falliti prima
  dell'implementazione, senza commit di output macchina volatile.

## 8. Osservabilita'

- Span `ingestion.command.consume` con attributi low-cardinality
  `messaging.system=kafka`, `messaging.destination.name`, partition,
  `ingestion.outcome`; span link da `traceparent` valido, mai parent trusted.
- Span `ingestion.source.fetch` con `storage.system=s3`, outcome e byte count;
  nessun bucket/key/path/scope.
- Span esistente `parse_file` e `persist_parsed_file`; aggiungere
  `ingestion.command.persist` con `ingestion.outcome=applied|duplicate|failed`.
  Il percorso worker usa lo span parser senza attributo `file_path`; la API
  one-shot storica conserva il contratto telemetry di SPEC-013.
- Counter `eci_ingestion_commands_total{outcome,reason}` con enum bounded;
  `eci_ingestion_source_bytes_total{outcome}`;
  `eci_ingestion_dlq_total{reason,outcome}`.
- Gauge `eci_ingestion_inflight_commands`; `eci_ingestion_ready{dependency}`
  con dependency in `kafka|postgres|minio`; histogram
  `eci_ingestion_command_duration_seconds{outcome}`.
- Vietati tenant, user, repo, ACL, command id, commit, path, object key, source,
  URL, token e credenziali in label, span attribute, log e DLQ.

## 9. Criteri di accettazione

- [x] Contratto JSON chiuso e migration receipt up/down aggiunti dopo ADR.
- [x] Scenari §3 scritti red-before-green e verdi CPU-only dove possibile.
- [x] Scope esclusivamente da header Kafka autenticati; body/LLM/source non
      possono modificarlo e Kafka ACL separa producer, worker e CDC.
- [x] Source key derivata, fetch bounded e verifica size/SHA-256/UTF-8 prima del
      parser; nessun URL/bucket/key dal messaggio.
- [x] Entita', outbox e receipt sono una transazione; replay identico produce
      zero nuovi effetti e conflitto produce zero write; receipt gia' durevoli
      sono classificati prima di qualunque object fetch.
- [x] Offset originale solo dopo DB commit o DLQ publish confermata; fault
      transienti conservano backlog e readiness 503; i retry applicano equal
      jitter bounded per evitare burst sincronizzati; rebalance invalida i
      record revocati prima di una nuova elaborazione e del commit offset,
      incluso il confine successivo alla publish DLQ.
- [x] Provenance soddisfa `repo/commit_sha/path/ingested_at` senza alterare i
      contratti golden/eval storici.
- [x] `/live`, `/ready`, `/metrics` e shutdown sono reali e testati.
- [x] Helm rende Deployment ingestion standard=4, max dichiarato=40, identity,
      ACL, Secret e NetworkPolicy least-privilege; nessun CronJob ingestion.
- [x] PostgreSQL e MinIO sono TLS-only con CA/hostname verificati; il chart
      monta al worker esclusivamente certificati CA pubblici.
- [x] Nessun secret, scope o sorgente in log, metriche, span o DLQ.
- [ ] `task build`, `task lint`, `task test`, `task test:integration`,
      `task guard`, `task k8s:validate` verdi; CI verde.

## 10. Review di approvazione e deviazioni

Review avversariale completata il 2026-08-30: nessuna contraddizione con ADD,
isolamento tenant, trust boundary LLM/body, source-of-truth/outbox,
idempotenza, fail-closed, separazione deterministico/probabilistico, traversal
bounded o deadline propagation. ADR-0023 rende esplicito il nuovo contratto;
non esiste una decisione architetturale nascosta.

Implementazione locale del 2026-08-30: i test red iniziali fallivano per il
modulo `ingestion::worker` e il `Deployment/ingestion` assenti. La suite verde
copre parsing/auth dello scope, key e limiti, probe/metriche, configurazione
Kafka TLS+offset manuale, DLQ sanitizzata, render/ACL/NetworkPolicy e una
integrazione PostgreSQL 17 reale con migration, replay identico e conflitto
atomico. `task test:integration` ha inoltre completato l'intera suite Docker del
repository. Il bucket `eci-sources`, il blob e le credenziali per-workload sono
prerequisiti provisionati dall'operatore/produttore e non valori o Secret
versionati; l'assenza del bucket mantiene readiness 503. Il passaggio a
`verified` resta subordinato a CI e review della PR. Le review successive hanno
aggiunto deadline I/O complete, polling durante retry, shutdown durevole,
privacy degli span parser, invalidazione per epoch/assignment al rebalance e
TLS con CA verificata per PostgreSQL e MinIO; nessuna correzione modifica il
contratto di scope o la semantica at-least-once. Il runner E2E storico invoca
ora esplicitamente `oneshot`; il runtime non reintroduce path positional
ambigui. Il receipt preflight elimina dipendenza da MinIO per replay gia'
completati, mentre la persistenza conserva advisory lock e verifica finale.
Il retry a buffer pieno conserva la membership Kafka senza oltrepassare il cap:
un record prefetched e' riavvolto prima di continuare. La readiness PostgreSQL
interroga deterministicamente tabelle e privilegi effettivamente richiesti;
un test PostgreSQL 17 con ruolo least-privilege verifica il pass e il fail dopo
revoca di `SELECT` sulla receipt.
Lo startup probe usa `/live`, separato dal dependency gate `/ready`, così un
outage iniziale prolungato non genera restart loop; il backoff esponenziale usa
equal jitter tra meta' e intero cap per desincronizzare le repliche.
Fetch, parse e persistenza sono eseguiti in un task separato mentre il loop
continua a pollare Kafka; il controllo epoch/ownership dopo il join resta il
gate prima di qualunque offset commit.
Il bootstrap dev genera una CA MinIO separata e una leaf `CA:FALSE` con SAN ed
EKU server, monta la CA nel trust store peer e distribuisce al worker soltanto
la CA pubblica. Lo span consume resta vivo attraverso la classificazione e
registra `ingestion.outcome=applied|duplicate|failed|retry` da enum chiuso.
Il parser CPU-bound usa `spawn_blocking`, così anche un runtime con un solo
worker continua a servire Kafka e probe. Lo shutdown latched interrompe il
backoff senza attendere un secondo segnale; `failed` viene registrato soltanto
dopo publish DLQ e offset commit riusciti, altrimenti l'outcome e' `retry`.
Metadata Kafka e readiness PostgreSQL sincrone sono isolate dal Tokio worker
pool con deadline esterne; il commit offset sincrono conserva acknowledgement
ma gira nel blocking pool con timeout broker 5s/deadline 6s. Il drain SIGTERM
e' limitato a 20s e il runtime non attende task blocking oltre altri 5s, entro
la `terminationGracePeriodSeconds: 30` verificata nel render Helm.
Una volta latched SIGTERM il worker non avvia piu' publish DLQ o commit offset:
un'eventuale transazione completata durante il drain viene ripresa come replay
idempotente tramite receipt dal nuovo owner.
La review finale ha corretto il mismatch Unicode: contratto e runtime contano
code point, mentre la key oggetto usa un digest length-prefixed del path UTF-8.
Path multibyte contract-valid non finiscono piu' in DLQ e la key resta sempre
entro il limite S3 senza rivelare il nome del file.
La review automatizzata sul medesimo head ha inoltre provato due regressioni:
un NUL nel sorgente UTF-8 non e' persistibile in `TEXT` e viene ora classificato
permanente prima del parser, mentre il bootstrap dev valida e prova la leaf
MinIO contro lo stesso FQDN configurato dal client invece del solo nome corto
e richiede esplicitamente lo scopo TLS server. Il parent span del comando viene
inoltre catturato ed entrato nel thread blocking, cosi' gli span del parser non
diventano nuove root scollegate. L'ultimo controllo contract/runtime conserva
la rappresentazione UUID originale durante la deserializzazione abbastanza a
lungo da rifiutare compact, braced e URN; la forma hyphenated rimane valida con
hex maiuscolo o minuscolo, come il format JSON Schema.
