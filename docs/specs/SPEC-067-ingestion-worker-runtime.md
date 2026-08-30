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
   e l'offset puo' avanzare.
3. **Conflitto fail-closed.** Given un `command_id` gia' completato, When cambia
   scope, commit, path o digest, Then nessuna mutazione canonica avviene, un
   reason code `command_id_conflict` raggiunge la DLQ e solo dopo si committa
   l'originale.
4. **Sorgente vincolata.** Given key/path/size/digest inatteso, symlink/URL o
   blob non UTF-8, When il comando e' validato/scaricato, Then nessun parser o
   PostgreSQL write parte; l'errore permanente sanitizzato va in DLQ senza
   contenuto, scope o path.
5. **Errore transitorio/backpressure.** Given Kafka, PostgreSQL o MinIO
   indisponibile, When un comando e' in-flight, Then nessun offset viene
   committato, la partizione non avanza, il retry usa backoff bounded con jitter
   e `/ready` resta 503 fino al recupero.
6. **Provenance completa.** Given un comando applicato, When si leggono
   CodeNode/CodeRelation/CodeChunk e relativi eventi, Then provenance contiene
   scope trusted, `repo`, `commit_sha`, `path`, un solo `ingested_at` DB e non
   contiene credenziali, URL o payload Kafka.
7. **Lifecycle reale.** Given dipendenze sane e consumer assegnato, When si
   chiamano i probe, Then `/live=204`, `/ready=204` e `/metrics` espone dati;
   dipendenza/assignment assente rende solo readiness 503 e non causa restart
   storm tramite liveness.
8. **Deployment worker pool.** Given render standard con applicazioni abilitate,
   When si ispeziona Helm, Then esiste `Deployment/ingestion` a 4 repliche con
   Service metrics, PDB/probe/resources, identita' Kafka propria, Secret
   enumerato e NetworkPolicy solo verso Kafka/PostgreSQL/MinIO; il CronJob
   transitorio non e' renderizzato e dev riduce onestamente le repliche.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| header scope mancante, duplicato, non UTF-8, blank/control/>256 byte | permanent deny prima di MinIO/parser/DB; DLQ sanitizzata |
| scope presente anche nel body/additional property | schema reject; il body non diventa autorita' |
| payload >64 KiB o JSON/schema/versione invalida | permanent deny/DLQ; nessun echo del payload |
| path assoluto, `.`/`..`, backslash, control o >1024 byte | permanent deny prima di costruire la key |
| commit/digest/UUID/size invalido o size oltre config | permanent deny |
| object key configurabile dal messaggio o endpoint non trusted | impossibile per tipo/schema; key derivata e endpoint solo env |
| GET object 404/digest/size/UTF-8 mismatch | permanent failure/DLQ; nessun write |
| MinIO timeout/5xx, Kafka transport, PostgreSQL unavailable | transient; no commit, readiness 503, retry bounded |
| parse panic per input ostile | catturato al command boundary, permanent `parse_failed`; il processo resta vivo |
| receipt esistente con fingerprint uguale | `Duplicate`, zero nuove righe outbox |
| receipt esistente con fingerprint diverso | `command_id_conflict`, zero write canonici |
| DLQ publish fallisce | original offset non committato |
| TLS/CA/cert/key/Secret assente | startup fail-closed; nessun PLAINTEXT/default credential |
| shutdown SIGTERM durante comando | stop polling, deadline grace; offset solo se commit completato |

## 5. Threat model e non-goals

**Attori:** producer esterno ostile senza ACL, producer di sistema compromesso,
tenant/client che controlla sorgente, payload/path malevolo, LLM compromesso,
operatore con Secret errato. **Asset:** scope tenant/repo/ACL, codice sorgente,
PostgreSQL/outbox, credenziali Kafka/MinIO, disponibilita' del worker. **Confini:**
la mTLS Kafka + ACL autentica il producer; i tre header costituiscono commit
context system-to-system; body e sorgente sono dati non fidati; endpoint,
bucket, limiti e credenziali sono config server-side. **Attacchi:** confused
deputy, forged body scope, SSRF/object-key traversal, decompression/size bomb,
poison pill, replay, command-id collision, offset-before-commit, secret leak in
metric/log/DLQ. **Mitigazioni:** topic/identity literal, schema chiuso, scope
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

- Unit Rust: schema/header/path/key/fingerprint; scope body rifiutato; ordering
  stabile; DLQ reason allow-list; payload/failure non compaiono in log/metric.
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
      zero nuovi effetti e conflitto produce zero write.
- [x] Offset originale solo dopo DB commit o DLQ publish confermata; fault
      transienti conservano backlog e readiness 503.
- [x] Provenance soddisfa `repo/commit_sha/path/ingested_at` senza alterare i
      contratti golden/eval storici.
- [x] `/live`, `/ready`, `/metrics` e shutdown sono reali e testati.
- [x] Helm rende Deployment ingestion standard=4, max dichiarato=40, identity,
      ACL, Secret e NetworkPolicy least-privilege; nessun CronJob ingestion.
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
`verified` resta subordinato a CI e review della PR.
