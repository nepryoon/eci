# ADR-0023 — Comando file Kafka autenticato per il worker ingestion

Stato: accepted
Data: 2026-08-30
Decisioni collegate: D1, D8, T7.1a / SPEC-067, ADR-0016

## Contesto

Il binario Rust ingestion esistente e' un CLI one-shot: legge un path locale,
riceve scope da variabili d'ambiente e termina. ADR-0016 lo rappresenta
onestamente come Job sospeso, ma l'ADD Modulo 3 §1.1, §1.2 e D8 richiede un
worker pool stateless 4–40, alimentato in modo asincrono, con backpressure
Kafka. Il nuovo ingresso crea un trust boundary: tenant, repository e ACL non
possono provenire dal sorgente, dal path, da un client finale o da output LLM.

Il worker deve inoltre conservare i sorgenti su object storage e tollerare la
redelivery at-least-once senza duplicare righe outbox. Kafka e MinIO sono gia'
componenti prescritti e distribuiti da SPEC-062; PostgreSQL resta l'unica
source of truth canonica.

## Opzioni considerate

1. **Loop sul CLI/PVC.** Scartato: non ha coda durabile, ownership del lavoro,
   offset, backpressure o idempotenza; la readiness sarebbe fittizia.
2. **API gRPC sincrona con coda PostgreSQL.** Scartata: contraddice il piano
   ingestion Kafka asincrono, aggiunge un listener pubblico e obbliga a
   progettare retry e admission separati.
3. **Evento Kafka con sorgente inline.** Scartato: lega la dimensione dei file
   ai limiti del broker, duplica sorgenti nel log e non rende MinIO la copia
   sorgente durevole.
4. **Evento Kafka con URL/presigned URL.** Scartato: un URL nel payload crea un
   vettore SSRF e rende endpoint/credenziali autorita' controllata dal
   producer.
5. **Comando Kafka per file + object key derivata.** Scelto: il producer di
   sistema deposita il blob in un bucket configurato e pubblica soltanto
   identita' del comando, commit, path, digest e size. Il worker deriva la key
   da scope autenticato e campi validati; nessun host/bucket/key arbitrario e'
   accettato dal messaggio.

## Decisione

Si introduce il contratto JSON `IngestionFileCommand` v1 sul topic literal
`eci.ingestion.file.v1`, partizionato per SHA-256 length-prefixed di
`tenant_id/repository/path`. Il valore contiene `command_id`, `commit_sha`,
`path`, `source_sha256` e `source_size_bytes`; non contiene scope, URL,
credenziali o contenuto sorgente. `tenant_id`, `repository` e `acl_group`
arrivano esclusivamente dagli header Kafka `eci-tenant-id`,
`eci-repository`, `eci-acl-group`. Solo il `KafkaUser` mTLS dedicato
`ingestion-commit-producer` puo' scrivere il topic; il worker possiede una
identita' distinta read-only sul topic/gruppo e write-only sulla DLQ.

Il bucket e l'endpoint MinIO sono configurazione trusted del workload. La key
e' derivata deterministicamente come
`sources/v1/<sha256(lp(scope))>/<commit_sha>/<sha256(lp(path-utf8))>` e deve coincidere
con quella usata dal producer. Entrambi gli hash usano componenti con lunghezza
prefissata. La key ha sempre 181 byte ASCII, resta entro il limite S3 di 1024
byte anche per nomi multibyte e non espone il path del repository. Il worker
rifiuta path assoluti, segmenti `.`/`..`, NUL/control character, backslash e
path oltre 1024 code point Unicode, esattamente la semantica `maxLength` di JSON
Schema 2020-12; conserva i path UTF-8 supportati dai repository senza confondere
byte e caratteri. Rifiuta inoltre commit non SHA-1 lower-hex, digest non SHA-256
lower-hex e file oltre il limite configurato. Scarica con un client S3
autenticato, applica un limite streaming, verifica byte count, SHA-256 e UTF-8
prima del parser.

Ogni replica consuma con `enable.auto.commit=false` e al massimo un comando per
partizione in elaborazione. Il write canonico e una receipt
`ingestion_command_receipt(command_id, fingerprint, completed_at)` avvengono
nella stessa transazione PostgreSQL. Una redelivery con fingerprint identico
e' un no-op; lo stesso `command_id` con fingerprint diverso e' un conflitto
permanente. L'offset e' committato sincronicamente soltanto dopo commit DB o
dopo pubblicazione confermata di un failure envelope sanitizzato in
`eci.ingestion.file.v1.DLQ`. Errori transitori di Kafka, PostgreSQL o MinIO non
committano il record e mettono la replica non-ready durante backoff bounded.

Il processo espone HTTP solo nel cluster su porta metrics: `/live` prova il
lifecycle locale, `/ready` riflette consumer assignment e check bounded di
Kafka/PostgreSQL/MinIO, `/metrics` espone conteggi e stato senza identita'. Il
Deployment iniziale ha quattro repliche e puo' scalare fino a quaranta; T7.2
introduce l'HPA CPU.

## Integrita' transazionale e provenienza

La receipt non sostituisce l'outbox e non e' una vista materializzata. Il
commit context arricchisce ogni provenance con `repo`, `commit_sha`, `path` e
`ingested_at`, oltre allo scope security server-side. Il timestamp e' generato
una volta dal database nella transazione. MinIO e PostgreSQL non condividono
una transazione: il blob deve esistere prima della pubblicazione e resta
immutabile; un crash prima del commit DB ripete soltanto GET/verifica, mentre
un crash dopo il commit DB viene assorbito dalla receipt.

## Conseguenze

- Il command ingress e il CDC outbox restano topic e identita' separati.
- Lo scope non e' ricavabile da path, body, LLM o client finale.
- Il lavoro e' durevole, partizionato e naturalmente soggetto a backpressure.
- La granularita' per file consente fault isolation e scaling CPU, ma non rende
  atomico un intero commit multi-file; la vista puo' avanzare file per file.
- Questa decisione copre solo upsert di file supportati. Le eliminazioni e la
  risoluzione compiler-accurate cross-file richiedono SPEC dedicate; non sono
  simulate cambiando le attese o ignorando tombstone.

## Rollback e sicurezza

Il rollback scala a zero il Deployment e ripristina il CronJob sospeso senza
cancellare topic, oggetti MinIO, receipt, entita' o outbox. La migration down
e' ammessa solo dopo aver verificato che nessun worker usa la tabella; non
cancella dati canonici. Credenziali Kafka/MinIO/PostgreSQL provengono solo da
Secret per-workload. Mancanza di header, TLS, ACL, bucket o receipt table
fallisce chiuso; nessun fallback plaintext, bucket/path locale o scope di
default e' consentito.

La derivazione hash del path sostituisce la precedente key percent-encoded
prima del merge di T7.1a; non esiste evidenza di oggetti di produzione con il
layout precedente. Un producer candidato gia' avviato deve ripubblicare il blob
sotto la key hash prima di inviare il comando. Un rollout futuro con oggetti
legacy richiederebbe dual-write o una migrazione esplicita producer-side: il
worker non tenta key alternative, per non trasformare input non fidato in una
superficie di probing. Il rollback applicativo deve quindi ripristinare insieme
producer e worker; le receipt gia' completate restano valide perche' il loro
fingerprint canonico include il path originale, non la key MinIO derivata.

## Review avversariale

La seconda passata ha tentato di invalidare la decisione rispetto agli
invarianti repository. Esito: nessuna contraddizione ADD; lo scope resta
autenticato e fail-closed, il body non amplia tenant/repo/ACL, PostgreSQL resta
l'unico writer canonico e nessuna vista riceve write diretti. Receipt+offset
manuale preservano idempotenza e at-least-once; non vengono introdotti
traversal, decisioni probabilistiche o modifiche al budget del query plane. Il
nuovo contratto e la migration sono espliciti e autorizzati da questo ADR,
mentre tombstone e atomicita' multi-file restano limiti dichiarati, non
garanzie implicite.
