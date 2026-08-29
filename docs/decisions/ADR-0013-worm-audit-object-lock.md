# ADR-0013 — Audit WORM su object storage con retention COMPLIANCE

Stato: accepted
Data: 2026-08-29
Decisione collegata: T6.5 / SPEC-059

## Contesto

L'ADD Modulo 3 §2.6.5 richiede un audit log immutabile append-only/WORM di
query e accessi. Un file aperto in append, una tabella SQL senza `UPDATE` nel
codice applicativo o il solo versioning non costituiscono WORM: un operatore
privilegiato può ancora riscrivere o cancellare la storia. Il repository
include già MinIO come object storage S3-compatible nello stack di sviluppo.

Il Verification Service deve inoltre fallire chiuso se non riesce a rendere
durevole l'evento di decisione. L'audit non può contenere risposta, query,
snippet o token, ma deve conservare identità autenticata, scope, decisione e
riferimenti necessari alla ricostruzione forense.

## Opzioni considerate

1. **File JSONL append-only.** Semplice, ma permessi filesystem e rotazione non
   impediscono modifica o cancellazione. Scartata.
2. **Tabella PostgreSQL con trigger anti-UPDATE/DELETE.** Migliora il controllo
   applicativo, ma owner/superuser può disabilitare il trigger; non realizza il
   requisito WORM. Scartata.
3. **Kafka compact-disabled come archivio.** Il log è append-oriented, ma
   retention amministrativa e cancellazione topic non garantiscono
   immutabilità. Scartata.
4. **S3 Object Lock in modalità COMPLIANCE.** Ogni evento è un oggetto
   versionato con retention esplicita. Prima della scadenza nessun utente,
   incluso root MinIO, può ridurre la retention o eliminare la versione.
   È l'opzione scelta.

## Decisione

Il backend di produzione è un bucket dedicato creato con Object Lock abilitato
alla creazione. Ogni evento viene serializzato come JSON canonico UTF-8 e
caricato con:

- chiave univoca
  `audit/v1/YYYY/MM/DD/<event_id>-<sha256_payload>.json`;
- Object Lock mode `COMPLIANCE`;
- `retain_until = recorded_at + ECI_AUDIT_RETENTION_DAYS`;
- retention minima configurabile di 1 giorno e default di 365 giorni;
- verifica della retention restituita dal server prima di considerare
  l'append riuscito.

Il bucket viene inizializzato idempotentemente. Se esiste già, il servizio
verifica che Object Lock sia abilitato; non prova ad abilitarlo retroattivamente
(S3 lo vieta) e fallisce chiuso. L'applicazione espone soltanto `PutObject`,
`GetObjectRetention` e le operazioni di bootstrap strettamente necessarie; non
riceve permessi di delete, bypass governance o modifica/riduzione retention.

Il payload contiene schema version, event id/timestamp, trace id, tenant/user
autenticati, repository/gruppi autorizzati, attempt, outcome, issue codes e
decisioni sulle citazioni. Non contiene testo della query, risposta, sorgente,
snippet, credenziali o JWT. JSON canonico e digest rendono evidente qualunque
alterazione fuori dal perimetro WORM.

MinIO documenta che Object Lock richiede versioning e deve essere abilitato
alla creazione del bucket; la modalità COMPLIANCE impedisce modifica o
cancellazione fino alla scadenza anche all'utente root:
https://min.io/docs/minio/linux/administration/object-management/object-retention.html

## Conseguenze

- L'esito di verifica non viene restituito se l'append o la verifica retention
  falliscono: non esiste una modalità degradabile senza audit.
- Oggetti piccoli e unici evitano update logici; anche un'improbabile collisione
  di chiave produce una nuova versione senza distruggere la precedente.
- Una cancellazione senza version id può creare un delete marker, ma non
  rimuove la versione WORM. L'integrazione verifica la versione esplicita e
  prova che la sua cancellazione viene negata.
- Il costo cresce con il numero di richieste; lifecycle può rimuovere versioni
  solo dopo la retention. Capacity planning e retention legale restano
  configurazione operativa, non input del chiamante.

## Rollback, sicurezza e compatibilità

Il rollback del codice non rimuove né rende mutabili gli eventi già bloccati.
Il bucket non deve essere riutilizzato da componenti non-audit. Credenziali e
endpoint provengono da secret/config di deployment, mai dal request body.
Un backend indisponibile blocca la risposta verificata, preservando il
fail-closed. Non cambia alcun contratto condiviso: le nuove interfacce sono
interne al Verification Service.

