# ADR-0022 — Marker di completamento dopo i write idempotenti dei sink

Stato: accepted
Data: 2026-08-30
Decisioni collegate: D1, T1.3 / SPEC-015, T3.1 / SPEC-033,
T3.2 / SPEC-034, T3.3 / SPEC-035, T7.1 / SPEC-062

## Contesto

Neo4j, Qdrant e OpenSearch non condividono una transazione con PostgreSQL.
Registrare `(event_id, consumer_name)` in `processed_events` prima del write
esterno crea quindi un falso completamento: se il write fallisce, il wrapper
pubblica il retry e committa l'originale, ma il retry vede il marker e salta il
write. La materializzazione resta mancante e il messaggio non raggiunge la
DLQ. Questa sequenza contraddice la garanzia at-least-once e il requisito ADD
di sink idempotenti.

## Opzioni considerate

1. **Marker prima del write.** Scartata: evita write duplicati ma perde
   definitivamente aggiornamenti quando il datastore esterno fallisce.
2. **Stato `in_progress`/`completed` con lease e recovery.** Valida, ma richiede
   nuova DDL, clock/lease ownership e una procedura di recupero non necessaria
   finché i write esterni sono già idempotenti per identità canonica.
3. **Check del marker, write esterno idempotente, marker di completamento.**
   Scelta: può ripetere un write dopo crash o in concorrenza, ma non converte
   un fallimento in duplicato e converge sullo stesso stato materiale.

## Decisione

`sink-graph`, `sink-vector` e `sink-search` verificano prima l'eventuale marker
scoped definito da ADR-0021. Se assente eseguono rispettivamente `MERGE`, Qdrant
upsert o OpenSearch index con ID deterministico. Il `MERGE` delle relazioni
imposta il peso assoluto già aggregato nel payload e non lo risomma a ogni
redelivery. Qdrant usa `wait=true` e accetta come successo soltanto
`UpdateStatus_Completed`, non un semplice acknowledge asincrono. Le mutation
Neo4j calcolano `changed` sotto gli stessi lock degli entity endpoint e
incrementano la generation GDS soltanto quando proprietà, label, topology o
weight cambiano realmente; un MERGE identico ripetuto non invalida gli score.
Inseriscono il marker soltanto dopo il successo esterno applicato. Un errore di
inserimento del marker viene ritornato: la riconsegna ripete in sicurezza il
write idempotente.

Due consegne concorrenti possono entrambe eseguire il write; la chiave composta
di `processed_events` serializza soltanto il completamento. Il perdente
dell'`INSERT ... ON CONFLICT DO NOTHING` ritorna duplicate dopo avere applicato
lo stesso write idempotente. Nessun lock distribuito o assunzione exactly-once
viene introdotta.

## Conseguenze

- Un outage esterno non lascia un marker e il retry/DLQ resta operativo.
- Un crash tra write e marker può causare un write ripetuto, mai una perdita;
  MERGE/upsert/index per ID rendono la ripetizione convergente.
- Un acknowledge Qdrant non applicato non diventa completion marker; un retry
  Neo4j senza cambiamento materiale non avanza la generation GDS.
- I payload permanentemente invalidi continuano a essere scartati e committati
  senza marker, come previsto dalle SPEC storiche.
- Nessun contratto protobuf, schema dati o ACL cambia.

## Rollback, migrazione e sicurezza

Non serve migrazione DDL. I marker esistenti restano validi come attestazioni
storiche e non vengono cancellati o reinterpretati. Il rollback al marker
anticipato è vietato perché reintroduce perdita silenziosa; in emergenza si
può fermare un consumer senza alterare i marker.

La sequenza non amplia lo scope: tenant, repository e ACL continuano a
provenire dal payload CDC autenticato/validato e i client datastore conservano
le identità least-privilege. Regressioni con PostgreSQL reale e backend
irraggiungibili provano che Neo4j, Qdrant e OpenSearch non lasciano marker dopo
un write fallito. Qdrant reale prova l'upsert blocking/completed; Neo4j reale
simula la perdita del marker e prova che il MERGE identico conserva la
generation della partizione.
