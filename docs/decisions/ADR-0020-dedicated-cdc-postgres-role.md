# ADR-0020 — Ruolo PostgreSQL dedicato per CDC logico

Stato: accepted
Data: 2026-08-30
Decisioni collegate: D8, T7.1 / SPEC-062

## Contesto

Kafka Connect usava il ruolo applicativo `eci`, owner del database, ma tale
ruolo non possedeva `REPLICATION`: il worker poteva diventare Ready senza poter
creare lo slot logico. Aggiungere `REPLICATION` al database owner avrebbe
risolto la sola disponibilità ampliando ulteriormente l’impatto di una
compromissione del connector.

Debezium richiede uno slot logico, lettura della tabella outbox e una
publication. La REST Connect è già loopback-only per ADR-0019, ma il processo
deve comunque ricevere credenziali database a privilegio minimo.

## Opzioni considerate

1. **Aggiungere `REPLICATION` a `eci`.** Scartato: conserva credenziali da
   database owner nel worker CDC.
2. **Rendere il connector superuser.** Scartato: viola least privilege.
3. **Ruolo `eci_cdc` gestito da CNPG e publication pre-creata.** Scelto: il
   worker ha solo LOGIN, REPLICATION e SELECT sulla singola outbox.

## Decisione

CloudNativePG riconcilia `eci_cdc` da un Secret dedicato
`eci-postgres-cdc`, con `login=true`, `replication=true` e tutti gli attributi
amministrativi disabilitati. La migration `0005_cdc_publication` eseguita dal
database owner crea `eci_outbox_publication` esclusivamente su
`public.outbox` e concede SELECT al ruolo se presente. Debezium usa
`publication.autocreate.mode=disabled`, quindi non deve possedere tabelle né
creare publication.

Il profilo dev genera una password casuale separata e la conserva nel Secret;
un upgrade riusa lo stesso valore. Produzione deve pre-provisionare il Secret
con chiavi `username=eci_cdc` e `password` prima del Cluster/Connect rollout.

## Conseguenze

- La readiness del worker non sostituisce la verifica di ruolo, publication e
  slot; il verifier controlla attributi e privilegi prima della registrazione.
- La compromissione Connect non consegna più le credenziali del database owner.
- La publication è un contratto SQL versionato e il rollback migration la
  rimuove senza eliminare il ruolo o lo slot eventualmente esistente.

## Migrazione, rollback e sicurezza

Ordine: Secret dedicato, riconciliazione ruolo CNPG, migration 0005,
registrazione connector. Un passo mancante deve fallire prima di dichiarare CDC
operativo. Il rollback del connector non elimina automaticamente slot o WAL;
l’operatore deve fermare Connect, verificare i consumer lag, rimuovere lo slot
solo se non più necessario e infine applicare la down migration. Password,
Secret data e DSN non entrano in log o artefatti.
