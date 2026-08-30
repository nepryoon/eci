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
3. **Ruolo `eci_cdc` gestito da CNPG e publication pre-creata.** Insufficiente
   da solo: se CDC è abilitato dopo che la migration è stata registrata, un
   grant condizionale al login role viene perso definitivamente.
4. **Privilege carrier passwordless sempre riconciliato.** Scelto: la migration
   concede SELECT a un ruolo NOLOGIN stabile e CNPG rende il login role membro
   soltanto quando CDC è abilitato.

## Decisione

CloudNativePG riconcilia sempre `eci_cdc_outbox_reader` come ruolo NOLOGIN,
senza password né privilegi amministrativi. La migration
`0005_cdc_publication` eseguita dal database owner crea
`eci_outbox_publication` esclusivamente su `public.outbox` e concede SELECT a
questo privilege carrier quando presente. Compose/testcontainer non hanno il
reconciler CNPG e conservano il percorso development owner-based preesistente;
la migration resta quindi portabile e non crea ruoli infrastrutturali. Nel
chart Kubernetes, invece, il carrier è un invariante sempre renderizzato e i
test ne verificano la presenza anche con CDC disabilitato. Quando CDC è
abilitato, CNPG riconcilia `eci_cdc` dal Secret dedicato
`eci-postgres-cdc`, con `login=true`, `replication=true`, membership in
`eci_cdc_outbox_reader` e tutti gli attributi amministrativi disabilitati.
Quando CDC è disabilitato il chart mantiene `eci_cdc` in `managed.roles` con
`ensure: absent`, così anche un passaggio enabled → disabled elimina il login
role storico invece di lasciarne LOGIN, REPLICATION e grant ereditati.
Debezium usa
`publication.autocreate.mode=disabled`, quindi non deve possedere tabelle né
creare publication.
Poiché il profilo standard ha failover automatico su PostgreSQL 17, Debezium
crea lo slot con `slot.failover=true` e CNPG abilita la sincronizzazione dei
logical decoding slot insieme a `hot_standby_feedback` e
`sync_replication_slots`; una sola metà della configurazione non è accettata.

Il profilo dev genera una password casuale separata e la conserva nel Secret;
un upgrade riusa lo stesso valore. Produzione deve pre-provisionare il Secret
con chiavi `username=eci_cdc` e `password` prima del Cluster/Connect rollout.

## Conseguenze

- La readiness del worker non sostituisce la verifica di ruolo, publication e
  slot; il verifier controlla attributi e privilegi prima della registrazione.
- La compromissione Connect non consegna più le credenziali del database owner.
- La publication è un contratto SQL versionato e il rollback migration la
  rimuove e revoca il grant dal carrier senza eliminare ruoli CNPG o lo slot
  eventualmente esistente.
- Un’installazione iniziale con `cdc.enabled=false` applica comunque il grant
  al carrier NOLOGIN. Abilitare CDC in seguito eredita il privilegio senza
  riscrivere né fingere di rieseguire una migration già applicata.
- Disabilitare CDC riconcilia esplicitamente l'assenza del login role; la sola
  omissione dalla lista CNPG non sarebbe una revoca.

## Migrazione, rollback e sicurezza

Ordine iniziale: riconciliazione carrier CNPG, migration 0005, eventuale Secret
dedicato e riconciliazione login role, registrazione connector. In un upgrade
disabled→enabled i primi due passi sono già conclusi e CNPG aggiunge la
membership prima della registrazione. Un passo mancante deve fallire prima di
dichiarare CDC operativo. Il rollback del connector non elimina automaticamente slot o WAL;
l’operatore deve fermare Connect, verificare i consumer lag, rimuovere lo slot
solo se non più necessario e infine applicare la down migration. Password,
Secret data e DSN non entrano in log o artefatti.
