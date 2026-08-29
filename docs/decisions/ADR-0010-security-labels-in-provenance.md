# ADR-0010 — Etichette di sicurezza additive nella provenance di ingestion

Stato: accettata · Data: 2026-08-29

## Contesto

T6.3 deve applicare lo stesso scope autenticato (`tenant_id`, repository e
gruppi ACL) prima del retrieval in Neo4j, Qdrant e OpenSearch. Il contratto D2
porta già `repo` nella provenance, ma non porta tenant o gruppo ACL; inoltre
l'implementazione di ingestion precedente emetteva intenzionalmente una
provenance minimale contenente il solo `path`. I sink non possono ricostruire
tenant o ACL dal contenuto, dal prompt o da valori statici di processo senza
creare un confused deputy quando un topic contiene più tenant.

Le alternative considerate sono:

1. configurare tenant/repo/ACL staticamente in ciascun sink: semplice, ma
   associa lo scope al consumer anziché all'evento e può etichettare con lo
   scope sbagliato eventi multi-tenant;
2. aggiungere header Kafka di sicurezza: coerente con il piano system-to-system,
   ma gli header non sono persistiti nel payload storico né nei record
   PostgreSQL e possono divergere durante replay/DLQ;
3. aggiungere le etichette alla provenance persistita e propagata negli eventi:
   mantiene dati e scope nello stesso record idempotente ed è verificabile a
   ogni sink.

## Decisione

Si adotta l'alternativa 3. `tenant_id` e `acl_group` diventano proprietà
additive di `provenance`; `repo` viene popolato dall'ingestion context insieme
alle due nuove proprietà. L'ingestion entrypoint richiede esplicitamente
`ECI_TENANT_ID`, `ECI_REPOSITORY` ed `ECI_ACL_GROUP`: non esistono default di
sicurezza. Le API interne di persistenza ricevono un `IngestionScope` validato,
che viene scritto nella stessa transazione dei nodi, chunk e outbox.

I campi restano opzionali nel JSON Schema per compatibilità di lettura e replay
con gli eventi storici già prodotti. Questa compatibilità non è un default
allow: T6.3 rende i record senza tutte le etichette non interrogabili perché
ogni query usa confronti positivi obbligatori. I sink nuovi rifiutano o
quarantinano eventi nuovi privi di scope invece di inventarlo. La migrazione
dei dati preesistenti deve essere effettuata da una procedura amministrativa
esplicita e auditata prima di renderli visibili; nessun backfill è inferito dal
path o dall'identità del consumer.

Nei materialized view le proprietà sono piatte e indicizzabili:

- Neo4j `CodeNode.tenant_id`, `.repo`, `.acl_group`;
- Qdrant payload `tenant_id`, `repo`, `acl_group`;
- OpenSearch document keyword `tenant_id`, `repo`, `acl_group`.

La provenance completa resta presente per citation e audit. Nessun valore
`expected_facts`, testo utente o output LLM entra in questa derivazione.

## Conseguenze

- Il contratto cambia solo in modo additivo; i consumer precedenti continuano
  a deserializzare eventi nuovi perché ignorano proprietà sconosciute.
- Una nuova ingestion senza scope valido fallisce prima della transazione.
- Eventi storici privi di scope restano archiviabili ma invisibili al query
  plane fail-closed; la baseline T5.6 e i suoi artefatti non vengono modificati.
- I tre sink devono propagare esattamente le etichette ricevute e non accettano
  override locali o request-derived.
- La difesa nativa Neo4j Enterprise e OpenSearch DLS resta un secondo livello;
  i predicati applicativi sono comunque obbligatori su ogni query.

## Compatibilità e migrazione

La modifica è backward-compatible a livello di parsing JSON ma security-
strict a livello di query. I nuovi producer emettono sempre i campi; i record
legacy non diventano accessibili finché un amministratore non esegue un
backfill con una mappatura tenant/repo/ACL autorevole. Rollback del producer:
ritornare alla versione precedente rende i nuovi record invisibili, non
cross-tenant. Rollback dei consumer: le proprietà additive vengono ignorate,
senza perdita dei payload storici.

## Impatto sicurezza

La decisione elimina l'uso di scope statici nei sink e lega l'etichetta al dato
nella transazione originale. Il confine di fiducia è l'ingestion system-to-
system; il query plane usa esclusivamente `SecurityContext` JWT-validato.
Campi assenti, vuoti o malformati non equivalgono mai a wildcard. La migrazione
non autorizza post-filtering e non modifica la regola fail-closed.

## Evidenza di attuazione

T6.3 implementa la decisione nel producer Rust, nei tre sink e nel query plane.
Il JSON Schema resta additivo; i test di integrazione usano scope espliciti e
inseriscono record cross-tenant che devono rimanere invisibili.
