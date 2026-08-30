# ADR-0018 — Boundary dei runtime Python library-only e del batch GDS scoped

Stato: accepted
Data: 2026-08-30
Decisione collegata: T7.1 / SPEC-062; follow-up T7.1c

## Contesto

`services/verification` e `services/summarization` contengono librerie e test,
ma non un entrypoint server né listener 8080/9090. Un Deployment senza comando
terminerebbe o resterebbe Not Ready. `tools/gds-impact` è invece un CLI batch
reale, ma richiede sempre entry node, tenant, repository e gruppo ACL; il
precedente CronJob schedulato senza argomenti falliva prima di contattare Neo4j.

## Opzioni considerate

1. **Wrapper/sleep e probe TCP.** Scartato: fabbrica servizi inesistenti.
2. **Argomenti GDS nei values Helm.** Scartato: renderebbe una configurazione di
   deploy trust source per scope security e lascerebbe scope nei release data.
3. **Server nuovi in T7.1.** Scartato: cambia API, autenticazione e lifecycle
   senza SPEC dedicata.
4. **Esclusione server + template batch suspended/scoped.** Scelta: rappresenta
   soltanto comportamenti eseguibili e mantiene scope in un Secret dedicato.

## Decisione

T7.1 non renderizza verification o summarization come Deployment, Service, PDB
o NetworkPolicy. T7.1c deve specificare e implementare API autenticate
long-running, propagazione deadline, probe reali e graceful shutdown.

`gds-impact` resta un CronJob sospeso usabile solo come template. I quattro
argomenti obbligatori sono espansi da variabili lette da
`eci-runtime-gds-impact`, insieme alle credenziali Neo4j. Un operatore prepara
un Secret per lo scope interno autorizzato e clona un Job bounded; il schedule
installato non parte mai autonomamente.

## Conseguenze

- Il catalogo non dichiara listener Python inesistenti.
- GDS non esegue job destinati a fallire e non prende scope da testo/LLM.
- T7.1 resta `implemented` finché T7.1a–c chiudono tutti i gap runtime.

## Rollback e sicurezza

Verification/summarization tornano nel catalogo solo dopo T7.1c e immagini per
digest. GDS può essere rimosso senza toccare Neo4j o Secret; è vietato
unsuspendere il template condiviso, riusare scope tra tenant o passare
`allowed_repos`/ACL come argomenti derivati dal caller non autenticato.
