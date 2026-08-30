# ADR-0016 — Boundary transitorio del CLI ingestion come Job sospeso

Stato: accepted
Data: 2026-08-30
Decisione collegata: T7.1 / SPEC-062; follow-up T7.1a

## Contesto

L'ADD Modulo 3 §1.1 e D8 prescrive ingestion stateless come worker pool
`Deployment` CPU-bound, scalabile 4–40. Il binario realmente presente in
`services/ingestion`, però, è il CLI sincrono one-shot definito dalle SPEC-014 e
SPEC-010: legge un file locale, richiede scope system-to-system esplicito,
scrive PostgreSQL/outbox e termina. Non apre la porta 9100 e non possiede ancora
un contratto di coda/API per ricevere commit.

Renderizzarlo come Deployment con probe TCP significherebbe dichiarare un
servizio inesistente. Aggiungere in T7.1 una coda, un endpoint o un loop di
watch senza SPEC/contratto introdurrebbe invece una decisione architetturale e
un trust boundary non revisionati.

## Opzioni considerate

1. **Deployment che rilancia il CLI in loop.** Scartato: duplica outbox,
   fabbrica lifecycle/readiness e non ha una sorgente di lavoro durabile.
2. **Nuova API o coda in T7.1.** Scartata: cambia interfaccia e autenticazione
   oltre lo scope deployment, senza contratto approvato.
3. **Non renderizzare ingestion.** Scartata: nasconde un limite operativo e non
   offre neppure un packaging fedele dell'eseguibile esistente.
4. **Template Kubernetes one-shot sospeso.** Scelta transitoria: rappresenta il
   processo reale senza eseguirlo automaticamente e rende espliciti snapshot e
   scope richiesti.

## Decisione

T7.1 fornisce `CronJob/ingestion-template` con `suspend: true`, usato solo come
template clonabile per Job. Il pod monta read-only il PVC esterno
`eci-ingestion-source`, riceve il path come unico argomento e legge soltanto da
`eci-runtime-ingestion` le quattro chiavi enumerate: `POSTGRES_DSN`,
`ECI_TENANT_ID`, `ECI_REPOSITORY`, `ECI_ACL_GROUP`. Non espone Service, non ha
probe inventate, non usa token Kubernetes e resta soggetto a deadline,
resources e NetworkPolicy puntuali.

Il template non soddisfa il worker pool D8 e non è evidenza per HPA ingestion.
T7.1a deve specificare e implementare l'ingresso durabile/autenticato, il
lifecycle long-running, readiness reale, backpressure e concorrenza; solo allora
ingestion torna Deployment e T7.1 può diventare `verified`. T7.2 dipende da
T7.1a per la propria parte HPA parsing.

## Conseguenze

- Il chart non mente sul comportamento del binario corrente.
- Ogni esecuzione è intenzionale e usa uno snapshot/scope predisposto; il
  template installato non parte da solo.
- Non viene creato un nuovo contratto o trust source in questa PR.
- T7.1 resta `implemented`, non `verified`, finché T7.1a non chiude la deviazione
  dal Deployment 4–40 dell'ADD.

## Rollback e sicurezza

Il rollback elimina il template sospeso senza toccare PVC sorgente,
PostgreSQL o outbox. Non è consentito rimuovere `suspend`, riusare Secret di
sibling, passare scope da testo utente/LLM o sostituire il PVC read-only con un
fixture incorporato. Un futuro worker deve derivare il commit context soltanto
da un canale system-to-system autenticato e fallire chiuso su scope mancante.
