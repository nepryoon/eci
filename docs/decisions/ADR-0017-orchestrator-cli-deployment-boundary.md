# ADR-0017 — Boundary del CLI orchestrator fuori dai Deployment

Stato: accepted
Data: 2026-08-30
Decisione collegata: T7.1 / SPEC-062; follow-up T7.1b

## Contesto

L'ADD Modulo 3 e D8 descrive l'orchestrator come servizio stateless della query
plane, con API autenticata, streaming e repliche scalabili. Il codice realmente
presente in `services/orchestrator` espone però soltanto il comando `eci`: i
sottocomandi `ask` ed `eval-golden` eseguono un'operazione e terminano. Non
esiste un server, non viene aperta la porta 8080/9090 e non esiste un contratto
di readiness HTTP.

Renderizzare questo artefatto come Deployment produrrebbe un rollout che entra
in crash loop o non diventa mai Ready. Aggiungere un server in T7.1 cambierebbe
interfaccia, trust boundary, streaming e lifecycle senza SPEC dedicata.

## Opzioni considerate

1. **Rilanciare il CLI in loop.** Scartato: inventa lifecycle e readiness e non
   fornisce un'interfaccia richiesta dall'ADD.
2. **Aggiungere un wrapper HTTP minimale in T7.1.** Scartato: creerebbe un
   contratto non revisionato e rischierebbe di fidarsi di scope caller-provided.
3. **Renderizzare il Deployment disabilitato o con `sleep`.** Scartato: sarebbe
   evidenza fittizia di un servizio inesistente.
4. **Escludere il workload e tracciare il follow-up.** Scelta: il chart dichiara
   soltanto processi con entrypoint e probe reali.

## Decisione

T7.1 non renderizza orchestrator come Deployment, Service, PDB o NetworkPolicy.
Il CLI resta disponibile per eval CPU/GPU e invocazioni controllate, ma non è
evidenza di un runtime Kubernetes. T7.1b deve specificare e implementare un
server long-running che riceve SecurityContext soltanto dal gateway autenticato,
propaga deadline, supporta lo streaming richiesto, espone readiness/liveness
reali e termina con graceful shutdown bounded. Solo dopo T7.1b T7.1 può essere
marcato `verified`; T7.3 dipende dal runtime per una traccia end-to-end reale.

## Conseguenze

- Il catalogo applicativo non dichiara un workload che non può avviarsi.
- T7.1 resta `implemented`, non `verified`, insieme al gap ingestion ADR-0016.
- T7.1b richiederà una SPEC e test security/failure-path prima di cambiare il
  chart; un nuovo contratto condiviso richiederà ADR dedicata.

## Rollback e sicurezza

Il rollback di questa decisione consiste nel reintrodurre il workload soltanto
dopo che T7.1b fornisce un'immagine per digest, entrypoint server e probe reali.
È proibito passare `allowed_repos`, tenant o gruppi ACL da testo utente/LLM,
accettare metadata interni forgiabili o degradare a default-allow se gateway,
OPA o dipendenze non sono disponibili.
