# ADR-0019 — Isolamento dei retry consumer e del control plane Kafka Connect

Stato: accepted
Data: 2026-08-30
Decisioni collegate: T3.3 / SPEC-035; T7.1 / SPEC-062

## Contesto

SPEC-035 definiva il retry sul topic primario. In un broker con identità e ACL
per workload questo obbliga ogni consumer ad avere `Write` sul topic prodotto
da Kafka Connect. Un consumer compromesso potrebbe quindi creare eventi
primari indistinguibili per origine e agire da confused deputy verso altri
consumer. Il retry-count non costituisce una prova di provenance.

Kafka Connect esponeva inoltre la REST API senza autenticazione tramite un
Service namespace-wide. Il processo usa `EnvVarConfigProvider` e possiede la
password PostgreSQL: una chiamata REST interna non autorizzata poteva creare o
modificare connector e indurlo a usare credenziali affidate al processo.

## Opzioni considerate

1. **Conservare i topic primari e affidarsi agli header.** Scartato: gli header
   sono controllati dal producer e non ripristinano la provenance.
2. **Aggiungere autenticazione custom a Kafka Connect REST.** Scartato per il
   profilo dev: introduce proxy/plugin e una nuova superficie secret senza
   necessità di una API di rete continua.
3. **Retry per consumer e REST loopback-only.** Scelta: usa ACL literal già
   previste, mantiene il data plane mTLS e rimuove il control plane dalla rete.

## Decisione

Ogni runtime consumer usa il topic deterministico
`{primary}.retry.{consumer}`. L'identità può leggere il topic primario e il
proprio retry topic, ma può scrivere soltanto il proprio retry topic e la DLQ
originaria. Kafka Connect resta l'unico writer dei quattro topic primari. I
topic sono dichiarati come `KafkaTopic`; auto-create resta disabilitato.

La zero-value di `resilience.Config` conserva il comportamento SPEC-035 per
compatibilità e per la baseline verificata. I runtime impostano sempre il
suffisso. Quando leggono un retry, il wrapper rimuove soltanto il proprio
suffisso esatto prima di chiamare `ProcessFunc`; nessun fuzzy matching o
header determina il topic di destinazione.

Kafka Connect configura `LISTENERS=http://127.0.0.1:8083`, non ha Service e usa
probe `exec` su loopback. È escluso dalla policy data-plane interna generale e
riceve soltanto DNS, Kafka mTLS 9093 e PostgreSQL 5432. La registrazione
post-migration avviene con accesso Kubernetes amministrativo tramite
`kubectl exec` e `curl` su loopback.

## Conseguenze

- La compromissione di un consumer non consente la scrittura di eventi
  primari o retry appartenenti a un altro consumer.
- Retry e DLQ mantengono ordinamento/at-least-once già documentati; per i retry
  esiste un topic aggiuntivo per coppia primary/consumer.
- La REST API Connect non è raggiungibile da pod o ingress; Kubernetes RBAC e
  audit governano l'operazione amministrativa.
- Le metriche T3.3 mantengono gli outcome esistenti.

## Migrazione, rollback e sicurezza

Prima del rollout devono essere Ready tutti i retry topic e le nuove ACL. I
consumer vengono poi aggiornati per sottoscrivere primary+retry; solo dopo si
rimuove `Write` sui primary. Helm/Strimzi riconciliano questa sequenza nel dev
cluster e lo smoke prova sia publish retry consentito sia publish primary
negato.

Il rollback applicativo richiede ripristinare insieme ACL e configurazione
consumer; non si deve avviare un binario storico privo di retry suffix dopo la
rimozione della relativa ACL. Il Service REST non va ripristinato: un futuro
control plane di rete richiede SPEC/ADR con autenticazione forte, autorizzazione
e protezione dei secret. Nessun topic storico o dato viene eliminato.
