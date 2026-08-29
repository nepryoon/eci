# SPEC-062 — Piattaforma Kubernetes/Helm e operatori per sviluppo
Stato: implemented
Task-tree: T7.1 · Servizi: `deploy/k8s`, tutti i workload ECI · ADD: Modulo 4 §1.3, §2.1–§2.3, D8
Contratti: nessuna modifica; `contracts/**` è consumato in sola lettura dalle immagini applicative

## 1. Obiettivo

Fornire una distribuzione Kubernetes dichiarativa, riproducibile e verificabile
per tutti i workload ECI e per gli store/operatori prescritti: Strimzi/Kafka
KRaft, Debezium/Kafka Connect, CloudNativePG/PostgreSQL 17, Neo4j, Qdrant e
OpenSearch Operator. Il
profilo production-like conserva replica, isolamento, storage e security
richiesti dall'ADD; un overlay `dev` riduce solo capacità e replica per un
cluster locale, senza trasformare semplificazioni locali in evidenza di HA o
RBAC Enterprise.

## 2. Interfaccia

```text
deploy/k8s/eci-platform/Chart.yaml
deploy/k8s/eci-platform/values.yaml
deploy/k8s/eci-platform/values-dev.yaml

task k8s:render     # helm template deterministico del profilo production-like
task k8s:validate   # render + lint + policy/schema checks CPU-only
task k8s:dev:up     # cluster kind + operatori pinned + secret runtime + workload dev
task k8s:dev:verify # readiness e connettività in-cluster
task k8s:dev:down   # elimina soltanto il cluster kind eci-dev
```

Valori pubblici minimi:

```yaml
global:
  imageRegistry: ghcr.io/nepryoon/eci
  imageTag: "<immutable git sha or release>"
  existingSecret: eci-runtime
  storageClass: ""
operators:
  strimziVersion: "1.2.0"
  cloudnativePGChartVersion: "0.29.0" # app 1.30.0
  openSearchOperatorVersion: "2.8.0"
dataPlane:
  postgres: {instances: 3, majorVersion: 17}
  kafka: {replicas: 3, kraft: true}
  neo4j: {edition: enterprise, coreReplicas: 3, gdsReplicas: 1}
  qdrant: {replicas: 3, replicationFactor: 2, shardNumber: 3}
  openSearch:
    replicas: 3
    securityPlugin: true
    adminCredentialsSecret: eci-opensearch-admin
    securityConfigSecret: eci-opensearch-security-config
```

I secret non sono valori Helm. Il chart usa esclusivamente
`secretKeyRef`/secret montati da `global.existingSecret` e dai due riferimenti
OpenSearch dedicati; il bootstrap dev li crea a runtime da input locali non
versionati e genera l'hash bcrypt richiesto dall'operator 2.8.0. Le immagini devono avere
tag immutabile; `latest`, tag vuoti e credenziali letterali sono rifiutati.

## 3. Comportamento

1. **Inventario completo.** Given i valori standard, When `task k8s:render`
   produce YAML, Then esistono i namespace `ingress`, `query-plane`,
   `gpu-plane`, `ingestion-plane`, `data-plane`, `observability`; Deployment,
   Service, account e PDB coprono Envoy/API helper, orchestrator, retrieval,
   verification, LLM gateway, summarization, semantic cache, tre sink,
   ingestion CPU, embedding/reranker/vLLM; GDS è un workload batch esplicito.
2. **Stateful production-like.** Given il profilo standard, When si renderizza,
   Then Kafka usa `KafkaNodePool` KRaft a 3 nodi/PV, CloudNativePG usa
   PostgreSQL 17 con 3 istanze, OpenSearch usa l'operator CR con Security
   plugin e 3 nodi, Kafka Connect usa Debezium pinned con TLS verso Strimzi e
   password PostgreSQL risolta dal Secret soltanto nel worker, mentre i valori
   Helm Neo4j e Qdrant descrivono core+read
   replica GDS e cluster distribuito a 3 repliche. Qdrant espone inoltre la
   configurazione di collection `shard_number=3`/`replication_factor=2` al job
   idempotente di bootstrap. MinIO standard usa quattro membri distribuiti,
   ciascuno con PVC da 100 GiB; il dev usa un solo StatefulSet con PVC da 1 GiB,
   mai repliche indipendenti su `emptyDir` dietro lo stesso Service.
3. **Disponibilità stateless.** Given un Deployment production-like, When si
   ispeziona il Pod template, Then rollout è `maxSurge=1/maxUnavailable=0`, PDB
   ha `minAvailable>=1`, esistono topology spread per zona e hostname,
   anti-affinity, requests/limits e probe coerenti con protocolli realmente
   esposti. I consumer privi di readiness applicativa usano startup/readiness
   `tcpSocket` sulla porta metriche, mai un endpoint inventato.
4. **Least privilege e segreti.** Given ogni oggetto renderizzato, When passa
   la policy statica, Then pod/container sono non-root, seccomp RuntimeDefault,
   privilege escalation e service-account token non necessari sono disabilitati
   nei workload production-like,
   capability sono eliminate e ogni password/API key proviene dal Secret
   esistente. Il solo Keycloak dev usa root filesystem scrivibile per la
   Quarkus augmentation di `start-dev`, resta UID non-root/capability-free e
   non è renderizzato nel profilo standard. NetworkPolicy default-deny e
   allow-list descrivono soltanto i flussi D8; nessun Ingress espone
   direttamente datastore o GPU. Poiché NetworkPolicy non seleziona il
   Service Kubernetes per nome e il punto di enforcement rispetto al DNAT
   dipende dal CNI, soltanto i pod operator/instance-manager noti possono
   uscire sulle porte API 443/6443; workload e datastore non ricevono egress
   HTTPS generale.
   OPA monta una copia byte-identica della policy canonica Compose e la carica
   esplicitamente al bootstrap; readiness TCP senza una decisione allow e una
   deny fail-closed verificabili non è considerata evidenza sufficiente.
5. **Overlay dev onesto.** Given `values-dev.yaml`, When il bootstrap gira su
   kind, Then usa repliche/storage/resource ridotti e Neo4j Community come
   previsto dagli ADR dev esistenti. I workload ECI senza immagini pubblicate
   restano esplicitamente disabilitati, non sono sostituiti da fake o sleep.
   L'overlay resta marcato non-HA/non-production e non è prova di grant Neo4j
   Enterprise, GPU, performance o disaster recovery.
6. **Operatori/versioni verificabili.** Given checkout pulito, When si installano
   gli operatori, Then gli URL/chart sono pinned a Strimzi 1.2.0,
   CloudNativePG chart 0.29.0 (app 1.30.0), OpenSearch Operator 2.8.0,
   Neo4j chart/app 5.26.30 e Qdrant chart 1.19.0. CRD/API usate corrispondono alle
   release e ogni install attende readiness con timeout bounded.
7. **Connettività reale.** Given il cluster dev pronto, When
   `task k8s:dev:verify` gira, Then un probe in-cluster risolve e raggiunge
   PostgreSQL, Kafka bootstrap, Kafka Connect con plugin PostgreSQL Debezium,
   Neo4j Bolt, Qdrant REST/gRPC, OpenSearch HTTPS, MinIO, Redis e OPA; inoltre
   PostgreSQL riporta `wal_level=logical`. I workload applicativi abilitati
   diventano Ready. Un
   componente assente/degradato produce exit non-zero e diagnostica mirata.
8. **Rollback sicuro.** Given un upgrade applicativo fallito, When Helm supera
   il timeout, Then `--atomic` ripristina la release precedente senza eliminare
   PVC. Given teardown dev, Then viene eliminato solo il cluster nominato
   `eci-dev`; PVC production e cluster non esplicitamente nominati non sono mai
   target di script distruttivi.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Helm/kubectl/kind assente | preflight fallisce con versione richiesta e link/script, nessuna mutazione |
| Docker indisponibile | `k8s:validate` resta CPU-only; `k8s:dev:up` fallisce prima di creare cluster |
| secret runtime assente | pod fail-closed/non schedulato; bootstrap stampa solo nomi chiave, mai valori |
| tag immagine vuoto, `latest` o non pinned | policy check non-zero |
| CRD operatore assente | apply dei CR attende/fallisce bounded; non passa a readiness dati |
| store non Ready | verifica non-zero con namespace/kind/nome; nessun retry infinito |
| cluster con risorse insufficienti | stato non verificato; raccoglie describe/events senza ridurre soglie standard |
| Neo4j Enterprise senza licenza | production-like richiede Secret/licenza; dev usa Community ed è marcato non equivalente |
| OpenSearch TLS/admin secret errato | nessun fallback a security plugin disabilitato; verifica fallisce |
| Qdrant collection già esistente | bootstrap idempotente verifica shard/replica; mismatch fallisce, non ricrea dati |
| policy OPA assente/non valida | OPA o lo smoke decisionale falliscono; nessun avvio senza policy/default allow |
| MinIO PVC non provisionabile | StatefulSet resta non Ready; nessun fallback automatico a `emptyDir` |
| upgrade stateful con campo immutabile | runbook richiede backup/rollback e migrazione esplicita; niente delete automatico PVC |
| teardown con nome cluster diverso | rifiutato; nessun glob o namespace esterno eliminato |

## 5. Threat model e non-goals

**Attori:** client di rete non autenticato, pod compromesso, immagine/tag
malevolo, autore values che tenta secret inline, operatore con namespace scope
eccessivo. **Asset:** codice tenant, credenziali store/OPA/OIDC, PostgreSQL
source of truth, viste materializzate, PVC e audit. **Confini:** Envoy è l'unico
ingresso; SecurityContext resta prodotto dal gateway autenticato; operatori
controllano solo le risorse necessarie; values non sono una trust source.
**Attacchi:** accesso diretto a store/GPU, service-account token theft,
cross-namespace lateral movement, tag mutable, secret in manifest/log, teardown
ampio, fallback OpenSearch/OPA insicuro. **Mitigazioni:** ClusterIP,
NetworkPolicy default-deny e allow-list, SA dedicati senza automount, pod
security, secret references, pin di release/immagini, script con target fisso e
fail-closed.

Non-goals: autoscaling HPA/KEDA (T7.2), collector/dashboard/alert completi
(T7.3), pipeline eval (T7.4), load test/SLO (T7.5), produzione cloud,
provisioning DNS/certificati/GPU, accettazione licenza Neo4j per conto di un
operatore, backup/restore production e modifica ADD/contratti. Il dev smoke non
dimostra HA, RBAC Enterprise, isolamento hardware GPU o performance D9.

## 6. Vincoli dall'ADD

- Modulo 1 §2.2.2 e Modulo 4 §1.1/§1.3: outbox CDC via Debezium/EventRouter,
  linguaggi e workload, Qdrant >=1.11, Neo4j 5.x Enterprise, PostgreSQL 17,
  Kafka KRaft via Strimzi e OpenSearch Security.
- Modulo 4 §2.1: pool CPU/GPU/memory; GPU intera vLLM, MIG production e
  time-slicing ammesso soltanto dev/batch.
- Modulo 4 §2.2: stateless rollout/PDB/topology; Kafka 3 broker/PV,
  CloudNativePG primary+2 standby, Neo4j read replica GDS, Qdrant replica/shard,
  OpenSearch operator 3 nodi.
- Modulo 4 §2.3: profili requests/limits e repliche production-like.
- D8: sei namespace logici e flussi fra ingress, query, GPU, ingestion, data e
  observability plane.
- Modulo 3 §2: filtri/autorizzazione restano applicativi/store-native;
  Kubernetes non diventa una scorciatoia per bypassarli.

## 7. Test plan

- Unit/policy (`tests/k8s/test_platform.py`): render Helm, inventario completo,
  pin, secret refs, securityContext, rollout/PDB/topology, namespace e
  NetworkPolicy; confronto stable del render normalizzato.
- Vendor values: tre release Neo4j 5.26.30 core da 128 GiB e un release GDS
  secondario da 256 GiB; lo script production-like richiede esplicitamente
  `NEO4J_ACCEPT_LICENSE_AGREEMENT=yes|eval` e non accetta la licenza per conto
  dell'operatore.
- Helm: `helm lint` e `helm template` per profilo standard/dev.
- Schema: `kubeconform` pinned con schema Kubernetes e CRD vendute/pinned;
  oggetti CR senza schema locale sono rifiutati, non ignorati.
- Supply chain: `scripts/k8s-validate.sh` rifiuta `latest`, digest/tag vuoti,
  `kind: Secret`, password/token/API key letterali e immagini senza pin.
- Integration locale: kind pinned, install operatori/chart pinned, apply
  atomic, `kubectl wait`, probe DNS/TCP/HTTP e raccolta eventi in errore.
- Regression: `task build`, `task lint`, `task test`, `task guard`; nessuna GPU
  e nessun servizio SaaS nel gate CI.

## 8. Osservabilità

- Label comuni bounded: `app.kubernetes.io/name`, `component`, `part-of=eci`,
  `version`; mai tenant/repo/user/query in label Kubernetes/Prometheus.
- Annotation Prometheus su porte metriche già esistenti e predisposizione
  `ServiceMonitor` disabilitabile; stack e dashboard restano T7.3.
- Eventi di bootstrap: fase, componente, versione, durata e outcome; nessun
  valore Secret, token, DSN o sorgente.
- Probe failure e rollout timeout mantengono diagnostica `get/describe/events`
  bounded. Metriche business e tracing end-to-end non vengono simulati.

## 9. Criteri di accettazione

- [x] Test §3/§7 registrati red-before-green e poi verdi.
- [x] Chart standard/dev lintano e renderizzano deterministicamente.
- [x] Inventario D8 completo; nessun placeholder/sleep container.
- [x] Debezium/Kafka Connect è pinned, usa TLS verso Strimzi e non incorpora
      la password PostgreSQL nella ConfigMap del connector.
- [x] OPA carica la policy canonica e lo smoke prova allow + deny fail-closed.
- [x] MinIO standard è distribuito 4x100 GiB su PVC; dev è 1x1 GiB su PVC.
- [x] Strimzi, CNPG, OpenSearch Operator, Neo4j e Qdrant hanno versioni pinned.
- [x] Standard: Kafka/PG/OpenSearch 3 nodi, Neo4j core+GDS, Qdrant 3 nodi e
      collection con replication factor 2/shard 3.
- [x] Tutti gli stateless hanno rollout, PDB, due topology spread, risorse e
      probe reali; profilo dev dichiara onestamente le riduzioni.
- [x] Nessun Secret/credenziale/tag mutable versionato; NetworkPolicy e pod
      security passano i check fail-closed.
- [x] Kubeconform valida built-in e CRD contro schemi pinned.
- [x] Cluster dev realmente creato; operatori/store Ready e connettività
      in-cluster verificata, oppure eventuale limite esterno/risorse è riportato
      senza dichiarare quel criterio soddisfatto.
- [x] Runbook documenta install, upgrade atomico, rollback, backup boundary,
      teardown scoped e raccolta diagnostica.
- [ ] `task build`, `task lint`, `task test`, `task guard`, `task k8s:validate`
      e CI verdi; SPEC `verified` solo dopo evidenza e review.

## 10. Evidenza di implementazione

Il commit test-first `7a052f1` ha registrato il fallimento atteso in assenza
del chart. L'implementazione è verificabile CPU-only con `task k8s:validate` e
gli artefatti sanificati sono in
`artifacts/t7.1/20260829T201542Z/` con `SHA256SUMS`.

Il 2026-08-29 un cluster kind 0.33.0/Kubernetes 1.34 è stato ricreato da zero:
le release ufficiali pinned hanno raggiunto readiness, OpenSearch Security è
stato inizializzato con password casuale non stampata, Neo4j ha riportato
5.26.30, CloudNativePG ha riportato `wal_level=logical` e il probe in-cluster
ha confermato il plugin PostgreSQL Debezium e raggiunto PostgreSQL, Kafka,
Kafka Connect, Neo4j Bolt, Qdrant REST/gRPC, OpenSearch, Redis, MinIO, OPA e
Keycloak. Un successivo upgrade atomico ha sostituito MinIO `Deployment` con
StatefulSet/PVC e ha provato una decisione OPA allow più una deny
`missing_tenant`; la connettività completa è rimasta verde. Tutti i sei
namespace ECI avevano Pod Security `restricted`; nessun workload applicativo
senza immagine pubblicata è stato avviato nel profilo dev.

Deviazioni dichiarate, non presentate come equivalenza production: replica
singola, Neo4j Community, storage ridotto, workload ECI/GPU disabilitati e
Keycloak `start-dev` con root filesystem scrivibile per la Quarkus augmentation
(UID non-root, seccomp e capability drop restano applicati). Il profilo
Enterprise richiede un'accettazione licenza esterna esplicita; non è stata
simulata né incorporata. La verifica di CI e la review PR restano necessarie
prima del passaggio a `verified`.

## 11. Review avversariale di approvazione

Pass eseguito il 2026-08-29. Nessuna modifica ADD/contratto né scrittura
diretta alle viste viene introdotta: gli operatori amministrano disponibilità,
mentre i soli sink conservano la proprietà delle materializzazioni. La rete non
è usata come sostituto dei filtri tenant; SecurityContext non deriva da values,
prompt o metadata non autenticato. OPA/store outage e Secret assente restano
fail-closed. Traversal, idempotenza e deadline applicative non cambiano; T7.2–5
restano separati e non si converte uno smoke probabilistico in garanzia.

Il secondo passaggio ha tentato di invalidare la proposta su cinque fronti.
(1) Un umbrella chart che incorporasse credenziali è stato escluso in favore di
`existingSecret`. (2) `latest` e install URL mobili sono rifiutati. (3) Il
profilo dev Neo4j Community non viene presentato come prova RBAC/HA Enterprise.
(4) Gli operatori non autorizzano scritture applicative o filtri post-retrieval.
(5) Lo scope del teardown è un nome kind letterale, non variabile/glob. (6) La
configurazione Debezium non contiene `expected` o credenziali: riusa
l'EventRouter già contrattualizzato e risolve la password esclusivamente
dall'ambiente autenticato del worker. (7) La policy OPA versionata nel chart è
controllata byte-per-byte contro la fonte Compose e resta fail-closed. (8)
L'object store standard non replica dischi effimeri divergenti: usa il minimo
cluster MinIO distribuito a quattro PVC. Non
emerge una decisione architetturale nuova: l'uso di Helm ufficiale per Neo4j e
Qdrant è esplicitamente una delle alternative già ammesse dall'ADD; quindi non
serve ADR.
