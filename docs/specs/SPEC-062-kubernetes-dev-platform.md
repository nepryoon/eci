# SPEC-062 — Piattaforma Kubernetes/Helm e operatori per sviluppo
Stato: implemented
Task-tree: T7.1 · Servizi: `deploy/k8s`, tutti i workload ECI · ADD: Modulo 4 §1.3, §2.1–§2.3, D8
Contratti: nessuna modifica; `contracts/**` è consumato in sola lettura dalle immagini applicative
ADR: ADR-0016 (CLI ingestion one-shot rappresentato senza fingere un server)

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
  imageReferences:
    api-gateway: "ghcr.io/nepryoon/eci/api-gateway@sha256:<registry digest>"
    # one registry-issued digest reference per first-party workload
  existingSecret: eci-runtime
  storageClass: ""
operators:
  strimziVersion: "1.2.0"
  cloudnativePGChartVersion: "0.29.0" # app 1.30.0
  openSearchOperatorVersion: "2.8.0"
dataPlane:
  postgres: {instances: 3, majorVersion: 17}
  kafka: {replicas: 3, kraft: true, topicPartitions: 12}
  neo4j: {edition: enterprise, coreReplicas: 3, gdsReplicas: 1}
  qdrant: {replicas: 3, replicationFactor: 2, shardNumber: 3}
  openSearch:
    replicas: 3
    securityPlugin: true
    adminCredentialsSecret: eci-opensearch-admin
    securityConfigSecret: eci-opensearch-security-config
```

I secret non sono valori Helm. Le applicazioni sono opt-in e ogni immagine
first-party deve essere fornita dal release process come riferimento OCI per
digest; in assenza di immagini pubblicate il profilo base non renderizza pod
applicativi né usa riferimenti inventati. Il chart usa esclusivamente
`secretKeyRef`/secret montati da `global.existingSecret` e dai due riferimenti
OpenSearch dedicati; il bootstrap dev li crea a runtime da input locali non
versionati e genera l'hash bcrypt richiesto dall'operator 2.8.0. Le immagini devono avere
digest SHA-256; tag-only, `latest`, riferimenti vuoti e credenziali letterali
sono rifiutati. Envoy richiede inoltre ConfigMap `eci-envoy-config` generato da
`deploy/envoy/envoy.yaml`/`retrieval.pb` e Secret TLS `eci-envoy-tls` esterni.

## 3. Comportamento

1. **Inventario completo.** Given riferimenti OCI first-party reali e
   `applications.enabled=true`, When il chart produce YAML, Then esistono i
   namespace `ingress`, `query-plane`,
   `gpu-plane`, `ingestion-plane`, `data-plane`, `observability`; Deployment,
   Service, account e PDB coprono Envoy/API gateway, retrieval,
   LLM gateway, semantic cache, tre sink,
   ingestion CPU, embedding/reranker/vLLM; GDS è un workload batch esplicito.
   Senza tali riferimenti il profilo infrastrutturale resta applicazioni-off e
   Helm fallisce se si forza l'abilitazione: nessun digest sintetico è default.
2. **Stateful production-like.** Given il profilo standard, When si renderizza,
   Then Kafka usa `KafkaNodePool` KRaft a 3 nodi/PV, CloudNativePG usa
   PostgreSQL 17 con 3 istanze, OpenSearch usa l'operator CR con Security
   plugin e 3 nodi, Kafka Connect usa Debezium pinned con una propria identità
   mTLS/ACL verso Strimzi e
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
   CloudNativePG riceve ingress soltanto sul target TLS webhook 9443; poiché
   NetworkPolicy non identifica un apiserver esterno, la sorgente portabile è
   `0.0.0.0/0` ma il selector resta limitato ai soli pod operator e il CIDR è
   restringibile per cluster.
   PostgreSQL riconcilia un ruolo `eci_cdc` dedicato con LOGIN+REPLICATION ma
   senza privilegi amministrativi; usa un Secret separato dal database owner.
   La publication outbox è creata dalla migration 0005 e Connect non può
   auto-crearla.
   Kafka espone soltanto il listener applicativo mTLS 9093 con simple
   authorization deny-by-default. Connect e ogni consumer hanno un
   `KafkaUser` distinto; topic e consumer group sono ACL literal senza
   wildcard. Connect è l'unico writer dei topic primari; ogni consumer può
   scrivere soltanto `{primary}.retry.{consumer}` e `{primary}.DLQ`. La REST
   Connect è loopback-only e priva di Service; finché questo confine resta in
   vigore il worker distribuito è obbligatoriamente a replica singola, perché
   un follower non può raggiungere la REST advertised del leader. Disabilitare
   il data plane omette anche Connect, connector config e policy dedicate.
   Ogni consumer monta soltanto la CA pubblica del broker, il proprio `user.crt` e
   `user.key`; reader e writer falliscono se trust o identità mancano. Retrieval e
   sink-search richiedono CA HTTP e credenziali OpenSearch, mentre Semantic
   Cache riceve esplicitamente la password del Redis `requirepass`. Nessuna CA
   privata viene copiata fra namespace.
   Redis resta intenzionalmente a replica singola finché non esiste un
   topology replicato approvato: il Service non bilancia mai cache standalone
   indipendenti. Lo StatefulSet usa AOF `everysec` e PVC (20 GiB standard,
   1 GiB dev); una perdita cache oltre quel confine è degradazione
   ricostruibile, non perdita di una vista autorevole. Quando porta applicativa e porta metriche coincidono,
   il Service/Pod dichiara una sola porta nominata, evitando chiavi duplicate.
   Ogni applicazione riceve solo un Secret per-workload con chiavi enumerate;
   nessun pod applicativo importa il Secret bootstrap condiviso. Le allow-list
   di rete selezionano chiamante, destinazione e porta, quindi essere nello
   stesso namespace non conferisce accesso ai datastore. Il probe dev gira nel
   data-plane e ottiene solo due eccezioni esplicite verso OPA/Keycloak;
   observability non riceve accesso generale alle API dati.
   Startup/readiness dei quattro worker Kafka chiama `/ready`, che usa lo
   stesso transport del consumer e verifica metadata di ogni topic,
   coordinator e offset access del gruppo. Errori TLS/topic/group rispondono
   503 senza dettagli; liveness resta locale per evitare restart storm durante
   un outage recuperabile del broker. Semantic Cache applica lo stesso
   principio: `/ready` esegue PING Redis con il client autenticato e risponde
   soltanto 204/503, mentre liveness resta locale. Retrieval Engine verifica
   in parallelo e con deadline Neo4j autenticato, collection Qdrant
   `code_embeddings`, indice OpenSearch `code_chunks` e `/health` nativo TEI
   per embedder/reranker. La verifica TEI non esegue inferenza. Anche qui il
   body HTTP resta vuoto e liveness resta locale.
5. **Overlay dev onesto.** Given `values-dev.yaml`, When il bootstrap gira su
   kind, Then usa repliche/storage/resource ridotti e Neo4j Community come
   previsto dagli ADR dev esistenti. I workload ECI senza immagini pubblicate
   restano esplicitamente disabilitati, non sono sostituiti da fake o sleep.
   L'overlay resta marcato non-HA/non-production e non è prova di grant Neo4j
   Enterprise, GPU, performance o disaster recovery.
   Il Keycloak dev espone l'issuer solo via HTTPS 8443 attraverso il Service e
   le NetworkPolicy. `dev-up.sh` genera localmente una identità hostname-bound
   non versionata; il gateway monta soltanto il certificato pubblico come CA
   aggiuntiva e lo smoke verifica discovery TLS e issuer esatto.
   Il binario ingestion corrente è one-shot: il chart lo rappresenta come
   template CronJob sospeso, con PVC sorgente read-only e scope Secret
   enumerato, non come Deployment/listener inesistente. ADR-0016 e T7.1a
   rendono esplicito che il worker pool D8 resta da implementare.
   Anche l'orchestrator corrente espone soltanto il CLI `eci ask`/`eval-golden`:
   non viene renderizzato come Deployment con listener inventato. ADR-0017 e
   T7.1b rendono esplicito il runtime API/streaming ancora da implementare.
   Verification e summarization sono oggi librerie Python prive di entrypoint
   server: ADR-0018/T7.1c ne vietano Deployment/listener fittizi e tracciano i
   runtime API autenticati mancanti. Il job GDS è un template sospeso: riceve
   entry node e scope soltanto dal proprio Secret preparato e viene clonato
   esplicitamente, mai avviato senza argomenti.
   I server GPU richiedono un PVC modelli esterno read-only e ricevono path e
   model/served ID canonici; nessun download di pesi o modello alternativo è
   un fallback implicito.
6. **Operatori/versioni verificabili.** Given checkout pulito, When si installano
   gli operatori, Then gli URL/chart sono pinned a Strimzi 1.2.0,
   CloudNativePG chart 0.29.0 (app 1.30.0), OpenSearch Operator 2.8.0,
   Neo4j chart/app 5.26.30 e Qdrant chart 1.19.0. CRD/API usate corrispondono alle
   release; ogni archivio è verificato con SHA-256 prima di Helm e ogni install
   attende readiness con timeout bounded. Il chart Qdrant, che richiede un tag
   semver nei values, passa sempre da un post-renderer che sostituisce
   l'immagine runtime con il digest multi-arch verificato e rifiuta ogni
   immagine mutabile residua. Anche il chart OpenSearch Operator passa da un
   post-renderer fail-closed che fissa per digest sia manager sia
   `kube-rbac-proxy`.
7. **Connettività reale.** Given il cluster dev pronto, When
   `task k8s:dev:verify` gira, Then un probe in-cluster risolve e raggiunge
   PostgreSQL, Kafka bootstrap, Kafka Connect con plugin PostgreSQL Debezium
   verificato via loopback exec,
   Neo4j Bolt, Qdrant REST/gRPC, OpenSearch HTTPS, MinIO, Redis e OPA; inoltre
   PostgreSQL riporta `wal_level=logical`; inoltre una prova mTLS Kafka
   pubblica sul proprio retry topic e riceve un deny sul topic primario. I workload applicativi abilitati
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
| Secret di un sibling riusato o chiave non enumerata | non referenziato dal Pod; review/policy test non-zero |
| digest chart Helm non corrispondente | installer termina prima della prima mutazione Helm |
| immagine vuota, tag-only, `latest` o non pin-nata per digest | policy check non-zero |
| applicazioni abilitate senza tutti i digest first-party | render Helm non-zero, nessun Deployment parziale |
| ingestion template senza PVC/scope esterni | resta sospeso/non eseguito; nessun fixture o scope di default |
| ConfigMap bootstrap Envoy o Secret TLS assente | Envoy non schedula/avvia; nessun fallback cleartext |
| CA/certificato/chiave Kafka assente o invalido con mTLS abilitato | Connect/sink/worker terminano prima di usare il broker |
| client Kafka tenta topic/gruppo sibling | broker nega tramite ACL literal; smoke e verifica falliscono se viene consentito |
| CA o credenziali OpenSearch assenti su HTTPS | retrieval/sink-search terminano prima di servire/consumare |
| password Redis assente con auth richiesta | semantic-cache termina prima di aprire il listener |
| password Redis errata/stale o Redis indisponibile | startup/readiness resta 503 senza dettagli backend; liveness locale non causa restart storm |
| credenziale/store/model backend Retrieval errato o indisponibile | `/ready` resta 503; controlli concorrenti bounded e nessun dettaglio backend o inferenza periodica |
| CRD operatore assente | apply dei CR attende/fallisce bounded; non passa a readiness dati |
| store non Ready | verifica non-zero con namespace/kind/nome; nessun retry infinito |
| cluster con risorse insufficienti | stato non verificato; raccoglie describe/events senza ridurre soglie standard |
| Neo4j Enterprise senza licenza | production-like richiede Secret/licenza; dev usa Community ed è marcato non equivalente |
| OpenSearch TLS/admin secret errato | nessun fallback a security plugin disabilitato; verifica fallisce |
| immagine Qdrant non coincide col tag atteso o resta mutabile | post-renderer/install termina prima dell'apply Helm |
| Qdrant collection già esistente | bootstrap idempotente verifica shard/replica; mismatch fallisce, non ricrea dati |
| PVC/path modello GPU assente | rollout GPU non supera startup/readiness; nessun download/fallback modello |
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
cross-namespace lateral movement, abuso di un client Kafka per leggere/scrivere
topic sibling, retag upstream, secret in manifest/log, teardown
ampio, fallback OpenSearch/OPA insicuro. **Mitigazioni:** ClusterIP,
NetworkPolicy default-deny e allow-list, SA dedicati senza automount, identità
mTLS Kafka per workload con ACL literal, pod security, secret references, pin
di release/immagini, script con target fisso e fail-closed.

Non-goals: worker ingestion long-running (T7.1a), orchestrator server/API
long-running (T7.1b), verification/summarization server API (T7.1c),
autoscaling HPA/KEDA (T7.2), collector/dashboard/alert completi
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
- Supply chain: `scripts/k8s-validate.sh` richiede `name@sha256:<64 hex>`,
  rifiuta tag-only, `kind: Secret`, password/token/API key letterali e immagini
  senza digest. Task 3.53.1, Helm e kubeconform sono scaricati con checksum.
- Kafka security: listener mTLS, `KafkaUser`/`KafkaTopic` con schemi locali,
  ACL senza wildcard, identità distinte, TLS reader/writer e smoke allow/deny.
- Vendor runtime: post-render Qdrant richiede esattamente l'immagine attesa e
  rifiuta qualsiasi container non pin-nato per digest.
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
- [x] Catalogo delle implementazioni correnti completo sotto opt-in con digest
      obbligatori; gap worker D8 dichiarato in ADR-0016/T7.1a, nessun
      placeholder/sleep container o immagine ECI inesistente di default.
- [x] Debezium/Kafka Connect è pinned, usa una propria identità mTLS/ACL verso Strimzi e non incorpora
      la password PostgreSQL nella ConfigMap del connector; il ruolo PostgreSQL
      dedicato `eci_cdc` ha REPLICATION+SELECT outbox senza privilegi owner/admin.
- [x] OPA carica la policy canonica e lo smoke prova allow + deny fail-closed.
- [x] MinIO standard è distribuito 4x100 GiB su PVC; dev è 1x1 GiB su PVC.
- [x] Strimzi, CNPG, OpenSearch Operator, Neo4j e Qdrant hanno versioni pinned.
- [x] Standard: Kafka/PG/OpenSearch 3 nodi, Neo4j core+GDS, Qdrant 3 nodi e
      collection con replication factor 2/shard 3.
- [x] Tutti i server stateless hanno rollout, PDB, due topology spread, risorse
      e probe reali; il CLI one-shot ha lifecycle Job, il profilo dev dichiara
      onestamente le riduzioni.
- [x] Nessun Secret/credenziale/tag mutable versionato; Envoy monta config,
      descriptor e TLS esterni sulla porta 8080 reale; NetworkPolicy e pod
      security passano i check fail-closed.
- [x] Kafka Connect e reader/writer usano identità mTLS distinte e ACL literal
      per topic/group; Connect è il solo writer primario, retry per-consumer e
      REST Connect loopback-only sono verificati; OpenSearch usa HTTPS+CA+Basic Auth;
      Redis `requirepass` è propagato esplicitamente, con unit test fail-closed.
- [x] Kafka Connect loopback-only è vincolato a una replica ed è omesso con il
      data plane; i quattro worker sono Ready solo dopo topic+group access via
      il transport Kafka autenticato; Semantic Cache è Ready solo dopo PING
      Redis autenticato; Retrieval Engine richiede tutti i cinque backend
      reali, mentre le liveness restano locali.
- [x] Redis standalone usa un solo backend StatefulSet con AOF/PVC e restart
      persistence verificata; porte service/metriche
      coincidenti non producono duplicati Kubernetes; Keycloak dev espone
      discovery OIDC HTTPS 8443 con certificato hostname-bound non versionato.
- [x] Secret applicativi e NetworkPolicy sono per-workload/per-destinazione;
      ogni archivio Helm terzo è verificato SHA-256 prima dell'installazione.
- [x] Qdrant runtime e chart-test images sono post-renderizzate/verificate per
      digest; il tag semver richiesto dal chart non arriva mai al kubelet.
- [x] vLLM/TEI usano path modello canonici da PVC esterno read-only e startup
      probe bounded; nessun download di pesi è implicito.
- [x] Ingestion CLI è modellato come template Job sospeso con input/scope
      espliciti, mai come server fittizio; deviazione D8 tracciata in ADR-0016.
- [x] Orchestrator CLI non è modellato come Deployment/listener fittizio;
      deviazione runtime tracciata in ADR-0017/T7.1b.
- [x] Verification/summarization library-only non sono modellati come server;
      deviazione tracciata in ADR-0018/T7.1c. GDS è un template sospeso con
      scope/entry node provenienti dal Secret dedicato.
- [x] API Gateway riceve un egress HTTPS soltanto verso CIDR issuer/proxy
      espliciti; lista assente blocca il render applicativo.
- [x] Kubeconform valida built-in e CRD contro schemi pinned.
- [x] Cluster dev realmente creato; operatori/store Ready e connettività
      in-cluster verificata, oppure eventuale limite esterno/risorse è riportato
      senza dichiarare quel criterio soddisfatto.
- [x] Runbook documenta install, upgrade atomico, rollback, backup boundary,
      teardown scoped e raccolta diagnostica.
- [ ] T7.1a sostituisce il Job transitorio con worker Deployment 4–40 e
      readiness/backpressure reali; solo allora lo stato può diventare verified.
- [ ] T7.1b fornisce il server orchestrator autenticato/streaming e probe reali;
      solo allora l'inventario applicativo T7.1 può diventare verified.
- [ ] T7.1c fornisce server verification/summarization autenticati e probe
      reali; solo allora l'inventario applicativo T7.1 può diventare verified.
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
(UID non-root, seccomp e capability drop restano applicati); il solo endpoint
Service/NetworkPolicy è HTTPS 8443 con certificato dev generato localmente. Il profilo
Enterprise richiede un'accettazione licenza esterna esplicita; non è stata
simulata né incorporata. La verifica di CI e la review PR restano necessarie
prima del passaggio a `verified`.

Il secondo ciclo di review ha eliminato i tag container-only, pin-nato Task
3.53.1 tramite checksum, introdotto routing runtime namespace-local, montato gli
input esterni Envoy bootstrap/descriptor/TLS sulla porta reale 8080 e ammesso
il solo target webhook CloudNativePG. Nessun digest ECI è stato inventato:
l'abilitazione applicativa richiede riferimenti pubblicati dal release process.
Un upgrade reale ha prima riprodotto l'immutabilità del vecchio Job Qdrant e
Helm ha eseguito rollback senza stato parziale; convertito il bootstrap
idempotente in hook bounded pre-install/pre-upgrade, la revision 6 è stata
applicata e l'intero smoke readiness/connectivity/OPA è tornato verde. Questa è
evidenza infrastrutturale, non evidenza di pubblicazione o readiness delle
applicazioni first-party.

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
`existingSecret`. (2) Tag-only/`latest` e install URL mobili sono rifiutati;
tutti i container renderizzati usano digest registry e Task è verificato per
checksum. (3) Il
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

Il pass security successivo ha individuato due confused-deputy boundary e ha
prodotto ADR-0019. Le ACL ora rendono Kafka Connect l'unico writer dei topic
primari e assegnano a ogni consumer un retry topic distinto; la REST Connect è
loopback-only, senza Service, ed esclusa dalla policy data-plane generale. Il
dev smoke deve provare sia il retry publish consentito sia il primary publish
negato e ispezionare il plugin soltanto tramite exec amministrativo.

ADR-0020 elimina inoltre il riuso del database owner in Connect: CNPG gestisce
`eci_cdc` con REPLICATION least-privilege, la migration 0005 crea la publication
outbox fissa e Connect usa un Secret distinto con autocreation disabilitata.
