# SPEC-036 — Metriche Prometheus di salute per i sink (T3.3, parte 2/2)
Stato: verified
Task-tree: T3.3 (secondo dei due sotto-task concordati — chiude T3.3) · Nuovo: libs/go/eci/metrics (Go) + integrazione in 4 servizi + Prometheus in docker-compose.yml · ADD: nessuna sezione specifica

## 0. Deviazione dal termine letterale "lag" (discussa e concordata con l'utente prima di questa SPEC)
Il task-tree originale dice "metriche lag Prometheus". Verificato (non presunto) che `kafka-go` (`Reader.Stats().Lag`) NON riporta un valore affidabile per reader configurati con `GroupID` — esattamente come sono configurati tutti e quattro i nostri sink: un manutentore del progetto conferma su un thread ufficiale che il valore "non è rappresentativo con più partizioni" e "per me riporta sempre 0". L'approccio corretto (leggere separatamente l'offset più recente per partizione e l'offset committato dal gruppo, poi calcolare la differenza) è sostanzialmente più laborioso e fuori scope qui. Questa SPEC espone invece metriche di salute costruite internamente al wrapper — contatori per esito, timestamp dell'ultimo messaggio processato — non "lag" in senso stretto di offset, ma segnali costruibili con certezza, senza dipendere da un'API che i suoi stessi autori descrivono come inaffidabile.

## 1. Obiettivo
Esporre via `/metrics` (formato Prometheus) lo stato di salute di ciascuno dei quattro sink — quanti messaggi processati/ritentati/finiti in DLQ, quando è stato processato l'ultimo messaggio — componendo attorno al wrapper `resilience.WithRetryAndDLQ` (SPEC-035) già esistente, senza toccarne la logica.

## 2. Interfaccia

**Nuovo package** `libs/go/eci/metrics`:
```go
// WithPrometheus avvolge una resilience.ProcessFunc GIÀ COMPOSTA con
// WithRetryAndDLQ (l'outcome che vede è quello FINALE, dopo retry/DLQ) —
// incrementa un contatore per outcome e aggiorna un gauge col timestamp
// dell'ultimo messaggio processato, entrambi etichettati per sinkName.
func WithPrometheus(sinkName string, inner resilience.ProcessFunc) resilience.ProcessFunc

// Handler espone http.Handler per /metrics (wrapper sottile su
// promhttp.Handler(), stesso registry globale di prometheus/client_golang).
func Handler() http.Handler
```

**Metriche esposte** (nomi dichiarati, prefisso `eci_`):
- `eci_messages_processed_total{sink, outcome}` (counter) — `outcome` ∈ `{processed, retried, dead_lettered}`, stessi valori di `resilience.Outcome`.
- `eci_last_processed_timestamp_seconds{sink}` (gauge) — `time.Now().Unix()` aggiornato a ogni chiamata, indipendentemente dall'esito.

**Composizione nei quattro sink**: `metrics.WithPrometheus("sink-graph", resilience.WithRetryAndDLQ(cfg, producer, ProcessMessage))` — metriche come strato PIÙ ESTERNO, vede l'outcome finale dopo che retry/DLQ ha già agito.

**Endpoint HTTP**: ciascuno dei quattro `main.go` avvia un piccolo server HTTP su una porta dichiarata (`:9090`, configurabile via `METRICS_PORT`) che serve `Handler()` su `/metrics` — in un goroutine separata, non blocca il consume-loop.

**Prometheus nello stack dev**: nuovo servizio `prometheus` in `deploy/compose/docker-compose.yml` (modifica dichiarata a un file condiviso, stesso principio già seguito per Redis/T2.3 e il profilo `gpu`/T2.4), più `deploy/compose/prometheus.yml` (scrape config: i quattro sink per nome del servizio compose, porta 9090, stesso principio di risoluzione DNS interna alla rete compose già usato ovunque in questo stack).

## 3. Comportamento (scenari)

1. **Dato** un messaggio processato con successo, **quando** ispeziono `/metrics`, **allora** `eci_messages_processed_total{sink="X",outcome="processed"}` è incrementato di 1.
2. **Dato** un messaggio che va in retry, **quando** ispeziono `/metrics`, **allora** il contatore per `outcome="retried"` è incrementato.
3. **Dato** un messaggio che finisce in DLQ, **quando** ispeziono `/metrics`, **allora** il contatore per `outcome="dead_lettered"` è incrementato.
4. **Dato** un messaggio appena processato, **quando** ispeziono `/metrics`, **allora** `eci_last_processed_timestamp_seconds{sink="X"}` riflette un timestamp recente (entro pochi secondi da ora).
5. **Dato** l'endpoint `/metrics` di UNO dei quattro sink reali (rappresentativo — stesso principio di SPEC-035, `sink-graph`), **quando** lo interrogo con una richiesta HTTP diretta, **allora** ottengo testo in formato Prometheus valido contenente i nomi di metrica dichiarati — verifica diretta via HTTP reale, non solo ispezione del registry in-process.
6. **Dato** il servizio Prometheus aggiunto allo stack dev, **quando** eseguo `task up` e interrogo l'API di Prometheus per i target configurati, **allora** i quattro sink compaiono come target `up` (raggiunti con successo) — verifica diretta che lo scrape config funzioni davvero, non solo che sia scritto.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Il server HTTP delle metriche non riesce ad avviarsi (porta già occupata) | Errore esplicito loggato, ma NON deve impedire l'avvio del consume-loop principale — le metriche sono osservabilità accessoria, non un prerequisito funzionale (coerente con "mai bloccare la funzione primaria per un problema di solo monitoraggio") |

## 5. Non-goals
Nessun vero lag basato su offset (§0, dichiarato). Nessun alerting/dashboard Grafana (fuori scope, solo esposizione ed scrape). Nessuna istogramma di durata di elaborazione (non richiesto esplicitamente dal task-tree, aggiungerlo ora sarebbe uno scope creep non necessario).

## 6. Vincoli dall'ADD
Nessuno specifico.

## 7. Test plan
Libreria condivisa: test unitari puri (nessun bisogno di Kafka/HTTP reali per la logica di incremento contatore/gauge — `prometheus/client_golang` espone un registry testabile in-process). Scenario 5/6: verifica HTTP/Prometheus reale, stack dev avviato via `task up`.

## 8. Osservabilità
Questa SPEC stessa È l'osservabilità aggiunta — nessun requisito ulteriore.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con evidenza diretta, in particolare 5 (HTTP reale) e 6 (Prometheus scrape reale via `task up`).
- [x] Edge case §4 verificato esplicitamente.
- [x] Applicato e verificato compilare/testare in tutti e quattro i sink.
- [x] Nessuna regressione sui test esistenti.

## 10. Deviazioni

1. **Topologia dei quattro sink: NON sono servizi Docker Compose (§2, §3 scenario 6).** Il testo di §2 presume risoluzione DNS per nome del servizio compose, "stesso principio... già usato ovunque in questo stack". Verificato (non presunto): nessun Dockerfile esiste per nessuno dei quattro sink, non sono mai stati aggiunti a `docker-compose.yml`, e ogni `main.go` conferma di default `localhost` per Postgres/Kafka/Neo4j/Qdrant/OpenSearch — girano come processi HOST, non container. Conflitto genuino tra SPEC e realtà verificata, risolto chiedendo esplicitamente all'utente (non deciso unilateralmente): l'utente ha scelto l'opzione raggiungimento via `host.docker.internal` con porte distinte per sink, anziché DNS interno alla rete compose. Implementato in `deploy/compose/prometheus.yml` (target `host.docker.internal:910{1,2,3,4}`) e `deploy/compose/docker-compose.yml` (nuovo servizio `prometheus` con `extra_hosts: ["host.docker.internal:host-gateway"]`, necessario su Docker Engine nativo Linux, non solo Docker Desktop). Verificato con un `task up` reale e un'interrogazione diretta di `/api/v1/targets`: tutti e quattro i target risultano `"health":"up"` (§3 scenario 6).

2. **Porta di default: distinta per sink (9101-9104), non l'unica `:9090` letterale di §2.** Conseguenza diretta della deviazione precedente: i quattro sink condividono lo stesso namespace di rete (l'host), quindi un identico `METRICS_PORT` di default per tutti e quattro avrebbe causato una collisione di bind reale se eseguiti insieme (come effettivamente accade in sviluppo). Ogni sink dichiara comunque `METRICS_PORT` come nome della variabile d'ambiente per l'override, invariato rispetto a §2 — solo il valore di default cambia per servizio (sink-graph=9101, embedding-worker=9102, sink-vector=9103, sink-search=9104). La porta `:9090` resta quella di Prometheus stesso (nessun conflitto: Prometheus gira nella rete Docker, i sink sull'host).

3. **Fix proattivo di `retrieval-engine` e `semantic-cache` (fuori dai "file toccabili" dichiarati, stesso precedente di SPEC-035 §10.4).** Il bump di `libs/go/go.mod` per `prometheus/client_golang` ha reso `go.mod`/`go.sum` di questi due moduli (che referenziano `libs/go` via `replace`) disallineati (`go: updates to go.mod needed`). Verificato PRIMA che diventasse una sorpresa scoperta tardi (lezione esplicita da SPEC-035): corretto con `go mod tidy` in entrambi, poi verificato `go build`/`go vet` puliti in tutti e tre i moduli dipendenti (incluso `llm-gateway`, che non necessitava fix). Nessuna modifica di logica applicativa, solo allineamento delle dipendenze transitive.

4. **Nessuna deviazione sul contenuto di §0 (health metrics al posto di "lag"):** implementato esattamente come pre-concordato — nessuna novità qui, solo conferma che l'implementazione rispetta la deviazione già dichiarata prima di questa SPEC.

Nessun'altra deviazione rispetto al testo di §1-§9.
