# SPEC-022 — Semantic Cache Service (T2.3)
Stato: verified
Task-tree: T2.3 (terzo task di Fase 2) · Nuovo servizio: services/semantic-cache (Go, scaffold vuoto da SPEC-001) · ADD: Modulo 1 §1.6.3, Modulo 3 §1.1/§2.6.3, Modulo 4 §1
Contratti: nuovo `contracts/proto/eci/semanticcache/v1/semanticcache.proto`; modifica dichiarata a `deploy/compose/docker-compose.yml` (nuovo servizio Redis)

## 1. Obiettivo
Un servizio Go che azzera le chiamate LLM ridondanti su sottoalberi AST invariati: cache chiave→valore con chiave **tripla** `ast_hash + logic_fingerprint + acl_scope` (ADD Modulo 3 §2.6.3 — non solo doppia come descritto più genericamente in §1.6.3), dove `acl_scope` esiste specificamente per impedire il riuso cross-ACL di un summary/embedding, non solo come metadata informativo. Nessun consumatore reale esiste ancora (T2.4/T5.5 sono task successivi) — questa SPEC costruisce l'infrastruttura del cache, non la logica di embedding/summary che la userà.

## 2. Interfaccia

**Storage: Redis** — decisione dichiarata, non specificata dall'ADD (che nomina solo "KV a bassissima latenza" senza una tecnologia concreta, verificato non essere presente né in Modulo 1 né Modulo 3 né Modulo 4). Motivazione: TTL nativo (evita reimplementare la scadenza a mano, dato che il value schema dell'ADD include esplicitamente `ttl`), nessun servizio Go esistente nel progetto scrive già su Postgres (sarebbe stato un pattern nuovo da inventare per un solo caso d'uso), e "bassissima latenza" (Modulo 4 §1) è esattamente il caso d'uso per cui Redis è la scelta standard di settore. **Modifica dichiarata a `deploy/compose/docker-compose.yml`** (file condiviso, già modificato più volte in Fase 0/1): nuovo servizio `redis:7-alpine`, porta 6379, nessuna persistenza AOF/RDB richiesta (è un cache, non una fonte di verità — il dato è sempre ricostruibile ricomputando embedding/summary).

**API gRPC** (nuovo proto, stesso pattern di contracts/proto/eci/retrieval/v1 — coerenza con l'unico altro servizio gRPC interno del progetto, retrieval-engine):
```protobuf
message CacheKey {
  string entity_id = 1;        // symbol_id/id del CodeNode (T1.1)
  string ast_hash = 2;         // T2.1
  string logic_fingerprint = 3; // stringa opaca fornita dal chiamante — vedi nota sotto
  string acl_scope = 4;        // placeholder in questa SPEC, vedi §2 "plumbing non enforcement"
}

message CacheValue {
  string embedding_ref = 1;
  string summary = 2;
  string model_id = 3;
  uint32 embedding_dim = 4;
  uint32 ttl_seconds = 5;      // 0 = nessuna scadenza
}

service SemanticCache {
  rpc Get(CacheKey) returns (GetResponse);   // GetResponse { bool hit = 1; CacheValue value = 2; }
  rpc Put(PutRequest) returns (PutResponse); // PutRequest { CacheKey key = 1; CacheValue value = 2; }
}
```

**`logic_fingerprint` come stringa opaca**: il servizio non calcola né interpreta questo valore — lo riceve dal chiamante e lo usa così com'è come parte della chiave. Motivazione dichiarata: il significato preciso di "logica e parametri della trasformazione" (modello di embedding, versione del prompt) dipende da servizi che non esistono ancora in questo progetto (T2.4 embedding reale, T5.5 summarization) — il Semantic Cache non deve anticipare i loro dettagli per funzionare correttamente oggi. Il chiamante futuro sarà responsabile di calcolare `logic_fingerprint` in modo deterministico dalla propria configurazione (es. `SHA256(model_id + model_version + prompt_version)`), non questo servizio.

**`acl_scope`: plumbing, non enforcement** — stesso principio già stabilito per `SecurityContext` in T0.8/T1.4: il campo esiste nella chiave (quindi partiziona già correttamente la cache per costruzione — due `acl_scope` diversi non possono MAI produrre un hit sullo stesso valore, anche accidentalmente), ma nessuna verifica di autorizzazione avviene qui. In questa SPEC, senza un chiamante reale che lo popoli con un valore significativo, il valore atteso è un placeholder costante (es. `"default"`) — l'enforcement vero arriva con Fase 6.

**Chiave Redis**: concatenazione deterministica dei quattro campi con un separatore che non può comparire nei valori attesi (SHA-256 esadecimali + id) — `fmt.Sprintf("scache:%s:%s:%s:%s", entity_id, ast_hash, logic_fingerprint, acl_scope)`. TTL applicato nativamente via `SET ... EX ttl_seconds` (o `SET` senza `EX` se `ttl_seconds == 0`).

## 3. Comportamento (scenari)

1. **Dato** un `Get` su una chiave mai scritta, **quando** chiamo il servizio, **allora** ricevo `hit: false`, nessun `CacheValue`.
2. **Dato** un `Put` seguito da un `Get` con la STESSA chiave esatta, **quando** chiamo `Get`, **allora** ricevo `hit: true` con lo stesso `CacheValue` scritto.
3. **Dato** lo stesso `Put`, **quando** chiamo `Get` con lo stesso `entity_id`/`ast_hash`/`acl_scope` ma un `logic_fingerprint` DIVERSO, **allora** ricevo `hit: false` — verifica diretta che il fingerprint partecipi davvero alla chiave, non solo che sia accettato come parametro.
4. **Dato** lo stesso `Put`, **quando** chiamo `Get` con lo stesso `entity_id`/`ast_hash`/`logic_fingerprint` ma un `acl_scope` DIVERSO, **allora** ricevo `hit: false` — verifica diretta della proprietà di sicurezza dichiarata in §2.6.3 dell'ADD (isolamento cross-ACL), non solo presunta dal fatto che il campo esiste nella chiave.
5. **Dato** un `Put` con `ttl_seconds` molto basso (es. 1), **quando** chiamo `Get` DOPO che il TTL è scaduto, **allora** ricevo `hit: false` — verifica diretta della scadenza reale via Redis, non solo dichiarata.
6. **Dato** un `Put` con `ttl_seconds: 0`, **quando** chiamo `Get` dopo un'attesa superiore al TTL usato nello scenario 5, **allora** ricevo ancora `hit: true` — verifica che `0` significhi genuinamente "nessuna scadenza", non "scadenza immediata" o un altro default silenzioso.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Redis irraggiungibile al momento di `Get`/`Put` | Errore gRPC esplicito (`Unavailable`), non un `hit: false` silenzioso che nasconderebbe un guasto infrastrutturale come se fosse un semplice cache miss |
| Un campo della chiave vuoto (es. `logic_fingerprint: ""`) | Nessun caso speciale — è comunque una chiave valida e deterministica, tratta come qualunque altra stringa; se il chiamante passa una stringa vuota, ottiene un comportamento di cache coerente ma probabilmente indesiderato dal SUO punto di vista, non un errore di questo servizio |
| `ttl_seconds` molto grande (es. superiore al massimo TTL supportato da Redis) | Errore esplicito dalla chiamata Redis stessa (propagato come errore gRPC), non un troncamento silenzioso a un valore diverso |

## 5. Non-goals
Nessun consumatore reale (T2.4/T5.5). Nessun enforcement di `acl_scope` (Fase 6). Nessuna logica di invalidazione selettiva/lineage tracking (menzionata nell'ADD ma esplicitamente parte di task successivi non ancora nel task-tree di Fase 2 con questo livello di dettaglio). Nessuna persistenza Redis (AOF/RDB) — accettabile perdita totale della cache a un riavvio, dato che ogni valore è sempre ricomputabile dalla pipeline a monte. Nessuna metrica di hit-rate/osservabilità dedicata oltre lo span OTel standard.

## 6. Vincoli dall'ADD
Modulo 1 §1.6.3: struttura chiave→valore, regola di riuso "did anything change?". Modulo 3 §2.6.3: chiave estesa con `acl_scope`, motivazione esplicita di sicurezza (prevenzione riuso cross-ACL) — non un'aggiunta arbitraria di questa SPEC. Modulo 4 §1: linguaggio Go, "KV a bassissima latenza, partizionamento per acl_scope" — coerente con la scelta Redis dichiarata in §2.

## 7. Test plan
Test di integrazione con testcontainers Redis (stesso principio di readiness reale già stabilito per Postgres/Neo4j/Kafka nei servizi precedenti), non un Redis mock — verifica diretta del comportamento reale di TTL/scadenza (scenario 5), non simulata.

## 8. Osservabilità
Stessa fondazione OTel di `libs/go/eci/observability` (SPEC-011) — uno span per `Get`/`Put`.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con evidenza diretta, in particolare gli scenari 3/4 (isolamento per fingerprint/ACL) e 5/6 (TTL reale, non simulato).
- [x] Edge case tabella §4 tutti verificati esplicitamente.
- [x] `docker-compose.yml` aggiornato e verificato con `task up` reale (Redis healthy insieme agli altri servizi).

## 10. Implementazione — deviazioni

Proto in `contracts/proto/eci/semanticcache/v1/semanticcache.proto`
(pipeline buf esistente, `task proto:gen` — nessun meccanismo nuovo,
genera in `libs/go/eci/semanticcache/v1/` come già fa per
`eci.retrieval.v1`, SPEC-002). File nuovo, non una modifica a un
contratto esistente: `scripts/guard.sh` non richiede un ADR (verificato
leggendo lo script — richiede ADR solo per M/D/R su `contracts/`, mai per
A). Firma del service copiata esattamente da SPEC-022 §2, incluso
l'asimmetria dichiarata `Get(CacheKey)` vs `Put(PutRequest)` (non
uniformata a un wrapper `GetRequest` per coerenza stilistica — sarebbe
stata una reinterpretazione non richiesta).

`deploy/compose/docker-compose.yml`: nuovo servizio `redis:7-alpine`,
`--save "" --appendonly no` (nessuna persistenza, come dichiarato in §2),
nessun volume nominato. Verificato con `task up` reale: tutti gli 8
servizi (postgres/neo4j/qdrant/opensearch/minio/kafka/kafka-connect/redis)
`healthy` dopo 33s.

`services/semantic-cache`: Go, stesso scheletro di `retrieval-engine`
(main.go con `observability.InitTracing` + `net.Listen` +
`grpc.StatsHandler(otelgrpc.NewServerHandler())`, nessun interceptor
SecurityContext — il contratto §2 di questa SPEC non lo prevede,
`acl_scope` è un campo esplicito della chiave, non derivato dai metadata
gRPC). `CacheValue` serializzato per intero via `proto.Marshal`/
`proto.Unmarshal` come valore Redis: `Get` ritorna esattamente lo stesso
messaggio scritto da `Put` (scenario 2), non uno ricostruito da un TTL
residuo ricalcolato. TTL via `SET ... EX ttl_seconds` (go-redis
`Set(ctx, key, val, duration)`; `duration == 0` → nessun `EX`, "nessuna
scadenza" nativa di go-redis, verificato dallo scenario 6 con un'attesa
reale).

**Nuove dipendenze Go, versioni esatte** (da `go.mod`/`go.sum` dopo `go mod
tidy`):
- `github.com/redis/go-redis/v9 v9.22.0` (client Redis di produzione).
- `github.com/testcontainers/testcontainers-go/modules/redis v0.44.0` (solo
  test, build tag `integration`) — trascina `testcontainers-go v0.44.0`
  come indiretta (più recente della `v0.43.0` usata da `retrieval-engine`:
  nessun conflitto, moduli Go separati con `go.sum` indipendenti).

**Deviazione — edge case "ttl_seconds molto grande" (§4, riga 3).**
Verificato EMPIRICAMENTE con `redis-cli` diretto contro il container reale
(`SET foo bar EX 4294967295` → `OK`, TTL confermato) che
`math.MaxUint32` secondi — il valore più grande rappresentabile dal campo
proto `uint32 ttl_seconds` — è ben accettato da Redis. Il vero errore
`"invalid expire time in 'set' command"` scatta solo per valori
dell'ordine di 10^16+ secondi (verificato anche questo empiricamente),
irraggiungibili da un `uint32`. Il trigger letterale descritto in questa
riga della tabella §4 non è quindi riproducibile con il tipo di campo
dichiarato in §2. Coperto invece con due test distinti: (a)
`EdgeCase_MaxUint32TTLNotSilentlyTruncated` — il valore massimo
rappresentabile viene scritto e riletto senza troncamento silenzioso; (b)
`EdgeCase_RedisUnavailable` — la garanzia di fondo dietro questa riga
(un errore Redis, qualunque ne sia la causa, diventa un errore gRPC
esplicito e non viene mai inghiottito) è verificata sullo STESSO percorso
di codice (`Set`/`Get` → errore → `codes.Unavailable`) tramite un client
Redis puntato su un indirizzo irraggiungibile, non tramite l'overflow
specifico che non è ottenibile con questo tipo.

**Deviazione — integrazione non cablata in `task test:integration`.**
`services/semantic-cache/internal/server/server_integration_test.go`
(build tag `integration`) non è aggiunto a `Taskfile.yml` `test:integration`:
lo stesso gap preesiste già per `services/retrieval-engine` (il suo
equivalente non è cablato nemmeno lì) — `Taskfile.yml` non è tra i file
toccabili dichiarati per questa SPEC, quindi non corretto qui. Verificato
manualmente con `go test -tags=integration ./...` da
`services/semantic-cache` (9/9 scenari ed edge case passati, container
Redis reale via testcontainers).

Nessun'altra deviazione. Nessun consumatore reale, nessun enforcement
`acl_scope`, nessuna invalidazione selettiva, nessuna persistenza Redis,
nessuna metrica dedicata oltre lo span OTel standard — tutto come da §5
Non-goals.
