//go:build integration

// SPEC-034 §3/§7 — test di integrazione: Kafka + Postgres + OpenSearch
// reali via testcontainers (stesso principio già stabilito per sink-graph/
// embedding-worker/sink-vector, SPEC-015/030/033) — nessun mock su nessuno
// dei tre fronti. Scenario 3 esegue una query full-text REALE (non solo un
// GetDocument per id, §7): verifica che l'analisi full-text sia
// genuinamente configurata, non solo che il documento esista.
//
// Esecuzione manuale (richiede Docker):
// go test -tags=integration ./internal/consumer/... -run TestSinkSearchConsumer -v
package consumer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcopensearch "github.com/testcontainers/testcontainers-go/modules/opensearch"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/sink-search/internal/consumer"
)

const (
	postgresUser     = "eci"
	postgresPassword = "eci-test-password-1234"
	postgresDB       = "eci"

	// embeddingWorkerConsumerName — valore LETTERALE del ConsumerName
	// esportato da services/embedding-worker/internal/consumer/consumer.go
	// (const ConsumerName = "embedding-worker"), copiato qui via lettura
	// diretta del sorgente: quel package vive sotto .../embedding-worker/
	// internal/..., quindi NON è importabile da questo modulo (regola
	// Go sui package "internal", vincolante a prescindere da eventuali
	// direttive replace tra moduli nello stesso repo) — SPEC-034 §4
	// scenario 5, deviazione dichiarata a fondo file/SPEC su come
	// verificato il fan-out senza poter importare la costante reale.
	embeddingWorkerConsumerName = "embedding-worker"
)

type stack struct {
	db         *sql.DB
	opensearch *opensearchapi.Client
}

func TestSinkSearchConsumer(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	t.Run("Scenario1_DocumentIndexedWithShaHexID", func(t *testing.T) {
		scenario1DocumentIndexedWithShaHexID(t, ctx, st)
	})
	t.Run("Scenario2_RedeliveryDoesNotDuplicate", func(t *testing.T) {
		scenario2RedeliveryDoesNotDuplicate(t, ctx, st)
	})
	t.Run("Scenario3_RealFullTextQueryFindsDocument", func(t *testing.T) {
		scenario3RealFullTextQueryFindsDocument(t, ctx, st)
	})
	t.Run("Scenario4_EnsureIndexIdempotent", func(t *testing.T) {
		scenario4EnsureIndexIdempotent(t, ctx, st)
	})
	t.Run("Scenario5_DistinctConsumerGroupFanOut", func(t *testing.T) {
		scenario5DistinctConsumerGroupFanOut(t, ctx, st)
	})
	t.Run("EdgeCase_OpenSearchUnreachableAtStartupReturnsErr", func(t *testing.T) {
		edgeCaseOpenSearchUnreachableAtStartupReturnsErr(t)
	})
	t.Run("EdgeCase_EmptyTextChunkStillIndexed", func(t *testing.T) {
		edgeCaseEmptyTextChunkStillIndexed(t, ctx, st)
	})
}

// ============================================================
// Scenario 1 — id SHA-256 esadecimale, confermato con scrittura reale
// (SPEC-034 §2/§9: "non presunto dalla sola documentazione").
// ============================================================

func scenario1DocumentIndexedWithShaHexID(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)

	// 64 caratteri esadecimali, stessa forma di code_node.id/entity_id
	// (SPEC-013) — usato qui come chunk id per confermare esplicitamente
	// che OpenSearch lo accetta come DocumentID (SPEC-034 §2/§9), a
	// prescindere dal fatto che code_chunk.id nella pipeline reale sia in
	// realtà un UUID Postgres (vedi nota nelle deviazioni della SPEC).
	chunkID := "d7ed32724f9bfdf61b8b47e1916750ea87441a0a4dee536f13f07f2ea1003510"
	entityID := "entity-1"
	text := "func Validate(prices []float64) error { return nil }"
	provenance := map[string]any{"path": "order_service.go"}
	eventID := uuid.NewString()

	payload := codeChunkPayload(chunkID, entityID, 0, text, provenance)
	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, payload, eventID)

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeStored {
		t.Fatalf("outcome = %v, want OutcomeStored", outcome)
	}

	doc := getDocument(t, ctx, st.opensearch, chunkID)
	if doc["text"] != text {
		t.Errorf("doc['text'] = %v, want %q", doc["text"], text)
	}
	if doc["entity_id"] != entityID {
		t.Errorf("doc['entity_id'] = %v, want %q", doc["entity_id"], entityID)
	}
	if doc["tenant_id"] != "tenant-test" || doc["repo"] != "sample-repo" || doc["acl_group"] != "developers" {
		t.Errorf("security labels = (%v,%v,%v), want (%q,%q,%q)",
			doc["tenant_id"], doc["repo"], doc["acl_group"],
			"tenant-test", "sample-repo", "developers")
	}
	gotProvenance, ok := doc["provenance"].(map[string]any)
	if !ok {
		t.Fatalf("doc['provenance'] non è un oggetto: %v", doc["provenance"])
	}
	if gotProvenance["path"] != "order_service.go" {
		t.Errorf("doc['provenance']['path'] = %v, want %q", gotProvenance["path"], "order_service.go")
	}

	assertProcessedEvent(t, ctx, st.db, eventID, consumer.ConsumerName)
}

// ============================================================
// Scenario 2 — redelivery at-least-once PRIMA del commit dell'offset.
// ============================================================

func scenario2RedeliveryDoesNotDuplicate(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)

	chunkID := uuid.NewString()
	payload := codeChunkPayload(chunkID, "entity-2", 0, "func Foo() {}", nil)
	eventID := uuid.NewString()
	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, payload, eventID)

	groupID := "sink-search-test-scenario2-" + eventID

	reader1 := newReaderWithGroup(brokers, groupID)
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	msg1, err := reader1.FetchMessage(fetchCtx)
	cancel()
	if err != nil {
		t.Fatalf("FetchMessage (prima consegna): %v", err)
	}
	outcome1, err := consumer.ProcessMessage(ctx, deps, msg1.Topic, msg1.Value, msg1.Headers)
	if err != nil {
		t.Fatalf("ProcessMessage (prima consegna): %v", err)
	}
	if outcome1 != consumer.OutcomeStored {
		t.Fatalf("outcome1 = %v, want OutcomeStored", outcome1)
	}
	if err := reader1.Close(); err != nil {
		t.Fatalf("chiusura reader1 (simulazione riavvio): %v", err)
	}

	if !documentExists(t, ctx, st.opensearch, chunkID) {
		t.Fatalf("documento id=%s atteso dopo la prima consegna", chunkID)
	}

	reader2 := newReaderWithGroup(brokers, groupID)
	defer reader2.Close()
	fetchCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	msg2, err := reader2.FetchMessage(fetchCtx2)
	cancel2()
	if err != nil {
		t.Fatalf("FetchMessage (redelivery): %v", err)
	}
	outcome2, err := consumer.ProcessMessage(ctx, deps, msg2.Topic, msg2.Value, msg2.Headers)
	if err != nil {
		t.Fatalf("ProcessMessage (redelivery): %v", err)
	}
	if outcome2 != consumer.OutcomeDuplicate {
		t.Fatalf("outcome2 = %v, want OutcomeDuplicate (dedup via processed_events)", outcome2)
	}
	if err := reader2.CommitMessages(ctx, msg2); err != nil {
		t.Fatalf("commit offset dopo redelivery: %v", err)
	}
}

// ============================================================
// Scenario 3 — query full-text REALE (§7: non solo GetDocument per id).
// ============================================================

func scenario3RealFullTextQueryFindsDocument(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)

	chunkID := uuid.NewString()
	// Parola rara e distintiva, improbabile in falsi positivi da altri
	// documenti scritti da altri scenari nello stesso indice condiviso.
	text := "func ReconcileWidgetInventory(warehouseID string) error { return nil }"
	payload := codeChunkPayload(chunkID, "entity-3", 0, text, nil)
	eventID := uuid.NewString()
	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, payload, eventID)

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeStored {
		t.Fatalf("outcome = %v, want OutcomeStored", outcome)
	}

	// Refresh esplicito: senza, il documento potrebbe non essere ancora
	// visibile alla ricerca (OpenSearch è near-real-time per costruzione,
	// non ci sono garanzie di visibilità immediata dopo un index/Create).
	refreshIndex(t, ctx, st.opensearch)

	hits := searchMatch(t, ctx, st.opensearch, "text", "ReconcileWidgetInventory")
	found := false
	for _, id := range hits {
		if id == chunkID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("query full-text su 'ReconcileWidgetInventory' non ha trovato il documento id=%s tra gli hit: %v", chunkID, hits)
	}
}

// ============================================================
// Scenario 4 — EnsureIndex idempotente.
// ============================================================

func scenario4EnsureIndexIdempotent(t *testing.T, ctx context.Context, st *stack) {
	exists := indexExists(t, ctx, st.opensearch)
	if !exists {
		t.Fatalf("precondizione: l'indice %q deve già esistere (creato da setupStack via EnsureIndex)", consumer.IndexName)
	}

	// Riavvio con indice già esistente: non deve fallire né tentare di
	// ricrearlo (SPEC-034 §3 scenario 4).
	if err := consumer.EnsureIndex(ctx, st.opensearch); err != nil {
		t.Fatalf("EnsureIndex su indice già esistente deve essere un no-op, non un errore: %v", err)
	}
}

// ============================================================
// Scenario 5 — fan-out: due consumer group DISTINTI sullo stesso topic
// ricevono ENTRAMBI il messaggio, indipendentemente (SPEC-034 §2/§3
// scenario 5) — verificato con due gruppi REALMENTE attivi in parallelo
// sullo stesso broker/topic, non presunto dal codice.
// ============================================================

func scenario5DistinctConsumerGroupFanOut(t *testing.T, ctx context.Context, st *stack) {
	_ = st
	brokers := startKafka(t, ctx)

	chunkID := uuid.NewString()
	payload := codeChunkPayload(chunkID, "entity-5", 0, "func Bar() {}", nil)
	eventID := uuid.NewString()
	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, payload, eventID)

	if consumer.ConsumerName == embeddingWorkerConsumerName {
		t.Fatalf("precondizione violata: sink-search e embedding-worker hanno LO STESSO consumer group id (%q) — il fan-out non funzionerebbe", consumer.ConsumerName)
	}

	sinkSearchReader := newReaderWithGroup(brokers, consumer.ConsumerName)
	defer sinkSearchReader.Close()
	embeddingWorkerReader := newReaderWithGroup(brokers, embeddingWorkerConsumerName)
	defer embeddingWorkerReader.Close()

	fetchCtx1, cancel1 := context.WithTimeout(ctx, 30*time.Second)
	msgA, err := sinkSearchReader.FetchMessage(fetchCtx1)
	cancel1()
	if err != nil {
		t.Fatalf("FetchMessage (gruppo sink-search): %v", err)
	}
	if string(msgA.Value) != string(payload) {
		t.Fatalf("messaggio ricevuto dal gruppo sink-search non combacia col messaggio prodotto")
	}
	if err := sinkSearchReader.CommitMessages(ctx, msgA); err != nil {
		t.Fatalf("commit offset gruppo sink-search: %v", err)
	}

	fetchCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	msgB, err := embeddingWorkerReader.FetchMessage(fetchCtx2)
	cancel2()
	if err != nil {
		t.Fatalf("FetchMessage (gruppo embedding-worker) — il messaggio doveva essere consegnato ANCHE a questo gruppo, indipendentemente dal primo: %v", err)
	}
	if string(msgB.Value) != string(payload) {
		t.Fatalf("messaggio ricevuto dal gruppo embedding-worker non combacia col messaggio prodotto")
	}
	if err := embeddingWorkerReader.CommitMessages(ctx, msgB); err != nil {
		t.Fatalf("commit offset gruppo embedding-worker: %v", err)
	}
}

// ============================================================
// Edge case §4 — OpenSearch irraggiungibile all'avvio.
// ============================================================

func edgeCaseOpenSearchUnreachableAtStartupReturnsErr(t *testing.T) {
	unreachable, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{"http://127.0.0.1:1"}},
	})
	if err != nil {
		return // errore già alla costruzione: comunque esplicito, non un panic.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.EnsureIndex(ctx, unreachable); err == nil {
		t.Fatal("atteso errore esplicito da EnsureIndex con OpenSearch irraggiungibile, ottenuto nil")
	}
}

// ============================================================
// Edge case §4 — testo vuoto: comunque indicizzato, nessun caso speciale.
// ============================================================

func edgeCaseEmptyTextChunkStillIndexed(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)

	chunkID := uuid.NewString()
	payload := codeChunkPayload(chunkID, "entity-empty", 0, "", nil)
	eventID := uuid.NewString()
	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, payload, eventID)

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeStored {
		t.Fatalf("outcome (testo vuoto) = %v, want OutcomeStored, nessun caso speciale atteso", outcome)
	}
	doc := getDocument(t, ctx, st.opensearch, chunkID)
	if doc["text"] != "" {
		t.Errorf("doc['text'] = %v, want stringa vuota", doc["text"])
	}
}

// ============================================================
// Harness: Postgres + OpenSearch (testcontainers, condivisi tra scenari).
// ============================================================

func setupStack(t *testing.T, ctx context.Context) *stack {
	t.Helper()
	db := startPostgres(t, ctx)
	osClient := startOpenSearch(t, ctx)

	if err := consumer.EnsureIndex(ctx, osClient); err != nil {
		t.Fatalf("EnsureIndex (setup iniziale): %v", err)
	}

	return &stack{db: db, opensearch: osClient}
}

func startOpenSearch(t *testing.T, ctx context.Context) *opensearchapi.Client {
	t.Helper()
	container, err := tcopensearch.Run(ctx, "opensearchproject/opensearch:2.11.1")
	if err != nil {
		t.Fatalf("avvio container opensearch: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container opensearch: %v", err)
		}
	})

	address, err := container.Address(ctx)
	if err != nil {
		t.Fatalf("opensearch Address: %v", err)
	}

	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{address}},
	})
	if err != nil {
		t.Fatalf("opensearchapi NewClient: %v", err)
	}
	return client
}

func startKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Fatalf("avvio container kafka: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container kafka: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka Brokers: %v", err)
	}
	ensureTopics(t, ctx, brokers, consumer.TopicCodeChunk)
	return brokers
}

func ensureTopics(t *testing.T, ctx context.Context, brokers []string, topics ...string) {
	t.Helper()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka per creazione topic: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("kafka Controller: %v", err)
	}
	controllerAddr := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		t.Fatalf("dial kafka controller (%s): %v", controllerAddr, err)
	}
	defer controllerConn.Close()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := controllerConn.CreateTopics(configs...); err != nil {
		t.Fatalf("CreateTopics %v: %v", topics, err)
	}
}

func startPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithUsername(postgresUser),
		tcpostgres.WithPassword(postgresPassword),
		tcpostgres.WithDatabase(postgresDB),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("avvio container postgres:17: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container postgres: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}

	// processed_events (D1, 0001_init.up.sql) è l'unica tabella richiesta
	// da questo consumer (nessuna tabella specifica di sink-search) — DDL
	// applicato qui direttamente, senza il CLI 'migrate' esterno (stesso
	// principio di sink-vector/SPEC-033: questo servizio non ha
	// migrazioni proprie sotto contracts/).
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE processed_events (
			event_id       UUID PRIMARY KEY,
			consumer_name  TEXT NOT NULL,
			processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		t.Fatalf("creazione processed_events: %v", err)
	}
	return db
}

// ============================================================
// Produzione/consumo messaggi sintetici.
// ============================================================

func newDeps(st *stack) consumer.Deps {
	return consumer.Deps{
		DB:         st.db,
		OpenSearch: st.opensearch,
		Logf:       func(format string, args ...any) {},
	}
}

func produce(t *testing.T, ctx context.Context, brokers []string, topic, key string, value []byte, eventID string) {
	t.Helper()
	headers := []kafka.Header{{Key: "event_id", Value: []byte(eventID)}}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value, Headers: headers}); err != nil {
		t.Fatalf("produzione messaggio sintetico su %s: %v", topic, err)
	}
}

// SPEC-035: FetchAndProcess accetta ora un resilience.ProcessFunc iniettato
// (nessuna modifica alla logica applicativa di ProcessMessage stessa) e
// ritorna il suo Outcome generico, non più il consumer.Outcome specifico
// del servizio. L'adapter chiama ProcessMessage DIRETTAMENTE (nessun
// resilience.WithRetryAndDLQ: questi scenari testano la logica applicativa
// del sink, non retry/DLQ, già coperto esaustivamente dal test di
// libs/go/eci/resilience, SPEC-035 §7) e cattura il suo Outcome reale
// tramite una variabile catturata per closure.
func fetchAndProcessOnce(t *testing.T, ctx context.Context, brokers []string, deps consumer.Deps) consumer.Outcome {
	t.Helper()
	reader := newReaderWithGroup(brokers, "sink-search-test-"+uuid.NewString())
	defer reader.Close()
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var innerOutcome consumer.Outcome
	process := func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		var err error
		innerOutcome, err = consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return 0, err
	}
	if _, err := consumer.FetchAndProcess(fetchCtx, reader, process); err != nil {
		t.Fatalf("FetchAndProcess: %v", err)
	}
	return innerOutcome
}

func newReaderWithGroup(brokers []string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    []string{consumer.TopicCodeChunk},
		CommitInterval: 0,
	})
}

// codeChunkPayload rispecchia ESATTAMENTE la forma prodotta da persist.rs
// per CodeChunk (SPEC-029/032: {id, entity_id, chunk_index, text,
// char_count, provenance?}).
func codeChunkPayload(id, entityID string, chunkIndex int, text string, provenance map[string]any) []byte {
	if provenance == nil {
		provenance = map[string]any{}
	}
	provenance["tenant_id"] = "tenant-test"
	provenance["repo"] = "sample-repo"
	provenance["acl_group"] = "developers"
	m := map[string]any{
		"id":          id,
		"entity_id":   entityID,
		"chunk_index": chunkIndex,
		"text":        text,
		"char_count":  len(text),
	}
	if provenance != nil {
		m["provenance"] = provenance
	}
	out, _ := json.Marshal(m)
	return out
}

// ============================================================
// Verifiche dirette su OpenSearch.
// ============================================================

func getDocument(t *testing.T, ctx context.Context, client *opensearchapi.Client, id string) map[string]any {
	t.Helper()
	resp, err := client.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      consumer.IndexName,
		DocumentID: id,
	})
	if err != nil {
		t.Fatalf("Document.Get id=%s: %v", id, err)
	}
	if !resp.Found {
		t.Fatalf("documento id=%s non trovato", id)
	}
	var source map[string]any
	if err := json.Unmarshal(resp.Source, &source); err != nil {
		t.Fatalf("decodifica _source id=%s: %v", id, err)
	}
	return source
}

// documentExists/indexExists: come consumer.EnsureIndex, ispezionano
// StatusCode direttamente sulla *opensearch.Response ritornata invece di
// fidarsi del solo `error` — Document.Exists/Indices.Exists sono richieste
// HEAD (corpo sempre vuoto), il meccanismo do() generico del client tenta
// comunque di decodificare un corpo di errore JSON su qualunque status
// >= 400 (404 incluso, l'esito NORMALE di "non esiste"), fallendo con
// "failed to json unmarshal body" su un corpo vuoto — verificato
// empiricamente (SPEC-034 §7).
func documentExists(t *testing.T, ctx context.Context, client *opensearchapi.Client, id string) bool {
	t.Helper()
	resp, err := client.Document.Exists(ctx, opensearchapi.DocumentExistsReq{
		Index:      consumer.IndexName,
		DocumentID: id,
	})
	if resp == nil {
		t.Fatalf("Document.Exists id=%s: %v", id, err)
	}
	return resp.StatusCode == 200
}

func indexExists(t *testing.T, ctx context.Context, client *opensearchapi.Client) bool {
	t.Helper()
	resp, err := client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{
		Indices: []string{consumer.IndexName},
	})
	if resp == nil {
		t.Fatalf("Indices.Exists: %v", err)
	}
	return resp.StatusCode == 200
}

func refreshIndex(t *testing.T, ctx context.Context, client *opensearchapi.Client) {
	t.Helper()
	if _, err := client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{
		Index: []string{consumer.IndexName},
	}); err != nil {
		t.Fatalf("Indices.Refresh: %v", err)
	}
}

// searchMatch esegue una query match REALE (§7) sul campo dato e ritorna
// gli id dei documenti risultato.
func searchMatch(t *testing.T, ctx context.Context, client *opensearchapi.Client, field, value string) []string {
	t.Helper()
	query := map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				field: value,
			},
		},
	}
	resp, err := client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{consumer.IndexName},
		Body:    opensearchutil.NewJSONReader(query),
	})
	if err != nil {
		t.Fatalf("Search match %s=%q: %v", field, value, err)
	}
	ids := make([]string, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		ids = append(ids, hit.ID)
	}
	return ids
}

func assertProcessedEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID, wantConsumerName string) {
	t.Helper()
	var consumerName string
	err := db.QueryRowContext(ctx, "SELECT consumer_name FROM processed_events WHERE event_id = $1", eventID).Scan(&consumerName)
	if err != nil {
		t.Fatalf("processed_events per event_id=%s: %v", eventID, err)
	}
	if consumerName != wantConsumerName {
		t.Errorf("processed_events.consumer_name = %q, want %q", consumerName, wantConsumerName)
	}
}
