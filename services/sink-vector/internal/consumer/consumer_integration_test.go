//go:build integration

// SPEC-033 §3/§7 — test di integrazione: Kafka + Postgres + Qdrant reali
// via testcontainers (stesso principio già stabilito per sink-graph/
// embedding-worker, SPEC-015/030) — nessun mock su nessuno dei tre fronti.
// Verifica diretta del punto scritto in Qdrant (query di lettura del punto
// per id), non solo che il consumer non sia andato in errore (§7).
//
// Esecuzione manuale (richiede Docker e 'migrate' sul PATH):
// go test -tags=integration ./internal/consumer/... -run TestSinkVectorConsumer -v
package consumer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/qdrant/go-client/qdrant"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcqdrant "github.com/testcontainers/testcontainers-go/modules/qdrant"

	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/sink-vector/internal/consumer"
)

const (
	postgresUser     = "eci"
	postgresPassword = "eci-test-password-1234"
	postgresDB       = "eci"
)

type stack struct {
	db     *sql.DB
	qdrant *qdrant.Client
}

func TestSinkVectorConsumer(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	t.Run("Scenario1_PointWrittenWithVectorAndPayload", func(t *testing.T) {
		scenario1PointWrittenWithVectorAndPayload(t, ctx, st)
	})
	t.Run("Scenario2_RedeliveryDoesNotDuplicate", func(t *testing.T) {
		scenario2RedeliveryDoesNotDuplicate(t, ctx, st)
	})
	t.Run("Scenario3And4_EnsureCollectionIdempotent", func(t *testing.T) {
		scenario3And4EnsureCollectionIdempotent(t, ctx, st)
	})
	t.Run("EdgeCase_QdrantUnreachableAtStartupReturnsErr", func(t *testing.T) {
		edgeCaseQdrantUnreachableAtStartupReturnsErr(t)
	})
	t.Run("EdgeCase_MessageWithoutSecurityScopeRejected", func(t *testing.T) {
		edgeCaseMessageWithoutSecurityScopeRejected(t, ctx, st)
	})
}

// ============================================================
// Scenario 1
// ============================================================

func scenario1PointWrittenWithVectorAndPayload(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)

	embeddingID := uuid.NewString()
	chunkID := uuid.NewString()
	entityID := "d7ed32724f9bfdf61b8b47e1916750ea87441a0a4dee536f13f07f2ea1003510"
	vector := syntheticVector(1)
	provenance := map[string]any{"path": "order_service.go"}
	eventID := uuid.NewString()

	payload := codeEmbeddingPayload(embeddingID, chunkID, entityID, vector, "test-model", provenance)
	produce(t, ctx, brokers, consumer.TopicCodeEmbedding, embeddingID, payload, eventID)

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeStored {
		t.Fatalf("outcome = %v, want OutcomeStored", outcome)
	}

	pointID := consumer.DerivePointID(embeddingID)
	points, err := st.qdrant.Get(ctx, &qdrant.GetPoints{
		CollectionName: consumer.CollectionName,
		Ids:            []*qdrant.PointId{qdrant.NewID(pointID)},
		WithVectors:    qdrant.NewWithVectors(true),
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		t.Fatalf("Get punto id=%s: %v", pointID, err)
	}
	if len(points) != 1 {
		t.Fatalf("punti trovati per id=%s: %d, want 1", pointID, len(points))
	}
	point := points[0]

	gotVector := point.GetVectors().GetVector().GetData()
	if len(gotVector) != len(vector) {
		t.Fatalf("len(vector) = %d, want %d", len(gotVector), len(vector))
	}
	// Confronto sulla DIREZIONE normalizzata, non sui valori grezzi:
	// verificato empiricamente che una collection Distance_Cosine
	// normalizza i vettori alla scrittura (Qdrant memorizza componenti
	// pre-normalizzate a norma 1 per il calcolo efficiente della cosine
	// similarity) — il valore grezzo ritornato da Get NON combacia
	// byte-per-byte con l'input, per costruzione di Qdrant stesso, non per
	// un bug dell'implementazione (verificato: gotVector[i] == vector[i] /
	// ‖vector‖ entro la precisione float32). "Stesso vettore" (SPEC-033 §3
	// scenario 1) è quindi verificato come "stessa direzione", l'unica
	// invariante che sopravvive alla normalizzazione lato server.
	if sim := cosineSimilarity(gotVector, vector); sim < 0.9999 {
		t.Fatalf("cosine similarity tra vettore atteso e ritornato = %v, want >= 0.9999 (stessa direzione)", sim)
	}

	payloadMap := point.GetPayload()
	if got := payloadMap["node_id"].GetStringValue(); got != entityID {
		t.Errorf("payload['node_id'] = %q, want %q", got, entityID)
	}
	if got := payloadMap["domain"].GetStringValue(); got != "code" {
		t.Errorf("payload['domain'] = %q, want %q", got, "code")
	}
	gotProvenance := payloadMap["provenance"].GetStructValue().GetFields()
	if got := gotProvenance["path"].GetStringValue(); got != "order_service.go" {
		t.Errorf("payload['provenance']['path'] = %q, want %q", got, "order_service.go")
	}

	assertProcessedEvent(t, ctx, st.db, eventID)
}

// ============================================================
// Scenario 2 — redelivery at-least-once PRIMA del commit dell'offset.
// ============================================================

func scenario2RedeliveryDoesNotDuplicate(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)

	embeddingID := uuid.NewString()
	vector := syntheticVector(2)
	payload := codeEmbeddingPayload(embeddingID, uuid.NewString(), "entity-2", vector, "test-model", nil)
	eventID := uuid.NewString()
	produce(t, ctx, brokers, consumer.TopicCodeEmbedding, embeddingID, payload, eventID)

	groupID := "sink-vector-test-scenario2-" + eventID

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

	pointID := consumer.DerivePointID(embeddingID)
	if n := countPoints(t, ctx, st.qdrant, pointID); n != 1 {
		t.Fatalf("punti con id=%s dopo la prima consegna = %d, want 1", pointID, n)
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

	if n := countPoints(t, ctx, st.qdrant, pointID); n != 1 {
		t.Fatalf("punti con id=%s dopo la redelivery = %d, want ancora 1 (nessun duplicato)", pointID, n)
	}
}

// ============================================================
// Scenario 3/4 — EnsureCollection idempotente: crea se assente, no-op se
// già esistente.
// ============================================================

func scenario3And4EnsureCollectionIdempotent(t *testing.T, ctx context.Context, st *stack) {
	exists, err := st.qdrant.CollectionExists(ctx, consumer.CollectionName)
	if err != nil {
		t.Fatalf("CollectionExists: %v", err)
	}
	if !exists {
		t.Fatalf("precondizione: la collection %q deve già esistere (creata da setupStack via EnsureCollection)", consumer.CollectionName)
	}

	info, err := st.qdrant.GetCollectionInfo(ctx, consumer.CollectionName)
	if err != nil {
		t.Fatalf("GetCollectionInfo: %v", err)
	}
	vectorParams := info.GetConfig().GetParams().GetVectorsConfig().GetParams()
	if got := vectorParams.GetSize(); got != consumer.VectorSize {
		t.Errorf("vector size = %d, want %d", got, consumer.VectorSize)
	}
	if got := vectorParams.GetDistance(); got != qdrant.Distance_Cosine {
		t.Errorf("distance = %v, want %v", got, qdrant.Distance_Cosine)
	}

	// Scenario 4: EnsureCollection su una collection GIÀ esistente non deve
	// fallire né tentare di ricrearla.
	if err := consumer.EnsureCollection(ctx, st.qdrant); err != nil {
		t.Fatalf("EnsureCollection su collection già esistente deve essere un no-op, non un errore: %v", err)
	}
}

// ============================================================
// Edge case §4 — Qdrant irraggiungibile all'avvio: errore esplicito.
// ============================================================

func edgeCaseQdrantUnreachableAtStartupReturnsErr(t *testing.T) {
	// Porta privilegiata 1: nessun listener possibile senza root,
	// connessione rifiutata immediatamente (stesso principio già usato per
	// l'embedder irraggiungibile in SPEC-030 §3 scenario 4).
	unreachable, err := qdrant.NewClient(&qdrant.Config{Host: "127.0.0.1", Port: 1})
	if err != nil {
		// Alcuni client falliscono già alla costruzione se la connessione
		// gRPC lazy fallisce subito: comunque un errore esplicito, non un
		// panic né un avvio silenzioso.
		return
	}
	defer unreachable.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.EnsureCollection(ctx, unreachable); err == nil {
		t.Fatal("atteso errore esplicito da EnsureCollection con Qdrant irraggiungibile, ottenuto nil")
	}
}

// ============================================================
// Edge case §4 — messaggio CodeEmbedding senza provenance: punto scritto
// comunque, senza la chiave provenance nel payload.
// ============================================================

func edgeCaseMessageWithoutSecurityScopeRejected(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)

	embeddingID := uuid.NewString()
	vector := syntheticVector(3)
	payload := codeEmbeddingPayloadWithoutProvenance(embeddingID, uuid.NewString(), "entity-no-provenance", vector, "test-model")
	eventID := uuid.NewString()
	produce(t, ctx, brokers, consumer.TopicCodeEmbedding, embeddingID, payload, eventID)

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeInvalidSkipped {
		t.Fatalf("outcome = %v, want OutcomeInvalidSkipped", outcome)
	}

	pointID := consumer.DerivePointID(embeddingID)
	points, err := st.qdrant.Get(ctx, &qdrant.GetPoints{
		CollectionName: consumer.CollectionName,
		Ids:            []*qdrant.PointId{qdrant.NewID(pointID)},
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		t.Fatalf("Get punto id=%s: %v", pointID, err)
	}
	if len(points) != 0 {
		t.Fatalf("punti trovati per id=%s: %d, want 0", pointID, len(points))
	}
	if got := points[0].GetPayload()["node_id"].GetStringValue(); got != "entity-no-provenance" {
		t.Errorf("payload['node_id'] = %q, want %q", got, "entity-no-provenance")
	}
}

// ============================================================
// Harness: Postgres + Qdrant (testcontainers, condivisi tra scenari).
// ============================================================

func setupStack(t *testing.T, ctx context.Context) *stack {
	t.Helper()
	db := startPostgres(t, ctx)
	qc := startQdrant(t, ctx)

	if err := consumer.EnsureCollection(ctx, qc); err != nil {
		t.Fatalf("EnsureCollection (setup iniziale): %v", err)
	}

	return &stack{db: db, qdrant: qc}
}

func startQdrant(t *testing.T, ctx context.Context) *qdrant.Client {
	t.Helper()
	container, err := tcqdrant.Run(ctx, "qdrant/qdrant:v1.15.1")
	if err != nil {
		t.Fatalf("avvio container qdrant: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container qdrant: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("qdrant Host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, "6334/tcp")
	if err != nil {
		t.Fatalf("qdrant MappedPort 6334/tcp: %v", err)
	}

	client, err := qdrant.NewClient(&qdrant.Config{Host: host, Port: int(mappedPort.Num())})
	if err != nil {
		t.Fatalf("qdrant NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func startKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()
	// Stesso principio (e stessa deviazione dichiarata, SPEC-015 §10) già
	// usato da sink-graph/embedding-worker: confluentinc/confluent-local
	// risolve l'advertised.listeners dinamicamente, necessario per un
	// consumer/produttore reale dal processo host.
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
	ensureTopics(t, ctx, brokers, consumer.TopicCodeEmbedding)
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
	// da questo consumer (nessuna tabella specifica di sink-vector) — DDL
	// applicato qui direttamente, senza il CLI 'migrate' esterno (questo
	// servizio non ha migrazioni proprie sotto contracts/, SPEC-033
	// Contratti: "nessuno").
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
		DB:     st.db,
		Qdrant: st.qdrant,
		Logf:   func(format string, args ...any) {},
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
	reader := newReaderWithGroup(brokers, "sink-vector-test-"+uuid.NewString())
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
		GroupTopics:    []string{consumer.TopicCodeEmbedding},
		CommitInterval: 0,
	})
}

// codeEmbeddingPayload rispecchia ESATTAMENTE la forma prodotta da
// consumer.go di embedding-worker (SPEC-030/031/032:
// {id, chunk_id, entity_id, vector, model_id, embedding_dim, provenance?}).
func codeEmbeddingPayload(id, chunkID, entityID string, vector []float32, modelID string, provenance map[string]any) []byte {
	if provenance == nil {
		provenance = map[string]any{}
	}
	provenance["tenant_id"] = "tenant-test"
	provenance["repo"] = "sample-repo"
	provenance["acl_group"] = "developers"
	m := map[string]any{
		"id":            id,
		"chunk_id":      chunkID,
		"entity_id":     entityID,
		"vector":        vector,
		"model_id":      modelID,
		"embedding_dim": len(vector),
	}
	if provenance != nil {
		m["provenance"] = provenance
	}
	out, _ := json.Marshal(m)
	return out
}

func codeEmbeddingPayloadWithoutProvenance(id, chunkID, entityID string, vector []float32, modelID string) []byte {
	m := map[string]any{
		"id": id, "chunk_id": chunkID, "entity_id": entityID,
		"vector": vector, "model_id": modelID, "embedding_dim": len(vector),
	}
	out, _ := json.Marshal(m)
	return out
}

// syntheticVector produce un vettore deterministico di VectorSize elementi
// (nessuna chiamata a embedder-fake in questa SPEC — §7 richiede solo
// Kafka+Postgres+Qdrant reali, il contenuto del vettore stesso non è sotto
// test qui). seed distingue i vettori tra scenari diversi.
func syntheticVector(seed int) []float32 {
	v := make([]float32, consumer.VectorSize)
	for i := range v {
		v[i] = float32(seed) + float32(i)/float32(consumer.VectorSize)
	}
	return v
}

// cosineSimilarity confronta la DIREZIONE di due vettori (non la
// magnitudine) — necessario perché Qdrant normalizza i vettori scritti in
// una collection Distance_Cosine (verificato empiricamente, SPEC-033).
func cosineSimilarity(a, b []float32) float64 {
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func countPoints(t *testing.T, ctx context.Context, client *qdrant.Client, pointID string) int {
	t.Helper()
	points, err := client.Get(ctx, &qdrant.GetPoints{
		CollectionName: consumer.CollectionName,
		Ids:            []*qdrant.PointId{qdrant.NewID(pointID)},
	})
	if err != nil {
		t.Fatalf("Get per count, id=%s: %v", pointID, err)
	}
	return len(points)
}

func assertProcessedEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	var consumerName string
	err := db.QueryRowContext(ctx, "SELECT consumer_name FROM processed_events WHERE event_id = $1", eventID).Scan(&consumerName)
	if err != nil {
		t.Fatalf("processed_events per event_id=%s: %v", eventID, err)
	}
	if consumerName != consumer.ConsumerName {
		t.Errorf("processed_events.consumer_name = %q, want %q", consumerName, consumer.ConsumerName)
	}
}
