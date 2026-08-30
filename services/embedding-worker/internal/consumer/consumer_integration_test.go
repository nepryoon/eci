//go:build integration

// SPEC-030 §3/§7 — test di integrazione: Kafka + Postgres reali via
// testcontainers (stesso principio di services/sink-graph, SPEC-015) più
// embedder-fake avviato come vero sottoprocesso uvicorn (stesso principio
// di services/ingestion/tests/embedding_integration_test.rs, SPEC-023) —
// non mock su nessuno dei tre fronti. Il consumer (ProcessMessage/
// FetchAndProcess) è istanziato DIRETTAMENTE nel test, non via il binario
// embedding-worker: messaggi Kafka prodotti sinteticamente con la stessa
// forma del payload CodeChunk prodotto da persist.rs (SPEC-029).
//
// Esecuzione manuale (richiede Docker, 'migrate' e python3 sul PATH):
// go test -tags=integration ./internal/consumer/... -run TestEmbeddingWorkerConsumer -v
package consumer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/embedding-worker/internal/consumer"
	"github.com/eci-project/eci/services/embedding-worker/internal/embedclient"
)

const (
	postgresUser     = "eci"
	postgresPassword = "eci-test-password-1234"
	postgresDB       = "eci"
	modelID          = "embedder-fake-test"
	embeddingDim     = 1536 // fisso in embedder-fake, SPEC-023 §2.
)

type stack struct {
	db   *sql.DB
	fake *embedderFakeProcess
}

func TestEmbeddingWorkerConsumer(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	t.Run("Scenario1_ChunkConsumedProducesEmbeddingRow", func(t *testing.T) {
		scenario1ChunkConsumedProducesEmbeddingRow(t, ctx, st)
	})
	t.Run("Scenario2_RedeliveryDoesNotDuplicate", func(t *testing.T) {
		scenario2RedeliveryDoesNotDuplicate(t, ctx, st)
	})
	t.Run("Scenario3_OutboxRowWithVectorIncluded", func(t *testing.T) {
		scenario3OutboxRowWithVectorIncluded(t, ctx, st)
	})
	t.Run("Scenario4_UnreachableEmbedderOffsetNotCommittedThenRecovers", func(t *testing.T) {
		scenario4UnreachableEmbedderOffsetNotCommittedThenRecovers(t, ctx, st)
	})
	t.Run("Scenario5_TwoDifferentChunksGetDistinctVectors", func(t *testing.T) {
		scenario5TwoDifferentChunksGetDistinctVectors(t, ctx, st)
	})
	t.Run("EdgeCase_EmptyTextChunkStillEmbedded", func(t *testing.T) {
		edgeCaseEmptyTextChunkStillEmbedded(t, ctx, st)
	})
	t.Run("SPEC074_DeleteAcknowledgedWithoutEmbedder", func(t *testing.T) {
		spec074DeleteAcknowledgedWithoutEmbedder(t, ctx, st)
	})
}

func spec074DeleteAcknowledgedWithoutEmbedder(t *testing.T, ctx context.Context, st *stack) {
	chunkID := insertCodeChunkFixture(t, ctx, st.db, "must never be embedded")
	eventID := uniqueUUID(t)
	deps := consumer.Deps{
		DB: st.db, Embed: embedclient.New("http://127.0.0.1:1"),
		ModelID: modelID, Logf: t.Logf,
	}
	payload, _ := json.Marshal(map[string]any{
		"id": chunkID, "entity_id": "entity-delete",
		"provenance": map[string]any{
			"tenant_id": "tenant-test", "repo": "local",
			"acl_group": "developers", "path": "delete.go",
		},
	})
	outboxBefore := countRows(t, ctx, st.db, "outbox")
	outcome, err := consumer.ProcessMessage(
		ctx, deps, consumer.TopicCodeChunk, payload, eventHeaders(eventID, "DELETE"),
	)
	if err != nil || outcome != consumer.OutcomeTombstoneAcknowledged {
		t.Fatalf("delete outcome=%v err=%v", outcome, err)
	}
	if countEmbeddingsForChunk(t, ctx, st.db, chunkID) != 0 {
		t.Fatal("DELETE recreated a canonical embedding")
	}
	if got := countRows(t, ctx, st.db, "outbox"); got != outboxBefore {
		t.Fatalf("DELETE emitted a second tombstone/outbox row: %d -> %d", outboxBefore, got)
	}
	assertProcessedEvent(t, ctx, st.db, eventID)

	outcome, err = consumer.ProcessMessage(
		ctx, deps, consumer.TopicCodeChunk, payload, eventHeaders(eventID, "DELETE"),
	)
	if err != nil || outcome != consumer.OutcomeDuplicate {
		t.Fatalf("delete replay outcome=%v err=%v", outcome, err)
	}
}

// ============================================================
// Scenario 1
// ============================================================

func scenario1ChunkConsumedProducesEmbeddingRow(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	text := "func Validate() error { return nil }"
	chunkID := insertCodeChunkFixture(t, ctx, st.db, text)
	eventID := uniqueUUID(t)

	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, codeChunkPayload(chunkID, "entity-1", 0, text), eventID, "")

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeStored {
		t.Fatalf("outcome = %v, want OutcomeStored", outcome)
	}

	row := readEmbedding(t, ctx, st.db, chunkID)
	if row.chunkID != chunkID {
		t.Errorf("chunk_id = %q, want %q", row.chunkID, chunkID)
	}
	if len(row.vector) != embeddingDim {
		t.Errorf("len(vector) = %d, want %d", len(row.vector), embeddingDim)
	}
	if row.modelID != modelID {
		t.Errorf("model_id = %q, want %q", row.modelID, modelID)
	}
	if row.embeddingDim != embeddingDim {
		t.Errorf("embedding_dim = %d, want %d", row.embeddingDim, embeddingDim)
	}

	assertProcessedEvent(t, ctx, st.db, eventID)
}

// ============================================================
// Scenario 2 — redelivery at-least-once PRIMA del commit dell'offset.
// ============================================================

func scenario2RedeliveryDoesNotDuplicate(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	text := "func Foo() {}"
	chunkID := insertCodeChunkFixture(t, ctx, st.db, text)
	eventID := uniqueUUID(t)

	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, codeChunkPayload(chunkID, "entity-2", 0, text), eventID, "")

	groupID := "embedding-worker-test-scenario2-" + eventID

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

	if n := countEmbeddingsForChunk(t, ctx, st.db, chunkID); n != 1 {
		t.Fatalf("righe code_embedding dopo la prima consegna = %d, want 1", n)
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

	if n := countEmbeddingsForChunk(t, ctx, st.db, chunkID); n != 1 {
		t.Fatalf("righe code_embedding dopo la redelivery = %d, want ancora 1 (nessun duplicato)", n)
	}
}

// ============================================================
// Scenario 3 — riga outbox aggregate_type='CodeEmbedding' col vettore incluso.
// ============================================================

func scenario3OutboxRowWithVectorIncluded(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	text := "func Bar() {}"
	chunkID := insertCodeChunkFixture(t, ctx, st.db, text)
	eventID := uniqueUUID(t)
	traceID := "0123456789abcdef0123456789abcdef"
	entityID := "entity-3"
	wantProvenance := map[string]any{"path": "order_service.go"}

	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, codeChunkPayloadWithProvenance(chunkID, entityID, 0, text, wantProvenance), eventID, traceID)

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeStored {
		t.Fatalf("outcome = %v, want OutcomeStored", outcome)
	}

	var payloadRaw []byte
	var gotTraceID sql.NullString
	err := st.db.QueryRowContext(ctx,
		"SELECT payload, trace_id FROM outbox WHERE aggregate_type = 'CodeEmbedding' AND payload->>'chunk_id' = $1",
		chunkID,
	).Scan(&payloadRaw, &gotTraceID)
	if err != nil {
		t.Fatalf("query outbox CodeEmbedding per chunk_id=%s: %v", chunkID, err)
	}

	var payload map[string]any
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		t.Fatalf("decodifica payload outbox: %v", err)
	}
	vector, ok := payload["vector"].([]any)
	if !ok {
		t.Fatalf("payload['vector'] non è un array: %v", payload["vector"])
	}
	if len(vector) != embeddingDim {
		t.Fatalf("len(payload['vector']) = %d, want %d — il vettore stesso deve essere incluso, non solo un riferimento", len(vector), embeddingDim)
	}
	if payload["model_id"] != modelID {
		t.Errorf("payload['model_id'] = %v, want %q", payload["model_id"], modelID)
	}
	if !gotTraceID.Valid || gotTraceID.String != traceID {
		t.Errorf("trace_id outbox = %v, want %q", gotTraceID, traceID)
	}
	// SPEC-031 §3 scenario 1: entity_id del messaggio CodeChunk in ingresso
	// deve propagare invariato nel payload outbox di CodeEmbedding —
	// necessario a Qdrant (T3.1) per il payload (node_id, domain,
	// provenance) richiesto dall'ADD, senza dover interrogare Postgres.
	if payload["entity_id"] != entityID {
		t.Errorf("payload['entity_id'] = %v, want %q", payload["entity_id"], entityID)
	}
	// SPEC-032 §3 scenario 2: provenance del messaggio CodeChunk in ingresso
	// deve propagare invariato nel payload outbox di CodeEmbedding — ultimo
	// pezzo mancante del payload Qdrant (node_id, domain, provenance)
	// richiesto dall'ADD.
	gotProvenance, ok := payload["provenance"].(map[string]any)
	if !ok {
		t.Fatalf("payload['provenance'] non è un oggetto: %v", payload["provenance"])
	}
	if gotProvenance["path"] != wantProvenance["path"] {
		t.Errorf("payload['provenance']['path'] = %v, want %q", gotProvenance["path"], wantProvenance["path"])
	}
}

// ============================================================
// Scenario 4 — servizio di embedding irraggiungibile: errore loggato,
// offset NON committato; il messaggio viene riprocessato con successo al
// ritorno del servizio, non perso.
// ============================================================

func scenario4UnreachableEmbedderOffsetNotCommittedThenRecovers(t *testing.T, ctx context.Context, st *stack) {
	brokers := startKafka(t, ctx)
	text := "func Baz() {}"
	chunkID := insertCodeChunkFixture(t, ctx, st.db, text)
	eventID := uniqueUUID(t)

	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, codeChunkPayload(chunkID, "entity-4", 0, text), eventID, "")

	groupID := "embedding-worker-test-scenario4-" + eventID

	// Deps puntato su un indirizzo irraggiungibile (§2/§3 scenario 4):
	// nessun listener sulla porta privilegiata 1, connessione rifiutata
	// immediatamente, non un timeout lento.
	brokenDeps := newDeps(st)
	brokenDeps.Embed = embedclient.New("http://127.0.0.1:1")

	reader1 := newReaderWithGroup(brokers, groupID)
	fetchCtx1, cancel1 := context.WithTimeout(ctx, 30*time.Second)
	outcome, err := fetchAndProcessDirect(fetchCtx1, reader1, brokenDeps)
	cancel1()
	if err == nil {
		t.Fatal("atteso errore dal FetchAndProcess con embedder irraggiungibile")
	}
	_ = outcome
	// Stesso principio di sink-graph scenario 2 (SPEC-015 §10): l'offset
	// non è stato committato, quindi il reader va chiuso qui (simula un
	// riavvio) — un nuovo Reader con lo STESSO group id è l'unico modo per
	// ottenere la redelivery dello stesso messaggio; richiamare
	// FetchMessage sullo STESSO Reader avanzerebbe oltre senza mai
	// riconsegnarlo (nessun secondo messaggio esiste, il fetch si
	// bloccherebbe per sempre).
	if err := reader1.Close(); err != nil {
		t.Fatalf("chiusura reader1 (simulazione riavvio): %v", err)
	}

	if n := countEmbeddingsForChunk(t, ctx, st.db, chunkID); n != 0 {
		t.Fatalf("righe code_embedding dopo il fallimento = %d, want 0 (nessuna scrittura parziale)", n)
	}
	assertNoProcessedEvent(t, ctx, st.db, eventID)

	// "Ritorno del servizio": nuovo reader, STESSO group id (offset non
	// committato -> redelivery), Deps con l'embedder-fake reale -> il
	// messaggio viene riconsegnato e processato con successo, non perso.
	reader2 := newReaderWithGroup(brokers, groupID)
	defer reader2.Close()
	workingDeps := newDeps(st)
	fetchCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	outcome2, err := fetchAndProcessDirect(fetchCtx2, reader2, workingDeps)
	cancel2()
	if err != nil {
		t.Fatalf("FetchAndProcess dopo il ripristino: %v", err)
	}
	if outcome2 != consumer.OutcomeStored {
		t.Fatalf("outcome dopo il ripristino = %v, want OutcomeStored", outcome2)
	}
	if n := countEmbeddingsForChunk(t, ctx, st.db, chunkID); n != 1 {
		t.Fatalf("righe code_embedding dopo il ripristino = %d, want 1", n)
	}
}

// ============================================================
// Scenario 5 — due chunk diversi ricevono vettori indipendenti.
// ============================================================

func scenario5TwoDifferentChunksGetDistinctVectors(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	reader := newReaderWithGroup(brokers, "embedding-worker-test-scenario5-"+uniqueUUID(t))
	defer reader.Close()

	fileText := "package main\n\nfunc Validate() error { return nil }\n"
	methodText := "func Validate() error { return nil }"
	fileChunkID := insertCodeChunkFixture(t, ctx, st.db, fileText)
	methodChunkID := insertCodeChunkFixture(t, ctx, st.db, methodText)
	fileEventID := uniqueUUID(t)
	methodEventID := uniqueUUID(t)

	produce(t, ctx, brokers, consumer.TopicCodeChunk, fileChunkID, codeChunkPayload(fileChunkID, "entity-file", 0, fileText), fileEventID, "")
	produce(t, ctx, brokers, consumer.TopicCodeChunk, methodChunkID, codeChunkPayload(methodChunkID, "entity-method", 0, methodText), methodEventID, "")

	if o := fetchAndProcessWithReader(t, ctx, reader, deps); o != consumer.OutcomeStored {
		t.Fatalf("outcome (file chunk) = %v, want OutcomeStored", o)
	}
	if o := fetchAndProcessWithReader(t, ctx, reader, deps); o != consumer.OutcomeStored {
		t.Fatalf("outcome (method chunk) = %v, want OutcomeStored", o)
	}

	fileRow := readEmbedding(t, ctx, st.db, fileChunkID)
	methodRow := readEmbedding(t, ctx, st.db, methodChunkID)

	if fileRow.chunkID == methodRow.chunkID {
		t.Fatal("i due chunk devono avere chunk_id distinti (precondizione del test)")
	}
	if vectorsEqual(fileRow.vector, methodRow.vector) {
		t.Fatal("i due chunk hanno testo diverso: i vettori devono essere distinti, non identici")
	}
}

// ============================================================
// Edge case §4 — testo vuoto: comunque inviato all'embedding, nessun caso
// speciale.
// ============================================================

func edgeCaseEmptyTextChunkStillEmbedded(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	chunkID := insertCodeChunkFixture(t, ctx, st.db, "")
	eventID := uniqueUUID(t)

	produce(t, ctx, brokers, consumer.TopicCodeChunk, chunkID, codeChunkPayload(chunkID, "entity-empty", 0, ""), eventID, "")

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeStored {
		t.Fatalf("outcome (testo vuoto) = %v, want OutcomeStored, nessun caso speciale atteso", outcome)
	}
	row := readEmbedding(t, ctx, st.db, chunkID)
	if len(row.vector) != embeddingDim {
		t.Fatalf("len(vector) per chunk a testo vuoto = %d, want %d", len(row.vector), embeddingDim)
	}
}

// ============================================================
// Harness: Postgres + Kafka (testcontainers) + embedder-fake (sottoprocesso reale).
// ============================================================

func setupStack(t *testing.T, ctx context.Context) *stack {
	t.Helper()
	db := startPostgres(t, ctx)
	fake := startEmbedderFake(t)
	return &stack{db: db, fake: fake}
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
	if _, err := exec.LookPath("migrate"); err != nil {
		t.Fatalf("binario 'migrate' non trovato sul PATH: %v (vedi SPEC-005)", err)
	}

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

	migrationsDir := readRepoPath(t, "contracts", "sql", "migrations")
	cmd := exec.CommandContext(ctx, "migrate", "-source", "file://"+migrationsDir, "-database", dsn, "up")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("migrate up fallita: %v\noutput:\n%s", err, out)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

// ============================================================
// embedder-fake: sottoprocesso uvicorn reale (stesso principio di
// services/ingestion/tests/embedding_integration_test.rs, SPEC-023).
// ============================================================

type embedderFakeProcess struct {
	cmd     *exec.Cmd
	BaseURL string
}

func startEmbedderFake(t *testing.T) *embedderFakeProcess {
	t.Helper()
	fakesDir := readRepoPath(t, "fakes", "embedder-fake")
	uvicornBin := ensureEmbedderFakeVenv(t, fakesDir)
	port := freePort(t)

	cmd := exec.Command(uvicornBin, "embedder_fake.main:app", "--port", fmt.Sprintf("%d", port))
	cmd.Dir = fakesDir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("StderrPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("avvio uvicorn (%s): %v", uvicornBin, err)
	}
	// Drena stdout/stderr: senza, il buffer del pipe si riempie e il
	// processo figlio si blocca in scrittura (stesso motivo dichiarato nel
	// test Rust equivalente, SPEC-023).
	go drainPipe(stdout, "embedder-fake stdout")
	go drainPipe(stderr, "embedder-fake stderr")

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForReady(t, baseURL, 30*time.Second)

	fake := &embedderFakeProcess{cmd: cmd, BaseURL: baseURL}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return fake
}

func drainPipe(r io.Reader, label string) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			fmt.Fprintf(os.Stderr, "[%s] %s", label, buf[:n])
		}
		if err != nil {
			return
		}
	}
}

func ensureEmbedderFakeVenv(t *testing.T, fakesDir string) string {
	t.Helper()
	venvDir := filepath.Join(fakesDir, ".venv")
	uvicornBin := filepath.Join(venvDir, "bin", "uvicorn")
	if _, err := os.Stat(uvicornBin); err == nil {
		return uvicornBin
	}

	if out, err := exec.Command("python3", "-m", "venv", venvDir).CombinedOutput(); err != nil {
		t.Fatalf("creazione venv (%s): %v\n%s", venvDir, err, out)
	}

	repoRoot := readRepoPath(t)
	pip := filepath.Join(venvDir, "bin", "pip")
	libsPy := filepath.Join(repoRoot, "libs", "py")
	out, err := exec.Command(pip, "install", "-q", "-e", libsPy, "-e", fakesDir).CombinedOutput()
	if err != nil {
		t.Fatalf("pip install -e (libs/py, embedder-fake) fallito: %v\n%s", err, out)
	}
	return uvicornBin
}

func waitForReady(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	client := embedclient.New(baseURL)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := client.Embed(context.Background(), "readiness probe")
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("embedder-fake non pronto su %s entro %v: %v", baseURL, timeout, lastErr)
}

func freePort(t *testing.T) int {
	t.Helper()
	// Stesso principio del test Rust equivalente: bind su :0, leggi la
	// porta assegnata dal SO, richiudi subito.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind porta libera: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// ============================================================
// Produzione/consumo messaggi sintetici.
// ============================================================

func newDeps(st *stack) consumer.Deps {
	return consumer.Deps{
		DB:      st.db,
		Embed:   embedclient.New(st.fake.BaseURL),
		ModelID: modelID,
		Logf:    func(format string, args ...any) {},
	}
}

func produce(t *testing.T, ctx context.Context, brokers []string, topic, key string, value []byte, eventID, traceID string) {
	t.Helper()
	headers := eventHeaders(eventID, "UPSERT")
	if traceID != "" {
		headers = append(headers, kafka.Header{Key: "trace_id", Value: []byte(traceID)})
	}
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

func eventHeaders(eventID, operation string) []kafka.Header {
	return []kafka.Header{
		{Key: "event_id", Value: []byte(eventID)},
		{Key: "event_type", Value: []byte(operation)},
	}
}

func fetchAndProcessOnce(t *testing.T, ctx context.Context, brokers []string, deps consumer.Deps) consumer.Outcome {
	t.Helper()
	reader := newReaderWithGroup(brokers, "embedding-worker-test-"+uniqueUUID(t))
	defer reader.Close()
	return fetchAndProcessWithReader(t, ctx, reader, deps)
}

func fetchAndProcessWithReader(t *testing.T, ctx context.Context, reader *kafka.Reader, deps consumer.Deps) consumer.Outcome {
	t.Helper()
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err := fetchAndProcessDirect(fetchCtx, reader, deps)
	if err != nil {
		t.Fatalf("FetchAndProcess: %v", err)
	}
	return outcome
}

// fetchAndProcessDirect adatta Deps a un resilience.ProcessFunc (SPEC-035:
// FetchAndProcess ora accetta un ProcessFunc iniettato, nessuna modifica
// alla logica applicativa di ProcessMessage stessa) che chiama
// ProcessMessage DIRETTAMENTE — nessun resilience.WithRetryAndDLQ qui:
// questi scenari testano la logica applicativa del sink (embedding,
// dedup...), non retry/DLQ, già coperto esaustivamente dal test di
// libs/go/eci/resilience (SPEC-035 §7). Cattura il consumer.Outcome reale
// tramite una variabile catturata per closure, dato che il
// resilience.Outcome generico ritornato da FetchAndProcess non porta più
// questa granularità.
func fetchAndProcessDirect(ctx context.Context, reader *kafka.Reader, deps consumer.Deps) (consumer.Outcome, error) {
	var innerOutcome consumer.Outcome
	process := func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		var err error
		innerOutcome, err = consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return 0, err
	}
	_, err := consumer.FetchAndProcess(ctx, reader, process)
	return innerOutcome, err
}

func newReaderWithGroup(brokers []string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    []string{consumer.TopicCodeChunk},
		CommitInterval: 0,
	})
}

// fixtureASTHash è un valore CHAR(64) qualunque per il code_node fittizio
// creato da insertCodeChunkFixture — non validato dal consumer (che non
// legge mai code_node/code_chunk da Postgres, solo il payload Kafka), serve
// solo a soddisfare il NOT NULL/CHAR(64) di code_node.ast_hash (SPEC-005).
var fixtureASTHash = strings.Repeat("0", 64)

// insertCodeChunkFixture crea un code_node + code_chunk REALI in Postgres
// (SPEC-029) con un chunk_id noto, prerequisito per soddisfare la FK
// code_embedding.chunk_id REFERENCES code_chunk(id) (SPEC-030 §2) — il
// consumer stesso non legge mai queste righe (lavora sul payload Kafka),
// ma la riga deve esistere perché l'INSERT su code_embedding non violi il
// vincolo referenziale.
func insertCodeChunkFixture(t *testing.T, ctx context.Context, db *sql.DB, text string) string {
	t.Helper()
	nodeID := uniqueUUID(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
		 VALUES ($1, 'code', 'Function', 'fixture', $2, '{}'::jsonb)`,
		nodeID, fixtureASTHash,
	); err != nil {
		t.Fatalf("insertCodeChunkFixture: INSERT code_node: %v", err)
	}

	chunkID := uniqueUUID(t)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO code_chunk (id, entity_id, chunk_index, text, char_count)
		 VALUES ($1, $2, 0, $3, $4)`,
		chunkID, nodeID, text, len(text),
	); err != nil {
		t.Fatalf("insertCodeChunkFixture: INSERT code_chunk: %v", err)
	}
	return chunkID
}

// codeChunkPayload rispecchia ESATTAMENTE la forma prodotta da persist.rs
// per CodeChunk (SPEC-029 §2: {id, entity_id, chunk_index, text, char_count}).
func codeChunkPayload(chunkID, entityID string, chunkIndex int, text string) []byte {
	payload := map[string]any{
		"id":          chunkID,
		"entity_id":   entityID,
		"chunk_index": chunkIndex,
		"text":        text,
		"char_count":  len(text),
	}
	out, _ := json.Marshal(payload)
	return out
}

// codeChunkPayloadWithProvenance — stessa forma di codeChunkPayload, con in
// più il campo provenance (SPEC-032 §2/§3 scenario 2), usato solo dove
// serve verificarne la propagazione senza toccare gli altri scenari.
func codeChunkPayloadWithProvenance(chunkID, entityID string, chunkIndex int, text string, provenance map[string]any) []byte {
	payload := map[string]any{
		"id":          chunkID,
		"entity_id":   entityID,
		"chunk_index": chunkIndex,
		"text":        text,
		"char_count":  len(text),
		"provenance":  provenance,
	}
	out, _ := json.Marshal(payload)
	return out
}

// ============================================================
// Verifiche dirette su Postgres.
// ============================================================

type embeddingRow struct {
	chunkID      string
	vector       []float64
	modelID      string
	embeddingDim int
}

func readEmbedding(t *testing.T, ctx context.Context, db *sql.DB, chunkID string) embeddingRow {
	t.Helper()
	var row embeddingRow
	var vec pq.Float64Array
	err := db.QueryRowContext(ctx,
		"SELECT chunk_id::text, vector, model_id, embedding_dim FROM code_embedding WHERE chunk_id = $1",
		chunkID,
	).Scan(&row.chunkID, &vec, &row.modelID, &row.embeddingDim)
	if err != nil {
		t.Fatalf("readEmbedding chunk_id=%s: %v", chunkID, err)
	}
	row.vector = []float64(vec)
	return row
}

func countEmbeddingsForChunk(t *testing.T, ctx context.Context, db *sql.DB, chunkID string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, "SELECT count(*) FROM code_embedding WHERE chunk_id = $1", chunkID).Scan(&n)
	if err != nil {
		t.Fatalf("countEmbeddingsForChunk chunk_id=%s: %v", chunkID, err)
	}
	return n
}

func countRows(t *testing.T, ctx context.Context, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
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

func assertNoProcessedEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, "SELECT count(*) FROM processed_events WHERE event_id = $1", eventID).Scan(&n)
	if err != nil {
		t.Fatalf("count processed_events per event_id=%s: %v", eventID, err)
	}
	if n != 0 {
		t.Fatalf("processed_events per event_id=%s = %d, want 0 (un fallimento dell'embedding non deve marcare l'evento come processato)", eventID, n)
	}
}

func vectorsEqual(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func uniqueUUID(t *testing.T) string {
	t.Helper()
	return uuid.NewString()
}

func readRepoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// services/embedding-worker/internal/consumer/consumer_integration_test.go -> repo root 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
