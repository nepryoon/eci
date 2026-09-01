//go:build integration

// SPEC-039 §3 scenari 1/2/4/5/6, §4 edge case — verifica con Postgres+Qdrant
// REALI (testcontainers, migration reali applicate col CLI `migrate`,
// stesso pattern di tools/reconcile/internal/neo4jtarget/neo4jtarget_integration_test.go)
// che il Target Qdrant confronti davvero l'esistenza/correttezza del punto
// Qdrant derivato da ciascuna riga code_embedding e ripubblichi un payload
// strutturalmente identico a quello di embedding-worker (T3.1). Lo scenario
// 3 (ciclo CDC completo via Debezium/Kafka/sink-vector reali) è in
// qdranttarget_e2e_integration_test.go — stack ancora più pesante, file
// separato (SPEC-039 §7).
package qdranttarget_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/qdrant/go-client/qdrant"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcqdrant "github.com/testcontainers/testcontainers-go/modules/qdrant"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/qdranttarget"
)

const (
	dbUser     = "eci"
	dbPassword = "eci-test-password-1234"
	dbName     = "eci"

	insertCodeNode = `INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
		VALUES ($1, 'code', 'Function', $1, $2, $3::jsonb)`
	insertCodeChunk = `INSERT INTO code_chunk (id, domain, entity_id, chunk_index, text, char_count)
		VALUES ($1, 'code', $2, 0, 'text', 4)`
	insertCodeEmbedding = `INSERT INTO code_embedding (id, domain, chunk_id, vector, model_id, embedding_dim)
		VALUES ($1, 'code', $2, $3, $4, $5)`

	// collectionName/pointIDNamespace — STESSI letterali di
	// services/sink-vector/internal/consumer (non importabili, vedi
	// qdranttarget.go): replicati anche qui nel test per costruire punti
	// Qdrant di prova nella STESSA collection/con lo STESSO id che
	// sink-vector avrebbe usato.
	collectionName = "code_embeddings"
	vectorSize     = 1536
)

// pointIDNamespace — stesso letterale di qdranttarget.go/consumer.go
// (SPEC-033 §10 punto 2), usato dal test per calcolare INDIPENDENTEMENTE
// il point id atteso (scenario 5) e per costruire punti Qdrant di prova.
var pointIDNamespace = uuid.MustParse("5f3e8a1c-9b6d-4e2a-8c7f-1a2b3c4d5e6f")

func derivePointIDForTest(id string) string {
	return uuid.NewSHA1(pointIDNamespace, []byte(id)).String()
}

// ============================================================
// Harness: Postgres + Qdrant reali, condivisi da tutti gli scenari di
// questo file (un solo avvio, stesso spirito di neo4jtarget_integration_test.go).
// ============================================================

type stack struct {
	db     *sql.DB
	qdrant *qdrant.Client
}

func TestQdrantTarget(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	t.Run("Scenario1_MatchingPointNoRepublish", func(t *testing.T) {
		scenario1MatchingPointNoRepublish(t, ctx, st)
	})
	t.Run("Scenario2_MissingPointRepublishes", func(t *testing.T) {
		scenario2MissingPointRepublishes(t, ctx, st)
	})
	t.Run("Scenario4_DivergentNodeIDRepublishes", func(t *testing.T) {
		scenario4DivergentNodeIDRepublishes(t, ctx, st)
	})
	t.Run("Scenario5_PointIDDerivationMatchesSinkVector", func(t *testing.T) {
		scenario5PointIDDerivationMatchesSinkVector(t, ctx, st)
	})
	t.Run("Scenario6_RepublishedPayloadMatchesEmbeddingWorkerShape", func(t *testing.T) {
		scenario6RepublishedPayloadMatchesEmbeddingWorkerShape(t, ctx, st)
	})
	t.Run("EdgeCase_QdrantUnreachableDuringCheckPropagatesError", func(t *testing.T) {
		edgeCaseQdrantUnreachableDuringCheckPropagatesError(t, ctx, st)
	})
	t.Run("EdgeCase_RowDeletedBeforeRepublishReturnsExplicitError", func(t *testing.T) {
		edgeCaseRowDeletedBeforeRepublishReturnsExplicitError(t, ctx, st)
	})
}

// ============================================================
// Scenario 1 — punto Qdrant esiste con payload.node_id combaciante.
// ============================================================

func scenario1MatchingPointNoRepublish(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario1-entity")
	embeddingID := insertFullRow(t, ctx, st.db, entityID, smallVector(1))
	createQdrantPoint(t, ctx, st.qdrant, embeddingID, entityID)

	target := qdranttarget.New(st.qdrant, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if countOutboxRowsFor(t, ctx, st.db, embeddingID) != 0 {
		t.Fatalf("righe outbox per embedding_id=%s dopo Reconcile = >0, want 0 (nessuna ripubblicazione, node_id combaciante)", embeddingID)
	}
	for _, rowErr := range report.Errored {
		if rowErr.RowID == embeddingID {
			t.Fatalf("riga embedding_id=%s in errore: %v, want nessun errore (scenario 1, node_id combaciante)", embeddingID, rowErr.Err)
		}
	}
}

// ============================================================
// Scenario 2 — punto Qdrant del tutto assente (evento perso simulato).
// ============================================================

func scenario2MissingPointRepublishes(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario2-entity")
	embeddingID := insertFullRow(t, ctx, st.db, entityID, smallVector(2))
	// Nessun punto Qdrant creato: simula un evento CodeEmbedding perso.

	target := qdranttarget.New(st.qdrant, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (embedding_id=%s mancante in Qdrant)", embeddingID)
	}
	if got := countOutboxRowsFor(t, ctx, st.db, embeddingID); got != 1 {
		t.Fatalf("righe outbox per embedding_id=%s = %d, want 1", embeddingID, got)
	}
}

// ============================================================
// Scenario 4 — punto Qdrant presente ma con payload.node_id DIVERGENTE.
// Stesso comportamento di rilevamento+ripubblicazione dello scenario 2.
// ============================================================

func scenario4DivergentNodeIDRepublishes(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario4-entity-postgres")
	divergentNodeID := hash64("scenario4-entity-QDRANT-DIFFERENTE")
	embeddingID := insertFullRow(t, ctx, st.db, entityID, smallVector(4))
	createQdrantPoint(t, ctx, st.qdrant, embeddingID, divergentNodeID)

	target := qdranttarget.New(st.qdrant, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (embedding_id=%s con node_id divergente)", embeddingID)
	}
	if got := countOutboxRowsFor(t, ctx, st.db, embeddingID); got != 1 {
		t.Fatalf("righe outbox per embedding_id=%s = %d, want 1", embeddingID, got)
	}
	// Verifica diretta: il punto Qdrant esiste ancora col node_id vecchio
	// (nessuna scrittura Qdrant diretta da parte della riconciliazione —
	// SOLO la riga outbox viene scritta, l'upsert reale è compito di
	// sink-vector via CDC, verificato separatamente nello scenario 3).
	gotNodeID := readQdrantNodeID(t, ctx, st.qdrant, embeddingID)
	if gotNodeID != divergentNodeID {
		t.Errorf("payload.node_id Qdrant dopo Reconcile = %q, want invariato %q (Republish scrive SOLO outbox)", gotNodeID, divergentNodeID)
	}
}

// ============================================================
// Scenario 5 — il point ID derivato da questo plugin per una riga nota è
// IDENTICO a quello che sink-vector avrebbe derivato per la STESSA riga.
//
// Verifica DIRETTA: il point id atteso è calcolato QUI, indipendentemente
// dal codice sotto test, con la STESSA formula UUIDv5/namespace fisso di
// services/sink-vector/internal/consumer.DerivePointID (letterale
// verificato in implementazione, non presunto — vedi commento su
// qdranttarget.go). Un punto viene scritto SOLO a quell'id calcolato
// indipendentemente; se Check() lo trova (e combacia), significa che
// qdranttarget.derivePointID ha prodotto ESATTAMENTE lo stesso id per la
// stessa riga — se le due derivazioni divergessero anche di un solo byte,
// Check() interrogherebbe un id diverso da dove il punto è stato scritto e
// non lo troverebbe mai (matches=false, non un errore silenzioso).
// ============================================================

func scenario5PointIDDerivationMatchesSinkVector(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario5-entity")
	embeddingID := insertFullRow(t, ctx, st.db, entityID, smallVector(5))

	wantPointID := derivePointIDForTest(embeddingID) // calcolo indipendente, stessa formula di sink-vector
	upsertQdrantPointAtID(t, ctx, st.qdrant, wantPointID, entityID)

	target := qdranttarget.New(st.qdrant, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if countOutboxRowsFor(t, ctx, st.db, embeddingID) != 0 {
		t.Fatalf("righe outbox per embedding_id=%s dopo Reconcile = >0, want 0 (il punto scritto all'id derivato INDIPENDENTEMENTE deve essere trovato da Check, provando che la derivazione di qdranttarget produce lo STESSO id di sink-vector)", embeddingID)
	}
	for _, rowErr := range report.Errored {
		if rowErr.RowID == embeddingID {
			t.Fatalf("riga embedding_id=%s in errore: %v, want nessun errore (scenario 5)", embeddingID, rowErr.Err)
		}
	}
}

// ============================================================
// Scenario 6 — il payload ripubblicato è strutturalmente equivalente a
// quello che embedding-worker (T3.1) avrebbe scritto per la STESSA riga.
// ============================================================

func scenario6RepublishedPayloadMatchesEmbeddingWorkerShape(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario6-entity")
	vector := smallVector(6)
	provenance := map[string]any{
		"tenant_id": "tenant-reconcile",
		"repo":      "repo-reconcile",
		"acl_group": "developers",
		"path":      "order_service.go",
	}
	provenanceJSON, err := json.Marshal(provenance)
	if err != nil {
		t.Fatalf("marshal provenance: %v", err)
	}
	embeddingID := insertFullRowWithProvenance(t, ctx, st.db, entityID, vector, provenanceJSON)
	// Nessun punto Qdrant creato -> ripubblicazione.

	target := qdranttarget.New(st.qdrant, st.db)
	if _, err := framework.Reconcile(ctx, st.db, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	gotPayload := readOutboxPayload(t, ctx, st.db, embeddingID)
	chunkID := chunkIDFor(t, ctx, st.db, embeddingID)

	// Forma ESATTA prodotta da embedding-worker
	// (services/embedding-worker/internal/consumer/consumer.go,
	// storeEmbedding) per questa stessa riga: {id, chunk_id, entity_id,
	// vector, model_id, embedding_dim, provenance?} — replicata qui
	// letteralmente, NON derivata dall'implementazione sotto test.
	wantPayload := map[string]any{
		"id":            embeddingID,
		"chunk_id":      chunkID,
		"entity_id":     entityID,
		"vector":        vector,
		"model_id":      "test-model",
		"embedding_dim": len(vector),
		"provenance":    provenance,
	}

	var gotDecoded map[string]any
	if err := json.Unmarshal(gotPayload, &gotDecoded); err != nil {
		t.Fatalf("payload outbox non è JSON valido: %v (raw: %s)", err, gotPayload)
	}
	wantRaw, err := json.Marshal(wantPayload)
	if err != nil {
		t.Fatalf("marshal payload atteso: %v", err)
	}
	var wantDecoded map[string]any
	if err := json.Unmarshal(wantRaw, &wantDecoded); err != nil {
		t.Fatalf("unmarshal payload atteso: %v", err)
	}

	if !reflect.DeepEqual(gotDecoded, wantDecoded) {
		t.Fatalf("payload ripubblicato = %s, want strutturalmente equivalente a %s", gotPayload, wantRaw)
	}
}

// ============================================================
// Edge case §4 — Qdrant irraggiungibile durante Check: errore propagato,
// mai un matches=false silenzioso.
// ============================================================

func edgeCaseQdrantUnreachableDuringCheckPropagatesError(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("unreachable-entity")
	embeddingID := insertFullRow(t, ctx, st.db, entityID, smallVector(99))

	// Client Qdrant puntato su una porta privilegiata irraggiungibile senza
	// root — stesso principio già usato da
	// services/sink-vector/internal/consumer/consumer_integration_test.go
	// (edgeCaseQdrantUnreachableAtStartupReturnsErr).
	badClient, err := qdrant.NewClient(&qdrant.Config{Host: "127.0.0.1", Port: 1})
	if err != nil {
		t.Fatalf("qdrant.NewClient (indirizzo irraggiungibile): %v", err)
	}
	defer badClient.Close()

	target := qdranttarget.New(badClient, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v (SourceRows non deve fallire, solo Check)", err)
	}

	var found bool
	for _, rowErr := range report.Errored {
		if rowErr.RowID == embeddingID {
			found = true
			if rowErr.Err == nil {
				t.Fatalf("riga embedding_id=%s in Errored ma Err=nil", embeddingID)
			}
		}
	}
	if !found {
		t.Fatalf("riga embedding_id=%s non trovata in report.Errored = %+v, want un errore di Check propagato (Qdrant irraggiungibile), non un matches=false silenzioso", embeddingID, report.Errored)
	}
	if report.Republished != 0 {
		t.Errorf("report.Republished = %d, want 0 (un errore di Check non deve mai risultare in una ripubblicazione)", report.Republished)
	}
}

// ============================================================
// Edge case §4 — la riga completa non si trova più in Postgres al momento
// di Republish (race: cancellata tra SourceRows e Republish): errore
// esplicito, mai un payload parziale/fabbricato.
// ============================================================

func edgeCaseRowDeletedBeforeRepublishReturnsExplicitError(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("deleted-entity")
	embeddingID := insertFullRow(t, ctx, st.db, entityID, smallVector(98))
	row := framework.SourceRow{ID: embeddingID, Fingerprint: []byte(entityID)}

	deleteCodeEmbeddingRow(t, ctx, st.db, embeddingID)

	target := qdranttarget.New(st.qdrant, st.db)
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = target.Republish(ctx, tx, row)
	if err == nil {
		t.Fatalf("Republish(embedding_id=%s, cancellato da Postgres) = nil error, want un errore esplicito", embeddingID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("Republish error = %v, want che avvolga sql.ErrNoRows (riga non trovata), non un errore generico", err)
	}
}

// ============================================================
// Harness setup.
// ============================================================

func setupStack(t *testing.T, ctx context.Context) *stack {
	t.Helper()
	db := startMigratedPostgres(t, ctx)
	qc := startQdrant(t, ctx)
	return &stack{db: db, qdrant: qc}
}

func startMigratedPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	if _, err := exec.LookPath("migrate"); err != nil {
		t.Fatalf("binario 'migrate' non trovato sul PATH: %v", err)
	}

	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithUsername(dbUser),
		tcpostgres.WithPassword(dbPassword),
		tcpostgres.WithDatabase(dbName),
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

	migrationsDir := repoPath(t, "contracts", "sql", "migrations")
	cmd := exec.CommandContext(ctx, "migrate", "-source", "file://"+migrationsDir, "-database", dsn, "up")
	if out, err := cmd.CombinedOutput(); err != nil {
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

// startQdrant avvia Qdrant reale e crea la collection code_embeddings con
// Size=1536/Distance=Cosine — STESSA configurazione di
// services/sink-vector/internal/consumer.EnsureCollection (SPEC-033 §2),
// replicata qui perché quel codice non è importabile (vedi qdranttarget.go)
// e in produzione è sink-vector stesso, non qdranttarget, a crearla al
// proprio avvio (SPEC-039 è solo lettore/verificatore di questa
// collection, mai il suo creatore).
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

	if err := client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     vectorSize,
			Distance: qdrant.Distance_Cosine,
		}),
	}); err != nil {
		t.Fatalf("CreateCollection(%s): %v", collectionName, err)
	}

	return client
}

// ============================================================
// Helper Postgres.
// ============================================================

func insertFullRow(t *testing.T, ctx context.Context, db *sql.DB, entityID string, vector []float32) (embeddingID string) {
	t.Helper()
	return insertFullRowWithProvenance(t, ctx, db, entityID, vector,
		[]byte(`{"tenant_id":"tenant-reconcile","repo":"repo-reconcile","acl_group":"developers","path":"default.go"}`))
}

// insertFullRowWithProvenance inserisce code_node+code_chunk+code_embedding
// (JOIN completo richiesto da qdranttarget.SourceRows/Republish, §2) e
// ritorna l'id generato di code_embedding (il vero row.ID riconciliato).
// entityID è usato sia come code_node.id sia come code_chunk.entity_id
// (stesso valore, coerente con lo schema: code_chunk.entity_id REFERENCES
// code_node(id)).
func insertFullRowWithProvenance(t *testing.T, ctx context.Context, db *sql.DB, entityID string, vector []float32, provenance []byte) (embeddingID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, insertCodeNode, entityID, hash64(entityID), provenance); err != nil {
		t.Fatalf("INSERT code_node id=%s: %v", entityID, err)
	}

	chunkID := uuid.NewString()
	if _, err := db.ExecContext(ctx, insertCodeChunk, chunkID, entityID); err != nil {
		t.Fatalf("INSERT code_chunk id=%s: %v", chunkID, err)
	}

	err := db.QueryRowContext(ctx,
		`INSERT INTO code_embedding (domain, chunk_id, vector, model_id, embedding_dim)
		 VALUES ('code', $1, $2, 'test-model', $3) RETURNING id::text`,
		chunkID, pq.Float32Array(vector), len(vector),
	).Scan(&embeddingID)
	if err != nil {
		t.Fatalf("INSERT code_embedding chunk_id=%s: %v", chunkID, err)
	}
	return embeddingID
}

func deleteCodeEmbeddingRow(t *testing.T, ctx context.Context, db *sql.DB, embeddingID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `DELETE FROM code_embedding WHERE id = $1`, embeddingID); err != nil {
		t.Fatalf("DELETE code_embedding id=%s: %v", embeddingID, err)
	}
}

func chunkIDFor(t *testing.T, ctx context.Context, db *sql.DB, embeddingID string) string {
	t.Helper()
	var chunkID string
	if err := db.QueryRowContext(ctx, `SELECT chunk_id::text FROM code_embedding WHERE id = $1`, embeddingID).Scan(&chunkID); err != nil {
		t.Fatalf("SELECT chunk_id embedding_id=%s: %v", embeddingID, err)
	}
	return chunkID
}

func countOutboxRowsFor(t *testing.T, ctx context.Context, db *sql.DB, aggregateID string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND aggregate_type = 'CodeEmbedding'`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("count(outbox) aggregate_id=%s: %v", aggregateID, err)
	}
	return n
}

func readOutboxPayload(t *testing.T, ctx context.Context, db *sql.DB, aggregateID string) []byte {
	t.Helper()
	var payload []byte
	if err := db.QueryRowContext(ctx,
		`SELECT payload FROM outbox WHERE aggregate_id = $1 AND aggregate_type = 'CodeEmbedding' ORDER BY created_at DESC LIMIT 1`,
		aggregateID,
	).Scan(&payload); err != nil {
		t.Fatalf("SELECT payload outbox aggregate_id=%s: %v", aggregateID, err)
	}
	return payload
}

// ============================================================
// Helper Qdrant.
// ============================================================

// createQdrantPoint scrive un punto Qdrant all'id DERIVATO (via
// derivePointIDForTest, stessa formula di qdranttarget/sink-vector) da
// embeddingID, con payload {node_id: nodeID, domain: "code"} — simula ciò
// che sink-vector avrebbe scritto per quella riga.
func createQdrantPoint(t *testing.T, ctx context.Context, client *qdrant.Client, embeddingID, nodeID string) {
	t.Helper()
	upsertQdrantPointAtID(t, ctx, client, derivePointIDForTest(embeddingID), nodeID)
}

func upsertQdrantPointAtID(t *testing.T, ctx context.Context, client *qdrant.Client, pointID, nodeID string) {
	t.Helper()
	payload, err := qdrant.TryValueMap(map[string]any{
		"node_id": nodeID,
		"domain":  "code",
	})
	if err != nil {
		t.Fatalf("costruzione payload Qdrant: %v", err)
	}
	_, err = client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collectionName,
		Points: []*qdrant.PointStruct{
			{
				Id:      qdrant.NewID(pointID),
				Vectors: qdrant.NewVectors(fullVector(1)...),
				Payload: payload,
			},
		},
	})
	if err != nil {
		t.Fatalf("Upsert punto Qdrant id=%s: %v", pointID, err)
	}
}

func readQdrantNodeID(t *testing.T, ctx context.Context, client *qdrant.Client, embeddingID string) string {
	t.Helper()
	pointID := derivePointIDForTest(embeddingID)
	points, err := client.Get(ctx, &qdrant.GetPoints{
		CollectionName: collectionName,
		Ids:            []*qdrant.PointId{qdrant.NewID(pointID)},
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		t.Fatalf("Get punto id=%s: %v", pointID, err)
	}
	if len(points) != 1 {
		t.Fatalf("punti trovati per id=%s: %d, want 1", pointID, len(points))
	}
	return points[0].GetPayload()["node_id"].GetStringValue()
}

// ============================================================
// Utility.
// ============================================================

// smallVector produce un vettore piccolo (4 dim) per le righe
// code_embedding di Postgres negli scenari che non toccano mai Qdrant
// direttamente con quel vettore (1/2/4/5/6, edge case) — Qdrant stesso
// richiede sempre vectorSize dim esatte per un upsert reale (fullVector,
// sotto), ma nessuno di questi scenari fa upsert del VETTORE letto da
// Postgres: solo Republish lo rilegge e lo ributta nel payload outbox
// (mai scritto su Qdrant da qdranttarget stesso, §2/§5 non-goals).
func smallVector(seed int) []float32 {
	v := make([]float32, 4)
	for i := range v {
		v[i] = float32(seed) + float32(i)/4
	}
	return v
}

// fullVector produce un vettore di vectorSize dim, richiesto da Qdrant per
// qualunque upsert reale in una collection code_embeddings (Size=1536) —
// usato per i punti Qdrant di prova (createQdrantPoint/
// upsertQdrantPointAtID) e, nel file e2e (scenario 3), anche come vettore
// REALE code_embedding.vector che sink-vector legge e scrive su Qdrant. Lo
// +0.001 evita che una componente arrivi a un valore intero esatto (es.
// seed+0/vectorSize = seed.0): un componente intero-esatto serializza via
// encoding/json senza punto decimale (es. "3" invece di "3.001") e,
// scoperto scrivendo lo scenario 3, fa fallire silenziosamente
// table.expand.json.payload del connector Debezium (schema JSON misto
// int/float nell'array "vector" → il payload arriva a sink-vector come
// stringa JSON grezza non espansa invece che come oggetto, mai loggato
// come errore) — vedi commento dettagliato nello scenario 3
// (qdranttarget_e2e_integration_test.go).
func fullVector(seed int) []float32 {
	v := make([]float32, vectorSize)
	for i := range v {
		v[i] = float32(seed) + 0.001 + float32(i)/float32(vectorSize)
	}
	return v
}

func hash64(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// tools/reconcile/internal/qdranttarget/qdranttarget_integration_test.go
	// -> repo root è 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
