//go:build integration

// SPEC-040 §3 scenari 1/2/4/5, §4 edge case — verifica con Postgres+
// OpenSearch REALI (testcontainers, migration reali applicate col CLI
// `migrate`, stesso pattern di
// tools/reconcile/internal/qdranttarget/qdranttarget_integration_test.go)
// che il Target OpenSearch confronti davvero code_chunk.text con il campo
// text del documento OpenSearch corrispondente e ripubblichi un payload
// strutturalmente identico a quello di persist_parsed_file (T1.2). Lo
// scenario 3 (ciclo CDC completo via Debezium/Kafka/sink-search reali) è
// in opensearchtarget_e2e_integration_test.go — stack ancora più pesante,
// file separato (SPEC-040 §7).
package opensearchtarget_test

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

	_ "github.com/lib/pq"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	tcopensearch "github.com/testcontainers/testcontainers-go/modules/opensearch"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/opensearchtarget"
)

const (
	dbUser     = "eci"
	dbPassword = "eci-test-password-1234"
	dbName     = "eci"

	insertCodeNode = `INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
		VALUES ($1, 'code', 'Function', $1, $2, $3::jsonb)`
	insertCodeChunk = `INSERT INTO code_chunk (domain, entity_id, chunk_index, text, char_count)
		VALUES ('code', $1, $2, $3, $4) RETURNING id::text`

	// indexName — STESSO letterale di
	// services/sink-search/internal/consumer.IndexName (non importabile,
	// vedi opensearchtarget.go), replicato qui nel test.
	indexName = "code_chunks"
)

// ============================================================
// Harness: Postgres + OpenSearch reali, condivisi da tutti gli scenari di
// questo file (un solo avvio, stesso spirito di
// qdranttarget_integration_test.go).
// ============================================================

type stack struct {
	db         *sql.DB
	opensearch *opensearchapi.Client
}

func TestOpenSearchTarget(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	t.Run("Scenario1_MatchingTextNoRepublish", func(t *testing.T) {
		scenario1MatchingTextNoRepublish(t, ctx, st)
	})
	t.Run("Scenario2_MissingDocumentRepublishes", func(t *testing.T) {
		scenario2MissingDocumentRepublishes(t, ctx, st)
	})
	t.Run("Scenario4_DivergentTextRepublishes", func(t *testing.T) {
		scenario4DivergentTextRepublishes(t, ctx, st)
	})
	t.Run("Scenario5_RepublishedPayloadMatchesPersistParsedFileShape", func(t *testing.T) {
		scenario5RepublishedPayloadMatchesPersistParsedFileShape(t, ctx, st)
	})
	t.Run("EdgeCase_OpenSearchUnreachableDuringCheckPropagatesError", func(t *testing.T) {
		edgeCaseOpenSearchUnreachableDuringCheckPropagatesError(t, ctx, st)
	})
	t.Run("EdgeCase_RowDeletedBeforeRepublishReturnsExplicitError", func(t *testing.T) {
		edgeCaseRowDeletedBeforeRepublishReturnsExplicitError(t, ctx, st)
	})
	t.Run("Review_LegacyDocumentWithMatchingTextStillRepublishes", func(t *testing.T) {
		reviewLegacyDocumentWithMatchingTextStillRepublishes(t, ctx, st)
	})
}

// ============================================================
// Scenario 1 — documento OpenSearch esiste con text combaciante.
// ============================================================

func scenario1MatchingTextNoRepublish(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario1-entity")
	text := "func Scenario1() {}"
	chunkID := insertFullRow(t, ctx, st.db, entityID, text)
	createOpenSearchDocument(t, ctx, st.opensearch, chunkID, text, entityID)

	target := opensearchtarget.New(st.opensearch, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if countOutboxRowsFor(t, ctx, st.db, chunkID) != 0 {
		t.Fatalf("righe outbox per chunk_id=%s dopo Reconcile = >0, want 0 (nessuna ripubblicazione, text combaciante)", chunkID)
	}
	for _, rowErr := range report.Errored {
		if rowErr.RowID == chunkID {
			t.Fatalf("riga chunk_id=%s in errore: %v, want nessun errore (scenario 1, text combaciante)", chunkID, rowErr.Err)
		}
	}
}

// ============================================================
// Scenario 2 — documento OpenSearch del tutto assente (evento perso).
// ============================================================

func scenario2MissingDocumentRepublishes(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario2-entity")
	text := "func Scenario2() {}"
	chunkID := insertFullRow(t, ctx, st.db, entityID, text)
	// Nessun documento OpenSearch creato: simula un evento CodeChunk perso.

	target := opensearchtarget.New(st.opensearch, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (chunk_id=%s mancante in OpenSearch)", chunkID)
	}
	if got := countOutboxRowsFor(t, ctx, st.db, chunkID); got != 1 {
		t.Fatalf("righe outbox per chunk_id=%s = %d, want 1", chunkID, got)
	}
}

// ============================================================
// Scenario 4 — documento OpenSearch presente ma con text DIVERGENTE.
// Stesso comportamento di rilevamento+ripubblicazione dello scenario 2.
// ============================================================

func scenario4DivergentTextRepublishes(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario4-entity")
	postgresText := "func Scenario4Postgres() {}"
	openSearchText := "func Scenario4OpenSearchDIVERGENTE() {}"
	chunkID := insertFullRow(t, ctx, st.db, entityID, postgresText)
	createOpenSearchDocument(t, ctx, st.opensearch, chunkID, openSearchText, entityID)

	target := opensearchtarget.New(st.opensearch, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (chunk_id=%s con text divergente)", chunkID)
	}
	if got := countOutboxRowsFor(t, ctx, st.db, chunkID); got != 1 {
		t.Fatalf("righe outbox per chunk_id=%s = %d, want 1", chunkID, got)
	}
	// Verifica diretta: il documento OpenSearch esiste ancora col text
	// vecchio (nessuna scrittura OpenSearch diretta da parte della
	// riconciliazione — SOLO la riga outbox viene scritta, l'upsert reale
	// è compito di sink-search via CDC, verificato separatamente nello
	// scenario 3).
	gotText := readOpenSearchText(t, ctx, st.opensearch, chunkID)
	if gotText != openSearchText {
		t.Errorf("text OpenSearch dopo Reconcile = %q, want invariato %q (Republish scrive SOLO outbox)", gotText, openSearchText)
	}
}

// ============================================================
// Scenario 5 — il payload ripubblicato è strutturalmente equivalente a
// quello che persist_parsed_file (T1.2) avrebbe scritto per la STESSA
// riga.
// ============================================================

func scenario5RepublishedPayloadMatchesPersistParsedFileShape(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("scenario5-entity")
	text := "func Scenario5() { return }"
	chunkID := insertFullRow(t, ctx, st.db, entityID, text)
	// Nessun documento OpenSearch creato -> ripubblicazione.

	target := opensearchtarget.New(st.opensearch, st.db)
	if _, err := framework.Reconcile(ctx, st.db, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	gotPayload := readOutboxPayload(t, ctx, st.db, chunkID)

	// Forma ESATTA prodotta da persist_parsed_file
	// (services/ingestion/src/persist.rs, funzione che inserisce
	// code_chunk+outbox) per questa stessa riga: {id, entity_id,
	// chunk_index, text, char_count, provenance with authenticated scope} —
	// replicata qui letteralmente, NON derivata dall'implementazione sotto
	// test.
	wantPayload := map[string]any{
		"id":          chunkID,
		"entity_id":   entityID,
		"chunk_index": 0,
		"text":        text,
		"char_count":  len(text),
		"provenance": map[string]any{
			"tenant_id": "tenant-reconcile",
			"repo":      "repo-reconcile",
			"acl_group": "developers",
			"path":      "default.go",
		},
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
// Edge case §4 — OpenSearch irraggiungibile durante Check: errore
// propagato, mai un matches=false silenzioso.
// ============================================================

func edgeCaseOpenSearchUnreachableDuringCheckPropagatesError(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("unreachable-entity")
	text := "func Unreachable() {}"
	chunkID := insertFullRow(t, ctx, st.db, entityID, text)

	// Client OpenSearch puntato su un indirizzo irraggiungibile — stesso
	// principio già usato da
	// services/sink-search/internal/consumer/consumer_integration_test.go
	// (edgeCaseOpenSearchUnreachableAtStartupReturnsErr).
	badClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{"http://127.0.0.1:1"}},
	})
	if err != nil {
		t.Fatalf("opensearchapi.NewClient (indirizzo irraggiungibile): %v", err)
	}

	target := opensearchtarget.New(badClient, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v (SourceRows non deve fallire, solo Check)", err)
	}

	var found bool
	for _, rowErr := range report.Errored {
		if rowErr.RowID == chunkID {
			found = true
			if rowErr.Err == nil {
				t.Fatalf("riga chunk_id=%s in Errored ma Err=nil", chunkID)
			}
		}
	}
	if !found {
		t.Fatalf("riga chunk_id=%s non trovata in report.Errored = %+v, want un errore di Check propagato (OpenSearch irraggiungibile), non un matches=false silenzioso", chunkID, report.Errored)
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
	text := "func Deleted() {}"
	chunkID := insertFullRow(t, ctx, st.db, entityID, text)
	row := framework.SourceRow{ID: chunkID, Fingerprint: []byte(text)}

	deleteCodeChunkRow(t, ctx, st.db, chunkID)

	target := opensearchtarget.New(st.opensearch, st.db)
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = target.Republish(ctx, tx, row)
	if err == nil {
		t.Fatalf("Republish(chunk_id=%s, cancellato da Postgres) = nil error, want un errore esplicito", chunkID)
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
	osClient := startOpenSearch(t, ctx)
	return &stack{db: db, opensearch: osClient}
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

// startOpenSearch avvia OpenSearch reale e crea l'indice code_chunks —
// STESSO mapping di services/sink-search/internal/consumer.EnsureIndex
// (SPEC-034 §2), replicato qui perché quel codice non è importabile (vedi
// opensearchtarget.go) e in produzione è sink-search stesso, non
// opensearchtarget, a crearlo al proprio avvio (SPEC-040 è solo lettore/
// verificatore di questo indice, mai il suo creatore).
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
		t.Fatalf("opensearchapi.NewClient: %v", err)
	}

	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"text":        map[string]any{"type": "text"},
				"entity_id":   map[string]any{"type": "keyword"},
				"chunk_index": map[string]any{"type": "integer"},
			},
		},
	}
	if _, err := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: indexName,
		Body:  opensearchutil.NewJSONReader(mapping),
	}); err != nil {
		t.Fatalf("Indices.Create(%s): %v", indexName, err)
	}

	return client
}

// ============================================================
// Helper Postgres.
// ============================================================

// insertFullRow inserisce code_node+code_chunk (JOIN completo richiesto da
// opensearchtarget.SourceRows/Republish, §2) e ritorna l'id generato di
// code_chunk (il vero row.ID riconciliato). entityID è usato come
// code_node.id (e come code_chunk.entity_id, stesso valore — schema:
// code_chunk.entity_id REFERENCES code_node(id)); provenance di default
// con scope autenticato e path, coerente con lo scenario 5.
func insertFullRow(t *testing.T, ctx context.Context, db *sql.DB, entityID, text string) (chunkID string) {
	t.Helper()
	provenance := []byte(`{"tenant_id":"tenant-reconcile","repo":"repo-reconcile","acl_group":"developers","path":"default.go"}`)
	if _, err := db.ExecContext(ctx, insertCodeNode, entityID, hash64(entityID), provenance); err != nil {
		t.Fatalf("INSERT code_node id=%s: %v", entityID, err)
	}

	err := db.QueryRowContext(ctx, insertCodeChunk, entityID, 0, text, len(text)).Scan(&chunkID)
	if err != nil {
		t.Fatalf("INSERT code_chunk entity_id=%s: %v", entityID, err)
	}
	return chunkID
}

func deleteCodeChunkRow(t *testing.T, ctx context.Context, db *sql.DB, chunkID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `DELETE FROM code_chunk WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("DELETE code_chunk id=%s: %v", chunkID, err)
	}
}

func countOutboxRowsFor(t *testing.T, ctx context.Context, db *sql.DB, aggregateID string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND aggregate_type = 'CodeChunk'`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("count(outbox) aggregate_id=%s: %v", aggregateID, err)
	}
	return n
}

func readOutboxPayload(t *testing.T, ctx context.Context, db *sql.DB, aggregateID string) []byte {
	t.Helper()
	var payload []byte
	if err := db.QueryRowContext(ctx,
		`SELECT payload FROM outbox WHERE aggregate_id = $1 AND aggregate_type = 'CodeChunk' ORDER BY created_at DESC LIMIT 1`,
		aggregateID,
	).Scan(&payload); err != nil {
		t.Fatalf("SELECT payload outbox aggregate_id=%s: %v", aggregateID, err)
	}
	return payload
}

// ============================================================
// Helper OpenSearch.
// ============================================================

func createOpenSearchDocument(t *testing.T, ctx context.Context, client *opensearchapi.Client, chunkID, text, entityID string) {
	t.Helper()
	body := map[string]any{
		"chunk_id": chunkID, "event_sequence": 1,
		"text": text, "entity_id": entityID, "chunk_index": 0,
	}
	if _, err := client.Index(ctx, opensearchapi.IndexReq{
		Index:      indexName,
		DocumentID: chunkID,
		Body:       opensearchutil.NewJSONReader(body),
	}); err != nil {
		t.Fatalf("Index documento id=%s: %v", chunkID, err)
	}
	refreshIndex(t, ctx, client)
}

func reviewLegacyDocumentWithMatchingTextStillRepublishes(t *testing.T, ctx context.Context, st *stack) {
	entityID := hash64("review-legacy-cursor-entity")
	text := "func LegacyCursor() {}"
	chunkID := insertFullRow(t, ctx, st.db, entityID, text)
	body := map[string]any{"text": text, "entity_id": entityID, "chunk_index": 0}
	if _, err := st.opensearch.Index(ctx, opensearchapi.IndexReq{
		Index: indexName, DocumentID: chunkID, Body: opensearchutil.NewJSONReader(body),
	}); err != nil {
		t.Fatalf("Index legacy document: %v", err)
	}
	refreshIndex(t, ctx, st.opensearch)
	report, err := framework.Reconcile(ctx, st.db, opensearchtarget.New(st.opensearch, st.db))
	if err != nil {
		t.Fatal(err)
	}
	if report.Republished == 0 || countOutboxRowsFor(t, ctx, st.db, chunkID) == 0 {
		t.Fatalf("legacy document with matching text was incorrectly accepted: %+v", report)
	}
}

func refreshIndex(t *testing.T, ctx context.Context, client *opensearchapi.Client) {
	t.Helper()
	if _, err := client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{
		Index: []string{indexName},
	}); err != nil {
		t.Fatalf("Indices.Refresh(%s): %v", indexName, err)
	}
}

func readOpenSearchText(t *testing.T, ctx context.Context, client *opensearchapi.Client, chunkID string) string {
	t.Helper()
	resp, err := client.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      indexName,
		DocumentID: chunkID,
	})
	if err != nil {
		t.Fatalf("Document.Get id=%s: %v", chunkID, err)
	}
	if !resp.Found {
		t.Fatalf("documento id=%s non trovato", chunkID)
	}
	var source struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(resp.Source, &source); err != nil {
		t.Fatalf("decodifica _source id=%s: %v", chunkID, err)
	}
	return source.Text
}

// ============================================================
// Utility.
// ============================================================

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
	// tools/reconcile/internal/opensearchtarget/opensearchtarget_integration_test.go
	// -> repo root è 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
