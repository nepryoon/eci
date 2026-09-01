//go:build integration

// SPEC-038 §3 scenari 1/2/4/5, §4 edge case — verifica con Postgres+Neo4j
// REALI (testcontainers, migration reali applicate col CLI `migrate`,
// stesso pattern di tools/reconcile/internal/framework/framework_integration_test.go
// e services/sink-graph/internal/consumer/consumer_integration_test.go) che
// il Target Neo4j confronti davvero code_node.ast_hash con la proprietà
// Neo4j corrispondente e ripubblichi un payload strutturalmente identico a
// quello di persist_parsed_file (T1.2). Lo scenario 3 (ciclo CDC completo
// via Debezium/Kafka/sink-graph reali) è in neo4jtarget_e2e_integration_test.go
// — stack ancora più pesante, file separato (SPEC-038 §7).
package neo4jtarget_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/neo4jtarget"
)

const (
	dbUser         = "eci"
	dbPassword     = "eci-test-password-1234"
	dbName         = "eci"
	neo4jPassword  = "eci-test-password-1234"
	insertCodeNode = `INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
		VALUES ($1, 'code', $2, $3, $4, $5::jsonb)`
)

// ============================================================
// Harness: Postgres + Neo4j reali, condivisi da tutti gli scenari di
// questo file (un solo avvio, stesso spirito di consumer_integration_test.go).
// ============================================================

type stack struct {
	db     *sql.DB
	driver neo4j.DriverWithContext
}

func TestNeo4jTarget(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	t.Run("Scenario1_MatchingAstHashNoRepublish", func(t *testing.T) {
		scenario1MatchingAstHashNoRepublish(t, ctx, st)
	})
	t.Run("Scenario2_MissingNodeRepublishes", func(t *testing.T) {
		scenario2MissingNodeRepublishes(t, ctx, st)
	})
	t.Run("Scenario4_DivergentAstHashRepublishes", func(t *testing.T) {
		scenario4DivergentAstHashRepublishes(t, ctx, st)
	})
	t.Run("Scenario5_RepublishedPayloadMatchesPersistParsedFileShape", func(t *testing.T) {
		scenario5RepublishedPayloadMatchesPersistParsedFileShape(t, ctx, st)
	})
	t.Run("EdgeCase_Neo4jUnreachableDuringCheckPropagatesError", func(t *testing.T) {
		edgeCaseNeo4jUnreachableDuringCheckPropagatesError(t, ctx, st)
	})
	t.Run("EdgeCase_RowDeletedBeforeRepublishReturnsExplicitError", func(t *testing.T) {
		edgeCaseRowDeletedBeforeRepublishReturnsExplicitError(t, ctx, st)
	})
}

// ============================================================
// Scenario 1 — Neo4j combacia, nessuna ripubblicazione.
// ============================================================

func scenario1MatchingAstHashNoRepublish(t *testing.T, ctx context.Context, st *stack) {
	id := uniqueID(t, "scenario1")
	astHash := hash64("scenario1")
	insertCodeNodeRow(t, ctx, st.db, id, "Function", "Foo", astHash, "a.go")
	createNeo4jNode(t, ctx, st.driver, id, astHash)

	target := neo4jtarget.New(st.driver, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if countOutboxRowsFor(t, ctx, st.db, id) != 0 {
		t.Fatalf("righe outbox per id=%s dopo Reconcile = >0, want 0 (nessuna ripubblicazione, ast_hash combaciante)", id)
	}
	// Checked/Matched includono anche righe di altri scenari nella stessa
	// tabella condivisa: qui verifichiamo solo che questa riga specifica NON
	// sia finita in Errored (asserzione diretta, non affidata al conteggio
	// aggregato del Report).
	for _, rowErr := range report.Errored {
		if rowErr.RowID == id {
			t.Fatalf("riga id=%s in errore: %v, want nessun errore (scenario 1, ast_hash combaciante)", id, rowErr.Err)
		}
	}
}

// ============================================================
// Scenario 2 — nodo Neo4j assente (evento perso simulato).
// ============================================================

func scenario2MissingNodeRepublishes(t *testing.T, ctx context.Context, st *stack) {
	id := uniqueID(t, "scenario2")
	insertCodeNodeRow(t, ctx, st.db, id, "Function", "Bar", hash64("scenario2"), "b.go")
	// Nessun nodo Neo4j creato: simula un evento perso.

	target := neo4jtarget.New(st.driver, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (id=%s mancante in Neo4j)", id)
	}
	if got := countOutboxRowsFor(t, ctx, st.db, id); got != 1 {
		t.Fatalf("righe outbox per id=%s = %d, want 1", id, got)
	}
}

// ============================================================
// Scenario 4 — nodo Neo4j presente ma con ast_hash DIVERGENTE (drift).
// Stesso comportamento di rilevamento+ripubblicazione dello scenario 2.
// ============================================================

func scenario4DivergentAstHashRepublishes(t *testing.T, ctx context.Context, st *stack) {
	id := uniqueID(t, "scenario4")
	neo4jAstHash := hash64("scenario4-neo4j-DIFFERENTE")
	insertCodeNodeRow(t, ctx, st.db, id, "Function", "Baz", hash64("scenario4-postgres"), "c.go")
	createNeo4jNode(t, ctx, st.driver, id, neo4jAstHash)

	target := neo4jtarget.New(st.driver, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (id=%s con ast_hash divergente)", id)
	}
	if got := countOutboxRowsFor(t, ctx, st.db, id); got != 1 {
		t.Fatalf("righe outbox per id=%s = %d, want 1", id, got)
	}
	// Verifica diretta: il nodo Neo4j esiste ancora con l'ast_hash vecchio
	// (nessuna scrittura Neo4j diretta da parte della riconciliazione stessa
	// — SOLO la riga outbox viene scritta, il MERGE reale è compito di
	// sink-graph via CDC, verificato separatamente nello scenario 3).
	gotAstHash := readNeo4jAstHash(t, ctx, st.driver, id)
	if gotAstHash != neo4jAstHash {
		t.Errorf("ast_hash Neo4j dopo Reconcile = %q, want invariato %q (Republish scrive SOLO outbox)", gotAstHash, neo4jAstHash)
	}
}

// ============================================================
// Scenario 5 — il payload ripubblicato è strutturalmente equivalente a
// quello che persist_parsed_file (T1.2) avrebbe scritto per la STESSA riga.
// ============================================================

func scenario5RepublishedPayloadMatchesPersistParsedFileShape(t *testing.T, ctx context.Context, st *stack) {
	id := uniqueID(t, "scenario5")
	scenario5AstHash := hash64("scenario5")
	insertCodeNodeRow(t, ctx, st.db, id, "Method", "Process", scenario5AstHash, "order_service.go")
	// Nodo assente in Neo4j -> ripubblicazione.

	target := neo4jtarget.New(st.driver, st.db)
	if _, err := framework.Reconcile(ctx, st.db, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	gotPayload := readOutboxPayload(t, ctx, st.db, id)

	// Forma ESATTA prodotta da persist_parsed_file per questa stessa riga
	// (services/ingestion/src/persist.rs): provenance includes the canonical
	// authenticated scope and path,
	// ext = {"node_type": ..., "language": "go"} — replicata qui letteralmente
	// (stessi campi, non un sottoinsieme), NON derivata dall'implementazione
	// sotto test.
	wantPayload := map[string]any{
		"id":       id,
		"domain":   "code",
		"name":     "Process",
		"ast_hash": scenario5AstHash,
		"provenance": map[string]any{
			"tenant_id": "tenant-reconcile",
			"repo":      "repo-reconcile",
			"acl_group": "developers",
			"path":      "order_service.go",
		},
		"ext": map[string]any{
			"node_type": "Method",
			"language":  "go",
		},
	}

	var gotDecoded map[string]any
	if err := json.Unmarshal(gotPayload, &gotDecoded); err != nil {
		t.Fatalf("payload outbox non è JSON valido: %v (raw: %s)", err, gotPayload)
	}
	// wantPayload passa per lo stesso round-trip JSON di gotDecoded (marshal
	// poi unmarshal) per normalizzare i tipi Go prima del confronto
	// strutturale (SPEC-038 §3 scenario 5: "strutturalmente equivalenti").
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
// Edge case §4 — Neo4j irraggiungibile durante Check: errore propagato,
// mai un matches=false silenzioso.
// ============================================================

func edgeCaseNeo4jUnreachableDuringCheckPropagatesError(t *testing.T, ctx context.Context, st *stack) {
	id := uniqueID(t, "edgecase-unreachable")
	insertCodeNodeRow(t, ctx, st.db, id, "Function", "Unreachable", hash64("unreachable"), "d.go")

	// Driver Neo4j puntato su un indirizzo irraggiungibile — NewDriverWithContext
	// non apre connessioni finché non serve, l'errore emerge alla prima query
	// reale (stesso principio di consumer_integration_test.go: DSN
	// irraggiungibile -> fallimento deterministico al primo uso).
	badDriver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:1", neo4j.BasicAuth("neo4j", "wrong", ""))
	if err != nil {
		t.Fatalf("NewDriverWithContext (uri irraggiungibile): %v", err)
	}
	defer badDriver.Close(ctx)

	target := neo4jtarget.New(badDriver, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v (SourceRows non deve fallire, solo Check)", err)
	}

	var found bool
	for _, rowErr := range report.Errored {
		if rowErr.RowID == id {
			found = true
			if rowErr.Err == nil {
				t.Fatalf("riga id=%s in Errored ma Err=nil", id)
			}
		}
	}
	if !found {
		t.Fatalf("riga id=%s non trovata in report.Errored = %+v, want un errore di Check propagato (Neo4j irraggiungibile), non un matches=false silenzioso", id, report.Errored)
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
	id := uniqueID(t, "edgecase-deleted")
	deletedAstHash := hash64("deleted")
	insertCodeNodeRow(t, ctx, st.db, id, "Function", "Deleted", deletedAstHash, "e.go")
	row := framework.SourceRow{ID: id, Fingerprint: []byte(deletedAstHash)}

	deleteCodeNodeRow(t, ctx, st.db, id)

	target := neo4jtarget.New(st.driver, st.db)
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = target.Republish(ctx, tx, row)
	if err == nil {
		t.Fatalf("Republish(id=%s, cancellato da Postgres) = nil error, want un errore esplicito", id)
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
	driver := startNeo4j(t, ctx)
	return &stack{db: db, driver: driver}
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

func startNeo4j(t *testing.T, ctx context.Context) neo4j.DriverWithContext {
	t.Helper()
	container, err := tcneo4j.Run(ctx, "neo4j:5-community", tcneo4j.WithAdminPassword(neo4jPassword))
	if err != nil {
		t.Fatalf("avvio container neo4j:5-community: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container neo4j: %v", err)
		}
	})
	boltURL, err := container.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("BoltUrl: %v", err)
	}
	driver, err := neo4j.NewDriverWithContext(boltURL, neo4j.BasicAuth("neo4j", neo4jPassword, ""))
	if err != nil {
		t.Fatalf("driver neo4j: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(ctx) })
	return driver
}

// ============================================================
// Helper Postgres.
// ============================================================

func insertCodeNodeRow(t *testing.T, ctx context.Context, db *sql.DB, id, nodeType, name, astHash, path string) {
	t.Helper()
	provenance, err := json.Marshal(map[string]any{
		"tenant_id": "tenant-reconcile",
		"repo":      "repo-reconcile",
		"acl_group": "developers",
		"path":      path,
	})
	if err != nil {
		t.Fatalf("marshal provenance: %v", err)
	}
	if _, err := db.ExecContext(ctx, insertCodeNode, id, nodeType, name, astHash, provenance); err != nil {
		t.Fatalf("INSERT code_node id=%s: %v", id, err)
	}
}

func deleteCodeNodeRow(t *testing.T, ctx context.Context, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `DELETE FROM code_node WHERE id = $1`, id); err != nil {
		t.Fatalf("DELETE code_node id=%s: %v", id, err)
	}
}

func countOutboxRowsFor(t *testing.T, ctx context.Context, db *sql.DB, aggregateID string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, aggregateID).Scan(&n); err != nil {
		t.Fatalf("count(outbox) aggregate_id=%s: %v", aggregateID, err)
	}
	return n
}

func readOutboxPayload(t *testing.T, ctx context.Context, db *sql.DB, aggregateID string) []byte {
	t.Helper()
	var payload []byte
	if err := db.QueryRowContext(ctx,
		`SELECT payload FROM outbox WHERE aggregate_id = $1 AND aggregate_type = 'CodeNode' ORDER BY created_at DESC LIMIT 1`,
		aggregateID,
	).Scan(&payload); err != nil {
		t.Fatalf("SELECT payload outbox aggregate_id=%s: %v", aggregateID, err)
	}
	return payload
}

// ============================================================
// Helper Neo4j.
// ============================================================

func createNeo4jNode(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id, astHash string) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, `MERGE (n {id: $id}) SET n.ast_hash = $ast_hash`, map[string]any{
		"id": id, "ast_hash": astHash,
	})
	if err != nil {
		t.Fatalf("creazione nodo Neo4j id=%s: %v", id, err)
	}
	if _, err := result.Consume(ctx); err != nil {
		t.Fatalf("consume creazione nodo Neo4j id=%s: %v", id, err)
	}
}

func readNeo4jAstHash(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id string) string {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, `MATCH (n {id: $id}) RETURN n.ast_hash AS ast_hash`, map[string]any{"id": id})
	if err != nil {
		t.Fatalf("lettura ast_hash Neo4j id=%s: %v", id, err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("lettura ast_hash Neo4j id=%s (single): %v", id, err)
	}
	astHash, _, err := neo4j.GetRecordValue[string](record, "ast_hash")
	if err != nil {
		t.Fatalf("campo ast_hash id=%s: %v", id, err)
	}
	return astHash
}

// ============================================================
// Utility.
// ============================================================

// hash64 produce una stringa esadecimale di 64 caratteri, stessa lunghezza
// esatta della colonna code_node.ast_hash (CHAR(64), non VARCHAR — SPEC-005):
// un valore di test più corto verrebbe restituito PADDATO con spazi da
// Postgres alla rilettura, un dettaglio invisibile in produzione (ast_hash
// reale è sempre un digest SHA-256 completo, persist.rs) ma che romperebbe
// silenziosamente il confronto byte-per-byte di Check/scenario 5 se i test
// usassero stringhe più corte — scoperto scrivendo questo stesso test.
func hash64(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

var idCounter int

func uniqueID(t *testing.T, prefix string) string {
	t.Helper()
	idCounter++
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), idCounter)
}

func repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// tools/reconcile/internal/neo4jtarget/neo4jtarget_integration_test.go
	// -> repo root è 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
