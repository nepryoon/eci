//go:build integration

// SPEC-028 §7 — test di integrazione: `postgres:17` via testcontainers,
// migration reali applicate col CLI `migrate` (lo stesso invocato da
// `task db:migrate`), stesso pattern di `tests/integration/postgres_ddl`
// (SPEC-005) e `tools/migrate-neo4j` — righe con timestamp NOTI inserite
// esplicitamente (non simulate), per verificare i conteggi con
// precisione (§7: "non approssimativamente").
package gc

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	integrationDBUser     = "eci"
	integrationDBPassword = "eci-test-password-1234"
	integrationDBName     = "eci"
)

func repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// tools/gc-postgres/internal/gc/runner_integration_test.go -> repo root
	// is 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}

func startMigratedPostgres(t *testing.T) (*sql.DB, string) {
	t.Helper()
	ctx := context.Background()

	if _, err := exec.LookPath("migrate"); err != nil {
		t.Fatalf("binario 'migrate' non trovato sul PATH: %v (go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest, poi verifica che $(go env GOPATH)/bin sia sul PATH)", err)
	}

	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithUsername(integrationDBUser),
		tcpostgres.WithPassword(integrationDBPassword),
		tcpostgres.WithDatabase(integrationDBName),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("avvio container postgres:17: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}

	migrationsDir := repoPath(t, "contracts", "sql", "migrations")
	runMigrateCLI(t, migrationsDir, dsn, "up")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db, dsn
}

func runMigrateCLI(t *testing.T, migrationsDir, dsn string, args ...string) {
	t.Helper()
	cmdArgs := append([]string{"-source", "file://" + migrationsDir, "-database", dsn}, args...)
	cmd := exec.Command("migrate", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("migrate %v fallito: %v\noutput:\n%s", args, err, out)
	}
}

func uuidFor(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
}

func insertOutboxRow(t *testing.T, db *sql.DB, aggregateID string, createdAt time.Time) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, created_at)
		 VALUES ('CodeNode', $1, 'UPSERT', '{}'::jsonb, $2)`,
		aggregateID, createdAt,
	)
	if err != nil {
		t.Fatalf("insert outbox (%s, %s): %v", aggregateID, createdAt, err)
	}
}

func insertProcessedEventRow(t *testing.T, db *sql.DB, eventID string, processedAt time.Time) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO processed_events (event_id, consumer_name, processed_at) VALUES ($1, 'gc-test', $2)`,
		eventID, processedAt,
	)
	if err != nil {
		t.Fatalf("insert processed_events (%s, %s): %v", eventID, processedAt, err)
	}
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count(%s): %v", table, err)
	}
	return n
}

func noopLog(string, ...any) {}

// SPEC-028 §3 scenari 1/2: righe più vecchie della soglia eliminate,
// righe più recenti conservate — outbox e processed_events verificate
// nello stesso test, ciascuna con le proprie righe note.
func TestRunWithDBDeletesOnlyRowsOlderThanRetention(t *testing.T) {
	db, _ := startMigratedPostgres(t)
	now := time.Now().UTC()

	// outbox: 2 righe vecchie (10 giorni), 2 recenti (1 ora).
	insertOutboxRow(t, db, "old-1", now.Add(-10*24*time.Hour))
	insertOutboxRow(t, db, "old-2", now.Add(-10*24*time.Hour))
	insertOutboxRow(t, db, "recent-1", now.Add(-1*time.Hour))
	insertOutboxRow(t, db, "recent-2", now.Add(-1*time.Hour))

	// processed_events: 3 righe vecchie, 1 recente.
	insertProcessedEventRow(t, db, uuidFor(1), now.Add(-10*24*time.Hour))
	insertProcessedEventRow(t, db, uuidFor(2), now.Add(-10*24*time.Hour))
	insertProcessedEventRow(t, db, uuidFor(3), now.Add(-10*24*time.Hour))
	insertProcessedEventRow(t, db, uuidFor(4), now.Add(-1*time.Hour))

	cfg := Config{
		OutboxRetention:          168 * time.Hour,
		ProcessedEventsRetention: 168 * time.Hour,
	}
	summary := RunWithDB(context.Background(), db, cfg, noopLog)

	if summary.Failed() {
		t.Fatalf("summary inatteso in errore: outbox=%v processed_events=%v", summary.Outbox.Err, summary.ProcessedEvents.Err)
	}
	if summary.Outbox.RowsAffected != 2 {
		t.Errorf("outbox.RowsAffected = %d, want 2", summary.Outbox.RowsAffected)
	}
	if summary.ProcessedEvents.RowsAffected != 3 {
		t.Errorf("processed_events.RowsAffected = %d, want 3", summary.ProcessedEvents.RowsAffected)
	}
	if got := countRows(t, db, "outbox"); got != 2 {
		t.Errorf("outbox rimaste = %d, want 2 (le 2 recenti)", got)
	}
	if got := countRows(t, db, "processed_events"); got != 1 {
		t.Errorf("processed_events rimaste = %d, want 1 (la recente)", got)
	}
}

// SPEC-028 §3 scenario 3: --dry-run conta correttamente ma non elimina.
func TestRunWithDBDryRunCountsWithoutDeleting(t *testing.T) {
	db, _ := startMigratedPostgres(t)
	now := time.Now().UTC()

	insertOutboxRow(t, db, "old-1", now.Add(-10*24*time.Hour))
	insertOutboxRow(t, db, "recent-1", now.Add(-1*time.Hour))
	insertProcessedEventRow(t, db, uuidFor(1), now.Add(-10*24*time.Hour))
	insertProcessedEventRow(t, db, uuidFor(2), now.Add(-1*time.Hour))

	cfg := Config{
		OutboxRetention:          168 * time.Hour,
		ProcessedEventsRetention: 168 * time.Hour,
		DryRun:                   true,
	}
	summary := RunWithDB(context.Background(), db, cfg, noopLog)

	if summary.Failed() {
		t.Fatalf("summary inatteso in errore: outbox=%v processed_events=%v", summary.Outbox.Err, summary.ProcessedEvents.Err)
	}
	if summary.Outbox.RowsAffected != 1 {
		t.Errorf("dry-run outbox count = %d, want 1", summary.Outbox.RowsAffected)
	}
	if summary.ProcessedEvents.RowsAffected != 1 {
		t.Errorf("dry-run processed_events count = %d, want 1", summary.ProcessedEvents.RowsAffected)
	}
	if got := countRows(t, db, "outbox"); got != 2 {
		t.Errorf("--dry-run non deve eliminare: outbox rimaste = %d, want 2 (tutte)", got)
	}
	if got := countRows(t, db, "processed_events"); got != 2 {
		t.Errorf("--dry-run non deve eliminare: processed_events rimaste = %d, want 2 (tutte)", got)
	}
}

// SPEC-028 §4 edge case: tabella vuota -> nessun errore, zero righe.
func TestRunWithDBEmptyTablesNoError(t *testing.T) {
	db, _ := startMigratedPostgres(t)

	cfg := Config{
		OutboxRetention:          168 * time.Hour,
		ProcessedEventsRetention: 168 * time.Hour,
	}
	summary := RunWithDB(context.Background(), db, cfg, noopLog)

	if summary.Failed() {
		t.Fatalf("summary inatteso in errore su tabelle vuote: outbox=%v processed_events=%v", summary.Outbox.Err, summary.ProcessedEvents.Err)
	}
	if summary.Outbox.RowsAffected != 0 {
		t.Errorf("outbox.RowsAffected = %d, want 0", summary.Outbox.RowsAffected)
	}
	if summary.ProcessedEvents.RowsAffected != 0 {
		t.Errorf("processed_events.RowsAffected = %d, want 0", summary.ProcessedEvents.RowsAffected)
	}
}

// SPEC-028 §3 scenario 5: soglie diverse per le due tabelle, verifica
// diretta che non si confondano — una riga vecchia di 2 giorni deve
// sopravvivere alla soglia lunga di outbox (720h) ma essere eliminata
// dalla soglia corta di processed_events (24h).
func TestRunWithDBDifferentRetentionPerTableDoNotCrossContaminate(t *testing.T) {
	db, _ := startMigratedPostgres(t)
	now := time.Now().UTC()
	twoDaysAgo := now.Add(-48 * time.Hour)

	insertOutboxRow(t, db, "two-days-old", twoDaysAgo)
	insertProcessedEventRow(t, db, uuidFor(1), twoDaysAgo)

	cfg := Config{
		OutboxRetention:          720 * time.Hour, // 30 giorni: la riga NON è vecchia abbastanza
		ProcessedEventsRetention: 24 * time.Hour,  // 1 giorno: la riga È vecchia abbastanza
	}
	summary := RunWithDB(context.Background(), db, cfg, noopLog)

	if summary.Failed() {
		t.Fatalf("summary inatteso in errore: outbox=%v processed_events=%v", summary.Outbox.Err, summary.ProcessedEvents.Err)
	}
	if summary.Outbox.RowsAffected != 0 {
		t.Errorf("outbox.RowsAffected = %d, want 0 (soglia 720h, riga di 2 giorni deve sopravvivere)", summary.Outbox.RowsAffected)
	}
	if summary.ProcessedEvents.RowsAffected != 1 {
		t.Errorf("processed_events.RowsAffected = %d, want 1 (soglia 24h, riga di 2 giorni deve essere eliminata)", summary.ProcessedEvents.RowsAffected)
	}
	if got := countRows(t, db, "outbox"); got != 1 {
		t.Errorf("outbox rimaste = %d, want 1", got)
	}
	if got := countRows(t, db, "processed_events"); got != 0 {
		t.Errorf("processed_events rimaste = %d, want 0", got)
	}
}

// SPEC-028 §4 edge case: il DELETE su outbox fallisce (lock/timeout) ma
// quello su processed_events ha comunque successo — le due transazioni
// sono indipendenti. Simulato con un lock ACCESS EXCLUSIVE reale tenuto
// da una connessione separata su `outbox` + un lock_timeout breve sulla
// transazione di gc-postgres (SPEC-028 §10, deviazione dichiarata: non
// richiesto esplicitamente da §2, aggiunto per rendere deterministico
// questo scenario invece di lasciare la DELETE bloccarsi indefinitamente
// su un lock reale).
func TestRunWithDBIndependentFailureDoesNotBlockTheOtherTable(t *testing.T) {
	db, dsn := startMigratedPostgres(t)
	now := time.Now().UTC()

	insertOutboxRow(t, db, "old-1", now.Add(-10*24*time.Hour))
	insertProcessedEventRow(t, db, uuidFor(1), now.Add(-10*24*time.Hour))

	lockConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open (connessione di lock): %v", err)
	}
	defer lockConn.Close()

	lockTx, err := lockConn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx lock: %v", err)
	}
	if _, err := lockTx.Exec("LOCK TABLE outbox IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("LOCK TABLE outbox: %v", err)
	}
	defer func() { _ = lockTx.Rollback() }()

	cfg := Config{
		OutboxRetention:          168 * time.Hour,
		ProcessedEventsRetention: 168 * time.Hour,
	}
	summary := RunWithDB(context.Background(), db, cfg, noopLog)

	if summary.Outbox.Err == nil {
		t.Error("summary.Outbox.Err = nil, atteso un errore di lock/timeout (outbox è locked da lockTx)")
	}
	if summary.ProcessedEvents.Err != nil {
		t.Errorf("summary.ProcessedEvents.Err = %v, atteso nil (processed_events non è locked, deve riuscire comunque)", summary.ProcessedEvents.Err)
	}
	if summary.ProcessedEvents.RowsAffected != 1 {
		t.Errorf("processed_events.RowsAffected = %d, want 1", summary.ProcessedEvents.RowsAffected)
	}
	if !summary.Failed() {
		t.Error("summary.Failed() = false, atteso true (almeno una tabella ha fallito)")
	}
}
