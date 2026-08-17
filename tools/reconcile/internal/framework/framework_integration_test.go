//go:build integration

// SPEC-037 §3 scenario 5 — verifica con Postgres REALE (testcontainers,
// migration reali applicate col CLI `migrate`, stesso pattern di
// tools/gc-postgres SPEC-028 §7 e tools/migrate-neo4j) che Republish
// scriva DAVVERO dentro una transazione: o la riga outbox è scritta per
// intero, o (in caso di fallimento di Republish DOPO un INSERT reale
// nella stessa tx) nulla resta scritto — nessuna scrittura parziale.
package framework

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

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
	// tools/reconcile/internal/framework/framework_integration_test.go ->
	// repo root è 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}

func startMigratedPostgres(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()

	if _, err := exec.LookPath("migrate"); err != nil {
		t.Fatalf("binario 'migrate' non trovato sul PATH: %v", err)
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
	cmd := exec.Command("migrate", "-source", "file://"+migrationsDir, "-database", dsn, "up")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("migrate up fallito: %v\noutput:\n%s", err, out)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func countOutboxRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM outbox").Scan(&n); err != nil {
		t.Fatalf("count(outbox): %v", err)
	}
	return n
}

// republishInsertOutbox scrive una riga outbox reale attraverso la tx
// passata da Reconcile — stessa forma minima già usata da
// tests/integration/outbox_cdc (SPEC-008/014), qui con aggregate_id
// derivato dalla SourceRow per poterla identificare dopo il commit.
func republishInsertOutbox(ctx context.Context, tx *sql.Tx, row SourceRow) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('CodeNode', $1, 'UPSERT', '{}'::jsonb)`,
		row.ID,
	)
	return err
}

// SPEC-037 §3 scenario 5, caso successo: Republish scrive, la
// transazione viene committata, la riga outbox esiste davvero.
func TestReconcileRepublishSuccessWritesOutboxRowAtomically(t *testing.T) {
	db := startMigratedPostgres(t)

	row := SourceRow{ID: "reconcile-scenario5-success", Fingerprint: []byte("fp")}
	target := Target{
		Name: "fake-pg",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return []SourceRow{row}, nil
		},
		Check:     func(ctx context.Context, r SourceRow) (bool, error) { return false, nil },
		Republish: republishInsertOutbox,
	}

	report, err := Reconcile(context.Background(), db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished != 1 || len(report.Errored) != 0 {
		t.Fatalf("report = %+v, want Republished=1 Errored=[]", report)
	}
	if got := countOutboxRows(t, db); got != 1 {
		t.Fatalf("righe in outbox = %d, want 1 (Republish deve aver committato)", got)
	}
	var aggregateID string
	if err := db.QueryRow("SELECT aggregate_id FROM outbox").Scan(&aggregateID); err != nil {
		t.Fatalf("SELECT aggregate_id: %v", err)
	}
	if aggregateID != row.ID {
		t.Errorf("aggregate_id = %q, want %q", aggregateID, row.ID)
	}
}

// SPEC-037 §3 scenario 5, caso fallimento: Republish esegue un INSERT
// reale nella tx e POI ritorna un errore — la transazione deve essere
// annullata (Rollback), nessuna scrittura parziale deve sopravvivere.
func TestReconcileRepublishFailureWritesNothingPartial(t *testing.T) {
	db := startMigratedPostgres(t)

	row := SourceRow{ID: "reconcile-scenario5-failure", Fingerprint: []byte("fp")}
	wantErr := errors.New("fallimento simulato DOPO una scrittura reale nella tx")
	target := Target{
		Name: "fake-pg",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return []SourceRow{row}, nil
		},
		Check: func(ctx context.Context, r SourceRow) (bool, error) { return false, nil },
		Republish: func(ctx context.Context, tx *sql.Tx, r SourceRow) error {
			if err := republishInsertOutbox(ctx, tx, r); err != nil {
				return fmt.Errorf("insert di setup fallito: %w", err)
			}
			// Scrittura reale già eseguita sulla tx; ora fallisce PRIMA del
			// commit -> Reconcile deve fare Rollback, non Commit parziale.
			return wantErr
		},
	}

	report, err := Reconcile(context.Background(), db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished != 0 {
		t.Errorf("Republished = %d, want 0 (il Republish è fallito)", report.Republished)
	}
	if len(report.Errored) != 1 || !errors.Is(report.Errored[0].Err, wantErr) {
		t.Fatalf("Errored = %v, want un elemento con err=%v", report.Errored, wantErr)
	}
	if got := countOutboxRows(t, db); got != 0 {
		t.Fatalf("righe in outbox = %d, want 0 (Rollback atteso, nessuna scrittura parziale)", got)
	}
}
