//go:build integration

// SPEC-005 §7 — test di integrazione (testcontainers, immagine postgres:17):
// applica `up` via il CLI `migrate` reale (lo stesso invocato da
// `task db:migrate`), verifica gli scenari 1/3/4/5/6 di §3, applica `down`
// e verifica lo scenario 2 (tabelle assenti). Richiede un daemon Docker
// raggiungibile e il binario `migrate` sul PATH — per questo è isolato
// dietro il build tag "integration" e non fa parte di `task test`.
package postgres_ddl_test

import (
	"context"
	"database/sql"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/lib/pq"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	integrationDBUser     = "eci"
	integrationDBPassword = "eci-test-password-1234"
	integrationDBName     = "eci"
)

func TestPostgresDDLMigration(t *testing.T) {
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

	// Scenario 1: DB vuoto, applico `up` -> le 4 tabelle esistono.
	runMigrateCLI(t, migrationsDir, dsn, "up")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetConnMaxLifetime(time.Minute)

	assertTablesExist(t, db, "code_node", "code_relation", "outbox", "processed_events")

	// Scenario 3: INSERT con domain non valido -> CHECK constraint violation.
	t.Run("Scenario3_CheckConstraintOnDomain", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
			VALUES ($1, 'invalid', 'File', 'foo.go', $2, '{}'::jsonb)`,
			"scenario3-node", sha256Fixture("scenario3"))
		assertPQErrorCode(t, err, "23514", "check_violation su domain")
	})

	// Scenario 5: code_relation con from_id inesistente -> FK violation.
	t.Run("Scenario5_ForeignKeyOnFromId", func(t *testing.T) {
		mustInsertCodeNode(t, ctx, db, "scenario5-to")
		_, err := db.ExecContext(ctx, `
			INSERT INTO code_relation (domain, rel_type, from_id, to_id)
			VALUES ('code', 'CALLS', $1, $2)`,
			"scenario5-from-does-not-exist", "scenario5-to")
		assertPQErrorCode(t, err, "23503", "foreign_key_violation su from_id")
	})

	// Scenario 6: processed_events, event_id duplicato -> PK violation (dedup).
	t.Run("Scenario6_ProcessedEventsPKDedup", func(t *testing.T) {
		const eventID = "11111111-1111-1111-1111-111111111111"
		_, err := db.ExecContext(ctx, `INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, 'sink-graph')`, eventID)
		if err != nil {
			t.Fatalf("primo insert in processed_events fallito: %v", err)
		}
		_, err = db.ExecContext(ctx, `INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, 'sink-graph')`, eventID)
		assertPQErrorCode(t, err, "23505", "unique_violation su event_id duplicato")
	})

	// Scenario 4: atomicità. INSERT code_node + INSERT outbox nella stessa
	// transazione, poi un secondo INSERT che viola un vincolo forza il
	// ROLLBACK -> nessuna delle due righe deve restare.
	t.Run("Scenario4_TransactionAtomicity", func(t *testing.T) {
		const nodeID = "scenario4-node"
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
			VALUES ($1, 'code', 'File', 'bar.go', $2, '{}'::jsonb)`,
			nodeID, sha256Fixture("scenario4")); err != nil {
			t.Fatalf("insert code_node nella tx: %v", err)
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
			VALUES ('CodeNode', $1, 'UPSERT', '{}'::jsonb)`, nodeID); err != nil {
			t.Fatalf("insert outbox nella tx: %v", err)
		}

		// Forza un errore prima del commit: id duplicato viola code_node PK.
		_, err = tx.ExecContext(ctx, `
			INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
			VALUES ($1, 'code', 'File', 'bar.go', $2, '{}'::jsonb)`,
			nodeID, sha256Fixture("scenario4"))
		assertPQErrorCode(t, err, "23505", "unique_violation su id duplicato (per forzare l'errore pre-commit)")

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}

		var count int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM code_node WHERE id = $1`, nodeID).Scan(&count); err != nil {
			t.Fatalf("count code_node: %v", err)
		}
		if count != 0 {
			t.Errorf("code_node dopo ROLLBACK: count = %d, want 0 (atomicità violata)", count)
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, nodeID).Scan(&count); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		if count != 0 {
			t.Errorf("outbox dopo ROLLBACK: count = %d, want 0 (atomicità violata)", count)
		}
	})

	// Scenario 2: applico `down` -> tutte le tabelle rimosse senza errori.
	runMigrateCLI(t, migrationsDir, dsn, "down", "1")
	assertTablesAbsent(t, db, "code_node", "code_relation", "outbox", "processed_events")
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

func assertTablesExist(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	for _, table := range tables {
		var exists bool
		err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("verifica esistenza tabella %s: %v", table, err)
		}
		if !exists {
			t.Errorf("tabella %s non trovata dopo 'migrate up'", table)
		}
	}
}

func assertTablesAbsent(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	for _, table := range tables {
		var exists bool
		err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("verifica assenza tabella %s: %v", table, err)
		}
		if exists {
			t.Errorf("tabella %s ancora presente dopo 'migrate down'", table)
		}
	}
}

func mustInsertCodeNode(t *testing.T, ctx context.Context, db *sql.DB, id string) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
		VALUES ($1, 'code', 'File', 'baz.go', $2, '{}'::jsonb)
		ON CONFLICT (id) DO NOTHING`, id, sha256Fixture(id))
	if err != nil {
		t.Fatalf("insert code_node di supporto (%s): %v", id, err)
	}
}

func assertPQErrorCode(t *testing.T, err error, wantCode, label string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: atteso errore, nessuno restituito", label)
	}
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		t.Fatalf("%s: errore non è un *pq.Error: %v", label, err)
	}
	if string(pqErr.Code) != wantCode {
		t.Fatalf("%s: codice errore = %s, want %s (%v)", label, pqErr.Code, wantCode, err)
	}
}

// sha256Fixture produce un placeholder valido per CHAR(64)/pattern
// sha256-like nei test (non è un vero digest, solo 64 caratteri hex).
func sha256Fixture(seed string) string {
	const hexDigits = "0123456789abcdef"
	out := make([]byte, 64)
	for i := range out {
		out[i] = hexDigits[(int(seed[i%len(seed)])+i)%16]
	}
	return string(out)
}
