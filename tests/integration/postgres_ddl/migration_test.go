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
	"os"
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

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetConnMaxLifetime(time.Minute)
	// CNPG keeps this passwordless privilege role present even while CDC is
	// disabled. The login role is intentionally created only after migrations
	// to reproduce a supported disabled -> enabled chart upgrade.
	if _, err := db.ExecContext(ctx, `CREATE ROLE eci_cdc_outbox_reader NOLOGIN NOREPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS`); err != nil {
		t.Fatalf("creazione ruolo privilegi CDC fixture: %v", err)
	}

	// Scenario 1: DB vuoto, applico `up` -> le 4 tabelle esistono.
	runMigrateCLI(t, migrationsDir, dsn, "up")

	assertTablesExist(t, db, "code_node", "code_relation", "outbox", "processed_events", "ingestion_command_receipt", "consumer_projection_watermark")

	if _, err := db.ExecContext(ctx, `CREATE ROLE eci_cdc LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS IN ROLE eci_cdc_outbox_reader`); err != nil {
		t.Fatalf("abilitazione ruolo CDC dopo migrations: %v", err)
	}

	t.Run("ADR0020_DedicatedCDCUpgradeRoleAndFixedPublication", func(t *testing.T) {
		var replication, superuser, createDB, createRole, bypassRLS bool
		if err := db.QueryRowContext(ctx, `
			SELECT rolreplication, rolsuper, rolcreatedb, rolcreaterole, rolbypassrls
			FROM pg_catalog.pg_roles WHERE rolname = 'eci_cdc'`,
		).Scan(&replication, &superuser, &createDB, &createRole, &bypassRLS); err != nil {
			t.Fatalf("lettura attributi eci_cdc: %v", err)
		}
		if !replication || superuser || createDB || createRole || bypassRLS {
			t.Fatalf("attributi eci_cdc inattesi: replication=%v superuser=%v createdb=%v createrole=%v bypassrls=%v", replication, superuser, createDB, createRole, bypassRLS)
		}
		var canSelect bool
		if err := db.QueryRowContext(ctx, `SELECT has_table_privilege('eci_cdc', 'public.outbox', 'SELECT')`).Scan(&canSelect); err != nil {
			t.Fatalf("verifica SELECT outbox per eci_cdc: %v", err)
		}
		if !canSelect {
			t.Fatal("eci_cdc non ha SELECT su public.outbox")
		}
		var publishedTable string
		if err := db.QueryRowContext(ctx, `
			SELECT schemaname || '.' || tablename
			FROM pg_catalog.pg_publication_tables
			WHERE pubname = 'eci_outbox_publication'`,
		).Scan(&publishedTable); err != nil {
			t.Fatalf("lettura publication CDC: %v", err)
		}
		if publishedTable != "public.outbox" {
			t.Fatalf("publication table = %q, want public.outbox", publishedTable)
		}
	})

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

	// Scenario 6: la deduplica e' per consumer. Lo stesso evento deve poter
	// essere elaborato da due consumer distinti (fan-out Kafka), mentre una
	// redelivery allo stesso consumer deve restare una violazione univoca.
	t.Run("Scenario6_ProcessedEventsPKDedup", func(t *testing.T) {
		const eventID = "11111111-1111-1111-1111-111111111111"
		_, err := db.ExecContext(ctx, `INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, 'sink-graph')`, eventID)
		if err != nil {
			t.Fatalf("primo insert in processed_events fallito: %v", err)
		}
		_, err = db.ExecContext(ctx, `INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, 'sink-search')`, eventID)
		if err != nil {
			t.Fatalf("stesso event_id per consumer distinto deve essere ammesso: %v", err)
		}
		_, err = db.ExecContext(ctx, `INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, 'sink-graph')`, eventID)
		assertPQErrorCode(t, err, "23505", "unique_violation sulla coppia event_id/consumer_name")
	})

	t.Run("Scenario6_RollbackFailsClosedWithFanOutProvenance", func(t *testing.T) {
		const eventID = "11111111-1111-1111-1111-111111111111"
		downSQL, err := os.ReadFile(repoPath(t, "contracts", "sql", "migrations", "0006_consumer_scoped_processed_events.down.sql"))
		if err != nil {
			t.Fatalf("lettura down migration 0006: %v", err)
		}
		_, err = db.ExecContext(ctx, string(downSQL))
		assertPQErrorCode(t, err, "P0001", "rollback fail-closed con fan-out gia' registrato")

		if _, err := db.ExecContext(ctx, `
			DELETE FROM processed_events
			WHERE event_id = $1 AND consumer_name = 'sink-search'`, eventID); err != nil {
			t.Fatalf("cleanup controllato fixture fan-out: %v", err)
		}
		if _, err := db.ExecContext(ctx, string(downSQL)); err != nil {
			t.Fatalf("down migration 0006 senza fan-out: %v", err)
		}
		_, err = db.ExecContext(ctx, `INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, 'sink-search')`, eventID)
		assertPQErrorCode(t, err, "23505", "chiave globale ripristinata dalla down migration")

		upSQL, err := os.ReadFile(repoPath(t, "contracts", "sql", "migrations", "0006_consumer_scoped_processed_events.up.sql"))
		if err != nil {
			t.Fatalf("lettura up migration 0006: %v", err)
		}
		if _, err := db.ExecContext(ctx, string(upSQL)); err != nil {
			t.Fatalf("ripristino schema 0006 dopo test rollback: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, 'sink-search')`, eventID); err != nil {
			t.Fatalf("ripristino fixture fan-out dopo up migration: %v", err)
		}
	})

	t.Run("SPEC070_DeleteReceiptRollbackIsRepresentable", func(t *testing.T) {
		const commandID = "22222222-2222-2222-2222-222222222222"
		if _, err := db.ExecContext(ctx, `
			INSERT INTO ingestion_command_receipt
			(command_id, fingerprint, tenant_id, repository, commit_sha, path, source_sha256, operation)
			VALUES ($1, $2, 'tenant-a', 'repo-a', $3, 'deleted.go', NULL, 'DELETE')`,
			commandID, sha256Fixture("delete-receipt"), "1111111111111111111111111111111111111111"); err != nil {
			t.Fatalf("insert DELETE receipt fixture: %v", err)
		}

		runMigrateCLI(t, migrationsDir, dsn, "down", "1")
		var receiptCount int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM ingestion_command_receipt WHERE command_id = $1`, commandID,
		).Scan(&receiptCount); err != nil {
			t.Fatalf("count receipt after 0008 down: %v", err)
		}
		if receiptCount != 0 {
			t.Fatalf("DELETE receipt survived rollback into UPSERT-only schema: count=%d", receiptCount)
		}
		runMigrateCLI(t, migrationsDir, dsn, "up", "1")
	})

	t.Run("ADR0025_OutboxSequenceAndConsumerWatermarkAreBounded", func(t *testing.T) {
		var first, second int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
			VALUES ('CodeNode', 'sequence-node', 'UPSERT', '{}'::jsonb)
			RETURNING event_sequence`).Scan(&first); err != nil {
			t.Fatalf("insert first sequenced outbox event: %v", err)
		}
		if err := db.QueryRowContext(ctx, `
			INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
			VALUES ('CodeNode', 'sequence-node', 'DELETE', '{}'::jsonb)
			RETURNING event_sequence`).Scan(&second); err != nil {
			t.Fatalf("insert second sequenced outbox event: %v", err)
		}
		if second <= first {
			t.Fatalf("outbox event sequence is not monotonic: first=%d second=%d", first, second)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO consumer_projection_watermark
			(consumer_name, aggregate_type, aggregate_id, event_sequence, operation)
			VALUES ('sink-graph', 'CodeNode', 'sequence-node', $1, 'DELETE')`, second); err != nil {
			t.Fatalf("insert consumer watermark: %v", err)
		}
		_, err := db.ExecContext(ctx, `
			INSERT INTO consumer_projection_watermark
			(consumer_name, aggregate_type, aggregate_id, event_sequence, operation)
			VALUES ('sink-graph', 'CodeNode', 'invalid', 0, 'UPSERT')`)
		assertPQErrorCode(t, err, "23514", "zero consumer watermark")
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

	// La down migration 0006 rifiuta correttamente di perdere record di
	// consumer distinti (scenario dedicato sopra). Rimuoviamo qui la sola
	// fixture fan-out prima del rollback completo.
	if _, err := db.ExecContext(ctx, `
		DELETE FROM processed_events
		WHERE event_id = '11111111-1111-1111-1111-111111111111'
		  AND consumer_name = 'sink-search'`); err != nil {
		t.Fatalf("cleanup fixture fan-out prima del rollback: %v", err)
	}

	// Scenario 2: applico `down` -> tutte le tabelle rimosse senza errori.
	// `down` senza contatore (non `down 1`, SPEC-027 §10 deviazione: `down
	// 1` annullava correttamente TUTTO finché esisteva una sola migration;
	// da quando 0002_lineage esiste, `down 1` annulla solo l'ultima e
	// lascia le tabelle di questa migration — `down` senza contatore
	// resta corretto indipendentemente da quante migration si accumulano).
	runMigrateCLI(t, migrationsDir, dsn, "down", "-all")
	assertTablesAbsent(t, db, "code_node", "code_relation", "outbox", "processed_events", "ingestion_command_receipt")
	var publicationExists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = 'eci_outbox_publication')`).Scan(&publicationExists); err != nil {
		t.Fatalf("verifica drop publication: %v", err)
	}
	if publicationExists {
		t.Fatal("eci_outbox_publication ancora presente dopo migrate down")
	}
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
