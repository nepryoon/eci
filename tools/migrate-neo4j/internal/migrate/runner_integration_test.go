//go:build integration

// SPEC-004 §7 — test di integrazione (testcontainers, immagine
// neo4j:5-community): applica la migration, verifica scenari 1/3/4;
// riapplica, verifica scenario 2; verifica scenario 5 (zero nodi
// applicativi dopo la migrazione). Richiede un daemon Docker raggiungibile,
// per questo è isolato dietro il build tag "integration" e non fa parte di
// `task test` (nessun docker garantito in ogni ambiente di sviluppo/CI).
package migrate_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"

	"github.com/eci-project/eci/tools/migrate-neo4j/internal/migrate"
)

const integrationAdminPassword = "eci-test-password-1234"

func TestMigrationAgainstRealNeo4j(t *testing.T) {
	ctx := context.Background()

	container, err := tcneo4j.Run(ctx, "neo4j:5-community", tcneo4j.WithAdminPassword(integrationAdminPassword))
	if err != nil {
		t.Fatalf("avvio container neo4j:5-community: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container: %v", err)
		}
	})

	boltURL, err := container.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("BoltUrl: %v", err)
	}

	src, err := os.ReadFile(schemaFixturePath(t))
	if err != nil {
		t.Fatalf("reading schema.cypher: %v", err)
	}
	statements := migrate.ParseStatements(string(src))

	cfg := migrate.Config{URI: boltURL, Username: "neo4j", Password: integrationAdminPassword}

	// Scenario 1: DB vuoto, prima applicazione -> tutti i constraint/indici creati.
	summary, err := migrate.Run(ctx, cfg, statements, t.Logf)
	if err != nil {
		t.Fatalf("prima migrazione fallita: %v", err)
	}
	if summary.Created != len(statements) {
		t.Fatalf("summary.Created = %d, want %d (prima applicazione, tutto nuovo)", summary.Created, len(statements))
	}
	if summary.AlreadyExists != 0 {
		t.Fatalf("summary.AlreadyExists = %d, want 0 alla prima applicazione", summary.AlreadyExists)
	}

	driver, err := neo4j.NewDriverWithContext(boltURL, neo4j.BasicAuth("neo4j", integrationAdminPassword, ""))
	if err != nil {
		t.Fatalf("driver di verifica: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(ctx) })

	// Scenario 3: SHOW CONSTRAINTS include i quattro constraint di D3
	// D3 (i property existence constraint code_node_id_exists/
	// code_node_domain_exists sono stati rimossi, vedi ADR-0004: richiedono
	// Neo4j Enterprise Edition, non disponibili su Community; il property
	// type constraint emb_vector_type è stato rimosso a sua volta, vedi
	// ADR-0005: VECTOR<FLOAT32>(1536) come tipo di proprietà è sintassi
	// Cypher 25-only non disponibile su neo4j:5-community).
	assertNamesPresent(t, ctx, driver, "SHOW CONSTRAINTS YIELD name", []string{
		"code_node_id", "file_path_unique", "method_symbol_unique", "gds_partition_scope_unique",
	})

	// Scenario 4: SHOW INDEXES elenca range + full-text + vector index di D3.
	assertNamesPresent(t, ctx, driver, "SHOW INDEXES YIELD name", []string{
		"code_node_ast_hash", "code_node_domain", "method_name", "rel_commit",
		"code_fulltext", "doc_fulltext", "code_embeddings",
	})

	// Scenario 2: riapplico sullo stesso DB -> nessun errore, tutto già esistente.
	summary2, err := migrate.Run(ctx, cfg, statements, t.Logf)
	if err != nil {
		t.Fatalf("seconda migrazione (idempotenza) fallita: %v", err)
	}
	if summary2.AlreadyExists != len(statements) {
		t.Fatalf("summary2.AlreadyExists = %d, want %d (riesecuzione idempotente)", summary2.AlreadyExists, len(statements))
	}
	if summary2.Created != 0 {
		t.Fatalf("summary2.Created = %d, want 0 alla riesecuzione", summary2.Created)
	}

	// Scenario 5: examples.cypher non viene mai eseguito dal runner -> zero
	// nodi applicativi nel DB, solo schema.
	assertNodeCountZero(t, ctx, driver)
}

func assertNamesPresent(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, query string, want []string) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		t.Fatalf("%s (collect): %v", query, err)
	}

	got := make(map[string]bool, len(records))
	for _, rec := range records {
		name, _, err := neo4j.GetRecordValue[string](rec, "name")
		if err != nil {
			t.Fatalf("record senza campo name: %v", err)
		}
		got[name] = true
	}

	for _, name := range want {
		if !got[name] {
			t.Errorf("%s: %q non trovato (trovati: %v)", query, name, got)
		}
	}
}

func assertNodeCountZero(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("count nodi: %v", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("count nodi (single): %v", err)
	}
	count, _, err := neo4j.GetRecordValue[int64](record, "c")
	if err != nil {
		t.Fatalf("count nodi: campo c mancante: %v", err)
	}
	if count != 0 {
		t.Fatalf("count nodi = %d, want 0 (examples.cypher non deve essere eseguito dalla migrazione)", count)
	}
}

func schemaFixturePath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// tools/migrate-neo4j/internal/migrate/runner_integration_test.go -> repo root is 4 levels up.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(root, "contracts", "cypher", "schema.cypher")
}
