// SPEC-004 §4, §7 — test unitari del loop di esecuzione (runAll) e delle
// funzioni pure di supporto, isolati dal driver Neo4j (nessun docker
// richiesto: la parte che parla col protocollo Bolt è coperta solo dai test
// di integrazione in runner_integration_test.go, //go:build integration).
package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRunAllCountsCreatedAndAlreadyExists(t *testing.T) {
	statements := []string{"stmt-a", "stmt-b", "stmt-c"}
	outcomes := []Outcome{OutcomeCreated, OutcomeAlreadyExists, OutcomeAlreadyExists}

	var calls []string
	step := func(_ context.Context, stmt string) (Outcome, error) {
		calls = append(calls, stmt)
		return outcomes[len(calls)-1], nil
	}

	summary, err := runAll(context.Background(), statements, step, func(string, ...any) {})
	if err != nil {
		t.Fatalf("runAll() error = %v, want nil", err)
	}
	if summary.Created != 1 || summary.AlreadyExists != 2 {
		t.Fatalf("summary = %+v, want Created=1 AlreadyExists=2", summary)
	}
	if len(calls) != 3 {
		t.Fatalf("step called %d times, want 3", len(calls))
	}
}

// SPEC-004 §4: "Uno statement DDL fallisce a metà lista ... il runner si
// ferma, riporta quale statement e l'errore esatto, non prosegue
// silenziosamente con i successivi".
func TestRunAllStopsAtFirstErrorAndReportsWhichStatement(t *testing.T) {
	statements := []string{"stmt-a", "stmt-b (fails)", "stmt-c"}
	wantErr := errors.New("boom")

	var calls []string
	step := func(_ context.Context, stmt string) (Outcome, error) {
		calls = append(calls, stmt)
		if stmt == "stmt-b (fails)" {
			return 0, wantErr
		}
		return OutcomeCreated, nil
	}

	summary, err := runAll(context.Background(), statements, step, func(string, ...any) {})
	if err == nil {
		t.Fatal("runAll() error = nil, want non-nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("runAll() error = %v, want wrapping %v", err, wantErr)
	}
	if !strings.Contains(err.Error(), "2/3") {
		t.Errorf("error %q does not name which statement (2/3) failed", err.Error())
	}
	if !strings.Contains(err.Error(), "stmt-b") {
		t.Errorf("error %q does not name the failing statement", err.Error())
	}
	if len(calls) != 2 {
		t.Fatalf("step called %d times, want 2 (must stop after the failure, not run stmt-c)", len(calls))
	}
	if summary.Created != 1 {
		t.Fatalf("summary.Created = %d, want 1 (only stmt-a completed)", summary.Created)
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("NEO4J_URI", "")
	t.Setenv("NEO4J_USER", "")
	t.Setenv("NEO4J_PASSWORD", "")

	cfg := ConfigFromEnv()
	if cfg.URI != "bolt://localhost:7687" {
		t.Errorf("URI = %q, want default bolt://localhost:7687", cfg.URI)
	}
	// Default allineato a NEO4J_AUTH=neo4j/eci-dev-only in
	// deploy/compose/docker-compose.yml (SPEC-006), non credenziali vuote.
	if cfg.Username != "neo4j" {
		t.Errorf("Username = %q, want default neo4j", cfg.Username)
	}
	if cfg.Password != "eci-dev-only" {
		t.Errorf("Password = %q, want default eci-dev-only", cfg.Password)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	t.Setenv("NEO4J_URI", "bolt://neo4j.internal:7687")
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "s3cret")

	cfg := ConfigFromEnv()
	if cfg.URI != "bolt://neo4j.internal:7687" {
		t.Errorf("URI = %q, want override", cfg.URI)
	}
	if cfg.Username != "neo4j" {
		t.Errorf("Username = %q, want neo4j", cfg.Username)
	}
	if cfg.Password != "s3cret" {
		t.Errorf("Password = %q, want s3cret", cfg.Password)
	}
}

// SPEC-004 §4: il vector index richiede una versione Neo4j che potrebbe non
// supportarlo — l'errore deve nominare esplicitamente il requisito, non
// essere un errore Cypher generico da decifrare.
func TestClassifyStatementErrorNamesVectorIndexRequirement(t *testing.T) {
	original := errors.New("Neo.ClientError.Statement.SyntaxError")
	stmt := "CREATE VECTOR INDEX code_embeddings IF NOT EXISTS\nFOR (n:CodeNode) ON (n.embedding)"

	wrapped := classifyStatementError(stmt, original)

	if !errors.Is(wrapped, original) {
		t.Fatalf("classifyStatementError() does not wrap the original error")
	}
	if !strings.Contains(wrapped.Error(), "5.13") {
		t.Errorf("error %q does not name the minimum Neo4j version", wrapped.Error())
	}
}

func TestClassifyStatementErrorPassesThroughForNonVectorStatements(t *testing.T) {
	original := errors.New("Neo.ClientError.Statement.SyntaxError")
	stmt := "CREATE CONSTRAINT code_node_id IF NOT EXISTS FOR (n:CodeNode) REQUIRE n.id IS UNIQUE"

	wrapped := classifyStatementError(stmt, original)

	if !errors.Is(wrapped, original) {
		t.Fatalf("classifyStatementError() does not wrap the original error")
	}
	if strings.Contains(wrapped.Error(), "5.13") {
		t.Errorf("error %q unexpectedly names the vector index version requirement", wrapped.Error())
	}
}

func TestFirstLine(t *testing.T) {
	got := firstLine("CREATE CONSTRAINT code_node_id IF NOT EXISTS\nFOR (n:CodeNode) REQUIRE n.id IS UNIQUE")
	want := "CREATE CONSTRAINT code_node_id IF NOT EXISTS"
	if got != want {
		t.Errorf("firstLine() = %q, want %q", got, want)
	}
}
