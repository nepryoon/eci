// SPEC-004 §7 — test unitari del parser statement-splitter: separatore ';'
// esplicito (§2 punto 2), robusto a statement multi-riga, commenti '//' e
// righe vuote.
package migrate

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseStatementsSingleStatement(t *testing.T) {
	got := ParseStatements("CREATE CONSTRAINT foo IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE;")
	want := []string{"CREATE CONSTRAINT foo IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE"}
	assertStatements(t, got, want)
}

func TestParseStatementsMultipleStatements(t *testing.T) {
	src := "CREATE CONSTRAINT a IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE;\n" +
		"CREATE CONSTRAINT b IF NOT EXISTS FOR (n:Y) REQUIRE n.id IS UNIQUE;"
	got := ParseStatements(src)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2 (%v)", len(got), got)
	}
}

func TestParseStatementsMultiLineStatement(t *testing.T) {
	src := "CREATE CONSTRAINT code_node_id IF NOT EXISTS\n" +
		"FOR (n:CodeNode) REQUIRE n.id IS UNIQUE;"
	got := ParseStatements(src)
	want := []string{"CREATE CONSTRAINT code_node_id IF NOT EXISTS\nFOR (n:CodeNode) REQUIRE n.id IS UNIQUE"}
	assertStatements(t, got, want)
}

func TestParseStatementsStripsLineComments(t *testing.T) {
	src := "// section header\n" +
		"// more header\n" +
		"CREATE CONSTRAINT a IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE;\n" +
		"// trailing comment, no statement here\n"
	got := ParseStatements(src)
	want := []string{"CREATE CONSTRAINT a IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE"}
	assertStatements(t, got, want)
}

func TestParseStatementsIgnoresBlankLines(t *testing.T) {
	src := "\n\nCREATE CONSTRAINT a IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE;\n\n\n" +
		"CREATE CONSTRAINT b IF NOT EXISTS FOR (n:Y) REQUIRE n.id IS UNIQUE;\n\n"
	got := ParseStatements(src)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2 (%v)", len(got), got)
	}
}

func TestParseStatementsNoTrailingSemicolonAtEOF(t *testing.T) {
	got := ParseStatements("CREATE CONSTRAINT a IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE")
	want := []string{"CREATE CONSTRAINT a IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE"}
	assertStatements(t, got, want)
}

func TestParseStatementsEmptySourceYieldsNoStatements(t *testing.T) {
	got := ParseStatements("   \n// only a comment\n\n")
	if len(got) != 0 {
		t.Fatalf("len(got) = %d, want 0 (%v)", len(got), got)
	}
}

// Il vero contracts/cypher/schema.cypher (dopo lo split §2: solo DDL, la
// rimozione dei due existence constraint per ADR-0004 e la rimozione del
// property type constraint emb_vector_type per ADR-0005) deve produrre
// esattamente 11 statement: 3 constraint + 5 range index (incluso l'indice
// security-scope di T6.3) + 2 full-text index + 1 vector index (scenario 3/4).
func TestParseStatementsRealSchemaFile(t *testing.T) {
	data, err := os.ReadFile(schemaFixturePath(t))
	if err != nil {
		t.Fatalf("reading schema.cypher: %v", err)
	}

	got := ParseStatements(string(data))
	if len(got) != 11 {
		t.Fatalf("len(got) = %d, want 11 statements from schema.cypher: %v", len(got), got)
	}
	for _, stmt := range got {
		if !strings.Contains(stmt, "IF NOT EXISTS") {
			t.Errorf("statement missing idempotency guard 'IF NOT EXISTS': %q", stmt)
		}
	}
}

func schemaFixturePath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// tools/migrate-neo4j/internal/migrate/parser_test.go -> repo root is 4 levels up.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(root, "contracts", "cypher", "schema.cypher")
}

func assertStatements(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d\ngot:  %#v\nwant: %#v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("statement %d:\ngot:  %q\nwant: %q", i, got[i], want[i])
		}
	}
}
