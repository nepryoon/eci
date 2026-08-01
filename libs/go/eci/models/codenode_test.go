// SPEC-003 §3 scenario 4, §4 edge case, §7 — ParseExt() table-driven contro
// i fixture condivisi con i test Python (tests/fixtures/jsonschema/), non
// duplicati per linguaggio.
package models

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func fixturesDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// libs/go/eci/models/codenode_test.go -> repo root is 4 levels up.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(root, "tests", "fixtures", "jsonschema")
}

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixturesDir(t), name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return data
}

func TestParseExt(t *testing.T) {
	cases := []struct {
		name    string
		fixture string
		wantErr bool
	}{
		{"code valid", "codenode_code_valid.json", false},
		{"doc valid", "codenode_doc_valid.json", false},
		{"legal valid", "codenode_legal_valid.json", false},
		{"legal domain with code extension fields", "codenode_legal_ext_mismatch.json", true},
		{"code domain without ext", "codenode_code_missing_ext.json", true},
		{"domain outside enum", "codenode_domain_invalid.json", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var node CodeNode
			if err := json.Unmarshal(loadFixture(t, tc.fixture), &node); err != nil {
				t.Fatalf("unmarshal CodeNode: %v", err)
			}

			_, err := node.ParseExt()
			if tc.wantErr && err == nil {
				t.Fatalf("ParseExt() = nil error, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ParseExt() error = %v, want nil", err)
			}
		})
	}
}

func TestParseExtCodeReturnsTypedExtension(t *testing.T) {
	var node CodeNode
	if err := json.Unmarshal(loadFixture(t, "codenode_code_valid.json"), &node); err != nil {
		t.Fatalf("unmarshal CodeNode: %v", err)
	}

	ext, err := node.ParseExt()
	if err != nil {
		t.Fatalf("ParseExt: %v", err)
	}

	codeExt, ok := ext.(CodeExtension)
	if !ok {
		t.Fatalf("ParseExt() type = %T, want CodeExtension", ext)
	}
	if codeExt.NodeType != "Method" {
		t.Fatalf("NodeType = %q, want %q", codeExt.NodeType, "Method")
	}
	if codeExt.SymbolID != "scip-java maven acme orders 1.0 OrderService#charge()." {
		t.Fatalf("SymbolID = %q, unexpected", codeExt.SymbolID)
	}
}
