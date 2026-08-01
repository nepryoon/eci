// SPEC-005 §7 — test di parità: i campi di code_node/code_relation in
// contracts/sql/migrations/0001_init.up.sql devono coprire tutti i campi
// definiti su CodeNode/CodeRelation in contracts/jsonschema/hybrid-graph.json
// (nessun campo dello schema JSON orfano nello schema SQL). Non richiede
// Docker: legge i due file staticamente, nessuna connessione a un DB.
package postgres_ddl_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// versioning si applica solo a code_node: il DDL flatta i campi
// dell'oggetto "versioning" di hybrid-graph.json in colonne scalari.
// code_relation non ha semantica di versioning (nessun version/is_current/
// valid_from/valid_to/supersedes nel DDL) — è un'esclusione esplicita,
// non un'omissione silenziosa: se hybrid-graph.json aggiunge un nuovo campo
// a CodeRelation, questo test fallisce finché la mappa non viene aggiornata
// a mano.
var codeNodeFieldToColumn = map[string]string{
	"id":           "id",
	"domain":       "domain",
	"name":         "name",
	"ast_hash":     "ast_hash",
	"content_hash": "content_hash",
	"embedding":    "embedding_ref",
	"provenance":   "provenance",
	"ext":          "ext",
	// "versioning" è un oggetto in hybrid-graph.json, flattato in colonne
	// scalari nel DDL (vedi SPEC-005 §2): gestito separatamente sotto,
	// non tramite questa mappa 1:1 campo->colonna.
}

var versioningFieldToColumn = map[string]string{
	"version":    "version",
	"is_current": "is_current",
	"valid_from": "valid_from",
	"valid_to":   "valid_to",
	"supersedes": "supersedes",
}

var codeRelationFieldToColumn = map[string]string{
	"id":         "id",
	"domain":     "domain",
	"rel_type":   "rel_type",
	"from_id":    "from_id",
	"to_id":      "to_id",
	"weight":     "weight",
	"provenance": "provenance",
	// "versioning" è presente (opzionale) su CodeRelation in
	// hybrid-graph.json ma intenzionalmente non mappato: le relazioni non
	// sono versionate in D2, solo i nodi lo sono. Vedi commento sopra.
}

var codeRelationSkippedFields = map[string]bool{
	"versioning": true,
}

func TestSQLColumnsCoverJSONSchemaFields(t *testing.T) {
	schema := loadHybridGraphSchema(t)
	ddl := loadUpMigrationSQL(t)

	codeNodeColumns := extractColumns(t, ddl, "code_node")
	codeRelationColumns := extractColumns(t, ddl, "code_relation")

	t.Run("CodeNode", func(t *testing.T) {
		fields := schemaProperties(t, schema, "CodeNode")
		for field := range fields {
			if field == "versioning" {
				versioningFields := schemaProperties(t, schema, "versioning")
				for vField := range versioningFields {
					col, ok := versioningFieldToColumn[vField]
					if !ok {
						t.Errorf("campo CodeNode.versioning.%q orfano: nessuna voce in versioningFieldToColumn", vField)
						continue
					}
					if !codeNodeColumns[col] {
						t.Errorf("CodeNode.versioning.%q -> colonna %q attesa ma assente in code_node (colonne trovate: %v)", vField, col, sortedKeys(codeNodeColumns))
					}
				}
				continue
			}
			col, ok := codeNodeFieldToColumn[field]
			if !ok {
				t.Errorf("campo CodeNode.%q orfano: nessuna voce in codeNodeFieldToColumn", field)
				continue
			}
			if !codeNodeColumns[col] {
				t.Errorf("CodeNode.%q -> colonna %q attesa ma assente in code_node (colonne trovate: %v)", field, col, sortedKeys(codeNodeColumns))
			}
		}
	})

	t.Run("CodeRelation", func(t *testing.T) {
		fields := schemaProperties(t, schema, "CodeRelation")
		for field := range fields {
			if codeRelationSkippedFields[field] {
				continue
			}
			col, ok := codeRelationFieldToColumn[field]
			if !ok {
				t.Errorf("campo CodeRelation.%q orfano: nessuna voce in codeRelationFieldToColumn né in codeRelationSkippedFields", field)
				continue
			}
			if !codeRelationColumns[col] {
				t.Errorf("CodeRelation.%q -> colonna %q attesa ma assente in code_relation (colonne trovate: %v)", field, col, sortedKeys(codeRelationColumns))
			}
		}
	})
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

type jsonSchemaDoc struct {
	Definitions map[string]struct {
		Properties map[string]json.RawMessage `json:"properties"`
	} `json:"definitions"`
}

func loadHybridGraphSchema(t *testing.T) jsonSchemaDoc {
	t.Helper()
	data, err := os.ReadFile(repoPath(t, "contracts", "jsonschema", "hybrid-graph.json"))
	if err != nil {
		t.Fatalf("reading hybrid-graph.json: %v", err)
	}
	var doc jsonSchemaDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parsing hybrid-graph.json: %v", err)
	}
	return doc
}

func schemaProperties(t *testing.T, doc jsonSchemaDoc, definition string) map[string]bool {
	t.Helper()
	def, ok := doc.Definitions[definition]
	if !ok {
		t.Fatalf("hybrid-graph.json: definizione %q non trovata", definition)
	}
	out := make(map[string]bool, len(def.Properties))
	for name := range def.Properties {
		out[name] = true
	}
	return out
}

func loadUpMigrationSQL(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(repoPath(t, "contracts", "sql", "migrations", "0001_init.up.sql"))
	if err != nil {
		t.Fatalf("reading 0001_init.up.sql: %v", err)
	}
	return string(data)
}

var createTableRe = regexp.MustCompile(`(?is)CREATE TABLE\s+(\w+)\s*\((.*?)\n\);`)

// extractColumns estrae i nomi di colonna dal blocco CREATE TABLE del file
// DDL di fixture (formato controllato: una definizione di colonna per riga,
// terminata da virgola tranne l'ultima). Non è un parser SQL generico.
func extractColumns(t *testing.T, ddl, table string) map[string]bool {
	t.Helper()
	matches := createTableRe.FindAllStringSubmatch(ddl, -1)
	for _, m := range matches {
		if m[1] != table {
			continue
		}
		out := map[string]bool{}
		for _, line := range strings.Split(m[2], "\n") {
			line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			first := strings.ToUpper(fields[0])
			switch first {
			case "PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK":
				continue
			}
			out[fields[0]] = true
		}
		return out
	}
	t.Fatalf("nessun blocco CREATE TABLE %s trovato in 0001_init.up.sql", table)
	return nil
}

func repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// tests/integration/postgres_ddl/parity_test.go -> repo root is 3 levels up.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
