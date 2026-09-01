//go:build integration

// Scenario 8 (SPEC-041 §3/§7) — test di parità: la reference Python di D5
// (copiata verbatim in testdata/hybrid_search_reference/,
// hybrid_search_reference.py) eseguita come SOTTOPROCESSO REALE (non un
// fixture JSON precalcolato) contro lo STESSO fixture Neo4j+Qdrant del test
// Go (setupFixture, fixture_integration_test.go). Confronta l'ordine dei
// node_id e i punteggi (entro tolleranza float) tra le due implementazioni.
//
// Esecuzione manuale (richiede Docker + python3):
//
//	go test -tags=integration ./internal/hybridsearch/... -run TestHybridSearchParity -v
package hybridsearch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/qdrant/go-client/qdrant"

	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
)

// referenceNode rispecchia il JSON stampato da run_reference.py — stessi
// campi (a parte "payload", scartato) del dataclass RetrievedNode di D5.
type referenceNode struct {
	NodeID        string   `json:"node_id"`
	Domain        string   `json:"domain"`
	Source        string   `json:"source"`
	VectorScore   *float64 `json:"vector_score"`
	HopDistance   *float64 `json:"hop_distance"`
	GraphRank     *int     `json:"graph_rank"`
	VectorRank    *int     `json:"vector_rank"`
	RRFScore      float64  `json:"rrf_score"`
	CombinedScore float64  `json:"combined_score"`
}

const floatTolerance = 1e-6

func TestHybridSearchParity(t *testing.T) {
	ctx := authenticatedContext(t, context.Background())
	f := setupFixture(t, ctx)
	pythonBin := ensureReferenceVenv(t)

	// The frozen D5 Python reference predates the authenticated canonical
	// hydration boundary. Remove the two security-only Qdrant canaries from
	// this isolated fixture so this test compares the ranking algorithm over
	// the same canonical candidates; dedicated integration tests continue to
	// prove that Go rejects both foreign-scope and orphan vector hits.
	wait := true
	deleted, err := f.qdrantClient.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: collectionName,
		Wait:           &wait,
		Points: qdrant.NewPointsSelector(
			qdrant.NewIDNum(stableUint64(f.foreignVectorID)),
			qdrant.NewIDNum(stableUint64("hs-unrelated-vector-node")),
		),
	})
	if err != nil || deleted.GetStatus() != qdrant.UpdateStatus_Completed {
		t.Fatalf("prune parity-only canaries: status=%v err=%v", deleted.GetStatus(), err)
	}

	goRanked, err := hybridsearch.HybridGraphVectorSearch(ctx, goodDeps(f), f.queryText, f.seedID, 2)
	if err != nil {
		t.Fatalf("HybridGraphVectorSearch (Go): %v", err)
	}

	pyRanked := runReference(t, pythonBin, f)

	if len(goRanked) != len(pyRanked) {
		t.Fatalf("len(goRanked)=%d, len(pyRanked)=%d\nGo:  %+v\nPy: %+v", len(goRanked), len(pyRanked), goRanked, pyRanked)
	}
	for i := range goRanked {
		g := goRanked[i]
		py := pyRanked[i]
		if g.NodeID != py.NodeID {
			t.Fatalf("posizione %d: node_id Go=%q, Python=%q (ordine diverso)\nGo:  %+v\nPy: %+v", i, g.NodeID, py.NodeID, goRanked, pyRanked)
		}
		if !floatsClose(g.RRFScore, py.RRFScore) {
			t.Errorf("posizione %d (%s): rrf_score Go=%v, Python=%v", i, g.NodeID, g.RRFScore, py.RRFScore)
		}
		if !floatsClose(g.CombinedScore, py.CombinedScore) {
			t.Errorf("posizione %d (%s): combined_score Go=%v, Python=%v", i, g.NodeID, g.CombinedScore, py.CombinedScore)
		}
		if !ptrFloatsClose(g.HopDistance, py.HopDistance) {
			t.Errorf("posizione %d (%s): hop_distance Go=%v, Python=%v", i, g.NodeID, derefF(g.HopDistance), derefF(py.HopDistance))
		}
	}
}

func floatsClose(a, b float64) bool {
	return math.Abs(a-b) < floatTolerance
}

func ptrFloatsClose(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return floatsClose(*a, *b)
}

func derefF(f *float64) any {
	if f == nil {
		return nil
	}
	return *f
}

func runReference(t *testing.T, pythonBin string, f *fixtureHandles) []referenceNode {
	t.Helper()
	scriptDir := repoPath(t, "services", "retrieval-engine", "testdata", "hybrid_search_reference")
	script := filepath.Join(scriptDir, "run_reference.py")

	cmd := exec.Command(pythonBin, script,
		"--neo4j-uri", f.neo4jBoltURL,
		"--neo4j-user", "neo4j",
		"--neo4j-password", neo4jAdminPassword,
		"--qdrant-host", f.qdrantHost,
		"--qdrant-grpc-port", fmt.Sprintf("%d", f.qdrantGRPCPort),
		"--collection", collectionName,
		"--embedder-base-url", f.embedderBaseURL,
		"--query", f.queryText,
		"--entry-node-id", f.seedID,
		"--max-depth", "2",
	)
	cmd.Dir = scriptDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("run_reference.py fallito: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	var nodes []referenceNode
	if err := json.Unmarshal(stdout.Bytes(), &nodes); err != nil {
		t.Fatalf("decodifica output run_reference.py: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	return nodes
}

func ensureReferenceVenv(t *testing.T) string {
	t.Helper()
	dir := repoPath(t, "services", "retrieval-engine", "testdata", "hybrid_search_reference")
	venvDir := filepath.Join(dir, ".venv")
	python := filepath.Join(venvDir, "bin", "python")
	if _, err := os.Stat(python); err == nil {
		return python
	}

	if out, err := exec.Command("python3", "-m", "venv", venvDir).CombinedOutput(); err != nil {
		t.Fatalf("creazione venv (%s): %v\n%s", venvDir, err, out)
	}
	pip := filepath.Join(venvDir, "bin", "pip")
	requirements := filepath.Join(dir, "requirements.txt")
	out, err := exec.Command(pip, "install", "-q", "-r", requirements).CombinedOutput()
	if err != nil {
		t.Fatalf("pip install -r requirements.txt fallito: %v\n%s", err, out)
	}
	return python
}
