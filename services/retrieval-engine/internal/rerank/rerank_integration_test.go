//go:build integration

// Test di integrazione: Neo4j reale via testcontainers (lettura batch di
// impact_score) + reranker-fake come sottoprocesso uvicorn reale (SPEC-044
// §7, stesso principio già stabilito per embedder-fake/T2.4). Esecuzione
// manuale (richiede Docker):
//
//	go test -tags=integration ./internal/rerank/... -run TestRerankIntegration -v
package rerank_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"

	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerank"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerankclient"
)

const neo4jAdminPassword = "eci-test-password-1234"

func hop(v float64) *float64 { return &v }

func TestRerankIntegration(t *testing.T) {
	ctx := context.Background()
	driver := startNeo4j(t, ctx)
	rerankerBaseURL := startRerankerFake(t)
	client := rerankclient.New(rerankerBaseURL)

	t.Run("Scenario1_FormulaMatchesDeclaredFormulaWithRealImpactScoreRead", func(t *testing.T) {
		seedNodeWithImpactScore(t, ctx, driver, "n1", 0.9)
		seedNodeWithImpactScore(t, ctx, driver, "n2", 0.1)

		candidates := []hybridsearch.RetrievedNode{
			{NodeID: "n1", HopDistance: hop(1)},
			{NodeID: "n2", HopDistance: hop(3)},
		}

		const beta, wHop, wImpact = 0.5, 0.5, 0.5
		ranked, err := rerank.Rerank(ctx, client, driver, "some query", candidates, 10, beta, wHop, wImpact)
		if err != nil {
			t.Fatalf("Rerank: %v", err)
		}
		if len(ranked) != 2 {
			t.Fatalf("len(ranked) = %d, want 2", len(ranked))
		}

		byID := map[string]rerank.RankedNode{}
		for _, r := range ranked {
			byID[r.Node.NodeID] = r
		}

		// rerank_score reale dal reranker-fake: deterministico (SHA-256 di
		// query+testo, dove il testo qui è il node_id — vedi candidateText
		// in rerank.go), non ricalcolato qui a mano (sarebbe una
		// re-implementazione dell'algoritmo del fake) — usato così com'è
		// letto dal risultato, che è comunque il PUNTO del test: la
		// formula si applica correttamente al punteggio REALE ricevuto dal
		// servizio reale.
		for id, impactScore := range map[string]float64{"n1": 0.9, "n2": 0.1} {
			r := byID[id]
			wantFinal := r.RerankScore + beta*(wHop*(1.0/(1.0+*candidateHop(candidates, id)))+wImpact*impactScore)
			if !floatsClose(r.FinalScore, wantFinal) {
				t.Errorf("%s: FinalScore = %v, want %v (formula ricalcolata dalle componenti reali)", id, r.FinalScore, wantFinal)
			}
			if !floatsClose(r.ImpactScoreNorm, impactScore) {
				t.Errorf("%s: ImpactScoreNorm = %v, want %v (letto da Neo4j reale)", id, r.ImpactScoreNorm, impactScore)
			}
		}
	})

	t.Run("Scenario4_MissingImpactScoreDefaultsToZeroWithRealNeo4j", func(t *testing.T) {
		seedNodeWithoutImpactScore(t, ctx, driver, "never-scored")
		candidates := []hybridsearch.RetrievedNode{{NodeID: "never-scored", HopDistance: hop(1)}}

		ranked, err := rerank.Rerank(ctx, client, driver, "query", candidates, 10, 0.5, 0.5, 0.5)
		if err != nil {
			t.Fatalf("Rerank: %v", err)
		}
		if len(ranked) != 1 || ranked[0].ImpactScoreNorm != 0.0 {
			t.Fatalf("ranked = %+v, want ImpactScoreNorm=0.0", ranked)
		}
	})

	t.Run("EdgeCase_RerankerUnreachableFailsExplicitly", func(t *testing.T) {
		unreachable := rerankclient.New("http://127.0.0.1:1")
		candidates := []hybridsearch.RetrievedNode{{NodeID: "n1", HopDistance: hop(1)}}

		_, err := rerank.Rerank(ctx, unreachable, driver, "query", candidates, 10, 0.5, 0.5, 0.5)
		if err == nil {
			t.Fatal("atteso errore esplicito con reranker irraggiungibile, ottenuto nil")
		}
	})
}

func candidateHop(candidates []hybridsearch.RetrievedNode, id string) *float64 {
	for _, c := range candidates {
		if c.NodeID == id {
			return c.HopDistance
		}
	}
	return nil
}

func floatsClose(a, b float64) bool {
	const eps = 1e-9
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < eps
}

func seedNodeWithImpactScore(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id string, score float64) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, "MERGE (n:CodeNode {id: $id}) SET n.impact_score = $score", map[string]any{"id": id, "score": score})
	if err != nil {
		t.Fatalf("seed nodo %s con impact_score: %v", id, err)
	}
}

func seedNodeWithoutImpactScore(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id string) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, "MERGE (n:CodeNode {id: $id})", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("seed nodo %s senza impact_score: %v", id, err)
	}
}

func startNeo4j(t *testing.T, ctx context.Context) neo4j.DriverWithContext {
	t.Helper()
	container, err := tcneo4j.Run(ctx, "neo4j:5-community", tcneo4j.WithAdminPassword(neo4jAdminPassword))
	if err != nil {
		t.Fatalf("avvio container neo4j: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container neo4j: %v", err)
		}
	})
	boltURL, err := container.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("BoltUrl: %v", err)
	}
	driver, err := neo4j.NewDriverWithContext(boltURL, neo4j.BasicAuth("neo4j", neo4jAdminPassword, ""))
	if err != nil {
		t.Fatalf("driver neo4j: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(ctx) })
	return driver
}

// ============================================================
// reranker-fake (sottoprocesso uvicorn reale) — stesso pattern di
// internal/hybridsearch/fixture_integration_test.go (embedder-fake, T4.1),
// replicato qui (modulo Python diverso, non importabile).
// ============================================================

func startRerankerFake(t *testing.T) string {
	t.Helper()
	fakesDir := repoPath(t, "fakes", "reranker-fake")
	uvicornBin := ensureRerankerFakeVenv(t, fakesDir)
	port := freePort(t)

	cmd := exec.Command(uvicornBin, "reranker_fake.main:app", "--port", fmt.Sprintf("%d", port))
	cmd.Dir = fakesDir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("StderrPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("avvio uvicorn (%s): %v", uvicornBin, err)
	}
	go drainPipe(stdout, "reranker-fake stdout")
	go drainPipe(stderr, "reranker-fake stderr")

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForReady(t, baseURL, 30*time.Second)

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return baseURL
}

func drainPipe(r io.Reader, label string) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			fmt.Fprintf(os.Stderr, "[%s] %s", label, buf[:n])
		}
		if err != nil {
			return
		}
	}
}

func ensureRerankerFakeVenv(t *testing.T, fakesDir string) string {
	t.Helper()
	venvDir := filepath.Join(fakesDir, ".venv")
	uvicornBin := filepath.Join(venvDir, "bin", "uvicorn")
	if _, err := os.Stat(uvicornBin); err == nil {
		return uvicornBin
	}

	if out, err := exec.Command("python3", "-m", "venv", venvDir).CombinedOutput(); err != nil {
		t.Fatalf("creazione venv (%s): %v\n%s", venvDir, err, out)
	}
	pip := filepath.Join(venvDir, "bin", "pip")
	libsPy := repoPath(t, "libs", "py")
	out, err := exec.Command(pip, "install", "-q", "-e", libsPy, "-e", fakesDir).CombinedOutput()
	if err != nil {
		t.Fatalf("pip install -e (libs/py, reranker-fake) fallito: %v\n%s", err, out)
	}
	return uvicornBin
}

func waitForReady(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	client := rerankclient.New(baseURL)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := client.Rerank(context.Background(), "readiness probe", []string{"x"})
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("reranker-fake non pronto su %s entro %v: %v", baseURL, timeout, lastErr)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind porta libera: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// services/retrieval-engine/internal/rerank/rerank_integration_test.go -> repo root 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
