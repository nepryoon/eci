//go:build integration

// SPEC-044 §9, T4.4 — verifica il WIRING lato server per enable_rerank: la
// formula proximity_boost/final_score è già verificata a fondo (unitari +
// integrazione Neo4j+reranker-fake reali) in internal/rerank — questo file
// esercita SOLO il dispatch gRPC (enable_rerank=true attiva il reranking,
// enable_rerank=false lo salta, un reranker irraggiungibile fa fallire
// l'intera RPC). Riusa l'harness Neo4j/Qdrant/embedder-fake già definito in
// hybridsearch_dispatch_integration_test.go (stesso package server_test) —
// zero duplicazione di quella parte; solo il reranker-fake è nuovo qui.
package server_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/qdrant/go-client/qdrant"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/embedclient"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerankclient"
	"github.com/eci-project/eci/services/retrieval-engine/internal/server"
)

const (
	rerankDispatchSeedID   = "rrd-seed"
	rerankDispatchCallerID = "rrd-caller"
)

func TestEnableRerankDispatch(t *testing.T) {
	ctx := authenticatedContext(context.Background(), "local")
	driver := startDispatchNeo4j(t, ctx)
	qc := startDispatchQdrant(t, ctx)
	embedderBaseURL := startDispatchEmbedderFake(t)
	rerankerBaseURL := startRerankerFakeForDispatch(t)

	ensureDispatchCollection(t, ctx, qc)
	seedRerankDispatchGraph(t, ctx, driver)

	client := startServerWithRerank(t, driver, qc, embedclient.New(embedderBaseURL), rerankclient.New(rerankerBaseURL))
	clientNoReranker := startServerWithRerank(t, driver, qc, embedclient.New(embedderBaseURL), nil)

	t.Run("EnableRerankTrue_ActivatesRerankingAndPopulatesScores", func(t *testing.T) {
		resp, err := client.HybridSearch(ctx, &retrievalv1.HybridSearchRequest{
			QueryText:    "impact of caller on seed",
			EntryNodeId:  rerankDispatchSeedID,
			MaxDepth:     1,
			EnableRerank: true,
		})
		if err != nil {
			t.Fatalf("HybridSearch (enable_rerank=true): %v", err)
		}
		if len(resp.GetNodes()) == 0 {
			t.Fatal("nessun nodo ritornato")
		}
		for _, n := range resp.GetNodes() {
			if n.GetScores() == nil {
				t.Fatalf("nodo %s senza Scores", n.GetNodeId())
			}
			// rerank_score/final_score prodotti dal reranking reale — non
			// zero-value (il fake produce un punteggio deterministico ma
			// non-zero per qualunque testo non vuoto, con probabilità
			// trascurabile di essere esattamente 0.0).
			if n.GetScores().GetRerankScore() == 0 && n.GetScores().GetFinalScore() == 0 {
				t.Errorf("nodo %s: RerankScore/FinalScore entrambi zero, atteso reranking applicato", n.GetNodeId())
			}
		}
	})

	t.Run("EnableRerankFalse_UnchangedT41BehaviorNoRerankCall", func(t *testing.T) {
		resp, err := client.HybridSearch(ctx, &retrievalv1.HybridSearchRequest{
			QueryText:   "impact of caller on seed",
			EntryNodeId: rerankDispatchSeedID,
			MaxDepth:    1,
			// EnableRerank omesso: default false.
		})
		if err != nil {
			t.Fatalf("HybridSearch (enable_rerank=false): %v", err)
		}
		if len(resp.GetNodes()) == 0 {
			t.Fatal("nessun nodo ritornato")
		}
		for _, n := range resp.GetNodes() {
			if n.GetScores().GetRerankScore() != 0 {
				t.Errorf("nodo %s: RerankScore = %v, want 0 (reranking non richiesto)", n.GetNodeId(), n.GetScores().GetRerankScore())
			}
		}
	})

	t.Run("EnableRerankTrue_RerankerUnavailableFailsExplicitly", func(t *testing.T) {
		_, err := clientNoReranker.HybridSearch(ctx, &retrievalv1.HybridSearchRequest{
			QueryText:    "impact of caller on seed",
			EntryNodeId:  rerankDispatchSeedID,
			MaxDepth:     1,
			EnableRerank: true,
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("status = %v, want FailedPrecondition (Reranker non configurato)", err)
		}
	})
}

func seedRerankDispatchGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		CREATE (seed:CodeNode {id: $seed_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (caller:CodeNode {id: $caller_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (caller)-[:CALLS {weight: 1}]->(seed)
	`, map[string]any{"seed_id": rerankDispatchSeedID, "caller_id": rerankDispatchCallerID})
	if err != nil {
		t.Fatalf("seed grafo dispatch rerank: %v", err)
	}
}

func startServerWithRerank(t *testing.T, driver neo4j.DriverWithContext, qc *qdrant.Client, embedder *embedclient.Client, reranker *rerankclient.Client) retrievalv1.RetrievalEngineClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(secctx.UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	retrievalv1.RegisterRetrievalEngineServer(srv, &server.Server{Driver: driver, Qdrant: qc, Embedder: embedder, Reranker: reranker})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return retrievalv1.NewRetrievalEngineClient(conn)
}

// ============================================================
// reranker-fake (sottoprocesso uvicorn reale) dedicato a questo file —
// stesso principio di duplicazione già stabilito tra i vari harness di
// dispatch (T4.1/T4.2): ciascun file costruisce il proprio, nessuna
// dipendenza cross-file fragile.
// ============================================================

func startRerankerFakeForDispatch(t *testing.T) string {
	t.Helper()
	fakesDir := repoPath(t, "fakes", "reranker-fake")
	uvicornBin := ensureRerankerFakeVenvForDispatch(t, fakesDir)
	port := dispatchFreePort(t)

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
	go dispatchDrainPipe(stdout, "reranker-fake stdout")
	go dispatchDrainPipe(stderr, "reranker-fake stderr")

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForRerankerFakeReady(t, baseURL, 30*time.Second)

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return baseURL
}

func ensureRerankerFakeVenvForDispatch(t *testing.T, fakesDir string) string {
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

func waitForRerankerFakeReady(t *testing.T, baseURL string, timeout time.Duration) {
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
