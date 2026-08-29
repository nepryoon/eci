//go:build integration

// SPEC-041 §9, T4.1 — verifica il WIRING lato server (dispatch in
// HybridSearch + retrievedNodeFromHybridSearch + main.go equivalente) per
// il percorso con entry_node_id: gli scenari 1-8 dell'algoritmo stesso
// sono già coperti a fondo (unitari + integrazione + parità Python) in
// internal/hybridsearch — questo file esercita SOLO il collegamento gRPC
// (dispatch, mapping proto, Server con Qdrant/Embedder reali) che nessun
// altro test tocca. File separato da server_integration_test.go (T1.4):
// zero rischio di alterare gli scenari 1-7 esistenti lì, stesso principio
// di isolamento già richiesto da SPEC-041 §9 ("nessuna regressione").
package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/qdrant/go-client/qdrant"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	tcqdrant "github.com/testcontainers/testcontainers-go/modules/qdrant"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/embedclient"
	"github.com/eci-project/eci/services/retrieval-engine/internal/server"
)

const (
	dispatchCollection = "code_embeddings"
	dispatchSeedID     = "hsd-seed"
	dispatchCallerID   = "hsd-caller"
)

func TestHybridSearchGraphVectorDispatch(t *testing.T) {
	ctx := authenticatedContext(context.Background(), "local")
	driver := startDispatchNeo4j(t, ctx)
	qc := startDispatchQdrant(t, ctx)
	embedderBaseURL := startDispatchEmbedderFake(t)

	// code_fulltext (D3, SPEC-004): richiesto dal percorso legacy T1.4
	// (EntryNodeIdEmpty_UsesLegacyFullTextPath sotto), stesso schema
	// applicato da server_integration_test.go.
	applyNeo4jSchema(t, ctx, driver)
	seedDispatchGraph(t, ctx, driver)
	ensureDispatchCollection(t, ctx, qc)

	client := startServerWithHybrid(t, driver, qc, embedclient.New(embedderBaseURL))

	t.Run("EntryNodeIdSet_UsesGraphVectorPath", func(t *testing.T) {
		resp, err := client.HybridSearch(ctx, &retrievalv1.HybridSearchRequest{
			QueryText:   "impact of caller on seed",
			EntryNodeId: dispatchSeedID,
			MaxDepth:    1,
		})
		if err != nil {
			t.Fatalf("HybridSearch (entry_node_id impostato): %v", err)
		}
		var found bool
		for _, n := range resp.GetNodes() {
			if n.GetNodeId() == dispatchCallerID {
				found = true
				if n.GetScores() == nil {
					t.Errorf("Scores assenti per %s, atteso RrfScore/FinalScore popolati", dispatchCallerID)
				}
			}
		}
		if !found {
			t.Errorf("%s (chiamante diretto di seed) non trovato: %+v", dispatchCallerID, resp.GetNodes())
		}
		if resp.GetGraphCandidates() == 0 {
			t.Errorf("GraphCandidates = 0, want > 0 (traversata grafo dovrebbe aver trovato %s)", dispatchCallerID)
		}
	})

	t.Run("EntryNodeIdEmpty_UsesLegacyFullTextPath", func(t *testing.T) {
		// Stesso Server (Qdrant/Embedder configurati), ma senza
		// entry_node_id: deve ricevere ESATTAMENTE il comportamento T1.4
		// (vector_leg_degraded sempre true), non il nuovo percorso.
		resp, err := client.HybridSearch(ctx, &retrievalv1.HybridSearchRequest{QueryText: "Seed"})
		if err != nil {
			t.Fatalf("HybridSearch (entry_node_id vuoto): %v", err)
		}
		if !resp.GetVectorLegDegraded() {
			t.Errorf("VectorLegDegraded = false, want true (percorso legacy T1.4, entry_node_id assente)")
		}
	})
}

func seedDispatchGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		CREATE (seed:CodeNode {id: $seed_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'seed.go', name: 'Seed'})
		CREATE (caller:CodeNode {id: $caller_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'caller.go', name: 'Caller'})
		CREATE (caller)-[:CALLS {weight: 1}]->(seed)
	`, map[string]any{"seed_id": dispatchSeedID, "caller_id": dispatchCallerID})
	if err != nil {
		t.Fatalf("seed grafo dispatch: %v", err)
	}
}

func ensureDispatchCollection(t *testing.T, ctx context.Context, qc *qdrant.Client) {
	t.Helper()
	exists, err := qc.CollectionExists(ctx, dispatchCollection)
	if err != nil {
		t.Fatalf("CollectionExists: %v", err)
	}
	if exists {
		return
	}
	if err := qc.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: dispatchCollection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     1536,
			Distance: qdrant.Distance_Cosine,
		}),
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
}

func startServerWithHybrid(t *testing.T, driver neo4j.DriverWithContext, qc *qdrant.Client, embedder *embedclient.Client) retrievalv1.RetrievalEngineClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(secctx.UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	retrievalv1.RegisterRetrievalEngineServer(srv, &server.Server{Driver: driver, Qdrant: qc, Embedder: embedder})
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
// Harness Neo4j/Qdrant/embedder-fake dedicato a questo file (stesso
// principio di duplicazione già stabilito tra sink-vector/embedding-worker:
// ciascun file di test costruisce il proprio harness, nessuna dipendenza
// cross-file fragile).
// ============================================================

func startDispatchNeo4j(t *testing.T, ctx context.Context) neo4j.DriverWithContext {
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

func startDispatchQdrant(t *testing.T, ctx context.Context) *qdrant.Client {
	t.Helper()
	container, err := tcqdrant.Run(ctx, "qdrant/qdrant:v1.15.1")
	if err != nil {
		t.Fatalf("avvio container qdrant: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container qdrant: %v", err)
		}
	})
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("qdrant Host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, "6334/tcp")
	if err != nil {
		t.Fatalf("qdrant MappedPort 6334/tcp: %v", err)
	}
	client, err := qdrant.NewClient(&qdrant.Config{Host: host, Port: int(mappedPort.Num())})
	if err != nil {
		t.Fatalf("qdrant NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func startDispatchEmbedderFake(t *testing.T) string {
	t.Helper()
	fakesDir := repoPath(t, "fakes", "embedder-fake")
	uvicornBin := ensureDispatchEmbedderFakeVenv(t, fakesDir)
	port := dispatchFreePort(t)

	cmd := exec.Command(uvicornBin, "embedder_fake.main:app", "--port", fmt.Sprintf("%d", port))
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
	go dispatchDrainPipe(stdout, "embedder-fake stdout")
	go dispatchDrainPipe(stderr, "embedder-fake stderr")

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	dispatchWaitForReady(t, baseURL, 30*time.Second)

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return baseURL
}

func dispatchDrainPipe(r io.Reader, label string) {
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

func ensureDispatchEmbedderFakeVenv(t *testing.T, fakesDir string) string {
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
		t.Fatalf("pip install -e (libs/py, embedder-fake) fallito: %v\n%s", err, out)
	}
	return uvicornBin
}

func dispatchWaitForReady(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	client := embedclient.New(baseURL)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := client.Embed(context.Background(), "readiness probe")
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("embedder-fake non pronto su %s entro %v: %v", baseURL, timeout, lastErr)
}

func dispatchFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind porta libera: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
