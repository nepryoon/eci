//go:build integration

// Harness condiviso per i test di integrazione di hybridsearch (SPEC-041
// §7): Neo4j+Qdrant reali via testcontainers (stesso pattern di
// SPEC-015/033), embedder-fake come sottoprocesso uvicorn reale (stesso
// pattern di embedding-worker, SPEC-030). seedFixture popola ENTRAMBI gli
// store con lo STESSO fixture usato sia dagli scenari 1-7
// (hybridsearch_integration_test.go) sia dallo scenario 8 di parità
// (parity_integration_test.go, che esegue la reference Python D5 come
// sottoprocesso reale contro lo stesso stato — SPEC-041 §7, "non un
// fixture JSON precalcolato").
package hybridsearch_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/qdrant/go-client/qdrant"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	tcqdrant "github.com/testcontainers/testcontainers-go/modules/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/embedclient"
)

const (
	neo4jAdminPassword = "eci-test-password-1234"
	collectionName     = "code_embeddings"
	vectorSize         = 1536
)

func authenticatedContext(t *testing.T, base context.Context) context.Context {
	t.Helper()
	sc := &retrievalv1.SecurityContext{
		TenantId: "tenant-test", UserId: "user-test",
		AllowedRepos: []string{"local"}, AclGroups: []string{"developers"},
	}
	encoded, err := proto.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}
	incoming := metadata.NewIncomingContext(base, metadata.Pairs("eci-security-context-bin", string(encoded)))
	var result context.Context
	_, err = secctx.UnaryServerInterceptor()(incoming, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		result = ctx
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// fixtureHandles porta tutto ciò che serve sia al test Go sia (scenario 8)
// al sottoprocesso Python: le stesse coordinate di connessione, gli stessi
// id, lo stesso query text.
type fixtureHandles struct {
	neo4jDriver  neo4j.DriverWithContext
	neo4jBoltURL string

	qdrantClient   *qdrant.Client
	qdrantHost     string
	qdrantGRPCPort int

	embedderBaseURL string

	// Id del grafo di test (SPEC-041 §7 fixture condiviso):
	//   unrelated  (nessuna relazione con seed)
	//   callerIndirect -[:CALLS]-> callerDirect -[:CALLS]-> seed
	seedID           string
	callerDirectID   string
	callerIndirectID string
	unrelatedID      string
	isolatedID       string // nodo isolato, nessuna relazione — scenario 7

	// Id dei punti vettoriali (payload node_id):
	//   callerDirect: vettore vicinissimo alla query -> presente in ENTRAMBE
	//     le liste (scenario 1).
	//   vectorOnly: vettore abbastanza vicino, ma nessun nodo grafo
	//     corrispondente -> solo vettoriale (scenario 3).
	//   unrelatedVector: embedding di un testo completamente diverso,
	//     lontano dalla query -> presente ma in fondo/fuori dal limite.
	vectorOnlyID string
	foreignVectorID string

	queryText string
}

func setupFixture(t *testing.T, ctx context.Context) *fixtureHandles {
	t.Helper()
	driver, boltURL := startNeo4j(t, ctx)
	qc, host, grpcPort := startQdrant(t, ctx)
	fakeBaseURL := startEmbedderFake(t)

	ensureCollection(t, ctx, qc)

	f := &fixtureHandles{
		neo4jDriver:      driver,
		neo4jBoltURL:     boltURL,
		qdrantClient:     qc,
		qdrantHost:       host,
		qdrantGRPCPort:   grpcPort,
		embedderBaseURL:  fakeBaseURL,
		seedID:           "hs-seed-node",
		callerDirectID:   "hs-caller-direct",
		callerIndirectID: "hs-caller-indirect",
		unrelatedID:      "hs-unrelated-graph-node",
		isolatedID:       "hs-isolated-node",
		vectorOnlyID:     "hs-vector-only-node",
		foreignVectorID:  "hs-foreign-vector-node",
		queryText:        "how does order processing call validation",
	}

	seedGraph(t, ctx, driver, f)
	seedVectors(t, ctx, fakeBaseURL, qc, f)

	return f
}

func seedGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, f *fixtureHandles) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.Run(ctx, `
		CREATE (seed:CodeNode {id: $seed_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'seed.go', name: 'Seed'})
		CREATE (direct:CodeNode {id: $caller_direct_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'direct.go', name: 'CallerDirect'})
		CREATE (indirect:CodeNode {id: $caller_indirect_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'indirect.go', name: 'CallerIndirect'})
		CREATE (unrelated:CodeNode {id: $unrelated_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'unrelated.go', name: 'Unrelated'})
		CREATE (isolated:CodeNode {id: $isolated_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'isolated.go', name: 'Isolated'})
		CREATE (:CodeNode {id: $vector_only_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'vector.go', name: 'VectorOnly'})
		CREATE (direct)-[:CALLS {weight: 1}]->(seed)
		CREATE (indirect)-[:CALLS {weight: 1}]->(direct)
	`, map[string]any{
		"seed_id":            f.seedID,
		"caller_direct_id":   f.callerDirectID,
		"caller_indirect_id": f.callerIndirectID,
		"unrelated_id":       f.unrelatedID,
		"isolated_id":        f.isolatedID,
		"vector_only_id":     f.vectorOnlyID,
	})
	if err != nil {
		t.Fatalf("seed del grafo di test: %v", err)
	}
}

// seedVectors: embedda queryText via embedder-fake (deterministico,
// SHA-256), poi scrive in Qdrant punti costruiti come piccole perturbazioni
// deterministiche di quel vettore (callerDirect/vectorOnly, "vicini" in
// cosine similarity) più un punto lontano (embedding reale di un testo
// non correlato) — nessun controllo diretto sui punteggi Qdrant, solo
// sull'ORDINE relativo, che la perturbazione garantisce deterministicamente.
func seedVectors(t *testing.T, ctx context.Context, embedderBaseURL string, qc *qdrant.Client, f *fixtureHandles) {
	t.Helper()
	embedder := embedclient.New(embedderBaseURL)

	qvec, err := embedder.Embed(ctx, f.queryText)
	if err != nil {
		t.Fatalf("embed query text: %v", err)
	}
	unrelatedVec, err := embedder.Embed(ctx, "a completely unrelated piece of legal contract text")
	if err != nil {
		t.Fatalf("embed unrelated text: %v", err)
	}

	callerDirectVec := perturb(qvec, 0.01, 1)
	vectorOnlyVec := perturb(qvec, 0.05, 2)

	points := []*qdrant.PointStruct{
		vectorPoint(t, f.callerDirectID, callerDirectVec),
		vectorPoint(t, f.vectorOnlyID, vectorOnlyVec),
		vectorPoint(t, "hs-unrelated-vector-node", unrelatedVec),
		vectorPointScoped(t, f.foreignVectorID, qvec, "tenant-b", "local", "developers"),
	}
	if _, err := qc.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collectionName,
		Points:         points,
	}); err != nil {
		t.Fatalf("upsert punti fixture: %v", err)
	}
}

func vectorPoint(t *testing.T, nodeID string, vec []float32) *qdrant.PointStruct {
	return vectorPointScoped(t, nodeID, vec, "tenant-test", "local", "developers")
}

func vectorPointScoped(t *testing.T, nodeID string, vec []float32, tenant, repo, group string) *qdrant.PointStruct {
	t.Helper()
	payload, err := qdrant.TryValueMap(map[string]any{
		"node_id": nodeID, "domain": "code",
		"tenant_id": tenant, "repo": repo, "acl_group": group,
	})
	if err != nil {
		t.Fatalf("costruzione payload per %s: %v", nodeID, err)
	}
	return &qdrant.PointStruct{
		Id:      qdrant.NewIDNum(stableUint64(nodeID)),
		Vectors: qdrant.NewVectors(vec...),
		Payload: payload,
	}
}

// stableUint64 deriva un id numerico Qdrant stabile dal node_id di test
// (Qdrant accetta sia UUID sia interi come point id — qui interi, più
// semplice di un UUIDv5 per un fixture di test locale).
func stableUint64(s string) uint64 {
	var h uint64 = 14695981039346656037 // FNV offset basis
	for _, c := range []byte(s) {
		h ^= uint64(c)
		h *= 1099511628211 // FNV prime
	}
	return h
}

func perturb(base []float32, scale float32, seed int64) []float32 {
	r := rand.New(rand.NewSource(seed))
	out := make([]float32, len(base))
	for i, v := range base {
		out[i] = v + scale*float32(r.NormFloat64())
	}
	return out
}

func ensureCollection(t *testing.T, ctx context.Context, qc *qdrant.Client) {
	t.Helper()
	exists, err := qc.CollectionExists(ctx, collectionName)
	if err != nil {
		t.Fatalf("CollectionExists: %v", err)
	}
	if exists {
		return
	}
	if err := qc.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     vectorSize,
			Distance: qdrant.Distance_Cosine,
		}),
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
}

// ============================================================
// Neo4j (testcontainers) — stesso pattern di server_integration_test.go.
// ============================================================

func startNeo4j(t *testing.T, ctx context.Context) (neo4j.DriverWithContext, string) {
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
	return driver, boltURL
}

// ============================================================
// Qdrant (testcontainers) — stesso pattern di sink-vector (SPEC-033).
// ============================================================

func startQdrant(t *testing.T, ctx context.Context) (*qdrant.Client, string, int) {
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
	return client, host, int(mappedPort.Num())
}

// ============================================================
// embedder-fake (sottoprocesso uvicorn reale) — stesso pattern di
// services/embedding-worker/internal/consumer/consumer_integration_test.go
// (SPEC-030), replicato qui (modulo Go diverso, non importabile).
// ============================================================

func startEmbedderFake(t *testing.T) string {
	t.Helper()
	fakesDir := repoPath(t, "fakes", "embedder-fake")
	uvicornBin := ensureEmbedderFakeVenv(t, fakesDir)
	port := freePort(t)

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
	go drainPipe(stdout, "embedder-fake stdout")
	go drainPipe(stderr, "embedder-fake stderr")

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForEmbedderReady(t, baseURL, 30*time.Second)

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

func ensureEmbedderFakeVenv(t *testing.T, fakesDir string) string {
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

func waitForEmbedderReady(t *testing.T, baseURL string, timeout time.Duration) {
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
	// services/retrieval-engine/internal/hybridsearch/fixture_integration_test.go -> repo root 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
