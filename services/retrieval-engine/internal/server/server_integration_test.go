//go:build integration

// SPEC-016 §7 — test di integrazione: testcontainers Neo4j (stesso
// pattern di SPEC-015), dati di setup inseriti direttamente via Cypher
// (non serve l'intera pipeline T1.1-T1.3 per testare SOLO il server di
// lettura). Il server gRPC è istanziato per davvero (stessi interceptor
// di produzione, main.go), un client gRPC reale gli parla via TCP
// loopback — non una chiamata diretta ai metodi Go, per esercitare anche
// la codifica/decodifica protobuf e gli interceptor.
package server_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/server"
)

const neo4jAdminPassword = "eci-test-password-1234"

// Id fissi del grafo di test, stessa forma prodotta da sink-graph
// (SPEC-015): labels :CodeNode + label specifica, id/domain/name/ast_hash/
// repo/path (+ symbol_id sui Method) — non un fixture ad-hoc arbitrario,
// rispecchia esattamente cosa il consumer scriverebbe per order_service.go
// (SPEC-014 fixture) più una Function per lo scenario 5.
const (
	fileID     = "test-file-order-service"
	classID    = "test-class-order-service"
	processID  = "test-method-process"
	validateID = "test-method-validate"
	computeID  = "test-func-compute-total"
	foreignID  = "test-foreign-tenant-node"
)

func TestRetrievalEngineServer(t *testing.T) {
	ctx := authenticatedContext(context.Background(), "local")
	driver := startNeo4j(t, ctx)
	seedGraph(t, ctx, driver)
	client := startServer(t, driver)

	t.Run("Scenario1_GetNodeFound", func(t *testing.T) {
		resp, err := client.GetNode(ctx, &retrievalv1.GetNodeRequest{NodeId: processID})
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		node := resp.GetNode()
		if node.GetNodeType() != "Method" {
			t.Errorf("NodeType = %q, want %q", node.GetNodeType(), "Method")
		}
		if node.GetName() != "Process" {
			t.Errorf("Name = %q, want %q", node.GetName(), "Process")
		}
		if node.GetAstHash() != "hash-process" {
			t.Errorf("AstHash = %q, want %q", node.GetAstHash(), "hash-process")
		}
		if node.GetProvenance().GetRepo() != "local" {
			t.Errorf("Provenance.Repo = %q, want %q", node.GetProvenance().GetRepo(), "local")
		}
		if node.GetProvenance().GetPath() != "order_service.go" {
			t.Errorf("Provenance.Path = %q, want %q", node.GetProvenance().GetPath(), "order_service.go")
		}
		// Campi mai popolati da questa pipeline: zero-value proto, non
		// valori fabbricati (SPEC-016 §2/§3 scenario 1).
		if node.GetSignature() != "" || node.GetSummary() != "" || node.GetSourceText() != "" {
			t.Errorf("signature/summary/source_text non sono zero-value: %+v", node)
		}
		if node.GetScores() != nil {
			t.Errorf("Scores = %+v, want nil (zero-value proto)", node.GetScores())
		}
	})

	t.Run("Scenario2_GetNodeNotFound", func(t *testing.T) {
		_, err := client.GetNode(ctx, &retrievalv1.GetNodeRequest{NodeId: "does-not-exist"})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("status = %v, want NotFound", err)
		}
	})

	t.Run("Scenario3_ExpandNeighbors", func(t *testing.T) {
		resp, err := client.ExpandNeighbors(ctx, &retrievalv1.ExpandNeighborsRequest{
			NodeId:    processID,
			Direction: retrievalv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED,
		})
		if err != nil {
			t.Fatalf("ExpandNeighbors: %v", err)
		}
		var foundValidate bool
		for _, n := range resp.GetNeighbors() {
			if n.GetNodeId() == foreignID {
				t.Fatalf("cross-tenant node leaked through traversal: %+v", n)
			}
			if n.GetNodeId() == validateID {
				foundValidate = true
			}
		}
		if !foundValidate {
			t.Errorf("Validate (id=%s) non trovato tra i vicini: %+v", validateID, resp.GetNeighbors())
		}
		var hasCalls bool
		for _, et := range resp.GetTraversedEdgeTypes() {
			if et == retrievalv1.EdgeType_EDGE_TYPE_CALLS {
				hasCalls = true
			}
		}
		if !hasCalls {
			t.Errorf("TraversedEdgeTypes = %v, want include EDGE_TYPE_CALLS", resp.GetTraversedEdgeTypes())
		}
	})

	t.Run("T6_3_CrossTenantGetIsIndistinguishableFromMissing", func(t *testing.T) {
		_, err := client.GetNode(ctx, &retrievalv1.GetNodeRequest{NodeId: foreignID})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("status=%v, want NotFound", err)
		}
	})

	t.Run("Scenario4_HybridSearchFindsByName", func(t *testing.T) {
		resp, err := client.HybridSearch(ctx, &retrievalv1.HybridSearchRequest{QueryText: "Validate"})
		if err != nil {
			t.Fatalf("HybridSearch: %v", err)
		}
		var found bool
		for _, n := range resp.GetNodes() {
			if n.GetNodeId() == validateID {
				found = true
			}
		}
		if !found {
			t.Errorf("Validate (id=%s) non trovato in HybridSearch(%q): %+v", validateID, "Validate", resp.GetNodes())
		}
		if resp.GetVectorCandidates() != 0 {
			t.Errorf("VectorCandidates = %d, want 0", resp.GetVectorCandidates())
		}
		if !resp.GetVectorLegDegraded() {
			t.Errorf("VectorLegDegraded = false, want true")
		}
	})

	t.Run("Scenario5_HybridSearchMissesInnerContent", func(t *testing.T) {
		// "return prices" è un frammento plausibile del CORPO di
		// computeTotal, mai presente nel suo NOME ("computeTotal") — con
		// signature/source_text non ancora popolati, l'indice full-text
		// non ha nient'altro su cui fare match: verifica esplicita del
		// limite dichiarato in SPEC-016 §2, non solo teorica.
		resp, err := client.HybridSearch(ctx, &retrievalv1.HybridSearchRequest{QueryText: "return prices"})
		if err != nil {
			t.Fatalf("HybridSearch: %v", err)
		}
		if len(resp.GetNodes()) != 0 {
			t.Errorf("Nodes = %+v, want vuoto (nessun match sul contenuto interno, solo su name)", resp.GetNodes())
		}
		if resp.GetGraphCandidates() != 0 {
			t.Errorf("GraphCandidates = %d, want 0", resp.GetGraphCandidates())
		}
	})

	t.Run("Scenario6_AuthenticatedScopeCannotBeExpandedByBody", func(t *testing.T) {
		mdCtx := authenticatedContext(context.Background(), "some-other-repo-not-local")
		forgedBody := &retrievalv1.SecurityContext{
			TenantId: "tenant-test", UserId: "user-test",
			AllowedRepos: []string{"local"}, AclGroups: []string{"developers"},
		}

		_, err := client.GetNode(mdCtx, &retrievalv1.GetNodeRequest{
			NodeId:          processID,
			SecurityContext: forgedBody,
		})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("status=%v, want NotFound senza rivelare il nodo fuori scope", err)
		}
	})

	t.Run("Scenario7_HealthNeo4jUnreachable", func(t *testing.T) {
		// Driver dedicato verso un indirizzo che rifiuta la connessione
		// (nessun listener) — non lo stesso Neo4j reale usato sopra.
		badDriver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:1", neo4j.NoAuth())
		if err != nil {
			t.Fatalf("driver verso indirizzo non raggiungibile: %v", err)
		}
		defer badDriver.Close(ctx)
		badClient := startServer(t, badDriver)

		healthCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		resp, err := badClient.Health(healthCtx, &retrievalv1.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("Health: atteso una risposta strutturata, non un errore gRPC: %v", err)
		}
		if resp.GetStatus() != retrievalv1.HealthCheckResponse_SERVING_STATUS_NOT_SERVING {
			t.Errorf("Status = %v, want SERVING_STATUS_NOT_SERVING", resp.GetStatus())
		}
		if resp.GetGraphLegHealthy() {
			t.Errorf("GraphLegHealthy = true, want false")
		}
		if resp.GetVectorLegHealthy() {
			t.Errorf("VectorLegHealthy = true, want false")
		}
		if resp.GetFulltextLegHealthy() {
			t.Errorf("FulltextLegHealthy = true, want false")
		}
	})
}

func authenticatedContext(ctx context.Context, repo string) context.Context {
	sc := &retrievalv1.SecurityContext{
		TenantId: "tenant-test", UserId: "user-test",
		AllowedRepos: []string{repo}, AclGroups: []string{"developers"},
	}
	data, err := proto.Marshal(sc)
	if err != nil {
		panic(err)
	}
	return metadata.AppendToOutgoingContext(ctx, "eci-security-context-bin", string(data))
}

// ============================================================
// Harness.
// ============================================================

func startNeo4j(t *testing.T, ctx context.Context) neo4j.DriverWithContext {
	t.Helper()
	container, err := tcneo4j.Run(ctx, "neo4j:5-community", tcneo4j.WithAdminPassword(neo4jAdminPassword))
	if err != nil {
		t.Fatalf("avvio container neo4j:5-community: %v", err)
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

	applyNeo4jSchema(t, ctx, driver)
	return driver
}

// applyNeo4jSchema applica contracts/cypher/schema.cypher (D3, SPEC-004):
// l'indice full-text code_fulltext usato dagli scenari 4/5 esiste solo
// se lo schema è applicato, esattamente come lo stack reale.
func applyNeo4jSchema(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	src := readRepoFile(t, "contracts", "cypher", "schema.cypher")
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	for _, stmt := range splitCypherStatements(src) {
		if _, err := session.Run(ctx, stmt, nil); err != nil {
			t.Fatalf("applicazione schema.cypher, statement %q: %v", firstLine(stmt), err)
		}
	}
}

// seedGraph inserisce direttamente via Cypher lo stesso grafo che
// sink-graph (SPEC-015) avrebbe prodotto per order_service.go (SPEC-009
// fixture) più una Function per lo scenario 5 — SPEC-016 §7: "non serve
// l'intera pipeline T1.1-T1.3 per testare SOLO il server di lettura".
func seedGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.Run(ctx, `
		CREATE (f:CodeNode:File {id: $file_id, domain: 'code', name: 'order_service.go', ast_hash: 'hash-file', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'order_service.go'})
		CREATE (c:CodeNode:Class {id: $class_id, domain: 'code', name: 'OrderService', ast_hash: 'hash-class', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'order_service.go'})
		CREATE (p:CodeNode:Method {id: $process_id, domain: 'code', name: 'Process', ast_hash: 'hash-process', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'order_service.go', symbol_id: $process_id})
		CREATE (v:CodeNode:Method {id: $validate_id, domain: 'code', name: 'Validate', ast_hash: 'hash-validate', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'order_service.go', symbol_id: $validate_id})
		CREATE (ct:CodeNode:Function {id: $compute_id, domain: 'code', name: 'computeTotal', ast_hash: 'hash-compute', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', path: 'util.go'})
		CREATE (foreign:CodeNode:Method {id: $foreign_id, domain: 'code', name: 'ForeignSecret', ast_hash: 'hash-foreign', tenant_id: 'tenant-b', repo: 'local', acl_group: 'developers', path: 'secret.go', symbol_id: $foreign_id})
		CREATE (f)-[:CONTAINS {weight: 1}]->(c)
		CREATE (f)-[:CONTAINS {weight: 1}]->(p)
		CREATE (f)-[:CONTAINS {weight: 1}]->(v)
		CREATE (p)-[:CALLS {weight: 1}]->(v)
		CREATE (foreign)-[:CALLS {weight: 1}]->(p)
	`, map[string]any{
		"file_id":     fileID,
		"class_id":    classID,
		"process_id":  processID,
		"validate_id": validateID,
		"compute_id":  computeID,
		"foreign_id":  foreignID,
	})
	if err != nil {
		t.Fatalf("seed del grafo di test: %v", err)
	}
}

func startServer(t *testing.T, driver neo4j.DriverWithContext) retrievalv1.RetrievalEngineClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(secctx.UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	retrievalv1.RegisterRetrievalEngineServer(srv, &server.Server{Driver: driver})

	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return retrievalv1.NewRetrievalEngineClient(conn)
}

// ============================================================
// Utility condivise (stesso pattern minimale di SPEC-015 §10 nota 9 —
// tools/migrate-neo4j/internal/migrate.ParseStatements è in un modulo Go
// diverso, non importabile qui senza una dipendenza cross-modulo).
// ============================================================

func splitCypherStatements(src string) []string {
	var kept []string
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		kept = append(kept, line)
	}
	joined := strings.Join(kept, "\n")

	var statements []string
	for _, raw := range strings.Split(joined, ";") {
		stmt := strings.TrimSpace(raw)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := repoPath(t, parts...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lettura %s: %v", path, err)
	}
	return string(data)
}

func repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// services/retrieval-engine/internal/server/server_integration_test.go -> repo root 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
