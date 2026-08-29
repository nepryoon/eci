//go:build integration

// SPEC-042 §7 — verifica end-to-end della RPC streaming ImpactAnalysis via
// un client gRPC reale (stesso principio di server_integration_test.go,
// T1.4): scenario 1 (streaming genuino: gli eventi arrivano in ordine
// livello-per-livello via stream.Recv(), non un'unica raffica finale — la
// prova RIGOROSA di "non un batch mascherato" è il test unitario puro
// TestStreamImpact_EmitErrorStopsTraversalEarly_ProvesGenuineLevelByLevelStreaming
// in internal/impactanalysis; questo test verifica il WIRING gRPC reale) e
// scenario 2 (impact_kind per tipo di arco). File separato da
// server_integration_test.go/hybridsearch_dispatch_integration_test.go:
// harness Neo4j proprio, zero rischio di alterare gli scenari esistenti
// (stesso principio di isolamento già stabilito in T4.1).
package server_test

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/server"
)

const (
	impactSeedID         = "ia-srv-seed"
	impactL1CallsID      = "ia-srv-l1-calls"
	impactL1ImplementsID = "ia-srv-l1-implements"
	impactL2ID           = "ia-srv-l2"
)

func TestImpactAnalysisStreamingDispatch(t *testing.T) {
	ctx := authenticatedContext(context.Background(), "local")
	driver := startImpactNeo4j(t, ctx)
	seedImpactGraph(t, ctx, driver)
	client := startImpactServer(t, driver)

	t.Run("Scenarios1And2_StreamsLevelByLevelWithCorrectImpactKind", func(t *testing.T) {
		stream, err := client.ImpactAnalysis(ctx, &retrievalv1.ImpactAnalysisRequest{
			EntryNodeId: impactSeedID,
			MaxDepth:    2,
			MaxNodes:    100,
		})
		if err != nil {
			t.Fatalf("ImpactAnalysis: %v", err)
		}

		var events []*retrievalv1.ImpactAnalysisEvent
		for {
			ev, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("stream.Recv: %v", err)
			}
			events = append(events, ev)
		}
		if len(events) == 0 {
			t.Fatal("nessun evento ricevuto")
		}

		// Scenario 1 (streaming genuino via wire): il PRIMO evento ricevuto
		// è un nodo di hop 1 — gli eventi arrivano livello per livello, non
		// in un'unica raffica finale che potrebbe restituirli in qualunque
		// ordine.
		first := events[0]
		firstNode := first.GetNode()
		if firstNode == nil {
			t.Fatalf("primo evento = %+v, want un ImpactedNode (hop 1)", first)
		}
		if firstNode.GetDepth() != 1 {
			t.Errorf("primo evento: Depth = %d, want 1", firstNode.GetDepth())
		}

		// Nessun nodo di hop 2 deve precedere TUTTI i nodi/il progress di
		// hop 1: verifica diretta dell'ordinamento livello-per-livello.
		seenDepth1Progress := false
		for _, e := range events {
			if n := e.GetNode(); n != nil && n.GetDepth() == 2 && !seenDepth1Progress {
				t.Fatalf("nodo di hop 2 (%s) ricevuto prima del progress di hop 1 — streaming non livello-per-livello", n.GetNode().GetNodeId())
			}
			if p := e.GetProgress(); p != nil && p.GetCurrentDepth() == 1 {
				seenDepth1Progress = true
			}
		}
		if !seenDepth1Progress {
			t.Fatal("nessun ImpactProgress con current_depth=1 ricevuto")
		}

		// Scenario 2: impact_kind (qui path_edge_types[0]) riflette il tipo
		// d'arco dell'ultimo hop.
		byID := map[string]*retrievalv1.ImpactedNode{}
		for _, e := range events {
			if n := e.GetNode(); n != nil {
				byID[n.GetNode().GetNodeId()] = n
			}
		}
		callsNode, ok := byID[impactL1CallsID]
		if !ok || len(callsNode.GetPathEdgeTypes()) != 1 || callsNode.GetPathEdgeTypes()[0] != retrievalv1.EdgeType_EDGE_TYPE_CALLS {
			t.Errorf("%s: path_edge_types = %+v, want [EDGE_TYPE_CALLS]", impactL1CallsID, byID[impactL1CallsID].GetPathEdgeTypes())
		}
		implementsNode, ok := byID[impactL1ImplementsID]
		if !ok || len(implementsNode.GetPathEdgeTypes()) != 1 || implementsNode.GetPathEdgeTypes()[0] != retrievalv1.EdgeType_EDGE_TYPE_IMPLEMENTS {
			t.Errorf("%s: path_edge_types = %+v, want [EDGE_TYPE_IMPLEMENTS]", impactL1ImplementsID, byID[impactL1ImplementsID].GetPathEdgeTypes())
		}
		l2Node, ok := byID[impactL2ID]
		if !ok || l2Node.GetDepth() != 2 || len(l2Node.GetPathEdgeTypes()) != 1 || l2Node.GetPathEdgeTypes()[0] != retrievalv1.EdgeType_EDGE_TYPE_EXTENDS {
			t.Errorf("%s: %+v, want Depth=2 path_edge_types=[EDGE_TYPE_EXTENDS]", impactL2ID, l2Node)
		}
	})

	t.Run("EdgeCase_MaxNodesZeroFailsWithInvalidArgument", func(t *testing.T) {
		stream, err := client.ImpactAnalysis(ctx, &retrievalv1.ImpactAnalysisRequest{
			EntryNodeId: impactSeedID,
			MaxDepth:    1,
			MaxNodes:    0,
		})
		if err != nil {
			t.Fatalf("ImpactAnalysis (apertura stream): %v", err)
		}
		_, err = stream.Recv()
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("status = %v, want InvalidArgument", err)
		}
	})
}

func seedImpactGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		CREATE (seed:CodeNode {id: $seed_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (lc:CodeNode {id: $l1_calls_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (li:CodeNode {id: $l1_implements_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (l2:CodeNode {id: $l2_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (lc)-[:CALLS {weight: 1}]->(seed)
		CREATE (li)-[:IMPLEMENTS {weight: 1}]->(seed)
		CREATE (l2)-[:EXTENDS {weight: 1}]->(lc)
	`, map[string]any{
		"seed_id":          impactSeedID,
		"l1_calls_id":      impactL1CallsID,
		"l1_implements_id": impactL1ImplementsID,
		"l2_id":            impactL2ID,
	})
	if err != nil {
		t.Fatalf("seed grafo impact: %v", err)
	}
}

func startImpactNeo4j(t *testing.T, ctx context.Context) neo4j.DriverWithContext {
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

func startImpactServer(t *testing.T, driver neo4j.DriverWithContext) retrievalv1.RetrievalEngineClient {
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
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return retrievalv1.NewRetrievalEngineClient(conn)
}
