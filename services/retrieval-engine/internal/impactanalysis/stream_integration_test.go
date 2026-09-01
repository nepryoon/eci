//go:build integration

// Test di integrazione: Neo4j reale via testcontainers (SPEC-042 §7) —
// scenari 3/5 e l'edge case §4 "percorso più breve vince" con Cypher
// reale (non solo il fake fetcher dei test unitari). Esecuzione manuale:
//
//	go test -tags=integration ./internal/impactanalysis/... -run TestStreamImpactIntegration -v
package impactanalysis_test

import (
	"context"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/impactanalysis"
)

const neo4jAdminPassword = "eci-test-password-1234"

func TestStreamImpactIntegration(t *testing.T) {
	ctx := authenticatedContext(t, context.Background())
	driver := startNeo4j(t, ctx)
	seedFanOutGraph(t, ctx, driver)
	seedSharedMultiPathGraph(t, ctx, driver)

	t.Run("Scenario3_CapTruncatesWithRealGraph", func(t *testing.T) {
		var events []impactanalysis.ImpactEvent
		err := impactanalysis.StreamImpact(ctx, driver, "ia-fanout-seed", impactanalysis.Options{MaxDepth: 1, MaxNodes: 3, FanoutCap: 100, EdgeTypes: []string{"CALLS"}, Direction: "REVERSE"},
			func(e impactanalysis.ImpactEvent) error { events = append(events, e); return nil })
		if err != nil {
			t.Fatalf("StreamImpact: %v", err)
		}
		nodeCount, lastProgress := summarize(events)
		if nodeCount != 3 {
			t.Errorf("nodeCount = %d, want 3 (cap raggiunto su 5 raggiungibili)", nodeCount)
		}
		if lastProgress == nil || !lastProgress.Truncated {
			t.Errorf("lastProgress = %+v, want Truncated=true", lastProgress)
		}
	})

	t.Run("Scenario4_NoTruncationWhenUnderCap", func(t *testing.T) {
		var events []impactanalysis.ImpactEvent
		err := impactanalysis.StreamImpact(ctx, driver, "ia-fanout-seed", impactanalysis.Options{MaxDepth: 1, MaxNodes: 100, FanoutCap: 100, EdgeTypes: []string{"CALLS"}, Direction: "REVERSE"},
			func(e impactanalysis.ImpactEvent) error { events = append(events, e); return nil })
		if err != nil {
			t.Fatalf("StreamImpact: %v", err)
		}
		nodeCount, lastProgress := summarize(events)
		if nodeCount != 5 {
			t.Errorf("nodeCount = %d, want 5 (tutti i raggiungibili, nessun cap)", nodeCount)
		}
		if lastProgress == nil || lastProgress.Truncated {
			t.Errorf("lastProgress = %+v, want Truncated=false", lastProgress)
		}
	})

	t.Run("Scenario5_UnknownEntryNodeYieldsEmptyProgressNoError", func(t *testing.T) {
		var events []impactanalysis.ImpactEvent
		err := impactanalysis.StreamImpact(ctx, driver, "does-not-exist-anywhere", impactanalysis.Options{MaxDepth: 3, MaxNodes: 100, FanoutCap: 100, EdgeTypes: []string{"CALLS"}, Direction: "REVERSE"},
			func(e impactanalysis.ImpactEvent) error { events = append(events, e); return nil })
		if err != nil {
			t.Fatalf("atteso nessun errore per entry_node_id inesistente, ottenuto: %v", err)
		}
		nodeCount, lastProgress := summarize(events)
		if nodeCount != 0 {
			t.Errorf("nodeCount = %d, want 0", nodeCount)
		}
		if lastProgress == nil || lastProgress.NodesExplored != 0 || lastProgress.Truncated {
			t.Errorf("lastProgress = %+v, want NodesExplored=0 Truncated=false", lastProgress)
		}
	})

	t.Run("EdgeCase_ShortestPathWinsWithRealMultiPathGraph", func(t *testing.T) {
		var events []impactanalysis.ImpactEvent
		err := impactanalysis.StreamImpact(ctx, driver, "ia-shared-seed", impactanalysis.Options{MaxDepth: 3, MaxNodes: 100, FanoutCap: 100, EdgeTypes: []string{"CALLS", "IMPLEMENTS", "EXTENDS"}, Direction: "REVERSE"},
			func(e impactanalysis.ImpactEvent) error { events = append(events, e); return nil })
		if err != nil {
			t.Fatalf("StreamImpact: %v", err)
		}

		var sharedOccurrences []*impactanalysis.ImpactNode
		for _, e := range events {
			if e.Node != nil && e.Node.NodeID == "ia-shared-node" {
				sharedOccurrences = append(sharedOccurrences, e.Node)
			}
		}
		if len(sharedOccurrences) != 1 {
			t.Fatalf("ia-shared-node emesso %d volte, want 1", len(sharedOccurrences))
		}
		got := sharedOccurrences[0]
		if got.HopDistance != 1 || got.EdgeType != "CALLS" {
			t.Errorf("ia-shared-node = %+v, want HopDistance=1 EdgeType=CALLS (percorso diretto più breve, non il percorso a 2 hop via IMPLEMENTS)", got)
		}
	})
}

func authenticatedContext(t *testing.T, base context.Context) context.Context {
	t.Helper()
	encoded, err := proto.Marshal(&retrievalv1.SecurityContext{TenantId: "tenant-test", UserId: "user-test", AllowedRepos: []string{"local"}, AclGroups: []string{"developers"}})
	if err != nil {
		t.Fatal(err)
	}
	incoming := metadata.NewIncomingContext(base, metadata.Pairs("eci-security-context-bin", string(encoded)))
	var result context.Context
	_, err = secctx.UnaryServerInterceptor()(incoming, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) { result = ctx; return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func summarize(events []impactanalysis.ImpactEvent) (nodeCount int, lastProgress *impactanalysis.ImpactProgress) {
	for _, e := range events {
		if e.Node != nil {
			nodeCount++
		}
		if e.Progress != nil {
			lastProgress = e.Progress
		}
	}
	return
}

// seedFanOutGraph: 5 nodi che chiamano direttamente il seed (hop 1) —
// usato per gli scenari di cap (3/4).
func seedFanOutGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		CREATE (seed:CodeNode {id: 'ia-fanout-seed', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (a:CodeNode {id: 'ia-fanout-a', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (b:CodeNode {id: 'ia-fanout-b', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (c:CodeNode {id: 'ia-fanout-c', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (d:CodeNode {id: 'ia-fanout-d', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (e:CodeNode {id: 'ia-fanout-e', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (a)-[:CALLS {weight: 1}]->(seed)
		CREATE (b)-[:CALLS {weight: 1}]->(seed)
		CREATE (c)-[:CALLS {weight: 1}]->(seed)
		CREATE (d)-[:CALLS {weight: 1}]->(seed)
		CREATE (e)-[:CALLS {weight: 1}]->(seed)
	`, nil)
	if err != nil {
		t.Fatalf("seed fan-out graph: %v", err)
	}
}

// seedSharedMultiPathGraph: "ia-shared-node" raggiungibile da
// "ia-shared-seed" via DUE percorsi di lunghezza diversa — diretto (1 hop,
// CALLS) e via "ia-shared-intermediate" (2 hop, IMPLEMENTS sull'ultimo
// hop) — il percorso più corto deve vincere.
func seedSharedMultiPathGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		CREATE (seed:CodeNode {id: 'ia-shared-seed', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (mid:CodeNode {id: 'ia-shared-intermediate', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (shared:CodeNode {id: 'ia-shared-node', domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers'})
		CREATE (mid)-[:EXTENDS {weight: 1}]->(seed)
		CREATE (shared)-[:CALLS {weight: 1}]->(seed)
		CREATE (shared)-[:IMPLEMENTS {weight: 1}]->(mid)
	`, nil)
	if err != nil {
		t.Fatalf("seed shared multi-path graph: %v", err)
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
