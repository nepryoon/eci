//go:build integration

// Test di integrazione: Neo4j reale via testcontainers CON GDS attivato
// (SPEC-043 §7 — "nessun test unitario puro ha senso qui, l'intera
// pipeline è una sequenza di chiamate GDS reali, non logica isolabile in
// Go"). Esecuzione manuale (richiede Docker):
//
//	go test -tags=integration ./internal/gdsimpact/... -run TestGDSImpact -v
package gdsimpact_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"

	"github.com/eci-project/eci/tools/gds-impact/internal/gdsimpact"
)

const neo4jAdminPassword = "eci-test-password-1234"

// Fixture a catena (SPEC-043 §3 scenario 1: "tre nodi raggiungibili a hop
// diversi"): entry <- n1 (CALLS, hop1) <- n2 (IMPLEMENTS, hop2) <- n3
// (IMPORTS, hop3) — copre i tre impact_kind di scenario 2 in un colpo solo.
const (
	entryID = "gds-entry"
	n1ID    = "gds-n1-calls"
	n2ID    = "gds-n2-implements"
	n3ID    = "gds-n3-imports"
)

func TestGDSImpact(t *testing.T) {
	ctx := context.Background()

	t.Run("Scenarios1And2_FormulaMatchesWrittenValuesAndImpactKind", func(t *testing.T) {
		driver := startNeo4jWithGDS(t, ctx)
		seedChainFixture(t, ctx, driver)
		cfg := mustConfig(t, "--entry-node-id", entryID, "--max-depth", "4")

		var logs []string
		result, err := gdsimpact.Run(ctx, driver, cfg, testLogf(&logs), gdsimpact.Hooks{})
		if err != nil {
			t.Fatalf("Run: %v\nlog:\n%s", err, joinLogs(logs))
		}
		if len(result.Scores) != 3 {
			t.Fatalf("len(Scores) = %d, want 3\n%+v", len(result.Scores), result.Scores)
		}

		byID := map[string]gdsimpact.NodeScore{}
		for _, s := range result.Scores {
			byID[s.NodeID] = s
		}

		wantKind := map[string]string{n1ID: "behavioral", n2ID: "syntactic", n3ID: "module-boundary"}
		wantHop := map[string]int{n1ID: 1, n2ID: 2, n3ID: 3}

		for id, want := range wantKind {
			s, ok := byID[id]
			if !ok {
				t.Fatalf("%s assente da Scores: %+v", id, result.Scores)
			}
			if s.ImpactKind != want {
				t.Errorf("%s: ImpactKind = %q, want %q", id, s.ImpactKind, want)
			}
			if s.HopDistance != wantHop[id] {
				t.Errorf("%s: HopDistance = %d, want %d", id, s.HopDistance, wantHop[id])
			}

			// Scenario 1: la formula dichiarata (SPEC-043 §2 punto 6)
			// ricalcolata A MANO dalle componenti già note (PPRNorm/
			// BetweennessNorm/HopDistance, le stesse restituite da GDS)
			// deve combaciare col valore ImpactScore calcolato dal job —
			// ed entrambi devono combaciare con quanto REALMENTE scritto
			// su Neo4j (non solo "un valore è stato scritto").
			wantScore := cfg.WPPR*s.PPRNorm + cfg.WProx*(1.0/float64(s.HopDistance)) + cfg.WBC*s.BetweennessNorm
			if !floatsClose(s.ImpactScore, wantScore) {
				t.Errorf("%s: ImpactScore = %v, want %v (formula ricalcolata a mano)", id, s.ImpactScore, wantScore)
			}

			gotScore, gotCommunity, gotKind := readBackNode(t, ctx, driver, id)
			if !floatsClose(gotScore, s.ImpactScore) {
				t.Errorf("%s: n.impact_score su Neo4j = %v, want %v (Result.Scores)", id, gotScore, s.ImpactScore)
			}
			if gotCommunity != s.CommunityID {
				t.Errorf("%s: n.community_id su Neo4j = %v, want %v", id, gotCommunity, s.CommunityID)
			}
			if gotKind != s.ImpactKind {
				t.Errorf("%s: n.impact_kind su Neo4j = %q, want %q", id, gotKind, s.ImpactKind)
			}
		}

		// entry stesso non riceve MAI un impact_score (§2: la
		// normalizzazione/scrittura riguarda solo i dipendenti).
		assertNoProperty(t, ctx, driver, entryID, "impact_score")

		assertNoResidualProjections(t, ctx, driver)
	})

	t.Run("Scenario3_CleansUpProjectionOnMidPipelineFailure", func(t *testing.T) {
		driver := startNeo4jWithGDS(t, ctx)
		seedChainFixture(t, ctx, driver)
		cfg := mustConfig(t, "--entry-node-id", entryID, "--max-depth", "4")

		boom := errors.New("fallimento forzato dopo la proiezione, prima del write-back")
		hooks := gdsimpact.Hooks{AfterProject: func() error { return boom }}

		var logs []string
		_, err := gdsimpact.Run(ctx, driver, cfg, testLogf(&logs), hooks)
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want wrapping %v", err, boom)
		}

		assertNoResidualProjections(t, ctx, driver)
		// Nessuna scrittura parziale: il fallimento era PRIMA del
		// write-back, nessun nodo deve avere impact_score.
		assertNoProperty(t, ctx, driver, n1ID, "impact_score")
	})

	t.Run("Scenario4_UnknownEntryNodeNoErrorNoWrite", func(t *testing.T) {
		driver := startNeo4jWithGDS(t, ctx)
		seedChainFixture(t, ctx, driver)
		cfg := mustConfig(t, "--entry-node-id", "does-not-exist-anywhere", "--max-depth", "4")

		var logs []string
		result, err := gdsimpact.Run(ctx, driver, cfg, testLogf(&logs), gdsimpact.Hooks{})
		if err != nil {
			t.Fatalf("atteso nessun errore per entry_node_id inesistente, ottenuto: %v", err)
		}
		if len(result.Scores) != 0 {
			t.Errorf("Scores = %+v, want vuoto", result.Scores)
		}
		assertNoResidualProjections(t, ctx, driver)
		assertNoProperty(t, ctx, driver, n1ID, "impact_score")
	})

	t.Run("Scenario5_DefaultSamplingSizeEqualsProjectionNodeCount", func(t *testing.T) {
		driver := startNeo4jWithGDS(t, ctx)
		seedChainFixture(t, ctx, driver)
		cfg := mustConfig(t, "--entry-node-id", entryID, "--max-depth", "4") // --sampling-size omesso

		var logs []string
		result, err := gdsimpact.Run(ctx, driver, cfg, testLogf(&logs), gdsimpact.Hooks{})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		// Proiezione = entry + 3 dipendenti = 4 nodi.
		if result.BetweennessSamplingSize != 4 {
			t.Errorf("BetweennessSamplingSize = %d, want 4 (conteggio nodi della proiezione)", result.BetweennessSamplingSize)
		}
	})

	t.Run("EdgeCase_MaxDepthZeroFailsExplicitlyBeforeAnyQuery", func(t *testing.T) {
		driver := startNeo4jWithGDS(t, ctx)
		cfg := gdsimpact.Config{EntryNodeID: entryID, MaxDepth: 0, WPPR: 0.5, WProx: 0.3, WBC: 0.2}

		_, err := gdsimpact.Run(ctx, driver, cfg, func(string, ...any) {}, gdsimpact.Hooks{})
		if err == nil {
			t.Fatal("atteso errore esplicito con max_depth=0, ottenuto nil")
		}
	})

	t.Run("EdgeCase_Neo4jUnreachableFailsExplicitly", func(t *testing.T) {
		badDriver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:1", neo4j.NoAuth())
		if err != nil {
			t.Fatalf("driver verso indirizzo non raggiungibile: %v", err)
		}
		defer badDriver.Close(ctx)

		cfg := mustConfig(t, "--entry-node-id", entryID, "--max-depth", "4")
		_, err = gdsimpact.Run(ctx, badDriver, cfg, func(string, ...any) {}, gdsimpact.Hooks{})
		if err == nil {
			t.Fatal("atteso errore esplicito con Neo4j irraggiungibile, ottenuto nil")
		}
	})
}

func mustConfig(t *testing.T, args ...string) gdsimpact.Config {
	t.Helper()
	cfg, err := gdsimpact.ParseConfig(args)
	if err != nil {
		t.Fatalf("ParseConfig(%v): %v", args, err)
	}
	return cfg
}

func floatsClose(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func testLogf(logs *[]string) func(string, ...any) {
	return func(format string, args ...any) {
		*logs = append(*logs, fmt.Sprintf(format, args...))
	}
}

func joinLogs(logs []string) string {
	out := ""
	for _, l := range logs {
		out += l + "\n"
	}
	return out
}

func seedChainFixture(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		CREATE (entry:CodeNode {id: $entry_id, domain: 'code', repo: 'local'})
		CREATE (n1:CodeNode {id: $n1_id, domain: 'code', repo: 'local'})
		CREATE (n2:CodeNode {id: $n2_id, domain: 'code', repo: 'local'})
		CREATE (n3:CodeNode {id: $n3_id, domain: 'code', repo: 'local'})
		CREATE (n1)-[:CALLS {weight: 1}]->(entry)
		CREATE (n2)-[:IMPLEMENTS {weight: 1}]->(n1)
		CREATE (n3)-[:IMPORTS {weight: 1}]->(n2)
	`, map[string]any{"entry_id": entryID, "n1_id": n1ID, "n2_id": n2ID, "n3_id": n3ID})
	if err != nil {
		t.Fatalf("seed fixture a catena: %v", err)
	}
}

func readBackNode(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id string) (score float64, community int64, kind string) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, `MATCH (n:CodeNode {id: $id}) RETURN n.impact_score AS score, n.community_id AS community, n.impact_kind AS kind`, map[string]any{"id": id})
	if err != nil {
		t.Fatalf("lettura nodo %s: %v", id, err)
	}
	rec, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("lettura nodo %s (single): %v", id, err)
	}
	scoreVal, _ := rec.Get("score")
	communityVal, _ := rec.Get("community")
	kindVal, _ := rec.Get("kind")
	score, _ = scoreVal.(float64)
	community, _ = communityVal.(int64)
	kind, _ = kindVal.(string)
	return
}

func assertNoProperty(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id, prop string) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "MATCH (n:CodeNode {id: $id}) RETURN n[$prop] AS v", map[string]any{"id": id, "prop": prop})
	if err != nil {
		t.Fatalf("lettura proprietà %s su %s: %v", prop, id, err)
	}
	rec, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("lettura proprietà %s su %s (single): %v", prop, id, err)
	}
	v, _ := rec.Get("v")
	if v != nil {
		t.Errorf("%s.%s = %v, want assente (nil)", id, prop, v)
	}
}

func assertNoResidualProjections(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "CALL gds.graph.list() YIELD graphName RETURN graphName", nil)
	if err != nil {
		t.Fatalf("gds.graph.list: %v", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		t.Fatalf("gds.graph.list (collect): %v", err)
	}
	if len(records) != 0 {
		names := make([]string, 0, len(records))
		for _, r := range records {
			v, _ := r.Get("graphName")
			names = append(names, v.(string))
		}
		t.Errorf("proiezioni residue nel catalogo GDS: %v, want nessuna", names)
	}
}

func startNeo4jWithGDS(t *testing.T, ctx context.Context) neo4j.DriverWithContext {
	t.Helper()
	container, err := tcneo4j.Run(ctx, "neo4j:5-community",
		tcneo4j.WithAdminPassword(neo4jAdminPassword),
		tcneo4j.WithLabsPlugin(tcneo4j.GraphDataScience),
	)
	if err != nil {
		t.Fatalf("avvio container neo4j (con plugin GDS): %v", err)
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
