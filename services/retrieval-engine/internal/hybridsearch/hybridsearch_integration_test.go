//go:build integration

// Test di integrazione: Neo4j+Qdrant reali via testcontainers,
// embedder-fake reale (SPEC-041 §7) — scenari 1-7 di SPEC-041 §3 e gli
// edge case di §4. Esecuzione manuale (richiede Docker):
//
//	go test -tags=integration ./internal/hybridsearch/... -run TestHybridSearchIntegration -v
package hybridsearch_test

import (
	"context"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/qdrant/go-client/qdrant"

	"github.com/eci-project/eci/services/retrieval-engine/internal/embedclient"
	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
)

func TestHybridSearchIntegration(t *testing.T) {
	ctx := authenticatedContext(t, context.Background())
	f := setupFixture(t, ctx)
	deps := goodDeps(f)

	t.Run("Scenarios1to3_FusionEndToEnd", func(t *testing.T) {
		scenarios1to3FusionEndToEnd(t, ctx, f, deps)
	})
	t.Run("Scenario5_QdrantUnreachableDegradesGracefully", func(t *testing.T) {
		scenario5QdrantUnreachableDegradesGracefully(t, ctx, f)
	})
	t.Run("Scenario6_Neo4jUnreachableFailsExplicitly", func(t *testing.T) {
		scenario6Neo4jUnreachableFailsExplicitly(t, ctx, f)
	})
	t.Run("Scenario7_BothLegsEmptyFailsExplicitly", func(t *testing.T) {
		scenario7BothLegsEmptyFailsExplicitly(t, ctx, f, deps)
	})
	t.Run("EdgeCase_MaxDepthLessThanOneFailsExplicitly", func(t *testing.T) {
		edgeCaseMaxDepthLessThanOneFailsExplicitly(t, ctx, f, deps)
	})
	t.Run("EdgeCase_EmptyQueryOrEntryNodeIdFailsExplicitlyNoIO", func(t *testing.T) {
		edgeCaseEmptyQueryOrEntryNodeIDFailsExplicitlyNoIO(t, ctx, deps)
	})
	t.Run("EdgeCase_UnknownEntryNodeIdReturnsEmptyGraphNotError", func(t *testing.T) {
		edgeCaseUnknownEntryNodeIDReturnsEmptyGraphNotError(t, ctx, f)
	})
}

func goodDeps(f *fixtureHandles) hybridsearch.Deps {
	return hybridsearch.Deps{
		Driver:   f.neo4jDriver,
		Qdrant:   f.qdrantClient,
		Embedder: embedclient.New(f.embedderBaseURL),
	}
}

// Scenari 1-3 (SPEC-041 §3): un end-to-end reale (non solo unitario, già
// coperto in hybridsearch_test.go) contro Neo4j+Qdrant veri — verifica che
// il WIRING (VectorSearch + GraphTraversal + RRFFuse) produca la fusione
// attesa: callerDirect in ENTRAMBE le liste (scenario 1), callerIndirect
// SOLO nel grafo (scenario 2), vectorOnly SOLO nel vettoriale (scenario 3).
func scenarios1to3FusionEndToEnd(t *testing.T, ctx context.Context, f *fixtureHandles, deps hybridsearch.Deps) {
	ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps, f.queryText, f.seedID, 2)
	if err != nil {
		t.Fatalf("HybridGraphVectorSearch: %v", err)
	}

	byID := map[string]hybridsearch.RetrievedNode{}
	for _, n := range ranked {
		byID[n.NodeID] = n
	}
	if _, leaked := byID[f.foreignVectorID]; leaked {
		t.Fatalf("cross-tenant Qdrant point leaked: %+v", byID[f.foreignVectorID])
	}

	direct, ok := byID[f.callerDirectID]
	if !ok {
		t.Fatalf("callerDirect (%s) assente dal risultato: %+v", f.callerDirectID, ranked)
	}
	if direct.GraphRank == nil || direct.VectorRank == nil {
		t.Errorf("callerDirect atteso in ENTRAMBE le liste: GraphRank=%v VectorRank=%v", direct.GraphRank, direct.VectorRank)
	}

	indirect, ok := byID[f.callerIndirectID]
	if !ok {
		t.Fatalf("callerIndirect (%s) assente dal risultato: %+v", f.callerIndirectID, ranked)
	}
	if indirect.GraphRank == nil {
		t.Errorf("callerIndirect atteso con GraphRank non-nil")
	}
	if indirect.VectorRank != nil {
		t.Errorf("callerIndirect atteso SOLO nel grafo, VectorRank = %v", indirect.VectorRank)
	}

	vectorOnly, ok := byID[f.vectorOnlyID]
	if !ok {
		t.Fatalf("vectorOnly (%s) assente dal risultato: %+v", f.vectorOnlyID, ranked)
	}
	if vectorOnly.VectorRank == nil {
		t.Errorf("vectorOnly atteso con VectorRank non-nil")
	}
	if vectorOnly.GraphRank != nil {
		t.Errorf("vectorOnly atteso SOLO nel vettoriale, GraphRank = %v", vectorOnly.GraphRank)
	}

	// unrelated (esiste nel grafo ma fuori dal raggio di 2 hop da seed, e
	// nessun punto vettoriale corrispondente) non deve comparire.
	if _, ok := byID[f.unrelatedID]; ok {
		t.Errorf("unrelated (%s) non doveva comparire nel risultato fuso", f.unrelatedID)
	}

	// callerDirect (hop=1, in entrambe le liste) deve precedere
	// callerIndirect (hop=2, solo grafo) nell'ordinamento finale.
	var posDirect, posIndirect = -1, -1
	for i, n := range ranked {
		if n.NodeID == f.callerDirectID {
			posDirect = i
		}
		if n.NodeID == f.callerIndirectID {
			posIndirect = i
		}
	}
	if posDirect < 0 || posIndirect < 0 || posDirect >= posIndirect {
		t.Errorf("ordine atteso: callerDirect prima di callerIndirect, ottenuto posizioni %d/%d", posDirect, posIndirect)
	}
}

// Scenario 5 (SPEC-041 §3): Qdrant irraggiungibile durante la vector
// search -> HybridGraphVectorSearch NON fallisce, prosegue con la sola
// gamba grafo.
func scenario5QdrantUnreachableDegradesGracefully(t *testing.T, ctx context.Context, f *fixtureHandles) {
	unreachable, err := qdrant.NewClient(&qdrant.Config{Host: "127.0.0.1", Port: 1})
	if err != nil {
		// Client rifiuta già alla costruzione: comunque un fallimento della
		// gamba vettoriale, non testabile oltre in questo processo.
		t.Skipf("client Qdrant verso indirizzo irraggiungibile fallisce già alla costruzione: %v", err)
	}
	defer unreachable.Close()

	deps := hybridsearch.Deps{
		Driver:   f.neo4jDriver,
		Qdrant:   unreachable,
		Embedder: embedclient.New(f.embedderBaseURL),
	}

	ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps, f.queryText, f.seedID, 2)
	if err != nil {
		t.Fatalf("atteso nessun errore (degradazione), ottenuto: %v", err)
	}
	if len(ranked) == 0 {
		t.Fatalf("atteso risultato non vuoto dalla sola gamba grafo, ottenuto vuoto")
	}
	for _, n := range ranked {
		if n.VectorRank != nil {
			t.Errorf("nodo %s ha VectorRank=%v, atteso nil (gamba vettoriale degradata)", n.NodeID, *n.VectorRank)
		}
	}
}

// Scenario 6 (SPEC-041 §3): Neo4j irraggiungibile durante la graph
// traversal -> HybridGraphVectorSearch fallisce esplicitamente (gamba
// obbligatoria, non degrada).
func scenario6Neo4jUnreachableFailsExplicitly(t *testing.T, ctx context.Context, f *fixtureHandles) {
	badDriver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:1", neo4j.NoAuth())
	if err != nil {
		t.Fatalf("driver verso indirizzo non raggiungibile: %v", err)
	}
	defer badDriver.Close(ctx)

	deps := hybridsearch.Deps{
		Driver:   badDriver,
		Qdrant:   f.qdrantClient,
		Embedder: embedclient.New(f.embedderBaseURL),
	}

	_, err = hybridsearch.HybridGraphVectorSearch(ctx, deps, f.queryText, f.seedID, 2)
	if err == nil {
		t.Fatal("atteso errore esplicito con Neo4j irraggiungibile, ottenuto nil")
	}
}

// Scenario 7 (SPEC-041 §3): sia grafo sia vettoriale privi di risultati ->
// errore esplicito, non una lista vuota silenziosa. isolatedID non ha
// relazioni nel grafo; WithRepo su un repo inesistente azzera anche la
// gamba vettoriale (nessun punto fixture ha quel repo nel payload).
func scenario7BothLegsEmptyFailsExplicitly(t *testing.T, ctx context.Context, f *fixtureHandles, deps hybridsearch.Deps) {
	_, err := hybridsearch.HybridGraphVectorSearch(ctx, deps, f.queryText, f.isolatedID, 2,
		hybridsearch.WithRepo("repo-che-non-esiste"))
	if err == nil {
		t.Fatal("atteso errore esplicito con grafo e vettoriale entrambi vuoti, ottenuto nil")
	}
}

// Edge case §4: max_depth <= 0 -> errore esplicito prima di costruire la
// query Cypher.
func edgeCaseMaxDepthLessThanOneFailsExplicitly(t *testing.T, ctx context.Context, f *fixtureHandles, deps hybridsearch.Deps) {
	_, err := hybridsearch.HybridGraphVectorSearch(ctx, deps, f.queryText, f.seedID, 0)
	if err == nil {
		t.Fatal("atteso errore esplicito con max_depth=0, ottenuto nil")
	}

	_, err = hybridsearch.GraphTraversal(ctx, f.neo4jDriver, f.seedID, -1, nil, nil, 200)
	if err == nil {
		t.Fatal("atteso errore esplicito da GraphTraversal con max_depth=-1, ottenuto nil")
	}
}

// Edge case §4: query o entry_node_id vuoti -> errore esplicito immediato,
// nessuna chiamata a Qdrant/Neo4j. Verificato passando dipendenze rotte:
// se la validazione avvenisse DOPO un tentativo di I/O, l'errore
// risultante sarebbe un errore di connessione, non il messaggio di
// validazione — la differenza distingue i due casi.
func edgeCaseEmptyQueryOrEntryNodeIDFailsExplicitlyNoIO(t *testing.T, ctx context.Context, deps hybridsearch.Deps) {
	brokenDriver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:1", neo4j.NoAuth())
	if err != nil {
		t.Fatalf("driver verso indirizzo non raggiungibile: %v", err)
	}
	defer brokenDriver.Close(ctx)
	brokenQdrant, err := qdrant.NewClient(&qdrant.Config{Host: "127.0.0.1", Port: 1})
	if err != nil {
		t.Skipf("client Qdrant verso indirizzo irraggiungibile fallisce già alla costruzione: %v", err)
	}
	defer brokenQdrant.Close()

	brokenDeps := hybridsearch.Deps{Driver: brokenDriver, Qdrant: brokenQdrant, Embedder: deps.Embedder}

	_, err = hybridsearch.HybridGraphVectorSearch(ctx, brokenDeps, "", "some-entry-node", 2)
	if err == nil {
		t.Fatal("atteso errore esplicito con query vuota, ottenuto nil")
	}
	if !strings.Contains(err.Error(), "obbligatori") {
		t.Errorf("errore = %v, atteso un errore di validazione (nessuna chiamata I/O tentata)", err)
	}

	_, err = hybridsearch.HybridGraphVectorSearch(ctx, brokenDeps, "some query", "", 2)
	if err == nil {
		t.Fatal("atteso errore esplicito con entry_node_id vuoto, ottenuto nil")
	}
	if !strings.Contains(err.Error(), "obbligatori") {
		t.Errorf("errore = %v, atteso un errore di validazione (nessuna chiamata I/O tentata)", err)
	}
}

// Edge case §4: entry_node_id che non corrisponde a nessun nodo esistente
// -> GraphTraversal ritorna zero risultati (non un errore) — MATCH su un
// nodo inesistente in Cypher produce semplicemente nessun match.
func edgeCaseUnknownEntryNodeIDReturnsEmptyGraphNotError(t *testing.T, ctx context.Context, f *fixtureHandles) {
	nodes, err := hybridsearch.GraphTraversal(ctx, f.neo4jDriver, "does-not-exist-anywhere", 2, nil, nil, 200)
	if err != nil {
		t.Fatalf("atteso nessun errore per entry_node_id inesistente, ottenuto: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("nodes = %+v, want vuoto", nodes)
	}
}
