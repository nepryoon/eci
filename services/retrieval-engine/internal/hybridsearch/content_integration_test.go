//go:build integration

// Test di integrazione: Neo4j+Qdrant+OpenSearch reali via testcontainers,
// TUTTI E TRE insieme per la prima volta (SPEC-045 §7), più embedder-fake
// reale (per l'embedding della query, T4.1). Scritti PRIMA
// dell'implementazione (TDD) — coprono SPEC-045 §3 scenari 1-5 e §4 edge
// case, via la funzione pubblica HybridGraphVectorSearch (non solo gli
// helper interni).
package hybridsearch_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	"github.com/qdrant/go-client/qdrant"
	tcopensearch "github.com/testcontainers/testcontainers-go/modules/opensearch"

	"github.com/eci-project/eci/services/retrieval-engine/internal/embedclient"
	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
)

const (
	contentSeedID          = "ch-seed"
	contentGraphDepID      = "ch-graph-dep"
	contentNoChunksID      = "ch-no-chunks"
	contentIsolatedEntryID = "ch-isolated-entry"
	contentVectorOnly1ID   = "ch-vector-only-1"
	contentVectorOnly2ID   = "ch-vector-only-2"
	contentQueryText       = "content hydration test query"
	codeChunksIndex        = "code_chunks" // SPEC-034, riusato in sola lettura qui
)

func TestHybridGraphVectorSearchContentHydration(t *testing.T) {
	ctx := authenticatedContext(t, context.Background())
	driver, callCount := startCountingNeo4j(t, ctx)
	qc, _, _ := startQdrant(t, ctx)
	embedderBaseURL := startEmbedderFake(t)
	osClient := startOpenSearchReal(t, ctx)

	ensureCollection(t, ctx, qc)
	seedContentGraph(t, ctx, driver)
	seedContentVectors(t, ctx, embedderBaseURL, qc)
	seedContentChunks(t, ctx, osClient)

	deps := func(includeOS *opensearchapi.Client) hybridsearch.Deps {
		return hybridsearch.Deps{
			Driver:     driver,
			Qdrant:     qc,
			Embedder:   embedclient.New(embedderBaseURL),
			OpenSearch: includeOS,
		}
	}

	t.Run("Scenario1_NameFromGraphLeg", func(t *testing.T) {
		ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps(osClient), contentQueryText, contentSeedID, 2)
		if err != nil {
			t.Fatalf("HybridGraphVectorSearch: %v", err)
		}
		got := findByID(t, ranked, contentGraphDepID)
		if got.Name != "GraphDep" {
			t.Errorf("Name = %q, want %q", got.Name, "GraphDep")
		}
	})

	t.Run("Scenario2_NameFromVectorLegViaBatchLookup", func(t *testing.T) {
		before := atomic.LoadInt32(callCount)
		ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps(osClient), contentQueryText, contentIsolatedEntryID, 2)
		if err != nil {
			t.Fatalf("HybridGraphVectorSearch: %v", err)
		}
		after := atomic.LoadInt32(callCount)

		n1 := findByID(t, ranked, contentVectorOnly1ID)
		n2 := findByID(t, ranked, contentVectorOnly2ID)
		if n1.Name != "VectorOnly1" {
			t.Errorf("%s: Name = %q, want %q", contentVectorOnly1ID, n1.Name, "VectorOnly1")
		}
		if n2.Name != "VectorOnly2" {
			t.Errorf("%s: Name = %q, want %q", contentVectorOnly2ID, n2.Name, "VectorOnly2")
		}

		// Prova diretta di "una query batch, non N separate" (SPEC-045 §3
		// scenario 2): entry_node_id isolato -> GraphTraversal fa UNA
		// query (risultato vuoto); il re-check ACL T6.3 fa UNA query batch;
		// DUE nodi vector-only mancano di Name -> se l'hydration fosse una
		// query PER NODO, il totale sarebbe 1+1+2=4. Una hydration batch
		// corretta produce 1+1+1=3.
		queriesUsed := after - before
		if queriesUsed > 3 {
			t.Errorf("query Neo4j eseguite = %d, want <= 3 (1 GraphTraversal + 1 ACL re-check BATCH + 1 hydration BATCH, non 1 per nodo mancante)", queriesUsed)
		}
	})

	t.Run("Scenario3_IncludeSourceTextFalseNoOpenSearchCall", func(t *testing.T) {
		// Client OpenSearch irraggiungibile: se il codice rispettasse
		// correttamente include_source_text=false (default), non lo
		// chiamerebbe mai -> nessun errore. Se lo chiamasse per errore,
		// l'intera chiamata fallirebbe con un errore di connessione.
		unreachable := unreachableOpenSearchClient(t)
		ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps(unreachable), contentQueryText, contentSeedID, 2)
		if err != nil {
			t.Fatalf("HybridGraphVectorSearch (include_source_text=false, OpenSearch irraggiungibile): %v — atteso nessuna chiamata OpenSearch", err)
		}
		got := findByID(t, ranked, contentGraphDepID)
		if got.SourceText != "" {
			t.Errorf("SourceText = %q, want vuoto (include_source_text=false)", got.SourceText)
		}
	})

	t.Run("Scenario4_IncludeSourceTextTrueConcatenatesInChunkIndexOrder", func(t *testing.T) {
		ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps(osClient), contentQueryText, contentSeedID, 2, hybridsearch.WithIncludeSourceText(true))
		if err != nil {
			t.Fatalf("HybridGraphVectorSearch: %v", err)
		}
		got := findByID(t, ranked, contentGraphDepID)
		want := "AAA\nBBB\nCCC" // chunk_index 0,1,2 — inseriti in OpenSearch in ordine scrambled (2,0,1)
		if got.SourceText != want {
			t.Errorf("SourceText = %q, want %q (ordine chunk_index crescente, non ordine di arrivo/inserimento)", got.SourceText, want)
		}
	})

	t.Run("Scenario5_NoChunksIndexedSourceTextEmptyNoError", func(t *testing.T) {
		ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps(osClient), contentQueryText, contentSeedID, 2, hybridsearch.WithIncludeSourceText(true))
		if err != nil {
			t.Fatalf("HybridGraphVectorSearch: %v", err)
		}
		got := findByID(t, ranked, contentNoChunksID)
		if got.SourceText != "" {
			t.Errorf("SourceText = %q, want vuoto (nessun chunk indicizzato)", got.SourceText)
		}
	})

	t.Run("EdgeCase_OpenSearchUnreachableWithIncludeSourceTextTrueFailsExplicitly", func(t *testing.T) {
		unreachable := unreachableOpenSearchClient(t)
		_, err := hybridsearch.HybridGraphVectorSearch(ctx, deps(unreachable), contentQueryText, contentSeedID, 2, hybridsearch.WithIncludeSourceText(true))
		if err == nil {
			t.Fatal("atteso errore esplicito con OpenSearch irraggiungibile e include_source_text=true, ottenuto nil")
		}
	})
}

func findByID(t *testing.T, nodes []hybridsearch.RetrievedNode, id string) hybridsearch.RetrievedNode {
	t.Helper()
	for _, n := range nodes {
		if n.NodeID == id {
			return n
		}
	}
	t.Fatalf("%s assente dal risultato: %+v", id, nodes)
	return hybridsearch.RetrievedNode{}
}

func seedContentGraph(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		CREATE (seed:CodeNode {id: $seed_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', name: 'Seed'})
		CREATE (dep:CodeNode {id: $dep_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', name: 'GraphDep'})
		CREATE (nc:CodeNode {id: $nc_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', name: 'NoChunks'})
		CREATE (iso:CodeNode {id: $iso_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', name: 'IsolatedEntry'})
		CREATE (v1:CodeNode {id: $v1_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', name: 'VectorOnly1'})
		CREATE (v2:CodeNode {id: $v2_id, domain: 'code', tenant_id: 'tenant-test', repo: 'local', acl_group: 'developers', name: 'VectorOnly2'})
		CREATE (dep)-[:CALLS {weight: 1}]->(seed)
		CREATE (nc)-[:CALLS {weight: 1}]->(seed)
	`, map[string]any{
		"seed_id": contentSeedID, "dep_id": contentGraphDepID, "nc_id": contentNoChunksID,
		"iso_id": contentIsolatedEntryID, "v1_id": contentVectorOnly1ID, "v2_id": contentVectorOnly2ID,
	})
	if err != nil {
		t.Fatalf("seed grafo content: %v", err)
	}
}

// seedContentVectors scrive due punti Qdrant (contentVectorOnly1ID/2ID)
// vicini all'embedding di contentQueryText — riusa perturb/vectorPoint/
// collectionName già definiti in fixture_integration_test.go (stesso
// package hybridsearch_test).
func seedContentVectors(t *testing.T, ctx context.Context, embedderBaseURL string, qc *qdrant.Client) {
	t.Helper()
	embedder := embedclient.New(embedderBaseURL)
	qvec, err := embedder.Embed(ctx, contentQueryText)
	if err != nil {
		t.Fatalf("embed query text: %v", err)
	}

	points := []*qdrant.PointStruct{
		vectorPoint(t, contentVectorOnly1ID, perturb(qvec, 0.01, 101)),
		vectorPoint(t, contentVectorOnly2ID, perturb(qvec, 0.02, 102)),
	}
	if _, err := qc.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collectionName,
		Points:         points,
	}); err != nil {
		t.Fatalf("upsert punti vettoriali content: %v", err)
	}
}

func unreachableOpenSearchClient(t *testing.T) *opensearchapi.Client {
	t.Helper()
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{"http://127.0.0.1:1"}},
	})
	if err != nil {
		t.Fatalf("opensearchapi NewClient (irraggiungibile): %v", err)
	}
	return client
}

func startOpenSearchReal(t *testing.T, ctx context.Context) *opensearchapi.Client {
	t.Helper()
	container, err := tcopensearch.Run(ctx, "opensearchproject/opensearch:2.11.1")
	if err != nil {
		t.Fatalf("avvio container opensearch: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container opensearch: %v", err)
		}
	})
	address, err := container.Address(ctx)
	if err != nil {
		t.Fatalf("opensearch Address: %v", err)
	}
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{address}},
	})
	if err != nil {
		t.Fatalf("opensearchapi NewClient: %v", err)
	}
	return client
}

func seedContentChunks(t *testing.T, ctx context.Context, client *opensearchapi.Client) {
	t.Helper()
	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"text":           map[string]any{"type": "text"},
				"entity_id":      map[string]any{"type": "keyword"},
				"chunk_index":    map[string]any{"type": "integer"},
				"chunk_id":       map[string]any{"type": "keyword"},
				"event_sequence": map[string]any{"type": "long"},
				"tenant_id":      map[string]any{"type": "keyword"},
				"repo":           map[string]any{"type": "keyword"},
				"acl_group":      map[string]any{"type": "keyword"},
			},
		},
	}
	if _, err := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: codeChunksIndex,
		Body:  opensearchutil.NewJSONReader(mapping),
	}); err != nil {
		t.Fatalf("Indices.Create(%s): %v", codeChunksIndex, err)
	}

	// Inseriti in ordine SCRAMBLED (chunk_index 2, 0, 1) — SPEC-045 §3
	// scenario 4 verifica che la concatenazione segua chunk_index
	// crescente, non l'ordine di arrivo/inserimento.
	type chunk struct {
		ChunkID       string `json:"chunk_id"`
		EventSequence int64  `json:"event_sequence"`
		EntityID      string `json:"entity_id"`
		ChunkIndex    int    `json:"chunk_index"`
		Text          string `json:"text"`
		TenantID      string `json:"tenant_id"`
		Repo          string `json:"repo"`
		ACLGroup      string `json:"acl_group"`
	}
	chunks := []chunk{
		{ChunkID: contentGraphDepID + "-0", EventSequence: 1, EntityID: contentGraphDepID, ChunkIndex: 2, Text: "CCC", TenantID: "tenant-test", Repo: "local", ACLGroup: "developers"},
		{ChunkID: contentGraphDepID + "-1", EventSequence: 2, EntityID: contentGraphDepID, ChunkIndex: 0, Text: "AAA", TenantID: "tenant-test", Repo: "local", ACLGroup: "developers"},
		{ChunkID: contentGraphDepID + "-2", EventSequence: 3, EntityID: contentGraphDepID, ChunkIndex: 1, Text: "BBB", TenantID: "tenant-test", Repo: "local", ACLGroup: "developers"},
		{ChunkID: contentGraphDepID + "-3", EventSequence: 4, EntityID: contentGraphDepID, ChunkIndex: 3, Text: "FOREIGN_SECRET", TenantID: "tenant-b", Repo: "local", ACLGroup: "developers"},
	}
	for i, c := range chunks {
		body, err := json.Marshal(c)
		if err != nil {
			t.Fatalf("marshal chunk: %v", err)
		}
		if _, err := client.Index(ctx, opensearchapi.IndexReq{
			Index:      codeChunksIndex,
			DocumentID: c.ChunkID,
			Body:       strings.NewReader(string(body)),
			Params:     opensearchapi.IndexParams{Refresh: "true"},
		}); err != nil {
			t.Fatalf("Index chunk %d: %v", i, err)
		}
	}
}

// startCountingNeo4j avvia un Neo4j reale via testcontainers e ritorna un
// driver che conta ogni chiamata session.Run — SPEC-045 §3 scenario 2:
// "verificabile contando le query eseguite, non solo il risultato".
func startCountingNeo4j(t *testing.T, ctx context.Context) (neo4j.DriverWithContext, *int32) {
	t.Helper()
	real, _ := startNeo4j(t, ctx)
	count := new(int32)
	return countingDriver{DriverWithContext: real, count: count}, count
}

type countingDriver struct {
	neo4j.DriverWithContext
	count *int32
}

func (d countingDriver) NewSession(ctx context.Context, cfg neo4j.SessionConfig) neo4j.SessionWithContext {
	return countingSession{SessionWithContext: d.DriverWithContext.NewSession(ctx, cfg), count: d.count}
}

type countingSession struct {
	neo4j.SessionWithContext
	count *int32
}

func (s countingSession) Run(ctx context.Context, cypher string, params map[string]any, configurers ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	atomic.AddInt32(s.count, 1)
	return s.SessionWithContext.Run(ctx, cypher, params, configurers...)
}
