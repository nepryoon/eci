package hybridsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
	"github.com/eci-project/eci/services/retrieval-engine/internal/securityfilter"
)

// codeChunksIndex — nome dichiarato dell'indice OpenSearch scritto da
// sink-search (SPEC-034 §2), riusato qui in sola lettura. Ridichiarato
// localmente (non importabile da services/sink-search/internal, un altro
// modulo Go — stesso principio già accettato per embedclient/T4.1).
const codeChunksIndex = "code_chunks"

const (
	sourceHydrationPageSize = 1000
	sourceHydrationPITTTL   = time.Minute
	// A request can select at most 10,000 nodes. This independent chunk cap
	// keeps pagination, response memory and concatenation bounded even when a
	// valid node has many chunks.
	maxSourceHydrationChunks = 100_000
)

// hydrateNames popola RetrievedNode.Name per ogni nodo con Name=="" (i
// risultati della sola gamba grafo lo hanno già, popolato direttamente
// dalla query di GraphTraversal — SPEC-045 §2). UNA query batch (UNWIND)
// per l'intero set mancante, non N query separate (SPEC-045 §3 scenario
// 2). Un fallimento della query è un errore esplicito (SPEC-045 §4: name è
// una proprietà base attesa sempre disponibile per un nodo :CodeNode
// reale — un suo fallimento indica un problema di connessione).
func hydrateNames(ctx context.Context, driver neo4j.DriverWithContext, nodes []RetrievedNode) error {
	scope, err := accessscope.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("security scope non valido: %w", err)
	}
	missing := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n.Name == "" {
			missing = append(missing, n.NodeID)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	params := securityfilter.Neo4jParams(scope)
	params["node_ids"] = missing
	result, err := session.Run(ctx, `
		UNWIND $node_ids AS id
		MATCH (n:CodeNode {id: id})
		WHERE n.tenant_id = $tenant_id
		  AND n.repo IN $allowed_repos
		  AND n.acl_group IN $acl_groups
		RETURN n.id AS node_id, n.name AS name
	`, params)
	if err != nil {
		return fmt.Errorf("lookup batch name: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return fmt.Errorf("lookup batch name (lettura risultati): %w", err)
	}

	names := make(map[string]string, len(records))
	for _, rec := range records {
		idVal, _ := rec.Get("node_id")
		nameVal, _ := rec.Get("name")
		id, _ := idVal.(string)
		name, _ := nameVal.(string)
		names[id] = name
	}

	for i := range nodes {
		if nodes[i].Name == "" {
			nodes[i].Name = names[nodes[i].NodeID]
		}
	}
	return nil
}

// chunkHit rispecchia i campi del documento code_chunks (SPEC-034 §2)
// necessari qui: entity_id/chunk_index/text.
type chunkHit struct {
	EntityID   string `json:"entity_id"`
	ChunkIndex int    `json:"chunk_index"`
	Text       string `json:"text"`
}

// HydrateSourceText popola RetrievedNode.SourceText leggendo i chunk da
// OpenSearch (code_chunks, SPEC-034) per l'intero set di nodi in UNA sola
// query batch (`terms` su entity_id — stesso principio "batch, non N
// separate" già stabilito per hydrateNames), poi concatena i chunk di
// ciascuna entità in ordine di chunk_index CRESCENTE (SPEC-045 §3 scenario
// 4 — l'ordine di arrivo dei risultati OpenSearch non è garantito). Un
// nodo senza chunk indicizzati resta SourceText="" (scenario 5, nessun
// errore) — solo un fallimento della QUERY stessa (OpenSearch
// irraggiungibile) è un errore esplicito (SPEC-045 §4: richiesto
// esplicitamente dal client via include_source_text=true).
func HydrateSourceText(ctx context.Context, client *opensearchapi.Client, nodes []RetrievedNode) error {
	ctx, observe := securityfilter.Observe(ctx, "opensearch")
	outcome := "error"
	defer func() { observe(outcome) }()
	scope, err := accessscope.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("security scope non valido: %w", err)
	}
	if client == nil {
		return fmt.Errorf("client OpenSearch non configurato (include_source_text=true richiede Deps.OpenSearch)")
	}
	if len(nodes) == 0 {
		outcome = "empty"
		return nil
	}

	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.NodeID
	}
	securityHeaders := securityfilter.OpenSearchHeaders(scope)
	pit, err := client.PointInTime.Create(ctx, opensearchapi.PointInTimeCreateReq{
		Indices: []string{codeChunksIndex},
		Header:  securityHeaders,
		Params:  opensearchapi.PointInTimeCreateParams{KeepAlive: sourceHydrationPITTTL},
	})
	if err != nil {
		return fmt.Errorf("apertura snapshot code_chunks: %w", err)
	}
	pitInspect := pit.Inspect()
	if pitInspect.Response == nil || pitInspect.Response.StatusCode < 200 || pitInspect.Response.StatusCode >= 300 ||
		pit.PitID == "" || pit.Shards.Failed != 0 {
		return fmt.Errorf("apertura snapshot code_chunks: risposta OpenSearch non valida")
	}
	defer func() {
		// Cleanup must survive client cancellation; the independent deadline and
		// server-side one-minute TTL keep both the call and leaked PIT bounded.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_, _ = client.PointInTime.Delete(cleanupCtx, opensearchapi.PointInTimeDeleteReq{
			PitID: []string{pit.PitID}, Header: securityHeaders,
		})
	}()

	query := map[string]any{
		"query":            securityfilter.OpenSearchFilter(scope, ids),
		"track_total_hits": true,
		"size":             sourceHydrationPageSize,
		"pit": map[string]any{
			"id": pit.PitID, "keep_alive": "1m",
		},
		// Re-ingestion can leave two projection documents for the same logical
		// chunk until reconciliation. PIT freezes the result set and the indexed
		// canonical chunk UUID is its unique sortable tiebreaker, so search_after
		// cannot skip a logical-coordinate tie. OpenSearch forbids sorting on _id.
		"sort": []any{
			map[string]any{"entity_id": "asc"},
			map[string]any{"chunk_index": "asc"},
			map[string]any{"chunk_id": "asc"},
		},
	}
	byEntity := make(map[string][]chunkHit)
	seenIDs := make(map[string]struct{})
	expectedTotal := -1
	for {
		resp, err := client.Search(ctx, &opensearchapi.SearchReq{
			Body:   opensearchutil.NewJSONReader(query),
			Header: securityHeaders,
		})
		if err != nil {
			return fmt.Errorf("ricerca batch code_chunks: %w", err)
		}
		inspect := resp.Inspect()
		if inspect.Response == nil || inspect.Response.StatusCode < 200 || inspect.Response.StatusCode >= 300 {
			return fmt.Errorf("ricerca batch code_chunks: risposta OpenSearch non valida")
		}
		if resp.Timeout || resp.Errors || resp.Hits.Total.Relation != "eq" {
			return fmt.Errorf("ricerca batch code_chunks: risposta parziale")
		}
		if expectedTotal < 0 {
			expectedTotal = resp.Hits.Total.Value
			if expectedTotal > maxSourceHydrationChunks {
				return fmt.Errorf("ricerca batch code_chunks: limite bounded superato")
			}
		} else if resp.Hits.Total.Value != expectedTotal {
			return fmt.Errorf("ricerca batch code_chunks: risultato cambiato durante la paginazione")
		}

		for _, hit := range resp.Hits.Hits {
			if _, duplicate := seenIDs[hit.ID]; duplicate {
				return fmt.Errorf("ricerca batch code_chunks: pagina duplicata")
			}
			seenIDs[hit.ID] = struct{}{}
			var c chunkHit
			if err := json.Unmarshal(hit.Source, &c); err != nil {
				continue // documento malformato: scartato, non un errore fatale della ricerca
			}
			byEntity[c.EntityID] = append(byEntity[c.EntityID], c)
		}
		if len(seenIDs) == expectedTotal {
			break
		}
		if len(resp.Hits.Hits) == 0 || len(resp.Hits.Hits) > sourceHydrationPageSize {
			return fmt.Errorf("ricerca batch code_chunks: paginazione incompleta")
		}
		lastSort := resp.Hits.Hits[len(resp.Hits.Hits)-1].Sort
		if len(lastSort) != 3 {
			return fmt.Errorf("ricerca batch code_chunks: cursore di paginazione non valido")
		}
		query["search_after"] = lastSort
	}
	for _, chunks := range byEntity {
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkIndex < chunks[j].ChunkIndex })
	}

	for i := range nodes {
		chunks, ok := byEntity[nodes[i].NodeID]
		if !ok {
			continue
		}
		texts := make([]string, len(chunks))
		for j, c := range chunks {
			texts[j] = c.Text
		}
		nodes[i].SourceText = strings.Join(texts, "\n")
	}
	outcome = "allow"
	return nil
}

// recheckAuthorized applies the defense-in-depth ACL check after fusion and
// before any hydration, reranking, or packing. It is not a substitute for the
// mandatory pre-retrieval filters in each leg.
func recheckAuthorized(ctx context.Context, driver neo4j.DriverWithContext, nodes []RetrievedNode) ([]RetrievedNode, error) {
	ctx, observe := securityfilter.Observe(ctx, "neo4j")
	outcome := "error"
	defer func() { observe(outcome) }()
	if len(nodes) == 0 {
		outcome = "empty"
		return nodes, nil
	}
	scope, err := accessscope.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("security scope non valido: %w", err)
	}
	ids := make([]string, len(nodes))
	for i := range nodes {
		ids[i] = nodes[i].NodeID
	}
	params := securityfilter.Neo4jParams(scope)
	params["node_ids"] = ids
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, `
		UNWIND $node_ids AS id
		MATCH (n:CodeNode {id: id})
		WHERE n.tenant_id = $tenant_id
		  AND n.repo IN $allowed_repos
		  AND n.acl_group IN $acl_groups
		RETURN n.id AS node_id
	`, params)
	if err != nil {
		return nil, fmt.Errorf("ACL re-check: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("ACL re-check risultati: %w", err)
	}
	allowed := make(map[string]struct{}, len(records))
	for _, rec := range records {
		value, _ := rec.Get("node_id")
		if id, ok := value.(string); ok {
			allowed[id] = struct{}{}
		}
	}
	out := make([]RetrievedNode, 0, len(nodes))
	for _, node := range nodes {
		if _, ok := allowed[node.NodeID]; ok {
			out = append(out, node)
		}
	}
	securityfilter.AddRecheckRemoved(len(nodes) - len(out))
	if len(out) == 0 {
		outcome = "empty"
	} else {
		outcome = "allow"
	}
	return out, nil
}
