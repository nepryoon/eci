package hybridsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
)

// codeChunksIndex — nome dichiarato dell'indice OpenSearch scritto da
// sink-search (SPEC-034 §2), riusato qui in sola lettura. Ridichiarato
// localmente (non importabile da services/sink-search/internal, un altro
// modulo Go — stesso principio già accettato per embedclient/T4.1).
const codeChunksIndex = "code_chunks"

// hydrateNames popola RetrievedNode.Name per ogni nodo con Name=="" (i
// risultati della sola gamba grafo lo hanno già, popolato direttamente
// dalla query di GraphTraversal — SPEC-045 §2). UNA query batch (UNWIND)
// per l'intero set mancante, non N query separate (SPEC-045 §3 scenario
// 2). Un fallimento della query è un errore esplicito (SPEC-045 §4: name è
// una proprietà base attesa sempre disponibile per un nodo :CodeNode
// reale — un suo fallimento indica un problema di connessione).
func hydrateNames(ctx context.Context, driver neo4j.DriverWithContext, nodes []RetrievedNode) error {
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

	result, err := session.Run(ctx, `
		UNWIND $node_ids AS id
		MATCH (n:CodeNode {id: id})
		RETURN n.id AS node_id, n.name AS name
	`, map[string]any{"node_ids": missing})
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

// hydrateSourceText popola RetrievedNode.SourceText leggendo i chunk da
// OpenSearch (code_chunks, SPEC-034) per l'intero set di nodi in UNA sola
// query batch (`terms` su entity_id — stesso principio "batch, non N
// separate" già stabilito per hydrateNames), poi concatena i chunk di
// ciascuna entità in ordine di chunk_index CRESCENTE (SPEC-045 §3 scenario
// 4 — l'ordine di arrivo dei risultati OpenSearch non è garantito). Un
// nodo senza chunk indicizzati resta SourceText="" (scenario 5, nessun
// errore) — solo un fallimento della QUERY stessa (OpenSearch
// irraggiungibile) è un errore esplicito (SPEC-045 §4: richiesto
// esplicitamente dal client via include_source_text=true).
func hydrateSourceText(ctx context.Context, client *opensearchapi.Client, nodes []RetrievedNode) error {
	if client == nil {
		return fmt.Errorf("client OpenSearch non configurato (include_source_text=true richiede Deps.OpenSearch)")
	}
	if len(nodes) == 0 {
		return nil
	}

	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.NodeID
	}

	query := map[string]any{
		"query": map[string]any{
			"terms": map[string]any{"entity_id": ids},
		},
		// Limite esplicito generoso: il default OpenSearch (10 risultati)
		// troncherebbe silenziosamente un'entità con molti chunk o un
		// candidate set con molte entità — scelta dichiarata, non presunta
		// dal default.
		"size": 1000,
	}

	resp, err := client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{codeChunksIndex},
		Body:    opensearchutil.NewJSONReader(query),
	})
	if err != nil {
		return fmt.Errorf("ricerca batch code_chunks: %w", err)
	}

	byEntity := make(map[string][]chunkHit)
	for _, hit := range resp.Hits.Hits {
		var c chunkHit
		if err := json.Unmarshal(hit.Source, &c); err != nil {
			continue // documento malformato: scartato, non un errore fatale della ricerca
		}
		byEntity[c.EntityID] = append(byEntity[c.EntityID], c)
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
	return nil
}
