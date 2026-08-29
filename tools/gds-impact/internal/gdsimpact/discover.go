package gdsimpact

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// depInfo è quanto sappiamo su un nodo dipendente scoperto dalla
// traversata: la distanza in hop dal nodo di ingresso e il tipo d'arco
// dell'ultimo hop del percorso più breve — stesso principio "percorso più
// corto vince" già stabilito in GraphTraversal/StreamImpact (T4.1/T4.2).
type depInfo struct {
	HopDistance int
	EdgeType    string
}

// levelCypher — STESSA struttura di GraphTraversal (T4.1) e del fetcher di
// StreamImpact (T4.2): reverse reachability, stesso insieme di sei tipi di
// arco. UN SOLO hop per chiamata (BFS livello-per-livello lato Go, non un
// `*1..N` a lunghezza variabile): tools/gds-impact è un modulo Go separato
// da services/retrieval-engine (non importabile — duplicazione dichiarata,
// stesso principio già accettato per embedclient in T4.1).
const levelCypher = `
MATCH (n:CodeNode) WHERE n.id IN $frontier_ids
MATCH (n)<-[r:CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS]-(dep:CodeNode)
WHERE n.tenant_id = $tenant_id AND n.repo = $repo AND n.acl_group = $acl_group
  AND dep.tenant_id = $tenant_id AND dep.repo = $repo AND dep.acl_group = $acl_group
  AND NOT dep.id IN $visited_ids
WITH dep, min(type(r)) AS edge_type
RETURN dep.id AS node_id, edge_type
ORDER BY node_id
`

// discoverSubgraph esegue la BFS livello-per-livello da entryNodeID fino a
// maxDepth, ritornando SOLO i dipendenti scoperti (mai entryNodeID stesso).
// entryNodeID inesistente o senza dipendenti -> mappa vuota, nessun errore
// (SPEC-043 §3 scenario 4, stesso principio già stabilito in T4.1/T4.2).
func discoverSubgraph(ctx context.Context, driver neo4j.DriverWithContext, entryNodeID string, scope ProjectionScope, maxDepth int) (map[string]depInfo, error) {
	visited := map[string]struct{}{entryNodeID: {}}
	frontier := []string{entryNodeID}
	deps := make(map[string]depInfo)

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	for depth := 1; depth <= maxDepth; depth++ {
		if len(frontier) == 0 {
			break
		}

		visitedIDs := make([]string, 0, len(visited))
		for id := range visited {
			visitedIDs = append(visitedIDs, id)
		}

		result, err := session.Run(ctx, levelCypher, map[string]any{
			"frontier_ids": frontier,
			"visited_ids":  visitedIDs,
			"tenant_id":    scope.TenantID,
			"repo":         scope.Repo,
			"acl_group":    scope.ACLGroup,
		})
		if err != nil {
			return nil, fmt.Errorf("gdsimpact: discovery livello %d: %w", depth, err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("gdsimpact: discovery livello %d (lettura risultati): %w", depth, err)
		}

		nextFrontier := make([]string, 0, len(records))
		for _, rec := range records {
			nodeIDVal, _ := rec.Get("node_id")
			edgeTypeVal, _ := rec.Get("edge_type")
			nodeID, _ := nodeIDVal.(string)
			edgeType, _ := edgeTypeVal.(string)

			if _, seen := visited[nodeID]; seen {
				continue
			}
			visited[nodeID] = struct{}{}
			deps[nodeID] = depInfo{HopDistance: depth, EdgeType: edgeType}
			nextFrontier = append(nextFrontier, nodeID)
		}
		frontier = nextFrontier
	}
	return deps, nil
}
