package impactanalysis

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// StreamImpact — SPEC-042 §2: reverse reachability bounded da entryNodeID,
// BFS livello-per-livello (non un'unica query a lunghezza variabile), cap
// max_nodes sul totale di nodi esplorati, emit chiamata progressivamente
// (nodo per nodo, poi un ImpactProgress per livello completato) — mai
// un'unica raffica finale.
func StreamImpact(ctx context.Context, driver neo4j.DriverWithContext, entryNodeID string, maxDepth, maxNodes int, domain, repo *string, emit func(ImpactEvent) error) error {
	fetch := neo4jLevelFetcher(driver, domain, repo)
	return runBFS(ctx, fetch, entryNodeID, maxDepth, maxNodes, emit)
}

// runBFS è il motore puro (nessuna dipendenza Neo4j diretta): testabile
// con un levelFetchFunc finto (SPEC-042 §7). Un livello alla volta:
// recupera i vicini non ancora visitati della frontiera corrente, emette
// un ImpactNode per ciascuno (rispettando il cap maxNodes, fermandosi
// PRIMA di esplorare oltre appena raggiunto), poi un ImpactProgress che
// riassume il livello — SEMPRE, anche quando il livello non ha scoperto
// nulla (SPEC-042 §3 scenario 5: entry_node_id sconosciuto -> un solo
// ImpactProgress con nodes_explored=0, zero ImpactNode).
func runBFS(ctx context.Context, fetch levelFetchFunc, entryNodeID string, maxDepth, maxNodes int, emit func(ImpactEvent) error) error {
	if maxNodes <= 0 {
		return fmt.Errorf("impactanalysis: max_nodes deve essere >= 1, ricevuto %d", maxNodes)
	}
	if maxDepth <= 0 {
		return fmt.Errorf("impactanalysis: max_depth deve essere >= 1, ricevuto %d", maxDepth)
	}

	visited := map[string]struct{}{entryNodeID: {}}
	frontier := []string{entryNodeID}
	nodesExplored := 0

	for depth := 1; depth <= maxDepth; depth++ {
		if len(frontier) == 0 {
			break
		}

		rows, err := fetch(ctx, frontier, visited)
		if err != nil {
			return fmt.Errorf("impactanalysis: fetch livello %d: %w", depth, err)
		}

		truncated := false
		levelCount := 0
		nextFrontier := make([]string, 0, len(rows))
		for _, row := range rows {
			if _, seen := visited[row.NodeID]; seen {
				// Già scoperto a un livello precedente (più corto): il
				// percorso più breve vince, questa occorrenza più lunga
				// viene scartata silenziosamente (SPEC-042 §4 edge case).
				continue
			}
			if nodesExplored >= maxNodes {
				truncated = true
				break
			}

			visited[row.NodeID] = struct{}{}
			nodesExplored++
			levelCount++
			nextFrontier = append(nextFrontier, row.NodeID)

			if err := emit(ImpactEvent{Node: &ImpactNode{
				NodeID:      row.NodeID,
				Domain:      row.Domain,
				HopDistance: depth,
				EdgeType:    row.EdgeType,
				Provenance:  row.Provenance,
			}}); err != nil {
				return err
			}
		}

		if err := emit(ImpactEvent{Progress: &ImpactProgress{
			NodesExplored: nodesExplored,
			FrontierSize:  levelCount,
			CurrentDepth:  depth,
			Truncated:     truncated,
		}}); err != nil {
			return err
		}

		if truncated {
			break
		}
		frontier = nextFrontier
	}
	return nil
}
