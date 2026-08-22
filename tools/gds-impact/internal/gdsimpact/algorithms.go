package gdsimpact

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// runPageRank — gds.pageRank.stream seedata su entryNodeID (ADD §1.4: "Su
// proiezione a orientamento REVERSE, il PPR misura quanto ciascun
// dipendente è 'esposto' al target"). maxIterations/dampingFactor: valori
// dichiarati dall'ADD (SPEC-043 §2 punto 3), non configurabili via flag
// (SPEC non li elenca tra i parametri CLI).
func runPageRank(ctx context.Context, driver neo4j.DriverWithContext, graphName, entryNodeID string) (map[string]float64, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (seed:CodeNode {id: $entry_node_id})
		CALL gds.pageRank.stream($graph_name, {sourceNodes: [seed], maxIterations: 20, dampingFactor: 0.85})
		YIELD nodeId, score
		RETURN gds.util.asNode(nodeId).id AS node_id, score
	`, map[string]any{"graph_name": graphName, "entry_node_id": entryNodeID})
	if err != nil {
		return nil, fmt.Errorf("gdsimpact: gds.pageRank.stream: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("gdsimpact: gds.pageRank.stream (lettura risultati): %w", err)
	}

	out := make(map[string]float64, len(records))
	for _, rec := range records {
		nodeIDVal, _ := rec.Get("node_id")
		scoreVal, _ := rec.Get("score")
		nodeID, _ := nodeIDVal.(string)
		score, _ := scoreVal.(float64)
		out[nodeID] = score
	}
	return out, nil
}

// runBetweenness — gds.betweenness.stream con samplingSize esplicito
// (risolto dal chiamante: nodeCount della proiezione se non specificato
// via flag, SPEC-043 §2 punto 4 / §3 scenario 5 — "impostare samplingSize
// al conteggio dei nodi del grafo produce risultati esatti").
func runBetweenness(ctx context.Context, driver neo4j.DriverWithContext, graphName string, samplingSize int) (map[string]float64, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		CALL gds.betweenness.stream($graph_name, {samplingSize: $sampling_size})
		YIELD nodeId, score
		RETURN gds.util.asNode(nodeId).id AS node_id, score
	`, map[string]any{"graph_name": graphName, "sampling_size": int64(samplingSize)})
	if err != nil {
		return nil, fmt.Errorf("gdsimpact: gds.betweenness.stream: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("gdsimpact: gds.betweenness.stream (lettura risultati): %w", err)
	}

	out := make(map[string]float64, len(records))
	for _, rec := range records {
		nodeIDVal, _ := rec.Get("node_id")
		scoreVal, _ := rec.Get("score")
		nodeID, _ := nodeIDVal.(string)
		score, _ := scoreVal.(float64)
		out[nodeID] = score
	}
	return out, nil
}

// runLeiden — gds.leiden.stream sulla proiezione UNDIRECTED (project.go).
// community id per nodo, non pesato nella formula di impact_score (ADD non
// lo include nella combinazione lineare) — scritto come proprietà separata
// community_id (SPEC-043 §2 punto 5/7).
func runLeiden(ctx context.Context, driver neo4j.DriverWithContext, undirectedGraphName string) (map[string]int64, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		CALL gds.leiden.stream($graph_name)
		YIELD nodeId, communityId
		RETURN gds.util.asNode(nodeId).id AS node_id, communityId
	`, map[string]any{"graph_name": undirectedGraphName})
	if err != nil {
		return nil, fmt.Errorf("gdsimpact: gds.leiden.stream: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("gdsimpact: gds.leiden.stream (lettura risultati): %w", err)
	}

	out := make(map[string]int64, len(records))
	for _, rec := range records {
		nodeIDVal, _ := rec.Get("node_id")
		communityVal, _ := rec.Get("communityId")
		nodeID, _ := nodeIDVal.(string)
		community, _ := communityVal.(int64)
		out[nodeID] = community
	}
	return out, nil
}
