package gdsimpact

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// logAlgorithmEstimates mantiene l'obbligo ADD §1.4 di stimare prima di
// eseguire gli algoritmi, ma lo fa sui graph name già costruiti dalla
// proiezione per-ACL. Non usa gds.graph.project.estimate sulla label globale:
// la procedura non supporta il filtro Cypher della proiezione e violerebbe il
// confine prescritto da ADD §2.2 / SPEC-061.
func logAlgorithmEstimates(ctx context.Context, driver neo4j.DriverWithContext, proj projection, samplingSize int, logf func(format string, args ...any)) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	estimates := []struct {
		name   string
		query  string
		params map[string]any
	}{
		{
			name:   "pageRank",
			query:  `CALL gds.pageRank.stream.estimate($graph_name, {maxIterations: 20, dampingFactor: 0.85}) YIELD requiredMemory RETURN requiredMemory`,
			params: map[string]any{"graph_name": proj.ReverseName},
		},
		{
			name:   "betweenness",
			query:  `CALL gds.betweenness.stream.estimate($graph_name, {samplingSize: $sampling_size}) YIELD requiredMemory RETURN requiredMemory`,
			params: map[string]any{"graph_name": proj.ReverseName, "sampling_size": int64(samplingSize)},
		},
		{
			name:   "leiden",
			query:  `CALL gds.leiden.stream.estimate($graph_name, {}) YIELD requiredMemory RETURN requiredMemory`,
			params: map[string]any{"graph_name": proj.UndirectedName},
		},
	}

	for _, estimate := range estimates {
		result, err := session.Run(ctx, estimate.query, estimate.params)
		if err != nil {
			logf("gdsimpact: estimate %s fallita (solo visibilità, non blocca l'esecuzione): %v", estimate.name, err)
			continue
		}
		rec, err := result.Single(ctx)
		if err != nil {
			logf("gdsimpact: estimate %s senza risultato (%v)", estimate.name, err)
			continue
		}
		requiredMemory, _ := rec.Get("requiredMemory")
		logf("gdsimpact: estimate %s sulla proiezione autorizzata: requiredMemory=%v", estimate.name, requiredMemory)
	}
}
