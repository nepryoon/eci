package gdsimpact

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// logProjectionEstimate — SPEC-043 §2 punto 1: gds.graph.project.estimate,
// log del risultato, nessun abort automatico ("solo visibilità, coerente
// con l'assenza di un budget di memoria configurato altrove nel
// progetto"). gds.graph.project.estimate accetta SOLO la forma nativa
// label-based (nodeProjection/relationshipProjection come label/mappa),
// non un filtro Cypher arbitrario sul sottografo scoperto (verificato
// empiricamente: il parametro è ANY ma la semantica è quella nativa, non
// la forma "subquery" usata da createProjections) — questa stima è quindi
// un limite SUPERIORE sull'INTERA label :CodeNode, non sul sottografo
// filtrato reale (più piccolo). Deviazione dichiarata, SPEC §10: nessuna
// stima nativa esiste per una proiezione filtrata da Cypher arbitrario;
// loggata chiaramente come tale, mai usata per un abort automatico (che
// sarebbe scorretto, essendo un limite superiore lasco).
func logProjectionEstimate(ctx context.Context, driver neo4j.DriverWithContext, logf func(format string, args ...any)) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		CALL gds.graph.project.estimate(
			'CodeNode',
			{CALLS: {type: 'CALLS'}, IMPLEMENTS: {type: 'IMPLEMENTS'}, EXTENDS: {type: 'EXTENDS'},
			 OVERRIDES: {type: 'OVERRIDES'}, DEPENDS_ON: {type: 'DEPENDS_ON'}, IMPORTS: {type: 'IMPORTS'}}
		) YIELD requiredMemory, nodeCount, relationshipCount
		RETURN requiredMemory, nodeCount, relationshipCount
	`, nil)
	if err != nil {
		logf("gdsimpact: gds.graph.project.estimate fallita (solo visibilità, non blocca l'esecuzione): %v", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		logf("gdsimpact: gds.graph.project.estimate: nessun risultato (%v)", err)
		return
	}
	requiredMemory, _ := rec.Get("requiredMemory")
	nodeCount, _ := rec.Get("nodeCount")
	relationshipCount, _ := rec.Get("relationshipCount")
	logf("gdsimpact: stima memoria per l'INTERA label :CodeNode (limite superiore, non il sottografo filtrato): requiredMemory=%v nodeCount=%v relationshipCount=%v", requiredMemory, nodeCount, relationshipCount)
}
