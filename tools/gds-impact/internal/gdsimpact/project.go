package gdsimpact

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// projectionRelPattern — STESSI sei tipi di arco di GraphTraversal/
// StreamImpact (T4.1/T4.2), riusati identici qui per coerenza col resto
// della pipeline di impact analysis.
const projectionRelPattern = "CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS"

// projectQuery — forma "subquery" di gds.graph.project (GDS 2.x, non
// deprecata — a differenza della forma gds.graph.project.cypher, verificata
// empiricamente non supportare orientamento UNDIRECTED per relationship
// type, necessario per Leiden, vedi projectUndirected sotto). Sorgente e
// target SCAMBIATI (m, n invece di n, m) per l'orientamento REVERSE — ADD
// (nota su Deliverable D5): "nella nuova Cypher-projection l'inversione si
// ottiene scambiando source/target". Filtrata a SOLO i nodi del sottografo
// scoperto (n.id/m.id IN $node_ids): a differenza della forma nativa
// gds.graph.project(nome, label, ...), che proietta l'INTERA label, questa
// forma accetta filtri Cypher arbitrari — necessario per "proietta il
// sottografo RILEVANTE", non l'intero grafo (SPEC-043 §2).
const projectReverseQuery = `
MATCH (n:CodeNode)-[r:` + projectionRelPattern + `]->(m:CodeNode)
WHERE n.id IN $node_ids AND m.id IN $node_ids
WITH gds.graph.project($graph_name, m, n) AS g
RETURN g.graphName AS graphName, g.nodeCount AS nodeCount, g.relationshipCount AS relationshipCount
`

// projectUndirectedQuery — stesso sottografo, ma UNDIRECTED: Leiden
// richiede un grafo non orientato ("The Leiden algorithm works only with
// undirected graphs", verificato empiricamente contro GDS 2.13 reale prima
// di scrivere questo codice — non presunto dalla sola documentazione).
// undirectedRelationshipTypes: ['*'] è supportato SOLO da questa forma
// "subquery", non da gds.graph.project.cypher (verificato: quest'ultima
// rifiuta la chiave con "Unexpected configuration key").
const projectUndirectedQuery = `
MATCH (n:CodeNode)-[r:` + projectionRelPattern + `]->(m:CodeNode)
WHERE n.id IN $node_ids AND m.id IN $node_ids
WITH gds.graph.project($graph_name, n, m, {}, {undirectedRelationshipTypes: ['*']}) AS g
RETURN g.graphName AS graphName, g.nodeCount AS nodeCount, g.relationshipCount AS relationshipCount
`

// projection porta i nomi delle DUE proiezioni GDS in-memory create per
// un'esecuzione (REVERSE per PageRank/betweenness, UNDIRECTED per Leiden —
// due proiezioni dello STESSO sottografo, non due sottografi diversi;
// deviazione dichiarata rispetto a "una proiezione" di SPEC-043 §2, vedi
// SPEC §10).
type projection struct {
	ReverseName    string
	UndirectedName string
	NodeCount      int64
}

// createProjections proietta {entryNodeID} ∪ dependents nelle due forme
// (REVERSE, UNDIRECTED). graphName univoco per invocazione (suffisso
// casuale): invocazioni concorrenti su entry_node_id diversi non
// collidono nel catalogo GDS.
func createProjections(ctx context.Context, driver neo4j.DriverWithContext, entryNodeID string, deps map[string]depInfo) (projection, error) {
	nodeIDs := make([]string, 0, len(deps)+1)
	nodeIDs = append(nodeIDs, entryNodeID)
	for id := range deps {
		nodeIDs = append(nodeIDs, id)
	}

	suffix, err := randomHex(8)
	if err != nil {
		return projection{}, fmt.Errorf("gdsimpact: generazione suffisso graphName: %w", err)
	}
	revName := "gds-impact-rev-" + suffix
	undirName := "gds-impact-undir-" + suffix

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	revResult, err := session.Run(ctx, projectReverseQuery, map[string]any{
		"node_ids":   nodeIDs,
		"graph_name": revName,
	})
	if err != nil {
		return projection{}, fmt.Errorf("gdsimpact: proiezione REVERSE: %w", err)
	}
	revRec, err := revResult.Single(ctx)
	if err != nil {
		return projection{}, fmt.Errorf("gdsimpact: proiezione REVERSE (lettura risultato): %w", err)
	}
	nodeCountVal, _ := revRec.Get("nodeCount")
	nodeCount, _ := nodeCountVal.(int64)

	undirResult, err := session.Run(ctx, projectUndirectedQuery, map[string]any{
		"node_ids":   nodeIDs,
		"graph_name": undirName,
	})
	if err != nil {
		// La proiezione REVERSE è già stata creata: propaghiamo l'errore,
		// il chiamante (Run) è responsabile della pulizia (defer) di
		// QUALUNQUE proiezione già creata a questo punto (SPEC-043 §4).
		return projection{ReverseName: revName, NodeCount: nodeCount}, fmt.Errorf("gdsimpact: proiezione UNDIRECTED: %w", err)
	}
	if _, err := undirResult.Single(ctx); err != nil {
		return projection{ReverseName: revName, NodeCount: nodeCount}, fmt.Errorf("gdsimpact: proiezione UNDIRECTED (lettura risultato): %w", err)
	}

	return projection{ReverseName: revName, UndirectedName: undirName, NodeCount: nodeCount}, nil
}

// dropProjections pulisce ENTRAMBE le proiezioni (SPEC-043 §2 punto 8:
// "sempre eseguita, anche in caso di errore"). Chiamata da un `defer` nel
// chiamante — ogni drop è indipendente (un fallimento sull'una non
// impedisce il tentativo sull'altra); gli errori sono loggati, mai
// propagati: non devono mai mascherare l'errore originale della pipeline
// (se presente) che ha innescato la pulizia.
func dropProjections(ctx context.Context, driver neo4j.DriverWithContext, p projection, logf func(format string, args ...any)) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	for _, name := range []string{p.ReverseName, p.UndirectedName} {
		if name == "" {
			continue
		}
		if _, err := session.Run(ctx, "CALL gds.graph.drop($name, false)", map[string]any{"name": name}); err != nil {
			logf("gdsimpact: gds.graph.drop(%s) fallito: %v", name, err)
		}
	}
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
