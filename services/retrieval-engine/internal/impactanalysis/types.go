// Package impactanalysis implementa la RPC streaming ImpactAnalysis
// (SPEC-042, T4.2): reverse reachability bounded, BFS esplicito
// livello-per-livello (non un'unica query Cypher a lunghezza variabile
// come GraphTraversal, T4.1) — necessario sia per lo streaming genuino sia
// per applicare il cap max_nodes PRIMA di esplorare oltre. Package
// sorella di internal/hybridsearch (T4.1): nessuna dipendenza reciproca,
// stessa struttura parallela (tipi propri, non riusati).
package impactanalysis

import "context"

// Provenance canonica (repo/path:righe@commit) — stessa forma di
// hybridsearch.Provenance, dichiarata separatamente per non introdurre una
// dipendenza cross-package tra due package "sorelle".
type Provenance struct {
	Repo, Path, Commit string
	StartLine, EndLine int
}

// ImpactNode è un nodo scoperto durante la traversata, pronto per essere
// emesso in streaming. EdgeType è il tipo dell'arco Cypher sull'ultimo hop
// del percorso più breve verso questo nodo (SPEC-042 §2, "impact_kind":
// CALLS/IMPLEMENTS/EXTENDS/OVERRIDES/DEPENDS_ON/IMPORTS).
type ImpactNode struct {
	NodeID      string
	Domain      string
	HopDistance int
	EdgeType    string
	Provenance  *Provenance
}

// ImpactProgress riassume l'esplorazione di UN livello di profondità
// completato (SPEC-042 §2).
type ImpactProgress struct {
	NodesExplored int // totale cumulativo su TUTTA la traversata finora
	FrontierSize  int // nodi scoperti in QUESTO livello
	CurrentDepth  int
	Truncated     bool // true se max_nodes è stato raggiunto in questo livello
}

// ImpactEvent è l'evento emesso da StreamImpact via `emit` — esattamente
// UNO tra Node e Progress è non-nil, stesso principio del oneof proto
// riusato lato server (ImpactAnalysisEvent).
type ImpactEvent struct {
	Node     *ImpactNode
	Progress *ImpactProgress
}

// fetchedNode è la forma grezza di un nodo scoperto da un fetch di UN
// livello — prodotta sia dal fetcher Neo4j reale (stream.go) sia dai fake
// fetcher usati nei test unitari puri (SPEC-042 §7).
type fetchedNode struct {
	NodeID     string
	Domain     string
	EdgeType   string
	Provenance *Provenance
}

// levelFetchFunc recupera UN livello di espansione: i vicini (via reverse
// reachability) dei nodi in frontierIDs, esclusi quelli già in visited.
// Iniettata per rendere runBFS testabile senza Neo4j reale (SPEC-042 §7).
type levelFetchFunc func(ctx context.Context, frontierIDs []string, visited map[string]struct{}) ([]fetchedNode, error)
