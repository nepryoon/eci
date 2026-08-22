// Package rerank applica il reranker cross-encoder (bge-reranker-v2-m3 via
// TEI) ai risultati fusi RRF di hybridsearch (T4.1) — SPEC-044, T4.4.
// Package sorella di hybridsearch/impactanalysis: nessuna dipendenza
// reciproca oltre a hybridsearch.RetrievedNode (il tipo dei candidati in
// ingresso, non un tipo introdotto qui).
package rerank

import (
	"context"

	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerankclient"
)

// RankedNode è un candidato dopo il reranking: il nodo originale più le
// componenti del calcolo (SPEC-044 §2), tutte esposte così i test possono
// ricalcolare la formula dichiarata dalle stesse componenti persistite —
// stesso principio già stabilito per hybridsearch.RetrievedNode/
// impactanalysis.ImpactNode.
type RankedNode struct {
	Node            hybridsearch.RetrievedNode
	RerankScore     float64
	ImpactScoreNorm float64
	ProximityBoost  float64
	FinalScore      float64
}

// Reranker espone il reranking di una query contro un insieme di testi —
// stesso contratto di rerankclient.Client.Rerank, qui come interfaccia Go
// per disaccoppiare rerank dal client HTTP concreto (stesso principio di
// hybridsearch.Embedder).
type Reranker interface {
	Rerank(ctx context.Context, query string, texts []string) ([]rerankclient.Result, error)
}

// impactScoreFetchFunc legge il punteggio n.impact_score (T4.3, SPEC-043)
// per un insieme di node_id in una SOLA query batch (SPEC-044 §2 punto 3:
// "non N query separate"). Un node_id assente dalla mappa ritornata
// significa "impact_score mai scritto per questo nodo" — il chiamante
// tratta questo come 0.0 (SPEC-044 §3 scenario 4), non un errore.
// Iniettata per rendere il motore testabile senza Neo4j reale (SPEC-044
// §7, stesso principio già stabilito per levelFetchFunc in
// impactanalysis/T4.2).
type impactScoreFetchFunc func(ctx context.Context, nodeIDs []string) (map[string]float64, error)

// proximityBoost — SPEC-044 §2, formula dichiarata:
// proximity_boost(node) = w_hop*(1/(1+hop_distance)) + w_impact*impact_score_norm.
// hop_distance assente (nil, nodo mai visto dalla gamba grafo) -> il
// termine w_hop*(...) è 0 (nessun boost di prossimità topologica
// applicabile), stesso principio già stabilito in hybridsearch
// (ApplyTopologicalProximity, T4.1) per un nodo senza hop_distance.
func proximityBoost(hopDistance *float64, impactScoreNorm, wHop, wImpact float64) float64 {
	hopTerm := 0.0
	if hopDistance != nil {
		hopTerm = 1.0 / (1.0 + *hopDistance)
	}
	return wHop*hopTerm + wImpact*impactScoreNorm
}
