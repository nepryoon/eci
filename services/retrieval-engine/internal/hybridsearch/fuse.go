package hybridsearch

import "sort"

// RRFFuse — port 1:1 di D5 `_rrf_fuse`: Reciprocal Rank Fusion, dedup per
// node_id. Un nodo presente in ENTRAMBE le liste accumula il contributo RRF
// di ciascuna (1/(k+rank), sommato non sostituito, SPEC-041 §3 scenario 1);
// hop_distance prende il minimo tra le due occorrenze se entrambe presenti;
// i campi vettoriali/grafo mancanti vengono riempiti dalla seconda
// occorrenza se la prima non li aveva. Ordine di merge identico a D5:
// grafo prima, poi vettoriale.
func RRFFuse(graphNodes, vectorNodes []RetrievedNode, k int) map[string]*RetrievedNode {
	fused := make(map[string]*RetrievedNode)

	merge := func(n RetrievedNode, rank *int) {
		if rank == nil {
			return
		}
		contrib := 1.0 / float64(k+*rank)
		ex, ok := fused[n.NodeID]
		if !ok {
			ex = &RetrievedNode{
				NodeID:      n.NodeID,
				Domain:      n.Domain,
				Source:      "fused",
				Name:        n.Name,
				VectorScore: n.VectorScore,
				HopDistance: n.HopDistance,
				GraphRank:   n.GraphRank,
				VectorRank:  n.VectorRank,
				Provenance:  n.Provenance,
				Payload:     copyPayload(n.Payload),
			}
			fused[n.NodeID] = ex
		} else {
			switch {
			case ex.HopDistance != nil && n.HopDistance != nil:
				if *n.HopDistance < *ex.HopDistance {
					ex.HopDistance = n.HopDistance
				}
			case ex.HopDistance == nil && n.HopDistance != nil:
				ex.HopDistance = n.HopDistance
			}
			if ex.VectorScore == nil {
				ex.VectorScore = n.VectorScore
			}
			if ex.GraphRank == nil {
				ex.GraphRank = n.GraphRank
			}
			if ex.VectorRank == nil {
				ex.VectorRank = n.VectorRank
			}
			if ex.Provenance == nil && n.Provenance != nil {
				ex.Provenance = n.Provenance
			}
			if ex.Name == "" {
				ex.Name = n.Name
			}
		}
		fused[n.NodeID].RRFScore += contrib
	}

	for _, n := range graphNodes {
		merge(n, n.GraphRank)
	}
	for _, n := range vectorNodes {
		merge(n, n.VectorRank)
	}
	return fused
}

// ApplyTopologicalProximity — port 1:1 di D5 `_apply_topological_proximity`:
// combined_score = rrf_score + beta*(1/(1+hop_distance) se presente,
// altrimenti 0), ordinamento decrescente per combined_score.
func ApplyTopologicalProximity(fused map[string]*RetrievedNode, beta float64) []RetrievedNode {
	results := make([]RetrievedNode, 0, len(fused))
	for _, n := range fused {
		prox := 0.0
		if n.HopDistance != nil {
			prox = 1.0 / (1 + *n.HopDistance)
		}
		n.CombinedScore = n.RRFScore + beta*prox
		results = append(results, *n)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].CombinedScore > results[j].CombinedScore
	})
	return results
}

func copyPayload(p map[string]any) map[string]any {
	if p == nil {
		return nil
	}
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}
