package gdsimpact

// impactKind — SPEC-043 §2: derivazione dichiarata SOLO dal tipo d'arco
// più vicino al nodo di ingresso nel percorso più breve (Non-goal
// esplicito sulla seconda metà della regola dell'ADD, cambio di firma —
// richiederebbe un confronto vecchia-firma/nuova-firma che questa
// pipeline non traccia).
func impactKind(edgeType string) string {
	switch edgeType {
	case "IMPORTS", "DEPENDS_ON":
		return "module-boundary"
	case "CALLS":
		return "behavioral"
	case "IMPLEMENTS", "EXTENDS", "OVERRIDES":
		return "syntactic"
	default:
		return ""
	}
}

// minMaxNormalize — normalizzazione min-max "sul sottografo proiettato"
// (SPEC-043 §2 punto 6). Quando tutti i valori coincidono (max == min: un
// solo nodo, o punteggi grezzi identici), la normalizzazione min-max non è
// definita algebricamente (divisione per zero) — 0.0 per ciascuno è la
// scelta dichiarata qui (nessun nodo si distingue dagli altri, coerente
// con l'assenza di segnale, non un errore).
func minMaxNormalize(raw map[string]float64) map[string]float64 {
	norm := make(map[string]float64, len(raw))
	if len(raw) == 0 {
		return norm
	}

	min, max := 0.0, 0.0
	first := true
	for _, v := range raw {
		if first {
			min, max = v, v
			first = false
			continue
		}
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	spread := max - min
	for id, v := range raw {
		if spread == 0 {
			norm[id] = 0.0
			continue
		}
		norm[id] = (v - min) / spread
	}
	return norm
}

// combineScores applica la formula di priorità dichiarata (SPEC-043 §2
// punto 6): impact_score = w_ppr*ppr_norm + w_prox*(1/hop_distance) +
// w_bc*betweenness_norm. La normalizzazione è calcolata SOLO sulla
// popolazione che riceverà un impact_score (i dipendenti, MAI il nodo di
// ingresso stesso — hop_distance=0 renderebbe 1/hop_distance indefinito,
// e "quanto è impattato il nodo di ingresso da se stesso" non ha senso).
func combineScores(deps map[string]depInfo, pprRaw, bcRaw map[string]float64, communities map[string]int64, cfg Config) []NodeScore {
	pprNorm := minMaxNormalize(pprRaw)
	bcNorm := minMaxNormalize(bcRaw)

	out := make([]NodeScore, 0, len(deps))
	for id, info := range deps {
		s := NodeScore{
			NodeID:          id,
			HopDistance:     info.HopDistance,
			EdgeType:        info.EdgeType,
			ImpactKind:      impactKind(info.EdgeType),
			PPRRaw:          pprRaw[id],
			PPRNorm:         pprNorm[id],
			BetweennessRaw:  bcRaw[id],
			BetweennessNorm: bcNorm[id],
			CommunityID:     communities[id],
		}
		s.ImpactScore = cfg.WPPR*s.PPRNorm + cfg.WProx*(1.0/float64(s.HopDistance)) + cfg.WBC*s.BetweennessNorm
		out = append(out, s)
	}
	return out
}
