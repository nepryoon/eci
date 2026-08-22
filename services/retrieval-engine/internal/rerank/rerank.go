package rerank

import (
	"context"
	"fmt"
	"sort"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerankclient"
)

// Rerank — SPEC-044 §2: reranking cross-encoder dei candidati fusi RRF
// (T4.1), riordinati per final_score = rerank_score + beta*proximity_boost
// (hop_distance T4.1 + impact_score T4.3). Costruisce il Reranker/
// impactScoreFetchFunc di produzione e delega a rerankCore (motore puro,
// testabile senza infrastruttura reale — SPEC-044 §7).
func Rerank(ctx context.Context, client *rerankclient.Client, driver neo4j.DriverWithContext, query string, candidates []hybridsearch.RetrievedNode, topK int, beta, wHop, wImpact float64) ([]RankedNode, error) {
	return rerankCore(ctx, client, neo4jImpactScoreFetcher(driver), query, candidates, topK, beta, wHop, wImpact)
}

// rerankCore è il motore puro (SPEC-044 §7): niente dipendenza diretta da
// TEI/Neo4j, solo le interfacce/closure iniettate. Ordine dei passi
// (SPEC-044 §2): (1) validazione topK; (2) candidate set vuoto -> uscita
// immediata, nessuna chiamata esterna (edge case §4 riga 2); (3) estrazione
// testo per candidato; (4) UNA chiamata al reranker per l'intero set; (5)
// UNA query batch per impact_score; (6) combinazione + ordinamento +
// troncamento a topK.
func rerankCore(ctx context.Context, reranker Reranker, fetchImpact impactScoreFetchFunc, query string, candidates []hybridsearch.RetrievedNode, topK int, beta, wHop, wImpact float64) ([]RankedNode, error) {
	if topK <= 0 {
		return nil, fmt.Errorf("rerank: topK deve essere >= 1, ricevuto %d", topK)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	texts := make([]string, len(candidates))
	for i, c := range candidates {
		texts[i] = candidateText(c)
	}

	rerankResults, err := reranker.Rerank(ctx, query, texts)
	if err != nil {
		return nil, fmt.Errorf("rerank: chiamata al servizio di reranking fallita: %w", err)
	}
	rerankScoreByIndex := make(map[int]float64, len(rerankResults))
	for _, r := range rerankResults {
		rerankScoreByIndex[r.Index] = r.Score
	}

	nodeIDs := make([]string, len(candidates))
	for i, c := range candidates {
		nodeIDs[i] = c.NodeID
	}
	impactScores, err := fetchImpact(ctx, nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("rerank: lettura batch impact_score fallita: %w", err)
	}

	ranked := make([]RankedNode, len(candidates))
	for i, c := range candidates {
		// impact_score assente dalla mappa (mai scritto da T4.3 per
		// questo nodo) -> zero-value Go 0.0, esattamente
		// impact_score_norm=0 richiesto da SPEC-044 §3 scenario 4 — nessun
		// controllo "ok" esplicito necessario.
		impactNorm := impactScores[c.NodeID]
		boost := proximityBoost(c.HopDistance, impactNorm, wHop, wImpact)
		rerankScore := rerankScoreByIndex[i]
		ranked[i] = RankedNode{
			Node:            c,
			RerankScore:     rerankScore,
			ImpactScoreNorm: impactNorm,
			ProximityBoost:  boost,
			FinalScore:      rerankScore + beta*boost,
		}
	}

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].FinalScore > ranked[j].FinalScore
	})
	if len(ranked) > topK {
		ranked = ranked[:topK]
	}
	return ranked, nil
}

// candidateText estrae il testo di un candidato da passare al reranker
// (SPEC-044 §2 punto 1: "solo i campi che la pipeline può popolare
// davvero"). hybridsearch.RetrievedNode (T4.1, porto fedele di D5) non
// porta name/summary/source_text — nessuno di questi campi è mai popolato
// da HybridGraphVectorSearch oggi (T4.1 invariato, §5 non-goals di questa
// SPEC) — node_id è l'UNICO campo testuale sempre presente. Deviazione
// dichiarata rispetto al testo letterale di §2 ("altrimenti il nome/
// summary disponibile" presume campi che la pipeline non popola ancora).
func candidateText(n hybridsearch.RetrievedNode) string {
	return n.NodeID
}

// neo4jImpactScoreFetcher — SPEC-044 §2 punto 3: UNA sola query batch
// (UNWIND) per l'intero candidate set, non N query separate.
// OPTIONAL MATCH copre sia "nodo esistente ma impact_score mai scritto"
// sia "nodo non trovato affatto" — in ENTRAMBI i casi il chiamante vede
// semplicemente l'assenza dalla mappa ritornata (SPEC-044 §3 scenario 4).
func neo4jImpactScoreFetcher(driver neo4j.DriverWithContext) impactScoreFetchFunc {
	return func(ctx context.Context, nodeIDs []string) (map[string]float64, error) {
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)

		result, err := session.Run(ctx, `
			UNWIND $node_ids AS id
			OPTIONAL MATCH (n:CodeNode {id: id})
			RETURN id AS node_id, n.impact_score AS impact_score
		`, map[string]any{"node_ids": nodeIDs})
		if err != nil {
			return nil, err
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return nil, err
		}

		out := make(map[string]float64, len(records))
		for _, rec := range records {
			nodeIDVal, _ := rec.Get("node_id")
			scoreVal, _ := rec.Get("impact_score")
			nodeID, _ := nodeIDVal.(string)
			if score, ok := scoreVal.(float64); ok {
				out[nodeID] = score
			}
		}
		return out, nil
	}
}
