// Unit puri per la formula proximity_boost/final_score (SPEC-044 §7):
// nessuna infrastruttura reale, un Reranker finto e un impactScoreFetchFunc
// finto sostituiscono TEI/Neo4j. Scritti PRIMA dell'implementazione (TDD)
// — coprono §3 scenari 1-4 e l'edge case §4.
package rerank

import (
	"context"
	"errors"
	"testing"

	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerankclient"
)

func hop(v float64) *float64 { return &v }

// fakeReranker ritorna, per ciascun testo in ingresso (qui: il NodeID del
// candidato, l'unico campo testuale sempre disponibile — vedi
// candidateText), il punteggio configurato in `scores` (per NodeID).
type fakeReranker struct {
	scores map[string]float64
	called bool
}

func (f *fakeReranker) Rerank(_ context.Context, _ string, texts []string) ([]rerankclient.Result, error) {
	f.called = true
	out := make([]rerankclient.Result, len(texts))
	for i, text := range texts {
		out[i] = rerankclient.Result{Index: i, Score: f.scores[text]}
	}
	return out, nil
}

func fakeImpactFetch(scores map[string]float64) (impactScoreFetchFunc, *bool) {
	called := false
	return func(_ context.Context, _ []string) (map[string]float64, error) {
		called = true
		return scores, nil
	}, &called
}

// Scenario 1 (SPEC-044 §3): il final_score di ciascun nodo combacia con la
// formula dichiarata, calcolabile a mano.
func TestRerankCore_FinalScoreMatchesDeclaredFormula(t *testing.T) {
	candidates := []hybridsearch.RetrievedNode{
		{NodeID: "n1", HopDistance: hop(1)},
		{NodeID: "n2", HopDistance: hop(3)},
	}
	reranker := &fakeReranker{scores: map[string]float64{"n1": 0.8, "n2": 0.2}}
	fetchImpact, _ := fakeImpactFetch(map[string]float64{"n1": 0.9, "n2": 0.1})

	const beta, wHop, wImpact = 0.5, 0.5, 0.5
	ranked, err := rerankCore(context.Background(), reranker, fetchImpact, "query", candidates, 10, beta, wHop, wImpact)
	if err != nil {
		t.Fatalf("rerankCore: %v", err)
	}
	if len(ranked) != 2 {
		t.Fatalf("len(ranked) = %d, want 2", len(ranked))
	}

	byID := map[string]RankedNode{}
	for _, r := range ranked {
		byID[r.Node.NodeID] = r
	}

	wantN1 := 0.8 + beta*(wHop*(1.0/(1.0+1.0))+wImpact*0.9)
	wantN2 := 0.2 + beta*(wHop*(1.0/(1.0+3.0))+wImpact*0.1)

	if got := byID["n1"].FinalScore; !floatsClose(got, wantN1) {
		t.Errorf("n1.FinalScore = %v, want %v", got, wantN1)
	}
	if got := byID["n2"].FinalScore; !floatsClose(got, wantN2) {
		t.Errorf("n2.FinalScore = %v, want %v", got, wantN2)
	}
}

// Scenario 2 (SPEC-044 §3): l'ordine riflette la combinazione dei due
// segnali, non uno isolatamente — un nodo con impact_score alto/rerank
// basso e un altro con rerank alto/impact_score basso, con beta
// sufficientemente grande, si scambiano di posizione rispetto al solo
// rerank_score.
func TestRerankCore_OrderReflectsCombinedSignalNotOneAlone(t *testing.T) {
	candidates := []hybridsearch.RetrievedNode{
		{NodeID: "high-rerank-low-impact", HopDistance: hop(1)},
		{NodeID: "low-rerank-high-impact", HopDistance: hop(1)},
	}
	reranker := &fakeReranker{scores: map[string]float64{
		"high-rerank-low-impact": 0.9,
		"low-rerank-high-impact": 0.6,
	}}
	fetchImpact, _ := fakeImpactFetch(map[string]float64{
		"high-rerank-low-impact": 0.0,
		"low-rerank-high-impact": 1.0,
	})

	// Solo rerank_score: "high-rerank-low-impact" (0.9) > "low-rerank-high-impact" (0.6).
	// beta=1, wHop=0, wImpact=1: proximity_boost = impact_score_norm.
	// final("high-rerank-low-impact") = 0.9 + 1*0 = 0.9
	// final("low-rerank-high-impact") = 0.6 + 1*1 = 1.6 -> ORDINE RIBALTATO.
	ranked, err := rerankCore(context.Background(), reranker, fetchImpact, "query", candidates, 10, 1.0, 0.0, 1.0)
	if err != nil {
		t.Fatalf("rerankCore: %v", err)
	}
	if len(ranked) != 2 {
		t.Fatalf("len(ranked) = %d, want 2", len(ranked))
	}
	if ranked[0].Node.NodeID != "low-rerank-high-impact" {
		t.Errorf("ranked[0] = %s, want low-rerank-high-impact (ordine deve riflettere la combinazione, non il solo rerank_score)", ranked[0].Node.NodeID)
	}
}

// Scenario 3 (SPEC-044 §3): candidate set più grande di topK -> risultato
// troncato esattamente a topK, i migliori per final_score.
func TestRerankCore_TruncatesToTopK(t *testing.T) {
	candidates := []hybridsearch.RetrievedNode{
		{NodeID: "n1", HopDistance: hop(1)},
		{NodeID: "n2", HopDistance: hop(1)},
		{NodeID: "n3", HopDistance: hop(1)},
		{NodeID: "n4", HopDistance: hop(1)},
	}
	reranker := &fakeReranker{scores: map[string]float64{"n1": 0.1, "n2": 0.9, "n3": 0.5, "n4": 0.3}}
	fetchImpact, _ := fakeImpactFetch(nil)

	ranked, err := rerankCore(context.Background(), reranker, fetchImpact, "query", candidates, 2, 0.0, 0.5, 0.5)
	if err != nil {
		t.Fatalf("rerankCore: %v", err)
	}
	if len(ranked) != 2 {
		t.Fatalf("len(ranked) = %d, want 2", len(ranked))
	}
	if ranked[0].Node.NodeID != "n2" || ranked[1].Node.NodeID != "n3" {
		t.Errorf("ranked = [%s, %s], want [n2, n3] (i due migliori per final_score)", ranked[0].Node.NodeID, ranked[1].Node.NodeID)
	}
}

// Scenario 4 (SPEC-044 §3): un nodo il cui impact_score non è mai stato
// scritto da T4.3 -> impact_score_norm=0, nessun errore.
func TestRerankCore_MissingImpactScoreDefaultsToZero(t *testing.T) {
	candidates := []hybridsearch.RetrievedNode{{NodeID: "never-scored", HopDistance: hop(1)}}
	reranker := &fakeReranker{scores: map[string]float64{"never-scored": 0.5}}
	fetchImpact, _ := fakeImpactFetch(map[string]float64{}) // mappa vuota: nessun impact_score per nessun nodo

	ranked, err := rerankCore(context.Background(), reranker, fetchImpact, "query", candidates, 10, 1.0, 0.0, 1.0)
	if err != nil {
		t.Fatalf("rerankCore: %v", err)
	}
	if len(ranked) != 1 {
		t.Fatalf("len(ranked) = %d, want 1", len(ranked))
	}
	if ranked[0].ImpactScoreNorm != 0.0 {
		t.Errorf("ImpactScoreNorm = %v, want 0.0", ranked[0].ImpactScoreNorm)
	}
	wantFinal := 0.5 + 1.0*(0.0*0.5+1.0*0.0)
	if !floatsClose(ranked[0].FinalScore, wantFinal) {
		t.Errorf("FinalScore = %v, want %v", ranked[0].FinalScore, wantFinal)
	}
}

// Edge case §4 riga 2: candidate set vuoto -> lista vuota, nessun errore,
// NESSUNA chiamata al servizio di reranking (niente da riordinare).
func TestRerankCore_EmptyCandidatesReturnsEmptyNoCallsMade(t *testing.T) {
	reranker := &fakeReranker{}
	fetchImpact, fetchCalled := fakeImpactFetch(nil)

	ranked, err := rerankCore(context.Background(), reranker, fetchImpact, "query", nil, 10, 0.5, 0.5, 0.5)
	if err != nil {
		t.Fatalf("rerankCore: %v", err)
	}
	if len(ranked) != 0 {
		t.Errorf("ranked = %+v, want vuoto", ranked)
	}
	if reranker.called {
		t.Error("il reranker è stato chiamato con un candidate set vuoto, atteso nessuna chiamata")
	}
	if *fetchCalled {
		t.Error("fetchImpact è stata chiamata con un candidate set vuoto, atteso nessuna chiamata")
	}
}

// Edge case §4 riga 1: topK <= 0 -> errore esplicito PRIMA di chiamare il
// reranker.
func TestRerankCore_TopKZeroOrNegativeFailsBeforeAnyCall(t *testing.T) {
	for _, topK := range []int{0, -1} {
		reranker := &fakeReranker{}
		fetchImpact, fetchCalled := fakeImpactFetch(nil)
		candidates := []hybridsearch.RetrievedNode{{NodeID: "n1"}}

		_, err := rerankCore(context.Background(), reranker, fetchImpact, "query", candidates, topK, 0.5, 0.5, 0.5)
		if err == nil {
			t.Errorf("topK=%d: atteso errore esplicito, ottenuto nil", topK)
		}
		if reranker.called {
			t.Errorf("topK=%d: il reranker è stato chiamato, atteso nessuna chiamata", topK)
		}
		if *fetchCalled {
			t.Errorf("topK=%d: fetchImpact è stata chiamata, atteso nessuna chiamata", topK)
		}
	}
}

// Scenario 5 (SPEC-044 §3, parte core): un fallimento del servizio di
// reranking propaga esplicitamente, non degrada (a differenza della gamba
// vettoriale di T4.1).
func TestRerankCore_RerankerFailurePropagatesExplicitly(t *testing.T) {
	boom := errors.New("servizio di reranking irraggiungibile")
	reranker := failingReranker{err: boom}
	fetchImpact, _ := fakeImpactFetch(nil)
	candidates := []hybridsearch.RetrievedNode{{NodeID: "n1"}}

	_, err := rerankCore(context.Background(), reranker, fetchImpact, "query", candidates, 10, 0.5, 0.5, 0.5)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapping %v", err, boom)
	}
}

type failingReranker struct{ err error }

func (f failingReranker) Rerank(context.Context, string, []string) ([]rerankclient.Result, error) {
	return nil, f.err
}

func floatsClose(a, b float64) bool {
	const eps = 1e-9
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < eps
}
