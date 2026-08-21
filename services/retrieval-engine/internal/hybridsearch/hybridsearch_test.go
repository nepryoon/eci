// Unit puri per RRFFuse/ApplyTopologicalProximity (SPEC-041 §7): nessuna
// infrastruttura reale, solo liste di RetrievedNode costruite a mano —
// scenari 1-4 di SPEC-041 §3, scritti PRIMA dell'implementazione (TDD).
package hybridsearch

import "testing"

func f(v float64) *float64 { return &v }
func i(v int) *int         { return &v }

const rrfK = 60

// Scenario 1: un nodo presente sia nel grafo sia nel vettoriale accumula
// la somma dei due contributi RRF (1/(k+graph_rank) + 1/(k+vector_rank)),
// non solo uno dei due.
func TestRRFFuse_NodeInBothLists_SumsContributions(t *testing.T) {
	graph := []RetrievedNode{{NodeID: "n1", Source: "graph", GraphRank: i(1), HopDistance: f(1)}}
	vector := []RetrievedNode{{NodeID: "n1", Source: "vector", VectorRank: i(2), VectorScore: f(0.9)}}

	fused := RRFFuse(graph, vector, rrfK)

	n, ok := fused["n1"]
	if !ok {
		t.Fatalf("n1 assente da fused: %+v", fused)
	}
	// Costanti Go non tipizzate sono sommate con precisione arbitraria a
	// compile time: per confrontarsi con l'accumulo a runtime (due
	// arrotondamenti float64 distinti, stesso ordine dell'implementazione),
	// il valore atteso va calcolato anch'esso a runtime.
	kFloat := float64(rrfK)
	want := 1.0/(kFloat+1) + 1.0/(kFloat+2)
	if n.RRFScore != want {
		t.Errorf("RRFScore = %v, want %v", n.RRFScore, want)
	}
	if n.Source != "fused" {
		t.Errorf("Source = %q, want %q", n.Source, "fused")
	}
}

// Scenario 2: un nodo presente SOLO nel grafo riflette solo il contributo grafo.
func TestRRFFuse_NodeOnlyInGraph_ReflectsOnlyGraphContribution(t *testing.T) {
	graph := []RetrievedNode{{NodeID: "n1", Source: "graph", GraphRank: i(3), HopDistance: f(2)}}

	fused := RRFFuse(graph, nil, rrfK)

	n, ok := fused["n1"]
	if !ok {
		t.Fatalf("n1 assente da fused: %+v", fused)
	}
	want := 1.0 / (rrfK + 3)
	if n.RRFScore != want {
		t.Errorf("RRFScore = %v, want %v", n.RRFScore, want)
	}
	if n.VectorRank != nil {
		t.Errorf("VectorRank = %v, want nil", n.VectorRank)
	}
}

// Scenario 3: un nodo presente SOLO nel vettoriale riflette solo il
// contributo vettoriale.
func TestRRFFuse_NodeOnlyInVector_ReflectsOnlyVectorContribution(t *testing.T) {
	vector := []RetrievedNode{{NodeID: "n1", Source: "vector", VectorRank: i(5), VectorScore: f(0.5)}}

	fused := RRFFuse(nil, vector, rrfK)

	n, ok := fused["n1"]
	if !ok {
		t.Fatalf("n1 assente da fused: %+v", fused)
	}
	want := 1.0 / (rrfK + 5)
	if n.RRFScore != want {
		t.Errorf("RRFScore = %v, want %v", n.RRFScore, want)
	}
	if n.GraphRank != nil {
		t.Errorf("GraphRank = %v, want nil", n.GraphRank)
	}
}

// hop_distance del nodo fuso prende il minimo tra le due occorrenze se
// entrambe presenti (SPEC-041 §2 RRFFuse).
func TestRRFFuse_HopDistanceTakesMinOfBothOccurrences(t *testing.T) {
	graph := []RetrievedNode{{NodeID: "n1", Source: "graph", GraphRank: i(1), HopDistance: f(3)}}
	vector := []RetrievedNode{{NodeID: "n1", Source: "vector", VectorRank: i(1), HopDistance: f(1)}}

	fused := RRFFuse(graph, vector, rrfK)

	n := fused["n1"]
	if n.HopDistance == nil || *n.HopDistance != 1 {
		t.Errorf("HopDistance = %v, want 1", n.HopDistance)
	}
}

// I campi vettoriali/grafo mancanti sulla prima occorrenza vengono riempiti
// dalla seconda se questa li possiede (SPEC-041 §2 RRFFuse) — qui la prima
// occorrenza (grafo) non ha provenance, la seconda (vettoriale) sì.
func TestRRFFuse_MissingProvenanceFilledFromSecondOccurrence(t *testing.T) {
	prov := &Provenance{Repo: "local", Path: "a.go"}
	graph := []RetrievedNode{{NodeID: "n1", Source: "graph", GraphRank: i(1)}}
	vector := []RetrievedNode{{NodeID: "n1", Source: "vector", VectorRank: i(1), Provenance: prov}}

	fused := RRFFuse(graph, vector, rrfK)

	n := fused["n1"]
	if n.Provenance == nil || n.Provenance.Path != "a.go" {
		t.Errorf("Provenance = %+v, want Path=a.go (riempita dalla seconda occorrenza)", n.Provenance)
	}
}

// Scenario 4: due nodi fusi con lo stesso rrf_score ma hop_distance diversa
// — quello con hop_distance minore ottiene un combined_score più alto dopo
// il boost di prossimità topologica.
func TestApplyTopologicalProximity_CloserHopDistanceHigherCombinedScore(t *testing.T) {
	fused := map[string]*RetrievedNode{
		"far":   {NodeID: "far", RRFScore: 0.5, HopDistance: f(4)},
		"close": {NodeID: "close", RRFScore: 0.5, HopDistance: f(1)},
	}

	ranked := ApplyTopologicalProximity(fused, 0.15)

	scores := map[string]float64{}
	for _, n := range ranked {
		scores[n.NodeID] = n.CombinedScore
	}
	if scores["close"] <= scores["far"] {
		t.Errorf("combined_score(close)=%v, combined_score(far)=%v — atteso close > far", scores["close"], scores["far"])
	}
	wantClose := 0.5 + 0.15*(1.0/(1+1))
	wantFar := 0.5 + 0.15*(1.0/(1+4))
	if scores["close"] != wantClose {
		t.Errorf("combined_score(close) = %v, want %v", scores["close"], wantClose)
	}
	if scores["far"] != wantFar {
		t.Errorf("combined_score(far) = %v, want %v", scores["far"], wantFar)
	}
}

// Nodo senza hop_distance (mai osservato dal grafo, solo vettoriale senza
// fusione con nessun risultato grafo): proximity = 0, combined_score =
// rrf_score puro (SPEC-041 §2 ApplyTopologicalProximity).
func TestApplyTopologicalProximity_NoHopDistanceZeroProximityBoost(t *testing.T) {
	fused := map[string]*RetrievedNode{
		"n1": {NodeID: "n1", RRFScore: 0.3, HopDistance: nil},
	}

	ranked := ApplyTopologicalProximity(fused, 0.15)

	if len(ranked) != 1 {
		t.Fatalf("len(ranked) = %d, want 1", len(ranked))
	}
	if ranked[0].CombinedScore != 0.3 {
		t.Errorf("CombinedScore = %v, want 0.3 (nessun boost senza hop_distance)", ranked[0].CombinedScore)
	}
}

// Risultato ordinato per combined_score decrescente.
func TestApplyTopologicalProximity_SortsDescendingByCombinedScore(t *testing.T) {
	fused := map[string]*RetrievedNode{
		"low":  {NodeID: "low", RRFScore: 0.1},
		"high": {NodeID: "high", RRFScore: 0.9},
		"mid":  {NodeID: "mid", RRFScore: 0.5},
	}

	ranked := ApplyTopologicalProximity(fused, 0.15)

	if len(ranked) != 3 {
		t.Fatalf("len(ranked) = %d, want 3", len(ranked))
	}
	if ranked[0].NodeID != "high" || ranked[1].NodeID != "mid" || ranked[2].NodeID != "low" {
		t.Errorf("ordine = [%s, %s, %s], want [high, mid, low]", ranked[0].NodeID, ranked[1].NodeID, ranked[2].NodeID)
	}
}
