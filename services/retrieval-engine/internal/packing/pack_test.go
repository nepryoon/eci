// Unit puri per Pack (SPEC-046 §7): nessuna infrastruttura reale, solo
// []rerank.RankedNode costruiti a mano — stesso principio già stabilito
// per rerankCore (T4.4). Scritti PRIMA dell'implementazione (TDD) —
// coprono SPEC-046 §3 scenari 1-6 e gli edge case §4.
package packing

import (
	"strings"
	"testing"

	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerank"
)

func hop(v float64) *float64 { return &v }

func node(id, name, sourceText string, hopDistance *float64, impactNorm, finalScore float64, prov *hybridsearch.Provenance) rerank.RankedNode {
	return rerank.RankedNode{
		Node: hybridsearch.RetrievedNode{
			NodeID:      id,
			Name:        name,
			SourceText:  sourceText,
			HopDistance: hopDistance,
			Provenance:  prov,
		},
		ImpactScoreNorm: impactNorm,
		FinalScore:      finalScore,
	}
}

func defaultBudget() TokenBudget {
	return DefaultTokenBudget()
}

// Scenario 1 (SPEC-046 §3): ciascuna delle quattro sezioni rispetta la
// propria quota (nessuna eccede la propria frazione del budget totale).
func TestPack_SectionsRespectTheirQuota(t *testing.T) {
	budget := TokenBudget{
		Total:                          1000,
		DefinitionsFraction:            0.4, // 400 token
		RelationsFraction:              0.2, // 200 token
		HierarchicalSummariesFraction:  0.2, // 200 token, mai consumata
		FullSourceFraction:             0.2, // 200 token
		FullSourceTopK:                 4,
		FullSourceImpactScoreThreshold: 0.5,
	}

	var candidates []rerank.RankedNode
	for i := 0; i < 5; i++ {
		candidates = append(candidates, node(
			"n"+string(rune('0'+i)),
			"Name"+string(rune('0'+i)),
			"short source text line one\nsecond line irrelevant",
			hop(float64(i+1)),
			0.1, // sotto soglia: nessuno qualifica per FullSource in questo test
			float64(10-i),
			nil,
		))
	}

	got := Pack(candidates, budget)

	defTokens := sumTokens(got.Definitions)
	if defTokens > int(float64(budget.Total)*budget.DefinitionsFraction) {
		t.Errorf("Definitions tokens = %d, want <= %d", defTokens, int(float64(budget.Total)*budget.DefinitionsFraction))
	}
	relTokens := sumTokens(got.Relations)
	if relTokens > int(float64(budget.Total)*budget.RelationsFraction) {
		t.Errorf("Relations tokens = %d, want <= %d", relTokens, int(float64(budget.Total)*budget.RelationsFraction))
	}
	if len(got.HierarchicalSummaries) != 0 {
		t.Errorf("HierarchicalSummaries = %+v, want vuoto (quota riservata ma mai consumata, §5 Non-goal)", got.HierarchicalSummaries)
	}
	fullTokens := sumTokens(got.FullSource)
	if fullTokens > int(float64(budget.Total)*budget.FullSourceFraction) {
		t.Errorf("FullSource tokens = %d, want <= %d", fullTokens, int(float64(budget.Total)*budget.FullSourceFraction))
	}
}

// Scenario 2 (SPEC-046 §3): un candidato con HopDistance==0 (il nodo di
// ingresso stesso) ha il suo SourceText incluso PER INTERO nella sezione
// Sorgente integrale (criterio (a), prima condizione).
func TestPack_HopDistanceZeroGetsFullSourceInFull(t *testing.T) {
	fullText := "riga uno del sorgente\nriga due\nriga tre completa"
	candidates := []rerank.RankedNode{
		node("entry", "Entry", fullText, hop(0), 0.0, 10.0, nil),
	}

	got := Pack(candidates, DefaultTokenBudget())

	frag := findFragment(t, got.FullSource, "entry")
	if frag.Text != fullText {
		t.Errorf("FullSource[entry].Text = %q, want SourceText per intero %q", frag.Text, fullText)
	}
}

// Scenario 3 (SPEC-046 §3): un candidato con ImpactScoreNorm sopra soglia
// ma budget della sezione Sorgente integrale esaurito NON riceve sorgente
// integrale (criterio (c) rispettato, non ignorato).
func TestPack_FullSourceExcludedWhenBudgetExhausted(t *testing.T) {
	budget := DefaultTokenBudget()
	budget.Total = 100
	budget.FullSourceFraction = 0.2 // 20 token = 80 caratteri disponibili

	// "first" (40 caratteri, ~10 token) entra comodamente nel budget da 20
	// token; "second" (500 caratteri, ~125 token) non entra più nello
	// spazio residuo (10 token) — deve essere escluso, non troncato.
	longText := strings.Repeat("x", 500)
	candidates := []rerank.RankedNode{
		node("first", "First", strings.Repeat("y", 40), nil, 0.9, 10.0, nil),
		node("second", "Second", longText, nil, 0.9, 9.0, nil),
	}

	got := Pack(candidates, budget)

	// "first" (punteggio più alto, stesso criterio di priorità) entra
	// comodamente; "second" (anch'esso sopra soglia) non deve ricevere
	// sorgente integrale perché non c'è più spazio nella quota.
	if _, ok := findFragmentOK(got.FullSource, "first"); !ok {
		t.Errorf("first assente da FullSource: %+v", got.FullSource)
	}
	for _, frag := range got.FullSource {
		if frag.NodeID == "second" {
			t.Fatalf("second ha ricevuto sorgente integrale nonostante il budget esaurito: %+v", frag)
		}
	}
}

func findFragmentOK(frags []Fragment, nodeID string) (Fragment, bool) {
	for _, f := range frags {
		if f.NodeID == nodeID {
			return f, true
		}
	}
	return Fragment{}, false
}

// Scenario 4 (SPEC-046 §3): un candidato con Provenance assente (nil) è
// comunque incluso, con un placeholder esplicito al posto della
// citazione, non un valore fabbricato.
func TestPack_MissingProvenanceGetsPlaceholderNotFabricated(t *testing.T) {
	candidates := []rerank.RankedNode{
		node("no-prov", "NoProv", "source", hop(1), 0.0, 10.0, nil),
	}

	got := Pack(candidates, DefaultTokenBudget())

	frag := findFragment(t, got.Definitions, "no-prov")
	if frag.Citation != "provenance non disponibile" {
		t.Errorf("Citation = %q, want %q", frag.Citation, "provenance non disponibile")
	}
}

func TestPack_PresentProvenanceBuildsRealCitation(t *testing.T) {
	prov := &hybridsearch.Provenance{Repo: "myrepo", Path: "a/b.go", StartLine: 10, EndLine: 20, Commit: "abc123"}
	candidates := []rerank.RankedNode{
		node("with-prov", "WithProv", "source", hop(1), 0.0, 10.0, prov),
	}

	got := Pack(candidates, DefaultTokenBudget())

	frag := findFragment(t, got.Definitions, "with-prov")
	want := "myrepo/a/b.go:10-20@abc123"
	if frag.Citation != want {
		t.Errorf("Citation = %q, want %q", frag.Citation, want)
	}
}

// Scenario 5 (SPEC-046 §3): l'ordine finale del packing è "a U" —
// verificato posizione per posizione, non solo che tutti i candidati
// siano presenti.
func TestPack_OrderIsUShaped(t *testing.T) {
	// Punteggi decrescenti n0..n5 (n0 il migliore).
	var candidates []rerank.RankedNode
	scores := []float64{60, 50, 40, 30, 20, 10}
	ids := []string{"n0", "n1", "n2", "n3", "n4", "n5"}
	for i, id := range ids {
		candidates = append(candidates, node(id, id, "text", hop(1), 0.0, scores[i], nil))
	}

	got := Pack(candidates, DefaultTokenBudget())

	// Atteso (algoritmo dichiarato, SPEC-046 §2): n0,n2,n4,n5,n3,n1.
	want := []string{"n0", "n2", "n4", "n5", "n3", "n1"}
	if len(got.Order) != len(want) {
		t.Fatalf("len(Order) = %d, want %d — Order = %v", len(got.Order), len(want), got.Order)
	}
	for i := range want {
		if got.Order[i] != want[i] {
			t.Errorf("Order[%d] = %q, want %q (Order completo = %v)", i, got.Order[i], want[i], got.Order)
		}
	}
}

// Scenario 6 (SPEC-046 §3): due candidati con lo stesso NodeID -> solo uno
// dei due appare nel risultato finale.
func TestPack_DuplicateNodeIDDeduplicated(t *testing.T) {
	candidates := []rerank.RankedNode{
		node("dup", "Dup", "source A", hop(1), 0.0, 10.0, nil),
		node("dup", "Dup", "source B", hop(1), 0.0, 5.0, nil),
	}

	got := Pack(candidates, DefaultTokenBudget())

	count := 0
	for _, id := range got.Order {
		if id == "dup" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("NodeID 'dup' appare %d volte in Order, want 1", count)
	}
	defCount := 0
	for _, f := range got.Definitions {
		if f.NodeID == "dup" {
			defCount++
		}
	}
	if defCount != 1 {
		t.Errorf("NodeID 'dup' appare %d volte in Definitions, want 1", defCount)
	}
}

// Edge case §4 riga 1: candidate set vuoto -> PackedContext con tutte le
// sezioni vuote, nessun errore (Pack non ritorna errore: "nessun errore"
// significa nessun panic/crash).
func TestPack_EmptyCandidateSet(t *testing.T) {
	got := Pack(nil, DefaultTokenBudget())

	if len(got.Order) != 0 || len(got.Definitions) != 0 || len(got.Relations) != 0 ||
		len(got.HierarchicalSummaries) != 0 || len(got.FullSource) != 0 {
		t.Errorf("PackedContext = %+v, want tutte le sezioni vuote", got)
	}
}

// Edge case §4 riga 2: budget totale troppo piccolo per includere anche un
// solo candidato nella sezione Definizioni -> il candidato con FinalScore
// più alto è comunque incluso, anche se eccede leggermente la quota (mai
// una risposta completamente vuota per un budget troppo stretto).
func TestPack_TinyBudgetStillIncludesTopCandidateInDefinitions(t *testing.T) {
	budget := TokenBudget{
		Total:                          1, // quota Definizioni = 0 (int(1*0.4)=0)
		DefinitionsFraction:            0.4,
		RelationsFraction:              0.2,
		HierarchicalSummariesFraction:  0.2,
		FullSourceFraction:             0.2,
		FullSourceTopK:                 4,
		FullSourceImpactScoreThreshold: 0.5,
	}
	candidates := []rerank.RankedNode{
		node("best", "Best", "some reasonably long source text here", hop(1), 0.0, 10.0, nil),
		node("second", "Second", "more text", hop(1), 0.0, 5.0, nil),
	}

	got := Pack(candidates, budget)

	if len(got.Definitions) == 0 {
		t.Fatal("Definitions vuoto, want almeno il candidato con FinalScore più alto ('best')")
	}
	if got.Definitions[0].NodeID != "best" {
		t.Errorf("Definitions[0].NodeID = %q, want %q", got.Definitions[0].NodeID, "best")
	}
}

func sumTokens(frags []Fragment) int {
	total := 0
	for _, f := range frags {
		total += f.TokenCount
	}
	return total
}

func findFragment(t *testing.T, frags []Fragment, nodeID string) Fragment {
	t.Helper()
	for _, f := range frags {
		if f.NodeID == nodeID {
			return f
		}
	}
	t.Fatalf("nodeID %q assente da %+v", nodeID, frags)
	return Fragment{}
}
