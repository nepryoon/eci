// Unit puri per il motore BFS livello-per-livello di StreamImpact
// (SPEC-042 §7): un `fetch` finto sostituisce Neo4j (nessuna infrastruttura
// reale necessaria), un `emit` finto accumula in uno slice. Scritti PRIMA
// dell'implementazione (TDD) — coprono §3 scenari 2/3/4/5/6, l'edge case
// §4 riga 1/2, e la prova decisiva che l'implementazione è streaming
// GENUINO livello-per-livello, non un batch mascherato (§3 scenario 1):
// se `emit` fallisce dopo il primo evento, `fetch` non deve MAI essere
// invocata per un livello successivo — provabile SOLO se ogni livello è
// effettivamente recuperato on-demand, non tutti insieme in anticipo.
package impactanalysis

import (
	"context"
	"errors"
	"testing"
)

// fakeFetcher registra le chiamate ricevute (frontiera + visited al
// momento della chiamata) e ritorna, in ordine, le risposte configurate
// per ciascun livello successivo.
type fakeFetcher struct {
	responses [][]fetchedNode
	errs      []error
	calls     [][]string // frontierIDs per ogni chiamata, nell'ordine ricevuto
}

func (f *fakeFetcher) fetch(_ context.Context, frontierIDs []string, _ map[string]struct{}) ([]fetchedNode, error) {
	i := len(f.calls)
	f.calls = append(f.calls, append([]string{}, frontierIDs...))
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i < len(f.responses) {
		return f.responses[i], nil
	}
	return nil, nil
}

func collectingEmit(events *[]ImpactEvent) func(ImpactEvent) error {
	return func(e ImpactEvent) error {
		*events = append(*events, e)
		return nil
	}
}

// Scenario 2 (SPEC-042 §3): impact_kind (qui EdgeType) riflette il tipo di
// arco dell'ultimo hop verso quel nodo — CALLS per un nodo raggiunto via
// CALLS, IMPLEMENTS per uno raggiunto via IMPLEMENTS.
func TestStreamImpact_PopulatesEdgeTypePerNode(t *testing.T) {
	f := &fakeFetcher{
		responses: [][]fetchedNode{
			{
				{NodeID: "caller", Domain: "code", EdgeType: "CALLS"},
				{NodeID: "implementor", Domain: "code", EdgeType: "IMPLEMENTS"},
			},
		},
	}
	var events []ImpactEvent
	err := runBFS(context.Background(), f.fetch, "seed", 2, 100, collectingEmit(&events))
	if err != nil {
		t.Fatalf("runBFS: %v", err)
	}

	byID := map[string]*ImpactNode{}
	for _, e := range events {
		if e.Node != nil {
			byID[e.Node.NodeID] = e.Node
		}
	}
	if byID["caller"] == nil || byID["caller"].EdgeType != "CALLS" {
		t.Errorf("caller.EdgeType = %+v, want CALLS", byID["caller"])
	}
	if byID["implementor"] == nil || byID["implementor"].EdgeType != "IMPLEMENTS" {
		t.Errorf("implementor.EdgeType = %+v, want IMPLEMENTS", byID["implementor"])
	}
	if byID["caller"].HopDistance != 1 {
		t.Errorf("caller.HopDistance = %d, want 1", byID["caller"].HopDistance)
	}
}

// Scenario 3 (SPEC-042 §3): più nodi raggiungibili di max_nodes -> la
// traversata si ferma al cap, l'ultimo ImpactProgress ha truncated=true,
// nessun errore.
func TestStreamImpact_CapTruncatesAndReportsTruncated(t *testing.T) {
	f := &fakeFetcher{
		responses: [][]fetchedNode{
			{
				{NodeID: "n1", EdgeType: "CALLS"},
				{NodeID: "n2", EdgeType: "CALLS"},
				{NodeID: "n3", EdgeType: "CALLS"},
			},
		},
	}
	var events []ImpactEvent
	err := runBFS(context.Background(), f.fetch, "seed", 3, 2, collectingEmit(&events))
	if err != nil {
		t.Fatalf("runBFS: %v", err)
	}

	var nodeCount int
	var lastProgress *ImpactProgress
	for _, e := range events {
		if e.Node != nil {
			nodeCount++
		}
		if e.Progress != nil {
			lastProgress = e.Progress
		}
	}
	if nodeCount != 2 {
		t.Errorf("nodeCount = %d, want 2 (cap raggiunto)", nodeCount)
	}
	if lastProgress == nil || !lastProgress.Truncated {
		t.Errorf("lastProgress = %+v, want Truncated=true", lastProgress)
	}
	// Il cap deve fermare la traversata PRIMA di esplorare oltre: nessuna
	// seconda chiamata a fetch per il livello successivo.
	if len(f.calls) != 1 {
		t.Errorf("fetch chiamata %d volte, want 1 (nessun livello successivo dopo il cap)", len(f.calls))
	}
}

// Scenario 4 (SPEC-042 §3): meno nodi raggiungibili di max_nodes ->
// truncated resta false in ogni ImpactProgress.
func TestStreamImpact_NoCapWhenUnderLimit(t *testing.T) {
	f := &fakeFetcher{
		responses: [][]fetchedNode{
			{{NodeID: "n1", EdgeType: "CALLS"}},
			{{NodeID: "n2", EdgeType: "CALLS"}},
		},
	}
	var events []ImpactEvent
	err := runBFS(context.Background(), f.fetch, "seed", 2, 100, collectingEmit(&events))
	if err != nil {
		t.Fatalf("runBFS: %v", err)
	}
	for _, e := range events {
		if e.Progress != nil && e.Progress.Truncated {
			t.Errorf("Progress = %+v, want Truncated=false ovunque", e.Progress)
		}
	}
}

// Scenario 5 (SPEC-042 §3): "entry_node_id" che non produce alcun risultato
// (equivalente, a livello di motore, a un fetch che ritorna zero righe al
// primo livello) -> zero ImpactNode, un ImpactProgress con
// nodes_explored=0/truncated=false, nessun errore.
func TestStreamImpact_EmptyFirstLevelYieldsSingleZeroProgressNoError(t *testing.T) {
	f := &fakeFetcher{responses: [][]fetchedNode{{}}}
	var events []ImpactEvent
	err := runBFS(context.Background(), f.fetch, "does-not-exist", 3, 100, collectingEmit(&events))
	if err != nil {
		t.Fatalf("runBFS: %v", err)
	}
	if len(events) != 1 || events[0].Progress == nil {
		t.Fatalf("events = %+v, want esattamente 1 evento Progress", events)
	}
	p := events[0].Progress
	if p.NodesExplored != 0 || p.Truncated {
		t.Errorf("Progress = %+v, want NodesExplored=0 Truncated=false", p)
	}
}

// Scenario 6 (SPEC-042 §3): max_nodes <= 0 -> errore esplicito PRIMA di
// avviare qualunque traversata (fetch mai invocata).
func TestStreamImpact_MaxNodesZeroOrNegativeFailsBeforeAnyFetch(t *testing.T) {
	for _, maxNodes := range []int{0, -1} {
		f := &fakeFetcher{}
		var events []ImpactEvent
		err := runBFS(context.Background(), f.fetch, "seed", 3, maxNodes, collectingEmit(&events))
		if err == nil {
			t.Errorf("max_nodes=%d: atteso errore esplicito, ottenuto nil", maxNodes)
		}
		if len(f.calls) != 0 {
			t.Errorf("max_nodes=%d: fetch invocata %d volte, want 0", maxNodes, len(f.calls))
		}
		if len(events) != 0 {
			t.Errorf("max_nodes=%d: eventi emessi = %+v, want nessuno", maxNodes, events)
		}
	}
}

// Edge case §4 riga 1: un fallimento di fetch A METÀ traversata (dopo che
// il primo livello ha già emesso nodi) ritorna un errore esplicito — i
// nodi già emessi restano nello slice (comportamento naturale dello
// streaming: un errore a metà stream non invalida quanto già ricevuto).
func TestStreamImpact_FetchFailureMidTraversalReturnsErrorAfterPriorNodesEmitted(t *testing.T) {
	boom := errors.New("neo4j irraggiungibile")
	f := &fakeFetcher{
		responses: [][]fetchedNode{
			{{NodeID: "n1", EdgeType: "CALLS"}},
		},
		errs: []error{nil, boom},
	}
	var events []ImpactEvent
	err := runBFS(context.Background(), f.fetch, "seed", 3, 100, collectingEmit(&events))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapping %v", err, boom)
	}
	var sawN1 bool
	for _, e := range events {
		if e.Node != nil && e.Node.NodeID == "n1" {
			sawN1 = true
		}
	}
	if !sawN1 {
		t.Errorf("n1 (emesso al primo livello, PRIMA del fallimento al secondo) assente da events: %+v", events)
	}
}

// Edge case §4 riga 2: un nodo raggiungibile tramite PIÙ percorsi di
// lunghezza diversa -> impact_kind/hop_distance riflettono il percorso PIÙ
// BREVE. Qui simulato: "shared" scoperto al livello 1 (via CALLS); il fake
// fetcher del livello 2 lo ripropone (come accadrebbe se fosse ANCHE
// raggiungibile in 2 hop via IMPLEMENTS da un altro nodo) — il motore deve
// scartarlo silenziosamente (già visitato), non sovrascrivere hop_distance/
// EdgeType con il percorso più lungo.
func TestStreamImpact_MultiplePathsToSameNode_ShortestPathWins(t *testing.T) {
	f := &fakeFetcher{
		responses: [][]fetchedNode{
			{
				{NodeID: "shared", EdgeType: "CALLS"},
				{NodeID: "other", EdgeType: "CALLS"},
			},
			{
				// "shared" riproposto al livello 2 con un tipo diverso: un
				// motore corretto lo ignora (già visitato al livello 1).
				{NodeID: "shared", EdgeType: "IMPLEMENTS"},
			},
		},
	}
	var events []ImpactEvent
	err := runBFS(context.Background(), f.fetch, "seed", 3, 100, collectingEmit(&events))
	if err != nil {
		t.Fatalf("runBFS: %v", err)
	}

	var sharedOccurrences []*ImpactNode
	for _, e := range events {
		if e.Node != nil && e.Node.NodeID == "shared" {
			sharedOccurrences = append(sharedOccurrences, e.Node)
		}
	}
	if len(sharedOccurrences) != 1 {
		t.Fatalf("shared emesso %d volte, want 1 (dedup su nodo già visitato)", len(sharedOccurrences))
	}
	if sharedOccurrences[0].HopDistance != 1 || sharedOccurrences[0].EdgeType != "CALLS" {
		t.Errorf("shared = %+v, want HopDistance=1 EdgeType=CALLS (percorso più breve, livello 1)", sharedOccurrences[0])
	}
}

// Prova decisiva di streaming GENUINO (SPEC-042 §3 scenario 1: "non un
// batch mascherato"): se `emit` fallisce sul PRIMO evento, `fetch` non deve
// MAI essere invocata per il livello successivo. Questo è vero SOLO se
// l'implementazione recupera un livello alla volta e chiama emit
// progressivamente — un'implementazione che calcolasse l'intera traversata
// PRIMA di iniziare a chiamare emit avrebbe già invocato fetch per TUTTI i
// livelli indipendentemente dal fallimento del primo emit.
func TestStreamImpact_EmitErrorStopsTraversalEarly_ProvesGenuineLevelByLevelStreaming(t *testing.T) {
	boom := errors.New("client se ne è andato")
	f := &fakeFetcher{
		responses: [][]fetchedNode{
			{{NodeID: "n1", EdgeType: "CALLS"}},
			{{NodeID: "n2", EdgeType: "CALLS"}}, // livello 2: MAI dovrebbe essere recuperato
			{{NodeID: "n3", EdgeType: "CALLS"}}, // livello 3: MAI dovrebbe essere recuperato
		},
	}
	emitCalls := 0
	emit := func(e ImpactEvent) error {
		emitCalls++
		if emitCalls == 1 {
			return boom
		}
		return nil
	}

	err := runBFS(context.Background(), f.fetch, "seed", 3, 100, emit)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapping %v", err, boom)
	}
	if len(f.calls) != 1 {
		t.Fatalf("fetch invocata %d volte, want 1 — la traversata non si è fermata subito dopo il fallimento di emit (indizio di un batch precomputato, non streaming genuino)", len(f.calls))
	}
}

// max_depth <= 0: stessa validazione difensiva già stabilita per
// GraphTraversal (T4.1) — non richiesta esplicitamente da nessuno scenario
// di SPEC-042, ma stesso principio di sicurezza/coerenza del progetto.
func TestStreamImpact_MaxDepthZeroOrNegativeFailsBeforeAnyFetch(t *testing.T) {
	f := &fakeFetcher{}
	var events []ImpactEvent
	err := runBFS(context.Background(), f.fetch, "seed", 0, 100, collectingEmit(&events))
	if err == nil {
		t.Fatal("atteso errore esplicito con max_depth=0, ottenuto nil")
	}
	if len(f.calls) != 0 {
		t.Errorf("fetch invocata %d volte, want 0", len(f.calls))
	}
}
