package impactanalysis

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type scriptedLevelFetcher struct {
	results []levelFetchResult
	calls   [][]string
}

func TestLevelCypherUsesOnlyWhitelistedDirectionAndBounds(t *testing.T) {
	query, err := levelCypher([]string{"CALLS", "IMPORTS"}, "FORWARD")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"(n)-[r:CALLS|IMPORTS]->(dep:CodeNode)", "LIMIT $fanout_probe", "dep.repo IN $allowed_repos", "dep.acl_group IN $acl_groups", "dep.impact_score, 0.0) >= $min_impact_score"} {
		if !strings.Contains(query, fragment) {
			t.Errorf("query missing %q", fragment)
		}
	}
	if _, err := levelCypher([]string{"CALLS]-(x) DELETE x //"}, "REVERSE"); err == nil {
		t.Fatal("non-whitelisted relationship reached query builder")
	}
	if _, err := levelCypher([]string{"CALLS"}, "BOTH"); err == nil {
		t.Fatal("non-whitelisted direction reached query builder")
	}
}

func (f *scriptedLevelFetcher) fetch(_ context.Context, frontier []string, _ map[string]struct{}) (levelFetchResult, error) {
	f.calls = append(f.calls, append([]string(nil), frontier...))
	i := len(f.calls) - 1
	if i >= len(f.results) {
		return levelFetchResult{}, nil
	}
	return f.results[i], nil
}

func completeOptions() Options {
	return Options{MaxDepth: 3, MaxNodes: 100, FanoutCap: 10}
}

func TestRunBFSCompletePathAndDeterministicKind(t *testing.T) {
	fetch := &scriptedLevelFetcher{results: []levelFetchResult{
		{Nodes: []fetchedNode{{ParentID: "seed", NodeID: "module", EdgeType: "CALLS", ImpactScore: .8}}},
		{Nodes: []fetchedNode{{ParentID: "module", NodeID: "leaf", EdgeType: "IMPORTS", ImpactScore: .7}}},
		{},
	}}
	var events []ImpactEvent
	if err := runBFS(context.Background(), fetch.fetch, "seed", completeOptions(), collectingEmit(&events)); err != nil {
		t.Fatal(err)
	}
	var leaf *ImpactNode
	for _, event := range events {
		if event.Node != nil && event.Node.NodeID == "leaf" {
			leaf = event.Node
		}
	}
	if leaf == nil {
		t.Fatal("leaf not emitted")
	}
	if !reflect.DeepEqual(leaf.PathEdgeTypes, []string{"CALLS", "IMPORTS"}) {
		t.Fatalf("path = %v, want complete two-hop path", leaf.PathEdgeTypes)
	}
	if leaf.ImpactKind != "MODULE_BOUNDARY" {
		t.Fatalf("impact kind = %q, want MODULE_BOUNDARY", leaf.ImpactKind)
	}
}

func TestRunBFSReportsIndependentTruncationReasons(t *testing.T) {
	t.Run("fanout", func(t *testing.T) {
		fetch := &scriptedLevelFetcher{results: []levelFetchResult{{
			Nodes:           []fetchedNode{{ParentID: "seed", NodeID: "n1"}},
			FanoutTruncated: true,
		}}}
		var events []ImpactEvent
		opts := completeOptions()
		opts.MaxDepth = 1
		if err := runBFS(context.Background(), fetch.fetch, "seed", opts, collectingEmit(&events)); err != nil {
			t.Fatal(err)
		}
		p := lastProgress(t, events)
		if !p.TruncatedByFanout || p.TruncatedByNodes {
			t.Fatalf("progress = %+v, want fanout only", p)
		}
	})

	t.Run("node cap", func(t *testing.T) {
		fetch := &scriptedLevelFetcher{results: []levelFetchResult{{Nodes: []fetchedNode{
			{ParentID: "seed", NodeID: "n1"}, {ParentID: "seed", NodeID: "n2"},
		}}}}
		var events []ImpactEvent
		opts := completeOptions()
		opts.MaxNodes = 1
		if err := runBFS(context.Background(), fetch.fetch, "seed", opts, collectingEmit(&events)); err != nil {
			t.Fatal(err)
		}
		p := lastProgress(t, events)
		if !p.TruncatedByNodes || p.TruncatedByFanout || p.TruncatedByDepth {
			t.Fatalf("progress = %+v, want node cap only", p)
		}
	})

	t.Run("depth probe", func(t *testing.T) {
		fetch := &scriptedLevelFetcher{results: []levelFetchResult{
			{Nodes: []fetchedNode{{ParentID: "seed", NodeID: "n1"}}},
			{Nodes: []fetchedNode{{ParentID: "n1", NodeID: "beyond"}}},
		}}
		var events []ImpactEvent
		opts := completeOptions()
		opts.MaxDepth = 1
		if err := runBFS(context.Background(), fetch.fetch, "seed", opts, collectingEmit(&events)); err != nil {
			t.Fatal(err)
		}
		p := lastProgress(t, events)
		if !p.TruncatedByDepth || p.TruncatedByNodes {
			t.Fatalf("progress = %+v, want depth only", p)
		}
		if len(fetch.calls) != 2 {
			t.Fatalf("fetch calls = %d, want bounded final probe", len(fetch.calls))
		}
	})
}

func TestRunBFSHydratesWholeLevelBeforeEmission(t *testing.T) {
	fetch := &scriptedLevelFetcher{results: []levelFetchResult{{Nodes: []fetchedNode{
		{ParentID: "seed", NodeID: "n1"}, {ParentID: "seed", NodeID: "n2"},
	}}}}
	opts := completeOptions()
	opts.MaxDepth = 1
	hydrateCalls := 0
	opts.HydrateLevel = func(_ context.Context, nodes []*ImpactNode) error {
		hydrateCalls++
		if len(nodes) != 2 {
			t.Fatalf("hydrate batch size = %d, want 2", len(nodes))
		}
		for _, node := range nodes {
			node.SourceText = "source:" + node.NodeID
		}
		return nil
	}
	var events []ImpactEvent
	if err := runBFS(context.Background(), fetch.fetch, "seed", opts, collectingEmit(&events)); err != nil {
		t.Fatal(err)
	}
	if hydrateCalls != 1 {
		t.Fatalf("hydrate calls = %d, want one batch call", hydrateCalls)
	}
	for _, event := range events {
		if event.Node != nil && event.Node.SourceText == "" {
			t.Fatalf("node emitted before hydration: %+v", event.Node)
		}
	}
}

func TestRunBFSCancellationStopsBeforeFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fetch := &scriptedLevelFetcher{}
	err := runBFS(ctx, fetch.fetch, "seed", completeOptions(), func(ImpactEvent) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(fetch.calls) != 0 {
		t.Fatalf("fetch called %d times after cancellation", len(fetch.calls))
	}
}

func lastProgress(t *testing.T, events []ImpactEvent) *ImpactProgress {
	t.Helper()
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Progress != nil {
			return events[i].Progress
		}
	}
	t.Fatal("missing progress event")
	return nil
}
