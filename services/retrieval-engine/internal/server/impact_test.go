package server

import (
	"math"
	"reflect"
	"testing"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/retrieval-engine/internal/impactanalysis"
)

func impactScope() accessscope.Scope {
	return accessscope.Scope{TenantID: "tenant", UserID: "user", AllowedRepos: []string{"repo-a", "repo-b"}, ACLGroups: []string{"dev"}}
}

func TestValidatedImpactOptionsAppliesDefaultsAndRestrictsRepos(t *testing.T) {
	req := &retrievalv1.ImpactAnalysisRequest{EntryNodeId: "seed", MaxNodes: 20, Repos: []string{"repo-b", "repo-x", "repo-b"}}
	opts, empty, err := validatedImpactOptions(impactScope(), req)
	if err != nil {
		t.Fatal(err)
	}
	if empty {
		t.Fatal("authorized repo intersection unexpectedly empty")
	}
	if opts.MaxDepth != defaultImpactMaxDepth || opts.FanoutCap != defaultImpactFanoutCap || opts.Direction != "REVERSE" {
		t.Fatalf("defaults = %+v", opts)
	}
	if !reflect.DeepEqual(opts.EdgeTypes, defaultImpactEdgeTypes) {
		t.Fatalf("edge types = %v, want %v", opts.EdgeTypes, defaultImpactEdgeTypes)
	}
	if !reflect.DeepEqual(opts.Repos, []string{"repo-b"}) {
		t.Fatalf("repos = %v, want authenticated intersection", opts.Repos)
	}
}

func TestValidatedImpactOptionsRejectsMalformedAndUnboundedInput(t *testing.T) {
	tests := []*retrievalv1.ImpactAnalysisRequest{
		{MaxNodes: 1},
		{EntryNodeId: "seed", MaxNodes: 0},
		{EntryNodeId: "seed", MaxNodes: maxImpactNodes + 1},
		{EntryNodeId: "seed", MaxNodes: 1, MaxDepth: maxImpactDepth + 1},
		{EntryNodeId: "seed", MaxNodes: 1, FanoutCapPerHop: maxImpactFanoutCap + 1},
		{EntryNodeId: "seed", MaxNodes: 1, MinImpactScore: math.NaN()},
		{EntryNodeId: "seed", MaxNodes: 1, MinImpactScore: 1.1},
		{EntryNodeId: "seed", MaxNodes: 1, EdgeTypes: []retrievalv1.EdgeType{retrievalv1.EdgeType_EDGE_TYPE_UNSPECIFIED}},
		{EntryNodeId: "seed", MaxNodes: 1, Direction: retrievalv1.TraversalDirection(99)},
		{EntryNodeId: "seed", MaxNodes: 1, Repos: []string{" bad"}},
	}
	for _, req := range tests {
		if _, _, err := validatedImpactOptions(impactScope(), req); err == nil {
			t.Errorf("request unexpectedly accepted: %+v", req)
		}
	}
}

func TestImpactedNodeConversionIncludesContentScorePathAndKind(t *testing.T) {
	got := impactedNodeFromInternal(&impactanalysis.ImpactNode{
		NodeID: "n", Domain: "code", NodeType: "Method", Name: "Run",
		Signature: "Run()", SourceText: "return", ASTHash: "hash",
		HopDistance: 2, ImpactScore: .75,
		PathEdgeTypes: []string{"CALLS", "IMPORTS"}, ImpactKind: "MODULE_BOUNDARY",
	})
	if got.GetNode().GetNodeType() != "Method" || got.GetNode().GetName() != "Run" || got.GetNode().GetSourceText() != "return" || got.GetNode().GetAstHash() != "hash" {
		t.Fatalf("node fields incomplete: %+v", got.GetNode())
	}
	if got.GetNode().GetScores().GetImpactScore() != .75 || got.GetImpactKind() != retrievalv1.ImpactKind_IMPACT_KIND_MODULE_BOUNDARY {
		t.Fatalf("score/kind incorrect: %+v", got)
	}
	wantPath := []retrievalv1.EdgeType{retrievalv1.EdgeType_EDGE_TYPE_CALLS, retrievalv1.EdgeType_EDGE_TYPE_IMPORTS}
	if !reflect.DeepEqual(got.GetPathEdgeTypes(), wantPath) {
		t.Fatalf("path = %v, want %v", got.GetPathEdgeTypes(), wantPath)
	}
}
