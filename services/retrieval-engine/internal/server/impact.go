package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/impactanalysis"
)

const (
	defaultImpactMaxDepth  = 4
	defaultImpactFanoutCap = 100
	maxImpactDepth         = 32
	maxImpactNodes         = 10_000
	maxImpactFanoutCap     = 1_000
	maxImpactRepos         = 128
	maxImpactRepoLength    = 256
)

var defaultImpactEdgeTypes = []string{"CALLS", "IMPLEMENTS", "EXTENDS", "OVERRIDES", "DEPENDS_ON", "IMPORTS"}

func (s *Server) ImpactAnalysis(req *retrievalv1.ImpactAnalysisRequest, stream retrievalv1.RetrievalEngine_ImpactAnalysisServer) error {
	ctx := stream.Context()
	scope, err := accessscope.FromContext(ctx)
	if err != nil {
		return status.Error(codes.PermissionDenied, "invalid security scope")
	}
	opts, emptyIntersection, err := validatedImpactOptions(scope, req)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetIncludeSourceText() {
		if s.OpenSearch == nil {
			return status.Error(codes.FailedPrecondition, "include_source_text requires configured OpenSearch")
		}
		opts.HydrateLevel = s.hydrateImpactLevel
	}
	if emptyIntersection {
		return stream.Send(impactAnalysisEventFromInternal(impactanalysis.ImpactEvent{Progress: &impactanalysis.ImpactProgress{CurrentDepth: 1}}))
	}

	emit := func(event impactanalysis.ImpactEvent) error {
		return stream.Send(impactAnalysisEventFromInternal(event))
	}
	if err := impactanalysis.StreamImpact(ctx, s.Driver, req.GetEntryNodeId(), opts, emit); err != nil {
		return impactStatusError(err)
	}
	return nil
}

func impactStatusError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "impact analysis canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "impact analysis deadline exceeded")
	default:
		return status.Error(codes.Unavailable, "impact analysis dependency failure")
	}
}

func validatedImpactOptions(scope accessscope.Scope, req *retrievalv1.ImpactAnalysisRequest) (impactanalysis.Options, bool, error) {
	if req == nil || req.GetEntryNodeId() == "" {
		return impactanalysis.Options{}, false, fmt.Errorf("entry_node_id is required")
	}
	maxNodes := int(req.GetMaxNodes())
	if maxNodes <= 0 || maxNodes > maxImpactNodes {
		return impactanalysis.Options{}, false, fmt.Errorf("max_nodes must be between 1 and %d", maxImpactNodes)
	}
	maxDepth := int(req.GetMaxDepth())
	if maxDepth == 0 {
		maxDepth = defaultImpactMaxDepth
	}
	if maxDepth > maxImpactDepth {
		return impactanalysis.Options{}, false, fmt.Errorf("max_depth must not exceed %d", maxImpactDepth)
	}
	capPerNode := int(req.GetFanoutCapPerHop())
	if capPerNode == 0 {
		capPerNode = defaultImpactFanoutCap
	}
	if capPerNode > maxImpactFanoutCap {
		return impactanalysis.Options{}, false, fmt.Errorf("fanout_cap_per_hop must not exceed %d", maxImpactFanoutCap)
	}
	score := req.GetMinImpactScore()
	if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 || score > 1 {
		return impactanalysis.Options{}, false, fmt.Errorf("min_impact_score must be finite and between 0 and 1")
	}

	edges := append([]string(nil), defaultImpactEdgeTypes...)
	if len(req.GetEdgeTypes()) > 0 {
		edges = make([]string, 0, len(req.GetEdgeTypes()))
		seen := make(map[string]struct{}, len(req.GetEdgeTypes()))
		for _, edge := range req.GetEdgeTypes() {
			value, ok := edgeTypeToString[edge]
			if !ok {
				return impactanalysis.Options{}, false, fmt.Errorf("edge_types contains unsupported value %d", edge)
			}
			if _, duplicate := seen[value]; !duplicate {
				seen[value] = struct{}{}
				edges = append(edges, value)
			}
		}
	}
	direction := "REVERSE"
	switch req.GetDirection() {
	case retrievalv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED, retrievalv1.TraversalDirection_TRAVERSAL_DIRECTION_REVERSE:
	case retrievalv1.TraversalDirection_TRAVERSAL_DIRECTION_FORWARD:
		direction = "FORWARD"
	default:
		return impactanalysis.Options{}, false, fmt.Errorf("direction contains unsupported value %d", req.GetDirection())
	}
	var domain *string
	if req.GetDomain() != retrievalv1.Domain_DOMAIN_UNSPECIFIED {
		value, ok := domainToString[req.GetDomain()]
		if !ok {
			return impactanalysis.Options{}, false, fmt.Errorf("domain contains unsupported value %d", req.GetDomain())
		}
		domain = &value
	}

	repos, empty, err := requestedRepoIntersection(scope.AllowedRepos, req.GetRepos())
	if err != nil {
		return impactanalysis.Options{}, false, err
	}
	return impactanalysis.Options{
		MaxDepth: maxDepth, MaxNodes: maxNodes, FanoutCap: capPerNode,
		EdgeTypes: edges, Direction: direction, MinImpactScore: score,
		Domain: domain, Repos: repos,
	}, empty, nil
}

func requestedRepoIntersection(allowed, requested []string) ([]string, bool, error) {
	if len(requested) == 0 {
		return nil, false, nil
	}
	if len(requested) > maxImpactRepos {
		return nil, false, fmt.Errorf("repos must not contain more than %d values", maxImpactRepos)
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, repo := range allowed {
		allowedSet[repo] = struct{}{}
	}
	intersection := make(map[string]struct{}, len(requested))
	for _, repo := range requested {
		if !validImpactRepo(repo) {
			return nil, false, fmt.Errorf("repos contains an invalid value")
		}
		if _, ok := allowedSet[repo]; ok {
			intersection[repo] = struct{}{}
		}
	}
	result := make([]string, 0, len(intersection))
	for repo := range intersection {
		result = append(result, repo)
	}
	sort.Strings(result)
	return result, len(result) == 0, nil
}

func validImpactRepo(repo string) bool {
	if repo == "" || len(repo) > maxImpactRepoLength || !utf8.ValidString(repo) || strings.TrimSpace(repo) != repo {
		return false
	}
	for _, r := range repo {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (s *Server) hydrateImpactLevel(ctx context.Context, nodes []*impactanalysis.ImpactNode) error {
	batch := make([]hybridsearch.RetrievedNode, len(nodes))
	for i, node := range nodes {
		batch[i].NodeID = node.NodeID
	}
	if err := hybridsearch.HydrateSourceText(ctx, s.OpenSearch, batch); err != nil {
		return err
	}
	for i := range nodes {
		nodes[i].SourceText = batch[i].SourceText
	}
	return nil
}

func impactAnalysisEventFromInternal(event impactanalysis.ImpactEvent) *retrievalv1.ImpactAnalysisEvent {
	switch {
	case event.Node != nil:
		return &retrievalv1.ImpactAnalysisEvent{Event: &retrievalv1.ImpactAnalysisEvent_Node{Node: impactedNodeFromInternal(event.Node)}}
	case event.Progress != nil:
		progress := event.Progress
		return &retrievalv1.ImpactAnalysisEvent{Event: &retrievalv1.ImpactAnalysisEvent_Progress{Progress: &retrievalv1.ImpactProgress{
			NodesEmitted: uint32(progress.NodesExplored), FrontierSize: uint32(progress.FrontierSize),
			CurrentDepth: uint32(progress.CurrentDepth), TruncatedByFanoutCap: progress.TruncatedByFanout,
			TruncatedByDepth: progress.TruncatedByDepth, TruncatedByNodeCap: progress.TruncatedByNodes,
		}}}
	default:
		panic(fmt.Sprintf("impactAnalysisEventFromInternal: empty event %+v", event))
	}
}

func impactedNodeFromInternal(value *impactanalysis.ImpactNode) *retrievalv1.ImpactedNode {
	node := &retrievalv1.RetrievedNode{
		NodeId: value.NodeID, Domain: domainFromProperty(value.Domain), NodeType: value.NodeType,
		Name: value.Name, Signature: value.Signature, SourceText: value.SourceText, AstHash: value.ASTHash,
		Scores: &retrievalv1.NodeScores{HopDistance: uint32(value.HopDistance), ImpactScore: value.ImpactScore},
	}
	if value.Provenance != nil {
		node.Provenance = &retrievalv1.Provenance{
			Repo: value.Provenance.Repo, Path: value.Provenance.Path,
			StartLine: uint32(value.Provenance.StartLine), EndLine: uint32(value.Provenance.EndLine), CommitSha: value.Provenance.Commit,
		}
	}
	path := make([]retrievalv1.EdgeType, 0, len(value.PathEdgeTypes))
	for _, edge := range value.PathEdgeTypes {
		if mapped, ok := stringToEdgeType[edge]; ok {
			path = append(path, mapped)
		}
	}
	kinds := map[string]retrievalv1.ImpactKind{
		"SYNTACTIC":       retrievalv1.ImpactKind_IMPACT_KIND_SYNTACTIC,
		"BEHAVIORAL":      retrievalv1.ImpactKind_IMPACT_KIND_BEHAVIORAL,
		"MODULE_BOUNDARY": retrievalv1.ImpactKind_IMPACT_KIND_MODULE_BOUNDARY,
	}
	return &retrievalv1.ImpactedNode{Node: node, ImpactKind: kinds[value.ImpactKind], PathEdgeTypes: path, Depth: uint32(value.HopDistance)}
}
