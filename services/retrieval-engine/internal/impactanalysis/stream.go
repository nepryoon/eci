package impactanalysis

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// StreamImpact performs a level-at-a-time bounded traversal. Options must be
// validated by the RPC boundary; defensive validation here protects internal
// callers and tests.
func StreamImpact(ctx context.Context, driver neo4j.DriverWithContext, entryNodeID string, opts Options, emit func(ImpactEvent) error) error {
	return runBFS(ctx, neo4jLevelFetcher(driver, opts), entryNodeID, opts, emit)
}

func runBFS(ctx context.Context, fetch levelFetchFunc, entryNodeID string, opts Options, emit func(ImpactEvent) error) error {
	if entryNodeID == "" {
		return fmt.Errorf("impactanalysis: entry_node_id is required")
	}
	if opts.MaxNodes <= 0 {
		return fmt.Errorf("impactanalysis: max_nodes must be >= 1, got %d", opts.MaxNodes)
	}
	if opts.MaxDepth <= 0 {
		return fmt.Errorf("impactanalysis: max_depth must be >= 1, got %d", opts.MaxDepth)
	}
	if opts.FanoutCap <= 0 {
		return fmt.Errorf("impactanalysis: fanout_cap_per_hop must be >= 1, got %d", opts.FanoutCap)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	visited := map[string]struct{}{entryNodeID: {}}
	paths := map[string][]string{entryNodeID: nil}
	frontier := []string{entryNodeID}
	nodesExplored := 0

	for depth := 1; depth <= opts.MaxDepth && len(frontier) > 0; depth++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := fetch(ctx, frontier, visited)
		if err != nil {
			return fmt.Errorf("impactanalysis: fetch level %d: %w", depth, err)
		}

		progress := &ImpactProgress{
			NodesExplored:     nodesExplored,
			CurrentDepth:      depth,
			TruncatedByFanout: result.FanoutTruncated,
		}
		levelNodes := make([]*ImpactNode, 0, len(result.Nodes))
		nextFrontier := make([]string, 0, len(result.Nodes))
		for _, row := range result.Nodes {
			if _, seen := visited[row.NodeID]; seen {
				continue
			}
			if nodesExplored >= opts.MaxNodes {
				progress.TruncatedByNodes = true
				break
			}
			parentID := row.ParentID
			if parentID == "" && len(frontier) == 1 {
				parentID = frontier[0]
			}
			parentPath, ok := paths[parentID]
			if !ok {
				continue
			}
			path := append(append([]string(nil), parentPath...), row.EdgeType)
			node := &ImpactNode{
				NodeID: row.NodeID, Domain: row.Domain, NodeType: row.NodeType,
				Name: row.Name, Signature: row.Signature, ASTHash: row.ASTHash,
				HopDistance: depth, ImpactScore: row.ImpactScore,
				PathEdgeTypes: path, ImpactKind: classifyImpact(path),
				EdgeType:   row.EdgeType,
				Provenance: row.Provenance,
			}
			visited[row.NodeID] = struct{}{}
			paths[row.NodeID] = path
			nodesExplored++
			levelNodes = append(levelNodes, node)
			nextFrontier = append(nextFrontier, row.NodeID)
		}
		progress.NodesExplored = nodesExplored
		progress.FrontierSize = len(levelNodes)
		if nodesExplored >= opts.MaxNodes && result.FanoutTruncated {
			progress.TruncatedByNodes = true
		}
		progress.Truncated = progress.TruncatedByNodes

		if opts.HydrateLevel != nil && len(levelNodes) > 0 {
			if err := opts.HydrateLevel(ctx, levelNodes); err != nil {
				return fmt.Errorf("impactanalysis: hydrate level %d: %w", depth, err)
			}
		}
		for _, node := range levelNodes {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := emit(ImpactEvent{Node: node}); err != nil {
				return err
			}
		}

		// Distinguish a naturally terminal max depth from real truncation.
		if depth == opts.MaxDepth && len(nextFrontier) > 0 && !progress.TruncatedByNodes {
			probe, err := fetch(ctx, nextFrontier, visited)
			if err != nil {
				return fmt.Errorf("impactanalysis: depth probe: %w", err)
			}
			progress.TruncatedByDepth = len(probe.Nodes) > 0
			if len(probe.Nodes) > 0 && nodesExplored >= opts.MaxNodes {
				progress.TruncatedByNodes = true
				progress.Truncated = true
			}
		}
		if err := emit(ImpactEvent{Progress: progress}); err != nil {
			return err
		}
		if progress.TruncatedByNodes || depth == opts.MaxDepth {
			break
		}
		frontier = nextFrontier
	}
	return nil
}

func classifyImpact(path []string) string {
	kind := "SYNTACTIC"
	for _, edge := range path {
		switch edge {
		case "IMPORTS", "DEPENDS_ON":
			return "MODULE_BOUNDARY"
		case "CALLS", "IMPLEMENTS", "EXTENDS", "OVERRIDES":
			kind = "BEHAVIORAL"
		}
	}
	return kind
}
