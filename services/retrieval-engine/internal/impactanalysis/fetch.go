package impactanalysis

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
	"github.com/eci-project/eci/services/retrieval-engine/internal/securityfilter"
)

var allowedRelationshipTypes = map[string]struct{}{
	"CALLS": {}, "IMPORTS": {}, "EXTENDS": {}, "IMPLEMENTS": {},
	"CONTAINS": {}, "DEPENDS_ON": {}, "REFERENCES": {}, "OVERRIDES": {},
	"DERIVED_FROM": {}, "GOVERNED_BY": {}, "CITES": {},
}

func neo4jLevelFetcher(driver neo4j.DriverWithContext, opts Options) levelFetchFunc {
	return func(ctx context.Context, frontierIDs []string, visited map[string]struct{}) (levelFetchResult, error) {
		ctx, observe := securityfilter.Observe(ctx, "neo4j")
		outcome := "error"
		defer func() { observe(outcome) }()
		scope, err := accessscope.FromContext(ctx)
		if err != nil {
			return levelFetchResult{}, err
		}
		query, err := levelCypher(opts.EdgeTypes, opts.Direction)
		if err != nil {
			return levelFetchResult{}, err
		}
		visitedIDs := make([]string, 0, len(visited))
		for id := range visited {
			visitedIDs = append(visitedIDs, id)
		}
		sort.Strings(visitedIDs)

		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)
		params := securityfilter.Neo4jParams(scope)
		params["frontier_ids"] = frontierIDs
		params["visited_ids"] = visitedIDs
		params["domain"] = derefOrNil(opts.Domain)
		params["requested_repos"] = opts.Repos
		params["min_impact_score"] = opts.MinImpactScore
		params["fanout_probe"] = int64(opts.FanoutCap + 1)
		result, err := session.Run(ctx, query, params)
		if err != nil {
			return levelFetchResult{}, err
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return levelFetchResult{}, err
		}

		out := levelFetchResult{Nodes: make([]fetchedNode, 0, len(records))}
		perParent := make(map[string]int, len(frontierIDs))
		for _, rec := range records {
			parent := recordString(rec, "parent_id")
			perParent[parent]++
			if perParent[parent] > opts.FanoutCap {
				out.FanoutTruncated = true
				continue
			}
			path := recordString(rec, "path")
			commit := recordString(rec, "commit")
			var provenance *Provenance
			if path != "" && commit != "" {
				provenance = &Provenance{
					Repo: recordString(rec, "repo"), Path: path,
					StartLine: recordInt(rec, "start_line"), EndLine: recordInt(rec, "end_line"),
					Commit: commit,
				}
			}
			out.Nodes = append(out.Nodes, fetchedNode{
				ParentID: parent, NodeID: recordString(rec, "node_id"),
				Domain:   orDefault(recordString(rec, "domain"), "code"),
				NodeType: recordString(rec, "node_type"), Name: recordString(rec, "name"),
				Signature: recordString(rec, "signature"), ASTHash: recordString(rec, "ast_hash"),
				EdgeType: recordString(rec, "edge_type"), ImpactScore: recordFloat(rec, "impact_score"),
				Provenance: provenance,
			})
		}
		if len(out.Nodes) == 0 {
			outcome = "empty"
		} else {
			outcome = "allow"
		}
		return out, nil
	}
}

func levelCypher(edgeTypes []string, direction string) (string, error) {
	if len(edgeTypes) == 0 {
		return "", fmt.Errorf("impactanalysis: edge_types cannot be empty internally")
	}
	for _, edge := range edgeTypes {
		if _, ok := allowedRelationshipTypes[edge]; !ok {
			return "", fmt.Errorf("impactanalysis: unsupported edge type %q", edge)
		}
	}
	relation := ":" + strings.Join(edgeTypes, "|")
	var pattern string
	switch direction {
	case "REVERSE":
		pattern = fmt.Sprintf("(n)<-[r%s]-(dep:CodeNode)", relation)
	case "FORWARD":
		pattern = fmt.Sprintf("(n)-[r%s]->(dep:CodeNode)", relation)
	default:
		return "", fmt.Errorf("impactanalysis: unsupported direction %q", direction)
	}
	return fmt.Sprintf(`
UNWIND $frontier_ids AS frontier_id
MATCH (n:CodeNode {id: frontier_id})
WHERE n.tenant_id = $tenant_id
  AND n.repo IN $allowed_repos
  AND n.acl_group IN $acl_groups
  AND (size($requested_repos) = 0 OR n.repo IN $requested_repos)
CALL {
  WITH n
  MATCH %s
  WHERE NOT dep.id IN $visited_ids
    AND dep.tenant_id = $tenant_id
    AND dep.repo IN $allowed_repos
    AND dep.acl_group IN $acl_groups
    AND (size($requested_repos) = 0 OR dep.repo IN $requested_repos)
    AND ($domain IS NULL OR dep.domain = $domain)
    AND coalesce(dep.impact_score, 0.0) >= $min_impact_score
  WITH n, dep, min(type(r)) AS edge_type
  ORDER BY coalesce(dep.impact_score, 0.0) DESC, dep.id ASC
  LIMIT $fanout_probe
  RETURN n.id AS parent_id,
         dep.id AS node_id, dep.domain AS domain,
         head([label IN labels(dep) WHERE label <> 'CodeNode']) AS node_type,
         dep.name AS name, dep.signature AS signature, dep.ast_hash AS ast_hash,
         dep.repo AS repo, dep.path AS path, dep.start_line AS start_line,
         dep.end_line AS end_line, dep.commit AS commit,
         coalesce(dep.impact_score, 0.0) AS impact_score, edge_type
}
RETURN parent_id, node_id, domain, node_type, name, signature, ast_hash,
       repo, path, start_line, end_line, commit, impact_score, edge_type
ORDER BY parent_id ASC, impact_score DESC, node_id ASC
`, pattern), nil
}

func derefOrNil(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func recordString(record *neo4j.Record, key string) string {
	value, _ := record.Get(key)
	result, _ := value.(string)
	return result
}

func recordInt(record *neo4j.Record, key string) int {
	value, _ := record.Get(key)
	switch n := value.(type) {
	case int64:
		return int(n)
	case int:
		return n
	case float64:
		return int(n)
	default:
		return 0
	}
}

func recordFloat(record *neo4j.Record, key string) float64 {
	value, _ := record.Get(key)
	switch n := value.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
