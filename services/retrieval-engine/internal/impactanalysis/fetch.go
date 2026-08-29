package impactanalysis

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
	"github.com/eci-project/eci/services/retrieval-engine/internal/securityfilter"
)

// levelCypher — STESSA struttura di GraphTraversal (T4.1, SPEC-041): stesso
// insieme di tipi di arco per la reverse reachability, stesso principio
// "percorso più breve vince" (qui via min(type(r)) per il tipo d'arco,
// analogo a min(length(path)) per hop_distance). A differenza di
// GraphTraversal, UN SOLO hop per chiamata (nessun `*1..N` a lunghezza
// variabile): SPEC-042 §2 richiede che la traversata proceda
// livello-per-livello, per abilitare streaming genuino e applicare il cap
// PRIMA di esplorare oltre.
const levelCypher = `
MATCH (n:CodeNode) WHERE n.id IN $frontier_ids
  AND n.tenant_id = $tenant_id
  AND n.repo IN $allowed_repos
  AND n.acl_group IN $acl_groups
MATCH (n)<-[r:CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS]-(dep:CodeNode)
WHERE NOT dep.id IN $visited_ids
  AND dep.tenant_id = $tenant_id
  AND dep.repo IN $allowed_repos
  AND dep.acl_group IN $acl_groups
  AND ($domain IS NULL OR dep.domain = $domain)
  AND ($repo   IS NULL OR dep.repo   = $repo)
WITH dep, min(type(r)) AS edge_type
RETURN dep.id         AS node_id,
       dep.domain     AS domain,
       dep.repo       AS repo,
       dep.path       AS path,
       dep.start_line AS start_line,
       dep.end_line   AS end_line,
       dep.commit     AS commit,
       edge_type
ORDER BY node_id
`

// neo4jLevelFetcher costruisce il levelFetchFunc di produzione: UNA query
// Cypher (un hop) per chiamata, riusa il driver Neo4j già esistente in
// retrieval-engine — nessuna nuova connessione (stesso principio di
// GraphTraversal, T4.1).
func neo4jLevelFetcher(driver neo4j.DriverWithContext, domain, repo *string) levelFetchFunc {
	return func(ctx context.Context, frontierIDs []string, visited map[string]struct{}) ([]fetchedNode, error) {
		ctx, observe := securityfilter.Observe(ctx, "neo4j")
		outcome := "error"
		defer func() { observe(outcome) }()
		scope, err := accessscope.FromContext(ctx)
		if err != nil {
			return nil, err
		}
		visitedIDs := make([]string, 0, len(visited))
		for id := range visited {
			visitedIDs = append(visitedIDs, id)
		}

		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)

		params := securityfilter.Neo4jParams(scope)
		params["frontier_ids"] = frontierIDs
		params["visited_ids"] = visitedIDs
		params["domain"] = derefOrNil(domain)
		params["repo"] = derefOrNil(repo)
		result, err := session.Run(ctx, levelCypher, params)
		if err != nil {
			return nil, err
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return nil, err
		}

		out := make([]fetchedNode, 0, len(records))
		for _, rec := range records {
			nodeID, _ := rec.Get("node_id")
			domainVal, _ := rec.Get("domain")
			pathVal, _ := rec.Get("path")
			commitVal, _ := rec.Get("commit")
			edgeTypeVal, _ := rec.Get("edge_type")

			var prov *Provenance
			if s, ok := pathVal.(string); ok && s != "" && commitVal != nil {
				repoVal, _ := rec.Get("repo")
				startLineVal, _ := rec.Get("start_line")
				endLineVal, _ := rec.Get("end_line")
				commitStr, _ := commitVal.(string)
				prov = &Provenance{
					Repo:      asString(repoVal),
					Path:      s,
					StartLine: asInt(startLineVal),
					EndLine:   asInt(endLineVal),
					Commit:    commitStr,
				}
			}

			out = append(out, fetchedNode{
				NodeID:     asString(nodeID),
				Domain:     orDefault(asString(domainVal), "code"),
				EdgeType:   asString(edgeTypeVal),
				Provenance: prov,
			})
		}
		if len(out) == 0 {
			outcome = "empty"
		} else {
			outcome = "allow"
		}
		return out, nil
	}
}

func derefOrNil(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt(v any) int {
	switch n := v.(type) {
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

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
