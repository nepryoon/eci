package hybridsearch

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
	"github.com/eci-project/eci/services/retrieval-engine/internal/securityfilter"
)

// graphTraversalCypher — STESSA query di D5 `_GRAPH_TRAVERSAL_CYPHER`, con
// UNA aggiunta dichiarata (SPEC-045 §2): `dep.name AS name` nella RETURN
// già esistente — nessuna nuova query, nessun cambiamento a
// WHERE/ORDER BY/LIMIT (stesso insieme di righe, stesso ordine). %d è
// max_depth, letterale intero validato (>=1) prima dell'interpolazione —
// Cypher non parametrizza i bound di lunghezza variabile (D5 §note, MAI
// accettare max_depth come stringa non validata: qui è un int Go, non
// testo, stesso principio di sicurezza già stabilito in SPEC-016 per
// ExpandNeighbors).
const graphTraversalCypher = `
MATCH (seed:CodeNode {id: $entry_node_id})
WHERE seed.tenant_id = $tenant_id
  AND seed.repo IN $allowed_repos
  AND seed.acl_group IN $acl_groups
MATCH path = (seed)<-[r:CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS*1..%d]-(dep:CodeNode)
WHERE all(x IN nodes(path) WHERE x.tenant_id = $tenant_id
      AND x.repo IN $allowed_repos AND x.acl_group IN $acl_groups)
  AND ($domain IS NULL OR dep.domain = $domain)
  AND ($repo   IS NULL OR dep.repo   = $repo)
WITH DISTINCT dep, min(length(path)) AS hop_distance
RETURN dep.id            AS node_id,
       dep.domain        AS domain,
       dep.name          AS name,
       dep.repo          AS repo,
       dep.path          AS path,
       dep.start_line    AS start_line,
       dep.end_line      AS end_line,
       dep.commit        AS commit,
       hop_distance      AS hop_distance
ORDER BY hop_distance ASC
LIMIT $graph_limit
`

// GraphTraversal — port 1:1 di D5 `_graph_traversal`: traversata inversa da
// entryNodeID, bounded a maxDepth, pruning DISTINCT. Riusa il driver Neo4j
// già esistente in retrieval-engine (T1.4), nessuna nuova connessione.
func GraphTraversal(ctx context.Context, driver neo4j.DriverWithContext, entryNodeID string, maxDepth int, domain, repo *string, graphLimit int) ([]RetrievedNode, error) {
	ctx, observe := securityfilter.Observe(ctx, "neo4j")
	outcome := "error"
	defer func() { observe(outcome) }()
	scope, err := accessscope.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("security scope non valido: %w", err)
	}
	if maxDepth < 1 {
		return nil, fmt.Errorf("max_depth deve essere >= 1, ricevuto %d", maxDepth)
	}
	cypher := fmt.Sprintf(graphTraversalCypher, maxDepth)

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	params := securityfilter.Neo4jParams(scope)
	params["entry_node_id"] = entryNodeID
	params["domain"] = derefOrNil(domain)
	params["repo"] = derefOrNil(repo)
	params["graph_limit"] = int64(graphLimit)
	result, err := session.Run(ctx, cypher, params)
	if err != nil {
		return nil, newHybridSearchError("Neo4j traversal fallito", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, newHybridSearchError("Neo4j traversal fallito (lettura risultati)", err)
	}

	out := make([]RetrievedNode, 0, len(records))
	for rank, rec := range records {
		nodeID, _ := rec.Get("node_id")
		domainVal, _ := rec.Get("domain")
		nameVal, _ := rec.Get("name")
		pathVal, _ := rec.Get("path")
		commitVal, _ := rec.Get("commit")
		hopVal, _ := rec.Get("hop_distance")

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

		hop := float64(asInt(hopVal))
		r := rank + 1
		out = append(out, RetrievedNode{
			NodeID:      asString(nodeID),
			Domain:      orDefault(asString(domainVal), "code"),
			Source:      "graph",
			Name:        asString(nameVal),
			HopDistance: &hop,
			GraphRank:   &r,
			Provenance:  prov,
		})
	}
	if len(out) == 0 {
		outcome = "empty"
	} else {
		outcome = "allow"
	}
	return out, nil
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
