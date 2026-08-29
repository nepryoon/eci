package gdsimpact

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const writeBackQuery = `
		UNWIND $rows AS row
		MATCH (n:CodeNode {id: row.id})
		WHERE n.tenant_id = $tenant_id AND n.repo = $repo AND n.acl_group = $acl_group
		SET n.impact_score = row.score, n.community_id = row.community, n.impact_kind = row.impact_kind,
		    n.impact_tenant_id = $tenant_id, n.impact_repo = $repo, n.impact_acl_group = $acl_group
	`

// writeBack — SPEC-043 §2 punto 7: SET n.impact_score/n.community_id/
// n.impact_kind su ciascun nodo del sottografo scoperto (MAI il nodo di
// ingresso, che non riceve un punteggio — vedi score.go). UNWIND in una
// singola query invece di N round-trip separati (nessun requisito
// esplicito nella SPEC su questo, scelta di efficienza non osservabile
// dall'esterno).
func writeBack(ctx context.Context, driver neo4j.DriverWithContext, scope ProjectionScope, scores []NodeScore) error {
	if len(scores) == 0 {
		return nil
	}

	rows := make([]map[string]any, 0, len(scores))
	for _, s := range scores {
		rows = append(rows, map[string]any{
			"id":          s.NodeID,
			"score":       s.ImpactScore,
			"community":   s.CommunityID,
			"impact_kind": s.ImpactKind,
		})
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.Run(ctx, writeBackQuery, map[string]any{
		"rows": rows, "tenant_id": scope.TenantID, "repo": scope.Repo, "acl_group": scope.ACLGroup,
	})
	if err != nil {
		return fmt.Errorf("gdsimpact: write-back impact_score/community_id/impact_kind: %w", err)
	}
	return nil
}
