package gdsimpact

import (
	"context"
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var ErrStalePartitionGeneration = errors.New("gdsimpact: partition generation stale")

const writeBackQuery = `
UNWIND $rows AS row
WITH row
ORDER BY row.id
MATCH (n:CodeNode {id: row.id})
WHERE n.tenant_id = $tenant_id AND n.repo = $repo AND n.acl_group = $acl_group
SET n._eci_write_lock = coalesce(n._eci_write_lock, 0) + 1
REMOVE n._eci_write_lock
WITH collect({node: n, row: row}) AS targets
WHERE size(targets) = size($rows)
MATCH (p:GDSPartition {tenant_id: $tenant_id, repo: $repo, acl_group: $acl_group})
SET p.write_lock = coalesce(p.write_lock, 0) + 1
WITH p, targets
WHERE p.generation = $partition_generation
UNWIND targets AS target
WITH p, target.node AS n, target.row AS row
SET n.impact_score = row.score, n.community_id = row.community, n.impact_kind = row.impact_kind,
    n.impact_tenant_id = $tenant_id, n.impact_repo = $repo, n.impact_acl_group = $acl_group,
    n.impact_generation = $partition_generation
RETURN count(n) AS written, p.generation AS generation
`

// writeBack — SPEC-043 §2 punto 7: SET n.impact_score/n.community_id/
// n.impact_kind su ciascun nodo del sottografo scoperto (MAI il nodo di
// ingresso, che non riceve un punteggio — vedi score.go). UNWIND in una
// singola query invece di N round-trip separati (nessun requisito
// esplicito nella SPEC su questo, scelta di efficienza non osservabile
// dall'esterno).
func writeBack(ctx context.Context, driver neo4j.DriverWithContext, scope ProjectionScope, partitionGeneration int64, scores []NodeScore) error {
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

	result, err := session.Run(ctx, writeBackQuery, map[string]any{
		"rows": rows, "tenant_id": scope.TenantID, "repo": scope.Repo, "acl_group": scope.ACLGroup,
		"partition_generation": partitionGeneration,
	})
	if err != nil {
		return fmt.Errorf("gdsimpact: write-back impact_score/community_id/impact_kind: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return fmt.Errorf("gdsimpact: write-back impact_score/community_id/impact_kind: %w", err)
	}
	if len(records) != 1 {
		return fmt.Errorf("%w: attesa generation=%d o target scope mutato", ErrStalePartitionGeneration, partitionGeneration)
	}
	written, _, err := neo4j.GetRecordValue[int64](records[0], "written")
	if err != nil || written != int64(len(scores)) {
		return fmt.Errorf("%w: scritti=%d attesi=%d", ErrStalePartitionGeneration, written, len(scores))
	}
	return nil
}
