package gdsimpact

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const capturePartitionGenerationQuery = `
MERGE (p:GDSPartition {tenant_id: $tenant_id, repo: $repo, acl_group: $acl_group})
ON CREATE SET p.generation = 0, p.write_lock = 0
RETURN p.generation AS generation
`

// capturePartitionGeneration crea lazy il metadata per grafi pre-ADR-0015 e
// cattura il fence prima di discovery/proiezione. Lo scope è già validato dal
// chiamante e non proviene da prompt o output LLM.
func capturePartitionGeneration(ctx context.Context, driver neo4j.DriverWithContext, scope ProjectionScope) (int64, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, capturePartitionGenerationQuery, map[string]any{
		"tenant_id": scope.TenantID,
		"repo":      scope.Repo,
		"acl_group": scope.ACLGroup,
	})
	if err != nil {
		return 0, fmt.Errorf("gdsimpact: cattura generation partizione: %w", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		return 0, fmt.Errorf("gdsimpact: lettura generation partizione: %w", err)
	}
	generation, _, err := neo4j.GetRecordValue[int64](record, "generation")
	if err != nil {
		return 0, fmt.Errorf("gdsimpact: generation partizione non valida: %w", err)
	}
	if generation < 0 {
		return 0, fmt.Errorf("gdsimpact: generation partizione negativa: %d", generation)
	}
	return generation, nil
}
