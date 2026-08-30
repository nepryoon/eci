package consumer

import (
	"strings"
	"testing"
)

func TestGDSInvalidationUsesPartitionGenerationWithoutPartitionScan(t *testing.T) {
	nodeQuery, err := mergeCodeNodeQuery("Function")
	if err != nil {
		t.Fatal(err)
	}
	relationQuery, err := mergeCodeRelationQuery("CALLS")
	if err != nil {
		t.Fatal(err)
	}

	for name, query := range map[string]string{"node": nodeQuery, "relation": relationQuery} {
		if !strings.Contains(query, "GDSPartition") || !strings.Contains(query, "generation") {
			t.Errorf("%s mutation does not advance a partition generation", name)
		}
		if strings.Contains(query, "MATCH (stale:CodeNode)") || strings.Contains(query, "REMOVE stale.") {
			t.Errorf("%s mutation performs an eager full-partition rewrite", name)
		}
		if !strings.Contains(query, "CASE WHEN changed") {
			t.Errorf("%s mutation does not gate generation advancement on a real graph change", name)
		}
		if strings.Contains(query, "ON MATCH SET p.generation = coalesce(p.generation, 0) + 1") {
			t.Errorf("%s mutation advances generation unconditionally on an idempotent retry", name)
		}
	}
}

func TestRelationWriteSetsAbsoluteWeightForRetryIdempotency(t *testing.T) {
	query, err := mergeCodeRelationQuery("CALLS")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "SET r.weight = coalesce($weight, 1)") {
		t.Fatal("relation MERGE must set the absolute payload weight")
	}
	if strings.Contains(query, "r.weight, 0) +") {
		t.Fatal("relation redelivery must not add the payload weight again")
	}
}

func TestGDSMutationLockOrderIsDeterministicAndScopeIsReadAfterEntityLock(t *testing.T) {
	nodeQuery, err := mergeCodeNodeQuery("Function")
	if err != nil {
		t.Fatal(err)
	}
	relationQuery, err := mergeCodeRelationQuery("CALLS")
	if err != nil {
		t.Fatal(err)
	}

	assertFragmentsInOrder(t, "node pre-lock", lockCodeNodeQuery,
		"MERGE (n:CodeNode {id: $id})",
		"SET n._eci_write_lock = coalesce(n._eci_write_lock, 0) + 1",
		"REMOVE n._eci_write_lock",
	)
	assertFragmentsInOrder(t, "node mutation", nodeQuery,
		"MATCH (n:CodeNode {id: $id})",
		"n.tenant_id AS old_tenant_id",
		"ORDER BY scope_tenant_id, scope_repo, scope_acl_group",
		"MERGE (p:GDSPartition",
	)
	if strings.Contains(nodeQuery, "OPTIONAL MATCH (existing:CodeNode") {
		t.Error("node mutation snapshots the previous scope before acquiring the node lock")
	}

	assertFragmentsInOrder(t, "relation endpoint pre-lock", lockCodeRelationEndpointsQuery,
		"ORDER BY endpoint_id",
		"MERGE (endpoint:CodeNode {id: endpoint_id})",
		"SET endpoint._eci_write_lock = coalesce(endpoint._eci_write_lock, 0) + 1",
		"REMOVE endpoint._eci_write_lock",
	)
	assertFragmentsInOrder(t, "relation mutation", relationQuery,
		"MATCH (from:CodeNode {id: $from_id})",
		"from.tenant_id AS from_tenant_id",
		"ORDER BY scope_tenant_id, scope_repo, scope_acl_group",
		"MERGE (p:GDSPartition",
	)
}

func assertFragmentsInOrder(t *testing.T, name, text string, fragments ...string) {
	t.Helper()
	previous := -1
	for _, fragment := range fragments {
		position := strings.Index(text, fragment)
		if position < 0 {
			t.Errorf("%s is missing %q", name, fragment)
			continue
		}
		if position <= previous {
			t.Errorf("%s does not order %q after the preceding lock step", name, fragment)
		}
		previous = position
	}
}
