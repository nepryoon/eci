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
	)
	assertFragmentsInOrder(t, "relation mutation", relationQuery,
		"MATCH (from:CodeNode {id: $from_id})",
		"existing_from.tenant_id",
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
