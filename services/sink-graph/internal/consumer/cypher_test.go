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
