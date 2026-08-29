package gdsimpact

import (
	"strings"
	"testing"
)

func TestProjectionScopeIsMandatoryAndBounded(t *testing.T) {
	valid := []string{
		"--entry-node-id", "entry", "--tenant-id", "tenant-a",
		"--repo", "repo-a", "--acl-group", "developers",
	}
	cfg, err := ParseConfig(valid)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scope != (ProjectionScope{TenantID: "tenant-a", Repo: "repo-a", ACLGroup: "developers"}) {
		t.Fatalf("scope=%+v", cfg.Scope)
	}

	tests := [][]string{
		{"--entry-node-id", "entry"},
		{"--entry-node-id", "entry", "--tenant-id", " ", "--repo", "repo-a", "--acl-group", "developers"},
		{"--entry-node-id", "entry", "--tenant-id", "tenant-a", "--repo", "repo\nforged", "--acl-group", "developers"},
		{"--entry-node-id", "entry", "--tenant-id", "tenant-a", "--repo", "repo-a", "--acl-group", strings.Repeat("x", 257)},
	}
	for _, args := range tests {
		if _, err := ParseConfig(args); err == nil {
			t.Errorf("ParseConfig(%q) accepted invalid/missing scope", args)
		}
	}
}

func TestRunRejectsManuallyConstructedMissingScopeBeforeDriverUse(t *testing.T) {
	cfg := Config{EntryNodeID: "entry", MaxDepth: 1, WPPR: .5, WProx: .3, WBC: .2}
	if _, err := Run(t.Context(), nil, cfg, func(string, ...any) {}, Hooks{}); err == nil {
		t.Fatal("Run accepted missing projection scope")
	}
}

func TestStoreBackedQueriesCarryFullProjectionScope(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		{name: "discovery", query: levelCypher},
		{name: "partition generation", query: capturePartitionGenerationQuery},
		{name: "reverse projection", query: projectReverseQuery},
		{name: "undirected projection", query: projectUndirectedQuery},
		{name: "page rank seed", query: pageRankQuery},
		{name: "write-back", query: writeBackQuery},
	}
	for _, item := range queries {
		for _, parameter := range []string{"$tenant_id", "$repo", "$acl_group"} {
			if !strings.Contains(item.query, parameter) {
				t.Errorf("%s query is missing mandatory scope parameter %s", item.name, parameter)
			}
		}
	}
}

func TestWriteBackUsesGenerationFenceAndSinglePartitionLock(t *testing.T) {
	for _, fragment := range []string{
		"GDSPartition", "write_lock", "$partition_generation",
		"n.impact_generation = $partition_generation",
		"size(targets) = size($rows)",
	} {
		if !strings.Contains(writeBackQuery, fragment) {
			t.Errorf("write-back query missing generation fence fragment %q", fragment)
		}
	}
}
