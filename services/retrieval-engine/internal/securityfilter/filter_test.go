package securityfilter

import (
	"reflect"
	"testing"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
)

func testScope() accessscope.Scope {
	return accessscope.Scope{
		TenantID: "tenant-a", UserID: "user-a",
		AllowedRepos: []string{"repo-a", "repo-b"}, ACLGroups: []string{"dev", "ops"},
	}
}

func TestNeo4jParamsContainOnlyAuthenticatedScope(t *testing.T) {
	got := Neo4jParams(testScope())
	want := map[string]any{
		"tenant_id": "tenant-a", "allowed_repos": []string{"repo-a", "repo-b"}, "acl_groups": []string{"dev", "ops"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Neo4jParams=%#v", got)
	}
}

func TestQdrantFilterAlwaysHasThreeSecurityMustConditions(t *testing.T) {
	filter := QdrantFilter(testScope(), "code", "repo-b")
	if filter == nil || len(filter.GetMust()) != 4 {
		t.Fatalf("must=%v", filter)
	}
	if got := filter.GetMust()[0].GetField().GetKey(); got != "tenant_id" {
		t.Fatalf("first key=%q", got)
	}
	if got := filter.GetMust()[1].GetField().GetKey(); got != "repo" {
		t.Fatalf("second key=%q", got)
	}
	if got := filter.GetMust()[2].GetField().GetKey(); got != "acl_group" {
		t.Fatalf("third key=%q", got)
	}
}

func TestRequestedRepositoryCanOnlyRestrict(t *testing.T) {
	if _, ok := EffectiveRepos(testScope(), "repo-c"); ok {
		t.Fatal("unauthorized requested repo must produce empty intersection")
	}
	if got, ok := EffectiveRepos(testScope(), "repo-b"); !ok || !reflect.DeepEqual(got, []string{"repo-b"}) {
		t.Fatalf("got=%v ok=%v", got, ok)
	}
}

func TestOpenSearchFilterAndHeadersAreScopeDerived(t *testing.T) {
	filter := OpenSearchFilter(testScope(), []string{"n1", "n2"})
	boolQuery, ok := filter["bool"].(map[string]any)
	if !ok || len(boolQuery["filter"].([]any)) != 4 {
		t.Fatalf("filter=%#v", filter)
	}
	headers := OpenSearchHeaders(testScope())
	if headers.Get("x-proxy-user") != "user-a" || headers.Get("x-proxy-ext-tenant_id") != "tenant-a" {
		t.Fatalf("headers=%v", headers)
	}
	if headers.Get("x-proxy-ext-allowed_repos") != `"repo-a","repo-b"` {
		t.Fatalf("repo attribute=%q", headers.Get("x-proxy-ext-allowed_repos"))
	}
}
