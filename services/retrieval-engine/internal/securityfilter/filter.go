// Package securityfilter builds datastore-specific predicates exclusively
// from the authenticated access scope. Request parameters may only narrow the
// scope through an explicit intersection.
package securityfilter

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/qdrant/go-client/qdrant"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
)

func Neo4jParams(scope accessscope.Scope) map[string]any {
	return map[string]any{
		"tenant_id":     scope.TenantID,
		"allowed_repos": append([]string(nil), scope.AllowedRepos...),
		"acl_groups":    append([]string(nil), scope.ACLGroups...),
	}
}

// EffectiveRepos intersects an optional functional repository restriction
// with the authenticated allow-list. Empty requested means the entire scope.
func EffectiveRepos(scope accessscope.Scope, requested string) ([]string, bool) {
	if requested == "" {
		return append([]string(nil), scope.AllowedRepos...), true
	}
	if !slices.Contains(scope.AllowedRepos, requested) {
		return nil, false
	}
	return []string{requested}, true
}

func QdrantFilter(scope accessscope.Scope, domain, requestedRepo string) *qdrant.Filter {
	repos, ok := EffectiveRepos(scope, requestedRepo)
	if !ok {
		// An impossible positive match is safer than returning nil. The caller
		// can also short-circuit, but this preserves the fail-closed invariant.
		repos = []string{"__eci_no_authorized_repository__"}
	}
	must := []*qdrant.Condition{
		qdrant.NewMatchKeyword("tenant_id", scope.TenantID),
		qdrant.NewMatchKeywords("repo", repos...),
		qdrant.NewMatchKeywords("acl_group", scope.ACLGroups...),
	}
	if domain != "" {
		must = append(must, qdrant.NewMatchKeyword("domain", domain))
	}
	return &qdrant.Filter{Must: must}
}

func OpenSearchFilter(scope accessscope.Scope, entityIDs []string) map[string]any {
	filters := []any{
		map[string]any{"term": map[string]any{"tenant_id": scope.TenantID}},
		map[string]any{"terms": map[string]any{"repo": append([]string(nil), scope.AllowedRepos...)}},
		map[string]any{"terms": map[string]any{"acl_group": append([]string(nil), scope.ACLGroups...)}},
	}
	if len(entityIDs) > 0 {
		filters = append(filters, map[string]any{"terms": map[string]any{"entity_id": append([]string(nil), entityIDs...)}})
	}
	return map[string]any{"bool": map[string]any{"filter": filters}}
}

func OpenSearchHeaders(scope accessscope.Scope) http.Header {
	h := make(http.Header)
	h.Set("x-proxy-user", scope.UserID)
	h.Set("x-proxy-roles", "eci-query-reader")
	h.Set("x-proxy-ext-tenant_id", scope.TenantID)
	h.Set("x-proxy-ext-allowed_repos", quotedList(scope.AllowedRepos))
	h.Set("x-proxy-ext-acl_groups", quotedList(scope.ACLGroups))
	return h
}

func quotedList(values []string) string {
	encoded, _ := json.Marshal(values) // validated UTF-8 strings; cannot fail
	return strings.TrimSuffix(strings.TrimPrefix(string(encoded), "["), "]")
}
