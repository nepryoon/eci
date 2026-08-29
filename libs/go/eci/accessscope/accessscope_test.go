package accessscope

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
)

func authenticatedContext(t *testing.T, sc *retrievalv1.SecurityContext) context.Context {
	t.Helper()
	encoded, err := proto.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"eci-security-context-bin", string(encoded),
	))
	var got context.Context
	_, err = secctx.UnaryServerInterceptor()(incoming, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		got = ctx
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestFromContextValidatesCopiesAndSorts(t *testing.T) {
	ctx := authenticatedContext(t, &retrievalv1.SecurityContext{
		TenantId:     "tenant-a",
		UserId:       "user-a",
		AllowedRepos: []string{"repo-b", "repo-a", "repo-a"},
		AclGroups:    []string{"ops", "dev", "dev"},
	})

	scope, err := FromContext(ctx)
	if err != nil {
		t.Fatalf("FromContext: %v", err)
	}
	if scope.TenantID != "tenant-a" {
		t.Fatalf("TenantID=%q", scope.TenantID)
	}
	if !reflect.DeepEqual(scope.AllowedRepos, []string{"repo-a", "repo-b"}) {
		t.Fatalf("AllowedRepos=%v", scope.AllowedRepos)
	}
	if !reflect.DeepEqual(scope.ACLGroups, []string{"dev", "ops"}) {
		t.Fatalf("ACLGroups=%v", scope.ACLGroups)
	}
}

func TestFromContextFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{"absent", context.Background()},
		{"empty_tenant", authenticatedContext(t, &retrievalv1.SecurityContext{UserId: "u", AllowedRepos: []string{"r"}, AclGroups: []string{"g"}})},
		{"empty_repos", authenticatedContext(t, &retrievalv1.SecurityContext{TenantId: "t", UserId: "u", AclGroups: []string{"g"}})},
		{"empty_groups", authenticatedContext(t, &retrievalv1.SecurityContext{TenantId: "t", UserId: "u", AllowedRepos: []string{"r"}})},
		{"blank_value", authenticatedContext(t, &retrievalv1.SecurityContext{TenantId: "t", UserId: "u", AllowedRepos: []string{" "}, AclGroups: []string{"g"}})},
		{"header_injection", authenticatedContext(t, &retrievalv1.SecurityContext{TenantId: "t\r\nx", UserId: "u", AllowedRepos: []string{"r"}, AclGroups: []string{"g"}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := FromContext(tt.ctx); err == nil {
				t.Fatal("expected fail-closed error")
			}
		})
	}
}

func TestScopeReturnsDefensiveCopies(t *testing.T) {
	ctx := authenticatedContext(t, &retrievalv1.SecurityContext{
		TenantId: "t", UserId: "u", AllowedRepos: []string{"r"}, AclGroups: []string{"g"},
	})
	scope, err := FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	scope.AllowedRepos[0] = "forged"
	again, err := FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.AllowedRepos[0] != "r" {
		t.Fatalf("context scope was mutated: %v", again.AllowedRepos)
	}
}

func TestFingerprintIsCanonicalAndExcludesUserIdentity(t *testing.T) {
	want := "c94ba9d3ff0d97a5fd8414abbcbad8c01bdc54c35436a9a69496e0b0d184eafa"
	first := Scope{
		TenantID:     "tenant-a",
		UserID:       "user-one",
		AllowedRepos: []string{"repo-a", "repo-b"},
		ACLGroups:    []string{"engineering", "readers"},
	}
	second := Scope{
		TenantID:     "tenant-a",
		UserID:       "different-user",
		AllowedRepos: []string{"repo-b", "repo-a", "repo-a"},
		ACLGroups:    []string{"readers", "engineering", "readers"},
	}
	if got := Fingerprint(first); got != want {
		t.Fatalf("Fingerprint(first) = %q, want shared vector %q", got, want)
	}
	if got := Fingerprint(second); got != want {
		t.Fatalf("Fingerprint(second) = %q, want canonical %q", got, want)
	}
}

func TestFingerprintSeparatesLengthDelimitedScopes(t *testing.T) {
	a := Scope{TenantID: "ab", AllowedRepos: []string{"c"}, ACLGroups: []string{"d"}}
	b := Scope{TenantID: "a", AllowedRepos: []string{"bc"}, ACLGroups: []string{"d"}}
	if Fingerprint(a) == Fingerprint(b) {
		t.Fatal("different length-delimited scopes produced the same fingerprint")
	}
}
