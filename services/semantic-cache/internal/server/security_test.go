package server

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	semanticcachev1 "github.com/eci-project/eci/libs/go/eci/semanticcache/v1"
)

const canonicalScope = "c94ba9d3ff0d97a5fd8414abbcbad8c01bdc54c35436a9a69496e0b0d184eafa"

func scopedContext(t *testing.T) context.Context {
	t.Helper()
	encoded, err := proto.Marshal(&retrievalv1.SecurityContext{
		TenantId:     "tenant-a",
		UserId:       "user-a",
		AllowedRepos: []string{"repo-b", "repo-a"},
		AclGroups:    []string{"readers", "engineering"},
	})
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

func TestMissingOrMismatchedScopeIsDeniedBeforeRedis(t *testing.T) {
	s := &Server{}
	validKey := func(scope string) *semanticcachev1.CacheKey {
		return &semanticcachev1.CacheKey{
			EntityId: "entity", AstHash: "ast", LogicFingerprint: "logic", AclScope: scope,
		}
	}
	tests := []struct {
		name string
		ctx  context.Context
		key  *semanticcachev1.CacheKey
	}{
		{name: "missing context", ctx: context.Background(), key: validKey(canonicalScope)},
		{name: "empty caller scope", ctx: scopedContext(t), key: validKey("")},
		{name: "forged caller scope", ctx: scopedContext(t), key: validKey(strings.Repeat("f", 64))},
		{name: "nil key", ctx: scopedContext(t), key: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Get(tt.ctx, tt.key)
			want := codes.PermissionDenied
			if tt.key == nil {
				want = codes.InvalidArgument
			}
			if status.Code(err) != want {
				t.Fatalf("Get status = %v, want %v", status.Code(err), want)
			}
		})
	}
	if _, err := s.Put(scopedContext(t), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put(nil) status = %v, want InvalidArgument", status.Code(err))
	}
}

func TestRedisKeyV2IsTupleCollisionSafeAndOpaque(t *testing.T) {
	a := &semanticcachev1.CacheKey{EntityId: "a:b", AstHash: "c", LogicFingerprint: "d", AclScope: canonicalScope}
	b := &semanticcachev1.CacheKey{EntityId: "a", AstHash: "b:c", LogicFingerprint: "d", AclScope: canonicalScope}
	gotA, gotB := redisKey(a), redisKey(b)
	if gotA == gotB {
		t.Fatalf("length-distinct tuples collided: %q", gotA)
	}
	if !strings.HasPrefix(gotA, "scache:v2:") {
		t.Fatalf("key = %q, want v2 namespace", gotA)
	}
	if strings.Contains(gotA, "a:b") || strings.Contains(gotA, canonicalScope) {
		t.Fatalf("physical key exposes raw logical fields: %q", gotA)
	}
}

func TestScopeTelemetryLabelsAreBounded(t *testing.T) {
	if got := boundedOperation("attacker-controlled"); got != "get" {
		t.Fatalf("boundedOperation = %q, want get", got)
	}
	if got := boundedScopeOutcome("tenant-a"); got != "error" {
		t.Fatalf("boundedScopeOutcome = %q, want error", got)
	}
}
