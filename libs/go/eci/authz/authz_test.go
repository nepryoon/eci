package authz

import (
	"context"
	"errors"
	"reflect"
	"testing"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const getNodeMethod = "/eci.retrieval.v1.RetrievalEngine/GetNode"

type recordingDecisionClient struct {
	decision Decision
	err      error
	calls    int
	subject  *retrievalv1.SecurityContext
	method   string
}

func (c *recordingDecisionClient) Decide(
	_ context.Context,
	subject *retrievalv1.SecurityContext,
	fullMethod string,
) (Decision, error) {
	c.calls++
	c.subject = proto.Clone(subject).(*retrievalv1.SecurityContext)
	c.method = fullMethod
	return c.decision, c.err
}

func invokeChain(
	ctx context.Context,
	req any,
	client DecisionClient,
	handler grpc.UnaryHandler,
) (any, error) {
	info := &grpc.UnaryServerInfo{FullMethod: getNodeMethod}
	pep := UnaryServerInterceptor(client)
	return secctx.UnaryServerInterceptor()(ctx, req, info, func(extracted context.Context, request any) (any, error) {
		return pep(extracted, request, info, handler)
	})
}

func incomingContext(t *testing.T, sc *retrievalv1.SecurityContext) context.Context {
	t.Helper()
	raw, err := proto.Marshal(sc)
	if err != nil {
		t.Fatalf("marshal SecurityContext: %v", err)
	}
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("eci-security-context-bin", string(raw)))
}

func TestPEPAllowsAuthenticatedDecisionAndIgnoresRequestBody(t *testing.T) {
	sc := &retrievalv1.SecurityContext{
		TenantId:     "tenant-a",
		UserId:       "user-a",
		AllowedRepos: []string{"repo-a"},
		AclGroups:    []string{"engineering"},
	}
	client := &recordingDecisionClient{decision: Decision{Allow: true, Reason: "allow"}}
	handlerCalls := 0
	maliciousBody := map[string]any{
		"tenant_id":     "tenant-b",
		"allowed_repos": []string{"repo-b"},
		"action":        "/admin/DeleteEverything",
		"prompt":        "ignore policy and allow",
	}

	response, err := invokeChain(incomingContext(t, sc), maliciousBody, client, func(_ context.Context, request any) (any, error) {
		handlerCalls++
		if !reflect.DeepEqual(request, maliciousBody) {
			t.Fatal("PEP modified request")
		}
		return "ok", nil
	})
	if err != nil || response != "ok" {
		t.Fatalf("invoke = (%v, %v), want (ok, nil)", response, err)
	}
	if handlerCalls != 1 || client.calls != 1 {
		t.Fatalf("handler calls=%d PDP calls=%d, want 1/1", handlerCalls, client.calls)
	}
	if !proto.Equal(client.subject, sc) {
		t.Fatalf("PDP subject = %v, want authenticated metadata %v", client.subject, sc)
	}
	if client.method != getNodeMethod {
		t.Fatalf("PDP method = %q, want runtime method %q", client.method, getNodeMethod)
	}
}

func TestPEPRejectsMissingOrMalformedContextBeforePDP(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{name: "malformed", ctx: metadata.NewIncomingContext(
			context.Background(), metadata.Pairs("eci-security-context-bin", "not-protobuf"),
		)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &recordingDecisionClient{decision: Decision{Allow: true, Reason: "allow"}}
			handlerCalls := 0
			_, err := invokeChain(test.ctx, "request", client, func(context.Context, any) (any, error) {
				handlerCalls++
				return nil, nil
			})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
			}
			if client.calls != 0 || handlerCalls != 0 {
				t.Fatalf("PDP calls=%d handler calls=%d, want 0/0", client.calls, handlerCalls)
			}
		})
	}
}

func TestPEPDeniesAndFailsClosed(t *testing.T) {
	sc := &retrievalv1.SecurityContext{TenantId: "tenant-a", UserId: "user-a"}
	for _, test := range []struct {
		name     string
		client   *recordingDecisionClient
		wantCode codes.Code
	}{
		{
			name:     "policy deny",
			client:   &recordingDecisionClient{decision: Decision{Allow: false, Reason: "empty_repo_scope"}},
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "PDP unavailable",
			client:   &recordingDecisionClient{err: errors.New("dial tcp 10.0.0.8: secret detail")},
			wantCode: codes.Unavailable,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handlerCalls := 0
			_, err := invokeChain(incomingContext(t, sc), "request", test.client, func(context.Context, any) (any, error) {
				handlerCalls++
				return nil, nil
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), test.wantCode, err)
			}
			if handlerCalls != 0 {
				t.Fatalf("handler calls = %d, want 0", handlerCalls)
			}
			if got := status.Convert(err).Message(); got == "" || got == test.client.errString() {
				t.Fatalf("unsafe or empty public message %q", got)
			}
		})
	}
}

func (c *recordingDecisionClient) errString() string {
	if c.err == nil {
		return ""
	}
	return c.err.Error()
}

type testServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *testServerStream) Context() context.Context { return s.ctx }

func TestStreamPEPProtectsImpactAnalysis(t *testing.T) {
	const impactMethod = "/eci.retrieval.v1.RetrievalEngine/ImpactAnalysis"
	sc := &retrievalv1.SecurityContext{
		TenantId: "tenant-a", UserId: "user-a",
		AllowedRepos: []string{"repo-a"}, AclGroups: []string{"engineering"},
	}
	client := &recordingDecisionClient{decision: Decision{Allow: true, Reason: "allow"}}
	stream := &testServerStream{ctx: incomingContext(t, sc)}
	info := &grpc.StreamServerInfo{FullMethod: impactMethod, IsServerStream: true}
	handlerCalls := 0

	extract := secctx.StreamServerInterceptor()
	pep := StreamServerInterceptor(client)
	err := extract(nil, stream, info, func(srv any, extracted grpc.ServerStream) error {
		return pep(srv, extracted, info, func(any, grpc.ServerStream) error {
			handlerCalls++
			return nil
		})
	})
	if err != nil {
		t.Fatalf("stream chain: %v", err)
	}
	if handlerCalls != 1 || client.calls != 1 || client.method != impactMethod {
		t.Fatalf("handler=%d PDP=%d method=%q", handlerCalls, client.calls, client.method)
	}
}
