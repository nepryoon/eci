package edge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/api-gateway/internal/authn"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeAuthenticator struct {
	result       *retrievalv1.SecurityContext
	err          error
	header       string
	trustedTrace string
	calls        int
}

func (f *fakeAuthenticator) Authenticate(_ context.Context, header, trace string) (*retrievalv1.SecurityContext, error) {
	f.calls++
	f.header = header
	f.trustedTrace = trace
	return f.result, f.err
}

type fakeImpactClient struct {
	call func(context.Context, *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error)
}

func (f *fakeImpactClient) ImpactAnalysis(ctx context.Context, req *retrievalv1.ImpactAnalysisRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
	return f.call(ctx, req)
}

type fakeStream struct {
	grpc.ClientStream
	ctx  context.Context
	recv func() (*retrievalv1.ImpactAnalysisEvent, error)
}

func (s *fakeStream) Context() context.Context                        { return s.ctx }
func (s *fakeStream) Recv() (*retrievalv1.ImpactAnalysisEvent, error) { return s.recv() }

func testSecurityContext() *retrievalv1.SecurityContext {
	return &retrievalv1.SecurityContext{
		TenantId: "tenant-a", UserId: "alice",
		AllowedRepos: []string{"repo-a"}, AclGroups: []string{"dev"},
		TraceId: "0123456789abcdef0123456789abcdef",
	}
}

func encodedContext(t *testing.T, sc *retrievalv1.SecurityContext) string {
	t.Helper()
	raw, err := proto.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func setTrustedRequestHeaders(t *testing.T, request *http.Request, sc *retrievalv1.SecurityContext) {
	t.Helper()
	request.Header.Set(SecurityContextHeader, encodedContext(t, sc))
	request.Header.Set("traceparent", "00-"+sc.GetTraceId()+"-1111111111111111-01")
}

func newTestHandler(t *testing.T, auth Authenticator, impact ImpactClient, random io.Reader) http.Handler {
	t.Helper()
	handler, err := NewEdgeHandler(auth, impact, EdgeConfig{
		MaxJSONBodyBytes: 1024,
		RequestTimeout:   time.Second,
		Random:           random,
		Registerer:       prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func noStreamClient() ImpactClient {
	return &fakeImpactClient{call: func(context.Context, *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
		return nil, status.Error(codes.Unavailable, "unused")
	}}
}

func TestAuthorizeDerivesAndReplacesReservedMetadata(t *testing.T) {
	auth := &fakeAuthenticator{result: testSecurityContext()}
	random := bytes.NewReader(bytes.Repeat([]byte{0x11}, 24))
	handler := newTestHandler(t, auth, noStreamClient(), random)
	req := httptest.NewRequest(http.MethodPost, "/authorize/eci.retrieval.v1.RetrievalEngine/GetNode", strings.NewReader("secret prompt must not be read"))
	req.Header.Set("Authorization", "Bearer signed-token")
	req.Header.Set(SecurityContextHeader, "forged")
	req.Header.Set("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
	if auth.header != "Bearer signed-token" || auth.calls != 1 {
		t.Fatalf("auth call=%d header=%q", auth.calls, auth.header)
	}
	if auth.trustedTrace != strings.Repeat("11", 16) {
		t.Fatalf("trusted trace=%q", auth.trustedTrace)
	}
	if got := response.Header().Get("traceparent"); got != "00-"+strings.Repeat("11", 16)+"-"+strings.Repeat("11", 8)+"-01" {
		t.Fatalf("traceparent=%q", got)
	}
	raw, err := base64.StdEncoding.DecodeString(response.Header().Get(SecurityContextHeader))
	if err != nil {
		t.Fatal(err)
	}
	got := new(retrievalv1.SecurityContext)
	if err := proto.Unmarshal(raw, got); err != nil {
		t.Fatal(err)
	}
	want := testSecurityContext()
	want.TraceId = strings.Repeat("11", 16)
	if !proto.Equal(got, want) {
		t.Fatalf("context=%v", got)
	}
	if strings.Contains(response.Body.String(), "signed-token") || strings.Contains(response.Body.String(), "secret prompt") {
		t.Fatal("secret reflected in auth response")
	}
}

func TestAuthorizeFailsClosedForAuthAndEntropyErrors(t *testing.T) {
	tests := []struct {
		name   string
		auth   *fakeAuthenticator
		random io.Reader
		want   int
	}{
		{"invalid token", &fakeAuthenticator{err: &authn.AuthError{Code: authn.ErrorInvalidToken}}, bytes.NewReader(bytes.Repeat([]byte{0x11}, 24)), http.StatusUnauthorized},
		{"auth backend", &fakeAuthenticator{err: errors.New("oidc down")}, bytes.NewReader(bytes.Repeat([]byte{0x11}, 24)), http.StatusServiceUnavailable},
		{"entropy", &fakeAuthenticator{result: testSecurityContext()}, errReader{}, http.StatusServiceUnavailable},
		{"all-zero trace context", &fakeAuthenticator{result: testSecurityContext()}, bytes.NewReader(make([]byte, 24)), http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, test.auth, noStreamClient(), test.random)
			req := httptest.NewRequest(http.MethodGet, "/authorize/x", nil)
			req.Header.Set("Authorization", "Bearer token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%q", response.Code, test.want, response.Body.String())
			}
			if response.Header().Get(SecurityContextHeader) != "" {
				t.Fatal("security metadata emitted on failure")
			}
			if strings.Contains(response.Body.String(), "oidc down") {
				t.Fatal("internal auth error leaked")
			}
		})
	}
}

func TestAuthorizePreservesValidAuthenticatedEmptyScopeForDownstreamPEP(t *testing.T) {
	securityContext := testSecurityContext()
	securityContext.AllowedRepos = nil
	securityContext.AclGroups = nil
	handler := newTestHandler(t, &fakeAuthenticator{result: securityContext}, noStreamClient(), bytes.NewReader(bytes.Repeat([]byte{0x11}, 24)))
	request := httptest.NewRequest(http.MethodGet, "/authorize/x", nil)
	request.Header.Set("Authorization", "Bearer valid-no-access-token")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
	raw, err := base64.StdEncoding.DecodeString(response.Header().Get(SecurityContextHeader))
	if err != nil {
		t.Fatal(err)
	}
	got := new(retrievalv1.SecurityContext)
	if err := proto.Unmarshal(raw, got); err != nil {
		t.Fatal(err)
	}
	if len(got.GetAllowedRepos()) != 0 || len(got.GetAclGroups()) != 0 {
		t.Fatalf("empty authenticated scope was defaulted: %v", got)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }

func TestSSEUsesOnlyAuthenticatedMetadataAndClearsForgedBodyScope(t *testing.T) {
	sc := testSecurityContext()
	var captured *retrievalv1.ImpactAnalysisRequest
	var capturedMD metadata.MD
	var capturedTraceID string
	client := &fakeImpactClient{call: func(ctx context.Context, req *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
		captured = proto.Clone(req).(*retrievalv1.ImpactAnalysisRequest)
		capturedMD, _ = metadata.FromOutgoingContext(ctx)
		capturedTraceID = oteltrace.SpanContextFromContext(ctx).TraceID().String()
		done := false
		return &fakeStream{ctx: ctx, recv: func() (*retrievalv1.ImpactAnalysisEvent, error) {
			if done {
				return nil, io.EOF
			}
			done = true
			return &retrievalv1.ImpactAnalysisEvent{Event: &retrievalv1.ImpactAnalysisEvent_Progress{Progress: &retrievalv1.ImpactProgress{NodesEmitted: 1}}}, nil
		}}, nil
	}}
	provider := trace.NewTracerProvider(trace.WithSampler(trace.AlwaysSample()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	handler, err := NewEdgeHandler(&fakeAuthenticator{}, client, EdgeConfig{
		MaxJSONBodyBytes: 1024,
		RequestTimeout:   time.Second,
		Random:           bytes.NewReader(make([]byte, 24)),
		Registerer:       prometheus.NewRegistry(),
		Tracer:           provider.Tracer("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"securityContext":{"tenantId":"tenant-b","allowedRepos":["secret"]},"entryNodeId":"A","maxDepth":2}`
	req := httptest.NewRequest(http.MethodPost, SSEPath, strings.NewReader(body))
	setTrustedRequestHeaders(t, req, sc)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, req)

	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status=%d content-type=%q body=%q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
	if captured.GetSecurityContext() != nil || captured.GetEntryNodeId() != "A" || captured.GetMaxDepth() != 2 {
		t.Fatalf("captured request=%v", captured)
	}
	values := capturedMD.Get(SecurityContextMetadataKey)
	if len(values) != 1 {
		t.Fatalf("metadata=%v", capturedMD)
	}
	raw, _ := proto.Marshal(sc)
	if values[0] != string(raw) {
		t.Fatal("outgoing SecurityContext did not match authenticated header")
	}
	if capturedTraceID != sc.GetTraceId() {
		t.Fatalf("downstream trace=%q security trace=%q", capturedTraceID, sc.GetTraceId())
	}
	if !strings.Contains(response.Body.String(), "data: {") || strings.Contains(response.Body.String(), "tenant-b") {
		t.Fatalf("SSE body=%q", response.Body.String())
	}
}

func TestSSEFlushesFirstFrameBeforeStreamCompletes(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	client := &fakeImpactClient{call: func(ctx context.Context, _ *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
		count := 0
		return &fakeStream{ctx: ctx, recv: func() (*retrievalv1.ImpactAnalysisEvent, error) {
			count++
			if count == 1 {
				return &retrievalv1.ImpactAnalysisEvent{Event: &retrievalv1.ImpactAnalysisEvent_Progress{Progress: &retrievalv1.ImpactProgress{NodesEmitted: 1}}}, nil
			}
			<-release
			if count == 2 {
				return &retrievalv1.ImpactAnalysisEvent{Event: &retrievalv1.ImpactAnalysisEvent_Progress{Progress: &retrievalv1.ImpactProgress{NodesEmitted: 2}}}, nil
			}
			return nil, io.EOF
		}}, nil
	}}
	server := httptest.NewServer(newTestHandler(t, &fakeAuthenticator{}, client, bytes.NewReader(make([]byte, 24))))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL+SSEPath, strings.NewReader(`{"entryNodeId":"A"}`))
	setTrustedRequestHeaders(t, req, testSecurityContext())
	ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
	defer cancel()
	response, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, `"nodesEmitted":1`) {
		t.Fatalf("first frame line=%q err=%v", first, err)
	}
	once.Do(func() { close(release) })
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), `"nodesEmitted":2`) {
		t.Fatalf("remaining=%q err=%v", rest, err)
	}
}

func TestSSEMapsPreHeaderErrorAndCancelsOpenStream(t *testing.T) {
	t.Run("pre-header", func(t *testing.T) {
		client := &fakeImpactClient{call: func(context.Context, *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
			return nil, status.Error(codes.Unavailable, "database details")
		}}
		handler := newTestHandler(t, &fakeAuthenticator{}, client, bytes.NewReader(make([]byte, 24)))
		req := httptest.NewRequest(http.MethodPost, SSEPath, strings.NewReader(`{"entryNodeId":"A"}`))
		setTrustedRequestHeaders(t, req, testSecurityContext())
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "database") {
			t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
		}
	})

	t.Run("cancel", func(t *testing.T) {
		observed := make(chan struct{})
		client := &fakeImpactClient{call: func(ctx context.Context, _ *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
			return &fakeStream{ctx: ctx, recv: func() (*retrievalv1.ImpactAnalysisEvent, error) {
				<-ctx.Done()
				close(observed)
				return nil, ctx.Err()
			}}, nil
		}}
		handler := newTestHandler(t, &fakeAuthenticator{}, client, bytes.NewReader(make([]byte, 24)))
		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodPost, SSEPath, strings.NewReader(`{"entryNodeId":"A"}`)).WithContext(ctx)
		setTrustedRequestHeaders(t, req, testSecurityContext())
		done := make(chan struct{})
		go func() { handler.ServeHTTP(httptest.NewRecorder(), req); close(done) }()
		cancel()
		select {
		case <-observed:
		case <-time.After(time.Second):
			t.Fatal("downstream context was not cancelled")
		}
		<-done
	})
}

func TestSSERejectsMalformedOversizedOrMissingAuthenticatedMetadata(t *testing.T) {
	var calls int
	client := &fakeImpactClient{call: func(context.Context, *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
		calls++
		return nil, status.Error(codes.Unavailable, "must not be reached")
	}}
	handler := newTestHandler(t, &fakeAuthenticator{}, client, bytes.NewReader(make([]byte, 24)))
	tests := []struct {
		name   string
		body   string
		header string
		trace  string
		want   int
	}{
		{"missing metadata", `{}`, "", "", http.StatusUnauthorized},
		{"bad metadata", `{}`, "%%%", trustedTraceparentHeader(testSecurityContext()), http.StatusUnauthorized},
		{"missing traceparent", `{}`, encodedContext(t, testSecurityContext()), "", http.StatusUnauthorized},
		{"mismatched traceparent", `{}`, encodedContext(t, testSecurityContext()), "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01", http.StatusUnauthorized},
		{"bad json", `{`, encodedContext(t, testSecurityContext()), trustedTraceparentHeader(testSecurityContext()), http.StatusBadRequest},
		{"too large", `{"entryNodeId":"` + strings.Repeat("x", 1100) + `"}`, encodedContext(t, testSecurityContext()), trustedTraceparentHeader(testSecurityContext()), http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, SSEPath, strings.NewReader(test.body))
			req.Header.Set(SecurityContextHeader, test.header)
			req.Header.Set("traceparent", test.trace)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%q", response.Code, test.want, response.Body.String())
			}
		})
	}
	if calls != 0 {
		t.Fatalf("backend calls=%d", calls)
	}
}

func trustedTraceparentHeader(sc *retrievalv1.SecurityContext) string {
	return "00-" + sc.GetTraceId() + "-1111111111111111-01"
}

func TestSSEEmitsBoundedErrorWhenHelperDeadlineExpiresAfterFirstFrame(t *testing.T) {
	client := &fakeImpactClient{call: func(ctx context.Context, _ *retrievalv1.ImpactAnalysisRequest) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error) {
		count := 0
		return &fakeStream{ctx: ctx, recv: func() (*retrievalv1.ImpactAnalysisEvent, error) {
			count++
			if count == 1 {
				return &retrievalv1.ImpactAnalysisEvent{Event: &retrievalv1.ImpactAnalysisEvent_Progress{Progress: &retrievalv1.ImpactProgress{NodesEmitted: 1}}}, nil
			}
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}}, nil
	}}
	handler, err := NewEdgeHandler(&fakeAuthenticator{}, client, EdgeConfig{
		RequestTimeout: 10 * time.Millisecond,
		Registerer:     prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, SSEPath, strings.NewReader(`{"entryNodeId":"A"}`))
	setTrustedRequestHeaders(t, request, testSecurityContext())
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "event: error") ||
		!strings.Contains(response.Body.String(), `{"code":"stream_error"}`) {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}
