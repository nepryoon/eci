//go:build integration

package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/api-gateway/internal/authn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const envoyImage = "envoyproxy/envoy:v1.39.0@sha256:d59f7f5fa10cff6d5892b6c5e7df5c9297ddfb2c3683e33fbfb82da24de4fa66"

type integrationAuthenticator struct {
	calls atomic.Int64
}

func (a *integrationAuthenticator) Authenticate(_ context.Context, header, traceID string) (*retrievalv1.SecurityContext, error) {
	a.calls.Add(1)
	identity := map[string][2]string{
		"Bearer integration-valid": {"tenant-a", "alice"},
		"Bearer integration-bob":   {"tenant-b", "bob"},
	}[header]
	if identity == [2]string{} {
		return nil, &authn.AuthError{Code: authn.ErrorInvalidToken}
	}
	return &retrievalv1.SecurityContext{
		TenantId:     identity[0],
		UserId:       identity[1],
		AllowedRepos: []string{"repo-a"},
		AclGroups:    []string{"dev"},
		TraceId:      traceID,
	}, nil
}

type observedCall struct {
	method          string
	securityContext *retrievalv1.SecurityContext
	bodyContext     *retrievalv1.SecurityContext
	traceparent     string
	tracestate      string
	baggage         string
	authorization   string
	rateLimitKey    string
	requestDeadline string
}

type integrationBackend struct {
	retrievalv1.UnimplementedRetrievalEngineServer
	mu            sync.Mutex
	calls         []observedCall
	releaseSecond chan struct{}
	cancelled     chan struct{}
	cancelOnce    sync.Once
	unknownCalls  int
}

func (b *integrationBackend) observe(ctx context.Context, method string, bodyContext *retrievalv1.SecurityContext) {
	var trusted *retrievalv1.SecurityContext
	var traceparent string
	var tracestate string
	var baggage string
	var authorization string
	var rateLimitKey string
	var requestDeadline string
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		values := incoming.Get(SecurityContextMetadataKey)
		if len(values) == 1 {
			candidate := new(retrievalv1.SecurityContext)
			if proto.Unmarshal([]byte(values[0]), candidate) == nil {
				trusted = candidate
			}
		}
		if values := incoming.Get("traceparent"); len(values) == 1 {
			traceparent = values[0]
		}
		if values := incoming.Get("tracestate"); len(values) == 1 {
			tracestate = values[0]
		}
		if values := incoming.Get("baggage"); len(values) == 1 {
			baggage = values[0]
		}
		if values := incoming.Get("authorization"); len(values) == 1 {
			authorization = values[0]
		}
		if values := incoming.Get("x-eci-rate-limit-key"); len(values) == 1 {
			rateLimitKey = values[0]
		}
		if values := incoming.Get("x-eci-request-deadline-unix-ms"); len(values) == 1 {
			requestDeadline = values[0]
		}
	}
	var clonedBody *retrievalv1.SecurityContext
	if bodyContext != nil {
		clonedBody = proto.Clone(bodyContext).(*retrievalv1.SecurityContext)
	}
	b.mu.Lock()
	b.calls = append(b.calls, observedCall{
		method: method, securityContext: trusted, bodyContext: clonedBody,
		requestDeadline: requestDeadline,
		traceparent:     traceparent, tracestate: tracestate, baggage: baggage,
		authorization: authorization,
		rateLimitKey:  rateLimitKey,
	})
	b.mu.Unlock()
}

func (b *integrationBackend) GetNode(ctx context.Context, request *retrievalv1.GetNodeRequest) (*retrievalv1.GetNodeResponse, error) {
	b.observe(ctx, "GetNode", request.GetSecurityContext())
	if request.GetNodeId() == "retry-control" {
		return nil, status.Error(codes.Unavailable, "retry control")
	}
	if request.GetNodeId() == "timeout-control" {
		time.Sleep(30 * time.Millisecond)
	}
	if request.GetNodeId() == "direct-deadline-budget" {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &retrievalv1.GetNodeResponse{Node: &retrievalv1.RetrievedNode{NodeId: request.GetNodeId(), Name: "OrderService.Process"}}, nil
}

func (b *integrationBackend) ImpactAnalysis(request *retrievalv1.ImpactAnalysisRequest, stream grpc.ServerStreamingServer[retrievalv1.ImpactAnalysisEvent]) error {
	b.observe(stream.Context(), "ImpactAnalysis", request.GetSecurityContext())
	if err := stream.Send(progressEvent(1)); err != nil {
		return err
	}
	if request.GetEntryNodeId() == "cancel" {
		<-stream.Context().Done()
		b.cancelOnce.Do(func() { close(b.cancelled) })
		return stream.Context().Err()
	}
	if request.GetEntryNodeId() == "deadline" {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	select {
	case <-b.releaseSecond:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(progressEvent(2))
}

func progressEvent(nodes uint32) *retrievalv1.ImpactAnalysisEvent {
	return &retrievalv1.ImpactAnalysisEvent{Event: &retrievalv1.ImpactAnalysisEvent_Progress{Progress: &retrievalv1.ImpactProgress{NodesEmitted: nodes}}}
}

func (b *integrationBackend) snapshot() []observedCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]observedCall(nil), b.calls...)
}

func (b *integrationBackend) unknownCallCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.unknownCalls
}

func TestEnvoyGatewayEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := &integrationBackend{
		releaseSecond: make(chan struct{}),
		cancelled:     make(chan struct{}),
	}
	grpcListener, grpcServer := startIntegrationGRPC(t, backend)
	grpcPort := listenerPort(t, grpcListener)
	authenticator := new(integrationAuthenticator)
	helperListener, _ := startIntegrationHelper(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(grpcPort)), authenticator)
	helperPort := listenerPort(t, helperListener)

	root := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	certificatePath, keyPath, roots := writeIntegrationTLSIdentity(t)
	oldDefaultClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "localhost",
	}}}
	t.Cleanup(func() { http.DefaultClient = oldDefaultClient })
	configPath := renderIntegrationEnvoyConfig(t, root, helperPort, grpcPort)
	baseURL, gatewayAddress := startIntegrationEnvoy(t, ctx, root, configPath, certificatePath, keyPath, helperPort, grpcPort)

	t.Run("health is public and bounded", func(t *testing.T) {
		response, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", response.StatusCode)
		}
	})

	t.Run("missing authentication fails before backend", func(t *testing.T) {
		before := len(backend.snapshot())
		response := postJSON(t, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", `{ "nodeId": "A" }`, "")
		defer response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized || len(backend.snapshot()) != before {
			t.Fatalf("status=%d backend calls=%d", response.StatusCode, len(backend.snapshot())-before)
		}
	})

	t.Run("plaintext is rejected before bearer authentication", func(t *testing.T) {
		before := len(backend.snapshot())
		request, _ := http.NewRequest(http.MethodPost, "http://"+gatewayAddress+"/eci.retrieval.v1.RetrievalEngine/GetNode", strings.NewReader(`{"nodeId":"A"}`))
		request.Header.Set("Authorization", "Bearer integration-valid")
		response, err := (&http.Client{Timeout: time.Second}).Do(request)
		if err == nil {
			_ = response.Body.Close()
			t.Fatalf("plaintext unexpectedly returned status %d", response.StatusCode)
		}
		if len(backend.snapshot()) != before {
			t.Fatal("plaintext request reached authenticated backend")
		}
	})

	t.Run("partial headers are closed before authentication", func(t *testing.T) {
		beforeAuth := authenticator.calls.Load()
		connection, err := tls.Dial("tcp", gatewayAddress, &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: "localhost",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if _, err := io.WriteString(connection, "GET /eci.retrieval.v1.RetrievalEngine/GetNode HTTP/1.1\r\nHost: localhost\r\nX-Slow:"); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 256)
		read, readErr := connection.Read(buffer)
		elapsed := time.Since(started)
		if elapsed > 750*time.Millisecond {
			t.Fatalf("partial headers were not rejected within the configured bound: err=%v elapsed=%s", readErr, elapsed)
		}
		if readErr == nil && !bytes.Contains(buffer[:read], []byte(" 408 ")) {
			t.Fatalf("partial headers returned an unexpected response: %q", buffer[:read])
		}
		if authenticator.calls.Load() != beforeAuth {
			t.Fatal("partial headers reached authenticator")
		}
	})

	t.Run("JSON is transcoded and reserved metadata is replaced", func(t *testing.T) {
		body := `{"nodeId":"node-a","securityContext":{"tenantId":"tenant-forged","userId":"mallory","allowedRepos":["repo-secret"],"aclGroups":["admins"],"traceId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer integration-valid")
		request.Header.Set(SecurityContextHeader, base64.StdEncoding.EncodeToString([]byte("forged")))
		request.Header.Set("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
		request.Header.Set("tracestate", "attacker=value")
		request.Header.Set("baggage", "tenant=tenant-forged")
		request.Header.Set(RateLimitKeyHeader, "forged-shared-bucket")
		request.Header.Set(RequestDeadlineHeader, "4102444800000")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		payload, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusOK || !strings.Contains(string(payload), "OrderService.Process") {
			t.Fatalf("status=%d body=%q", response.StatusCode, payload)
		}
		call := lastCall(t, backend, "GetNode")
		assertTrustedContext(t, call.securityContext)
		assertTrustedTraceparent(t, call)
		if call.tracestate != "" || call.baggage != "" {
			t.Fatalf("untrusted propagation state reached backend: tracestate=%q baggage=%q", call.tracestate, call.baggage)
		}
		if call.authorization != "" {
			t.Fatalf("bearer token reached backend: %q", call.authorization)
		}
		if call.rateLimitKey != "" {
			t.Fatalf("internal rate-limit key reached backend: %q", call.rateLimitKey)
		}
		if call.requestDeadline != "" {
			t.Fatalf("internal request deadline reached backend: %q", call.requestDeadline)
		}
		if call.bodyContext.GetTenantId() != "tenant-forged" {
			t.Fatalf("expected body provenance to remain distinguishable, got %v", call.bodyContext)
		}
	})

	t.Run("native gRPC traverses auth and preserves HTTP2", func(t *testing.T) {
		connection, err := grpc.NewClient(gatewayAddress, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: "localhost",
		})))
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		forged, _ := proto.Marshal(&retrievalv1.SecurityContext{TenantId: "tenant-forged", AllowedRepos: []string{"secret"}})
		requestContext := metadata.NewOutgoingContext(ctx, metadata.Pairs(
			"authorization", "Bearer integration-valid",
			SecurityContextMetadataKey, string(forged),
		))
		response, err := retrievalv1.NewRetrievalEngineClient(connection).GetNode(requestContext, &retrievalv1.GetNodeRequest{NodeId: "grpc-node"})
		if err != nil || response.GetNode().GetNodeId() != "grpc-node" {
			t.Fatalf("response=%v err=%v", response, err)
		}
		call := lastCall(t, backend, "GetNode")
		assertTrustedContext(t, call.securityContext)
		assertTrustedTraceparent(t, call)
		if call.authorization != "" {
			t.Fatalf("gRPC bearer token reached backend: %q", call.authorization)
		}
		beforeUnknown := backend.unknownCallCount()
		unknownResponse := new(retrievalv1.GetNodeResponse)
		err = connection.Invoke(requestContext, "/eci.retrieval.v1.RetrievalEngine/Unknown", &retrievalv1.GetNodeRequest{}, unknownResponse)
		if err == nil {
			t.Fatal("unknown native gRPC method was accepted")
		}
		if backend.unknownCallCount() != beforeUnknown {
			t.Fatal("unknown native gRPC method reached backend")
		}

		streamContext, cancelStream := context.WithCancel(requestContext)
		stream, err := retrievalv1.NewRetrievalEngineClient(connection).ImpactAnalysis(streamContext, &retrievalv1.ImpactAnalysisRequest{EntryNodeId: "grpc-stream"})
		if err != nil {
			t.Fatal(err)
		}
		first, err := stream.Recv()
		if err != nil || first.GetProgress().GetNodesEmitted() != 1 {
			t.Fatalf("first stream event=%v err=%v", first, err)
		}
		cancelStream()
	})

	t.Run("forged proxy and router controls cannot amplify upstream", func(t *testing.T) {
		before := len(backend.snapshot())
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", strings.NewReader(`{"nodeId":"retry-control"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer integration-valid")
		request.Header.Set("X-Forwarded-For", "127.0.0.1")
		request.Header.Set("X-Envoy-Retry-On", "5xx")
		request.Header.Set("X-Envoy-Max-Retries", "3")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if got := len(backend.snapshot()) - before; got != 1 {
			t.Fatalf("forged retry controls produced %d upstream attempts", got)
		}

		before = len(backend.snapshot())
		request, _ = http.NewRequest(http.MethodPost, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", strings.NewReader(`{"nodeId":"timeout-control"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer integration-valid")
		request.Header.Set("X-Forwarded-For", "127.0.0.1")
		request.Header.Set("X-Envoy-Upstream-Rq-Timeout-Ms", "1")
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK || len(backend.snapshot())-before != 1 {
			t.Fatalf("forged timeout changed request: status=%d attempts=%d", response.StatusCode, len(backend.snapshot())-before)
		}
	})

	t.Run("SSE helper deadline emits bounded terminal frame through Envoy", func(t *testing.T) {
		response := postJSON(t, baseURL+SSEPath, `{"entryNodeId":"deadline"}`, "Bearer integration-valid")
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || !strings.Contains(string(payload), "event: error") ||
			!strings.Contains(string(payload), `{"code":"stream_error"}`) {
			t.Fatalf("status=%d body=%q", response.StatusCode, payload)
		}
	})

	t.Run("SSE deadline includes body buffering time", func(t *testing.T) {
		before := len(backend.snapshot())
		connection, err := tls.Dial("tcp", gatewayAddress, &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: "localhost",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		body := `{"entryNodeId":"slow-body"}`
		headers := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer integration-valid\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", SSEPath, len(body))
		if _, err := io.WriteString(connection, headers+body[:1]); err != nil {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
		if _, err := io.WriteString(connection, body[1:]); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusGatewayTimeout {
			payload, _ := io.ReadAll(response.Body)
			t.Fatalf("status=%d body=%q", response.StatusCode, payload)
		}
		if len(backend.snapshot()) != before {
			t.Fatal("slow body reached gRPC backend after absolute deadline")
		}
	})

	t.Run("direct retrieval deadline includes body buffering and upstream time", func(t *testing.T) {
		before := len(backend.snapshot())
		connection, err := tls.Dial("tcp", gatewayAddress, &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: "localhost",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		body := `{"nodeId":"direct-deadline-budget"}`
		headers := fmt.Sprintf("POST /eci.retrieval.v1.RetrievalEngine/GetNode HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer integration-valid\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(body))
		if _, err := io.WriteString(connection, headers+body[:1]); err != nil {
			t.Fatal(err)
		}
		time.Sleep(150 * time.Millisecond)
		if _, err := io.WriteString(connection, body[1:]); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusGatewayTimeout {
			payload, _ := io.ReadAll(response.Body)
			t.Fatalf("status=%d body=%q", response.StatusCode, payload)
		}
		if delta := len(backend.snapshot()) - before; delta > 1 {
			t.Fatalf("direct deadline produced %d upstream attempts", delta)
		}
	})

	t.Run("SSE flushes before upstream completion", func(t *testing.T) {
		response := postJSON(t, baseURL+SSEPath, `{"entryNodeId":"stream","securityContext":{"tenantId":"tenant-forged"}}`, "Bearer integration-valid")
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
			t.Fatalf("status=%d content-type=%q", response.StatusCode, response.Header.Get("Content-Type"))
		}
		reader := bufio.NewReader(response.Body)
		first, err := reader.ReadString('\n')
		if err != nil || !strings.Contains(first, `"nodesEmitted":1`) {
			t.Fatalf("first=%q err=%v", first, err)
		}
		close(backend.releaseSecond)
		remainder, err := io.ReadAll(reader)
		if err != nil || !strings.Contains(string(remainder), `"nodesEmitted":2`) {
			t.Fatalf("remainder=%q err=%v", remainder, err)
		}
		call := lastCall(t, backend, "ImpactAnalysis")
		assertTrustedContext(t, call.securityContext)
		assertTrustedTraceparent(t, call)
		if call.bodyContext != nil {
			t.Fatalf("SSE forwarded body SecurityContext: %v", call.bodyContext)
		}
	})

	t.Run("SSE cancellation reaches gRPC backend", func(t *testing.T) {
		requestContext, cancelRequest := context.WithCancel(ctx)
		request, _ := http.NewRequestWithContext(requestContext, http.MethodPost, baseURL+SSEPath, strings.NewReader(`{"entryNodeId":"cancel"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer integration-valid")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(response.Body)
		if _, err := reader.ReadString('\n'); err != nil {
			t.Fatal(err)
		}
		cancelRequest()
		_ = response.Body.Close()
		select {
		case <-backend.cancelled:
		case <-time.After(5 * time.Second):
			t.Fatal("gRPC backend did not observe SSE cancellation")
		}
	})

	t.Run("strict JSON and route validation reject passthrough", func(t *testing.T) {
		before := len(backend.snapshot())
		for _, target := range []string{
			"/eci.retrieval.v1.RetrievalEngine/GetNode?unknown=true",
			"/eci.retrieval.v1.RetrievalEngine/Unknown",
			"/not-allow-listed",
		} {
			response := postJSON(t, baseURL+target, `{}`, "Bearer integration-valid")
			_ = response.Body.Close()
			if response.StatusCode != http.StatusBadRequest && response.StatusCode != http.StatusNotFound {
				t.Fatalf("target=%s status=%d", target, response.StatusCode)
			}
		}
		for name, body := range map[string]string{
			"malformed": `{`,
			"oversized": `{"nodeId":"` + strings.Repeat("x", (1<<20)+1) + `"}`,
		} {
			response := postJSON(t, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", body, "Bearer integration-valid")
			_ = response.Body.Close()
			if response.StatusCode != http.StatusBadRequest && response.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("%s status=%d", name, response.StatusCode)
			}
		}
		if got := len(backend.snapshot()); got != before {
			t.Fatalf("strictly rejected requests reached backend: delta=%d", got-before)
		}
	})

	t.Run("rate limit returns Retry-After without upstream call", func(t *testing.T) {
		before := len(backend.snapshot())
		allowed, limited := 0, 0
		for index := 0; index < 30; index++ {
			response := postJSON(t, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", fmt.Sprintf(`{"nodeId":"rate-%d"}`, index), "Bearer integration-valid")
			if response.StatusCode == http.StatusTooManyRequests {
				limited++
				if response.Header.Get("Retry-After") == "" {
					t.Fatal("429 omitted Retry-After")
				}
			} else if response.StatusCode == http.StatusOK {
				allowed++
			} else {
				t.Fatalf("unexpected status=%d", response.StatusCode)
			}
			_ = response.Body.Close()
		}
		delta := len(backend.snapshot()) - before
		if limited == 0 || delta != allowed {
			t.Fatalf("allowed=%d limited=%d backend delta=%d", allowed, limited, delta)
		}
		response := postJSON(t, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", `{"nodeId":"other-caller"}`, "Bearer integration-bob")
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("one caller exhausted another caller bucket: status=%d", response.StatusCode)
		}
	})

	t.Run("coarse pre-auth limit protects verifier and backend", func(t *testing.T) {
		beforeBackend := len(backend.snapshot())
		beforeAuth := authenticator.calls.Load()
		limited := 0
		const attempts = 150
		for index := 0; index < attempts; index++ {
			response := postJSON(t, baseURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", `{}`, fmt.Sprintf("Bearer invalid-%d", index))
			if response.StatusCode == http.StatusTooManyRequests {
				limited++
				if response.Header.Get("Retry-After") == "" {
					t.Fatal("pre-auth 429 omitted Retry-After")
				}
			} else if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unexpected invalid-token status=%d", response.StatusCode)
			}
			_ = response.Body.Close()
		}
		if limited == 0 || authenticator.calls.Load()-beforeAuth >= attempts {
			t.Fatalf("limited=%d verifier calls=%d attempts=%d", limited, authenticator.calls.Load()-beforeAuth, attempts)
		}
		if len(backend.snapshot()) != beforeBackend {
			t.Fatal("unauthenticated flood reached backend")
		}
	})

	t.Run("auth helper outage fails closed", func(t *testing.T) {
		deadHelperPort := availablePort(t)
		outageConfigPath := renderIntegrationEnvoyConfig(t, root, deadHelperPort, grpcPort)
		outageURL, _ := startIntegrationEnvoy(t, ctx, root, outageConfigPath, certificatePath, keyPath, deadHelperPort, grpcPort)
		before := len(backend.snapshot())
		response := postJSON(t, outageURL+"/eci.retrieval.v1.RetrievalEngine/GetNode", `{}`, "Bearer integration-valid")
		defer response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable || len(backend.snapshot()) != before {
			t.Fatalf("status=%d backend calls=%d", response.StatusCode, len(backend.snapshot())-before)
		}
	})

	grpcServer.GracefulStop()
}

func startIntegrationGRPC(t *testing.T, backend *integrationBackend) (net.Listener, *grpc.Server) {
	t.Helper()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, _ grpc.ServerStream) error {
		backend.mu.Lock()
		backend.unknownCalls++
		backend.mu.Unlock()
		return status.Error(codes.Unimplemented, "unknown method")
	}))
	retrievalv1.RegisterRetrievalEngineServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener, server
}

func startIntegrationHelper(t *testing.T, retrievalAddress string, authenticator Authenticator) (net.Listener, *http.Server) {
	t.Helper()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	connection, err := grpc.NewClient(
		retrievalAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithTracerProvider(provider),
			otelgrpc.WithPropagators(propagation.TraceContext{}),
		)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	handler, err := NewEdgeHandler(authenticator, retrievalv1.NewRetrievalEngineClient(connection), EdgeConfig{
		RequestTimeout: 200 * time.Millisecond,
		Registerer:     prometheus.NewRegistry(),
		Tracer:         provider.Tracer("integration"),
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); _ = listener.Close() })
	return listener, server
}

func startIntegrationEnvoy(t *testing.T, ctx context.Context, root, configPath, certificatePath, keyPath string, helperPort, grpcPort int) (string, string) {
	t.Helper()
	if binary := os.Getenv("ECI_ENVOY_BINARY"); binary != "" {
		publicPort := availablePort(t)
		adminPort := availablePort(t)
		contents, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatal(err)
		}
		config := string(contents)
		config = strings.ReplaceAll(config, testcontainers.HostInternal, "127.0.0.1")
		config = strings.Replace(config, "port_value: 8080", "port_value: "+strconv.Itoa(publicPort), 1)
		config = strings.Replace(config, "port_value: 9901", "port_value: "+strconv.Itoa(adminPort), 1)
		config = strings.ReplaceAll(config, "/etc/envoy/retrieval.pb", filepath.Join(root, "deploy", "envoy", "retrieval.pb"))
		config = strings.ReplaceAll(config, "/etc/envoy/tls/tls.crt", certificatePath)
		config = strings.ReplaceAll(config, "/etc/envoy/tls/tls.key", keyPath)
		nativeConfig := filepath.Join(t.TempDir(), "envoy-native.yaml")
		if err := os.WriteFile(nativeConfig, []byte(config), 0o600); err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(ctx, binary, "--config-path", nativeConfig, "--log-level", "warning", "--use-dynamic-base-id")
		output := new(strings.Builder)
		command.Stdout, command.Stderr = output, output
		if err := command.Start(); err != nil {
			t.Fatalf("start native Envoy: %v", err)
		}
		t.Cleanup(func() {
			if command.Process != nil {
				_ = command.Process.Kill()
			}
			_ = command.Wait()
		})
		baseURL := "https://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort))
		waitForEnvoyHealth(t, baseURL, output)
		return baseURL, net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort))
	}

	containerRequest := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        envoyImage,
			ExposedPorts: []string{"8080/tcp"},
			Cmd:          []string{"--config-path", "/etc/envoy/envoy.yaml", "--log-level", "warning"},
			Files: []testcontainers.ContainerFile{
				{HostFilePath: configPath, ContainerFilePath: "/etc/envoy/envoy.yaml", FileMode: 0o644},
				{HostFilePath: filepath.Join(root, "deploy", "envoy", "retrieval.pb"), ContainerFilePath: "/etc/envoy/retrieval.pb", FileMode: 0o644},
				{HostFilePath: certificatePath, ContainerFilePath: "/etc/envoy/tls/tls.crt", FileMode: 0o644},
				// Test-only ephemeral key; production uses a Secret volume with fsGroup.
				{HostFilePath: keyPath, ContainerFilePath: "/etc/envoy/tls/tls.key", FileMode: 0o644},
			},
			WaitingFor: wait.ForListeningPort("8080/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	}
	if err := testcontainers.WithHostPortAccess(helperPort, grpcPort)(&containerRequest); err != nil {
		t.Fatal(err)
	}
	container, err := testcontainers.GenericContainer(ctx, containerRequest)
	if err != nil {
		t.Fatalf("start %s: %v", envoyImage, err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := container.MappedPort(ctx, "8080/tcp")
	if err != nil {
		t.Fatal(err)
	}
	address := net.JoinHostPort(host, mapped.Port())
	baseURL := "https://" + address
	waitForEnvoyHealth(t, baseURL, new(strings.Builder))
	return baseURL, address
}

func writeIntegrationTLSIdentity(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          new(big.Int).SetInt64(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certificatePath := filepath.Join(directory, "tls.crt")
	keyPath := filepath.Join(directory, "tls.key")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("append test certificate")
	}
	return certificatePath, keyPath, roots
}

func availablePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listenerPort(t, listener)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func waitForEnvoyHealth(t *testing.T, baseURL string, output *strings.Builder) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatalf("Envoy did not become ready: %s", output.String())
		case <-ticker.C:
			response, err := http.Get(baseURL + "/healthz")
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return
				}
			}
		}
	}
}

func renderIntegrationEnvoyConfig(t *testing.T, root string, helperPort, grpcPort int) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(root, "deploy", "envoy", "envoy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	config := string(contents)
	config = strings.ReplaceAll(config, "address: api-gateway", "address: "+testcontainers.HostInternal)
	config = strings.ReplaceAll(config, "address: retrieval-engine", "address: "+testcontainers.HostInternal)
	config = strings.ReplaceAll(config, "8081", strconv.Itoa(helperPort))
	config = strings.ReplaceAll(config, "50053", strconv.Itoa(grpcPort))
	config = strings.ReplaceAll(config, "request_headers_timeout: 5s", "request_headers_timeout: 0.2s")
	path := filepath.Join(t.TempDir(), "envoy.yaml")
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func listenerPort(t *testing.T, listener net.Listener) int {
	t.Helper()
	return listener.Addr().(*net.TCPAddr).Port
}

func postJSON(t *testing.T, target, body, authorization string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func lastCall(t *testing.T, backend *integrationBackend, method string) observedCall {
	t.Helper()
	calls := backend.snapshot()
	for index := len(calls) - 1; index >= 0; index-- {
		if calls[index].method == method {
			return calls[index]
		}
	}
	t.Fatalf("no backend %s call", method)
	return observedCall{}
}

func assertTrustedContext(t *testing.T, context *retrievalv1.SecurityContext) {
	t.Helper()
	if context == nil || context.GetTenantId() != "tenant-a" || context.GetUserId() != "alice" ||
		len(context.GetAllowedRepos()) != 1 || context.GetAllowedRepos()[0] != "repo-a" ||
		len(context.GetAclGroups()) != 1 || context.GetAclGroups()[0] != "dev" || !validTraceID(context.GetTraceId()) {
		t.Fatalf("untrusted SecurityContext: %v", context)
	}
}

func assertTrustedTraceparent(t *testing.T, call observedCall) {
	t.Helper()
	wantPrefix := "00-" + call.securityContext.GetTraceId() + "-"
	if !strings.HasPrefix(call.traceparent, wantPrefix) || len(call.traceparent) != 55 || strings.Contains(call.traceparent, "bbbbbbbbbbbbbbbb") {
		t.Fatalf("untrusted traceparent %q for context %v", call.traceparent, call.securityContext)
	}
}
