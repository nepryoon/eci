// Package edge implements the internal helpers behind the public Envoy edge.
package edge

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/api-gateway/internal/authn"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	SecurityContextHeader         = "Eci-Security-Context-Bin"
	SecurityContextMetadataKey    = "eci-security-context-bin"
	RateLimitKeyHeader            = "X-Eci-Rate-Limit-Key"
	RequestDeadlineHeader         = "X-Eci-Request-Deadline-Unix-Ms"
	SSEPath                       = "/v1/impact-analysis:stream"
	defaultMaxJSONBodyBytes       = 1 << 20
	defaultRequestTimeout         = 30 * time.Second
	maxScopeValues                = 256
	maxScopeValueBytes            = 256
	maxSecurityContextHeaderBytes = 12 * 1024
	defaultMaxConcurrentSSECaller = 10
	defaultMaxConcurrentSSE       = 1000
)

type Authenticator interface {
	Authenticate(context.Context, string, string) (*retrievalv1.SecurityContext, error)
}

type ImpactClient interface {
	ImpactAnalysis(
		context.Context,
		*retrievalv1.ImpactAnalysisRequest,
		...grpc.CallOption,
	) (grpc.ServerStreamingClient[retrievalv1.ImpactAnalysisEvent], error)
}

type EdgeConfig struct {
	MaxJSONBodyBytes          int64
	RequestTimeout            time.Duration
	MaxConcurrentSSEPerCaller int
	MaxConcurrentSSE          int
	Random                    io.Reader
	Registerer                prometheus.Registerer
	Tracer                    trace.Tracer
}

type handler struct {
	auth    Authenticator
	impact  ImpactClient
	maxBody int64
	timeout time.Duration
	random  io.Reader
	total   *prometheus.CounterVec
	active  prometheus.Gauge
	tracer  trace.Tracer
	sseGate *sseAdmission
}

type sseAdmission struct {
	mu             sync.Mutex
	maxPerCaller   int
	maxTotal       int
	activeByCaller map[string]int
	activeTotal    int
}

func newSSEAdmission(maxPerCaller, maxTotal int) *sseAdmission {
	return &sseAdmission{
		maxPerCaller:   maxPerCaller,
		maxTotal:       maxTotal,
		activeByCaller: make(map[string]int),
	}
}

func (a *sseAdmission) acquire(securityContext *retrievalv1.SecurityContext) (func(), string) {
	key := authenticatedCallerRateLimitKey(securityContext.GetTenantId(), securityContext.GetUserId())
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activeTotal >= a.maxTotal {
		return nil, "aggregate_limit"
	}
	if a.activeByCaller[key] >= a.maxPerCaller {
		return nil, "caller_limit"
	}
	a.activeByCaller[key]++
	a.activeTotal++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.activeByCaller[key]--
			a.activeTotal--
			if a.activeByCaller[key] == 0 {
				delete(a.activeByCaller, key)
			}
		})
	}, ""
}

func NewEdgeHandler(auth Authenticator, impact ImpactClient, cfg EdgeConfig) (http.Handler, error) {
	if auth == nil || impact == nil {
		return nil, fmt.Errorf("edge: authenticator and impact client are required")
	}
	if cfg.Registerer == nil {
		return nil, fmt.Errorf("edge: prometheus registerer is required")
	}
	if cfg.MaxJSONBodyBytes < 0 || cfg.RequestTimeout < 0 || cfg.MaxConcurrentSSEPerCaller < 0 || cfg.MaxConcurrentSSE < 0 {
		return nil, fmt.Errorf("edge: limits must not be negative")
	}
	if cfg.MaxJSONBodyBytes == 0 {
		cfg.MaxJSONBodyBytes = defaultMaxJSONBodyBytes
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.MaxConcurrentSSEPerCaller == 0 {
		cfg.MaxConcurrentSSEPerCaller = defaultMaxConcurrentSSECaller
	}
	if cfg.MaxConcurrentSSE == 0 {
		cfg.MaxConcurrentSSE = defaultMaxConcurrentSSE
	}
	if cfg.MaxConcurrentSSEPerCaller > cfg.MaxConcurrentSSE {
		return nil, fmt.Errorf("edge: per-caller SSE limit must not exceed aggregate limit")
	}
	if cfg.Random == nil {
		cfg.Random = rand.Reader
	}
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("eci/services/api-gateway/edge")
	}
	total := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eci",
		Subsystem: "gateway_edge",
		Name:      "requests_total",
		Help:      "Internal edge-helper requests by bounded route and outcome.",
	}, []string{"route", "outcome"})
	if err := cfg.Registerer.Register(total); err != nil {
		return nil, fmt.Errorf("edge: register request metric: %w", err)
	}
	active := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "eci",
		Subsystem: "gateway_edge",
		Name:      "active_sse",
		Help:      "Currently admitted SSE requests across all authenticated callers.",
	})
	if err := cfg.Registerer.Register(active); err != nil {
		return nil, fmt.Errorf("edge: register active SSE metric: %w", err)
	}

	h := &handler{
		auth: auth, impact: impact, maxBody: cfg.MaxJSONBodyBytes,
		timeout: cfg.RequestTimeout, random: cfg.Random, total: total, active: active, tracer: cfg.Tracer,
		sseGate: newSSEAdmission(cfg.MaxConcurrentSSEPerCaller, cfg.MaxConcurrentSSE),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/authorize/", h.authorize)
	mux.HandleFunc(SSEPath, h.sse)
	mux.HandleFunc("/healthz", h.health)
	return mux, nil
}

func (h *handler) authorize(w http.ResponseWriter, r *http.Request) {
	requestDeadline := time.Now().Add(h.timeout)
	traceID, parentSpanID, identifierErr := h.identifiers()
	spanParent := r.Context()
	if identifierErr == nil {
		parsedTraceID, traceErr := trace.TraceIDFromHex(traceID)
		parsedSpanID, spanErr := trace.SpanIDFromHex(parentSpanID)
		if traceErr != nil || spanErr != nil {
			identifierErr = fmt.Errorf("edge: generated invalid trace context")
		} else {
			parent := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID: parsedTraceID, SpanID: parsedSpanID,
				TraceFlags: trace.FlagsSampled, Remote: true,
			})
			spanParent = trace.ContextWithRemoteSpanContext(spanParent, parent)
		}
	}
	ctx, span := h.tracer.Start(spanParent, "eci.gateway.authorize")
	defer span.End()
	outcome := "error"
	defer func() { h.record(span, "auth", outcome) }()

	if identifierErr != nil {
		writeError(w, http.StatusServiceUnavailable, "authentication unavailable")
		outcome = "unavailable"
		return
	}
	authorizeSpan := span.SpanContext()
	if !authorizeSpan.IsValid() || authorizeSpan.TraceID().String() != traceID {
		writeError(w, http.StatusServiceUnavailable, "authentication unavailable")
		outcome = "unavailable"
		return
	}
	securityContext, err := h.auth.Authenticate(ctx, r.Header.Get("Authorization"), traceID)
	if err != nil {
		var authenticationError *authn.AuthError
		if errors.As(err, &authenticationError) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "authentication required")
			outcome = "denied"
			return
		}
		writeError(w, http.StatusServiceUnavailable, "authentication unavailable")
		outcome = "unavailable"
		return
	}
	if securityContext == nil {
		writeError(w, http.StatusServiceUnavailable, "authentication unavailable")
		outcome = "unavailable"
		return
	}
	canonical := proto.Clone(securityContext).(*retrievalv1.SecurityContext)
	canonical.TraceId = traceID
	if !validSecurityContext(canonical) {
		writeError(w, http.StatusServiceUnavailable, "authentication unavailable")
		outcome = "unavailable"
		return
	}
	raw, err := proto.Marshal(canonical)
	if err != nil || base64.StdEncoding.EncodedLen(len(raw)) > maxSecurityContextHeaderBytes {
		writeError(w, http.StatusServiceUnavailable, "authentication unavailable")
		outcome = "unavailable"
		return
	}
	w.Header().Set(SecurityContextHeader, base64.StdEncoding.EncodeToString(raw))
	w.Header().Set(RateLimitKeyHeader, authenticatedCallerRateLimitKey(canonical.GetTenantId(), canonical.GetUserId()))
	w.Header().Set(RequestDeadlineHeader, strconv.FormatInt(requestDeadline.UnixMilli(), 10))
	w.Header().Set("Traceparent", "00-"+authorizeSpan.TraceID().String()+"-"+authorizeSpan.SpanID().String()+"-01")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	outcome = "allow"
}

func authenticatedCallerRateLimitKey(tenantID, userID string) string {
	// The separator makes the tuple unambiguous. The opaque value is used only
	// inside Envoy and is stripped before routing to application services.
	digest := sha256.Sum256([]byte(tenantID + "\x00" + userID))
	return hex.EncodeToString(digest[:])
}

func (h *handler) identifiers() (string, string, error) {
	raw := make([]byte, 24)
	if _, err := io.ReadFull(h.random, raw); err != nil {
		return "", "", err
	}
	traceID, spanID := hex.EncodeToString(raw[:16]), hex.EncodeToString(raw[16:])
	if traceID == strings.Repeat("0", 32) || spanID == strings.Repeat("0", 16) {
		return "", "", fmt.Errorf("edge: generated invalid trace context")
	}
	return traceID, spanID, nil
}

func (h *handler) sse(w http.ResponseWriter, r *http.Request) {
	rawContext, securityContext, metadataOK := decodeSecurityContext(r.Header.Get(SecurityContextHeader))
	requestDeadline, deadlineOK := trustedRequestDeadline(r.Header.Get(RequestDeadlineHeader), time.Now(), h.timeout)
	parentContext := r.Context()
	if metadataOK {
		parent, ok := trustedTraceparent(r.Header.Get("traceparent"), securityContext.GetTraceId())
		metadataOK = ok
		if ok {
			parentContext = trace.ContextWithRemoteSpanContext(parentContext, parent)
		}
	}
	requestContext, cancel := context.WithDeadline(parentContext, requestDeadline)
	defer cancel()
	ctx, span := h.tracer.Start(requestContext, "eci.gateway.sse")
	defer span.End()
	outcome := "error"
	defer func() { h.record(span, "sse", outcome) }()

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		outcome = "invalid"
		return
	}
	if !metadataOK || !deadlineOK {
		writeError(w, http.StatusUnauthorized, "authentication required")
		outcome = "denied"
		return
	}
	release, limitOutcome := h.sseGate.acquire(securityContext)
	if limitOutcome != "" {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "too many concurrent streams")
		outcome = limitOutcome
		return
	}
	defer release()
	h.active.Inc()
	defer h.active.Dec()

	body := http.MaxBytesReader(w, r.Body, h.maxBody)
	payload, err := io.ReadAll(body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(w, http.StatusRequestEntityTooLarge, "request too large")
		} else {
			writeError(w, http.StatusBadRequest, "invalid request")
		}
		outcome = "invalid"
		return
	}
	request := new(retrievalv1.ImpactAnalysisRequest)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payload, request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		outcome = "invalid"
		return
	}
	// Body authority is never used. The authenticated protobuf metadata is the
	// sole scope input for downstream PEP/query enforcement.
	request.SecurityContext = nil

	if ctx.Err() != nil {
		writeError(w, http.StatusGatewayTimeout, "request deadline exceeded")
		outcome = "deadline"
		return
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		SecurityContextMetadataKey, string(rawContext),
	))
	stream, err := h.impact.ImpactAnalysis(ctx, request)
	if err != nil {
		span.SetAttributes(attribute.Int("rpc.grpc.status_code", int(status.Code(err))))
		writeGRPCError(w, err)
		outcome = "upstream_error"
		return
	}
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			outcome = "complete"
			return
		}
		span.SetAttributes(attribute.Int("rpc.grpc.status_code", int(status.Code(err))))
		writeGRPCError(w, err)
		outcome = "upstream_error"
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unavailable")
		outcome = "unavailable"
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if !writeEvent(w, first) {
		outcome = "client_error"
		return
	}
	flusher.Flush()

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			outcome = "complete"
			return
		}
		if err != nil {
			span.SetAttributes(attribute.Int("rpc.grpc.status_code", int(status.Code(err))))
			if errors.Is(ctx.Err(), context.DeadlineExceeded) || requestContext.Err() == nil {
				_, _ = io.WriteString(w, "event: error\ndata: {\"code\":\"stream_error\"}\n\n")
				flusher.Flush()
				outcome = "upstream_error"
			} else {
				outcome = "cancelled"
			}
			return
		}
		if !writeEvent(w, event) {
			outcome = "client_error"
			return
		}
		flusher.Flush()
	}
}

func writeEvent(w io.Writer, event *retrievalv1.ImpactAnalysisEvent) bool {
	payload, err := (protojson.MarshalOptions{}).Marshal(event)
	if err != nil {
		return false
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err == nil
}

func decodeSecurityContext(encoded string) ([]byte, *retrievalv1.SecurityContext, bool) {
	if encoded == "" {
		return nil, nil, false
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, nil, false
	}
	securityContext := new(retrievalv1.SecurityContext)
	if err := proto.Unmarshal(raw, securityContext); err != nil || !validSecurityContext(securityContext) {
		return nil, nil, false
	}
	return raw, securityContext, true
}

func trustedRequestDeadline(header string, now time.Time, maximum time.Duration) (time.Time, bool) {
	unixMillis, err := strconv.ParseInt(header, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	deadline := time.UnixMilli(unixMillis)
	maximumDeadline := now.Add(maximum)
	if deadline.After(maximumDeadline) {
		deadline = maximumDeadline
	}
	return deadline, true
}

func trustedTraceparent(header, expectedTraceID string) (trace.SpanContext, bool) {
	if header != strings.ToLower(header) || len(header) != 55 {
		return trace.SpanContext{}, false
	}
	parts := strings.Split(header, "-")
	if len(parts) != 4 || parts[0] != "00" || parts[1] != expectedTraceID || len(parts[3]) != 2 {
		return trace.SpanContext{}, false
	}
	traceID, err := trace.TraceIDFromHex(parts[1])
	if err != nil {
		return trace.SpanContext{}, false
	}
	spanID, err := trace.SpanIDFromHex(parts[2])
	if err != nil {
		return trace.SpanContext{}, false
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil || len(flags) != 1 {
		return trace.SpanContext{}, false
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.TraceFlags(flags[0]), Remote: true,
	})
	return spanContext, spanContext.IsValid()
}

func validSecurityContext(sc *retrievalv1.SecurityContext) bool {
	return sc != nil && validValue(sc.GetTenantId()) && validValue(sc.GetUserId()) &&
		validTraceID(sc.GetTraceId()) && validList(sc.GetAllowedRepos()) && validList(sc.GetAclGroups())
}

func validList(values []string) bool {
	if len(values) > maxScopeValues {
		return false
	}
	for _, value := range values {
		if !validValue(value) {
			return false
		}
	}
	return true
}

func validValue(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) || len([]byte(value)) > maxScopeValueBytes {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validTraceID(value string) bool {
	if len(value) != 32 || value == strings.Repeat("0", 32) {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func writeGRPCError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	httpCode := http.StatusBadGateway
	safe := "upstream request failed"
	switch code {
	case codes.Unauthenticated:
		httpCode, safe = http.StatusUnauthorized, "authentication required"
	case codes.PermissionDenied:
		httpCode, safe = http.StatusForbidden, "authorization denied"
	case codes.InvalidArgument:
		httpCode, safe = http.StatusBadRequest, "invalid request"
	case codes.DeadlineExceeded:
		httpCode, safe = http.StatusGatewayTimeout, "request deadline exceeded"
	case codes.Unavailable:
		httpCode, safe = http.StatusServiceUnavailable, "upstream unavailable"
	}
	writeError(w, httpCode, safe)
}

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		h.total.WithLabelValues("health", "invalid").Inc()
		return
	}
	h.total.WithLabelValues("health", "allow").Inc()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "{\"status\":\"ok\"}\n")
}

func (h *handler) record(span trace.Span, route, outcome string) {
	h.total.WithLabelValues(route, outcome).Inc()
	span.SetAttributes(
		attribute.String("gateway.route", route),
		attribute.String("gateway.outcome", outcome),
	)
}
