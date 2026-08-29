// Package authz provides the fail-closed gRPC Policy Enforcement Point for
// the central OPA Policy Decision Point prescribed by ADD Module 3 section
// 2.1. Authorization input is deliberately limited to authenticated
// SecurityContext metadata and the method name supplied by the gRPC runtime.
package authz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxResponseBytes = 64 * 1024

var serviceNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

var knownReasons = map[string]struct{}{
	"allow":            {},
	"unknown_action":   {},
	"missing_tenant":   {},
	"missing_user":     {},
	"empty_repo_scope": {},
	"empty_acl_scope":  {},
	"policy_denied":    {},
}

// Config is trusted process configuration. It must never be populated from a
// request, prompt, model output, or caller-supplied metadata.
type Config struct {
	Endpoint          string
	DecisionPath      string
	Service           string
	Timeout           time.Duration
	AllowInsecureHTTP bool
}

// ConfigFromEnvironment reads trusted process configuration. OPA_URL has no
// implicit value: a service cannot accidentally start without an explicitly
// configured PDP.
func ConfigFromEnvironment(service string) (Config, error) {
	timeoutRaw := os.Getenv("OPA_TIMEOUT")
	if timeoutRaw == "" {
		timeoutRaw = "100ms"
	}
	timeout, err := time.ParseDuration(timeoutRaw)
	if err != nil {
		return Config{}, fmt.Errorf("authz: invalid OPA_TIMEOUT")
	}
	allowInsecure := false
	if raw := os.Getenv("OPA_ALLOW_INSECURE_HTTP"); raw != "" {
		allowInsecure, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("authz: invalid OPA_ALLOW_INSECURE_HTTP")
		}
	}
	decisionPath := os.Getenv("OPA_DECISION_PATH")
	if decisionPath == "" {
		decisionPath = "/v1/data/eci/authz/decision"
	}
	return Config{
		Endpoint:          os.Getenv("OPA_URL"),
		DecisionPath:      decisionPath,
		Service:           service,
		Timeout:           timeout,
		AllowInsecureHTTP: allowInsecure,
	}, nil
}

// Decision is the strict, bounded result understood by the PEP.
type Decision struct {
	Allow  bool
	Reason string
}

// DecisionClient is the narrow seam used by the interceptor and its tests.
// There is intentionally no request-body or arbitrary attributes parameter.
type DecisionClient interface {
	Decide(
		ctx context.Context,
		securityContext *retrievalv1.SecurityContext,
		fullMethod string,
	) (Decision, error)
}

// Client calls the fixed OPA decision endpoint.
type Client struct {
	decisionURL *url.URL
	httpClient  *http.Client
	service     string
	timeout     time.Duration
	total       *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	tracer      trace.Tracer
}

type opaEnvelope struct {
	Input opaInput `json:"input"`
}

type opaInput struct {
	Subject opaSubject `json:"subject"`
	Action  string     `json:"action"`
}

type opaSubject struct {
	TenantID     string   `json:"tenant_id"`
	UserID       string   `json:"user_id"`
	AllowedRepos []string `json:"allowed_repos"`
	ACLGroups    []string `json:"acl_groups"`
}

type opaResponse struct {
	Result *struct {
		Allow  *bool  `json:"allow"`
		Reason string `json:"reason"`
	} `json:"result"`
	DecisionID string `json:"decision_id,omitempty"`
}

// New validates all trusted configuration, verifies OPA health, and registers
// bounded telemetry before returning a usable client. Unhealthy OPA is a
// startup error, never a signal to bypass authorization.
func New(ctx context.Context, cfg Config, registerer prometheus.Registerer) (*Client, error) {
	endpoint, decisionURL, err := validateConfig(cfg)
	if err != nil {
		return nil, err
	}
	if registerer == nil {
		return nil, fmt.Errorf("authz: prometheus registerer is required")
	}

	httpClient := &http.Client{
		Timeout: cfg.Timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	if err := preflight(ctx, httpClient, endpoint); err != nil {
		return nil, err
	}

	total := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eci",
		Subsystem: "authz",
		Name:      "decisions_total",
		Help:      "OPA authorization decisions by bounded service, outcome, and reason.",
	}, []string{"service", "outcome", "reason"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "eci",
		Subsystem: "authz",
		Name:      "pdp_duration_seconds",
		Help:      "OPA policy decision latency by bounded service and outcome.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"service", "outcome"})
	if err := registerer.Register(total); err != nil {
		return nil, fmt.Errorf("authz: register decisions metric: %w", err)
	}
	if err := registerer.Register(duration); err != nil {
		return nil, fmt.Errorf("authz: register duration metric: %w", err)
	}

	return &Client{
		decisionURL: decisionURL,
		httpClient:  httpClient,
		service:     cfg.Service,
		timeout:     cfg.Timeout,
		total:       total,
		duration:    duration,
		tracer:      otel.Tracer("eci/libs/go/authz"),
	}, nil
}

func validateConfig(cfg Config) (*url.URL, *url.URL, error) {
	if !serviceNamePattern.MatchString(cfg.Service) {
		return nil, nil, fmt.Errorf("authz: invalid service name")
	}
	if cfg.Timeout <= 0 || cfg.Timeout > 2*time.Second {
		return nil, nil, fmt.Errorf("authz: timeout must be >0 and <=2s")
	}
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, nil, fmt.Errorf("authz: invalid OPA endpoint")
	}
	if endpoint.Path != "" && endpoint.Path != "/" {
		return nil, nil, fmt.Errorf("authz: OPA endpoint must not contain a path")
	}
	if endpoint.Scheme != "https" {
		if endpoint.Scheme != "http" || !cfg.AllowInsecureHTTP {
			return nil, nil, fmt.Errorf("authz: OPA endpoint must use HTTPS outside explicit dev mode")
		}
	}
	if !strings.HasPrefix(cfg.DecisionPath, "/") || strings.ContainsAny(cfg.DecisionPath, "?#") || strings.Contains(cfg.DecisionPath, "//") {
		return nil, nil, fmt.Errorf("authz: invalid OPA decision path")
	}
	decisionURL := *endpoint
	decisionURL.Path = cfg.DecisionPath
	return endpoint, &decisionURL, nil
}

func preflight(ctx context.Context, client *http.Client, endpoint *url.URL) error {
	healthURL := *endpoint
	healthURL.Path = "/health"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL.String(), nil)
	if err != nil {
		return fmt.Errorf("authz: build OPA health request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("authz: OPA health preflight failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("authz: OPA health preflight returned HTTP %d", response.StatusCode)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return nil
}

// Decide submits only authenticated subject attributes and the gRPC runtime
// method. It never serializes trace IDs, request fields, prompts, or model data.
func (c *Client) Decide(
	ctx context.Context,
	securityContext *retrievalv1.SecurityContext,
	fullMethod string,
) (Decision, error) {
	ctx, span := c.tracer.Start(ctx, "eci.authz.check")
	defer span.End()
	started := time.Now()
	if securityContext == nil {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: SecurityContext is required")
	}

	envelope := opaEnvelope{Input: opaInput{
		Subject: opaSubject{
			TenantID:     securityContext.GetTenantId(),
			UserID:       securityContext.GetUserId(),
			AllowedRepos: append([]string(nil), securityContext.GetAllowedRepos()...),
			ACLGroups:    append([]string(nil), securityContext.GetAclGroups()...),
		},
		Action: fullMethod,
	}}
	body, err := json.Marshal(envelope)
	if err != nil {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: encode OPA input: %w", err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, c.decisionURL.String(), bytes.NewReader(body))
	if err != nil {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: build OPA decision request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: OPA decision request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: OPA decision returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: read OPA decision response: %w", err)
	}
	if len(raw) > maxResponseBytes {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: OPA decision response exceeds %d bytes", maxResponseBytes)
	}
	var decoded opaResponse
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.Result == nil || decoded.Result.Allow == nil {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: invalid OPA decision response schema")
	}
	reason := normalizeReason(decoded.Result.Reason)
	decision := Decision{Allow: *decoded.Result.Allow, Reason: reason}
	if decision.Allow && reason != "allow" {
		c.record(span, started, "error", "pdp_error")
		return Decision{}, fmt.Errorf("authz: inconsistent OPA allow response")
	}
	if !decision.Allow && reason == "allow" {
		reason = "policy_denied"
		decision.Reason = reason
	}
	outcome := "deny"
	if decision.Allow {
		outcome = "allow"
	}
	c.record(span, started, outcome, reason)
	return decision, nil
}

func normalizeReason(reason string) string {
	if _, ok := knownReasons[reason]; ok {
		return reason
	}
	return "policy_denied"
}

func (c *Client) record(span trace.Span, started time.Time, outcome, reason string) {
	c.total.WithLabelValues(c.service, outcome, reason).Inc()
	c.duration.WithLabelValues(c.service, outcome).Observe(time.Since(started).Seconds())
	span.SetAttributes(
		attribute.String("eci.authz.service", c.service),
		attribute.String("eci.authz.outcome", outcome),
		attribute.String("eci.authz.reason", reason),
	)
}

// UnaryServerInterceptor is a fail-closed PEP. It must be chained after
// secctx.UnaryServerInterceptor so that only extracted metadata is considered.
func UnaryServerInterceptor(client DecisionClient) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		securityContext, ok := secctx.FromContext(ctx)
		if !ok || securityContext == nil {
			return nil, status.Error(codes.Unauthenticated, "authentication required")
		}
		decision, err := client.Decide(ctx, securityContext, info.FullMethod)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "authorization service unavailable")
		}
		if !decision.Allow {
			return nil, status.Error(codes.PermissionDenied, "authorization denied")
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor applies the same fail-closed decision to streaming
// RPCs. It must be chained after secctx.StreamServerInterceptor.
func StreamServerInterceptor(client DecisionClient) grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		securityContext, ok := secctx.FromContext(stream.Context())
		if !ok || securityContext == nil {
			return status.Error(codes.Unauthenticated, "authentication required")
		}
		decision, err := client.Decide(stream.Context(), securityContext, info.FullMethod)
		if err != nil {
			return status.Error(codes.Unavailable, "authorization service unavailable")
		}
		if !decision.Allow {
			return status.Error(codes.PermissionDenied, "authorization denied")
		}
		return handler(srv, stream)
	}
}
