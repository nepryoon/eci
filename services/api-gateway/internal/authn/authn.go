// Package authn validates gateway bearer tokens and derives the protobuf
// SecurityContext exclusively from authenticated OIDC claims.
package authn

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/coreos/go-oidc/v3/oidc"
	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	maxAuthorizationHeaderBytes = 16 * 1024
	maxIdentityBytes            = 256
	maxScopeValueBytes          = 256
	maxScopeValues              = 256
)

// Config contains trusted process configuration. None of these fields may be
// populated from a request or model-controlled input.
type Config struct {
	Issuer                  string
	Audience                string
	AllowHTTPForDevelopment bool
}

// ErrorCode is safe to expose at the HTTP boundary. It deliberately excludes
// verifier and cryptographic details.
type ErrorCode string

const (
	ErrorMissingToken  ErrorCode = "missing_token"
	ErrorInvalidToken  ErrorCode = "invalid_token"
	ErrorInvalidClaims ErrorCode = "invalid_claims"
	ErrorInvalidTrace  ErrorCode = "invalid_trace_id"
)

// AuthError reports only a closed, low-cardinality reason.
type AuthError struct {
	Code ErrorCode
}

func (e *AuthError) Error() string {
	return "authentication failed: " + string(e.Code)
}

type tokenVerifier interface {
	Verify(context.Context, string) (json.RawMessage, error)
}

type oidcTokenVerifier struct {
	verifier *oidc.IDTokenVerifier
}

func (v oidcTokenVerifier) Verify(ctx context.Context, raw string) (json.RawMessage, error) {
	token, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return nil, err
	}
	var claims json.RawMessage
	if err := token.Claims(&claims); err != nil {
		return nil, err
	}
	return claims, nil
}

type tokenClaims struct {
	Subject      *string   `json:"sub"`
	Type         *string   `json:"typ"`
	TenantID     *string   `json:"tenant_id"`
	AllowedRepos *[]string `json:"allowed_repos"`
	ACLGroups    *[]string `json:"acl_groups"`
}

// Authenticator verifies bearer tokens and creates SecurityContext values.
type Authenticator struct {
	verifier tokenVerifier
	total    *prometheus.CounterVec
	tracer   trace.Tracer
}

// New validates configuration, performs OIDC discovery, and constructs a
// verifier restricted to RS256. Discovery failure is fatal and never turns
// into a default-allow path.
func New(ctx context.Context, cfg Config, registerer prometheus.Registerer) (*Authenticator, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if registerer == nil {
		return nil, fmt.Errorf("authn: prometheus registerer is required")
	}

	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("authn: OIDC discovery failed: %w", err)
	}
	verifier := provider.VerifierContext(context.WithoutCancel(ctx), &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return newAuthenticator(oidcTokenVerifier{verifier: verifier}, registerer)
}

func newAuthenticator(verifier tokenVerifier, registerer prometheus.Registerer) (*Authenticator, error) {
	if verifier == nil {
		return nil, fmt.Errorf("authn: token verifier is required")
	}
	if registerer == nil {
		return nil, fmt.Errorf("authn: prometheus registerer is required")
	}
	total := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eci",
		Subsystem: "gateway",
		Name:      "authentication_total",
		Help:      "Gateway authentication attempts by closed outcome and reason.",
	}, []string{"outcome", "reason"})
	if err := registerer.Register(total); err != nil {
		return nil, fmt.Errorf("authn: register authentication metric: %w", err)
	}
	return &Authenticator{
		verifier: verifier,
		total:    total,
		tracer:   otel.Tracer("eci/services/api-gateway/authn"),
	}, nil
}

// Authenticate derives authorization scope only from the verified token. The
// trace ID is supplied independently by the trusted gateway tracing layer.
func (a *Authenticator) Authenticate(
	ctx context.Context,
	authorizationHeader string,
	trustedTraceID string,
) (*retrievalv1.SecurityContext, error) {
	ctx, span := a.tracer.Start(ctx, "eci.gateway.authenticate")
	defer span.End()

	token, code := bearerToken(authorizationHeader)
	if code != "" {
		a.record(span, "failure", code)
		return nil, &AuthError{Code: code}
	}
	if !isValidTraceID(trustedTraceID) {
		a.record(span, "failure", ErrorInvalidTrace)
		return nil, &AuthError{Code: ErrorInvalidTrace}
	}

	rawClaims, err := a.verifier.Verify(ctx, token)
	if err != nil {
		a.record(span, "failure", ErrorInvalidToken)
		return nil, &AuthError{Code: ErrorInvalidToken}
	}
	claims, err := validateClaims(rawClaims)
	if err != nil {
		a.record(span, "failure", ErrorInvalidClaims)
		return nil, &AuthError{Code: ErrorInvalidClaims}
	}

	securityContext := &retrievalv1.SecurityContext{
		TenantId:     *claims.TenantID,
		UserId:       *claims.Subject,
		AllowedRepos: canonicalScope(*claims.AllowedRepos),
		AclGroups:    canonicalScope(*claims.ACLGroups),
		TraceId:      trustedTraceID,
	}
	a.record(span, "success", "success")
	return securityContext, nil
}

func (a *Authenticator) record(span trace.Span, outcome string, reason ErrorCode) {
	a.total.WithLabelValues(outcome, string(reason)).Inc()
	span.SetAttributes(
		attribute.String("auth.outcome", outcome),
		attribute.String("auth.reason", string(reason)),
	)
}

func validateConfig(cfg Config) error {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return fmt.Errorf("authn: issuer and audience are required")
	}
	if cfg.Issuer != strings.TrimSpace(cfg.Issuer) || cfg.Audience != strings.TrimSpace(cfg.Audience) {
		return fmt.Errorf("authn: issuer and audience must not contain surrounding whitespace")
	}
	parsed, err := url.Parse(cfg.Issuer)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("authn: issuer must be an absolute URL without query or fragment")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		return nil
	case "http":
		if !cfg.AllowHTTPForDevelopment || !isLoopbackHost(parsed.Hostname()) {
			return fmt.Errorf("authn: HTTP issuer is allowed only for explicit loopback development")
		}
		return nil
	default:
		return fmt.Errorf("authn: issuer scheme must be HTTPS")
	}
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func bearerToken(header string) (string, ErrorCode) {
	if len(header) > maxAuthorizationHeaderBytes {
		return "", ErrorInvalidToken
	}
	const separator = " "
	index := strings.Index(header, separator)
	if index != len("Bearer") || !strings.EqualFold(header[:index], "Bearer") {
		return "", ErrorMissingToken
	}
	token := header[index+len(separator):]
	if token == "" || strings.ContainsAny(token, " \t\r\n,") {
		return "", ErrorMissingToken
	}
	return token, ""
}

func isValidTraceID(value string) bool {
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

func validateClaims(raw json.RawMessage) (tokenClaims, error) {
	var claims tokenClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return claims, err
	}
	if claims.Subject == nil || !validIdentity(*claims.Subject) {
		return claims, fmt.Errorf("invalid sub")
	}
	if claims.Type == nil || *claims.Type != "Bearer" {
		return claims, fmt.Errorf("invalid typ")
	}
	if claims.TenantID == nil || !validIdentity(*claims.TenantID) {
		return claims, fmt.Errorf("invalid tenant_id")
	}
	if claims.AllowedRepos == nil || !validScope(*claims.AllowedRepos) {
		return claims, fmt.Errorf("invalid allowed_repos")
	}
	if claims.ACLGroups == nil || !validScope(*claims.ACLGroups) {
		return claims, fmt.Errorf("invalid acl_groups")
	}
	return claims, nil
}

func validIdentity(value string) bool {
	return validBoundedString(value, maxIdentityBytes)
}

func validScope(values []string) bool {
	if len(values) > maxScopeValues {
		return false
	}
	for _, value := range values {
		if !validBoundedString(value, maxScopeValueBytes) {
			return false
		}
	}
	return true
}

func validBoundedString(value string, maximum int) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	return !strings.ContainsFunc(value, unicode.IsControl)
}

func canonicalScope(values []string) []string {
	unique := make(map[string]struct{}, len(values))
	for _, value := range values {
		unique[value] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for value := range unique {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}
