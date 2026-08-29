package authn

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const validTraceID = "0123456789abcdef0123456789abcdef"

type stubVerifier struct {
	claims json.RawMessage
	err    error
	calls  int
}

func (s *stubVerifier) Verify(_ context.Context, _ string) (json.RawMessage, error) {
	s.calls++
	return s.claims, s.err
}

func validClaims() json.RawMessage {
	return json.RawMessage(`{
        "sub":"user-7",
        "typ":"Bearer",
        "tenant_id":"tenant-42",
        "allowed_repos":["repo-b","repo-a","repo-a"],
        "acl_groups":["group-z","group-x","group-x"]
    }`)
}

func newTestAuthenticator(t *testing.T, verifier tokenVerifier) (*Authenticator, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	authenticator, err := newAuthenticator(verifier, registry)
	if err != nil {
		t.Fatalf("newAuthenticator: %v", err)
	}
	return authenticator, registry
}

func authCode(t *testing.T, err error) ErrorCode {
	t.Helper()
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("error = %v, want *AuthError", err)
	}
	return authErr.Code
}

func metricValue(t *testing.T, registry *prometheus.Registry, outcome, reason string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "eci_gateway_authentication_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["outcome"] == outcome && labels["reason"] == reason {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestAuthenticateMapsOnlyVerifiedClaimsDeterministically(t *testing.T) {
	verifier := &stubVerifier{claims: validClaims()}
	authenticator, registry := newTestAuthenticator(t, verifier)

	securityContext, err := authenticator.Authenticate(
		context.Background(),
		"Bearer signed.jwt.value",
		validTraceID,
	)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}

	if securityContext.GetTenantId() != "tenant-42" || securityContext.GetUserId() != "user-7" {
		t.Fatalf("identity mismatch: %+v", securityContext)
	}
	if !slices.Equal(securityContext.GetAllowedRepos(), []string{"repo-a", "repo-b"}) {
		t.Fatalf("allowed_repos = %v", securityContext.GetAllowedRepos())
	}
	if !slices.Equal(securityContext.GetAclGroups(), []string{"group-x", "group-z"}) {
		t.Fatalf("acl_groups = %v", securityContext.GetAclGroups())
	}
	if securityContext.GetTraceId() != validTraceID {
		t.Fatalf("trace_id = %q", securityContext.GetTraceId())
	}
	if got := metricValue(t, registry, "success", "success"); got != 1 {
		t.Fatalf("success counter = %v, want 1", got)
	}
}

func TestMissingOrMalformedBearerFailsBeforeVerification(t *testing.T) {
	tests := []string{
		"",
		"Basic abc",
		"Bearer",
		"Bearer ",
		"Bearer  token",
		"Bearer\ttoken",
		"Bearer\ntoken",
		"Bearer first second",
		"Bearer first,Bearer second",
	}
	for _, header := range tests {
		t.Run(header, func(t *testing.T) {
			verifier := &stubVerifier{claims: validClaims()}
			authenticator, _ := newTestAuthenticator(t, verifier)

			_, err := authenticator.Authenticate(context.Background(), header, validTraceID)
			if code := authCode(t, err); code != ErrorMissingToken {
				t.Fatalf("code = %q, want %q", code, ErrorMissingToken)
			}
			if verifier.calls != 0 {
				t.Fatalf("verifier calls = %d, want 0", verifier.calls)
			}
		})
	}
}

func TestBearerSchemeIsCaseInsensitiveWithoutRelaxingSyntax(t *testing.T) {
	authenticator, _ := newTestAuthenticator(t, &stubVerifier{claims: validClaims()})
	if _, err := authenticator.Authenticate(
		context.Background(), "bearer signed.jwt.value", validTraceID,
	); err != nil {
		t.Fatalf("Authenticate lower-case bearer scheme: %v", err)
	}
}

func TestInvalidTraceIDFailsClosed(t *testing.T) {
	tests := []string{
		"",
		"00000000000000000000000000000000",
		"0123456789ABCDEF0123456789ABCDEF",
		"0123456789abcdef",
		"g123456789abcdef0123456789abcdef",
	}
	for _, traceID := range tests {
		t.Run(traceID, func(t *testing.T) {
			authenticator, _ := newTestAuthenticator(t, &stubVerifier{claims: validClaims()})
			_, err := authenticator.Authenticate(
				context.Background(), "Bearer signed.jwt.value", traceID,
			)
			if code := authCode(t, err); code != ErrorInvalidTrace {
				t.Fatalf("code = %q, want %q", code, ErrorInvalidTrace)
			}
		})
	}
}

func TestInvalidTokenDoesNotLeakVerifierDetails(t *testing.T) {
	verifier := &stubVerifier{err: errors.New("rsa verification failed with private detail")}
	authenticator, registry := newTestAuthenticator(t, verifier)

	_, err := authenticator.Authenticate(
		context.Background(), "Bearer forged.jwt.value", validTraceID,
	)
	if code := authCode(t, err); code != ErrorInvalidToken {
		t.Fatalf("code = %q, want %q", code, ErrorInvalidToken)
	}
	if err.Error() != "authentication failed: invalid_token" {
		t.Fatalf("error leaked verifier detail: %q", err)
	}
	if got := metricValue(t, registry, "failure", "invalid_token"); got != 1 {
		t.Fatalf("failure counter = %v, want 1", got)
	}
}

func TestAuthenticationSpanUsesOnlyClosedOutcomeAttributes(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
	})
	authenticator, _ := newTestAuthenticator(t, &stubVerifier{claims: validClaims()})

	if _, err := authenticator.Authenticate(
		context.Background(), "Bearer signed.jwt.value", validTraceID,
	); err != nil {
		t.Fatal(err)
	}

	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "eci.gateway.authenticate" {
		t.Fatalf("spans = %v", spans)
	}
	attributes := map[string]string{}
	for _, item := range spans[0].Attributes() {
		attributes[string(item.Key)] = item.Value.AsString()
	}
	if len(attributes) != 2 || attributes["auth.outcome"] != "success" || attributes["auth.reason"] != "success" {
		t.Fatalf("span attributes = %v", attributes)
	}
}

func TestRequiredClaimsAndLimitsFailClosed(t *testing.T) {
	tooMany := make([]string, maxScopeValues+1)
	for index := range tooMany {
		tooMany[index] = "repo"
	}
	tooManyJSON, err := json.Marshal(map[string]any{
		"sub":           "user-7",
		"typ":           "Bearer",
		"tenant_id":     "tenant-42",
		"allowed_repos": tooMany,
		"acl_groups":    []string{},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]json.RawMessage{
		"missing sub":           json.RawMessage(`{"typ":"Bearer","tenant_id":"t","allowed_repos":[],"acl_groups":[]}`),
		"empty sub":             json.RawMessage(`{"sub":"","typ":"Bearer","tenant_id":"t","allowed_repos":[],"acl_groups":[]}`),
		"wrong token type":      json.RawMessage(`{"sub":"u","typ":"ID","tenant_id":"t","allowed_repos":[],"acl_groups":[]}`),
		"missing tenant":        json.RawMessage(`{"sub":"u","typ":"Bearer","allowed_repos":[],"acl_groups":[]}`),
		"missing repos":         json.RawMessage(`{"sub":"u","typ":"Bearer","tenant_id":"t","acl_groups":[]}`),
		"repos wrong type":      json.RawMessage(`{"sub":"u","typ":"Bearer","tenant_id":"t","allowed_repos":"repo","acl_groups":[]}`),
		"missing groups":        json.RawMessage(`{"sub":"u","typ":"Bearer","tenant_id":"t","allowed_repos":[]}`),
		"empty scope element":   json.RawMessage(`{"sub":"u","typ":"Bearer","tenant_id":"t","allowed_repos":[""],"acl_groups":[]}`),
		"whitespace identity":   json.RawMessage(`{"sub":"u","typ":"Bearer","tenant_id":" ","allowed_repos":[],"acl_groups":[]}`),
		"oversized scope count": tooManyJSON,
	}

	for name, claims := range tests {
		t.Run(name, func(t *testing.T) {
			authenticator, _ := newTestAuthenticator(t, &stubVerifier{claims: claims})
			_, err := authenticator.Authenticate(
				context.Background(), "Bearer signed.jwt.value", validTraceID,
			)
			if code := authCode(t, err); code != ErrorInvalidClaims {
				t.Fatalf("code = %q, want %q", code, ErrorInvalidClaims)
			}
		})
	}
}

func TestSecurityContextTransportBudgetFailsClosed(t *testing.T) {
	largeScope := make([]string, 80)
	for index := range largeScope {
		largeScope[index] = strings.Repeat("r", 200) + string(rune('a'+index%26))
	}
	raw, err := json.Marshal(map[string]any{
		"sub": "user-7", "typ": "Bearer", "tenant_id": "tenant-42",
		"allowed_repos": largeScope, "acl_groups": []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, _ := newTestAuthenticator(t, &stubVerifier{claims: raw})

	securityContext, authenticateErr := authenticator.Authenticate(
		context.Background(), "Bearer signed.jwt.value", validTraceID,
	)
	if securityContext != nil {
		t.Fatalf("oversized context returned: %v", securityContext)
	}
	var authError *AuthError
	if !errors.As(authenticateErr, &authError) || authError.Code != ErrorInvalidClaims {
		t.Fatalf("error=%v want invalid_claims", authenticateErr)
	}
}

func TestEmptyScopeArraysAreValidAndDoNotDefault(t *testing.T) {
	claims := json.RawMessage(`{
        "sub":"user-7","typ":"Bearer","tenant_id":"tenant-42",
        "allowed_repos":[],"acl_groups":[]
    }`)
	authenticator, _ := newTestAuthenticator(t, &stubVerifier{claims: claims})

	securityContext, err := authenticator.Authenticate(
		context.Background(), "Bearer signed.jwt.value", validTraceID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(securityContext.GetAllowedRepos()) != 0 || len(securityContext.GetAclGroups()) != 0 {
		t.Fatalf("unexpected default scope: %+v", securityContext)
	}
}

func TestAuthorizationHeaderSizeIsBounded(t *testing.T) {
	verifier := &stubVerifier{claims: validClaims()}
	authenticator, _ := newTestAuthenticator(t, verifier)
	header := "Bearer " + string(make([]byte, maxAuthorizationHeaderBytes))

	_, err := authenticator.Authenticate(context.Background(), header, validTraceID)
	if code := authCode(t, err); code != ErrorInvalidToken {
		t.Fatalf("code = %q, want %q", code, ErrorInvalidToken)
	}
	if verifier.calls != 0 {
		t.Fatalf("verifier calls = %d, want 0", verifier.calls)
	}
}
