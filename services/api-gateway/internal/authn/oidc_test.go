package authn

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type oidcFixture struct {
	testing *testing.T
	server  *httptest.Server
	mu      sync.RWMutex
	keys    map[string]*rsa.PublicKey
}

func newOIDCFixture(t *testing.T) *oidcFixture {
	t.Helper()
	fixture := &oidcFixture{testing: t, keys: map[string]*rsa.PublicKey{}}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *oidcFixture) serveHTTP(response http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/.well-known/openid-configuration":
		writeJSON(f.testing, response, map[string]any{
			"issuer":                                f.server.URL,
			"jwks_uri":                              f.server.URL + "/jwks",
			"authorization_endpoint":                f.server.URL + "/authorize",
			"token_endpoint":                        f.server.URL + "/token",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	case "/jwks":
		f.mu.RLock()
		keys := make([]map[string]string, 0, len(f.keys))
		for keyID, publicKey := range f.keys {
			keys = append(keys, publicJWK(keyID, publicKey))
		}
		f.mu.RUnlock()
		response.Header().Set("Cache-Control", "no-cache")
		writeJSON(f.testing, response, map[string]any{"keys": keys})
	default:
		http.NotFound(response, request)
	}
}

func writeJSON(t *testing.T, response http.ResponseWriter, value any) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(value); err != nil {
		t.Errorf("encode OIDC response: %v", err)
	}
}

func publicJWK(keyID string, key *rsa.PublicKey) map[string]string {
	exponent := big.NewInt(int64(key.E)).Bytes()
	return map[string]string{
		"alg": "RS256",
		"e":   base64.RawURLEncoding.EncodeToString(exponent),
		"kid": keyID,
		"kty": "RSA",
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"use": "sig",
	}
}

func generateRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return key
}

func (f *oidcFixture) setKey(keyID string, key *rsa.PrivateKey) {
	f.mu.Lock()
	f.keys = map[string]*rsa.PublicKey{keyID: &key.PublicKey}
	f.mu.Unlock()
}

func signedJWT(t *testing.T, keyID string, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := key.Sign(rand.Reader, digest[:], crypto.SHA256)
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func unsignedJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "."
}

func oidcClaims(issuer string) map[string]any {
	return map[string]any{
		"iss":           issuer,
		"aud":           "eci-gateway",
		"exp":           time.Now().Add(time.Hour).Unix(),
		"iat":           time.Now().Add(-time.Minute).Unix(),
		"sub":           "user-7",
		"typ":           "Bearer",
		"tenant_id":     "tenant-42",
		"allowed_repos": []string{"repo-a"},
		"acl_groups":    []string{"group-x"},
	}
}

func newOIDCAuthenticator(t *testing.T, issuer string) *Authenticator {
	t.Helper()
	authenticator, err := New(context.Background(), Config{
		Issuer:                  issuer,
		Audience:                "eci-gateway",
		AllowHTTPForDevelopment: true,
	}, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return authenticator
}

func TestOIDCVerificationAcceptsValidRS256AndRefreshesRotatedKID(t *testing.T) {
	fixture := newOIDCFixture(t)
	firstKey := generateRSAKey(t)
	fixture.setKey("first", firstKey)
	authenticator := newOIDCAuthenticator(t, fixture.server.URL)

	firstToken := signedJWT(t, "first", firstKey, oidcClaims(fixture.server.URL))
	if _, err := authenticator.Authenticate(
		context.Background(), "Bearer "+firstToken, validTraceID,
	); err != nil {
		t.Fatalf("authenticate first key: %v", err)
	}

	secondKey := generateRSAKey(t)
	fixture.setKey("second", secondKey)
	secondToken := signedJWT(t, "second", secondKey, oidcClaims(fixture.server.URL))
	if _, err := authenticator.Authenticate(
		context.Background(), "Bearer "+secondToken, validTraceID,
	); err != nil {
		t.Fatalf("authenticate rotated key: %v", err)
	}
}

func TestOIDCVerificationRejectsSignatureIssuerAudienceExpiryAndAlgorithm(t *testing.T) {
	fixture := newOIDCFixture(t)
	trustedKey := generateRSAKey(t)
	fixture.setKey("trusted", trustedKey)
	authenticator := newOIDCAuthenticator(t, fixture.server.URL)

	wrongIssuer := oidcClaims(fixture.server.URL)
	wrongIssuer["iss"] = "https://attacker.invalid"
	wrongAudience := oidcClaims(fixture.server.URL)
	wrongAudience["aud"] = "other-service"
	expired := oidcClaims(fixture.server.URL)
	expired["exp"] = time.Now().Add(-time.Minute).Unix()

	tests := map[string]string{
		"bad signature":  signedJWT(t, "trusted", generateRSAKey(t), oidcClaims(fixture.server.URL)),
		"wrong issuer":   signedJWT(t, "trusted", trustedKey, wrongIssuer),
		"wrong audience": signedJWT(t, "trusted", trustedKey, wrongAudience),
		"expired":        signedJWT(t, "trusted", trustedKey, expired),
		"alg none":       unsignedJWT(t, oidcClaims(fixture.server.URL)),
	}

	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := authenticator.Authenticate(
				context.Background(), "Bearer "+token, validTraceID,
			)
			if code := authCode(t, err); code != ErrorInvalidToken {
				t.Fatalf("code = %q, want %q", code, ErrorInvalidToken)
			}
		})
	}
}

func TestNewRejectsUnsafeOrIncompleteConfiguration(t *testing.T) {
	tests := map[string]Config{
		"missing issuer":   {Audience: "eci-gateway"},
		"missing audience": {Issuer: "https://idp.example/realms/eci"},
		"relative issuer":  {Issuer: "/realms/eci", Audience: "eci-gateway"},
		"http by default":  {Issuer: "http://localhost:8081/realms/eci", Audience: "eci-gateway"},
		"remote http with dev opt-in": {
			Issuer: "http://idp.example/realms/eci", Audience: "eci-gateway", AllowHTTPForDevelopment: true,
		},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(context.Background(), config, prometheus.NewRegistry()); err == nil {
				t.Fatal("New succeeded, want configuration error")
			}
		})
	}
}
