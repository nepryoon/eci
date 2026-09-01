//go:build integration

package authn

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const keycloakImage = "quay.io/keycloak/keycloak:26.7.2"

func ephemeralPassword(t *testing.T) string {
	t.Helper()
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate ephemeral password: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(value)
}

func TestKeycloakRealmIssuesTokenAcceptedByGatewayAuthenticator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	realmFile, err := filepath.Abs("../../../../deploy/compose/keycloak/eci-realm.json")
	if err != nil {
		t.Fatalf("realm path: %v", err)
	}
	adminPassword := ephemeralPassword(t)
	userPassword := ephemeralPassword(t)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        keycloakImage,
			ExposedPorts: []string{"8080/tcp"},
			Env: map[string]string{
				"KC_BOOTSTRAP_ADMIN_USERNAME": "admin",
				"KC_BOOTSTRAP_ADMIN_PASSWORD": adminPassword,
				"KC_HEALTH_ENABLED":           "true",
				"ECI_DEV_USER_PASSWORD":       userPassword,
			},
			Cmd: []string{"start-dev", "--import-realm"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      realmFile,
				ContainerFilePath: "/opt/keycloak/data/import/eci-realm.json",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForHTTP("/realms/eci/.well-known/openid-configuration").
				WithPort("8080/tcp").
				WithStartupTimeout(2 * time.Minute),
		},
	})
	if err != nil {
		t.Fatalf("start Keycloak %s: %v", keycloakImage, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("terminate Keycloak: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Keycloak host: %v", err)
	}
	port, err := container.MappedPort(ctx, "8080/tcp")
	if err != nil {
		t.Fatalf("Keycloak port: %v", err)
	}
	issuer := fmt.Sprintf("http://%s:%s/realms/eci", host, port.Port())
	token := requestKeycloakToken(t, ctx, issuer, userPassword)

	authenticator, err := New(ctx, Config{
		Issuer:                  issuer,
		Audience:                "eci-gateway",
		AllowHTTPForDevelopment: true,
	}, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New against Keycloak: %v", err)
	}
	securityContext, err := authenticator.Authenticate(
		ctx, "Bearer "+token, validTraceID,
	)
	if err != nil {
		t.Fatalf("Authenticate Keycloak token: %v", err)
	}

	if securityContext.GetTenantId() != "tenant-dev" {
		t.Fatalf("tenant_id = %q", securityContext.GetTenantId())
	}
	if securityContext.GetUserId() == "" {
		t.Fatal("user_id (sub) is empty")
	}
	if !slices.Equal(securityContext.GetAllowedRepos(), []string{"sample-repo"}) {
		t.Fatalf("allowed_repos = %v", securityContext.GetAllowedRepos())
	}
	if !slices.Equal(securityContext.GetAclGroups(), []string{"developers"}) {
		t.Fatalf("acl_groups = %v", securityContext.GetAclGroups())
	}
}

func requestKeycloakToken(t *testing.T, ctx context.Context, issuer, password string) string {
	t.Helper()
	form := url.Values{
		"grant_type": {"password"},
		"client_id":  {"eci-dev-cli"},
		"username":   {"eci-dev"},
		"password":   {password},
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		issuer+"/protocol/openid-connect/token",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		t.Fatalf("build token request: %v", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("request token: %v", err)
	}
	defer response.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode token response (status %d): %v", response.StatusCode, err)
	}
	if response.StatusCode != http.StatusOK || body.AccessToken == "" {
		t.Fatalf("token endpoint status=%d error=%q description=%q", response.StatusCode, body.Error, body.Description)
	}
	return body.AccessToken
}
