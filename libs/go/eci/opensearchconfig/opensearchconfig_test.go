package opensearchconfig

import (
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHTTPSRequiresTrustAndCredentials(t *testing.T) {
	t.Setenv("OPENSEARCH_USERNAME", "")
	t.Setenv("OPENSEARCH_PASSWORD", "")
	t.Setenv("OPENSEARCH_CA_FILE", "")
	if _, err := FromEnvironment("https://opensearch.example"); err == nil {
		t.Fatal("expected HTTPS without trust/auth to fail")
	}

	server := httptest.NewTLSServer(nil)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ca.crt")
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENSEARCH_USERNAME", "eci-client")
	t.Setenv("OPENSEARCH_PASSWORD", "test-password")
	t.Setenv("OPENSEARCH_CA_FILE", path)
	config, err := FromEnvironment("https://opensearch.example")
	if err != nil {
		t.Fatal(err)
	}
	if config.Username != "eci-client" || config.Password != "test-password" || len(config.CACert) == 0 {
		t.Fatalf("unexpected config: username=%q ca=%d", config.Username, len(config.CACert))
	}
}
