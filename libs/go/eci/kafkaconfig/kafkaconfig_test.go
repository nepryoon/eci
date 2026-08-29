package kafkaconfig

import (
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestFromEnvironmentTLSIsFailClosed(t *testing.T) {
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	t.Setenv("KAFKA_TLS_CA_FILE", "")
	if _, err := FromEnvironment(); err == nil {
		t.Fatal("expected missing CA to fail")
	}

	server := httptest.NewTLSServer(nil)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ca.crt")
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_TLS_CA_FILE", path)
	config, err := FromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.Dialer == nil || config.Dialer.TLS == nil || config.Transport == nil || config.Transport.TLS == nil {
		t.Fatal("expected TLS on reader dialer and writer transport")
	}
}
