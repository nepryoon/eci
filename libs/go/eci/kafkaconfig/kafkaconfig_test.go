package kafkaconfig

import (
	"crypto/x509"
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

func TestFromEnvironmentMTLSRequiresAndLoadsClientIdentity(t *testing.T) {
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	t.Setenv("KAFKA_MTLS_ENABLED", "true")

	server := httptest.NewTLSServer(nil)
	defer server.Close()
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.crt")
	certPath := filepath.Join(tempDir, "user.crt")
	keyPath := filepath.Join(tempDir, "user.key")
	certificate := server.TLS.Certificates[0]
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	if err := os.WriteFile(caPath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_TLS_CA_FILE", caPath)
	t.Setenv("KAFKA_TLS_CERT_FILE", certPath)
	t.Setenv("KAFKA_TLS_KEY_FILE", "")
	if _, err := FromEnvironment(); err == nil {
		t.Fatal("expected missing client key to fail closed")
	}

	privateKey, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_TLS_KEY_FILE", keyPath)
	config, err := FromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(config.Dialer.TLS.Certificates); got != 1 {
		t.Fatalf("reader TLS client certificates = %d, want 1", got)
	}
	if got := len(config.Transport.TLS.Certificates); got != 1 {
		t.Fatalf("writer TLS client certificates = %d, want 1", got)
	}
}
