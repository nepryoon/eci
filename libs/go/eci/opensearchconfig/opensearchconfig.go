// Package opensearchconfig validates TLS trust and authentication configuration
// for OpenSearch clients from trusted process environment only.
package opensearchconfig

import (
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	Username string
	Password string
	CACert   []byte
}

func FromEnvironment(endpoint string) (Config, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return Config{}, fmt.Errorf("OPENSEARCH_URL must be an absolute URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Config{}, fmt.Errorf("OPENSEARCH_URL scheme must be http or https")
	}
	username := os.Getenv("OPENSEARCH_USERNAME")
	password := os.Getenv("OPENSEARCH_PASSWORD")
	if (username == "") != (password == "") {
		return Config{}, fmt.Errorf("OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD must be supplied together")
	}
	result := Config{Username: username, Password: password}
	if parsed.Scheme != "https" {
		return result, nil
	}
	if username == "" || password == "" {
		return Config{}, fmt.Errorf("OpenSearch HTTPS requires OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD")
	}
	caFile := strings.TrimSpace(os.Getenv("OPENSEARCH_CA_FILE"))
	if caFile == "" {
		return Config{}, fmt.Errorf("OpenSearch HTTPS requires OPENSEARCH_CA_FILE")
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return Config{}, fmt.Errorf("OpenSearch CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return Config{}, fmt.Errorf("OpenSearch CA file contains no PEM certificates")
	}
	result.CACert = ca
	return result, nil
}
