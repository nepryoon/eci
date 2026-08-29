package main

import (
	"testing"
	"time"
)

func TestLoadConfigRequiresTrustedIdentityAndBackendConfiguration(t *testing.T) {
	t.Setenv("ECI_OIDC_ISSUER", "")
	t.Setenv("ECI_OIDC_AUDIENCE", "")
	t.Setenv("RETRIEVAL_ENGINE_ADDR", "")

	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig accepted missing trusted configuration")
	}
}

func TestLoadConfigRejectsUnsafeOrMalformedValues(t *testing.T) {
	base := map[string]string{
		"ECI_OIDC_ISSUER":       "https://idp.example.test/realms/eci",
		"ECI_OIDC_AUDIENCE":     "eci-api",
		"RETRIEVAL_ENGINE_ADDR": "retrieval-engine:50053",
	}
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"bad dev bool", "ECI_OIDC_ALLOW_HTTP_DEV", "sometimes"},
		{"bad max body", "API_GATEWAY_MAX_JSON_BODY_BYTES", "0"},
		{"bad timeout", "API_GATEWAY_REQUEST_TIMEOUT", "31s"},
		{"whitespace backend", "RETRIEVAL_ENGINE_ADDR", " retrieval-engine:50053"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range base {
				t.Setenv(key, value)
			}
			t.Setenv("ECI_OIDC_ALLOW_HTTP_DEV", "false")
			t.Setenv("API_GATEWAY_MAX_JSON_BODY_BYTES", "1048576")
			t.Setenv("API_GATEWAY_REQUEST_TIMEOUT", "30s")
			t.Setenv(test.key, test.value)
			if _, err := loadConfig(); err == nil {
				t.Fatal("loadConfig accepted invalid value")
			}
		})
	}
}

func TestLoadConfigUsesBoundedOperationalDefaults(t *testing.T) {
	t.Setenv("ECI_OIDC_ISSUER", "https://idp.example.test/realms/eci")
	t.Setenv("ECI_OIDC_AUDIENCE", "eci-api")
	t.Setenv("RETRIEVAL_ENGINE_ADDR", "retrieval-engine:50053")
	t.Setenv("ECI_OIDC_ALLOW_HTTP_DEV", "")
	t.Setenv("API_GATEWAY_ADDR", "")
	t.Setenv("API_GATEWAY_METRICS_ADDR", "")
	t.Setenv("API_GATEWAY_MAX_JSON_BODY_BYTES", "")
	t.Setenv("API_GATEWAY_REQUEST_TIMEOUT", "")

	got, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if got.allowHTTPForDevelopment || got.listenAddress != ":8081" || got.metricsAddress != ":9107" {
		t.Fatalf("unexpected defaults: %+v", got)
	}
	if got.maxJSONBodyBytes != 1<<20 || got.requestTimeout != 30*time.Second {
		t.Fatalf("unexpected bounds: %+v", got)
	}
}
