package main

import (
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMainWiresSecurityContextBeforeUnaryAndStreamPEP(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	source := string(raw)
	for _, required := range []string{
		"authz.ConfigFromEnvironment(\"retrieval-engine\")",
		"authz.New(ctx, authzConfig, prometheus.DefaultRegisterer)",
		"grpc.ChainUnaryInterceptor(\n\t\t\tsecctx.UnaryServerInterceptor(),\n\t\t\tauthz.UnaryServerInterceptor(authorizer)",
		"grpc.ChainStreamInterceptor(\n\t\t\tsecctx.StreamServerInterceptor(),\n\t\t\tauthz.StreamServerInterceptor(authorizer)",
		"newMetricsHandler(prometheus.DefaultGatherer)",
		"defaultMetricsPort = \"9105\"",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("main.go missing required fail-closed wiring %q", required)
		}
	}
}

func TestMetricsHandlerExposesAuthorizationCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "eci_authz_decisions_total"})
	registry.MustRegister(counter)
	counter.Inc()
	request := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()
	newMetricsHandler(registry).ServeHTTP(response, request)
	raw, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatalf("read metrics response: %v", err)
	}
	if response.Code != 200 || !strings.Contains(string(raw), "eci_authz_decisions_total 1") {
		t.Fatalf("metrics response code=%d body=%s", response.Code, raw)
	}
}
