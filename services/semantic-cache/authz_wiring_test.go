package main

import (
	"os"
	"strings"
	"testing"
)

func TestMainWiresSecurityContextBeforeUnaryPEP(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	source := string(raw)
	for _, required := range []string{
		"authz.ConfigFromEnvironment(\"semantic-cache\")",
		"authz.New(ctx, authzConfig, prometheus.DefaultRegisterer)",
		"grpc.ChainUnaryInterceptor(\n\t\t\tsecctx.UnaryServerInterceptor(),\n\t\t\tauthz.UnaryServerInterceptor(authorizer)",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("main.go missing required fail-closed wiring %q", required)
		}
	}
}
