package main

import (
	"os"
	"strings"
	"testing"
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
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("main.go missing required fail-closed wiring %q", required)
		}
	}
}
