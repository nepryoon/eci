package main

import (
	"os"
	"testing"
	"time"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("LLM_GATEWAY_ROUTES", "fake=http://fake:8001|vllm-fake,real=http://real:8000|qwen")
	t.Setenv("LLM_GATEWAY_DEFAULT", "http://fake:8001|vllm-fake")
	t.Setenv("LLM_GATEWAY_TIMEOUT", "2s")
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Routes) != 2 || cfg.Routes["real"].Model != "qwen" || cfg.Timeout != 2*time.Second || cfg.DefaultRoute.Model != "vllm-fake" {
		t.Fatalf("%+v", cfg)
	}
}
func TestInvalidRoutesEnv(t *testing.T) {
	old := os.Getenv("LLM_GATEWAY_ROUTES")
	defer os.Setenv("LLM_GATEWAY_ROUTES", old)
	t.Setenv("LLM_GATEWAY_ROUTES", "broken")
	if _, err := configFromEnv(); err == nil {
		t.Fatal("expected error")
	}
}
