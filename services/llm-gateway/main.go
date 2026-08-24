package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/eci-project/eci/services/llm-gateway/internal/gateway"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	h, err := gateway.NewHandler(cfg, nil)
	if err != nil {
		log.Fatal(err)
	}
	addr := env("LLM_GATEWAY_ADDR", ":8002")
	log.Printf("llm-gateway listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, h))
}
func configFromEnv() (gateway.Config, error) {
	cfg := gateway.Config{Routes: map[string]gateway.Route{}, Timeout: duration("LLM_GATEWAY_TIMEOUT", 15*time.Second), FailureThreshold: integer("LLM_GATEWAY_FAILURE_THRESHOLD", 3), OpenDuration: duration("LLM_GATEWAY_OPEN_DURATION", 30*time.Second)}
	for _, item := range strings.Split(os.Getenv("LLM_GATEWAY_ROUTES"), ",") {
		if strings.TrimSpace(item) == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return cfg, fmt.Errorf("invalid LLM_GATEWAY_ROUTES item %q", item)
		}
		r, err := parseRoute(parts[1])
		if err != nil {
			return cfg, err
		}
		cfg.Routes[parts[0]] = r
	}
	if raw := os.Getenv("LLM_GATEWAY_DEFAULT"); raw != "" {
		r, err := parseRoute(raw)
		if err != nil {
			return cfg, err
		}
		cfg.DefaultRoute = r
	}
	return cfg, nil
}
func parseRoute(raw string) (gateway.Route, error) {
	parts := strings.SplitN(raw, "|", 2)
	u, err := url.Parse(parts[0])
	if err != nil {
		return gateway.Route{}, err
	}
	model := "default"
	if len(parts) == 2 {
		model = parts[1]
	}
	return gateway.Route{Upstream: u, Model: model}, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func duration(k string, d time.Duration) time.Duration {
	v, e := time.ParseDuration(os.Getenv(k))
	if e == nil {
		return v
	}
	return d
}
func integer(k string, d int) int {
	v, e := strconv.Atoi(os.Getenv(k))
	if e == nil {
		return v
	}
	return d
}
