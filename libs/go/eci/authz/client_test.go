package authz

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/prometheus/client_golang/prometheus"
)

type capturedInput struct {
	Input struct {
		Subject struct {
			TenantID     string   `json:"tenant_id"`
			UserID       string   `json:"user_id"`
			AllowedRepos []string `json:"allowed_repos"`
			ACLGroups    []string `json:"acl_groups"`
		} `json:"subject"`
		Action string `json:"action"`
	} `json:"input"`
}

func TestClientUsesStrictOPAEnvelopeAndNormalizesReason(t *testing.T) {
	var mu sync.Mutex
	var captured []capturedInput
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/v1/data/eci/authz/decision":
			var value capturedInput
			if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
				t.Errorf("decode OPA request: %v", err)
			}
			mu.Lock()
			captured = append(captured, value)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"result":{"allow":false,"reason":"attacker-controlled-cardinality"},"decision_id":"opaque"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(context.Background(), Config{
		Endpoint:          server.URL,
		DecisionPath:      "/v1/data/eci/authz/decision",
		Service:           "retrieval-engine",
		Timeout:           time.Second,
		AllowInsecureHTTP: true,
	}, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sc := &retrievalv1.SecurityContext{
		TenantId:     "tenant-a",
		UserId:       "user-a",
		AllowedRepos: []string{"repo-b", "repo-a"},
		AclGroups:    []string{"group-b", "group-a"},
		TraceId:      "must-not-be-sent-to-opa",
	}
	decision, err := client.Decide(context.Background(), sc, getNodeMethod)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if decision.Allow || decision.Reason != "policy_denied" {
		t.Fatalf("decision = %+v, want normalized deny", decision)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 {
		t.Fatalf("captured requests = %d, want 1", len(captured))
	}
	input := captured[0].Input
	if input.Subject.TenantID != "tenant-a" || input.Subject.UserID != "user-a" || input.Action != getNodeMethod {
		t.Fatalf("OPA input = %+v", input)
	}
	raw, _ := json.Marshal(captured[0])
	for _, forbidden := range []string{"trace_id", "must-not-be-sent-to-opa", "prompt", "query"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("OPA envelope contains forbidden field/value %q: %s", forbidden, raw)
		}
	}
}

func TestClientFailsClosedOnStartupAndRuntimeProtocolErrors(t *testing.T) {
	t.Run("startup health unavailable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
		}))
		defer server.Close()
		_, err := New(context.Background(), validConfig(server.URL), prometheus.NewRegistry())
		if err == nil {
			t.Fatal("New succeeded with unhealthy PDP")
		}
	})

	for _, test := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "non-200",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "secret upstream detail", http.StatusInternalServerError)
			},
		},
		{
			name: "malformed JSON",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, `{not-json`)
			},
		},
		{
			name: "missing result",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, `{}`)
			},
		},
		{
			name: "oversize",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, strings.Repeat("x", maxResponseBytes+1))
			},
		},
		{
			name: "timeout",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				time.Sleep(100 * time.Millisecond)
				fmt.Fprint(w, `{"result":{"allow":true,"reason":"allow"}}`)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := decisionServer(test.handler)
			defer server.Close()
			cfg := validConfig(server.URL)
			if test.name == "timeout" {
				cfg.Timeout = 10 * time.Millisecond
			}
			client, err := New(context.Background(), cfg, prometheus.NewRegistry())
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			_, err = client.Decide(context.Background(), validSubject(), getNodeMethod)
			if err == nil {
				t.Fatal("Decide succeeded on protocol failure")
			}
			if strings.Contains(err.Error(), "secret upstream detail") {
				t.Fatalf("error leaked response body: %v", err)
			}
		})
	}
}

func TestConfigRejectsUnsafeOrAmbiguousValues(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{Endpoint: "http://opa:8181", DecisionPath: "/v1/data/eci/authz/decision", Service: "retrieval-engine", Timeout: time.Second},
		{Endpoint: "https://opa.example", DecisionPath: "relative", Service: "retrieval-engine", Timeout: time.Second},
		{Endpoint: "https://opa.example", DecisionPath: "/v1/data/eci/authz/decision", Service: "", Timeout: time.Second},
		{Endpoint: "https://opa.example", DecisionPath: "/v1/data/eci/authz/decision", Service: "retrieval-engine", Timeout: 3 * time.Second},
	} {
		if _, err := New(context.Background(), cfg, prometheus.NewRegistry()); err == nil {
			t.Fatalf("New accepted invalid config: %+v", cfg)
		}
	}
}

func validConfig(endpoint string) Config {
	return Config{
		Endpoint:          endpoint,
		DecisionPath:      "/v1/data/eci/authz/decision",
		Service:           "retrieval-engine",
		Timeout:           time.Second,
		AllowInsecureHTTP: true,
	}
}

func validSubject() *retrievalv1.SecurityContext {
	return &retrievalv1.SecurityContext{
		TenantId:     "tenant-a",
		UserId:       "user-a",
		AllowedRepos: []string{"repo-a"},
		AclGroups:    []string{"engineering"},
	}
}

func decisionServer(decision http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		decision(w, r)
	}))
}
