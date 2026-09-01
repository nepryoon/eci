package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func TestDependencyReadinessRequiresEveryBackend(t *testing.T) {
	var calls atomic.Int32
	ready := func(context.Context) error {
		calls.Add(1)
		return nil
	}
	if err := checkDependencies(context.Background(), ready, ready, ready, ready, ready); err != nil {
		t.Fatalf("healthy dependencies failed: %v", err)
	}
	if calls.Load() != 5 {
		t.Fatalf("checks called = %d, want 5", calls.Load())
	}
	if err := checkDependencies(context.Background(), ready, func(context.Context) error {
		return errors.New("backend secret detail")
	}); err == nil {
		t.Fatal("failed dependency reported ready")
	}
}

func TestChunkCursorMigrationMarkerIsRequiredBeforeReaderStarts(t *testing.T) {
	for _, test := range []struct {
		name    string
		mapping string
		wantErr bool
	}{
		{name: "verified", mapping: `{"code_chunks":{"mappings":{"_meta":{"eci_chunk_cursor_schema":1}}}}`},
		{name: "legacy mapping", mapping: `{"code_chunks":{"mappings":{"properties":{"chunk_id":{"type":"keyword"}}}}}`, wantErr: true},
		{name: "wrong generation", mapping: `{"code_chunks":{"mappings":{"_meta":{"eci_chunk_cursor_schema":2}}}}`, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || request.URL.Path != "/code_chunks/_mapping" {
					http.Error(response, fmt.Sprintf("unexpected %s %s", request.Method, request.URL.Path), http.StatusBadRequest)
					return
				}
				response.Header().Set("Content-Type", "application/json")
				_, _ = response.Write([]byte(test.mapping))
			}))
			defer server.Close()
			client, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{server.URL}}})
			if err != nil {
				t.Fatal(err)
			}
			err = requireChunkCursorMigration(context.Background(), client)
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestDependencyReadinessHandlerIsFailClosedAndLowDetail(t *testing.T) {
	for _, test := range []struct {
		name   string
		checks []dependencyCheck
		status int
	}{
		{name: "ready", checks: []dependencyCheck{func(context.Context) error { return nil }}, status: http.StatusNoContent},
		{name: "not ready", checks: []dependencyCheck{func(context.Context) error { return errors.New("backend secret detail") }}, status: http.StatusServiceUnavailable},
		{name: "missing checks", status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			newDependencyReadinessHandler(test.checks...).ServeHTTP(
				response,
				httptest.NewRequest(http.MethodGet, "/ready", nil),
			)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if response.Body.String() != "" {
				t.Fatalf("readiness leaked body %q", response.Body.String())
			}
		})
	}
}
