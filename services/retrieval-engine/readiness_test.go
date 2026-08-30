package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
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
