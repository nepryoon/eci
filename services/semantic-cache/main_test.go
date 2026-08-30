package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRedisOptionsRequirePasswordWhenAuthenticationEnabled(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis.data-plane.svc:6379")
	t.Setenv("REDIS_REQUIRE_AUTH", "true")
	t.Setenv("REDIS_PASSWORD", "")
	if _, err := redisOptionsFromEnvironment(); err == nil {
		t.Fatal("expected missing Redis password to fail closed")
	}

	t.Setenv("REDIS_PASSWORD", "test-password")
	options, err := redisOptionsFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if options.Addr != "redis.data-plane.svc:6379" || options.Password != "test-password" {
		t.Fatalf("unexpected Redis options: addr=%q password_set=%t", options.Addr, options.Password != "")
	}
}

func TestDependencyReadinessHandlerFailsClosedWithoutLeakingRedisDetails(t *testing.T) {
	for _, test := range []struct {
		name   string
		check  func(context.Context) error
		status int
	}{
		{name: "authenticated", check: func(context.Context) error { return nil }, status: http.StatusNoContent},
		{name: "wrong password", check: func(context.Context) error { return errors.New("WRONGPASS secret detail") }, status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			newDependencyReadinessHandler(test.check).ServeHTTP(
				response,
				httptest.NewRequest(http.MethodGet, "/ready", nil),
			)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if response.Body.String() != "" {
				t.Fatalf("readiness leaked response body %q", response.Body.String())
			}
		})
	}
}
