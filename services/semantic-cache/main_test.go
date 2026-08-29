package main

import "testing"

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
