package config

import (
	"os"
	"testing"
)

// SPEC-011 §3 scenario 4 (equivalente Go di SPEC-010 §3 scenario 4).

func TestEnvOrDefaultReturnsDefaultWhenUnset(t *testing.T) {
	key := "ECI_TEST_CONFIG_UNSET"
	os.Unsetenv(key)
	if got := EnvOrDefault(key, "./sample-repo"); got != "./sample-repo" {
		t.Fatalf("EnvOrDefault(%q, ...) = %q, want %q", key, got, "./sample-repo")
	}
}

func TestEnvOrDefaultReturnsOverrideWhenSet(t *testing.T) {
	key := "ECI_TEST_CONFIG_OVERRIDE"
	t.Setenv(key, "/custom/path")
	if got := EnvOrDefault(key, "./sample-repo"); got != "/custom/path" {
		t.Fatalf("EnvOrDefault(%q, ...) = %q, want %q", key, got, "/custom/path")
	}
}

// SPEC-011 §4 edge case: stringa vuota impostata esplicitamente è un
// valore, non un'assenza.
func TestEnvOrDefaultExplicitEmptyStringIsNotTreatedAsAbsent(t *testing.T) {
	key := "ECI_TEST_CONFIG_EMPTY"
	t.Setenv(key, "")
	if got := EnvOrDefault(key, "./sample-repo"); got != "" {
		t.Fatalf("EnvOrDefault(%q, ...) = %q, want empty string (not default)", key, got)
	}
}
