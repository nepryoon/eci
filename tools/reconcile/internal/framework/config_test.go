package framework

import "testing"

func TestParseConfigMissingDSNIsExplicitError(t *testing.T) {
	_, err := ParseConfig([]string{"--target", "fake"})
	if err == nil {
		t.Fatal("ParseConfig senza --dsn: atteso un errore, ottenuto nil")
	}
}

func TestParseConfigMissingTargetIsExplicitError(t *testing.T) {
	_, err := ParseConfig([]string{"--dsn", "postgres://x/y"})
	if err == nil {
		t.Fatal("ParseConfig senza --target: atteso un errore, ottenuto nil")
	}
}

func TestParseConfigOK(t *testing.T) {
	cfg, err := ParseConfig([]string{"--dsn", "postgres://x/y", "--target", "neo4j"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.DSN != "postgres://x/y" {
		t.Errorf("DSN = %q, want %q", cfg.DSN, "postgres://x/y")
	}
	if cfg.Target != "neo4j" {
		t.Errorf("Target = %q, want %q", cfg.Target, "neo4j")
	}
}
