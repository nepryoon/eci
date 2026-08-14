package gc

import (
	"strings"
	"testing"
	"time"
)

func TestParseConfigDefaultsRetentionTo168hWhenUnset(t *testing.T) {
	cfg, err := ParseConfig([]string{"--dsn", "postgres://x/y"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	want := 168 * time.Hour
	if cfg.OutboxRetention != want {
		t.Errorf("OutboxRetention = %v, want %v", cfg.OutboxRetention, want)
	}
	if cfg.ProcessedEventsRetention != want {
		t.Errorf("ProcessedEventsRetention = %v, want %v", cfg.ProcessedEventsRetention, want)
	}
	if cfg.DryRun {
		t.Errorf("DryRun = true, want false (default)")
	}
}

func TestParseConfigMissingDSNIsExplicitError(t *testing.T) {
	_, err := ParseConfig([]string{})
	if err == nil {
		t.Fatal("ParseConfig senza --dsn: atteso un errore, ottenuto nil")
	}
}

// SPEC-028 §4: valore di retention non parsabile -> errore esplicito
// fail-fast, non un default silenzioso.
func TestParseConfigUnparsableRetentionFlagIsExplicitError(t *testing.T) {
	_, err := ParseConfig([]string{"--dsn", "postgres://x/y", "--outbox-retention", "abc"})
	if err == nil {
		t.Fatal("ParseConfig con --outbox-retention=abc: atteso un errore, ottenuto nil")
	}
	if !strings.Contains(err.Error(), "outbox-retention") {
		t.Errorf("errore = %q, atteso che nomini il flag incriminato", err.Error())
	}
}

func TestParseConfigFlagOverridesRetentionExplicitly(t *testing.T) {
	cfg, err := ParseConfig([]string{
		"--dsn", "postgres://x/y",
		"--outbox-retention", "720h",
		"--processed-events-retention", "24h",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.OutboxRetention != 720*time.Hour {
		t.Errorf("OutboxRetention = %v, want 720h", cfg.OutboxRetention)
	}
	if cfg.ProcessedEventsRetention != 24*time.Hour {
		t.Errorf("ProcessedEventsRetention = %v, want 24h", cfg.ProcessedEventsRetention)
	}
}

func TestParseConfigDryRunFlag(t *testing.T) {
	cfg, err := ParseConfig([]string{"--dsn", "postgres://x/y", "--dry-run"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if !cfg.DryRun {
		t.Errorf("DryRun = false, want true")
	}
}

// SPEC-028 §2: env come secondo livello di precedenza (dopo il flag,
// prima del default).
func TestParseConfigFallsBackToEnvWhenFlagUnset(t *testing.T) {
	t.Setenv("GC_OUTBOX_RETENTION", "48h")
	cfg, err := ParseConfig([]string{"--dsn", "postgres://x/y"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.OutboxRetention != 48*time.Hour {
		t.Errorf("OutboxRetention = %v, want 48h da GC_OUTBOX_RETENTION", cfg.OutboxRetention)
	}
}

func TestParseConfigFlagTakesPrecedenceOverEnv(t *testing.T) {
	t.Setenv("GC_OUTBOX_RETENTION", "48h")
	cfg, err := ParseConfig([]string{"--dsn", "postgres://x/y", "--outbox-retention", "12h"})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.OutboxRetention != 12*time.Hour {
		t.Errorf("OutboxRetention = %v, want 12h (flag deve prevalere sull'env)", cfg.OutboxRetention)
	}
}
