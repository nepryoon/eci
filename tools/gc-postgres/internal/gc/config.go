// Package gc implementa la garbage collection periodica di outbox/
// processed_events (SPEC-028, T2.6 parte 2/2): parsing configurazione
// (flag + env, default 168h) e purge indipendente per tabella.
package gc

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// DefaultRetention è il default dichiarato per entrambe le soglie
// (SPEC-028 §2): non specificato dall'ADD, scelto come corrispondenza
// ragionevole con la retention di default comune di Kafka.
const DefaultRetention = "168h"

// Config — parametri di un'esecuzione di gc-postgres (SPEC-028 §2).
type Config struct {
	DSN                      string
	OutboxRetention          time.Duration
	ProcessedEventsRetention time.Duration
	DryRun                   bool
}

// ParseConfig legge `args` (tipicamente os.Args[1:]) come flag CLI
// (SPEC-028 §2: `--dsn`, `--outbox-retention`, `--processed-events-retention`,
// `--dry-run`). Le soglie di retention, se non passate via flag, cadono
// su `GC_OUTBOX_RETENTION`/`GC_PROCESSED_EVENTS_RETENTION` (env), poi su
// DefaultRetention — mai hardcoded senza un punto di override. Un valore
// di retention non parsabile come durata è un errore esplicito (SPEC-028
// §4, fail-fast), non un default silenzioso.
func ParseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("gc-postgres", flag.ContinueOnError)
	dsn := fs.String("dsn", "", "Postgres DSN (obbligatorio)")
	outboxRetentionFlag := fs.String("outbox-retention", "", "retention per outbox (default "+DefaultRetention+" o GC_OUTBOX_RETENTION)")
	processedEventsRetentionFlag := fs.String("processed-events-retention", "", "retention per processed_events (default "+DefaultRetention+" o GC_PROCESSED_EVENTS_RETENTION)")
	dryRun := fs.Bool("dry-run", false, "stampa solo i conteggi che sarebbero eliminati, senza eliminare")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if *dsn == "" {
		return Config{}, fmt.Errorf("--dsn è obbligatorio")
	}

	outboxRetention, err := resolveDuration(*outboxRetentionFlag, "GC_OUTBOX_RETENTION")
	if err != nil {
		return Config{}, fmt.Errorf("--outbox-retention: %w", err)
	}
	processedEventsRetention, err := resolveDuration(*processedEventsRetentionFlag, "GC_PROCESSED_EVENTS_RETENTION")
	if err != nil {
		return Config{}, fmt.Errorf("--processed-events-retention: %w", err)
	}

	return Config{
		DSN:                      *dsn,
		OutboxRetention:          outboxRetention,
		ProcessedEventsRetention: processedEventsRetention,
		DryRun:                   *dryRun,
	}, nil
}

// resolveDuration applica la precedenza flag > env > default dichiarata
// da SPEC-028 §2, poi parsa il risultato come time.Duration.
func resolveDuration(flagValue, envKey string) (time.Duration, error) {
	raw := flagValue
	if raw == "" {
		raw = envOrDefault(envKey, DefaultRetention)
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("valore di retention non parsabile come durata (%q): %w", raw, err)
	}
	return d, nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
