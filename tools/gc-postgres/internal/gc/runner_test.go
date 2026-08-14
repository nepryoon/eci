package gc

import (
	"context"
	"testing"
	"time"
)

// SPEC-028 §4: DSN Postgres non raggiungibile -> errore esplicito, nessun
// tentativo silenzioso di continuare. Nessun bisogno di Docker: una porta
// locale su cui nulla ascolta rifiuta la connessione immediatamente.
func TestOpenAndRunReturnsExplicitErrorOnUnreachableDSN(t *testing.T) {
	cfg := Config{
		DSN:                      "postgres://eci:x@127.0.0.1:1/eci?sslmode=disable&connect_timeout=2",
		OutboxRetention:          time.Hour,
		ProcessedEventsRetention: time.Hour,
	}

	_, err := OpenAndRun(context.Background(), cfg, t.Logf)
	if err == nil {
		t.Fatal("OpenAndRun con DSN irraggiungibile: atteso un errore, ottenuto nil")
	}
}
