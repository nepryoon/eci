// run_test.go — wiring della CLI (Config -> apertura DB -> Reconcile),
// invocata con un Target FINTO nei propri test (SPEC-037 §2: "la CLI
// che lo invoca con un target FINTO nei propri test"). Nessun bisogno di
// Postgres reale: il caso DSN irraggiungibile fallisce prima di
// qualunque query, il caso "target sconosciuto" fallisce prima ancora di
// aprire una connessione.
package framework

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func fakeTargetForCLI() Target {
	return Target{
		Name: "fake",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return nil, nil
		},
		Check:     func(ctx context.Context, row SourceRow) (bool, error) { return true, nil },
		Republish: func(ctx context.Context, tx *sql.Tx, row SourceRow) error { return nil },
	}
}

func TestOpenAndRunUnknownTargetIsExplicitError(t *testing.T) {
	cfg := Config{DSN: "postgres://x/y", Target: "does-not-exist"}
	targets := map[string]Target{"fake": fakeTargetForCLI()}

	_, err := OpenAndRun(context.Background(), cfg, targets, t.Logf)
	if err == nil {
		t.Fatal("OpenAndRun con target sconosciuto: atteso un errore, ottenuto nil")
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("errore = %q, atteso che nomini il target sconosciuto", err.Error())
	}
}

func TestOpenAndRunUnreachableDSNIsExplicitError(t *testing.T) {
	cfg := Config{DSN: "postgres://eci:x@127.0.0.1:1/eci?sslmode=disable&connect_timeout=2", Target: "fake"}
	targets := map[string]Target{"fake": fakeTargetForCLI()}

	_, err := OpenAndRun(context.Background(), cfg, targets, t.Logf)
	if err == nil {
		t.Fatal("OpenAndRun con DSN irraggiungibile: atteso un errore, ottenuto nil")
	}
}
