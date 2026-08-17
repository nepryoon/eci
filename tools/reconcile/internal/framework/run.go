package framework

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	_ "github.com/lib/pq"
)

// OpenAndRun risolve cfg.Target contro `targets` (il binding nome->Target
// concreto avviene nei plugin, parti 2/4-4/4, che popolano questa mappa —
// §2: "non in questa SPEC"), apre la connessione DSN, verifica la
// raggiungibilità (fail-fast esplicito, stesso principio di
// tools/gc-postgres SPEC-028 §4), poi esegue Reconcile.
func OpenAndRun(ctx context.Context, cfg Config, targets map[string]Target, logf func(format string, args ...any)) (Report, error) {
	target, ok := targets[cfg.Target]
	if !ok {
		return Report{}, fmt.Errorf("target sconosciuto: %q (disponibili: %v)", cfg.Target, targetNames(targets))
	}

	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return Report{}, fmt.Errorf("apertura connessione Postgres fallita: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return Report{}, fmt.Errorf("Postgres non raggiungibile: %w", err)
	}

	logf("reconcile: target=%s avviato", target.Name)
	report, err := Reconcile(ctx, db, target)
	if err != nil {
		return report, err
	}
	logf("reconcile: target=%s checked=%d matched=%d republished=%d errored=%d",
		target.Name, report.Checked, report.Matched, report.Republished, len(report.Errored))
	return report, nil
}

func targetNames(targets map[string]Target) []string {
	names := make([]string, 0, len(targets))
	for name := range targets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
