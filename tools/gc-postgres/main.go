// Command gc-postgres elimina periodicamente le righe più vecchie di una
// soglia configurabile da `outbox` e `processed_events` — SPEC-028
// (ADD Modulo 1 §1.6.6/§2.2: entrambe le tabelle "vanno purgate
// periodicamente per evitare crescita illimitata"). Binario standalone
// invocabile manualmente o da un cron/systemd timer esterno, non un
// servizio long-running (nessuna infrastruttura di scheduling qui, §5).
//
// Uso: gc-postgres --dsn <postgres-dsn> [--outbox-retention 168h]
//
//	[--processed-events-retention 168h] [--dry-run]
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/eci-project/eci/tools/gc-postgres/internal/gc"
)

func main() {
	cfg, err := gc.ParseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(2)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)

	summary, err := gc.OpenAndRun(context.Background(), cfg, logger.Printf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(1)
	}

	verb := "eliminate"
	if cfg.DryRun {
		verb = "che sarebbero eliminate (--dry-run)"
	}
	logger.Printf(
		"riepilogo: outbox %d righe %s, processed_events %d righe %s",
		summary.Outbox.RowsAffected, verb, summary.ProcessedEvents.RowsAffected, verb,
	)

	if summary.Failed() {
		os.Exit(1)
	}
}
