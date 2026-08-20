// Command reconcile è il job periodico di riconciliazione (SPEC-037,
// T3.4 parte 1/4): enumera i fingerprint reali da Postgres, li confronta
// con lo stato di una vista downstream, e per ogni mismatch ripubblica
// la riga outbox corrispondente. Binario standalone invocabile
// manualmente o da un cron/systemd timer esterno, non un servizio
// long-running (stesso principio di tools/gc-postgres, SPEC-028, §5).
//
// Questa SPEC costruisce solo l'orchestrazione condivisa: nessun Target
// reale è registrato qui (§2/§5) — i tre plugin per Neo4j/Qdrant/
// OpenSearch (parti 2/4-4/4) aggiungeranno la propria voce a `targets`
// senza modificare il framework.
//
// Uso: reconcile --dsn <postgres-dsn> --target <nome>
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
)

// targets è il registro dei Target concreti disponibili alla CLI — vuoto
// in questa SPEC (§5: nessun plugin reale ancora). Le parti 2/4-4/4 lo
// popoleranno con la propria voce.
var targets = map[string]framework.Target{}

func main() {
	cfg, err := framework.ParseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(2)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)

	report, err := framework.OpenAndRun(context.Background(), cfg, targets, logger.Printf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(1)
	}

	for _, rowErr := range report.Errored {
		logger.Printf("riga in errore: id=%s err=%v", rowErr.RowID, rowErr.Err)
	}

	if len(report.Errored) > 0 {
		os.Exit(1)
	}
}
