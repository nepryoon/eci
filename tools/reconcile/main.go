// Command reconcile è il job periodico di riconciliazione (SPEC-037,
// T3.4 parte 1/4): enumera i fingerprint reali da Postgres, li confronta
// con lo stato di una vista downstream, e per ogni mismatch ripubblica
// la riga outbox corrispondente. Binario standalone invocabile
// manualmente o da un cron/systemd timer esterno, non un servizio
// long-running (stesso principio di tools/gc-postgres, SPEC-028, §5).
//
// SPEC-037 costruiva solo l'orchestrazione condivisa, con `targets` vuota
// (§2/§5). SPEC-038 (T3.4 parte 2/4) aggiunge qui la prima voce reale,
// "neo4j", senza toccare il framework — i due plugin restanti (Qdrant/
// OpenSearch, parti 3/4-4/4) aggiungeranno la propria allo stesso modo.
//
// Uso: reconcile --dsn <postgres-dsn> --target <nome>
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/neo4jtarget"
)

func main() {
	ctx := context.Background()

	cfg, err := framework.ParseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(2)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)

	neo4jURI := config.EnvOrDefault("NEO4J_URI", "bolt://localhost:7687")
	neo4jUser := config.EnvOrDefault("NEO4J_USER", "neo4j")
	neo4jPassword := config.EnvOrDefault("NEO4J_PASSWORD", "eci-dev-only")
	neo4jDriver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPassword, ""))
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE: creazione driver Neo4j:", err)
		os.Exit(1)
	}
	defer neo4jDriver.Close(ctx)

	// db=nil: neo4jtarget.New lo accetta per l'interfaccia dichiarata da
	// SPEC-038 §2 ma non lo referenzia mai (SourceRows/Republish operano
	// sulla *sql.DB/*sql.Tx che il framework stesso apre da cfg.DSN dentro
	// OpenAndRun, sotto — vedi commento su neo4jtarget.New). Aprire qui una
	// SECONDA connessione Postgres solo per riempire questo parametro
	// sarebbe una risorsa spesa per niente.
	targets := map[string]framework.Target{
		"neo4j": neo4jtarget.New(neo4jDriver, nil),
	}

	report, err := framework.OpenAndRun(ctx, cfg, targets, logger.Printf)
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
