// Command reconcile è il job periodico di riconciliazione (SPEC-037,
// T3.4 parte 1/4): enumera i fingerprint reali da Postgres, li confronta
// con lo stato di una vista downstream, e per ogni mismatch ripubblica
// la riga outbox corrispondente. Binario standalone invocabile
// manualmente o da un cron/systemd timer esterno, non un servizio
// long-running (stesso principio di tools/gc-postgres, SPEC-028, §5).
//
// SPEC-037 costruiva solo l'orchestrazione condivisa, con `targets` vuota
// (§2/§5). SPEC-038 (T3.4 parte 2/4) ha aggiunto qui la prima voce reale,
// "neo4j"; SPEC-039 (T3.4 parte 3/4) ha aggiunto "qdrant"; SPEC-040
// (T3.4 parte 4/4, ultima) aggiunge "opensearch" allo stesso modo, senza
// toccare il framework — con questa, T3.4 e Fase 3 sono complete.
//
// Uso: reconcile --dsn <postgres-dsn> --target <nome>
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/qdrant/go-client/qdrant"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/neo4jtarget"
	"github.com/eci-project/eci/tools/reconcile/internal/opensearchtarget"
	"github.com/eci-project/eci/tools/reconcile/internal/qdranttarget"
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

	qdrantHost := config.EnvOrDefault("QDRANT_HOST", "localhost")
	qdrantPort := config.EnvOrDefault("QDRANT_GRPC_PORT", "6334")
	qdrantPortNum, err := strconv.Atoi(qdrantPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERRORE: QDRANT_GRPC_PORT non valido (%q): %v\n", qdrantPort, err)
		os.Exit(1)
	}
	qdrantClient, err := qdrant.NewClient(&qdrant.Config{Host: qdrantHost, Port: qdrantPortNum})
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE: creazione client Qdrant:", err)
		os.Exit(1)
	}
	defer qdrantClient.Close()

	openSearchURL := config.EnvOrDefault("OPENSEARCH_URL", "http://localhost:9200")
	openSearchClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{openSearchURL}},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE: creazione client OpenSearch:", err)
		os.Exit(1)
	}

	// db=nil per tutti e tre i target: neo4jtarget.New/qdranttarget.New/
	// opensearchtarget.New accettano *sql.DB per l'interfaccia dichiarata
	// da SPEC-038/039/040 §2 ma non lo referenziano mai (SourceRows/
	// Republish operano sulla *sql.DB/*sql.Tx che il framework stesso apre
	// da cfg.DSN dentro OpenAndRun, sotto — vedi commento su
	// neo4jtarget.New/qdranttarget.New/opensearchtarget.New). Aprire qui
	// una SECONDA connessione Postgres solo per riempire questo parametro
	// sarebbe una risorsa spesa per niente.
	targets := map[string]framework.Target{
		"neo4j":      neo4jtarget.New(neo4jDriver, nil),
		"qdrant":     qdranttarget.New(qdrantClient, nil),
		"opensearch": opensearchtarget.New(openSearchClient, nil),
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
