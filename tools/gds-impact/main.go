// Command gds-impact è il job batch GDS (SPEC-043, T4.3): dato un nodo di
// ingresso, proietta il sottografo reverse-reachable in-memory via Neo4j
// Graph Data Science, esegue PPR seedata + betweenness campionata + Leiden,
// combina i risultati nella formula di priorità dell'ADD, e scrive
// impact_score/community_id/impact_kind sui nodi Neo4j interessati.
// Binario standalone invocabile manualmente o da un trigger esterno, non
// un servizio long-running (stesso principio di tools/reconcile/
// tools/gc-postgres).
//
// Uso: gds-impact --entry-node-id=<id> [--max-depth=4] [--sampling-size=N]
//
//	[--w-ppr=0.5] [--w-prox=0.3] [--w-bc=0.2]
//
// Env: NEO4J_URI (default bolt://localhost:7687), NEO4J_USER, NEO4J_PASSWORD.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/tools/gds-impact/internal/gdsimpact"
)

func main() {
	cfg, err := gdsimpact.ParseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(2)
	}

	ctx := context.Background()
	neo4jURI := config.EnvOrDefault("NEO4J_URI", "bolt://localhost:7687")
	neo4jUser := config.EnvOrDefault("NEO4J_USER", "neo4j")
	neo4jPassword := config.EnvOrDefault("NEO4J_PASSWORD", "eci-dev-only")
	driver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPassword, ""))
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE: creazione driver Neo4j:", err)
		os.Exit(1)
	}
	defer driver.Close(ctx)

	logger := log.New(os.Stdout, "", log.LstdFlags)

	result, err := gdsimpact.Run(ctx, driver, cfg, logger.Printf, gdsimpact.Hooks{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(1)
	}

	logger.Printf("riepilogo: %d nodi con impact_score scritto (samplingSize betweenness=%d)", len(result.Scores), result.BetweennessSamplingSize)
}
