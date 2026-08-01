// Command migrate-neo4j applica il DDL Cypher idempotente di D3
// (contracts/cypher/schema.cypher) a un'istanza Neo4j — SPEC-004.
//
// Uso: migrate-neo4j <path/a/schema.cypher>
// Env: NEO4J_URI (default bolt://localhost:7687), NEO4J_USER, NEO4J_PASSWORD.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/eci-project/eci/tools/migrate-neo4j/internal/migrate"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "uso: %s <path/a/schema.cypher>\n", os.Args[0])
		os.Exit(2)
	}

	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "ERRORE:", err)
		os.Exit(1)
	}
}

func run(schemaPath string) error {
	src, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("lettura %s: %w", schemaPath, err)
	}

	statements := migrate.ParseStatements(string(src))
	if len(statements) == 0 {
		return fmt.Errorf("nessuno statement DDL trovato in %s", schemaPath)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)
	cfg := migrate.ConfigFromEnv()

	summary, err := migrate.Run(context.Background(), cfg, statements, logger.Printf)
	if err != nil {
		return err
	}

	logger.Printf(
		"riepilogo: %d statement eseguiti (%d creati, %d già esistenti), 0 errori",
		len(statements), summary.Created, summary.AlreadyExists,
	)
	return nil
}
