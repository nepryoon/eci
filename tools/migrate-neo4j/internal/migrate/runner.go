package migrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Config — SPEC-004 §2 punto 1: NEO4J_URI (default bolt://localhost:7687),
// NEO4J_USER, NEO4J_PASSWORD da env.
type Config struct {
	URI      string
	Username string
	Password string
}

// ConfigFromEnv legge la configurazione di connessione da env, con il
// default esplicito per NEO4J_URI previsto dalla SPEC.
func ConfigFromEnv() Config {
	return Config{
		URI:      envOrDefault("NEO4J_URI", "bolt://localhost:7687"),
		Username: os.Getenv("NEO4J_USER"),
		Password: os.Getenv("NEO4J_PASSWORD"),
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Outcome è l'esito dell'esecuzione di un singolo statement DDL, distinto
// via ResultSummary.Counters() (§8: "N statement eseguiti, M già esistenti").
type Outcome int

const (
	OutcomeCreated Outcome = iota
	OutcomeAlreadyExists
)

// Summary è il riepilogo finale stampato dal runner (§8).
type Summary struct {
	Created       int
	AlreadyExists int
}

// stepFunc esegue un singolo statement e ne classifica l'esito. Isolare
// questa firma da Run() rende testabile senza Neo4j la logica di controllo
// del loop (stop-on-first-error, conteggi) in runAll.
type stepFunc func(ctx context.Context, statement string) (Outcome, error)

// runAll esegue gli statement in ordine tramite step, fermandosi al primo
// errore (SPEC-004 §4: "il runner si ferma, riporta quale statement e
// l'errore esatto, non prosegue silenziosamente con i successivi").
func runAll(ctx context.Context, statements []string, step stepFunc, logf func(format string, args ...any)) (*Summary, error) {
	summary := &Summary{}

	for i, stmt := range statements {
		outcome, err := step(ctx, stmt)
		if err != nil {
			logf("[%d/%d] ERRORE su %q: %v", i+1, len(statements), firstLine(stmt), err)
			return summary, fmt.Errorf("statement %d/%d fallito (%s): %w", i+1, len(statements), firstLine(stmt), err)
		}

		switch outcome {
		case OutcomeCreated:
			summary.Created++
			logf("[%d/%d] creato: %s", i+1, len(statements), firstLine(stmt))
		case OutcomeAlreadyExists:
			summary.AlreadyExists++
			logf("[%d/%d] già esistente: %s", i+1, len(statements), firstLine(stmt))
		}
	}

	return summary, nil
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}

// minVectorIndexVersion è la versione minima Neo4j che supporta il native
// vector index nativo usato da D3 (CREATE VECTOR INDEX ... Cypher 25).
const minVectorIndexVersion = "5.13"

// classifyStatementError arricchisce l'errore di uno statement fallito con
// un messaggio esplicito quando la causa più probabile è nota (SPEC-004 §4:
// "errore esplicito che nomina la versione minima richiesta, non un errore
// Cypher generico da decifrare" per il vector index).
func classifyStatementError(statement string, err error) error {
	if strings.Contains(statement, "VECTOR INDEX") {
		return fmt.Errorf("creazione vector index fallita — richiede Neo4j >= %s con supporto native vector index: %w", minVectorIndexVersion, err)
	}
	return err
}

// classifyConnectError distingue un'istanza irraggiungibile da credenziali
// errate (SPEC-004 §4, due edge case distinti).
func classifyConnectError(cfg Config, err error) error {
	var authErr *neo4j.InvalidAuthenticationError
	if errors.As(err, &authErr) {
		return fmt.Errorf("autenticazione Neo4j fallita per l'utente %q su %s: %w", cfg.Username, cfg.URI, err)
	}
	return fmt.Errorf("impossibile raggiungere Neo4j all'indirizzo %s (verifica che l'istanza sia avviata e raggiungibile): %w", cfg.URI, err)
}

// Run applica ogni statement in una sessione Neo4j separata (SPEC-004 §2
// punto 3) e ritorna il riepilogo finale. Si ferma al primo statement che
// fallisce.
func Run(ctx context.Context, cfg Config, statements []string, logf func(format string, args ...any)) (*Summary, error) {
	driver, err := neo4j.NewDriverWithContext(cfg.URI, neo4j.BasicAuth(cfg.Username, cfg.Password, ""))
	if err != nil {
		return nil, fmt.Errorf("creazione driver Neo4j (uri=%s): %w", cfg.URI, err)
	}
	defer driver.Close(ctx)

	if err := driver.VerifyAuthentication(ctx, nil); err != nil {
		return nil, classifyConnectError(cfg, err)
	}

	step := func(ctx context.Context, stmt string) (Outcome, error) {
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)

		result, err := session.Run(ctx, stmt, nil)
		if err != nil {
			return 0, classifyStatementError(stmt, err)
		}
		resultSummary, err := result.Consume(ctx)
		if err != nil {
			return 0, classifyStatementError(stmt, err)
		}

		counters := resultSummary.Counters()
		if counters.ConstraintsAdded() > 0 || counters.IndexesAdded() > 0 {
			return OutcomeCreated, nil
		}
		return OutcomeAlreadyExists, nil
	}

	return runAll(ctx, statements, step, logf)
}
