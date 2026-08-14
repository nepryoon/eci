package gc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// lockTimeout limita quanto a lungo la transazione di purge di UNA
// tabella può attendere un lock contentato prima di fallire (SPEC-028
// §10, deviazione dichiarata: non richiesto esplicitamente da §2, ma
// necessario perché l'edge case §4 "il DELETE su outbox fallisce ma
// quello su processed_events avrebbe successo (lock/timeout)" resti
// un fallimento ESPLICITO e limitato nel tempo, non un blocco
// indefinito — coerente con l'invarianza generale del progetto contro
// attese non limitate, stesso principio del budget di deadline gRPC).
const lockTimeout = 2 * time.Second

// TableResult è l'esito della purge (o del conteggio dry-run) su UNA
// tabella (SPEC-028 §2/§3): indipendente dall'esito dell'altra tabella.
type TableResult struct {
	Table        string
	RowsAffected int64
	Err          error
}

// Summary è il riepilogo finale di un'esecuzione (SPEC-028 §8): outbox e
// processed_events sono purgate indipendentemente, ciascuna con il
// proprio esito.
type Summary struct {
	Outbox          TableResult
	ProcessedEvents TableResult
}

// Failed riporta se ALMENO UNA delle due tabelle ha fallito (SPEC-028
// §4: "l'exit code finale riflette se almeno una delle due è fallita").
func (s Summary) Failed() bool {
	return s.Outbox.Err != nil || s.ProcessedEvents.Err != nil
}

// OpenAndRun apre la connessione DSN, verifica la raggiungibilità
// (SPEC-028 §4: DSN non raggiungibile -> errore esplicito, nessun
// tentativo di purge), poi esegue RunWithDB.
func OpenAndRun(ctx context.Context, cfg Config, logf func(format string, args ...any)) (Summary, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return Summary{}, fmt.Errorf("apertura connessione Postgres fallita: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return Summary{}, fmt.Errorf("Postgres non raggiungibile: %w", err)
	}

	return RunWithDB(ctx, db, cfg, logf), nil
}

// RunWithDB esegue la purge (o il conteggio dry-run) di `outbox` e
// `processed_events` in transazioni indipendenti (SPEC-028 §2/§4): un
// fallimento sull'una non impedisce il tentativo sull'altra.
func RunWithDB(ctx context.Context, db *sql.DB, cfg Config, logf func(format string, args ...any)) Summary {
	outbox := purgeTable(ctx, db, "outbox", "created_at", cfg.OutboxRetention, cfg.DryRun)
	logResult(logf, outbox)

	processedEvents := purgeTable(ctx, db, "processed_events", "processed_at", cfg.ProcessedEventsRetention, cfg.DryRun)
	logResult(logf, processedEvents)

	return Summary{Outbox: outbox, ProcessedEvents: processedEvents}
}

func logResult(logf func(format string, args ...any), result TableResult) {
	if result.Err != nil {
		logf("%s: ERRORE: %v", result.Table, result.Err)
		return
	}
	logf("%s: %d righe", result.Table, result.RowsAffected)
}

// purgeTable elimina (o, in dry-run, conta) le righe di `table` più
// vecchie di `retention` secondo `timestampColumn` (SPEC-028 §2). Ogni
// tabella riceve la propria transazione breve, indipendente da quella
// dell'altra (SPEC-028 §4 — un fallimento sull'una non deve impedire il
// tentativo sull'altra, gestito dal chiamante invocando questa funzione
// due volte in sequenza indipendente, mai in una transazione condivisa).
func purgeTable(ctx context.Context, db *sql.DB, table, timestampColumn string, retention time.Duration, dryRun bool) TableResult {
	result := TableResult{Table: table}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Err = fmt.Errorf("apertura transazione su %s: %w", table, err)
		return result
	}
	defer func() { _ = tx.Rollback() }() // no-op se già committata

	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%s'", lockTimeout)); err != nil {
		result.Err = fmt.Errorf("impostazione lock_timeout su %s: %w", table, err)
		return result
	}

	retentionSeconds := retention.Seconds()

	if dryRun {
		query := fmt.Sprintf(
			"SELECT count(*) FROM %s WHERE %s < now() - make_interval(secs => $1)",
			table, timestampColumn,
		)
		var count int64
		if err := tx.QueryRowContext(ctx, query, retentionSeconds).Scan(&count); err != nil {
			result.Err = fmt.Errorf("conteggio dry-run su %s: %w", table, err)
			return result
		}
		result.RowsAffected = count
		return result
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s < now() - make_interval(secs => $1)",
		table, timestampColumn,
	)
	res, err := tx.ExecContext(ctx, query, retentionSeconds)
	if err != nil {
		result.Err = fmt.Errorf("DELETE su %s: %w", table, err)
		return result
	}
	affected, err := res.RowsAffected()
	if err != nil {
		result.Err = fmt.Errorf("RowsAffected su %s: %w", table, err)
		return result
	}
	if err := tx.Commit(); err != nil {
		result.Err = fmt.Errorf("commit transazione su %s: %w", table, err)
		return result
	}
	result.RowsAffected = affected
	return result
}
