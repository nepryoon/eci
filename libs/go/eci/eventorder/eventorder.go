// Package eventorder serializes projection effects by canonical aggregate
// sequence. It prevents retry-topic delivery from applying an older UPSERT
// after a later DELETE while retaining at-least-once, idempotent processing.
package eventorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/eci-project/eci/libs/go/eci/outboxmeta"
)

// State is the result of acquiring an aggregate processing guard.
type State uint8

const (
	Ready State = iota
	Duplicate
	Stale
)

// Guard holds a PostgreSQL transaction-scoped aggregate lock across the
// external idempotent effect and its completion marker.
type Guard struct {
	tx            *sql.Tx
	consumer      string
	aggregateType string
	aggregateID   string
	metadata      outboxmeta.Metadata
	closed        bool
}

// Begin acquires the consumer+aggregate lock and classifies duplicate or
// out-of-order events. A stale event is marked processed atomically before
// Stale is returned; callers must not execute its external effect.
func Begin(
	ctx context.Context,
	db *sql.DB,
	consumer, aggregateType, aggregateID string,
	metadata outboxmeta.Metadata,
) (*Guard, State, error) {
	if db == nil || consumer == "" || aggregateType == "" || aggregateID == "" ||
		metadata.EventID == "" || metadata.Sequence <= 0 {
		return nil, Ready, errors.New("event ordering input is invalid")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, Ready, fmt.Errorf("begin event ordering transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		orderingKey(consumer, aggregateType, aggregateID),
	); err != nil {
		return nil, Ready, fmt.Errorf("acquire event ordering lock: %w", err)
	}

	processed, err := isProcessed(ctx, tx, metadata.EventID, consumer)
	if err != nil {
		return nil, Ready, fmt.Errorf("check event completion: %w", err)
	}
	if processed {
		if err := tx.Commit(); err != nil {
			return nil, Ready, fmt.Errorf("commit duplicate event check: %w", err)
		}
		rollback = false
		return nil, Duplicate, nil
	}

	var watermark int64
	err = tx.QueryRowContext(ctx, `
		SELECT event_sequence
		FROM consumer_projection_watermark
		WHERE consumer_name = $1 AND aggregate_type = $2 AND aggregate_id = $3`,
		consumer, aggregateType, aggregateID,
	).Scan(&watermark)
	if err != nil && err != sql.ErrNoRows {
		return nil, Ready, fmt.Errorf("read aggregate watermark: %w", err)
	}
	if err == nil && metadata.Sequence <= watermark {
		if err := markProcessed(ctx, tx, metadata.EventID, consumer); err != nil {
			return nil, Ready, fmt.Errorf("complete stale event: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, Ready, fmt.Errorf("commit stale event: %w", err)
		}
		rollback = false
		return nil, Stale, nil
	}

	rollback = false
	return &Guard{
		tx: tx, consumer: consumer, aggregateType: aggregateType,
		aggregateID: aggregateID, metadata: metadata,
	}, Ready, nil
}

// Tx exposes the guarded transaction for effects that are themselves
// canonical PostgreSQL mutations, such as embedding materialization.
func (g *Guard) Tx() *sql.Tx {
	if g == nil || g.closed {
		return nil
	}
	return g.tx
}

// Complete advances the aggregate watermark and records event completion in
// the same transaction. External effects must finish successfully first.
func (g *Guard) Complete(ctx context.Context) error {
	if g == nil || g.closed {
		return errors.New("event ordering guard is closed")
	}
	defer func() { g.closed = true }()
	result, err := g.tx.ExecContext(ctx, `
		INSERT INTO consumer_projection_watermark
			(consumer_name, aggregate_type, aggregate_id, event_sequence, operation)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (consumer_name, aggregate_type, aggregate_id) DO UPDATE
		SET event_sequence = EXCLUDED.event_sequence,
		    operation = EXCLUDED.operation,
		    updated_at = transaction_timestamp()
		WHERE consumer_projection_watermark.event_sequence < EXCLUDED.event_sequence`,
		g.consumer, g.aggregateType, g.aggregateID, g.metadata.Sequence, string(g.metadata.Operation),
	)
	if err != nil {
		_ = g.tx.Rollback()
		return fmt.Errorf("advance aggregate watermark: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		_ = g.tx.Rollback()
		return fmt.Errorf("advance aggregate watermark: invariant violation")
	}
	if err := markProcessed(ctx, g.tx, g.metadata.EventID, g.consumer); err != nil {
		_ = g.tx.Rollback()
		return fmt.Errorf("record event completion: %w", err)
	}
	if err := g.tx.Commit(); err != nil {
		return fmt.Errorf("commit event completion: %w", err)
	}
	return nil
}

// Abort releases the aggregate lock without marking the event. It is safe to
// call after Complete and is intended for defer.
func (g *Guard) Abort() {
	if g == nil || g.closed {
		return
	}
	g.closed = true
	_ = g.tx.Rollback()
}

func isProcessed(ctx context.Context, tx *sql.Tx, eventID, consumer string) (bool, error) {
	var processed bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_name = $2
	)`, eventID, consumer).Scan(&processed)
	return processed, err
}

func markProcessed(ctx context.Context, tx *sql.Tx, eventID, consumer string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO processed_events (event_id, consumer_name)
		VALUES ($1, $2)
		ON CONFLICT (event_id, consumer_name) DO NOTHING`, eventID, consumer)
	return err
}

func orderingKey(values ...string) string {
	var builder strings.Builder
	for _, value := range values {
		builder.WriteString(strconv.Itoa(len(value)))
		builder.WriteByte(':')
		builder.WriteString(value)
	}
	return builder.String()
}
