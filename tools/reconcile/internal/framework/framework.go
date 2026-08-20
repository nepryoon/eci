// Package framework implementa l'ORCHESTRAZIONE condivisa di
// riconciliazione (SPEC-037, T3.4 parte 1/4): enumera i fingerprint reali
// da Postgres tramite un Target, li confronta con lo stato di una vista
// downstream, e per ogni mismatch ripubblica la riga outbox
// corrispondente — riattivando lo stesso meccanismo CDC che l'avrebbe
// scritta correttamente la prima volta. Questo package non conosce la
// semantica di nessuna vista specifica (Neo4j/Qdrant/OpenSearch, parti
// 2/4-4/4): riceve le tre funzioni di un Target e le orchestra soltanto.
package framework

import (
	"context"
	"database/sql"
	"fmt"
)

// SourceRow è UNA riga da riconciliare — id dell'entità Postgres +
// fingerprint atteso (forma dipendente dal plugin: questo framework la
// tratta come byte opachi, §2).
type SourceRow struct {
	ID          string
	Fingerprint []byte
}

// Target incapsula tutto ciò che serve per riconciliare UNA vista contro
// Postgres — le tre funzioni sono fornite dai plugin (parti 2/4-4/4),
// questo framework le orchestra soltanto (§2).
type Target struct {
	Name       string
	SourceRows func(ctx context.Context, db *sql.DB) ([]SourceRow, error)
	Check      func(ctx context.Context, row SourceRow) (matches bool, err error)
	Republish  func(ctx context.Context, tx *sql.Tx, row SourceRow) error
}

// RowError associa l'ID di una SourceRow all'errore incontrato
// processandola (§2: Report.Errored) — mai un panic, un mismatch/errore
// su UNA riga non ferma le altre.
type RowError struct {
	RowID string
	Err   error
}

// Report è il riepilogo di un'esecuzione di Reconcile (§2/§8).
type Report struct {
	Checked     int
	Matched     int
	Republished int
	Errored     []RowError
}

// Reconcile enumera le SourceRow tramite target.SourceRows, poi per
// ciascuna chiama target.Check: se combacia, incrementa Matched; se no,
// apre una transazione, chiama target.Republish, la committa e
// incrementa Republished. Un errore di Check o Republish fa finire la
// riga in Errored (Report.Errored) e NON interrompe le righe successive
// (§2/§3 scenari 3-4). Un errore di target.SourceRows stessa fa fallire
// Reconcile con un errore esplicito, senza un Report parziale (§4 edge
// case: senza l'elenco delle righe non c'è nulla da riconciliare).
func Reconcile(ctx context.Context, db *sql.DB, target Target) (Report, error) {
	rows, err := target.SourceRows(ctx, db)
	if err != nil {
		return Report{}, fmt.Errorf("SourceRows (target=%s): %w", target.Name, err)
	}

	var report Report
	for _, row := range rows {
		report.Checked++

		matches, err := target.Check(ctx, row)
		if err != nil {
			report.Errored = append(report.Errored, RowError{RowID: row.ID, Err: err})
			continue
		}
		if matches {
			report.Matched++
			continue
		}

		if err := republishOne(ctx, db, target, row); err != nil {
			report.Errored = append(report.Errored, RowError{RowID: row.ID, Err: err})
			continue
		}
		report.Republished++
	}

	return report, nil
}

// republishOne apre una transazione dedicata per UNA riga, chiama
// target.Republish e la committa — o Rollback in caso di errore, mai una
// scrittura parziale (§3 scenario 5).
func republishOne(ctx context.Context, db *sql.DB, target Target, row SourceRow) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("apertura transazione (target=%s, row=%s): %w", target.Name, row.ID, err)
	}
	defer func() { _ = tx.Rollback() }() // no-op se già committata

	if err := target.Republish(ctx, tx, row); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transazione (target=%s, row=%s): %w", target.Name, row.ID, err)
	}
	return nil
}
