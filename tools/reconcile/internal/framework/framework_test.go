// Package framework — test di ORCHESTRAZIONE (SPEC-037 §3 scenari 1-4, §4
// edge case) con un Target FINTO (funzioni Go semplici): nessun bisogno
// di Postgres/Neo4j/Qdrant/OpenSearch reali, coerente con §7 ("verificare
// la sola orchestrazione"). L'unica dipendenza da *sql.DB/*sql.Tx è
// soddisfatta dal fakeDriver (fakedriver_test.go): Reconcile apre una
// transazione reale (nel senso di database/sql) ma il Target finto non
// vi esegue mai query, quindi nessun driver Postgres è necessario qui.
package framework

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

func sourceRows(n int) []SourceRow {
	out := make([]SourceRow, n)
	for i := range out {
		out[i] = SourceRow{ID: fmt.Sprintf("row-%d", i), Fingerprint: []byte("fp")}
	}
	return out
}

// Scenario 1: tutte le righe combaciano -> Matched = totale, Republished = 0.
func TestReconcileAllMatchedNoneRepublished(t *testing.T) {
	db := newFakeDB(t)
	src := sourceRows(5)

	target := Target{
		Name: "fake",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return src, nil
		},
		Check: func(ctx context.Context, row SourceRow) (bool, error) {
			return true, nil
		},
		Republish: func(ctx context.Context, tx *sql.Tx, row SourceRow) error {
			t.Fatalf("Republish non deve essere chiamata quando Check=true (riga %s)", row.ID)
			return nil
		},
	}

	report, err := Reconcile(context.Background(), db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Checked != 5 {
		t.Errorf("Checked = %d, want 5", report.Checked)
	}
	if report.Matched != 5 {
		t.Errorf("Matched = %d, want 5", report.Matched)
	}
	if report.Republished != 0 {
		t.Errorf("Republished = %d, want 0", report.Republished)
	}
	if len(report.Errored) != 0 {
		t.Errorf("Errored = %v, want vuoto", report.Errored)
	}
}

// Scenario 2: solo le righe che non combaciano vengono ripubblicate,
// Republished ne riflette il conteggio esatto.
func TestReconcileMismatchedRowsAreRepublishedExactly(t *testing.T) {
	db := newFakeDB(t)
	src := sourceRows(4)
	mismatched := map[string]bool{"row-1": true, "row-3": true}
	var republishedIDs []string

	target := Target{
		Name: "fake",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return src, nil
		},
		Check: func(ctx context.Context, row SourceRow) (bool, error) {
			return !mismatched[row.ID], nil
		},
		Republish: func(ctx context.Context, tx *sql.Tx, row SourceRow) error {
			republishedIDs = append(republishedIDs, row.ID)
			return nil
		},
	}

	report, err := Reconcile(context.Background(), db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Checked != 4 {
		t.Errorf("Checked = %d, want 4", report.Checked)
	}
	if report.Matched != 2 {
		t.Errorf("Matched = %d, want 2", report.Matched)
	}
	if report.Republished != 2 {
		t.Errorf("Republished = %d, want 2", report.Republished)
	}
	if len(republishedIDs) != 2 {
		t.Fatalf("Republish chiamata %d volte, want 2 (%v)", len(republishedIDs), republishedIDs)
	}
	for _, id := range republishedIDs {
		if !mismatched[id] {
			t.Errorf("Republish chiamata per %q, che NON è una riga in mismatch", id)
		}
	}
}

// Scenario 3: una riga il cui Check ritorna un errore finisce in Errored,
// le righe successive vengono comunque processate (nessun panic/stop).
func TestReconcileCheckErrorGoesToErroredWithoutStoppingOthers(t *testing.T) {
	db := newFakeDB(t)
	src := sourceRows(3)
	var checkedIDs []string
	wantErr := errors.New("vista temporaneamente irraggiungibile (simulato)")

	target := Target{
		Name: "fake",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return src, nil
		},
		Check: func(ctx context.Context, row SourceRow) (bool, error) {
			checkedIDs = append(checkedIDs, row.ID)
			if row.ID == "row-1" {
				return false, wantErr
			}
			return true, nil
		},
		Republish: func(ctx context.Context, tx *sql.Tx, row SourceRow) error {
			t.Fatalf("Republish non deve essere chiamata per una riga il cui Check ha fallito (riga %s)", row.ID)
			return nil
		},
	}

	report, err := Reconcile(context.Background(), db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(checkedIDs) != 3 {
		t.Fatalf("Check chiamata per %v, want tutte e 3 le righe (nessuna deve fermare le altre)", checkedIDs)
	}
	if len(report.Errored) != 1 {
		t.Fatalf("Errored = %v, want esattamente 1 elemento", report.Errored)
	}
	if report.Errored[0].RowID != "row-1" {
		t.Errorf("Errored[0].RowID = %q, want %q", report.Errored[0].RowID, "row-1")
	}
	if !errors.Is(report.Errored[0].Err, wantErr) {
		t.Errorf("Errored[0].Err = %v, want %v", report.Errored[0].Err, wantErr)
	}
	if report.Matched != 2 {
		t.Errorf("Matched = %d, want 2 (le altre due righe)", report.Matched)
	}
}

// Scenario 4: una riga il cui Republish fallisce -> stesso comportamento
// dello scenario 3 (Errored, le altre righe continuano).
func TestReconcileRepublishErrorGoesToErroredWithoutStoppingOthers(t *testing.T) {
	db := newFakeDB(t)
	src := sourceRows(3)
	var republishAttempts []string
	wantErr := errors.New("scrittura outbox fallita (simulato)")

	target := Target{
		Name: "fake",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return src, nil
		},
		Check: func(ctx context.Context, row SourceRow) (bool, error) {
			return false, nil // tutte in mismatch, forza Republish per tutte
		},
		Republish: func(ctx context.Context, tx *sql.Tx, row SourceRow) error {
			republishAttempts = append(republishAttempts, row.ID)
			if row.ID == "row-1" {
				return wantErr
			}
			return nil
		},
	}

	report, err := Reconcile(context.Background(), db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(republishAttempts) != 3 {
		t.Fatalf("Republish chiamata per %v, want tutte e 3 le righe (nessuna deve fermare le altre)", republishAttempts)
	}
	if len(report.Errored) != 1 {
		t.Fatalf("Errored = %v, want esattamente 1 elemento", report.Errored)
	}
	if report.Errored[0].RowID != "row-1" {
		t.Errorf("Errored[0].RowID = %q, want %q", report.Errored[0].RowID, "row-1")
	}
	if !errors.Is(report.Errored[0].Err, wantErr) {
		t.Errorf("Errored[0].Err = %v, want %v", report.Errored[0].Err, wantErr)
	}
	if report.Republished != 2 {
		t.Errorf("Republished = %d, want 2 (le due che non falliscono)", report.Republished)
	}
}

// §4 edge case: SourceRows stessa fallisce -> Reconcile ritorna un errore
// esplicito, non un Report parziale.
func TestReconcileSourceRowsErrorReturnsExplicitErrorNotPartialReport(t *testing.T) {
	db := newFakeDB(t)
	wantErr := errors.New("postgres irraggiungibile (simulato)")

	target := Target{
		Name: "fake",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return nil, wantErr
		},
		Check: func(ctx context.Context, row SourceRow) (bool, error) {
			t.Fatal("Check non deve essere chiamata quando SourceRows fallisce")
			return false, nil
		},
		Republish: func(ctx context.Context, tx *sql.Tx, row SourceRow) error {
			t.Fatal("Republish non deve essere chiamata quando SourceRows fallisce")
			return nil
		},
	}

	report, err := Reconcile(context.Background(), db, target)
	if err == nil {
		t.Fatal("Reconcile con SourceRows in errore: atteso un errore, ottenuto nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want wrapping di %v", err, wantErr)
	}
	if report.Checked != 0 || report.Matched != 0 || report.Republished != 0 || len(report.Errored) != 0 {
		t.Errorf("report = %+v, want valore zero quando SourceRows fallisce", report)
	}
}

// §4 edge case: zero righe da SourceRows -> Report con tutti i contatori
// a zero, nessun errore (comportamento normale, non un caso speciale).
func TestReconcileZeroRowsReturnsZeroedReportNoError(t *testing.T) {
	db := newFakeDB(t)

	target := Target{
		Name: "fake",
		SourceRows: func(ctx context.Context, db *sql.DB) ([]SourceRow, error) {
			return nil, nil
		},
		Check: func(ctx context.Context, row SourceRow) (bool, error) {
			t.Fatal("Check non deve essere chiamata su zero righe")
			return false, nil
		},
		Republish: func(ctx context.Context, tx *sql.Tx, row SourceRow) error {
			t.Fatal("Republish non deve essere chiamata su zero righe")
			return nil
		},
	}

	report, err := Reconcile(context.Background(), db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Checked != 0 || report.Matched != 0 || report.Republished != 0 || len(report.Errored) != 0 {
		t.Errorf("report = %+v, want tutto zero", report)
	}
}
