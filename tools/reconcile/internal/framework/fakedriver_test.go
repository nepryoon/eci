package framework

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"
)

// fakeDriver è un driver database/sql minimale, sufficiente a far
// funzionare db.BeginTx/tx.Commit/tx.Rollback senza un Postgres reale —
// usato dai test di ORCHESTRAZIONE (scenari 1-4, §7: "nessun bisogno di
// Postgres... reali per verificare la sola orchestrazione"). Reconcile
// non esegue mai query dirette sulla connessione: apre solo una
// transazione prima di chiamare Republish (che nei test riceve la *sql.Tx
// ma, essendo un Target finto, non la usa per emettere query reali) —
// Exec/Query non sono quindi necessari.
type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) { return &fakeConn{}, nil }

type fakeConn struct{}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("fakeConn: Prepare non supportato (non necessario per questi test)")
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return fakeTx{}, nil }

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

var registerFakeDriverOnce sync.Once

// newFakeDB apre una *sql.DB sul fakeDriver — registrato una sola volta
// per processo di test (sql.Register panica su registrazioni duplicate
// dello stesso nome).
func newFakeDB(t *testing.T) *sql.DB {
	t.Helper()
	registerFakeDriverOnce.Do(func() {
		sql.Register("reconcile-fake", fakeDriver{})
	})
	db, err := sql.Open("reconcile-fake", "")
	if err != nil {
		t.Fatalf("sql.Open(reconcile-fake): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
