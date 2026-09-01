// Package neo4jtarget implementa framework.Target (SPEC-037) per Neo4j
// (SPEC-038, T3.4 parte 2/4): confronta code_node.ast_hash (Postgres,
// fonte di verità) con la proprietà ast_hash del nodo corrispondente in
// Neo4j, ripubblica come evento CodeNode in outbox ogni entità mancante o
// divergente — riattivando lo stesso percorso CDC->sink-graph->Neo4j
// (T1.3) che l'avrebbe scritta correttamente la prima volta.
package neo4jtarget

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
)

// targetName identifica questo Target nella CLI reconcile (--target=neo4j,
// tools/reconcile/main.go) e in framework.Report/log.
const targetName = "neo4j"

// New costruisce il Target Neo4j (SPEC-038 §2). Il driver Neo4j è LO
// STESSO già in uso in sink-graph/retrieval-engine (T1.3/T1.4, verificato
// in implementazione: github.com/neo4j/neo4j-go-driver/v5 v5.28.4) — nessuna
// nuova libreria, nessuna nuova decisione di connessione.
//
// db non è referenziato direttamente dalle closure ritornate: SourceRows
// riceve la propria *sql.DB dal framework ad ogni chiamata (framework.go,
// stesso oggetto aperto da OpenAndRun/cfg.DSN) e Republish opera
// interamente sulla *sql.Tx già aperta dal framework per quella riga —
// mantenuto solo per rispettare l'interfaccia dichiarata da SPEC-038 §2
// (parità con gli altri due plugin, Qdrant/OpenSearch, parti 3/4-4/4, che
// probabilmente condivideranno la stessa forma di costruttore).
func New(neo4jDriver neo4j.DriverWithContext, db *sql.DB) framework.Target {
	return framework.Target{
		Name:       targetName,
		SourceRows: sourceRows,
		Check:      newCheck(neo4jDriver),
		Republish:  republish,
	}
}

// sourceRows implementa SPEC-038 §2: SELECT id, ast_hash FROM code_node —
// un SourceRow per riga, Fingerprint = []byte(ast_hash).
func sourceRows(ctx context.Context, db *sql.DB) ([]framework.SourceRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, ast_hash FROM code_node`)
	if err != nil {
		return nil, fmt.Errorf("neo4jtarget: SELECT id, ast_hash FROM code_node: %w", err)
	}
	defer rows.Close()

	var result []framework.SourceRow
	for rows.Next() {
		var id, astHash string
		if err := rows.Scan(&id, &astHash); err != nil {
			return nil, fmt.Errorf("neo4jtarget: scan code_node: %w", err)
		}
		result = append(result, framework.SourceRow{ID: id, Fingerprint: []byte(astHash)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("neo4jtarget: iterazione code_node: %w", err)
	}
	return result, nil
}

// newCheck implementa SPEC-038 §2: MATCH (n {id: $id}) RETURN n.ast_hash
// AS ast_hash — nodo assente -> matches=false; nodo presente con ast_hash
// divergente -> matches=false; nodo presente con ast_hash combaciante ->
// matches=true. Neo4j irraggiungibile (o qualunque altro errore di
// query/lettura) propaga un errore vero, mai un matches=false silenzioso
// (SPEC-038 §4).
func newCheck(driver neo4j.DriverWithContext) func(ctx context.Context, row framework.SourceRow) (bool, error) {
	return func(ctx context.Context, row framework.SourceRow) (bool, error) {
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)

		result, err := session.Run(ctx, `MATCH (n {id: $id}) RETURN n.ast_hash AS ast_hash`, map[string]any{
			"id": row.ID,
		})
		if err != nil {
			return false, fmt.Errorf("neo4jtarget: query Check id=%s: %w", row.ID, err)
		}

		// result.Collect (stesso pattern di
		// services/retrieval-engine/internal/server/server.go GetNode):
		// slice vuota + err=nil distingue "nodo assente" (business,
		// matches=false) da un vero errore di lettura/connessione
		// (propagato, mai silenzioso).
		records, err := result.Collect(ctx)
		if err != nil {
			return false, fmt.Errorf("neo4jtarget: lettura risultati Check id=%s: %w", row.ID, err)
		}
		if len(records) == 0 {
			return false, nil
		}

		astHash, _, err := neo4j.GetRecordValue[string](records[0], "ast_hash")
		if err != nil {
			return false, fmt.Errorf("neo4jtarget: campo ast_hash Check id=%s: %w", row.ID, err)
		}
		return astHash == string(row.Fingerprint), nil
	}
}

// republish implementa SPEC-038 §2: dato un SourceRow (solo id+fingerprint),
// interroga di nuovo Postgres — dentro la STESSA transazione già aperta dal
// framework (SPEC-037 §2) — per la riga COMPLETA di code_node, poi inserisce
// in outbox (aggregate_type='CodeNode') un payload nella STESSA forma
// esatta che persist_parsed_file (T1.2, services/ingestion/src/persist.rs)
// avrebbe scritto per quella riga.
func republish(ctx context.Context, tx *sql.Tx, row framework.SourceRow) error {
	var domain, nodeType, name, astHash string
	var provenance []byte
	err := tx.QueryRowContext(ctx,
		`SELECT domain, node_type, name, ast_hash, provenance FROM code_node WHERE id = $1 FOR UPDATE`,
		row.ID,
	).Scan(&domain, &nodeType, &name, &astHash, &provenance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("neo4jtarget: code_node id=%s non trovato in Postgres al momento di Republish (cancellato dopo SourceRows): %w", row.ID, err)
		}
		return fmt.Errorf("neo4jtarget: lettura code_node id=%s: %w", row.ID, err)
	}

	// Stessa identica forma di persist.rs (persist_parsed_file,
	// services/ingestion/src/persist.rs): canonical provenance is copied
	// intact so tenant_id/repo/acl_group can never be dropped by a repair;
	// ext = {"node_type": ..., "language": "go"} — "language" hardcoded a
	// "go" è il comportamento REALE di persist.rs oggi (T1.1 estrae solo Go
	// al momento di questa SPEC), non un difetto da correggere qui: SPEC-038
	// §2 chiede la forma ESATTA prodotta da persist_parsed_file per quella
	// riga, non una versione "corretta" di essa.
	payload, err := json.Marshal(map[string]any{
		"id":         row.ID,
		"domain":     domain,
		"name":       name,
		"ast_hash":   astHash,
		"provenance": json.RawMessage(provenance),
		"ext": map[string]any{
			"node_type": nodeType,
			"language":  "go",
		},
	})
	if err != nil {
		return fmt.Errorf("neo4jtarget: marshal payload CodeNode id=%s: %w", row.ID, err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('CodeNode', $1, 'UPSERT', $2)`,
		row.ID, payload,
	); err != nil {
		return fmt.Errorf("neo4jtarget: insert outbox CodeNode id=%s: %w", row.ID, err)
	}
	return nil
}
