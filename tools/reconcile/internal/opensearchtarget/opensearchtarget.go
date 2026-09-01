// Package opensearchtarget implementa framework.Target (SPEC-037) per
// OpenSearch (SPEC-040, T3.4 parte 4/4): confronta code_chunk.text
// (Postgres, fonte di verità) con il campo text del documento OpenSearch
// corrispondente, ripubblica come evento CodeChunk in outbox ogni entità
// mancante o divergente — riattivando lo stesso percorso
// CDC->sink-search->OpenSearch (T3.2) che l'avrebbe scritta correttamente
// la prima volta.
package opensearchtarget

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
)

// targetName identifica questo Target nella CLI reconcile
// (--target=opensearch, tools/reconcile/main.go) e in
// framework.Report/log.
const targetName = "opensearch"

// indexName — STESSA costante di
// services/sink-search/internal/consumer.IndexName (SPEC-034 §2). Non
// importabile direttamente: services/sink-search è un modulo Go SEPARATO
// (proprio go.mod, module github.com/eci-project/eci/services/sink-search)
// da tools/reconcile (module github.com/eci-project/eci/tools/reconcile)
// — nessun go.work né direttiva replace collega i due moduli in questo
// repo (verificato: tools/reconcile/go.mod non ha alcun replace verso
// services/sink-search). E anche a prescindere dal confine di modulo,
// consumer.IndexName vive sotto services/sink-search/internal/, e le
// regole di visibilità Go per i pacchetti internal/ impedirebbero
// comunque l'import da qualunque codice fuori dall'albero radicato in
// services/sink-search/ — quindi non riesportabile nemmeno con un
// replace. Replicata qui identica (stesso valore letterale), non
// reinventata.
const indexName = "code_chunks"

// New costruisce il Target OpenSearch (SPEC-040 §2). Il client
// OpenSearch è LO STESSO già in uso in sink-search (T3.2, SPEC-034,
// github.com/opensearch-project/opensearch-go/v4 v4.7.3, path /v4
// esplicito, forma opensearchapi.NewClient con metodi namespaced —
// verificato in implementazione leggendo services/sink-search/go.mod e
// main.go, non presunto) — nessuna nuova libreria.
//
// db non è referenziato direttamente dalle closure ritornate — stessa
// deviazione già dichiarata da neo4jtarget.New/qdranttarget.New
// (SPEC-038 §10 punto 2 / SPEC-039 §10 punto 4): SourceRows riceve la
// propria *sql.DB dal framework ad ogni chiamata, Republish opera sulla
// *sql.Tx già aperta dal framework. Mantenuto solo per parità con
// l'interfaccia dichiarata da SPEC-040 §2 (e con gli altri due plugin).
func New(client *opensearchapi.Client, db *sql.DB) framework.Target {
	return framework.Target{
		Name:       targetName,
		SourceRows: sourceRows,
		Check:      newCheck(client),
		Republish:  republish,
	}
}

// sourceRows implementa SPEC-040 §2: SELECT id, text FROM code_chunk —
// un SourceRow per riga, Fingerprint = []byte(text).
func sourceRows(ctx context.Context, db *sql.DB) ([]framework.SourceRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, text FROM code_chunk`)
	if err != nil {
		return nil, fmt.Errorf("opensearchtarget: SELECT id, text FROM code_chunk: %w", err)
	}
	defer rows.Close()

	var result []framework.SourceRow
	for rows.Next() {
		var id, text string
		if err := rows.Scan(&id, &text); err != nil {
			return nil, fmt.Errorf("opensearchtarget: scan code_chunk: %w", err)
		}
		result = append(result, framework.SourceRow{ID: id, Fingerprint: []byte(text)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("opensearchtarget: iterazione code_chunk: %w", err)
	}
	return result, nil
}

// documentSource rispecchia i soli campi di cui Check ha bisogno dal
// _source del documento OpenSearch (SPEC-040 §2) — non l'intero
// documento (entity_id/chunk_index/provenance non servono al confronto
// del fingerprint).
type documentSource struct {
	ChunkID       string `json:"chunk_id"`
	EventSequence *int64 `json:"event_sequence"`
	Text          string `json:"text"`
}

// newCheck implementa SPEC-040 §2: client.Document.Get(ctx,
// DocumentGetReq{Index: indexName, DocumentID: row.ID}) — DocumentID =
// row.ID DIRETTAMENTE (nessuna derivazione necessaria, a differenza di
// Qdrant/SPEC-039); documento assente (404) -> matches=false; presente
// con text divergente -> matches=false; combaciante -> matches=true.
//
// Document.Get usa http.MethodGet con un target di decodifica non-nil
// (a differenza di Document.Exists/Indices.Exists, richieste HEAD senza
// corpo): verificato nel sorgente del client
// (opensearchapi/opensearchapi.go, funzione do[T]) che QUALUNQUE status
// >= 400 — 404 incluso, l'esito NORMALE di "documento assente" — fa
// ritornare un errore non-nil da Document.Get, MA il *DocumentGetResp
// ritornato non è mai nil (punta sempre alla struct locale, anche in
// caso di errore) e resta ispezionabile via resp.Inspect().Response per
// distinguere "404 reale" da "connessione irraggiungibile" (stesso
// principio già stabilito da consumer.EnsureIndex/documentExists in
// sink-search, SPEC-034 §7: mai fidarsi del solo error). Un
// Inspect().Response nil (nessuna risposta HTTP ottenuta affatto, es.
// connection refused) o uno status diverso da 404 propaga un errore
// vero, mai un matches=false silenzioso (SPEC-040 §4).
func newCheck(client *opensearchapi.Client) func(ctx context.Context, row framework.SourceRow) (bool, error) {
	return func(ctx context.Context, row framework.SourceRow) (bool, error) {
		resp, err := client.Document.Get(ctx, opensearchapi.DocumentGetReq{
			Index:      indexName,
			DocumentID: row.ID,
		})
		if err != nil {
			httpResp := resp.Inspect().Response
			if httpResp == nil {
				return false, fmt.Errorf("opensearchtarget: Document.Get id=%s: %w", row.ID, err)
			}
			if httpResp.StatusCode == 404 {
				return false, nil
			}
			return false, fmt.Errorf("opensearchtarget: Document.Get id=%s: status inatteso %d: %w", row.ID, httpResp.StatusCode, err)
		}

		var source documentSource
		if err := json.Unmarshal(resp.Source, &source); err != nil {
			return false, fmt.Errorf("opensearchtarget: decodifica _source id=%s: %w", row.ID, err)
		}
		return source.Text == string(row.Fingerprint) &&
			source.ChunkID == row.ID &&
			source.EventSequence != nil && *source.EventSequence >= 0, nil
	}
}

// republish implementa SPEC-040 §2: dato un SourceRow (id=code_chunk.id,
// Fingerprint=text), interroga di nuovo Postgres — dentro la STESSA
// transazione già aperta dal framework (SPEC-037 §2) — per la riga
// completa di code_chunk (entity_id, chunk_index, text, char_count) +
// provenance dal code_node referenziato (stesso principio già stabilito
// da SPEC-038/039: code_chunk non ha una colonna provenance propria,
// vive solo su code_node — JOIN esplicito), poi inserisce in outbox
// (aggregate_type='CodeChunk') un payload nella STESSA forma esatta che
// persist_parsed_file (T1.2, services/ingestion/src/persist.rs) avrebbe
// scritto per quella riga.
func republish(ctx context.Context, tx *sql.Tx, row framework.SourceRow) error {
	var entityID, text string
	var chunkIndex, charCount int
	var provenance []byte
	err := tx.QueryRowContext(ctx,
		`SELECT cc.entity_id, cc.chunk_index, cc.text, cc.char_count, cn.provenance
		 FROM code_chunk cc
		 JOIN code_node cn ON cn.id = cc.entity_id
		 WHERE cc.id = $1
		 FOR UPDATE OF cn, cc`,
		row.ID,
	).Scan(&entityID, &chunkIndex, &text, &charCount, &provenance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("opensearchtarget: code_chunk id=%s non trovato in Postgres al momento di Republish (cancellato dopo SourceRows): %w", row.ID, err)
		}
		return fmt.Errorf("opensearchtarget: lettura code_chunk id=%s: %w", row.ID, err)
	}

	// Stessa identica forma di persist_parsed_file
	// (services/ingestion/src/persist.rs): {id, entity_id, chunk_index,
	// text, char_count, provenance: {path: file_path}} — code_node.provenance
	// è già esattamente quella forma ({"path": ...}, JSONB NOT NULL,
	// 0001_init.up.sql), incorporata qui invariata, non ricostruita a mano.
	payload, err := json.Marshal(map[string]any{
		"id":          row.ID,
		"entity_id":   entityID,
		"chunk_index": chunkIndex,
		"text":        text,
		"char_count":  charCount,
		"provenance":  json.RawMessage(provenance),
	})
	if err != nil {
		return fmt.Errorf("opensearchtarget: marshal payload CodeChunk id=%s: %w", row.ID, err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('CodeChunk', $1, 'UPSERT', $2)`,
		row.ID, payload,
	); err != nil {
		return fmt.Errorf("opensearchtarget: insert outbox CodeChunk id=%s: %w", row.ID, err)
	}
	return nil
}
