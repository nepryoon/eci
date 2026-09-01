// Package qdranttarget implementa framework.Target (SPEC-037) per Qdrant
// (SPEC-039, T3.4 parte 3/4): confronta l'esistenza e la correttezza del
// punto Qdrant derivato da ciascuna riga code_embedding (Postgres, fonte
// di verità) con lo stato reale in Qdrant, ripubblica come evento
// CodeEmbedding in outbox ogni entità mancante o divergente —
// riattivando lo stesso percorso CDC->sink-vector->Qdrant (T3.1) che
// l'avrebbe scritta correttamente la prima volta.
package qdranttarget

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/qdrant/go-client/qdrant"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
)

// targetName identifica questo Target nella CLI reconcile (--target=qdrant,
// tools/reconcile/main.go) e in framework.Report/log.
const targetName = "qdrant"

// collectionName — STESSA costante di services/sink-vector/internal/consumer
// (consumer.CollectionName, SPEC-033 §2). Non importabile direttamente:
// services/sink-vector è un modulo Go SEPARATO (proprio go.mod, module
// github.com/eci-project/eci/services/sink-vector) da tools/reconcile
// (module github.com/eci-project/eci/tools/reconcile) — nessun go.work né
// direttiva replace collega i due moduli in questo repo (verificato:
// tools/reconcile/go.mod non ha alcun replace verso services/sink-vector).
// E anche a prescindere dal confine di modulo, consumer.CollectionName vive
// sotto services/sink-vector/internal/, e le regole di visibilità Go per i
// pacchetti internal/ impedirebbero comunque l'import da qualunque codice
// fuori dall'albero radicato in services/sink-vector/ — quindi non
// riesportabile nemmeno con un replace. Replicata qui identica (stesso
// valore letterale), non reinventata.
const collectionName = "code_embeddings"

// pointIDNamespace — STESSO namespace fisso di
// services/sink-vector/internal/consumer.pointIDNamespace (SPEC-033 §2),
// per lo stesso motivo di collectionName sopra: non importabile (modulo
// Go separato + pacchetto internal/), replicato identico. Un namespace
// diverso produrrebbe point id DIVERSI da quelli già scritti da
// sink-vector per le STESSE righe code_embedding, rompendo la
// corrispondenza — verificato esplicitamente dallo scenario 5 (§3).
var pointIDNamespace = uuid.MustParse("5f3e8a1c-9b6d-4e2a-8c7f-1a2b3c4d5e6f")

// derivePointID — STESSA identica logica di
// services/sink-vector/internal/consumer.DerivePointID (SPEC-033 §2):
// UUIDv5 deterministico, namespace fisso, applicato SEMPRE (non
// condizionalmente). Non riusabile direttamente per lo stesso motivo di
// collectionName/pointIDNamespace sopra (modulo Go separato, pacchetto
// internal/) — replicata riga per riga, non reinventata con parametri
// diversi.
func derivePointID(id string) string {
	return uuid.NewSHA1(pointIDNamespace, []byte(id)).String()
}

// New costruisce il Target Qdrant (SPEC-039 §2). Il client Qdrant è LO
// STESSO già in uso in sink-vector (T3.1, SPEC-033,
// github.com/qdrant/go-client v1.19.0) — nessuna nuova libreria.
//
// db non è referenziato direttamente dalle closure ritornate — stessa
// deviazione già dichiarata da neo4jtarget.New (SPEC-038 §10 punto 2):
// SourceRows riceve la propria *sql.DB dal framework ad ogni chiamata,
// Republish opera sulla *sql.Tx già aperta dal framework. Mantenuto solo
// per parità con l'interfaccia dichiarata da SPEC-039 §2 (e con
// neo4jtarget.New).
func New(qdrantClient *qdrant.Client, db *sql.DB) framework.Target {
	return framework.Target{
		Name:       targetName,
		SourceRows: sourceRows,
		Check:      newCheck(qdrantClient),
		Republish:  republish,
	}
}

// sourceRows implementa SPEC-039 §2: SELECT ce.id, cc.entity_id FROM
// code_embedding ce JOIN code_chunk cc ON cc.id = ce.chunk_id —
// code_embedding non ha una colonna entity_id diretta (vive su
// code_chunk, SPEC-029/032). Fingerprint = []byte(entity_id).
func sourceRows(ctx context.Context, db *sql.DB) ([]framework.SourceRow, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT ce.id, cc.entity_id FROM code_embedding ce JOIN code_chunk cc ON cc.id = ce.chunk_id`)
	if err != nil {
		return nil, fmt.Errorf("qdranttarget: SELECT ce.id, cc.entity_id FROM code_embedding: %w", err)
	}
	defer rows.Close()

	var result []framework.SourceRow
	for rows.Next() {
		var id, entityID string
		if err := rows.Scan(&id, &entityID); err != nil {
			return nil, fmt.Errorf("qdranttarget: scan code_embedding/code_chunk: %w", err)
		}
		result = append(result, framework.SourceRow{ID: id, Fingerprint: []byte(entityID)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("qdranttarget: iterazione code_embedding: %w", err)
	}
	return result, nil
}

// newCheck implementa SPEC-039 §2: deriva il point ID (UUIDv5 da row.ID),
// interroga Qdrant per quel punto; punto assente -> matches=false; punto
// presente con payload.node_id divergente da row.Fingerprint ->
// matches=false; combaciante -> matches=true. Qdrant irraggiungibile (o
// qualunque altro errore di query/lettura) propaga un errore vero, mai un
// matches=false silenzioso (SPEC-039 §4).
//
// NON un confronto byte-per-byte del vettore (SPEC-039 §2): Qdrant
// normalizza a norma 1 i vettori di una collection Distance_Cosine
// (scoperto empiricamente in SPEC-033 §10), quindi il vettore letto non
// combacerebbe mai esattamente con quello scritto anche quando tutto è
// corretto. Il fingerprint qui è entity_id (propagato fino al payload
// Qdrant come node_id) — esistenza più correttezza minima.
func newCheck(client *qdrant.Client) func(ctx context.Context, row framework.SourceRow) (bool, error) {
	return func(ctx context.Context, row framework.SourceRow) (bool, error) {
		pointID := derivePointID(row.ID)
		points, err := client.Get(ctx, &qdrant.GetPoints{
			CollectionName: collectionName,
			Ids:            []*qdrant.PointId{qdrant.NewID(pointID)},
			WithPayload:    qdrant.NewWithPayload(true),
		})
		if err != nil {
			return false, fmt.Errorf("qdranttarget: Get punto id=%s (pointID=%s): %w", row.ID, pointID, err)
		}
		if len(points) == 0 {
			return false, nil
		}

		nodeID := points[0].GetPayload()["node_id"].GetStringValue()
		return nodeID == string(row.Fingerprint), nil
	}
}

// republish implementa SPEC-039 §2: dato un SourceRow (id=code_embedding.id,
// Fingerprint=entity_id), interroga di nuovo Postgres — dentro la STESSA
// transazione già aperta dal framework (SPEC-037 §2) — per la riga
// completa (code_embedding.vector/model_id/embedding_dim +
// code_chunk.entity_id/provenance), poi inserisce in outbox
// (aggregate_type='CodeEmbedding') un payload nella STESSA forma esatta
// che embedding-worker (T3.1, SPEC-030/031/032,
// services/embedding-worker/internal/consumer/consumer.go storeEmbedding)
// avrebbe scritto.
//
// code_chunk NON ha una colonna provenance propria (verificato:
// contracts/sql/migrations/0003_code_chunk.up.sql) — provenance vive solo
// su code_node (0001_init.up.sql, JSONB NOT NULL) e viene propagata a
// valle SOLO nei payload Kafka (SPEC-032 §2, mai persistita come colonna
// su code_chunk/code_embedding). "code_chunk.entity_id/provenance" (§2)
// è quindi interpretato come entity_id da code_chunk + provenance dal
// code_node referenziato da quell'entity_id (stesso JOIN che
// persist_parsed_file avrebbe usato per popolarla la prima volta).
func republish(ctx context.Context, tx *sql.Tx, row framework.SourceRow) error {
	var chunkID, modelID string
	var embeddingDim int
	var vector pq.Float32Array
	var entityID string
	var provenance []byte
	err := tx.QueryRowContext(ctx,
		`SELECT ce.chunk_id, ce.vector, ce.model_id, ce.embedding_dim, cc.entity_id, cn.provenance
		 FROM code_embedding ce
		 JOIN code_chunk cc ON cc.id = ce.chunk_id
		 JOIN code_node cn ON cn.id = cc.entity_id
		 WHERE ce.id = $1
		 FOR UPDATE OF cn, cc, ce`,
		row.ID,
	).Scan(&chunkID, &vector, &modelID, &embeddingDim, &entityID, &provenance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("qdranttarget: code_embedding id=%s non trovato in Postgres al momento di Republish (cancellato dopo SourceRows): %w", row.ID, err)
		}
		return fmt.Errorf("qdranttarget: lettura code_embedding id=%s: %w", row.ID, err)
	}

	// Stessa identica forma di storeEmbedding
	// (services/embedding-worker/internal/consumer/consumer.go): provenance
	// omessa dal payload (mai un valore fabbricato) quando assente/NULL —
	// stesso principio di SPEC-032 §2/§4, qui applicabile perché
	// code_node.provenance è NOT NULL ma potrebbe comunque essere il JSON
	// letterale "null" in casi degeneri; len(provenance) > 0 e non-"null"
	// sono entrambi trattati come presenti, coerente con json.RawMessage
	// non-nil lato embedding-worker.
	payloadFields := map[string]any{
		"id":            row.ID,
		"chunk_id":      chunkID,
		"entity_id":     entityID,
		"vector":        []float32(vector),
		"model_id":      modelID,
		"embedding_dim": embeddingDim,
	}
	if len(provenance) > 0 {
		payloadFields["provenance"] = json.RawMessage(provenance)
	}
	payload, err := json.Marshal(payloadFields)
	if err != nil {
		return fmt.Errorf("qdranttarget: marshal payload CodeEmbedding id=%s: %w", row.ID, err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('CodeEmbedding', $1, 'UPSERT', $2)`,
		row.ID, payload,
	); err != nil {
		return fmt.Errorf("qdranttarget: insert outbox CodeEmbedding id=%s: %w", row.ID, err)
	}
	return nil
}
