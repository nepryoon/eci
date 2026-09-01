// Package consumer implementa il consumer Kafka -> embedding di
// embedding-worker (SPEC-030 §2): legge da outbox.event.CodeChunk, dedup
// via watermark+processed_events (stesso meccanismo generico di sink-graph,
// SPEC-015), chiama il client di embedding per il testo del chunk e scrive
// il vettore risultante in code_embedding più una riga outbox
// (aggregate_type='CodeEmbedding').
package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/eventorder"
	"github.com/eci-project/eci/libs/go/eci/kafkatrace"
	"github.com/eci-project/eci/libs/go/eci/models"
	"github.com/eci-project/eci/libs/go/eci/outboxmeta"
	"github.com/eci-project/eci/libs/go/eci/securitylabels"
	"github.com/eci-project/eci/services/embedding-worker/internal/embedclient"
)

// ConsumerName identifica questo consumer in processed_events.consumer_name
// (SPEC-030 §2) e come GroupID del consumer group Kafka.
const ConsumerName = "embedding-worker"

// TopicCodeChunk è il topic instradato dall'EventRouter Debezium per
// aggregate_type='CodeChunk' (SPEC-029, verificato end-to-end in quella
// SPEC: outbox.event.<aggregate_type>).
const TopicCodeChunk = "outbox.event.CodeChunk"

// Deps sono le dipendenze di ProcessMessage — iniettate esplicitamente
// (nessuno stato globale), stesso principio di sink-graph (SPEC-015 §2).
type Deps struct {
	DB *sql.DB
	// Embed è un client REALE (mai un mock) verso l'endpoint /embed nativo
	// TEI — embedder-fake in sviluppo/test, vero TEI quando disponibile
	// (SPEC-030 §2).
	Embed *embedclient.Client
	// ModelID identifica il modello usato per calcolare il vettore,
	// registrato per riga in code_embedding (SPEC-030 §2/§6).
	ModelID string
	Logf    func(format string, args ...any)
}

// Outcome distingue gli esiti di ProcessMessage per il chiamante.
type Outcome int

const (
	OutcomeStored Outcome = iota
	OutcomeTombstoneAcknowledged
	OutcomeDuplicate
	OutcomeInvalidSkipped
)

// codeChunkPayload rispecchia la forma prodotta da persist.rs per CodeChunk
// (SPEC-029 §2: {id, entity_id, chunk_index, text, char_count}). `id` è il
// chunk_id (code_chunk.id) — nessuno struct condiviso in libs/go/eci/models
// per CodeChunk al momento di questa SPEC, quindi definito qui, locale a
// questo consumer (stesso principio di codeRelationPayload in sink-graph,
// SPEC-015 §10). `EntityID` (SPEC-031 §2) e `Provenance` (SPEC-032 §2): già
// presenti nel messaggio in ingresso, propagati invariati nel payload
// outbox di CodeEmbedding — nessuna nuova query, solo lettura di campi già
// deserializzati. `Provenance` è `json.RawMessage` (non uno struct dedicato
// `{Path string}`, scelta lasciata aperta da SPEC-032 §2): stesso principio
// già usato per `OutboxEvent.Payload` in libs/go/eci/models/outbox.go — un
// blob JSON opaco che questo consumer non deve interpretare, solo
// ritrasmettere byte-per-byte. Zero value (`nil`) quando il messaggio in
// ingresso non ha la chiave `provenance` (SPEC-032 §4 edge case, lato Rust):
// nessun default fabbricato, la chiave resta semplicemente omessa anche nel
// payload in uscita (vedi storeEmbedding).
type codeChunkPayload struct {
	ID         string          `json:"id"`
	EntityID   string          `json:"entity_id"`
	Text       string          `json:"text"`
	Provenance json.RawMessage `json:"provenance"`
}

type codeChunkTombstone struct {
	ID         string            `json:"id"`
	EntityID   string            `json:"entity_id"`
	Provenance models.Provenance `json:"provenance"`
}

// ProcessMessage elabora UN messaggio Kafka già fetchato (SPEC-030 §2/§3):
//  1. estrae event_id dagli header;
//  2. chiama il client di embedding per il testo del chunk — un errore qui
//     (servizio irraggiungibile) propaga SENZA toccare il database, cosicché
//     nessuno stato venga marcato "processato" per un lavoro mai
//     completato (§2/§4: "l'offset Kafka NON viene confermato" — un
//     redelivery successivo deve poter ritentare l'INTERO lavoro, dedup
//     incluso);
//  3. dedup atomico + scrittura di code_embedding + riga outbox, TUTTI
//     nella STESSA transazione Postgres (deviazione dichiarata da SPEC-030
//     §2, vedi nota a fondo SPEC: possibile perché, a differenza di
//     sink-graph/Neo4j, qui ogni scrittura ha come destinazione la STESSA
//     Postgres — un'unica transazione evita che un evento venga marcato
//     processato senza che la riga corrispondente sia mai stata scritta).
//
// Un errore ritornato (non-nil) significa "infrastruttura irraggiungibile,
// NON committare l'offset" (SPEC-030 §2/§4). Un payload malformato o senza
// id non è un errore ritornato: viene loggato esplicitamente e la funzione
// ritorna (OutcomeInvalidSkipped, nil) — il chiamante committa comunque
// l'offset, stesso principio di sink-graph (SPEC-015 §3 scenario 5).
func ProcessMessage(ctx context.Context, deps Deps, topic string, value []byte, headers []kafka.Header) (Outcome, error) {
	metadata, metadataErr := outboxmeta.Parse(headers)
	if metadataErr != nil {
		deps.Logf("embedding-worker: metadata outbox non valida, scartata")
		return OutcomeInvalidSkipped, nil
	}
	if topic != TopicCodeChunk {
		deps.Logf("embedding-worker: topic sconosciuto, scartato")
		return OutcomeInvalidSkipped, nil
	}
	eventID := metadata.EventID

	if metadata.Operation == outboxmeta.OperationDelete {
		var tombstone codeChunkTombstone
		if err := json.Unmarshal(value, &tombstone); err != nil || tombstone.ID == "" || tombstone.EntityID == "" || tombstone.Provenance.Path == "" {
			deps.Logf("embedding-worker: tombstone CodeChunk non valido, scartato")
			return OutcomeInvalidSkipped, nil
		}
		if !securitylabels.Valid(tombstone.Provenance.TenantID, tombstone.Provenance.Repo, tombstone.Provenance.ACLGroup) {
			securitylabels.Observe(ConsumerName, "rejected")
			deps.Logf("embedding-worker: tombstone CodeChunk senza security labels valide, scartato")
			return OutcomeInvalidSkipped, nil
		}
		securitylabels.Observe(ConsumerName, "accepted")
		guard, orderState, err := eventorder.Begin(ctx, deps.DB, ConsumerName, "CodeChunk", tombstone.ID, metadata)
		if err != nil {
			return OutcomeInvalidSkipped, fmt.Errorf("acquire ordered tombstone: %w", err)
		}
		if orderState == eventorder.Duplicate || orderState == eventorder.Stale {
			return OutcomeDuplicate, nil
		}
		defer guard.Abort()
		if err := guard.Complete(ctx); err != nil {
			return OutcomeInvalidSkipped, fmt.Errorf("complete ordered tombstone: %w", err)
		}
		return OutcomeTombstoneAcknowledged, nil
	}

	var chunk codeChunkPayload
	if err := json.Unmarshal(value, &chunk); err != nil {
		deps.Logf("embedding-worker: payload CodeChunk non decodificabile (event_id=%s): %v", eventID, err)
		return OutcomeInvalidSkipped, nil
	}
	if chunk.ID == "" {
		deps.Logf("embedding-worker: payload CodeChunk senza id (event_id=%s)", eventID)
		return OutcomeInvalidSkipped, nil
	}
	guard, orderState, err := eventorder.Begin(ctx, deps.DB, ConsumerName, "CodeChunk", chunk.ID, metadata)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("acquire ordered event: %w", err)
	}
	if orderState == eventorder.Duplicate || orderState == eventorder.Stale {
		deps.Logf("embedding-worker: evento duplicato o superato, effetto non applicato")
		return OutcomeDuplicate, nil
	}
	defer guard.Abort()

	vector, err := deps.Embed.Embed(ctx, chunk.Text)
	if err != nil {
		deps.Logf("embedding-worker: chiamata di embedding fallita (event_id=%s, chunk_id=%s): %v", eventID, chunk.ID, err)
		return OutcomeInvalidSkipped, fmt.Errorf("embed chunk_id=%s: %w", chunk.ID, err)
	}

	traceID, _ := kafkatrace.TraceIDFromHeaders(headers)

	if err := storeEmbedding(ctx, guard.Tx(), deps, chunk.ID, chunk.EntityID, chunk.Provenance, vector, traceID); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("store embedding chunk_id=%s: %w", chunk.ID, err)
	}
	if err := guard.Complete(ctx); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("complete ordered event: %w", err)
	}
	return OutcomeStored, nil
}

// storeEmbedding inserisce code_embedding e il relativo outbox event nella
// transazione ordinata aperta dal chiamante. Il completion marker e il
// watermark del consumer vengono committati atomicamente da Guard.Complete.
func storeEmbedding(ctx context.Context, tx *sql.Tx, deps Deps, chunkID, entityID string, provenance json.RawMessage, vector []float32, traceID string) error {
	vector64 := make(pq.Float64Array, len(vector))
	for i, v := range vector {
		vector64[i] = float64(v)
	}

	var embeddingID string
	err := tx.QueryRowContext(ctx,
		`INSERT INTO code_embedding (chunk_id, vector, model_id, embedding_dim)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id::text`,
		chunkID, vector64, deps.ModelID, len(vector),
	).Scan(&embeddingID)
	if err != nil {
		return err
	}

	payloadFields := map[string]any{
		"id":            embeddingID,
		"chunk_id":      chunkID,
		"entity_id":     entityID,
		"vector":        vector,
		"model_id":      deps.ModelID,
		"embedding_dim": len(vector),
	}
	// SPEC-032 §2/§4: se il messaggio in ingresso non aveva provenance
	// (len(provenance) == 0, campo assente), la chiave resta omessa dal
	// payload in uscita — mai un valore fabbricato.
	if len(provenance) > 0 {
		payloadFields["provenance"] = provenance
	}
	payload, err := json.Marshal(payloadFields)
	if err != nil {
		return err
	}

	var traceIDParam any
	if traceID != "" {
		traceIDParam = traceID
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id)
		 VALUES ('CodeEmbedding', $1, 'UPSERT', $2, $3)`,
		embeddingID, payload, traceIDParam,
	)
	if err != nil {
		return err
	}
	return nil
}
