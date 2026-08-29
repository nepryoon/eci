// Package consumer implementa il consumer Kafka -> Qdrant di sink-vector
// (SPEC-033 §2, T3.1): legge da outbox.event.CodeEmbedding, dedup via
// processed_events (stessa tabella condivisa, stesso principio già
// stabilito da sink-graph/SPEC-015 ed embedding-worker/SPEC-030), scrive
// ciascun embedding come punto Qdrant con payload
// {node_id, domain, provenance}.
package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/securitylabels"
)

// ConsumerName identifica questo consumer in processed_events.consumer_name
// (SPEC-033 §2) e come GroupID del consumer group Kafka.
const ConsumerName = "sink-vector"

// TopicCodeEmbedding è il topic instradato dall'EventRouter Debezium per
// aggregate_type='CodeEmbedding' (SPEC-030, stesso meccanismo generico già
// verificato end-to-end per CodeChunk in SPEC-029).
const TopicCodeEmbedding = "outbox.event.CodeEmbedding"

// eventIDHeaderKey — stesso header promosso dal connector Debezium già
// consumato da sink-graph/embedding-worker.
const eventIDHeaderKey = "event_id"

// CollectionName è il nome dichiarato della collection Qdrant (SPEC-033 §2).
const CollectionName = "code_embeddings"

// VectorSize combacia con jina-code-embeddings-1.5b (verificato in T2.4,
// SPEC-023) — stessa dimensione già assunta ovunque in questo progetto
// (embedder-fake, SPEC-023 §2).
const VectorSize = 1536

// pointIDNamespace è il namespace fisso per la derivazione UUIDv5 dei point
// id Qdrant (SPEC-033 §2). UUIDv4 generato una tantum per questo progetto —
// nessun significato semantico oltre a rendere la derivazione deterministica
// e stabile nel tempo (stesso id in ingresso -> stesso point id, sempre).
var pointIDNamespace = uuid.MustParse("5f3e8a1c-9b6d-4e2a-8c7f-1a2b3c4d5e6f")

// DerivePointID deriva un id Qdrant valido (UUID) dall'id opaco `id`
// (SPEC-033 §2): verificato empiricamente contro un'istanza Qdrant reale
// che il client ufficiale (`qdrant.NewID`/`NewIDUUID`) esiste ma richiede
// un UUID RFC4122 valido — un id SHA-256 esadecimale grezzo (formato di
// `code_node.id`/`entity_id` in questo progetto, SPEC-013) viene rifiutato
// dal SERVER Qdrant stesso con "Unable to parse UUID", non solo dal client.
// UUIDv5 (deterministico, namespace fisso) applicato qui SEMPRE, non solo
// come fallback condizionale: stesso codice sia che `id` sia già un UUID
// (code_embedding.id, generato da Postgres) sia che non lo sia, nessuna
// logica di branching su "è già un UUID?" da mantenere.
func DerivePointID(id string) string {
	return uuid.NewSHA1(pointIDNamespace, []byte(id)).String()
}

// EnsureCollection verifica se CollectionName esiste già (SPEC-033 §3
// scenari 3/4); se no, la crea con Size=VectorSize/Distance=Cosine. Nessuna
// migrazione nel senso Postgres — Qdrant non ne ha, l'idempotenza è
// ottenuta interamente a livello applicativo. Un errore qui (Qdrant
// irraggiungibile) propaga esplicitamente (SPEC-033 §4): il chiamante
// (main.go) non deve avviare il consumo in uno stato inconsistente.
func EnsureCollection(ctx context.Context, client *qdrant.Client) error {
	exists, err := client.CollectionExists(ctx, CollectionName)
	if err != nil {
		return fmt.Errorf("CollectionExists(%s): %w", CollectionName, err)
	}
	if !exists {
		m, payloadM := uint64(0), uint64(16)
		if err := client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: CollectionName,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     VectorSize,
				Distance: qdrant.Distance_Cosine,
			}),
			HnswConfig: &qdrant.HnswConfigDiff{M: &m, PayloadM: &payloadM},
		}); err != nil {
			return fmt.Errorf("CreateCollection(%s): %w", CollectionName, err)
		}
	}
	m, payloadM := uint64(0), uint64(16)
	if err := client.UpdateCollection(ctx, &qdrant.UpdateCollection{
		CollectionName: CollectionName,
		HnswConfig:     &qdrant.HnswConfigDiff{M: &m, PayloadM: &payloadM},
	}); err != nil {
		return fmt.Errorf("UpdateCollection(%s) HNSW tenant config: %w", CollectionName, err)
	}

	info, err := client.GetCollectionInfo(ctx, CollectionName)
	if err != nil {
		return fmt.Errorf("GetCollectionInfo(%s): %w", CollectionName, err)
	}
	wait := true
	for _, field := range []string{"tenant_id", "repo", "acl_group"} {
		if existing, ok := info.GetPayloadSchema()[field]; ok {
			if existing.GetDataType() != qdrant.PayloadSchemaType_Keyword {
				return fmt.Errorf("payload index %s.%s non-keyword", CollectionName, field)
			}
			if field == "tenant_id" && !existing.GetParams().GetKeywordIndexParams().GetIsTenant() {
				return fmt.Errorf("payload index %s.tenant_id esistente senza is_tenant=true: migrazione esplicita richiesta", CollectionName)
			}
			continue
		}
		req := &qdrant.CreateFieldIndexCollection{
			CollectionName: CollectionName,
			FieldName:      field,
			FieldType:      qdrant.FieldType_FieldTypeKeyword.Enum(),
			Wait:           &wait,
		}
		if field == "tenant_id" {
			isTenant := true
			req.FieldIndexParams = qdrant.NewPayloadIndexParams(&qdrant.KeywordIndexParams{IsTenant: &isTenant})
		}
		if _, err := client.CreateFieldIndex(ctx, req); err != nil {
			return fmt.Errorf("CreateFieldIndex(%s.%s): %w", CollectionName, field, err)
		}
	}
	return nil
}

// Deps sono le dipendenze di ProcessMessage — iniettate esplicitamente
// (nessuno stato globale), stesso principio di sink-graph (SPEC-015 §2).
type Deps struct {
	DB     *sql.DB
	Qdrant *qdrant.Client
	Logf   func(format string, args ...any)
}

// Outcome distingue gli esiti di ProcessMessage per il chiamante.
type Outcome int

const (
	OutcomeStored Outcome = iota
	OutcomeDuplicate
	OutcomeInvalidSkipped
)

// codeEmbeddingPayload rispecchia la forma prodotta da consumer.go di
// embedding-worker (SPEC-030/031/032:
// {id, chunk_id, entity_id, vector, model_id, embedding_dim, provenance?}).
// `Provenance` è `json.RawMessage` (stesso principio già scelto in
// embedding-worker per SPEC-032, un blob JSON opaco da ritrasmettere, non
// da interpretare) — zero value (`nil`) quando il messaggio in ingresso non
// ha la chiave `provenance` (SPEC-033 §4 edge case).
type codeEmbeddingPayload struct {
	ID         string          `json:"id"`
	EntityID   string          `json:"entity_id"`
	Vector     []float32       `json:"vector"`
	ModelID    string          `json:"model_id"`
	Provenance json.RawMessage `json:"provenance"`
}

type securityProvenance struct {
	TenantID string `json:"tenant_id"`
	Repo     string `json:"repo"`
	ACLGroup string `json:"acl_group"`
}

// ProcessMessage elabora UN messaggio Kafka già fetchato (SPEC-033 §2/§3):
//  1. estrae event_id dagli header;
//  2. dedup via processed_events (stesso meccanismo di sink-graph, PRIMA
//     della scrittura Qdrant — Qdrant non è Postgres, non può condividere
//     una transazione con il dedup, a differenza di embedding-worker/
//     SPEC-030 dove entrambe le scritture erano nella stessa Postgres);
//  3. se nuovo, upsert di UN punto Qdrant con id derivato (DerivePointID)
//     dall'id dell'embedding e payload {node_id, domain, provenance?}.
//
// Un errore ritornato (non-nil) significa "infrastruttura irraggiungibile,
// NON committare l'offset" (SPEC-033 §4). Un payload malformato o senza id
// non è un errore ritornato: viene loggato esplicitamente e la funzione
// ritorna (OutcomeInvalidSkipped, nil) — il chiamante committa comunque
// l'offset, stesso principio di sink-graph (SPEC-015 §3 scenario 5).
func ProcessMessage(ctx context.Context, deps Deps, topic string, value []byte, headers []kafka.Header) (Outcome, error) {
	eventID, ok := eventIDFromHeaders(headers)
	if !ok {
		deps.Logf("sink-vector: messaggio su %s senza header %q valido, scartato (offset committato)", topic, eventIDHeaderKey)
		return OutcomeInvalidSkipped, nil
	}

	if topic != TopicCodeEmbedding {
		deps.Logf("sink-vector: topic sconosciuto %q (event_id=%s), scartato", topic, eventID)
		return OutcomeInvalidSkipped, nil
	}

	var msg codeEmbeddingPayload
	if err := json.Unmarshal(value, &msg); err != nil {
		deps.Logf("sink-vector: payload CodeEmbedding non decodificabile (event_id=%s): %v", eventID, err)
		return OutcomeInvalidSkipped, nil
	}
	if msg.ID == "" {
		deps.Logf("sink-vector: payload CodeEmbedding senza id (event_id=%s)", eventID)
		return OutcomeInvalidSkipped, nil
	}
	var security securityProvenance
	if len(msg.Provenance) == 0 || json.Unmarshal(msg.Provenance, &security) != nil ||
		!securitylabels.Valid(security.TenantID, security.Repo, security.ACLGroup) {
		securitylabels.Observe(ConsumerName, securitylabels.Outcome(security.TenantID, security.Repo, security.ACLGroup))
		deps.Logf("sink-vector: payload CodeEmbedding senza security labels valide (event_id=%s), scartato", eventID)
		return OutcomeInvalidSkipped, nil
	}
	securitylabels.Observe(ConsumerName, "accepted")

	isNew, err := markProcessed(ctx, deps.DB, eventID)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("dedup event_id=%s: %w", eventID, err)
	}
	if !isNew {
		deps.Logf("sink-vector: event_id=%s già in processed_events, skip upsert (redelivery)", eventID)
		return OutcomeDuplicate, nil
	}

	if err := upsertPoint(ctx, deps.Qdrant, msg); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("upsert punto Qdrant id=%s: %w", msg.ID, err)
	}
	return OutcomeStored, nil
}

// eventIDFromHeaders — stessa logica di sink-graph (SPEC-015 §2 punto 1).
func eventIDFromHeaders(headers []kafka.Header) (string, bool) {
	for _, h := range headers {
		if h.Key != eventIDHeaderKey {
			continue
		}
		if len(h.Value) == 0 {
			return "", false
		}
		return string(h.Value), true
	}
	return "", false
}

// markProcessed — stesso identico dedup atomico di sink-graph (SPEC-015 §2
// punto 2): INSERT...ON CONFLICT DO NOTHING RETURNING.
func markProcessed(ctx context.Context, db *sql.DB, eventID string) (isNew bool, err error) {
	var returned string
	err = db.QueryRowContext(ctx,
		`INSERT INTO processed_events (event_id, consumer_name)
		 VALUES ($1, $2)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING event_id`,
		eventID, ConsumerName,
	).Scan(&returned)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// upsertPoint scrive UN punto Qdrant per il messaggio (SPEC-033 §2): id
// derivato da msg.ID (DerivePointID — msg.ID è code_embedding.id, l'identità
// specifica di QUESTA embedding/chunk, non msg.EntityID/node_id, che
// identifica l'entità sorgente e può essere condiviso da PIÙ chunk/
// embedding della stessa entità — usarlo come point id causerebbe
// sovrascritture silenziose tra i punti di chunk diversi della stessa
// entità, deviazione dichiarata rispetto alla lettura letterale di §2, vedi
// nota a fondo SPEC), payload {node_id: msg.EntityID, domain: "code",
// provenance?: msg.Provenance}.
func upsertPoint(ctx context.Context, client *qdrant.Client, msg codeEmbeddingPayload) error {
	payloadFields := map[string]any{
		"node_id": msg.EntityID,
		"domain":  "code",
	}
	if len(msg.Provenance) > 0 {
		var provenance any
		if err := json.Unmarshal(msg.Provenance, &provenance); err != nil {
			return fmt.Errorf("decodifica provenance: %w", err)
		}
		payloadFields["provenance"] = provenance
		if p, ok := provenance.(map[string]any); ok {
			payloadFields["tenant_id"] = p["tenant_id"]
			payloadFields["repo"] = p["repo"]
			payloadFields["acl_group"] = p["acl_group"]
		}
	}
	qdrantPayload, err := qdrant.TryValueMap(payloadFields)
	if err != nil {
		return fmt.Errorf("costruzione payload Qdrant: %w", err)
	}

	_, err = client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: CollectionName,
		Points: []*qdrant.PointStruct{
			{
				Id:      qdrant.NewID(DerivePointID(msg.ID)),
				Vectors: qdrant.NewVectors(msg.Vector...),
				Payload: qdrantPayload,
			},
		},
	})
	return err
}
