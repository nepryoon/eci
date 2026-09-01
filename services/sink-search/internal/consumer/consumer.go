// Package consumer implementa il consumer Kafka -> OpenSearch di
// sink-search (SPEC-034 §2, T3.2): legge da outbox.event.CodeChunk, dedup
// via watermark+processed_events (stessa base condivisa, stesso principio già
// stabilito da sink-graph/embedding-worker/sink-vector), indicizza
// ciascun chunk come documento OpenSearch per la ricerca full-text —
// SECONDO consumatore dello stesso topic già consumato da
// embedding-worker, con un consumer group id proprio e distinto (fan-out).
package consumer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/eventorder"
	"github.com/eci-project/eci/libs/go/eci/outboxmeta"
	"github.com/eci-project/eci/libs/go/eci/securitylabels"
)

// ConsumerName identifica questo consumer in processed_events.consumer_name
// (SPEC-034 §2) e come GroupID del consumer group Kafka — DISTINTO da
// "embedding-worker" (SPEC-034 §2/§3 scenario 5: entrambi i servizi
// consumano lo stesso topic outbox.event.CodeChunk, fan-out via consumer
// group diversi, non uno condiviso).
const ConsumerName = "sink-search"

// TopicCodeChunk è il topic instradato dall'EventRouter Debezium per
// aggregate_type='CodeChunk' (SPEC-029, verificato end-to-end in quella
// SPEC). Nessuna catena di prerequisiti: a differenza di CodeEmbedding
// (T3.1), questo topic esiste ed è popolato dal 2026-08-14.
const TopicCodeChunk = "outbox.event.CodeChunk"

// IndexName è il nome dichiarato dell'indice OpenSearch (SPEC-034 §2).
const IndexName = "code_chunks"

// EnsureIndex verifica se IndexName esiste già (SPEC-034 §3 scenario 4);
// se no, lo crea con un mapping minimo: `text` analizzato per full-text,
// `entity_id`/`chunk_index` memorizzati come keyword/integer (non
// necessariamente analizzati, SPEC-034 §2), `provenance` con mapping
// dinamico di default. Nessuna migrazione nel senso Postgres — OpenSearch
// non ne ha, l'idempotenza è ottenuta interamente a livello applicativo,
// stesso principio di EnsureCollection in sink-vector (SPEC-033). Un
// errore qui (OpenSearch irraggiungibile) propaga esplicitamente
// (SPEC-034 §4): il chiamante (main.go) non deve avviare il consumo in
// uno stato inconsistente.
func EnsureIndex(ctx context.Context, client *opensearchapi.Client) error {
	// Indices.Exists esegue una HEAD request (corpo SEMPRE vuoto per
	// definizione HTTP): il meccanismo generico `do()` di questo client
	// tratta qualunque status >= 400 — 404 incluso, l'esito NORMALE e
	// atteso di "l'indice non esiste ancora" — come un errore da
	// decodificare come corpo JSON di errore, che su un corpo vuoto
	// fallisce con "failed to json unmarshal body" invece di un errore
	// utile. Verificato empiricamente contro un'istanza OpenSearch reale
	// (SPEC-034 §7): l'unico modo corretto di distinguere "non esiste"
	// (StatusCode 404, resp non-nil, err comunque non-nil) da "irraggiungibile"
	// (resp nil) è ispezionare la *opensearch.Response ritornata
	// direttamente, non fidarsi del solo valore di errore.
	existsResp, err := client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{
		Indices: []string{IndexName},
	})
	switch {
	case existsResp == nil:
		return fmt.Errorf("Indices.Exists(%s): %w", IndexName, err)
	case existsResp.StatusCode == 200:
		return ensureSecurityMapping(ctx, client)
	case existsResp.StatusCode != 404:
		return fmt.Errorf("Indices.Exists(%s): status inatteso %d: %w", IndexName, existsResp.StatusCode, err)
	}
	// StatusCode == 404: l'indice non esiste ancora, prosegue alla creazione.

	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": mergeProperties(map[string]any{
				"text":        map[string]any{"type": "text"},
				"entity_id":   map[string]any{"type": "keyword"},
				"chunk_index": map[string]any{"type": "integer"},
				// OpenSearch forbids sorting on the _id metadata field. Keep the
				// canonical chunk UUID as a doc-valued keyword so PIT/search_after
				// readers have a stable, unique final cursor coordinate.
				"chunk_id": map[string]any{"type": "keyword"},
			}, securityProperties()),
		},
	}
	if _, err := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: IndexName,
		Body:  opensearchutil.NewJSONReader(mapping),
	}); err != nil {
		var createError *opensearch.StructError
		if errors.As(err, &createError) &&
			createError.Status == http.StatusBadRequest &&
			createError.Err.Type == "resource_already_exists_exception" {
			// Another identical replica won the 404 -> create race. Reconcile
			// the required security fields so incompatible mappings still fail;
			// do not suppress any other OpenSearch error.
			return ensureSecurityMapping(ctx, client)
		}
		return fmt.Errorf("Indices.Create(%s): %w", IndexName, err)
	}
	return nil
}

func ensureSecurityMapping(ctx context.Context, client *opensearchapi.Client) error {
	// Also reconcile the sortable document identity on an existing index.
	// Historical views are rebuildable, but accepting the mapping before a
	// rolling sink deployment keeps new writes and readers compatible.
	properties := securityProperties()
	properties["chunk_id"] = map[string]any{"type": "keyword"}
	securityMapping := map[string]any{"properties": properties}
	if _, err := client.Indices.Mapping.Put(ctx, opensearchapi.MappingPutReq{
		Indices: []string{IndexName}, Body: opensearchutil.NewJSONReader(securityMapping),
	}); err != nil {
		return fmt.Errorf("Mapping.Put(%s): %w", IndexName, err)
	}
	return nil
}

func securityProperties() map[string]any {
	return map[string]any{
		"tenant_id": map[string]any{"type": "keyword"},
		"repo":      map[string]any{"type": "keyword"},
		"acl_group": map[string]any{"type": "keyword"},
	}
}

func mergeProperties(a, b map[string]any) map[string]any {
	for key, value := range b {
		a[key] = value
	}
	return a
}

// Deps sono le dipendenze di ProcessMessage — iniettate esplicitamente
// (nessuno stato globale), stesso principio di sink-graph (SPEC-015 §2).
type Deps struct {
	DB         *sql.DB
	OpenSearch *opensearchapi.Client
	Logf       func(format string, args ...any)
}

// Outcome distingue gli esiti di ProcessMessage per il chiamante.
type Outcome int

const (
	OutcomeStored Outcome = iota
	OutcomeDeleted
	OutcomeDuplicate
	OutcomeInvalidSkipped
)

// codeChunkPayload rispecchia la forma prodotta da persist.rs per CodeChunk
// (SPEC-029/032: {id, entity_id, chunk_index, text, char_count,
// provenance?}). `id` è l'id del chunk (code_chunk.id) — usato come
// DocumentID OpenSearch (SPEC-034 §2), non entity_id (un'entità può
// produrre più chunk, stessa lezione di T3.1/SPEC-033). `Provenance` è
// `json.RawMessage` (stesso principio già scelto in embedding-worker/
// sink-vector per SPEC-032/033, un blob JSON opaco da ritrasmettere, non
// da interpretare).
type codeChunkPayload struct {
	ID         string          `json:"id"`
	EntityID   string          `json:"entity_id"`
	ChunkIndex int             `json:"chunk_index"`
	Text       string          `json:"text"`
	Provenance json.RawMessage `json:"provenance"`
}

type securityProvenance struct {
	TenantID string `json:"tenant_id"`
	Repo     string `json:"repo"`
	ACLGroup string `json:"acl_group"`
	Path     string `json:"path"`
}

type codeChunkTombstone struct {
	ID       string             `json:"id"`
	EntityID string             `json:"entity_id"`
	Security securityProvenance `json:"provenance"`
}

// ProcessMessage elabora UN messaggio Kafka già fetchato (SPEC-034 §2/§3):
//  1. estrae event_id dagli header;
//  2. ordering e dedup via watermark+processed_events (stesso meccanismo di sink-graph/
//     sink-vector, PRIMA della scrittura OpenSearch — OpenSearch non è
//     Postgres, non può condividere una transazione col dedup);
//  3. se nuovo, indicizza UN documento con DocumentID = msg.ID.
//
// Un errore ritornato (non-nil) significa "infrastruttura irraggiungibile,
// NON committare l'offset" (SPEC-034 §4). Un payload malformato o senza id
// non è un errore ritornato: viene loggato esplicitamente e la funzione
// ritorna (OutcomeInvalidSkipped, nil) — il chiamante committa comunque
// l'offset, stesso principio di sink-graph (SPEC-015 §3 scenario 5).
func ProcessMessage(ctx context.Context, deps Deps, topic string, value []byte, headers []kafka.Header) (Outcome, error) {
	metadata, metadataErr := outboxmeta.Parse(headers)
	if metadataErr != nil {
		deps.Logf("sink-search: metadata outbox non valida, scartata")
		return OutcomeInvalidSkipped, nil
	}

	if topic != TopicCodeChunk {
		deps.Logf("sink-search: topic sconosciuto, scartato")
		return OutcomeInvalidSkipped, nil
	}
	eventID := metadata.EventID

	if metadata.Operation == outboxmeta.OperationDelete {
		return processDelete(ctx, deps, value, metadata)
	}

	var chunk codeChunkPayload
	if err := json.Unmarshal(value, &chunk); err != nil {
		deps.Logf("sink-search: payload CodeChunk non decodificabile (event_id=%s): %v", eventID, err)
		return OutcomeInvalidSkipped, nil
	}
	if chunk.ID == "" {
		deps.Logf("sink-search: payload CodeChunk senza id (event_id=%s)", eventID)
		return OutcomeInvalidSkipped, nil
	}
	var security securityProvenance
	if len(chunk.Provenance) == 0 || json.Unmarshal(chunk.Provenance, &security) != nil ||
		!securitylabels.Valid(security.TenantID, security.Repo, security.ACLGroup) {
		securitylabels.Observe(ConsumerName, securitylabels.Outcome(security.TenantID, security.Repo, security.ACLGroup))
		deps.Logf("sink-search: payload CodeChunk senza security labels valide (event_id=%s), scartato", eventID)
		return OutcomeInvalidSkipped, nil
	}
	securitylabels.Observe(ConsumerName, "accepted")

	guard, orderState, err := eventorder.Begin(ctx, deps.DB, ConsumerName, "CodeChunk", chunk.ID, metadata)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("acquire ordered event: %w", err)
	}
	if orderState == eventorder.Duplicate || orderState == eventorder.Stale {
		deps.Logf("sink-search: evento duplicato o superato, effetto non applicato")
		return OutcomeDuplicate, nil
	}
	defer guard.Abort()

	if err := indexDocument(ctx, deps.OpenSearch, chunk); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("indicizzazione documento id=%s: %w", chunk.ID, err)
	}
	if err := guard.Complete(ctx); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("complete ordered event: %w", err)
	}
	return OutcomeStored, nil
}

func processDelete(ctx context.Context, deps Deps, value []byte, metadata outboxmeta.Metadata) (Outcome, error) {
	var tombstone codeChunkTombstone
	if err := json.Unmarshal(value, &tombstone); err != nil || tombstone.ID == "" || tombstone.EntityID == "" || tombstone.Security.Path == "" {
		deps.Logf("sink-search: tombstone CodeChunk non valido, scartato")
		return OutcomeInvalidSkipped, nil
	}
	if !securitylabels.Valid(tombstone.Security.TenantID, tombstone.Security.Repo, tombstone.Security.ACLGroup) {
		securitylabels.Observe(ConsumerName, "rejected")
		deps.Logf("sink-search: tombstone CodeChunk senza security labels valide, scartato")
		return OutcomeInvalidSkipped, nil
	}
	securitylabels.Observe(ConsumerName, "accepted")

	guard, orderState, err := eventorder.Begin(ctx, deps.DB, ConsumerName, "CodeChunk", tombstone.ID, metadata)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("acquire ordered tombstone: %w", err)
	}
	if orderState == eventorder.Duplicate || orderState == eventorder.Stale {
		deps.Logf("sink-search: tombstone duplicato o superato")
		return OutcomeDuplicate, nil
	}
	defer guard.Abort()

	if err := deleteDocument(ctx, deps.OpenSearch, tombstone); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("delete OpenSearch projection: %w", err)
	}
	if err := guard.Complete(ctx); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("complete ordered tombstone: %w", err)
	}
	return OutcomeDeleted, nil
}

// indexDocument scrive UN documento OpenSearch per il chunk (SPEC-034 §2):
// DocumentID = chunk.ID direttamente (nessuna derivazione necessaria — a
// differenza di Qdrant/T3.1, OpenSearch accetta stringhe arbitrarie come
// DocumentID, confermato con una scrittura reale nel test di integrazione,
// non presunto dalla sola documentazione). `client.Index` (endpoint
// `PUT {index}/_doc/{id}`) SOVRASCRIVE se l'id esiste già — a differenza
// di `Document.Create` (endpoint `_create`, verificato nel sorgente del
// client: fallisce con 409 su id duplicato), `Index` è la scelta corretta
// qui per la stessa idempotenza upsert-by-id già scelta per Qdrant/T3.1.
func indexDocument(ctx context.Context, client *opensearchapi.Client, chunk codeChunkPayload) error {
	body := map[string]any{
		"text":        chunk.Text,
		"entity_id":   chunk.EntityID,
		"chunk_index": chunk.ChunkIndex,
		"chunk_id":    chunk.ID,
	}
	if len(chunk.Provenance) > 0 {
		var provenance any
		if err := json.Unmarshal(chunk.Provenance, &provenance); err != nil {
			return fmt.Errorf("decodifica provenance: %w", err)
		}
		body["provenance"] = provenance
		if p, ok := provenance.(map[string]any); ok {
			body["tenant_id"] = p["tenant_id"]
			body["repo"] = p["repo"]
			body["acl_group"] = p["acl_group"]
		}
	}

	_, err := client.Index(ctx, opensearchapi.IndexReq{
		Index:      IndexName,
		DocumentID: chunk.ID,
		Body:       opensearchutil.NewJSONReader(body),
		// A following tombstone is implemented as a scope-intersected query.
		// Do not record the upsert as processed until search can observe it;
		// otherwise an immediately ordered DELETE can match zero documents
		// while the realtime GET API still exposes the stale projection.
		Params: opensearchapi.IndexParams{Refresh: "wait_for"},
	})
	return err
}

func deleteDocument(ctx context.Context, client *opensearchapi.Client, tombstone codeChunkTombstone) error {
	request, err := buildDeleteRequest(tombstone)
	if err != nil {
		return err
	}
	response, err := client.Document.DeleteByQuery(ctx, request)
	if err != nil {
		return err
	}
	return validateDeleteResponse(response)
}

// buildDeleteRequest performs authorization and deletion as one server-side
// operation. A preliminary GET followed by DELETE-by-ID would introduce a
// time-of-check/time-of-use gap and is intentionally not used.
func buildDeleteRequest(tombstone codeChunkTombstone) (opensearchapi.DocumentDeleteByQueryReq, error) {
	body, err := json.Marshal(map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []any{
					map[string]any{"ids": map[string]any{"values": []string{tombstone.ID}}},
					map[string]any{"term": map[string]any{"tenant_id": tombstone.Security.TenantID}},
					map[string]any{"term": map[string]any{"repo": tombstone.Security.Repo}},
					map[string]any{"term": map[string]any{"acl_group": tombstone.Security.ACLGroup}},
				},
			},
		},
	})
	if err != nil {
		return opensearchapi.DocumentDeleteByQueryReq{}, fmt.Errorf("build delete query: %w", err)
	}
	refresh, waitForCompletion, allowNoIndices := true, true, false
	maxDocs := 1
	return opensearchapi.DocumentDeleteByQueryReq{
		Indices: []string{IndexName},
		Body:    bytes.NewReader(body),
		Params: opensearchapi.DocumentDeleteByQueryParams{
			AllowNoIndices:    &allowNoIndices,
			MaxDocs:           &maxDocs,
			Refresh:           &refresh,
			WaitForCompletion: &waitForCompletion,
		},
	}, nil
}

func validateDeleteResponse(response *opensearchapi.DocumentDeleteByQueryResp) error {
	if response == nil {
		return fmt.Errorf("OpenSearch returned no delete result")
	}
	if response.TimedOut || response.VersionConflicts != 0 || len(response.Failures) != 0 {
		return fmt.Errorf("OpenSearch delete did not complete")
	}
	if response.Total < 0 || response.Total > 1 || response.Deleted < 0 || response.Deleted != response.Total {
		return fmt.Errorf("OpenSearch delete returned inconsistent counts")
	}
	return nil
}
