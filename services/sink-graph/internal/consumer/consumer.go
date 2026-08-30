// Package consumer implementa il consumer Kafka -> MERGE Neo4j di
// sink-graph (SPEC-015, T1.3): legge da outbox.event.CodeNode/CodeRelation,
// dedup via processed_events, MERGE idempotente su Neo4j rispettando i
// vincoli reali di D3 (contracts/cypher/schema.cypher).
package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/models"
	"github.com/eci-project/eci/libs/go/eci/securitylabels"
)

// ConsumerName identifica questo consumer in processed_events.consumer_name
// (SPEC-015 §2) e come GroupID del consumer group Kafka.
const ConsumerName = "sink-graph"

// Topic dei due aggregate_type instradati dall'EventRouter Debezium
// (route.by.field=aggregate_type, verificato in SPEC-008 §2/§9:
// outbox.event.<aggregate_type>).
const (
	TopicCodeNode     = "outbox.event.CodeNode"
	TopicCodeRelation = "outbox.event.CodeRelation"
)

// eventIDHeaderKey è il nome dell'header Kafka promosso dall'addendum al
// connector (SPEC-015 §2: transforms.outbox.table.fields.additional.placement
// include "id:header:event_id" — l'header `id` di default di Debezium
// esiste già senza l'addendum, ma sotto un nome diverso; questo consumer
// legge specificamente `event_id`, il nome che l'addendum introduce).
const eventIDHeaderKey = "event_id"

// Deps sono le dipendenze di ProcessMessage — iniettate esplicitamente
// (nessuno stato globale) per restare testabile con testcontainers senza
// toccare env/singleton di processo.
type Deps struct {
	DB    *sql.DB
	Neo4j neo4j.DriverWithContext
	// Logf riceve i log espliciti richiesti da §3 scenario 5/§4 (payload
	// scartato) — iniettato per essere verificabile nei test senza
	// dipendere dal package log globale.
	Logf func(format string, args ...any)
}

// Outcome distingue gli esiti di ProcessMessage per il chiamante (usato
// nei test per asserzioni precise, non solo "nessun errore").
type Outcome int

const (
	OutcomeMerged Outcome = iota
	OutcomeDuplicate
	OutcomeInvalidSkipped
)

// ProcessMessage elabora UN messaggio Kafka già fetchato (SPEC-015 §2,
// passi 1-3):
//  1. estrae event_id dagli header;
//  2. verifica processed_events senza prenotare l'evento;
//  3. se nuovo, decodifica il payload e fa MERGE idempotente su Neo4j;
//  4. solo dopo il MERGE riuscito registra processed_events.
//
// Un errore ritornato (non-nil) significa "infrastruttura irraggiungibile,
// NON committare l'offset" (SPEC-015 §4: Postgres/Neo4j persi a metà
// elaborazione -> redelivery). Un payload malformato o con
// node_type/rel_type fuori enum NON è un errore ritornato: viene loggato
// esplicitamente e la funzione ritorna (OutcomeInvalidSkipped, nil) — il
// chiamante committa comunque l'offset (SPEC-015 §3 scenario 5/§4: "non un
// crash, non un'interpolazione Cypher con un valore non validato", "offset
// comunque committato").
func ProcessMessage(ctx context.Context, deps Deps, topic string, value []byte, headers []kafka.Header) (Outcome, error) {
	eventID, ok := eventIDFromHeaders(headers)
	if !ok {
		deps.Logf("sink-graph: messaggio su %s senza header %q valido, scartato (offset committato)", topic, eventIDHeaderKey)
		return OutcomeInvalidSkipped, nil
	}

	processed, err := isProcessed(ctx, deps.DB, eventID)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("dedup event_id=%s: %w", eventID, err)
	}
	if processed {
		deps.Logf("sink-graph: event_id=%s già in processed_events, skip MERGE (redelivery)", eventID)
		return OutcomeDuplicate, nil
	}

	var outcome Outcome
	switch topic {
	case TopicCodeNode:
		outcome, err = mergeCodeNode(ctx, deps, value, eventID)
	case TopicCodeRelation:
		outcome, err = mergeCodeRelation(ctx, deps, value, eventID)
	default:
		deps.Logf("sink-graph: topic sconosciuto %q (event_id=%s), scartato", topic, eventID)
		return OutcomeInvalidSkipped, nil
	}
	if err != nil || outcome != OutcomeMerged {
		return outcome, err
	}
	isNew, err := markProcessed(ctx, deps.DB, eventID)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("complete dedup event_id=%s after MERGE: %w", eventID, err)
	}
	if !isNew {
		deps.Logf("sink-graph: event_id=%s completato concorrentemente, MERGE idempotente già applicato", eventID)
		return OutcomeDuplicate, nil
	}
	return outcome, nil
}

func isProcessed(ctx context.Context, db *sql.DB, eventID string) (bool, error) {
	var processed bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_name = $2
		)`,
		eventID, ConsumerName,
	).Scan(&processed)
	return processed, err
}

// eventIDFromHeaders estrae l'header event_id (SPEC-015 §2 punto 1). Un
// header assente o con valore vuoto è trattato come assente, non come
// stringa vuota valida.
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

// markProcessed registra il completamento solo dopo il MERGE esterno.
// isNew=false quando l'INSERT non ha inserito nulla (event_id già
// presente): sql.ErrNoRows su una query con RETURNING è esattamente
// questo, non un errore da propagare.
func markProcessed(ctx context.Context, db *sql.DB, eventID string) (isNew bool, err error) {
	var returned string
	err = db.QueryRowContext(ctx,
		`INSERT INTO processed_events (event_id, consumer_name)
		 VALUES ($1, $2)
		 ON CONFLICT (event_id, consumer_name) DO NOTHING
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

// mergeCodeNode decodifica il payload CodeNode (stessa forma prodotta da
// persist.rs, SPEC-014) e fa MERGE su Neo4j (SPEC-015 §2).
// models.CodeNode/ParseExt (libs/go/eci/models, SPEC-003) sono riusati
// per la decodifica JSON e per il dispaccio su Ext in base a Domain: la
// loro validazione di node_type copre l'intero enum D2 (più ampio),
// quindi qui si applica IN AGGIUNTA la whitelist più stretta di
// mergeCodeNodeQuery PRIMA di qualunque interpolazione Cypher — mai un
// valore non ri-validato da questo pacchetto dentro una stringa di query.
func mergeCodeNode(ctx context.Context, deps Deps, value []byte, eventID string) (Outcome, error) {
	var node models.CodeNode
	if err := json.Unmarshal(value, &node); err != nil {
		deps.Logf("sink-graph: payload CodeNode non decodificabile (event_id=%s): %v", eventID, err)
		return OutcomeInvalidSkipped, nil
	}

	extAny, err := node.ParseExt()
	if err != nil {
		deps.Logf("sink-graph: CodeNode.ParseExt fallita (event_id=%s, id=%s): %v", eventID, node.ID, err)
		return OutcomeInvalidSkipped, nil
	}
	ext, ok := extAny.(models.CodeExtension)
	if !ok {
		deps.Logf("sink-graph: CodeNode con domain non-'code' inatteso (event_id=%s, id=%s)", eventID, node.ID)
		return OutcomeInvalidSkipped, nil
	}
	if !securitylabels.Valid(node.Provenance.TenantID, node.Provenance.Repo, node.Provenance.ACLGroup) {
		securitylabels.Observe(ConsumerName, securitylabels.Outcome(node.Provenance.TenantID, node.Provenance.Repo, node.Provenance.ACLGroup))
		deps.Logf("sink-graph: CodeNode senza security labels valide (event_id=%s, id=%s), scartato", eventID, node.ID)
		return OutcomeInvalidSkipped, nil
	}
	securitylabels.Observe(ConsumerName, "accepted")

	query, err := mergeCodeNodeQuery(ext.NodeType)
	if err != nil {
		deps.Logf("sink-graph: %v (event_id=%s, id=%s)", err, eventID, node.ID)
		return OutcomeInvalidSkipped, nil
	}

	params := map[string]any{
		"id":        node.ID,
		"domain":    node.Domain,
		"name":      node.Name,
		"ast_hash":  node.AstHash,
		"tenant_id": node.Provenance.TenantID,
		"repo":      node.Provenance.Repo,
		"acl_group": node.Provenance.ACLGroup,
		"path":      node.Provenance.Path,
	}

	if err := runWrite(ctx, deps.Neo4j, lockCodeNodeQuery, query, params); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("MERGE CodeNode id=%s: %w", node.ID, err)
	}
	return OutcomeMerged, nil
}

// codeRelationPayload rispecchia il payload prodotto da persist.rs per
// CodeRelation (SPEC-014 §2) — nessuno struct condiviso in libs/go/eci/models
// per CodeRelation al momento di questa SPEC (solo CodeNode, SPEC-003),
// quindi definito qui, locale a questo consumer.
type codeRelationPayload struct {
	ID      string `json:"id"`
	Domain  string `json:"domain"`
	RelType string `json:"rel_type"`
	FromID  string `json:"from_id"`
	ToID    string `json:"to_id"`
	// Weight è sempre un conteggio intero di occorrenze (Option<u32> in
	// persist.rs, SPEC-014 §2) — *int64, non *float64: un parametro
	// Cypher float per $weight produrrebbe un risultato float64 anche
	// dove il letterale di fallback `coalesce($weight, 1)` è un intero,
	// un'inconsistenza di tipo scoperta scrivendo il test di integrazione
	// (SPEC-015 §10).
	Weight *int64 `json:"weight"`
}

// mergeCodeRelation decodifica il payload CodeRelation e fa MERGE su Neo4j
// (SPEC-015 §2). Stessa disciplina di mergeCodeNode: rel_type ri-validato
// da mergeCodeRelationQuery prima di qualunque interpolazione Cypher.
func mergeCodeRelation(ctx context.Context, deps Deps, value []byte, eventID string) (Outcome, error) {
	var rel codeRelationPayload
	if err := json.Unmarshal(value, &rel); err != nil {
		deps.Logf("sink-graph: payload CodeRelation non decodificabile (event_id=%s): %v", eventID, err)
		return OutcomeInvalidSkipped, nil
	}
	if rel.FromID == "" || rel.ToID == "" {
		deps.Logf("sink-graph: payload CodeRelation senza from_id/to_id (event_id=%s)", eventID)
		return OutcomeInvalidSkipped, nil
	}

	query, err := mergeCodeRelationQuery(rel.RelType)
	if err != nil {
		deps.Logf("sink-graph: %v (event_id=%s, from=%s, to=%s)", err, eventID, rel.FromID, rel.ToID)
		return OutcomeInvalidSkipped, nil
	}

	var weight any
	if rel.Weight != nil {
		weight = *rel.Weight
	}
	params := map[string]any{
		"from_id": rel.FromID,
		"to_id":   rel.ToID,
		"weight":  weight,
	}

	if err := runWrite(ctx, deps.Neo4j, lockCodeRelationEndpointsQuery, query, params); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("MERGE CodeRelation %s %s->%s: %w", rel.RelType, rel.FromID, rel.ToID, err)
	}
	return OutcomeMerged, nil
}

// runWrite acquisisce prima i lock CodeNode in ordine totale e poi esegue la
// mutation nella stessa explicit transaction. ExecuteWrite applica inoltre il
// retry bounded del driver agli errori transient Neo4j; entrambi i Result sono
// consumati perché gli errori reali emergono solo al Consume.
func runWrite(ctx context.Context, driver neo4j.DriverWithContext, lockQuery, mutationQuery string, params map[string]any) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		for _, query := range []string{lockQuery, mutationQuery} {
			result, err := tx.Run(ctx, query, params)
			if err != nil {
				return nil, err
			}
			if _, err := result.Consume(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}
