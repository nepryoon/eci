// Package consumer implementa il consumer Kafka -> proiezione Neo4j di
// sink-graph (SPEC-015, T1.3): legge da outbox.event.CodeNode/CodeRelation,
// ordering/dedup via watermark+processed_events, MERGE idempotente su Neo4j rispettando i
// vincoli reali di D3 (contracts/cypher/schema.cypher).
package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/eventorder"
	"github.com/eci-project/eci/libs/go/eci/models"
	"github.com/eci-project/eci/libs/go/eci/outboxmeta"
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
	OutcomeDeleted
	OutcomeDuplicate
	OutcomeInvalidSkipped
)

// ProcessMessage elabora UN messaggio Kafka già fetchato (SPEC-015 §2,
// passi 1-3):
//  1. valida event_id e operation dagli header CDC;
//  2. serializza l'aggregate e rifiuta duplicate/retry superati dal watermark;
//  3. se nuovo, applica MERGE o DELETE idempotente su Neo4j;
//  4. solo dopo l'effetto riuscito registra watermark e processed_events.
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
	metadata, metadataErr := outboxmeta.Parse(headers)
	if metadataErr != nil {
		deps.Logf("sink-graph: metadata outbox non valida, scartata")
		return OutcomeInvalidSkipped, nil
	}
	eventID := metadata.EventID
	aggregateType, aggregateID, valid := graphAggregateCoordinates(topic, value)
	if !valid {
		deps.Logf("sink-graph: payload senza coordinate aggregate valide, scartato")
		return OutcomeInvalidSkipped, nil
	}
	guard, orderState, err := eventorder.Begin(ctx, deps.DB, ConsumerName, aggregateType, aggregateID, metadata)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("acquire ordered event: %w", err)
	}
	if orderState == eventorder.Duplicate || orderState == eventorder.Stale {
		deps.Logf("sink-graph: evento duplicato o superato, effetto non applicato")
		return OutcomeDuplicate, nil
	}
	defer guard.Abort()

	var outcome Outcome
	switch metadata.Operation {
	case outboxmeta.OperationUpsert:
		switch topic {
		case TopicCodeNode:
			outcome, err = mergeCodeNode(ctx, deps, value, eventID)
		case TopicCodeRelation:
			outcome, err = mergeCodeRelation(ctx, deps, value, eventID)
		default:
			deps.Logf("sink-graph: topic sconosciuto, scartato")
			return OutcomeInvalidSkipped, nil
		}
	case outboxmeta.OperationDelete:
		switch topic {
		case TopicCodeNode:
			outcome, err = deleteCodeNode(ctx, deps, value)
		case TopicCodeRelation:
			outcome, err = deleteCodeRelation(ctx, deps, value)
		default:
			deps.Logf("sink-graph: topic sconosciuto, scartato")
			return OutcomeInvalidSkipped, nil
		}
	}
	if err != nil || (outcome != OutcomeMerged && outcome != OutcomeDeleted) {
		return outcome, err
	}
	if err := guard.Complete(ctx); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("complete ordered event: %w", err)
	}
	return outcome, nil
}

func graphAggregateCoordinates(topic string, value []byte) (string, string, bool) {
	switch topic {
	case TopicCodeNode:
		var node struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(value, &node) != nil || node.ID == "" {
			return "", "", false
		}
		return "CodeNode", node.ID, true
	case TopicCodeRelation:
		var relation struct {
			RelType string `json:"rel_type"`
			FromID  string `json:"from_id"`
			ToID    string `json:"to_id"`
		}
		if json.Unmarshal(value, &relation) != nil {
			return "", "", false
		}
		identity, err := RelationAggregateID(relation.RelType, relation.FromID, relation.ToID)
		if err != nil {
			return "", "", false
		}
		return "CodeRelation", identity, true
	default:
		return "", "", false
	}
}

// RelationAggregateID identifies the logical edge represented in Neo4j.
// PostgreSQL row UUIDs change across delete/re-ingest, while the projection is
// a single rel_type/from/to edge. Length-prefixing avoids delimiter ambiguity.
func RelationAggregateID(relType, fromID, toID string) (string, error) {
	if !allowedRelTypes[relType] || fromID == "" || toID == "" {
		return "", errors.New("invalid relation aggregate coordinates")
	}
	return fmt.Sprintf("%d:%s%d:%s%d:%s", len(relType), relType, len(fromID), fromID, len(toID), toID), nil
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

type codeNodeTombstone struct {
	ID         string            `json:"id"`
	Provenance models.Provenance `json:"provenance"`
}

type codeRelationTombstone struct {
	ID         string            `json:"id"`
	RelType    string            `json:"rel_type"`
	FromID     string            `json:"from_id"`
	ToID       string            `json:"to_id"`
	Provenance models.Provenance `json:"provenance"`
}

func deleteCodeNode(ctx context.Context, deps Deps, value []byte) (Outcome, error) {
	var tombstone codeNodeTombstone
	if err := json.Unmarshal(value, &tombstone); err != nil || tombstone.ID == "" || tombstone.Provenance.Path == "" {
		deps.Logf("sink-graph: tombstone CodeNode non valido, scartato")
		return OutcomeInvalidSkipped, nil
	}
	if !securitylabels.Valid(tombstone.Provenance.TenantID, tombstone.Provenance.Repo, tombstone.Provenance.ACLGroup) {
		securitylabels.Observe(ConsumerName, "rejected")
		deps.Logf("sink-graph: tombstone CodeNode senza security labels valide, scartato")
		return OutcomeInvalidSkipped, nil
	}
	securitylabels.Observe(ConsumerName, "accepted")
	params := map[string]any{
		"id": tombstone.ID, "tenant_id": tombstone.Provenance.TenantID,
		"repo": tombstone.Provenance.Repo, "acl_group": tombstone.Provenance.ACLGroup,
		"path": tombstone.Provenance.Path,
	}
	if err := runDeleteCodeNode(ctx, deps.Neo4j, params); errors.Is(err, errDeleteScopeMismatch) {
		deps.Logf("sink-graph: tombstone CodeNode fuori scope, scartato")
		return OutcomeInvalidSkipped, nil
	} else if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("DELETE CodeNode projection: %w", err)
	}
	return OutcomeDeleted, nil
}

func deleteCodeRelation(ctx context.Context, deps Deps, value []byte) (Outcome, error) {
	var tombstone codeRelationTombstone
	if err := json.Unmarshal(value, &tombstone); err != nil || tombstone.ID == "" || tombstone.FromID == "" || tombstone.ToID == "" {
		deps.Logf("sink-graph: tombstone CodeRelation non valido, scartato")
		return OutcomeInvalidSkipped, nil
	}
	if !securitylabels.Valid(tombstone.Provenance.TenantID, tombstone.Provenance.Repo, tombstone.Provenance.ACLGroup) {
		securitylabels.Observe(ConsumerName, "rejected")
		deps.Logf("sink-graph: tombstone CodeRelation senza security labels valide, scartato")
		return OutcomeInvalidSkipped, nil
	}
	securitylabels.Observe(ConsumerName, "accepted")
	query, err := deleteCodeRelationQuery(tombstone.RelType)
	if err != nil {
		deps.Logf("sink-graph: tombstone CodeRelation con tipo non valido, scartato")
		return OutcomeInvalidSkipped, nil
	}
	params := map[string]any{"from_id": tombstone.FromID, "to_id": tombstone.ToID}
	if err := runWrite(ctx, deps.Neo4j, lockDeleteRelationEndpointsQuery, query, params); err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("DELETE CodeRelation projection: %w", err)
	}
	return OutcomeDeleted, nil
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

var errDeleteScopeMismatch = errors.New("delete scope mismatch")

func runDeleteCodeNode(ctx context.Context, driver neo4j.DriverWithContext, params map[string]any) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		lockResult, err := tx.Run(ctx, lockDeleteCodeNodeQuery, params)
		if err != nil {
			return nil, err
		}
		if _, err := lockResult.Consume(ctx); err != nil {
			return nil, err
		}

		scopeResult, err := tx.Run(ctx, checkDeleteCodeNodeScopeQuery, params)
		if err != nil {
			return nil, err
		}
		if !scopeResult.Next(ctx) {
			if err := scopeResult.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		scopeMatches, _, err := neo4j.GetRecordValue[bool](scopeResult.Record(), "scope_matches")
		if err != nil {
			return nil, err
		}
		if !scopeMatches {
			return nil, errDeleteScopeMismatch
		}

		deleteResult, err := tx.Run(ctx, deleteCodeNodeQuery, params)
		if err != nil {
			return nil, err
		}
		if _, err := deleteResult.Consume(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}
