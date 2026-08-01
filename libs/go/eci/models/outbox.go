// SPEC-003 §2. Struct Go scritto a mano per l'envelope evento outbox
// (contracts/jsonschema/outbox-event.json).
package models

import (
	"encoding/json"
	"fmt"
)

// OutboxEvent rispecchia contracts/jsonschema/outbox-event.json.
type OutboxEvent struct {
	ID            string          `json:"id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     string          `json:"created_at"`
	TraceID       *string         `json:"trace_id,omitempty"`
}

var outboxRequiredFields = []string{
	"id", "aggregate_type", "aggregate_id", "event_type", "payload", "created_at",
}

var outboxAggregateTypes = map[string]bool{"CodeNode": true, "CodeRelation": true}

var outboxEventTypes = map[string]bool{"UPSERT": true, "DELETE": true}

// ParseOutboxEvent valida e decodifica l'envelope evento outbox.
// encoding/json non distingue un campo required assente da uno con valore
// zero, quindi la presenza delle chiavi richieste va controllata
// esplicitamente su una decodifica generica prima di popolare lo struct
// tipizzato.
func ParseOutboxEvent(data []byte) (*OutboxEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal outbox event: %w", err)
	}

	for _, field := range outboxRequiredFields {
		if _, ok := raw[field]; !ok {
			return nil, fmt.Errorf("missing required field %q", field)
		}
	}

	var event OutboxEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal outbox event: %w", err)
	}

	if !outboxAggregateTypes[event.AggregateType] {
		return nil, fmt.Errorf("invalid aggregate_type %q", event.AggregateType)
	}
	if !outboxEventTypes[event.EventType] {
		return nil, fmt.Errorf("invalid event_type %q", event.EventType)
	}

	return &event, nil
}
