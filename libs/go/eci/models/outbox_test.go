// SPEC-003 §3 scenario 5 — ParseOutboxEvent() contro i fixture condivisi con
// i test Python (tests/fixtures/jsonschema/).
package models

import (
	"fmt"
	"testing"
)

func TestParseOutboxEvent(t *testing.T) {
	cases := []struct {
		name    string
		fixture string
		wantErr bool
	}{
		{"valid", "outbox_event_valid.json", false},
		{"missing aggregate_id", "outbox_event_missing_aggregate_id.json", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseOutboxEvent(loadFixture(t, tc.fixture))
			if tc.wantErr && err == nil {
				t.Fatalf("ParseOutboxEvent() = nil error, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ParseOutboxEvent() error = %v, want nil", err)
			}
		})
	}
}

func TestParseOutboxEventFieldValues(t *testing.T) {
	event, err := ParseOutboxEvent(loadFixture(t, "outbox_event_valid.json"))
	if err != nil {
		t.Fatalf("ParseOutboxEvent: %v", err)
	}
	if event.AggregateType != "CodeNode" {
		t.Fatalf("AggregateType = %q, want %q", event.AggregateType, "CodeNode")
	}
	if event.EventType != "UPSERT" {
		t.Fatalf("EventType = %q, want %q", event.EventType, "UPSERT")
	}
}

func TestParseOutboxEventAcceptsEveryMaterializedAggregateTombstone(t *testing.T) {
	for _, aggregateType := range []string{"CodeNode", "CodeRelation", "CodeChunk", "CodeEmbedding"} {
		t.Run(aggregateType, func(t *testing.T) {
			data := []byte(fmt.Sprintf(`{
				"id":"11111111-1111-1111-1111-111111111111",
				"aggregate_type":%q,
				"aggregate_id":"entity-id",
				"event_type":"DELETE",
				"payload":{},
				"created_at":"2025-01-01T00:00:00Z"
			}`, aggregateType))
			if _, err := ParseOutboxEvent(data); err != nil {
				t.Fatalf("ParseOutboxEvent(%s DELETE): %v", aggregateType, err)
			}
		})
	}
}
