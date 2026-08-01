// SPEC-003 §3 scenario 5 — ParseOutboxEvent() contro i fixture condivisi con
// i test Python (tests/fixtures/jsonschema/).
package models

import "testing"

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
