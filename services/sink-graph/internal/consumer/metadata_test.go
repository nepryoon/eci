package consumer

import (
	"context"
	"strings"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

func TestProcessMessageRejectsNonCanonicalMetadataBeforeDependencies(t *testing.T) {
	eventID := "11111111-1111-1111-1111-111111111111"
	cases := map[string][]kafka.Header{
		"missing operation": {{Key: "event_id", Value: []byte(eventID)}, {Key: "event_sequence", Value: []byte("42")}},
		"duplicate operation": {
			{Key: "event_id", Value: []byte(eventID)},
			{Key: "event_type", Value: []byte("UPSERT")},
			{Key: "event_type", Value: []byte("DELETE")},
			{Key: "event_sequence", Value: []byte("42")},
		},
		"noncanonical event id": {
			{Key: "event_id", Value: []byte("not-an-id")},
			{Key: "event_type", Value: []byte("DELETE")},
			{Key: "event_sequence", Value: []byte("42")},
		},
	}
	for name, headers := range cases {
		t.Run(name, func(t *testing.T) {
			var logs []string
			outcome, err := ProcessMessage(
				context.Background(),
				Deps{Logf: func(format string, args ...any) { logs = append(logs, format) }},
				TopicCodeNode,
				[]byte(`{"id":"sensitive"}`),
				headers,
			)
			if err != nil || outcome != OutcomeInvalidSkipped {
				t.Fatalf("outcome=%v err=%v", outcome, err)
			}
			for _, logLine := range logs {
				if strings.Contains(logLine, eventID) || strings.Contains(logLine, "sensitive") {
					t.Fatalf("invalid metadata log leaks input: %q", logLine)
				}
			}
		})
	}
}

func TestRelationAggregateIDUsesLogicalProjectedIdentity(t *testing.T) {
	got, err := RelationAggregateID("CALLS", "a:b", "c")
	if err != nil {
		t.Fatal(err)
	}
	if got != "5:CALLS3:a:b1:c" {
		t.Fatalf("logical relation identity=%q", got)
	}
	if _, err := RelationAggregateID("INJECTED", "a", "b"); err == nil {
		t.Fatal("unknown relation type accepted")
	}
	if _, err := RelationAggregateID("CALLS", "", "b"); err == nil {
		t.Fatal("empty endpoint accepted")
	}
}
