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
		"missing operation": {{Key: "event_id", Value: []byte(eventID)}},
		"duplicate operation": {
			{Key: "event_id", Value: []byte(eventID)},
			{Key: "event_type", Value: []byte("UPSERT")},
			{Key: "event_type", Value: []byte("DELETE")},
		},
		"noncanonical event id": {
			{Key: "event_id", Value: []byte("not-an-id")},
			{Key: "event_type", Value: []byte("DELETE")},
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
