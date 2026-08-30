package consumer

import (
	"context"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

func TestMetadataIsValidatedBeforeEmbeddingOrDatabase(t *testing.T) {
	outcome, err := ProcessMessage(
		context.Background(),
		Deps{Logf: func(string, ...any) {}},
		TopicCodeChunk,
		[]byte(`{"id":"sensitive","text":"secret"}`),
		[]kafka.Header{{Key: "event_id", Value: []byte("11111111-1111-1111-1111-111111111111")}},
	)
	if err != nil || outcome != OutcomeInvalidSkipped {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
}

func TestDeleteHasAnExplicitOutcome(t *testing.T) {
	var _ Outcome = OutcomeTombstoneAcknowledged
}
