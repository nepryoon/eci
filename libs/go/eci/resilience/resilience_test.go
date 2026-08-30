package resilience_test

import (
	"testing"

	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/resilience"
)

// Unit test puri per RetryCount (nessun bisogno di Kafka reale qui —
// stesso principio già adottato per le altre librerie condivise di
// libs/go/eci, es. config/kafkatrace) — il comportamento temporale/di
// instradamento di WithRetryAndDLQ stesso resta coperto solo dal test di
// integrazione (SPEC-035 §7: "non verificabile con un mock").

func TestRetryCountAbsentHeaderIsZero(t *testing.T) {
	if got := resilience.RetryCount(nil); got != 0 {
		t.Fatalf("RetryCount(nil) = %d, want 0", got)
	}
	if got := resilience.RetryCount([]kafka.Header{{Key: "other", Value: []byte("1")}}); got != 0 {
		t.Fatalf("RetryCount senza x-eci-retry-count = %d, want 0", got)
	}
}

func TestRetryCountValidValue(t *testing.T) {
	headers := []kafka.Header{{Key: resilience.RetryCountHeaderKey, Value: []byte("3")}}
	if got := resilience.RetryCount(headers); got != 3 {
		t.Fatalf("RetryCount = %d, want 3", got)
	}
}

// SPEC-035 §4 edge case: valore non numerico -> trattato come 0.
func TestRetryCountNonNumericValueIsZero(t *testing.T) {
	headers := []kafka.Header{{Key: resilience.RetryCountHeaderKey, Value: []byte("not-a-number")}}
	if got := resilience.RetryCount(headers); got != 0 {
		t.Fatalf("RetryCount con valore non numerico = %d, want 0", got)
	}
}

func TestRetryCountNegativeValueIsZero(t *testing.T) {
	headers := []kafka.Header{{Key: resilience.RetryCountHeaderKey, Value: []byte("-1")}}
	if got := resilience.RetryCount(headers); got != 0 {
		t.Fatalf("RetryCount con valore negativo = %d, want 0", got)
	}
}

func TestRetryTopicIsConsumerScopedAndOriginalTopicIsStable(t *testing.T) {
	const suffix = ".retry.embedding-worker"
	if got := resilience.RetryTopic("outbox.event.CodeChunk", suffix); got != "outbox.event.CodeChunk.retry.embedding-worker" {
		t.Fatalf("RetryTopic = %q", got)
	}
	if got := resilience.OriginalTopic("outbox.event.CodeChunk.retry.embedding-worker", suffix); got != "outbox.event.CodeChunk" {
		t.Fatalf("OriginalTopic(retry) = %q", got)
	}
	if got := resilience.OriginalTopic("outbox.event.CodeChunk", suffix); got != "outbox.event.CodeChunk" {
		t.Fatalf("OriginalTopic(primary) = %q", got)
	}
}
