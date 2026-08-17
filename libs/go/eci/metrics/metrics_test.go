package metrics

// Test unitari puri (SPEC-036 §7: "nessun bisogno di Kafka/HTTP reali per
// la logica di incremento contatore/gauge — prometheus/client_golang
// espone un registry testabile in-process"). Scritto come test INTERNO
// (package metrics, non metrics_test) per poter leggere i valori delle
// metriche di package tramite testutil.ToFloat64 — l'API pubblica
// dichiarata da §2 (WithPrometheus/Handler) non espone altrimenti un modo
// di ispezionare i contatori. Ogni scenario usa un sinkName UNICO per
// evitare interferenze tra test che condividono lo stesso registry globale
// di default (stesso registry servito da Handler() in produzione, §2).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/resilience"
)

// SPEC-036 §3 scenario 1.
func TestWithPrometheusIncrementsProcessedCounter(t *testing.T) {
	sink := "test-sink-scenario1-processed"
	inner := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return resilience.OutcomeProcessed, nil
	}
	wrapped := WithPrometheus(sink, inner)

	before := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "processed"))
	if _, err := wrapped(context.Background(), "some-topic", []byte("v"), nil); err != nil {
		t.Fatalf("wrapped: %v", err)
	}
	after := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "processed"))
	if after != before+1 {
		t.Fatalf("eci_messages_processed_total{sink=%q,outcome=\"processed\"} = %v, want %v", sink, after, before+1)
	}
}

// SPEC-036 §3 scenario 2.
func TestWithPrometheusIncrementsRetriedCounter(t *testing.T) {
	sink := "test-sink-scenario2-retried"
	inner := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return resilience.OutcomeRetried, nil
	}
	wrapped := WithPrometheus(sink, inner)

	before := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "retried"))
	if _, err := wrapped(context.Background(), "some-topic", []byte("v"), nil); err != nil {
		t.Fatalf("wrapped: %v", err)
	}
	after := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "retried"))
	if after != before+1 {
		t.Fatalf("eci_messages_processed_total{sink=%q,outcome=\"retried\"} = %v, want %v", sink, after, before+1)
	}
}

// SPEC-036 §3 scenario 3.
func TestWithPrometheusIncrementsDeadLetteredCounter(t *testing.T) {
	sink := "test-sink-scenario3-dead-lettered"
	inner := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return resilience.OutcomeDeadLettered, nil
	}
	wrapped := WithPrometheus(sink, inner)

	before := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "dead_lettered"))
	if _, err := wrapped(context.Background(), "some-topic", []byte("v"), nil); err != nil {
		t.Fatalf("wrapped: %v", err)
	}
	after := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "dead_lettered"))
	if after != before+1 {
		t.Fatalf("eci_messages_processed_total{sink=%q,outcome=\"dead_lettered\"} = %v, want %v", sink, after, before+1)
	}
}

// SPEC-036 §3 scenario 4.
func TestWithPrometheusUpdatesLastProcessedTimestamp(t *testing.T) {
	sink := "test-sink-scenario4-timestamp"
	inner := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return resilience.OutcomeProcessed, nil
	}
	wrapped := WithPrometheus(sink, inner)

	before := time.Now().Unix()
	if _, err := wrapped(context.Background(), "some-topic", []byte("v"), nil); err != nil {
		t.Fatalf("wrapped: %v", err)
	}
	after := time.Now().Unix()

	got := testutil.ToFloat64(lastProcessedTimestampSeconds.WithLabelValues(sink))
	if got < float64(before) || got > float64(after) {
		t.Fatalf("eci_last_processed_timestamp_seconds{sink=%q} = %v, want tra %d e %d", sink, got, before, after)
	}
}

// Edge case indiretto: WithPrometheus deve aggiornare il timestamp e il
// contatore ANCHE quando inner ritorna un errore (SPEC-036 §3 scenario 4:
// "indipendentemente dall'esito") — un tentativo è comunque avvenuto,
// l'errore va propagato al chiamante ma non deve far saltare
// l'aggiornamento delle metriche.
func TestWithPrometheusUpdatesMetricsEvenOnInnerError(t *testing.T) {
	sink := "test-sink-inner-error"
	innerErr := context.DeadlineExceeded
	inner := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return resilience.OutcomeRetried, innerErr
	}
	wrapped := WithPrometheus(sink, inner)

	before := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "retried"))
	_, err := wrapped(context.Background(), "some-topic", []byte("v"), nil)
	if err != innerErr {
		t.Fatalf("errore ritornato = %v, want %v (propagato invariato)", err, innerErr)
	}
	after := testutil.ToFloat64(messagesProcessedTotal.WithLabelValues(sink, "retried"))
	if after != before+1 {
		t.Fatalf("il contatore deve incrementare anche quando inner ritorna un errore: before=%v after=%v", before, after)
	}
}

// Verifica diretta che Handler() esponga le metriche in formato Prometheus
// valido, contenenti i nomi di metrica dichiarati (SPEC-036 §2) — non solo
// che il registry in-process le contenga, un primo passo verso lo
// scenario 5 (verificato via HTTP reale separatamente sul sink
// rappresentativo).
func TestHandlerExposesDeclaredMetricNames(t *testing.T) {
	sink := "test-sink-handler-exposition"
	inner := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return resilience.OutcomeProcessed, nil
	}
	wrapped := WithPrometheus(sink, inner)
	if _, err := wrapped(context.Background(), "some-topic", []byte("v"), nil); err != nil {
		t.Fatalf("wrapped: %v", err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, name := range []string{"eci_messages_processed_total", "eci_last_processed_timestamp_seconds"} {
		if !strings.Contains(body, name) {
			t.Fatalf("corpo di /metrics non contiene %q:\n%s", name, body)
		}
	}
}
