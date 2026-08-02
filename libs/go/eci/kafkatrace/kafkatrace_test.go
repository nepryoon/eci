package kafkatrace

import (
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

const validTraceID = "4dce3c5c6f7943886982db4b6c3c8b50"

// SPEC-011 §3 scenario 4.
func TestTraceIDFromHeadersValid(t *testing.T) {
	headers := []kafka.Header{
		{Key: "trace_id", Value: []byte(validTraceID)},
	}
	got, ok := TraceIDFromHeaders(headers)
	if !ok {
		t.Fatalf("ok = false, want true per header trace_id valido")
	}
	if got != validTraceID {
		t.Fatalf("TraceIDFromHeaders = %q, want %q", got, validTraceID)
	}

	link, ok := SpanLinkFromTraceID(got)
	if !ok {
		t.Fatalf("SpanLinkFromTraceID(%q): ok = false, want true", got)
	}
	if !link.SpanContext.IsValid() {
		t.Fatalf("SpanLinkFromTraceID(%q): SpanContext non valido: %+v", got, link.SpanContext)
	}
	if link.SpanContext.TraceID().String() != validTraceID {
		t.Fatalf("link.SpanContext.TraceID() = %q, want %q", link.SpanContext.TraceID().String(), validTraceID)
	}
}

// SPEC-011 §3 scenario 5 / §4 edge case.
func TestTraceIDFromHeadersMissing(t *testing.T) {
	headers := []kafka.Header{
		{Key: "other-header", Value: []byte("irrelevant")},
	}
	if _, ok := TraceIDFromHeaders(headers); ok {
		t.Fatalf("ok = true, want false (header trace_id assente)")
	}
}

func TestTraceIDFromHeadersMalformedNotHex(t *testing.T) {
	headers := []kafka.Header{
		{Key: "trace_id", Value: []byte("not-hexadecimal-at-all-xxxxxxxx")},
	}
	if _, ok := TraceIDFromHeaders(headers); ok {
		t.Fatalf("ok = true, want false (valore non esadecimale)")
	}
}

func TestTraceIDFromHeadersMalformedWrongLength(t *testing.T) {
	headers := []kafka.Header{
		{Key: "trace_id", Value: []byte("abc123")},
	}
	if _, ok := TraceIDFromHeaders(headers); ok {
		t.Fatalf("ok = true, want false (lunghezza diversa da 32 caratteri)")
	}
}

func TestSpanLinkFromTraceIDInvalidInput(t *testing.T) {
	if _, ok := SpanLinkFromTraceID("not-a-valid-trace-id"); ok {
		t.Fatalf("ok = true, want false per trace_id non valido")
	}
}
