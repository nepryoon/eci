// Package kafkatrace estrae il trace_id dagli header dei messaggi Kafka
// prodotti dal connector Debezium (colonna outbox.trace_id promossa a
// header, vedi addendum SPEC-011 §2 a deploy/compose/debezium-outbox-connector.json)
// e costruisce un trace.Link per collegare il consumo (sink-graph, T1.3)
// al trace del produttore — LINK, non parent-child: pattern OTel standard
// per un confine asincrono di messaging (SPEC-011 §6).
package kafkatrace

import (
	"crypto/rand"
	"regexp"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"
)

// headerKey è il nome dell'header Kafka promosso dal connector Debezium
// (transforms.outbox.table.fields.additional.placement=trace_id:header:trace_id).
const headerKey = "trace_id"

// traceIDPattern valida un trace_id esadecimale W3C Trace Context di
// esattamente 32 caratteri (128 bit) — stesso formato di SPEC-010 §2.
var traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// TraceIDFromHeaders estrae il trace_id dagli header di un messaggio
// Kafka. ok=false se l'header trace_id è assente o il valore non è un
// trace_id esadecimale valido di 32 caratteri — trattato come assente,
// non troncato/adattato silenziosamente a un formato valido (SPEC-011 §4).
func TraceIDFromHeaders(headers []kafka.Header) (traceID string, ok bool) {
	for _, h := range headers {
		if h.Key != headerKey {
			continue
		}
		v := string(h.Value)
		if !traceIDPattern.MatchString(v) {
			return "", false
		}
		return v, true
	}
	return "", false
}

// SpanLinkFromTraceID costruisce un trace.Link da passare come opzione
// all'apertura di uno span di consumo. ok=false se traceID non è un
// trace_id esadecimale valido di 32 caratteri (stessa validazione di
// TraceIDFromHeaders).
//
// L'header Kafka promosso dal connector porta solo il trace_id, non uno
// span_id del produttore (fuori scope, non propagato — SPEC-011 §2/§5):
// il Link viene quindi costruito con uno SpanID generato casualmente
// (valido, non-zero, ma non corrispondente a un vero span del
// produttore) — scelta deliberata per ottenere un SpanContext
// strutturalmente valido (IsValid()==true, requisito OTel per un Link
// utile) pur non avendo un vero span_id da riferire; documentata come
// deviazione in SPEC-011 §10.
func SpanLinkFromTraceID(traceID string) (link trace.Link, ok bool) {
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return trace.Link{}, false
	}

	var randomSpanID trace.SpanID
	if _, err := rand.Read(randomSpanID[:]); err != nil {
		return trace.Link{}, false
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     randomSpanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	return trace.Link{SpanContext: sc}, true
}
