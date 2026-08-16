package consumer

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/eci-project/eci/libs/go/eci/kafkatrace"
	"github.com/eci-project/eci/libs/go/eci/resilience"
)

var tracer = otel.Tracer("sink-graph")

// FetchAndProcess esegue un ciclo fetch->process->commit (SPEC-015 §2/§4):
// un errore di `process` (infrastruttura irraggiungibile) NON committa
// l'offset — il messaggio verrà riconsegnato al riavvio, gestito
// correttamente da processed_events (§3 scenario 2). Apre uno span per
// messaggio processato (§8), collegato via span link al trace del
// produttore quando l'header trace_id è presente e valido
// (kafkatrace.TraceIDFromHeaders/SpanLinkFromTraceID, SPEC-011) — LINK,
// non parent-child, stesso pattern già stabilito in SPEC-011 per un
// confine asincrono di messaging.
//
// `process` (SPEC-035 §2, T3.3 parte 1/2): iniettato dal chiamante
// (main.go) — tipicamente ProcessMessage avvolta con Deps catturato per
// closure e passata attraverso resilience.WithRetryAndDLQ. FetchAndProcess
// stessa non conosce né Deps né la logica di retry/DLQ, resta un puro
// orchestratore fetch/span/commit — nessuna modifica alla logica
// applicativa di ProcessMessage stessa (SPEC-035 §2).
func FetchAndProcess(ctx context.Context, reader *kafka.Reader, process resilience.ProcessFunc) (resilience.Outcome, error) {
	msg, err := reader.FetchMessage(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetch message: %w", err)
	}

	var spanOpts []trace.SpanStartOption
	if traceID, ok := kafkatrace.TraceIDFromHeaders(msg.Headers); ok {
		if link, ok := kafkatrace.SpanLinkFromTraceID(traceID); ok {
			spanOpts = append(spanOpts, trace.WithLinks(link))
		}
	}
	spanCtx, span := tracer.Start(ctx, "sink-graph.process_message", spanOpts...)
	defer span.End()

	outcome, err := process(spanCtx, msg.Topic, msg.Value, msg.Headers)
	if err != nil {
		return outcome, err
	}

	if err := reader.CommitMessages(ctx, msg); err != nil {
		return outcome, fmt.Errorf("commit offset (topic=%s): %w", msg.Topic, err)
	}
	return outcome, nil
}
