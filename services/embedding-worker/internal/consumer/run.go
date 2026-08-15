package consumer

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/eci-project/eci/libs/go/eci/kafkatrace"
)

var tracer = otel.Tracer("embedding-worker")

// FetchAndProcess esegue un ciclo fetch->process->commit (SPEC-030 §2/§4):
// un errore di ProcessMessage (chiamata di embedding fallita o
// infrastruttura irraggiungibile) NON committa l'offset — il messaggio
// verrà riconsegnato al prossimo poll, gestito correttamente da
// processed_events (§3 scenario 4). Apre uno span per messaggio processato
// (§8), collegato via span link al trace del produttore quando l'header
// trace_id è presente e valido — stesso pattern di sink-graph (SPEC-011/
// SPEC-015).
func FetchAndProcess(ctx context.Context, reader *kafka.Reader, deps Deps) (Outcome, error) {
	msg, err := reader.FetchMessage(ctx)
	if err != nil {
		return OutcomeInvalidSkipped, fmt.Errorf("fetch message: %w", err)
	}

	var spanOpts []trace.SpanStartOption
	if traceID, ok := kafkatrace.TraceIDFromHeaders(msg.Headers); ok {
		if link, ok := kafkatrace.SpanLinkFromTraceID(traceID); ok {
			spanOpts = append(spanOpts, trace.WithLinks(link))
		}
	}
	spanCtx, span := tracer.Start(ctx, "embedding-worker.process_message", spanOpts...)
	defer span.End()

	outcome, err := ProcessMessage(spanCtx, deps, msg.Topic, msg.Value, msg.Headers)
	if err != nil {
		deps.Logf("embedding-worker: elaborazione fallita (topic=%s), offset NON committato: %v", msg.Topic, err)
		return outcome, err
	}

	if err := reader.CommitMessages(ctx, msg); err != nil {
		return outcome, fmt.Errorf("commit offset (topic=%s): %w", msg.Topic, err)
	}
	return outcome, nil
}
