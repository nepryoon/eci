package consumer

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/eci-project/eci/libs/go/eci/kafkatrace"
)

var tracer = otel.Tracer("sink-vector")

// FetchAndProcess esegue un ciclo fetch->process->commit (SPEC-033 §2/§4):
// un errore di ProcessMessage (Postgres/Qdrant irraggiungibile) NON
// committa l'offset — il messaggio verrà riconsegnato al prossimo poll.
// Apre uno span per messaggio processato (§8), collegato via span link al
// trace del produttore quando l'header trace_id è presente e valido —
// stesso pattern di sink-graph/embedding-worker (SPEC-011/015/030).
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
	spanCtx, span := tracer.Start(ctx, "sink-vector.process_message", spanOpts...)
	defer span.End()

	outcome, err := ProcessMessage(spanCtx, deps, msg.Topic, msg.Value, msg.Headers)
	if err != nil {
		deps.Logf("sink-vector: elaborazione fallita (topic=%s), offset NON committato: %v", msg.Topic, err)
		return outcome, err
	}

	if err := reader.CommitMessages(ctx, msg); err != nil {
		return outcome, fmt.Errorf("commit offset (topic=%s): %w", msg.Topic, err)
	}
	return outcome, nil
}
