// SPEC-011 §3 scenario 1 — verifica manuale (non automatizzabile in modo
// pulito senza parsing dell'output, §7): InitTracing deve produrre output
// leggibile su stdout per uno span reale, non solo compilare. Stesso
// pattern di libs/rust/eci-common/examples/manual_scenario1.rs (SPEC-010).
//
// Uso: go run ./eci/observability/examplemain (dalla root di libs/go)
package main

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"

	"github.com/eci-project/eci/libs/go/eci/observability"
)

func main() {
	shutdown, err := observability.InitTracing("retrieval-engine")
	if err != nil {
		fmt.Fprintf(os.Stderr, "InitTracing: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
		}
	}()

	tracer := otel.Tracer("examplemain")
	_, span := tracer.Start(context.Background(), "ingest_commit")
	fmt.Fprintf(os.Stderr, "[manual check] trace_id dentro lo span: %s\n", span.SpanContext().TraceID())
	span.End()
}
