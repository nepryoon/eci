// Package observability fornisce il bootstrap OTel per i servizi Go
// (equivalente di libs/rust/eci-common::observability, SPEC-010): un
// TracerProvider con exporter stdout/console, pronto a passare a un
// backend OTLP reale in Fase 7 cambiando solo l'exporter.
package observability

import (
	"context"
	"fmt"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.33.0"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	initMu   sync.Mutex
	initDone bool
)

// InitTracing inizializza il TracerProvider OTel con un exporter
// stdout/console e lo imposta come TracerProvider globale (otel.SetTracerProvider).
// Ritorna una funzione di shutdown da chiamare via defer per il flush
// pulito alla chiusura del processo.
//
// Va chiamata una sola volta per processo. **Edge case (SPEC-011 §4)**: se
// chiamata una seconda volta, NON panica — è un no-op (il TracerProvider
// globale già impostato resta attivo), con un warning esplicito su
// stderr, stesso comportamento non-panicante di SPEC-010 §4. La funzione
// di shutdown ritornata dalla chiamata no-op è comunque valida (esegue lo
// shutdown del proprio TracerProvider inerte, mai impostato come globale).
func InitTracing(serviceName string) (func(context.Context) error, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, fmt.Errorf("eci/observability: creazione stdout exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, fmt.Errorf("eci/observability: costruzione resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		// WithSyncer: export sincrono a ogni fine-span (non batched) — gli
		// span sono visibili su stdout immediatamente, comportamento
		// deterministico per lo scenario 1 (ispezione manuale) e per i test.
		sdktrace.WithSyncer(exporter),
		sdktrace.WithResource(res),
	)

	initMu.Lock()
	alreadyInit := initDone
	if !alreadyInit {
		initDone = true
		otel.SetTracerProvider(tp)
		// go.opentelemetry.io/otel non imposta un TextMapPropagator globale
		// di default (resta no-op finché non chiamato esplicitamente, a
		// differenza dell'SDK Python che lo fa automaticamente) — senza
		// questa riga otelgrpc.NewServerHandler()/NewClientHandler() non
		// estrae/inietta mai un header traceparent W3C, anche se l'altro
		// lato lo invia correttamente. Bug latente scoperto durante la
		// verifica di interoperabilità cross-linguaggio di SPEC-012 (il
		// test di coesistenza di SPEC-011 §3 verificava solo che
		// l'interceptor SecurityContext e otelgrpc non confliggessero, non
		// l'estrazione reale di un trace context in ingresso).
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		))
	}
	initMu.Unlock()

	if alreadyInit {
		fmt.Fprintln(os.Stderr,
			"eci/observability: InitTracing chiamata più di una volta nello stesso processo; "+
				"il TracerProvider globale esistente resta attivo (no-op).")
	}

	return tp.Shutdown, nil
}
