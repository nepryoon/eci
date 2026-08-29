package server

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var scopeDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "eci_semantic_cache_scope_decisions_total",
	Help: "Authenticated semantic-cache scope decisions by bounded operation and outcome.",
}, []string{"operation", "outcome"})

func observeScope(ctx context.Context, operation string) (context.Context, func(string)) {
	operation = boundedOperation(operation)
	ctx, span := otel.Tracer("eci/semantic-cache/security").Start(ctx, "eci.semantic_cache.scope")
	span.SetAttributes(attribute.String("eci.operation", operation))
	return ctx, func(outcome string) {
		outcome = boundedScopeOutcome(outcome)
		scopeDecisions.WithLabelValues(operation, outcome).Inc()
		span.SetAttributes(attribute.String("eci.outcome", outcome))
		span.End()
	}
}

func boundedOperation(value string) string {
	if value == "put" {
		return "put"
	}
	return "get"
}

func boundedScopeOutcome(value string) string {
	switch value {
	case "allow", "deny", "error":
		return value
	default:
		return "error"
	}
}
