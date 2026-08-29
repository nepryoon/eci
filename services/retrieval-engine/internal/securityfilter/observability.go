package securityfilter

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var (
	filterTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "eci_retrieval_security_filter_total",
		Help: "Pre-retrieval security filter outcomes by bounded store and outcome.",
	}, []string{"store", "outcome"})
	recheckRemoved = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eci_retrieval_acl_recheck_removed_total",
		Help: "Candidates removed by the authoritative Neo4j ACL re-check.",
	})
)

// Observe starts the bounded security-filter span and returns a completion
// closure. Callers must pass only allow, empty, or error.
func Observe(ctx context.Context, store string) (context.Context, func(string)) {
	ctx, span := otel.Tracer("eci/retrieval/security").Start(ctx, "eci.retrieval.security_filter")
	span.SetAttributes(attribute.String("eci.store", boundedStore(store)))
	return ctx, func(outcome string) {
		outcome = boundedOutcome(outcome)
		store = boundedStore(store)
		filterTotal.WithLabelValues(store, outcome).Inc()
		span.SetAttributes(attribute.String("eci.outcome", outcome))
		span.End()
	}
}

func AddRecheckRemoved(count int) {
	if count > 0 {
		recheckRemoved.Add(float64(count))
	}
}

func boundedStore(value string) string {
	switch value {
	case "neo4j", "qdrant", "opensearch":
		return value
	default:
		return "neo4j"
	}
}

func boundedOutcome(value string) string {
	switch value {
	case "allow", "empty", "error":
		return value
	default:
		return "error"
	}
}
