// Package securitylabels validates and observes materialized-view labels.
package securitylabels

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var total = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "eci_sink_security_labels_total",
	Help: "Security-label validation outcomes for materialized-view sinks.",
}, []string{"sink", "outcome"})

func Valid(values ...string) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 256 || !utf8.ValidString(value) {
			return false
		}
		for _, r := range value {
			if unicode.IsControl(r) {
				return false
			}
		}
	}
	return true
}

func Outcome(values ...string) string {
	for _, value := range values {
		if value == "" { return "missing" }
	}
	if !Valid(values...) { return "invalid" }
	return "accepted"
}

func Observe(sink, outcome string) {
	switch sink {
	case "sink-graph", "sink-vector", "sink-search":
	default:
		sink = "unknown"
	}
	switch outcome {
	case "accepted", "missing", "invalid":
	default:
		outcome = "invalid"
	}
	total.WithLabelValues(sink, outcome).Inc()
}
