package securityfilter

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestSecurityFilterMetricsUseBoundedLabels(t *testing.T) {
	_, done := Observe(context.Background(), "attacker-controlled-store")
	done("attacker-controlled-outcome")
	AddRecheckRemoved(2)
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	foundFilter, foundRecheck := false, false
	for _, family := range families {
		switch family.GetName() {
		case "eci_retrieval_security_filter_total":
			foundFilter = true
			for _, metric := range family.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetValue() == "attacker-controlled-store" || label.GetValue() == "attacker-controlled-outcome" {
						t.Fatal("unbounded label leaked into metric")
					}
				}
			}
		case "eci_retrieval_acl_recheck_removed_total":
			foundRecheck = true
		}
	}
	if !foundFilter || !foundRecheck {
		t.Fatalf("metrics missing: filter=%v recheck=%v", foundFilter, foundRecheck)
	}
}
