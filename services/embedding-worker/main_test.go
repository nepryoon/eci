package main

import (
	"context"
	"errors"
	"testing"
)

func TestCombinedReadinessRequiresKafkaAndEmbedder(t *testing.T) {
	want := errors.New("unhealthy")
	for _, test := range []struct {
		name      string
		kafkaErr  error
		embedErr  error
		wantErr   error
		embedCall bool
	}{
		{name: "both healthy", embedCall: true},
		{name: "missing embedder check", wantErr: errReadinessCheckMissing},
		{name: "Kafka unhealthy", kafkaErr: want, wantErr: want},
		{name: "embedder unhealthy", embedErr: want, wantErr: want, embedCall: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			embedCalled := false
			embedCheck := func(context.Context) error { embedCalled = true; return test.embedErr }
			if test.name == "missing embedder check" {
				embedCheck = nil
			}
			check := combinedReadiness(func(context.Context) error { return test.kafkaErr }, embedCheck)
			if err := check(context.Background()); !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if embedCalled != test.embedCall {
				t.Fatalf("embed check called = %v, want %v", embedCalled, test.embedCall)
			}
		})
	}
}
