package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/resilience"
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

func TestWaitUntilDependencyReadyDoesNotReturnDuringOutage(t *testing.T) {
	var calls atomic.Int32
	check := func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("cold")
		}
		return nil
	}
	if err := waitUntilDependencyReady(context.Background(), time.Millisecond, check); err != nil {
		t.Fatalf("waitUntilDependencyReady: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("dependency checks = %d, want 3", got)
	}
}

func TestWaitUntilDependencyReadyHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitUntilDependencyReady(ctx, time.Hour, func(context.Context) error {
		return errors.New("down")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

type fakeLoopReader struct {
	message  kafka.Message
	commits  int
	closed   bool
	onCommit func()
}

func (r *fakeLoopReader) FetchMessage(context.Context) (kafka.Message, error) {
	return r.message, nil
}

func (r *fakeLoopReader) CommitMessages(context.Context, ...kafka.Message) error {
	r.commits++
	if r.onCommit != nil {
		r.onCommit()
	}
	return nil
}

func (r *fakeLoopReader) Close() error {
	r.closed = true
	return nil
}

func TestConsumeLoopReopensReaderBeforeLaterOffsetsCanCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first := &fakeLoopReader{message: kafka.Message{Topic: "chunks", Offset: 7}}
	second := &fakeLoopReader{message: first.message, onCommit: cancel}
	readers := []closeableReader{first, second}
	created := 0
	factory := func() closeableReader {
		reader := readers[created]
		created++
		return reader
	}
	processed := 0
	readyChecks := 0
	process := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		processed++
		if processed == 1 {
			return resilience.OutcomeProcessed, errors.New("TEI unavailable")
		}
		return resilience.OutcomeProcessed, nil
	}

	err := consumeLoop(ctx, time.Millisecond, func(context.Context) error {
		readyChecks++
		return nil
	}, factory, process, func(string, ...any) {})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("consumeLoop error = %v, want context.Canceled", err)
	}
	if created != 2 || !first.closed || !second.closed {
		t.Fatalf("reader lifecycle: created=%d first.closed=%v second.closed=%v", created, first.closed, second.closed)
	}
	if first.commits != 0 || second.commits != 1 {
		t.Fatalf("commits before/after recovery = %d/%d, want 0/1", first.commits, second.commits)
	}
	if readyChecks != 2 {
		t.Fatalf("dependency checks = %d, want initial plus post-failure recovery", readyChecks)
	}
}
