package kafkaready

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

type fakeClient struct {
	metadata    *kafka.MetadataResponse
	metadataErr error
	coordinator *kafka.FindCoordinatorResponse
	coordErr    error
	offsets     *kafka.OffsetFetchResponse
	offsetsErr  error
	fetch       *kafka.FetchResponse
	fetchErr    error
}

func (f fakeClient) Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	return f.metadata, f.metadataErr
}

func (f fakeClient) FindCoordinator(context.Context, *kafka.FindCoordinatorRequest) (*kafka.FindCoordinatorResponse, error) {
	return f.coordinator, f.coordErr
}

func (f fakeClient) OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error) {
	return f.offsets, f.offsetsErr
}

func (f fakeClient) Fetch(context.Context, *kafka.FetchRequest) (*kafka.FetchResponse, error) {
	return f.fetch, f.fetchErr
}

func TestCheckerRequiresTopicAndGroupAuthorization(t *testing.T) {
	validMetadata := &kafka.MetadataResponse{Topics: []kafka.Topic{{Name: "events", Partitions: []kafka.Partition{{ID: 0, Leader: kafka.Broker{Host: "broker", Port: 9093}}}}}}
	validCoordinator := &kafka.FindCoordinatorResponse{
		Coordinator: &kafka.FindCoordinatorResponseCoordinator{NodeID: 1, Host: "broker", Port: 9093},
	}
	validOffsets := &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{
		"events": {{Partition: 0, CommittedOffset: -1}},
	}}
	validFetch := &kafka.FetchResponse{Topic: "events", Partition: 0, Records: kafka.NewRecordReader()}

	tests := []struct {
		name   string
		client fakeClient
		ok     bool
	}{
		{name: "authorized", client: fakeClient{metadata: validMetadata, coordinator: validCoordinator, offsets: validOffsets, fetch: validFetch}, ok: true},
		{name: "transport failure", client: fakeClient{metadataErr: errors.New("tls failed")}},
		{name: "topic denied", client: fakeClient{metadata: &kafka.MetadataResponse{Topics: []kafka.Topic{{Name: "events", Error: errors.New("topic authorization failed")}}}, coordinator: validCoordinator, offsets: validOffsets}},
		{name: "group coordinator denied", client: fakeClient{metadata: validMetadata, coordinator: &kafka.FindCoordinatorResponse{Error: errors.New("group authorization failed")}, offsets: validOffsets}},
		{name: "missing coordinator", client: fakeClient{metadata: validMetadata, coordinator: &kafka.FindCoordinatorResponse{}, offsets: validOffsets}},
		{name: "group offsets denied", client: fakeClient{metadata: validMetadata, coordinator: validCoordinator, offsets: &kafka.OffsetFetchResponse{Error: errors.New("group authorization failed")}}},
		{name: "partition offset denied", client: fakeClient{metadata: validMetadata, coordinator: validCoordinator, offsets: &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{"events": {{Partition: 0, Error: errors.New("group authorization failed")}}}}}},
		{name: "incomplete offsets", client: fakeClient{metadata: validMetadata, coordinator: validCoordinator, offsets: &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{"events": {}}}}},
		{name: "topic read denied", client: fakeClient{metadata: validMetadata, coordinator: validCoordinator, offsets: validOffsets, fetchErr: errors.New("topic read authorization failed")}},
		{name: "topic read response denied", client: fakeClient{metadata: validMetadata, coordinator: validCoordinator, offsets: validOffsets, fetch: &kafka.FetchResponse{Topic: "events", Partition: 0, Error: errors.New("topic read authorization failed")}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checker := newChecker(test.client, "worker", []string{"events"})
			err := checker.Check(context.Background())
			if test.ok && err != nil {
				t.Fatalf("authorized readiness failed: %v", err)
			}
			if !test.ok && err == nil {
				t.Fatal("unauthorized/unreachable Kafka reported ready")
			}
		})
	}
}

func TestHandlerReturnsClosedLowDetailStatus(t *testing.T) {
	for _, test := range []struct {
		name   string
		check  func(context.Context) error
		status int
	}{
		{name: "ready", check: func(context.Context) error { return nil }, status: http.StatusNoContent},
		{name: "not ready", check: func(context.Context) error { return errors.New("secret broker detail") }, status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			Handler(test.check).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if response.Body.String() != "" {
				t.Fatalf("readiness leaked body %q", response.Body.String())
			}
		})
	}
}
