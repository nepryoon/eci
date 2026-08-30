package consumer

import (
	"context"
	"testing"

	"github.com/qdrant/go-client/qdrant"
	kafka "github.com/segmentio/kafka-go"
)

func TestUpsertCompletionRequiresAppliedQdrantResult(t *testing.T) {
	req, err := buildUpsertRequest(codeEmbeddingPayload{
		ID:       "embedding-id",
		EntityID: "entity-id",
		Vector:   []float32{0.25, 0.75},
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.Wait == nil || !req.GetWait() {
		t.Fatal("Qdrant upsert must wait until the update is applied")
	}

	if err := validateAppliedUpdate(&qdrant.UpdateResult{Status: qdrant.UpdateStatus_Completed}); err != nil {
		t.Fatalf("completed update rejected: %v", err)
	}
	for _, result := range []*qdrant.UpdateResult{
		nil,
		{Status: qdrant.UpdateStatus_UnknownUpdateStatus},
		{Status: qdrant.UpdateStatus_Acknowledged},
		{Status: qdrant.UpdateStatus_ClockRejected},
		{Status: qdrant.UpdateStatus_WaitTimeout},
	} {
		if err := validateAppliedUpdate(result); err == nil {
			t.Fatalf("non-completed update accepted: %#v", result)
		}
	}
}

func TestDeleteRequestWaitsAndIntersectsPointWithCanonicalScope(t *testing.T) {
	request := buildDeleteRequest(codeEmbeddingTombstone{
		ID: "embedding-id",
		Security: securityProvenance{
			TenantID: "tenant-a", Repo: "repo-a", ACLGroup: "developers",
		},
	})
	if request.Wait == nil || !request.GetWait() {
		t.Fatal("Qdrant delete must wait until applied")
	}
	filter := request.GetPoints().GetFilter()
	if filter == nil || len(filter.GetMust()) != 4 {
		t.Fatalf("delete filter = %+v, want has-id plus three scope conditions", filter)
	}
	if got := filter.GetMust()[0].GetHasId().GetHasId(); len(got) != 1 || got[0].GetUuid() != DerivePointID("embedding-id") {
		t.Fatalf("delete has-id = %+v", got)
	}
	for i, field := range []string{"tenant_id", "repo", "acl_group"} {
		if got := filter.GetMust()[i+1].GetField().GetKey(); got != field {
			t.Fatalf("scope condition %d key=%q want=%q", i, got, field)
		}
	}
}

func TestProcessRejectsMissingOperationBeforeDependencies(t *testing.T) {
	outcome, err := ProcessMessage(
		context.Background(), Deps{Logf: func(string, ...any) {}},
		TopicCodeEmbedding, []byte(`{"id":"sensitive"}`),
		[]kafka.Header{{Key: "event_id", Value: []byte("11111111-1111-1111-1111-111111111111")}},
	)
	if err != nil || outcome != OutcomeInvalidSkipped {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
	var _ Outcome = OutcomeDeleted
}
