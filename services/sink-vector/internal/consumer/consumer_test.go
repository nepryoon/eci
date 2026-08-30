package consumer

import (
	"testing"

	"github.com/qdrant/go-client/qdrant"
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
