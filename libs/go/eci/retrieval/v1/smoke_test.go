package retrievalv1

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// SPEC-002 §3 scenario 2, §7 — round-trip di marshalling protobuf sui
// messaggi chiave generati da retrieval.proto.

func TestSecurityContextRoundTrip(t *testing.T) {
	want := &SecurityContext{TenantId: "t1"}
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &SecurityContext{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.GetTenantId() != "t1" {
		t.Fatalf("TenantId = %q, want %q", got.GetTenantId(), "t1")
	}
}

func TestRetrievedNodeRoundTrip(t *testing.T) {
	want := &RetrievedNode{
		NodeId: "n1",
		Domain: Domain_DOMAIN_CODE,
		Name:   "MyClass",
		Scores: &NodeScores{RrfScore: 0.5},
	}
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &RetrievedNode{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.GetNodeId() != "n1" {
		t.Fatalf("NodeId = %q, want %q", got.GetNodeId(), "n1")
	}
	if got.GetScores().GetRrfScore() != 0.5 {
		t.Fatalf("Scores.RrfScore = %v, want 0.5", got.GetScores().GetRrfScore())
	}
}

func TestHybridSearchRequestRoundTrip(t *testing.T) {
	want := &HybridSearchRequest{
		SecurityContext: &SecurityContext{TenantId: "t1"},
		QueryText:       "chi chiama X?",
		TopK:            25,
	}
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &HybridSearchRequest{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.GetSecurityContext().GetTenantId() != "t1" {
		t.Fatalf("SecurityContext.TenantId = %q, want %q", got.GetSecurityContext().GetTenantId(), "t1")
	}
	if got.GetTopK() != 25 {
		t.Fatalf("TopK = %d, want 25", got.GetTopK())
	}
}
