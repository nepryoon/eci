package hybridsearch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
)

func TestHydrateSourceTextBatchesSortsAndCarriesAuthenticatedScope(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("x-proxy-ext-tenant_id") != "tenant" || !strings.Contains(request.Header.Get("x-proxy-ext-allowed_repos"), "repo") {
			t.Errorf("missing authenticated scope headers: %v", request.Header)
		}
		w.Header().Set("content-type", "application/json")
		_, _ = fmt.Fprint(w, `{"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":2,"relation":"eq"},"hits":[{"_id":"2","_source":{"entity_id":"n","chunk_index":1,"text":"B"}},{"_id":"1","_source":{"entity_id":"n","chunk_index":0,"text":"A"}}]}}`)
	}))
	t.Cleanup(server.Close)

	nodes := []RetrievedNode{{NodeID: "n"}}
	if err := HydrateSourceText(hydrationContext(t), openSearchTestClient(t, server.URL), nodes); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || nodes[0].SourceText != "A\nB" {
		t.Fatalf("calls=%d source=%q, want one batch and ordered chunks", calls.Load(), nodes[0].SourceText)
	}
}

func TestHydrateSourceTextRejectsPartialResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = fmt.Fprint(w, `{"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":2,"relation":"eq"},"hits":[{"_id":"1","_source":{"entity_id":"n","chunk_index":0,"text":"A"}}]}}`)
	}))
	t.Cleanup(server.Close)
	if err := HydrateSourceText(hydrationContext(t), openSearchTestClient(t, server.URL), []RetrievedNode{{NodeID: "n"}}); err == nil {
		t.Fatal("partial source hydration was silently accepted")
	}
}

func openSearchTestClient(t *testing.T, address string) *opensearchapi.Client {
	t.Helper()
	client, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{address}}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func hydrationContext(t *testing.T) context.Context {
	t.Helper()
	encoded, err := proto.Marshal(&retrievalv1.SecurityContext{TenantId: "tenant", UserId: "user", AllowedRepos: []string{"repo"}, AclGroups: []string{"dev"}})
	if err != nil {
		t.Fatal(err)
	}
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs("eci-security-context-bin", string(encoded)))
	var result context.Context
	_, err = secctx.UnaryServerInterceptor()(incoming, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		result = ctx
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
