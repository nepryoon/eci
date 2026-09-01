package hybridsearch

import (
	"context"
	"encoding/json"
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

func TestHydrateSourceTextPaginatesBeyondOneThousandChunks(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		call := calls.Add(1)
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode search body: %v", err)
		}
		if call == 1 {
			if _, ok := body["search_after"]; ok {
				t.Error("first page unexpectedly contains search_after")
			}
		} else if call == 2 {
			if _, ok := body["search_after"]; !ok {
				t.Error("second page is missing search_after")
			}
		} else {
			t.Fatalf("unexpected hydration page %d", call)
		}

		first, last := 0, 1000
		if call == 2 {
			first, last = 1000, 1001
		}
		hits := make([]map[string]any, 0, last-first)
		for i := first; i < last; i++ {
			hits = append(hits, map[string]any{
				"_id":     fmt.Sprintf("chunk-%04d", i),
				"_source": map[string]any{"entity_id": "n", "chunk_index": i, "text": fmt.Sprintf("C%d", i)},
				"sort":    []any{"n", i},
			})
		}
		w.Header().Set("content-type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"took": 1, "timed_out": false,
			"_shards": map[string]any{"total": 1, "successful": 1, "skipped": 0, "failed": 0},
			"hits":    map[string]any{"total": map[string]any{"value": 1001, "relation": "eq"}, "hits": hits},
		}); err != nil {
			t.Errorf("encode search response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	nodes := []RetrievedNode{{NodeID: "n"}}
	if err := HydrateSourceText(hydrationContext(t), openSearchTestClient(t, server.URL), nodes); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || !strings.HasPrefix(nodes[0].SourceText, "C0\nC1\n") || !strings.HasSuffix(nodes[0].SourceText, "\nC1000") {
		t.Fatalf("calls=%d source prefix/suffix unexpected", calls.Load())
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
