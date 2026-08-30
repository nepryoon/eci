package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	kafka "github.com/segmentio/kafka-go"
)

func TestDeleteRequestIsSynchronousAndIntersectsIDWithCanonicalScope(t *testing.T) {
	req, err := buildDeleteRequest(codeChunkTombstone{
		ID: "chunk-id",
		Security: securityProvenance{
			TenantID: "tenant-a", Repo: "repo-a", ACLGroup: "developers",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpReq, err := req.GetRequest(http.MethodPost)
	if err != nil {
		t.Fatal(err)
	}
	query := httpReq.URL.Query()
	if query.Get("refresh") != "true" || query.Get("wait_for_completion") != "true" || query.Get("max_docs") != "1" {
		t.Fatalf("delete query params=%v", query)
	}

	var body struct {
		Query struct {
			Bool struct {
				Filter []map[string]json.RawMessage `json:"filter"`
			} `json:"bool"`
		} `json:"query"`
	}
	if err := json.NewDecoder(httpReq.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Query.Bool.Filter) != 4 {
		t.Fatalf("delete filter=%+v, want id plus three scope conditions", body.Query.Bool.Filter)
	}
	var ids struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal(body.Query.Bool.Filter[0]["ids"], &ids); err != nil {
		t.Fatal(err)
	}
	if len(ids.Values) != 1 || ids.Values[0] != "chunk-id" {
		t.Fatalf("ids filter=%v", ids.Values)
	}
	for index, field := range []string{"tenant_id", "repo", "acl_group"} {
		var term map[string]string
		if err := json.Unmarshal(body.Query.Bool.Filter[index+1]["term"], &term); err != nil {
			t.Fatal(err)
		}
		if _, ok := term[field]; !ok {
			t.Fatalf("scope filter %d=%v want field %q", index, term, field)
		}
	}
}

func TestDeleteCompletionRejectsPartialResponses(t *testing.T) {
	if err := validateDeleteResponse(&opensearchapi.DocumentDeleteByQueryResp{Total: 1, Deleted: 1}); err != nil {
		t.Fatalf("complete delete rejected: %v", err)
	}
	if err := validateDeleteResponse(&opensearchapi.DocumentDeleteByQueryResp{}); err != nil {
		t.Fatalf("absent document delete rejected: %v", err)
	}
	for _, response := range []*opensearchapi.DocumentDeleteByQueryResp{
		nil,
		{TimedOut: true},
		{VersionConflicts: 1},
		{Total: 1, Deleted: 0},
		{Total: 2, Deleted: 2},
		{Failures: []opensearchapi.BulkByScrollFailure{{}}},
	} {
		if err := validateDeleteResponse(response); err == nil {
			t.Fatalf("partial delete response accepted: %#v", response)
		}
	}
}

func TestProcessRejectsMissingOperationBeforeDependencies(t *testing.T) {
	outcome, err := ProcessMessage(
		context.Background(), Deps{Logf: func(string, ...any) {}},
		TopicCodeChunk, []byte(`{"id":"sensitive"}`),
		[]kafka.Header{{Key: "event_id", Value: []byte("11111111-1111-1111-1111-111111111111")}},
	)
	if err != nil || outcome != OutcomeInvalidSkipped {
		t.Fatalf("outcome=%v err=%v", outcome, err)
	}
	var _ Outcome = OutcomeDeleted
}

func TestEnsureIndexToleratesConcurrentCreation(t *testing.T) {
	var headCount atomic.Int32
	var createCount atomic.Int32
	bothHeads := make(chan struct{})
	var closeHeads sync.Once

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodHead && request.URL.Path == "/"+IndexName:
			if headCount.Add(1) == 2 {
				closeHeads.Do(func() { close(bothHeads) })
			}
			<-bothHeads
			response.WriteHeader(http.StatusNotFound)
		case request.Method == http.MethodPut && request.URL.Path == "/"+IndexName:
			if createCount.Add(1) == 1 {
				response.Header().Set("Content-Type", "application/json")
				_, _ = response.Write([]byte(`{"acknowledged":true,"shards_acknowledged":true,"index":"code_chunks"}`))
				return
			}
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusBadRequest)
			_, _ = response.Write([]byte(`{"error":{"type":"resource_already_exists_exception","reason":"index already exists","index":"code_chunks"},"status":400}`))
		case request.Method == http.MethodPut && request.URL.Path == "/"+IndexName+"/_mapping":
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write([]byte(`{"acknowledged":true}`))
		default:
			http.Error(response, fmt.Sprintf("unexpected request %s %s", request.Method, request.URL.Path), http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{server.URL}},
	})
	if err != nil {
		t.Fatal(err)
	}

	errors := make(chan error, 2)
	for range 2 {
		go func() { errors <- EnsureIndex(context.Background(), client) }()
	}
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatalf("concurrent EnsureIndex failed: %v", err)
		}
	}
	if got := createCount.Load(); got != 2 {
		t.Fatalf("create attempts = %d, want 2 to exercise already-exists race", got)
	}
}
