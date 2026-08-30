package consumer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

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
