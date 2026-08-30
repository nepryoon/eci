package rerankclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthUsesNativeTEIHealthWithoutInference(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || request.URL.Path != "/health" {
					t.Fatalf("request = %s %s, want GET /health", request.Method, request.URL.Path)
				}
				response.WriteHeader(status)
			}))
			defer server.Close()
			err := New(server.URL).Health(context.Background())
			if status == http.StatusOK && err != nil {
				t.Fatalf("healthy TEI failed: %v", err)
			}
			if status != http.StatusOK && err == nil {
				t.Fatal("unhealthy TEI reported ready")
			}
		})
	}
}
