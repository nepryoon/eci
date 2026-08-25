package gateway

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func route(raw, model string) Route { u, _ := url.Parse(raw); return Route{Upstream: u, Model: model} }
func request(h http.Handler, body string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	return r
}
func TestRoutingRewriteAndResponse(t *testing.T) {
	var got map[string]any
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer u.Close()
	h, _ := NewHandler(Config{Routes: map[string]Route{"code": route(u.URL, "qwen")}, Timeout: time.Second, FailureThreshold: 2, OpenDuration: time.Second}, u.Client())
	r := request(h, `{"model":"code"}`)
	if r.Code != 201 || got["model"] != "qwen" || r.Header().Get("X-ECI-Upstream-Model") != "qwen" {
		t.Fatalf("%d %v %v", r.Code, got, r.Header())
	}
}
func TestInvalidAndDefault(t *testing.T) {
	h, _ := NewHandler(Config{Timeout: time.Second, FailureThreshold: 2, OpenDuration: time.Second}, http.DefaultClient)
	if request(h, `{"model":"x"}`).Code != 400 || request(h, `bad`).Code != 400 {
		t.Fatal("invalid accepted")
	}
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil))
	if r.Code != 405 {
		t.Fatal(r.Code)
	}
}
func TestFourXXDoesNotOpenBreaker(t *testing.T) {
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "bad", 422) }))
	defer u.Close()
	h, _ := NewHandler(Config{DefaultRoute: route(u.URL, "m"), Timeout: time.Second, FailureThreshold: 1, OpenDuration: time.Hour}, u.Client())
	if request(h, `{"model":"x"}`).Code != 422 || request(h, `{"model":"x"}`).Code != 422 {
		t.Fatal("4xx opened breaker")
	}
}
func TestStreamingFlush(t *testing.T) {
	release := make(chan struct{})
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := w.(http.Flusher)
		w.Write([]byte("data: one\n\n"))
		f.Flush()
		<-release
		w.Write([]byte("data: two\n\n"))
		f.Flush()
	}))
	defer u.Close()
	h, _ := NewHandler(Config{DefaultRoute: route(u.URL, "m"), Timeout: time.Second, FailureThreshold: 2, OpenDuration: time.Second}, u.Client())
	s := httptest.NewServer(h)
	defer s.Close()
	resp, e := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","stream":true}`))
	if e != nil {
		t.Fatal(e)
	}
	rd := bufio.NewReader(resp.Body)
	line, _ := rd.ReadString('\n')
	if line != "data: one\n" {
		t.Fatal(line)
	}
	close(release)
	rest, _ := rd.ReadString(0)
	if !strings.Contains(rest, "data: two") {
		t.Fatal(rest)
	}
}
func TestBreakerRecovery(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, "x", 500)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer u.Close()
	h, _ := NewHandler(Config{DefaultRoute: route(u.URL, "m"), Timeout: time.Second, FailureThreshold: 2, OpenDuration: 20 * time.Millisecond}, u.Client())
	call := func() int { return request(h, `{"model":"m"}`).Code }
	if call() != 500 || call() != 500 || call() != 503 {
		t.Fatal("not open")
	}
	time.Sleep(25 * time.Millisecond)
	fail.Store(false)
	if call() != 200 || call() != 200 {
		t.Fatal("not closed")
	}
}
func TestFailedHalfOpenProbeReopensCircuit(t *testing.T) {
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "x", 500) }))
	defer u.Close()
	h, _ := NewHandler(Config{DefaultRoute: route(u.URL, "m"), Timeout: time.Second, FailureThreshold: 1, OpenDuration: 10 * time.Millisecond}, u.Client())
	if request(h, `{"model":"m"}`).Code != 500 || request(h, `{"model":"m"}`).Code != 503 {
		t.Fatal("not open")
	}
	time.Sleep(15 * time.Millisecond)
	if request(h, `{"model":"m"}`).Code != 500 || request(h, `{"model":"m"}`).Code != 503 {
		t.Fatal("failed probe did not reopen")
	}
}
func TestFourXXHalfOpenProbeClosesCircuit(t *testing.T) {
	var status atomic.Int64
	status.Store(500)
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "x", int(status.Load()))
	}))
	defer u.Close()
	h, _ := NewHandler(Config{DefaultRoute: route(u.URL, "m"), Timeout: time.Second, FailureThreshold: 1, OpenDuration: 10 * time.Millisecond}, u.Client())
	if request(h, `{"model":"m"}`).Code != 500 || request(h, `{"model":"m"}`).Code != 503 {
		t.Fatal("not open")
	}
	time.Sleep(15 * time.Millisecond)
	status.Store(422)
	if request(h, `{"model":"m"}`).Code != 422 || request(h, `{"model":"m"}`).Code != 422 {
		t.Fatal("4xx half-open probe did not close circuit")
	}
}
func TestTimeoutHealthAndLimit(t *testing.T) {
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer u.Close()
	h, _ := NewHandler(Config{DefaultRoute: route(u.URL, "m"), Timeout: 10 * time.Millisecond, FailureThreshold: 2, OpenDuration: time.Second}, u.Client())
	if request(h, `{"model":"m"}`).Code != 504 {
		t.Fatal("timeout")
	}
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if r.Code != 200 {
		t.Fatal("health")
	}
	huge := bytes.Repeat([]byte("x"), (4<<20)+1)
	r = httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(huge)))
	if r.Code != 413 {
		t.Fatal(r.Code)
	}
}
