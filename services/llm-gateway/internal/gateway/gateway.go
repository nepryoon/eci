package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"
)

const maxBody = 4 << 20

type Route struct {
	Upstream *url.URL
	Model    string
}
type Config struct {
	Routes           map[string]Route
	DefaultRoute     Route
	Timeout          time.Duration
	FailureThreshold int
	OpenDuration     time.Duration
}
type breaker struct {
	mu       sync.Mutex
	failures int
	opened   time.Time
	probe    bool
}
type handler struct {
	cfg      Config
	client   *http.Client
	breakers map[string]*breaker
}

func NewHandler(cfg Config, client *http.Client) (http.Handler, error) {
	if cfg.Timeout <= 0 || cfg.FailureThreshold <= 0 || cfg.OpenDuration <= 0 {
		return nil, errors.New("timeout, failure threshold and open duration must be positive")
	}
	if client == nil {
		client = http.DefaultClient
	}
	bs := map[string]*breaker{}
	validate := func(key string, r Route) error {
		if r.Upstream == nil || r.Upstream.Scheme == "" || r.Upstream.Host == "" || r.Model == "" {
			return fmt.Errorf("invalid route %q", key)
		}
		bs[key] = &breaker{}
		return nil
	}
	for k, r := range cfg.Routes {
		if err := validate(k, r); err != nil {
			return nil, err
		}
	}
	if cfg.DefaultRoute.Upstream != nil {
		if err := validate("__default__", cfg.DefaultRoute); err != nil {
			return nil, err
		}
	}
	return &handler{cfg: cfg, client: client, breakers: bs}, nil
}
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		w.WriteHeader(200)
		return
	}
	if r.URL.Path != "/v1/chat/completions" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(405)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	if len(body) > maxBody {
		http.Error(w, "body too large", 413)
		return
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	alias, _ := payload["model"].(string)
	if alias == "" {
		http.Error(w, "model required", 400)
		return
	}
	route, ok := h.cfg.Routes[alias]
	key := alias
	if !ok {
		route = h.cfg.DefaultRoute
		key = "__default__"
		if route.Upstream == nil {
			http.Error(w, "unknown model", 400)
			return
		}
	}
	b := h.breakers[key]
	if !b.allow(h.cfg.OpenDuration) {
		http.Error(w, "circuit open", 503)
		return
	}
	payload["model"] = route.Model
	encoded, _ := json.Marshal(payload)
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
	defer cancel()
	target := *route.Upstream
	target.Path = path.Join(target.Path, "/v1/chat/completions")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(encoded))
	if err != nil {
		b.failure(h.cfg.FailureThreshold)
		http.Error(w, "upstream request", 502)
		return
	}
	req.Header = r.Header.Clone()
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		b.failure(h.cfg.FailureThreshold)
		http.Error(w, "upstream timeout", 504)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		b.failure(h.cfg.FailureThreshold)
	} else if resp.StatusCode < 400 {
		b.success()
	}
	for k, v := range resp.Header {
		for _, x := range v {
			w.Header().Add(k, x)
		}
	}
	w.Header().Set("X-ECI-Upstream-Model", route.Model)
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, e := resp.Body.Read(buf)
		if n > 0 {
			_, _ = w.Write(buf[:n])
			if flusher != nil {
				flusher.Flush()
			}
		}
		if e != nil {
			break
		}
	}
}
func (b *breaker) allow(open time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.opened.IsZero() {
		return true
	}
	if time.Since(b.opened) < open {
		return false
	}
	if b.probe {
		return false
	}
	b.probe = true
	return true
}
func (b *breaker) failure(threshold int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	wasProbe := b.probe
	b.probe = false
	b.failures++
	if wasProbe || (b.failures >= threshold && b.opened.IsZero()) {
		b.opened = time.Now()
	}
}
func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.opened = time.Time{}
	b.probe = false
}
