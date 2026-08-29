package edge

import (
	"bytes"
	"crypto/sha256"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvoyConfigurationPreservesSecurityFilterOrderAndStrictness(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	contents, err := os.ReadFile(filepath.Join(root, "deploy", "envoy", "envoy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	config := string(contents)
	ordered := []string{
		"envoy.filters.http.header_mutation",
		"eci.filters.http.pre_auth_local_ratelimit",
		"envoy.filters.http.ext_authz",
		"envoy.filters.http.header_mutation",
		"envoy.filters.http.local_ratelimit",
		"envoy.filters.http.header_mutation",
		"envoy.filters.http.buffer",
		"envoy.filters.http.grpc_json_transcoder",
		"envoy.filters.http.router",
	}
	position := -1
	for _, filter := range ordered {
		next := strings.Index(config[position+1:], "- name: "+filter)
		if next < 0 {
			t.Fatalf("missing ordered filter %q", filter)
		}
		position += next + 1
	}
	for _, required := range []string{
		"remove: eci-security-context-bin",
		"remove: traceparent",
		"remove: tracestate",
		"remove: baggage",
		"remove: authorization",
		"remove: x-eci-rate-limit-key",
		"header_name: x-eci-rate-limit-key",
		"descriptor_key: authenticated_caller",
		"key: authenticated_caller",
		"max_dynamic_descriptors: 10000",
		"stat_prefix: eci_edge_pre_auth",
		"failure_mode_allow: false",
		"max_request_bytes: 1048576",
		"reject_unknown_method: true",
		"ignore_unknown_query_parameters: false",
		"eci.retrieval.v1.RetrievalEngine",
		`regex: ^/eci\.retrieval\.v1\.RetrievalEngine/(HybridSearch|GetNode|ExpandNeighbors|ImpactAnalysis)$`,
		"key: retry-after",
		"address: 127.0.0.1",
		"timeout: 0s",
		"idle_timeout: 35s",
		"name: envoy.transport_sockets.tls",
		`alpn_protocols: ["h2", "http/1.1"]`,
		"tls_minimum_protocol_version: TLSv1_2",
		"filename: /etc/envoy/tls/tls.crt",
		"filename: /etc/envoy/tls/tls.key",
	} {
		if !strings.Contains(config, required) {
			t.Errorf("missing strict config fragment %q", required)
		}
	}
	if strings.Contains(config, "failure_mode_allow: true") || strings.Contains(config, "match_incoming_request_route: true") {
		t.Fatal("gateway contains fail-open or route-cache-sensitive configuration")
	}
	if strings.Contains(config, "prefix: /eci.retrieval.v1.RetrievalEngine/") {
		t.Fatal("native gRPC route is a prefix rather than a method allow-list")
	}
}

func TestEnvoyDescriptorIsDeterministicallyGenerated(t *testing.T) {
	if _, err := exec.LookPath("buf"); err != nil {
		t.Skip("buf is installed in the deterministic CI guard job")
	}
	root := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	checkedIn, err := os.ReadFile(filepath.Join(root, "deploy", "envoy", "retrieval.pb"))
	if err != nil {
		t.Fatal(err)
	}
	generated := filepath.Join(t.TempDir(), "retrieval.pb")
	command := exec.Command("buf", "build", "contracts", "--as-file-descriptor-set", "-o", generated)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("buf build: %v: %s", err, output)
	}
	actual, err := os.ReadFile(generated)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(checkedIn, actual) {
		t.Fatalf("descriptor differs: checked-in=%x generated=%x", sha256.Sum256(checkedIn), sha256.Sum256(actual))
	}
}
