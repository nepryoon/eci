package authz

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const opaImage = "openpolicyagent/opa:1.20.1"

func TestProductionClientAgainstRealPinnedOPA(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	policyFile, err := filepath.Abs("../../../../deploy/compose/opa/policies/eci_authz.rego")
	if err != nil {
		t.Fatalf("policy path: %v", err)
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        opaImage,
			ExposedPorts: []string{"8181/tcp"},
			Cmd:          []string{"run", "--server", "--addr=0.0.0.0:8181", "/policies"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      policyFile,
				ContainerFilePath: "/policies/eci_authz.rego",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForHTTP("/health").
				WithPort("8181/tcp").
				WithStatusCodeMatcher(func(code int) bool { return code == 200 }).
				WithStartupTimeout(time.Minute),
		},
	})
	if err != nil {
		t.Fatalf("start OPA %s: %v", opaImage, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("terminate OPA: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("OPA host: %v", err)
	}
	port, err := container.MappedPort(ctx, "8181/tcp")
	if err != nil {
		t.Fatalf("OPA port: %v", err)
	}
	client, err := New(ctx, validConfig(fmt.Sprintf("http://%s:%s", host, port.Port())), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New against OPA: %v", err)
	}

	allowed, err := client.Decide(ctx, validSubject(), getNodeMethod)
	if err != nil {
		t.Fatalf("allowed decision: %v", err)
	}
	if !allowed.Allow || allowed.Reason != "allow" {
		t.Fatalf("allowed decision = %+v", allowed)
	}

	emptyScope := validSubject()
	emptyScope.AllowedRepos = nil
	denied, err := client.Decide(ctx, emptyScope, getNodeMethod)
	if err != nil {
		t.Fatalf("denied decision: %v", err)
	}
	if denied.Allow || denied.Reason != "empty_repo_scope" {
		t.Fatalf("denied decision = %+v", denied)
	}
}
