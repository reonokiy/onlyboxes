package runner

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/onlyboxes/onlyboxes/worker/worker-kubernetes/internal/config"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// Run against a dedicated kind namespace, never the caller's default cluster.
func TestKindSessionLifecycle(t *testing.T) {
	path := os.Getenv("ONLYBOXES_KIND_KUBECONFIG")
	if path == "" {
		t.Skip("kind integration is opt-in")
	}
	rc, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal("kind configuration unavailable")
	}
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		t.Fatal(err)
	}
	ns := os.Getenv("ONLYBOXES_KIND_NAMESPACE")
	if ns == "" {
		t.Fatal("test namespace required")
	}
	b := &podBackend{client: client, config: rc, namespace: ns, worker: "kind-contract"}
	cfg := config.Config{TerminalExecImage: "ghcr.io/astral-sh/uv:python3.12-bookworm-slim", TerminalExecMemoryLimit: "256Mi", TerminalExecCPULimit: "1", TerminalLeaseMinSec: 1, TerminalLeaseMaxSec: 60, TerminalLeaseDefaultSec: 10, TerminalOutputLimitBytes: 4096, TerminalSessionMaxInflight: 1}
	m := newSessionManager(b, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := m.execute(ctx, terminalExecRequest{Command: "printf retained > /workspace/state; printf success; printf diagnostic >&2; exit 7"})
	if err != nil {
		t.Fatalf("first exec failed: %v", err)
	}
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = b.remove(cleanup, sessionName(result.SessionID))
	}()
	if result.Stdout != "success" || result.Stderr != "diagnostic" || result.ExitCode != 7 || !result.Created {
		t.Fatal("terminal result contract mismatch")
	}
	again, err := m.execute(ctx, terminalExecRequest{SessionID: result.SessionID, Command: "cat /workspace/state"})
	if err != nil || again.Stdout != "retained" || again.Created {
		t.Fatal("session persistence failed")
	}
	file, err := m.resource(ctx, terminalResourceRequest{SessionID: result.SessionID, FilePath: "/workspace/state", Action: "read"})
	if err != nil || string(file.Blob) != "retained" || file.SizeBytes != 8 {
		t.Fatal("file read contract mismatch")
	}
	if _, err = m.resource(ctx, terminalResourceRequest{SessionID: result.SessionID, FilePath: "/workspace/missing", Action: "read"}); err == nil {
		t.Fatal("missing file was accepted")
	}
	large, err := m.execute(ctx, terminalExecRequest{SessionID: result.SessionID, Command: "python3 -c 'print(\"x\"*10000)'"})
	if err != nil || !large.StdoutTruncated || len(large.Stdout) != cfg.TerminalOutputLimitBytes {
		t.Fatal("bounded exec failed")
	}
	timeout, c := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = m.execute(timeout, terminalExecRequest{SessionID: result.SessionID, Command: "sleep 30"})
	c()
	if err == nil {
		t.Fatal("timeout did not cancel")
	}
	cleanup, c := context.WithTimeout(ctx, 10*time.Second)
	defer c()
	if _, err = b.owned(cleanup, sessionName(result.SessionID)); err == nil {
		t.Fatal("timed-out sandbox survived")
	}
}
