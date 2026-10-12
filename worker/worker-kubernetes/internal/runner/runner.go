package runner

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	registryv1 "github.com/onlyboxes/onlyboxes/api/gen/go/registry/v1"
	"github.com/onlyboxes/onlyboxes/worker/worker-kubernetes/internal/config"
	"github.com/onlyboxes/onlyboxes/worker/worker-kubernetes/internal/logging"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	minHeartbeatInterval         = time.Second
	initialReconnectDelay        = time.Second
	maxReconnectDelay            = 15 * time.Second
	maxProtocolInt32             = int64(1<<31 - 1)
	echoCapabilityName           = "echo"
	pythonExecCapabilityName     = "pythonexec"
	pythonExecCapabilityDeclared = "pythonExec"
)

var waitReconnect = waitReconnectDelay
var applyJitter = jitterDuration
var runPythonExec func(context.Context, string) (pythonExecRunResult, error)
var runTerminalExec = runTerminalExecUnavailable
var runTerminalLeaseRenew = runTerminalLeaseRenewUnavailable
var runTerminalResource = runTerminalResourceUnavailable
var activeSessionCountFn = func() int32 { return 0 }
var recoverTerminalSessionsFn func(context.Context, []*registryv1.TerminalSessionRecoveryCandidate) ([]*registryv1.TerminalSessionRecoveryResult, error)

func validateTerminalMaxActiveSessions(n int) error {
	if n < 0 || int64(n) > maxProtocolInt32 {
		return errors.New("invalid maximum active sessions")
	}
	return nil
}

func Run(ctx context.Context, cfg config.Config) error {
	if strings.TrimSpace(cfg.WorkerID) == "" || strings.TrimSpace(cfg.WorkerSecret) == "" {
		return errors.New("WORKER_ID and WORKER_SECRET are required")
	}
	if cfg.ProxyEnabled {
		return errors.New("Kubernetes public preview is not configured; disable WORKER_PROXY_ENABLED")
	}
	if err := validateTerminalMaxActiveSessions(cfg.TerminalMaxActiveSessions); err != nil {
		return err
	}
	var rc *rest.Config
	var err error
	if path := os.Getenv("KUBECONFIG"); path != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", path)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return errors.New("Kubernetes client configuration unavailable")
	}
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return err
	}
	ns := os.Getenv("WORKER_KUBERNETES_NAMESPACE")
	if ns == "" {
		return errors.New("WORKER_KUBERNETES_NAMESPACE is required")
	}
	backend := &podBackend{client: client, config: rc, namespace: ns, worker: cfg.WorkerID, runtimeClass: os.Getenv("WORKER_KUBERNETES_RUNTIME_CLASS")}
	manager := newSessionManager(backend, cfg)
	runPythonExec = manager.python
	runTerminalExec = manager.execute
	runTerminalLeaseRenew = manager.renew
	runTerminalResource = manager.resource
	activeSessionCountFn = manager.count
	recoverTerminalSessionsFn = manager.recover
	go manager.janitor(ctx)
	delay := initialReconnectDelay
	for ctx.Err() == nil {
		err := runSession(ctx, cfg)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		logging.Warnf("registry connection interrupted; reconnecting")
		if err := waitReconnect(ctx, delay); err != nil {
			return err
		}
		delay = nextReconnectDelay(delay)
	}
	return ctx.Err()
}
