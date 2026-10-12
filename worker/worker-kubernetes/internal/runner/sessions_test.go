package runner

import (
	"context"
	"testing"
	"time"

	registryv1 "github.com/onlyboxes/onlyboxes/api/gen/go/registry/v1"
	"github.com/onlyboxes/onlyboxes/worker/worker-kubernetes/internal/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestRecoveryLeaseAndCleanup(t *testing.T) {
	ctx := context.Background()
	id := "fixture-session"
	labels := map[string]string{"app.kubernetes.io/name": "onlyboxes-sandbox", "onlyboxes.io/worker": "worker-a"}
	pod := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "sandbox", Labels: labels}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	}
	client := fake.NewClientset(pod(sessionName(id)), pod("orphan"))
	b := &podBackend{client: client, namespace: "sandbox", worker: "worker-a"}
	m := newSessionManager(b, config.Config{TerminalLeaseMinSec: 1, TerminalLeaseDefaultSec: 2, TerminalLeaseMaxSec: 30, TerminalSessionMaxInflight: 1, TerminalMaxActiveSessions: 1})
	results, recoveryErr := m.recover(ctx, []*registryv1.TerminalSessionRecoveryCandidate{{SessionId: id, LeaseExpiresUnixMs: time.Now().Add(5 * time.Second).UnixMilli()}})
	if recoveryErr != nil || len(results) != 1 || results[0].Status != registryv1.TerminalSessionRecoveryResult_RECOVERED {
		t.Fatal("recovery failed")
	}
	if _, err := client.CoreV1().Pods("sandbox").Get(ctx, "orphan", metav1.GetOptions{}); err == nil {
		t.Fatal("orphan survived")
	}
	s, created, err := m.acquire(ctx, id, false, nil)
	if err != nil || created {
		t.Fatal("recovered session unavailable")
	}
	if _, _, err := m.acquire(ctx, id, false, nil); err == nil {
		t.Fatal("concurrency limit missing")
	}
	if _, _, err := m.acquire(ctx, "second", true, nil); err == nil {
		t.Fatal("capacity limit missing")
	}
	m.release(s)
	before := s.expires
	lease, err := m.renew(ctx, terminalLeaseRenewPayload{SessionID: id, LeaseTTLSec: 20})
	if err != nil || lease.LeaseExpiresUnixMS <= before.UnixMilli() {
		t.Fatal("renewal failed")
	}
	m.mu.Lock()
	s.expires = time.Now().Add(-time.Second)
	m.mu.Unlock()
	m.sweep(ctx)
	if m.count() != 0 {
		t.Fatal("expired session retained")
	}
	if _, err := client.CoreV1().Pods("sandbox").Get(ctx, s.name, metav1.GetOptions{}); err == nil {
		t.Fatal("expired pod retained")
	}
}

func TestRecoveryAPIFailurePreservesPods(t *testing.T) {
	id := "recovery-fixture"
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: sessionName(id), Namespace: "sandbox", Labels: map[string]string{"app.kubernetes.io/name": "onlyboxes-sandbox", "onlyboxes.io/worker": "worker-a"}}}
	client := fake.NewClientset(pod)
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("fixture API outage")
	})
	m := newSessionManager(&podBackend{client: client, namespace: "sandbox", worker: "worker-a"}, config.Config{})
	results, err := m.recover(context.Background(), []*registryv1.TerminalSessionRecoveryCandidate{{SessionId: id, LeaseExpiresUnixMs: time.Now().Add(time.Minute).UnixMilli()}})
	if err == nil || results != nil {
		t.Fatal("API outage was reported as a lost session")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("API outage caused destructive cleanup")
		}
	}
}

func TestResourceClaimPreservesLeaseAndRejectsInvalidTTL(t *testing.T) {
	client := fake.NewClientset()
	m := newSessionManager(&podBackend{client: client}, config.Config{TerminalLeaseMinSec: 1, TerminalLeaseDefaultSec: 30, TerminalLeaseMaxSec: 60, TerminalSessionMaxInflight: 1})
	ready := make(chan struct{})
	close(ready)
	expires := time.Now().Add(5 * time.Second)
	s := &session{id: "fixture", name: sessionName("fixture"), expires: expires, ready: ready}
	m.sessions[s.id] = s
	claimed, _, err := m.claim(context.Background(), s.id, false, nil, false)
	if err != nil || !claimed.expires.Equal(expires) {
		t.Fatal("file access extended session lease")
	}
	m.release(claimed)
	invalid := 0
	if _, _, err := m.acquire(context.Background(), "new", true, &invalid); err == nil {
		t.Fatal("invalid lease accepted")
	}
	if len(client.Actions()) != 0 || len(m.sessions) != 1 {
		t.Fatal("invalid lease created a sandbox")
	}
}
