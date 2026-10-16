package runner

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSandboxSecurityAndOwnership(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	b := &podBackend{client: client, namespace: "sandbox", worker: "worker-a"}
	if err := b.create(ctx, "test", "runtime:test", "256Mi", "1"); err != nil {
		t.Fatal(err)
	}
	p, err := client.CoreV1().Pods("sandbox").Get(ctx, "test", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if *p.Spec.AutomountServiceAccountToken || !*p.Spec.SecurityContext.RunAsNonRoot || *p.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation || !*p.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("unsafe sandbox")
	}
	if p.Spec.HostNetwork || len(p.Spec.Volumes) != 2 || p.Spec.Containers[0].Resources.Limits.Memory().Value() != 256*1024*1024 {
		t.Fatal("invalid resource isolation")
	}
	other := &podBackend{client: client, namespace: "sandbox", worker: "worker-b"}
	if err := other.remove(ctx, "test"); err == nil {
		t.Fatal("cross-worker delete allowed")
	}
	if err := b.remove(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	if err := b.remove(ctx, "test"); err != nil {
		t.Fatal("delete is not idempotent")
	}
}
func TestReadyCanceledAndTerminated(t *testing.T) {
	client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed", Namespace: "sandbox", Labels: map[string]string{"app.kubernetes.io/name": "onlyboxes-sandbox", "onlyboxes.io/worker": "worker-a"}}, Status: corev1.PodStatus{Phase: corev1.PodFailed}})
	b := &podBackend{client: client, namespace: "sandbox", worker: "worker-a"}
	if err := b.ready(context.Background(), "failed"); err == nil {
		t.Fatal("terminated sandbox considered ready")
	}
}
func TestBoundedOutput(t *testing.T) {
	b := &boundedBuffer{limit: 3}
	n, err := b.Write([]byte("abcdef"))
	if n != 6 || err != nil || b.String() != "abc" || !b.truncated {
		t.Fatal("output limit broken")
	}
}
