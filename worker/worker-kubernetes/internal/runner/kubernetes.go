package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/client-go/util/exec"
)

// podBackend owns only sandboxes belonging to one registered OnlyBoxes worker.
// The deployment's RBAC must independently restrict it to a sandbox namespace.
type podBackend struct {
	client       kubernetes.Interface
	config       *rest.Config
	namespace    string
	worker       string
	runtimeClass string
}

func (b *podBackend) labels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": "onlyboxes-sandbox", "onlyboxes.io/worker": b.worker}
}

func (b *podBackend) create(ctx context.Context, name, image, memory, cpu string) error {
	mem, err := resource.ParseQuantity(memory)
	if err != nil {
		return fmt.Errorf("invalid memory limit: %w", err)
	}
	cpus, err := resource.ParseQuantity(cpu)
	if err != nil {
		return fmt.Errorf("invalid CPU limit: %w", err)
	}
	if mem.Sign() <= 0 || cpus.Sign() <= 0 {
		return errors.New("resource limits must be positive")
	}
	no := false
	uid := int64(1000)
	yes := true
	limits := corev1.ResourceList{corev1.ResourceMemory: mem, corev1.ResourceCPU: cpus, corev1.ResourceEphemeralStorage: resource.MustParse("2Gi")}
	requests := corev1.ResourceList{corev1.ResourceMemory: mem, corev1.ResourceCPU: cpus, corev1.ResourceEphemeralStorage: resource.MustParse("256Mi")}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.namespace, Labels: b.labels()},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &no,
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext:              &corev1.PodSecurityContext{RunAsUser: &uid, RunAsGroup: &uid, RunAsNonRoot: &yes, FSGroup: &uid, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{
				Name: "sandbox", Image: image, WorkingDir: "/workspace",
				Env:             []corev1.EnvVar{{Name: "HOME", Value: "/tmp"}, {Name: "UV_CACHE_DIR", Value: "/tmp/uv-cache"}},
				Command:         []string{"sh", "-c", "while true; do sleep 3600; done"},
				Resources:       corev1.ResourceRequirements{Requests: requests, Limits: limits},
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				VolumeMounts:    []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}, {Name: "tmp", MountPath: "/tmp"}},
			}},
			Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(1<<30, resource.BinarySI)}}}, {Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(1<<30, resource.BinarySI)}}}},
		},
	}
	if b.runtimeClass != "" {
		pod.Spec.RuntimeClassName = &b.runtimeClass
	}
	_, err = b.client.CoreV1().Pods(b.namespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func (b *podBackend) owned(ctx context.Context, name string) (*corev1.Pod, error) {
	pod, err := b.client.CoreV1().Pods(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	for key, value := range b.labels() {
		if pod.Labels[key] != value {
			return nil, errors.New("sandbox ownership mismatch")
		}
	}
	return pod, nil
}

func (b *podBackend) ready(ctx context.Context, name string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		pod, err := b.owned(ctx, name)
		if err != nil {
			return err
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return errors.New("sandbox is terminated")
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *podBackend) remove(ctx context.Context, name string) error {
	pod, err := b.owned(ctx, name)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	zero := int64(0)
	uid := types.UID(pod.UID)
	return b.client.CoreV1().Pods(b.namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero, Preconditions: &metav1.Preconditions{UID: &uid}})
}

func (b *podBackend) exec(ctx context.Context, name string, command []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	if _, err := b.owned(ctx, name); err != nil {
		return 0, err
	}
	request := b.client.CoreV1().RESTClient().Post().Resource("pods").Name(name).Namespace(b.namespace).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: "sandbox", Command: command, Stdin: stdin != nil, Stdout: stdout != nil, Stderr: stderr != nil}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(b.config, "POST", request.URL())
	if err != nil {
		return 0, err
	}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: stdin, Stdout: stdout, Stderr: stderr})
	if ctx.Err() != nil {
		// Closing an exec stream does not guarantee remote process termination.
		// Retire the sandbox so a timed-out command cannot keep running.
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cleanupErr := b.remove(cleanup, name); cleanupErr != nil {
			return 0, errors.Join(ctx.Err(), fmt.Errorf("sandbox cleanup failed: %w", cleanupErr))
		}
		return 0, ctx.Err()
	}
	var exit utilexec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitStatus(), nil
	}
	return 0, err
}

type boundedBuffer struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := b.limit - b.Len()
	if remaining < len(p) {
		b.truncated = true
		if remaining < 0 {
			remaining = 0
		}
		p = p[:remaining]
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}
