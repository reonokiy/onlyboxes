# Kubernetes Worker

`worker-kubernetes` registers with the existing Console gRPC protocol and creates
one Kubernetes Pod per terminal session. Python tasks use separate short-lived
Pods. Terminal files survive calls in the same session and disappear when the Pod
is removed. No Docker socket or host filesystem mount is required.

Build the image from the repository root:

```sh
docker build -f worker/worker-kubernetes/Dockerfile -t onlyboxes-worker-kubernetes:dev .
```

The deployment example in `deploy/worker.yaml` confines RBAC to its sandbox
namespace. Create a normal Worker through Console and deliver its registration
to `onlyboxes-worker-registration`, with keys `worker-id` and `worker-secret`,
through your secret manager. Console gRPC requires TLS by default. For an isolated
local development cluster only, set `WORKER_CONSOLE_INSECURE=true`.

## Configuration

Common heartbeat, logging, lease, concurrency and resource settings match the
Docker Worker. Kubernetes-specific settings are:

| Variable | Purpose |
| --- | --- |
| `WORKER_KUBERNETES_NAMESPACE` | Required sandbox namespace |
| `WORKER_KUBERNETES_RUNTIME_CLASS` | Optional sandbox RuntimeClass |
| `KUBECONFIG` | Optional local Kubernetes client configuration; otherwise in-cluster authentication |
| `WORKER_TERMINAL_EXEC_KUBERNETES_IMAGE` | Terminal runtime image |
| `WORKER_PYTHON_EXEC_KUBERNETES_IMAGE` | Python runtime image |

Use one active replica per Worker identity. Separate replicas must have separate
registrations. The Console owns session IDs and lease recovery candidates; the
Worker reconnects and recovers surviving Pods after restart, and removes its
orphan Pods. Kubernetes names are hashes of session IDs.

## Isolation and limits

Sandbox Pods run as UID 1000 with no ServiceAccount token, dropped capabilities,
RuntimeDefault seccomp, a read-only root filesystem and no privilege escalation.
CPU and memory requests equal limits; ephemeral storage has a 256 MiB request and
2 GiB limit. Workspace and temporary volumes each have a 1 GiB size limit. A timed-out
exec retires its entire Pod, because closing Kubernetes exec alone cannot guarantee
termination of remote processes. Other commands in that session are interrupted.

The example NetworkPolicy permits DNS and public HTTP/HTTPS while excluding common
private and metadata networks. Adjust exclusions for your cluster and host address
ranges. Enforcement requires a NetworkPolicy-capable CNI; kind's default CNI does
not establish proof of network isolation. Ordinary Pods share the node kernel;
use an appropriate RuntimeClass and dedicated nodes for untrusted execution.

The Pod janitor retries failed deletions. Lease expiry is enforced while the Worker
is running; after an outage, recovery cleans expired and orphan sandboxes. File
exports stream without buffering the full file; set
`WORKER_TERMINAL_EXPORT_MAX_BYTES` to enforce an export size limit. Public preview proxy and
Docker PID-limit settings are not implemented by this backend.

## Tests

Run `go test -race ./...` in this module. The opt-in real Kubernetes test requires
`ONLYBOXES_KIND_KUBECONFIG` and `ONLYBOXES_KIND_NAMESPACE` pointing to a dedicated
local kind cluster and test namespace. It verifies command streams and exit codes,
session persistence, file reads and timeout cleanup.

## Fork container image

The fork's `Kubernetes image` workflow publishes Console and Worker together as
`ghcr.io/reonokiy/onlyboxes:kubernetes-<git-sha>`. OCI source metadata links the
package to this repository. The default entrypoint is `/worker`; use command
`/console` for the control service. Both run as UID 1000 and require no shell.

Build it with `docker build -f docker/kubernetes.dockerfile .`. Each build runs
the Console and Kubernetes Worker module tests. Pin the published image digest
in production. The package must allow public anonymous pulls before deploying
without a registry credential; GitHub package visibility is configured in its
package settings.
