# Development validation

Validated locally on 2026-10-12 against upstream revision `364e521` (0.14.0),
using Go 1.27.1, Docker 26.1.5, kind 0.31.0 and Kubernetes 1.35.0.

## Checks passed

- Kubernetes Worker: `go test -race -p 2 ./...` and `go vet ./...`.
- Existing Docker Worker and Console: `go test -p 2 ./...` in each module.
- Python syntax, shell syntax, Go formatting and `git diff --check`.
- Actual kind deployment of the development Console and Kubernetes Worker,
  with namespace-scoped RBAC, restricted Pod Security and a persistent test
  database volume.
- REST echo, terminal stdout/stderr and nonzero exit codes, persistent session
  files, Python tasks and file-resource tasks.
- MCP discovery, echo, Python, terminal execution and image reads.
- Streamed file export, upload-header forwarding, and MCP `exportFile` through
  Console's S3 signer to a local upload fixture; uploaded bytes are checked by
  SHA-256.
- Worker restart and Console restart both recover the existing session Pod and
  retain its files.
- Expired sessions return REST 404; timed-out commands return REST 504 and their
  Pods are removed. No test sandbox Pods remain after the suite.

The complete kind suite ends with `ALL KIND CONTRACT TESTS PASSED` in
`scripts/.dev/kind-test.log`. Reproduce it through the commands in
[`scripts/README.md`](../../../scripts/README.md#kind-development-contract-tests).

## Scope

The S3 fixture checks signing integration, transfer and interface behavior; it
does not emulate AWS authorization or validate signatures. No external S3/B2
credentials or production cluster resources are used. The default kind CNI does
not enforce NetworkPolicy, so these tests do not prove network isolation.
Public preview proxy support is not implemented by the Kubernetes Worker.
