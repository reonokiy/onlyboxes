#!/usr/bin/env python3
"""Build and verify the development Console/Worker in a dedicated kind cluster.

Credentials are generated per run, sent directly to Kubernetes over stdin, and
kept in process memory. API responses and subprocess diagnostics are never logged.
"""
import argparse
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
CLUSTER = "onlyboxes-dev"
NS = "onlyboxes-kind-dev"
IMAGE = "onlyboxes-kubernetes:dev"


def run(command, *, data=None, cwd=None, env=None):
    result = subprocess.run(command, input=data, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, cwd=cwd, env=env)
    if result.returncode:
        raise RuntimeError(f"{Path(command[0]).name} failed with exit {result.returncode}")
    return result.stdout


def wait(check, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (urllib.error.URLError, RuntimeError):
            pass
        time.sleep(0.5)
    raise RuntimeError("condition did not become ready")


class Harness:
    def __init__(self, kubeconfig):
        self.kubeconfig = kubeconfig
        self.kubectl = ["kubectl", "--kubeconfig", kubeconfig,
                        "--context", f"kind-{CLUSTER}"]
        self.cookies = http.cookiejar.CookieJar()
        self.http = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies))
        self.token = None
        self.forward = None
        self.mcp_session = None
        self.upload_endpoint = None

    def apply(self, *objects):
        for obj in objects:
            if obj.get("kind") == "Secret":
                run(self.kubectl + ["create", "-f", "-"], data=json.dumps(obj).encode())
        objects = [obj for obj in objects if obj.get("kind") != "Secret"]
        if not objects:
            return
        run(self.kubectl + ["apply", "-f", "-"], data=json.dumps(
            {"apiVersion": "v1", "kind": "List", "items": objects}).encode())

    def request(self, path, body=None, execution=False):
        headers = {"Content-Type": "application/json"}
        if execution:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request("http://127.0.0.1:18089" + path,
                                         data=None if body is None else json.dumps(body).encode(),
                                         headers=headers)
        try:
            with self.http.open(request, timeout=75) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            # Neither response bodies nor credential-bearing URLs enter logs.
            raise RuntimeError(f"API {path.split('?')[0]} HTTP {error.code}") from None

    def mcp(self, method, params, ident=1):
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                   "Authorization": "Bearer " + self.token}
        if self.mcp_session:
            headers["Mcp-Session-Id"] = self.mcp_session
        message = {"jsonrpc": "2.0", "method": method, "params": params}
        if ident is not None:
            message["id"] = ident
        request = urllib.request.Request("http://127.0.0.1:18089/mcp", data=json.dumps(message).encode(), headers=headers)
        with self.http.open(request, timeout=75) as response:
            self.mcp_session = response.headers.get("Mcp-Session-Id", self.mcp_session)
            body = response.read().decode()
            if not body:
                return None
            if response.headers.get("Content-Type", "").startswith("text/event-stream"):
                body = next(line[5:].strip() for line in body.splitlines() if line.startswith("data:"))
            result = json.loads(body)
            assert "error" not in result
            return result.get("result")

    def deployment(self, name, command, env, *, service_account=None):
        container = {"name": name, "image": IMAGE, "imagePullPolicy": "Never",
                     "command": command if isinstance(command, list) else [command], "env": env,
                     "securityContext": {"allowPrivilegeEscalation": False,
                                         "capabilities": {"drop": ["ALL"]}},
                     "resources": {"requests": {"cpu": "100m", "memory": "128Mi"},
                                   "limits": {"cpu": "1", "memory": "512Mi"}}}
        spec = {"securityContext": {"runAsUser": 1000, "runAsGroup": 1000,
                                    "runAsNonRoot": True, "fsGroup": 1000,
                                    "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [container]}
        if service_account:
            spec["serviceAccountName"] = service_account
        else:
            spec["automountServiceAccountToken"] = False
        if name == "upload":
            container["readinessProbe"] = {"httpGet": {"path": "/", "port": 8080}, "periodSeconds": 1}
        if name == "console":
            spec["volumes"] = [{"name": "db", "persistentVolumeClaim": {"claimName": "console-db"}}]
            container["volumeMounts"] = [{"name": "db", "mountPath": "/db"}]
        return {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name, "namespace": NS},
                "spec": {"replicas": 1, "strategy": {"type": "Recreate"},
                         "selector": {"matchLabels": {"app": name}},
                         "template": {"metadata": {"labels": {"app": name}}, "spec": spec}}}

    @staticmethod
    def env(values):
        return [{"name": key, "value": value} for key, value in values.items()]

    @staticmethod
    def secret_env(secret, fields):
        return [{"name": key, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}} for key in fields]

    def setup(self):
        existing = run(self.kubectl + ["get", "namespace", NS, "--ignore-not-found", "-o", "name"])
        if existing.strip():
            # This fixed namespace is exclusively owned by this development harness.
            run(self.kubectl + ["delete", "namespace", NS, "--wait=true", "--timeout=90s"])
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS,
                    "labels": {"pod-security.kubernetes.io/enforce": "restricted"}}})
        self.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "upload", "namespace": NS},
                    "spec": {"selector": {"app": "upload"}, "ports": [{"port": 8080}]}})
        upload_ip = run(self.kubectl + ["get", "service", "upload", "-n", NS, "-o", "jsonpath={.spec.clusterIP}"]).decode()
        self.upload_endpoint = "http://" + upload_ip + ":8080"
        # Use an explicit test PV so the harness is independent of kind's
        # optional local-path provisioner and its helper-Pod security policy.
        run(self.kubectl + ["delete", "pv", "onlyboxes-kind-db", "--ignore-not-found", "--wait=true"])
        run(["docker", "exec", f"{CLUSTER}-control-plane", "mkdir", "-p", "/var/local/onlyboxes-kind-db"])
        run(["docker", "exec", f"{CLUSTER}-control-plane", "chown", "1000:1000", "/var/local/onlyboxes-kind-db"])
        # Each test run starts with its own database directory, while Console
        # rollouts during that run retain the same volume.
        database = "/db/console-" + secrets.token_hex(8) + ".db"
        password = secrets.token_urlsafe(32)
        bootstrap = {"CONSOLE_DASHBOARD_PASSWORD": password, "CONSOLE_HASH_KEY": secrets.token_hex(32),
                     "CONSOLE_EXPORT_FILE_AK": secrets.token_hex(16), "CONSOLE_EXPORT_FILE_SK": secrets.token_urlsafe(32)}
        self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "console-bootstrap", "namespace": NS},
                    "stringData": bootstrap},
                   {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "console-db", "namespace": NS},
                    "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "", "volumeName": "onlyboxes-kind-db", "resources": {"requests": {"storage": "128Mi"}}}},
                   {"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": "onlyboxes-kind-db"},
                    "spec": {"capacity": {"storage": "128Mi"}, "accessModes": ["ReadWriteOnce"], "storageClassName": "",
                             "persistentVolumeReclaimPolicy": "Retain", "hostPath": {"path": "/var/local/onlyboxes-kind-db", "type": "Directory"}}},
                   {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "console", "namespace": NS},
                    "spec": {"selector": {"app": "console"}, "ports": [{"name": "http", "port": 8089}, {"name": "grpc", "port": 50051}]}},
                   self.deployment("console", "/usr/local/bin/onlyboxes-console",
                    self.env({"CONSOLE_DASHBOARD_USERNAME": "kind-admin", "CONSOLE_DB_PATH": database,
                              "CONSOLE_EXPORT_FILE_ENDPOINT": self.upload_endpoint, "CONSOLE_EXPORT_FILE_REGION": "us-east-1",
                              "CONSOLE_EXPORT_FILE_BUCKET_NAME": "kind-contract", "CONSOLE_EXPORT_FILE_EXPORT_PREFIX": "exports",
                              "CONSOLE_EXPORT_RETURN_SCHEMA": "ALL",
                              "CONSOLE_LOG_BOOTSTRAP_CREDENTIALS": "false"}) + self.secret_env("console-bootstrap", bootstrap)))
        run(self.kubectl + ["rollout", "status", "deployment/console", "-n", NS, "--timeout=120s"])
        self.forward = subprocess.Popen(self.kubectl + ["port-forward", "-n", NS, "service/console", "18089:8089", "--address=127.0.0.1"],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait(lambda: self.request("/api/v1/auth/login", {"username": "kind-admin", "password": password}))
        self.token = self.request("/api/v1/tokens", {"name": "kind-contract"})["token"]
        registration = self.request("/api/v1/workers", {"type": "normal"})
        worker = {"WORKER_ID": registration["node_id"], "WORKER_SECRET": registration["worker_secret"]}
        self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "worker-registration", "namespace": NS}, "stringData": worker},
                   {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "worker", "namespace": NS}},
                   {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "worker", "namespace": NS},
                    "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "get", "list", "delete"]},
                              {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["create"]}]},
                   {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "worker", "namespace": NS},
                    "subjects": [{"kind": "ServiceAccount", "name": "worker", "namespace": NS}],
                    "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "worker"}},
                   self.deployment("worker", "/usr/local/bin/onlyboxes-worker-kubernetes",
                    self.env({"WORKER_CONSOLE_GRPC_TARGET": "console:50051", "WORKER_CONSOLE_INSECURE": "true",
                              "WORKER_KUBERNETES_NAMESPACE": NS, "WORKER_TERMINAL_EXEC_KUBERNETES_IMAGE": IMAGE,
                              "WORKER_PYTHON_EXEC_KUBERNETES_IMAGE": IMAGE, "WORKER_TERMINAL_LEASE_MIN_SEC": "1",
                              "WORKER_TERMINAL_LEASE_DEFAULT_SEC": "30", "WORKER_TERMINAL_OUTPUT_LIMIT_BYTES": "4096",
                              "WORKER_TERMINAL_MAX_ACTIVE_SESSIONS": "4", "WORKER_HEARTBEAT_INTERVAL_SEC": "1"}) +
                    self.secret_env("worker-registration", worker), service_account="worker"))
        run(self.kubectl + ["rollout", "status", "deployment/worker", "-n", NS, "--timeout=120s"])
        wait(lambda: any(item["status"] == "online" for item in self.request("/api/v1/workers")["items"]))
        print("PASS: Console and Kubernetes Worker deployed and registered", flush=True)

    def contract(self):
        echo = wait(lambda: self.request("/api/v1/commands/echo", {"message": "contract"}, True))
        assert echo.get("message") == "contract"
        terminal = self.request("/api/v1/commands/terminal", {"command": "printf retained > /workspace/state; printf ok; printf diagnostic >&2; exit 7"}, True)
        assert terminal["stdout"] == "ok" and terminal["stderr"] == "diagnostic" and terminal["exit_code"] == 7
        sid = terminal["session_id"]
        again = self.request("/api/v1/commands/terminal", {"command": "cat /workspace/state", "session_id": sid}, True)
        assert again["stdout"] == "retained" and not again["created"]
        print("PASS: REST echo, terminal result and persistent session contracts", flush=True)
        for capability, input_data in [("pythonExec", {"code": "print(6 * 7)"}),
                                       ("terminalResource", {"session_id": sid, "file_path": "/workspace/state", "action": "read"})]:
            result = self.request("/api/v1/tasks", {"capability": capability, "input": input_data, "mode": "sync", "timeout_ms": 60000}, True)
            assert result["status"] == "succeeded"
            if capability == "pythonExec":
                assert result["result"]["output"].strip() == "42" and result["result"]["exit_code"] == 0
            else:
                assert result["result"]["blob"] == "cmV0YWluZWQ=" and result["result"]["size_bytes"] == 8
        print("PASS: Python and file-resource task contracts", flush=True)
        self.mcp("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                "clientInfo": {"name": "onlyboxes-kind-contract", "version": "1"}})
        self.mcp("notifications/initialized", {}, None)
        tools = self.mcp("tools/list", {})
        assert {"echo", "pythonExec", "terminalExec", "readImage", "exportFile"}.issubset({tool["name"] for tool in tools["tools"]})
        for tool, arguments, field, expected in [("echo", {"message": "mcp-contract"}, "message", "mcp-contract"),
                                                  ("pythonExec", {"code": "print(21 * 2)"}, "output", "42\n"),
                                                  ("terminalExec", {"command": "cat /workspace/state", "session_id": sid}, "stdout", "retained")]:
            result = self.mcp("tools/call", {"name": tool, "arguments": arguments})
            assert not result.get("isError", False)
            value = result.get("structuredContent")
            if value is None:
                value = json.loads(next(item["text"] for item in result["content"] if item["type"] == "text"))
            assert value[field] == expected
        print("PASS: MCP discovery, echo, Python and terminal tool contracts", flush=True)
        fixture = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jhK0AAAAASUVORK5CYII="
        self.request("/api/v1/commands/terminal", {"session_id": sid,
                     "command": "python3 -c \"import base64; open('/workspace/pixel.png','wb').write(base64.b64decode('" + fixture + "'))\""}, True)
        image = self.mcp("tools/call", {"name": "readImage", "arguments": {"session_id": sid, "file_path": "/workspace/pixel.png"}})
        assert not image.get("isError", False)
        assert any(item.get("type") == "image" and item.get("mimeType") == "image/png" and item.get("data") == fixture for item in image["content"])
        print("PASS: MCP image resource contract", flush=True)
        receiver = """
from http.server import BaseHTTPRequestHandler, HTTPServer
import hashlib, json
state = {}
class Handler(BaseHTTPRequestHandler):
 def log_message(self, *args): pass
 def do_PUT(self):
  data = self.rfile.read(int(self.headers.get('Content-Length', '0')))
  state.update(size=len(data), digest=hashlib.sha256(data).hexdigest(), header=self.headers.get('x-amz-meta-contract'))
  self.send_response(200); self.end_headers()
 def do_GET(self):
  body=json.dumps(state).encode(); self.send_response(200); self.end_headers(); self.wfile.write(body)
HTTPServer(('0.0.0.0',8080), Handler).serve_forever()
"""
        self.apply(self.deployment("upload", ["python3", "-c", receiver], []),
                   {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "upload", "namespace": NS},
                    "spec": {"selector": {"app": "upload"}, "ports": [{"port": 8080}]}})
        run(self.kubectl + ["rollout", "status", "deployment/upload", "-n", NS, "--timeout=120s"])
        # Deployment readiness can precede Service endpoint propagation.
        wait(lambda: self.request("/api/v1/commands/terminal", {"session_id": sid,
                    "command": "python3 -c \"import urllib.request; assert urllib.request.urlopen('http://upload:8080',timeout=2).status == 200\""}, True)["exit_code"] == 0)
        exported = self.request("/api/v1/tasks", {"capability": "terminalResource", "mode": "sync", "timeout_ms": 60000,
                     "input": {"session_id": sid, "file_path": "/workspace/state", "action": "export",
                               "signed_url": "http://upload:8080/upload", "headers": {"x-amz-meta-contract": "verified"}}}, True)
        assert exported["status"] == "succeeded" and exported["result"]["size_bytes"] == 8
        checked = self.request("/api/v1/commands/terminal", {"session_id": sid,
                    "command": "python3 -c \"import urllib.request,json,hashlib; d=json.load(urllib.request.urlopen('http://upload:8080')); assert d['size']==8 and d['header']=='verified' and d['digest']==hashlib.sha256(b'retained').hexdigest(); print('upload-verified')\""}, True)
        assert checked["stdout"].strip() == "upload-verified"
        print("PASS: streamed file export and object-storage header forwarding", flush=True)
        exported_tool = self.mcp("tools/call", {"name": "exportFile", "arguments": {"session_id": sid, "file_path": "/workspace/state"}})
        assert not exported_tool.get("isError", False), "MCP export tool failed"
        value = exported_tool.get("structuredContent")
        if value is None:
            value = json.loads(next(item["text"] for item in exported_tool["content"] if item["type"] == "text"))
        assert value.get("signed_url", "").startswith(self.upload_endpoint + "/") and value.get("filename") == "state", "MCP export result schema mismatch"
        checked = self.request("/api/v1/commands/terminal", {"session_id": sid,
                    "command": "python3 -c \"import urllib.request,json,hashlib; d=json.load(urllib.request.urlopen('http://upload:8080')); assert d['size']==8 and d['digest']==hashlib.sha256(b'retained').hexdigest(); print('mcp-export-verified')\""}, True)
        assert checked["stdout"].strip() == "mcp-export-verified"
        print("PASS: MCP exportFile with Console S3 signing and a local upload fixture", flush=True)
        run(self.kubectl + ["rollout", "restart", "deployment/worker", "-n", NS])
        run(self.kubectl + ["rollout", "status", "deployment/worker", "-n", NS, "--timeout=120s"])
        restored = wait(lambda: self.request("/api/v1/commands/terminal", {"command": "cat /workspace/state", "session_id": sid}, True))
        assert restored["stdout"] == "retained"
        print("PASS: Worker restart recovers the same session Pod", flush=True)
        run(self.kubectl + ["rollout", "restart", "deployment/console", "-n", NS])
        run(self.kubectl + ["rollout", "status", "deployment/console", "-n", NS, "--timeout=120s"])
        # Port-forward is tied to the selected Console Pod and must reconnect.
        self.forward.terminate(); self.forward.wait(timeout=10)
        self.forward = subprocess.Popen(self.kubectl + ["port-forward", "-n", NS, "service/console", "18089:8089", "--address=127.0.0.1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        restored = wait(lambda: self.request("/api/v1/commands/terminal", {"command": "cat /workspace/state", "session_id": sid}, True))
        assert restored["stdout"] == "retained"
        print("PASS: Console restart preserves routing and session files", flush=True)
        short = self.request("/api/v1/commands/terminal", {"command": "printf leased", "lease_ttl_sec": 5}, True)
        assert short["stdout"] == "leased"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                self.request("/api/v1/commands/terminal", {"command": "true", "session_id": short["session_id"], "lease_ttl_sec": 1}, True)
            except RuntimeError as error:
                if str(error).endswith("HTTP 404"):
                    break
                raise
            # Avoid continually extending the lease while polling the interface.
            time.sleep(6)
        else:
            raise RuntimeError("expired session remained accessible")
        print("PASS: expired session returns the existing REST 404 contract", flush=True)
        try:
            self.request("/api/v1/commands/terminal", {"command": "sleep 30", "session_id": sid, "timeout_ms": 500}, True)
        except RuntimeError as error:
            assert str(error).endswith("HTTP 504")
        else:
            raise RuntimeError("timed-out command was not rejected")
        def no_sandboxes():
            items = json.loads(run(self.kubectl + ["get", "pods", "-n", NS, "-l", "app.kubernetes.io/name=onlyboxes-sandbox", "-o", "json"]))["items"]
            return not items
        wait(no_sandboxes, timeout=30)
        print("PASS: timeout returns REST 504 and all test sandbox Pods are removed", flush=True)
        print("ALL KIND CONTRACT TESTS PASSED", flush=True)

    def close(self):
        if self.forward:
            self.forward.terminate()
            self.forward.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--binary-dir", required=True)
    parser.add_argument("--skip-build", action="store_true", help="Use explicitly prebuilt development binaries")
    args = parser.parse_args()
    if not args.skip_build:
        print("Building Console and Kubernetes Worker from the checkout", flush=True)
        Path(args.binary_dir).mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, CGO_ENABLED="0", GOOS="linux")
        for directory, binary in [("console", "console"), ("worker/worker-kubernetes", "worker-kubernetes")]:
            run(["go", "build", "-p", "2", "-trimpath", "-o", str(Path(args.binary_dir).resolve() / binary), "./cmd/" + binary], cwd=ROOT / directory, env=env)
    print("Building development image", flush=True)
    with tempfile.TemporaryDirectory(prefix="onlyboxes-image-") as stage:
        for name in ("console", "worker-kubernetes"):
            shutil.copyfile(Path(args.binary_dir) / name, Path(stage) / name)
            os.chmod(Path(stage) / name, 0o755)
        shutil.copyfile(ROOT / "worker/worker-kubernetes/deploy/dev.Dockerfile", Path(stage) / "Dockerfile")
        run(["docker", "build", "-t", IMAGE, stage])
    run(["kind", "load", "docker-image", IMAGE, "--name", CLUSTER])
    print("Deploying development services", flush=True)
    harness = Harness(args.kubeconfig)
    try:
        harness.setup()
        harness.contract()
    finally:
        harness.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Assertion messages and external responses may contain sensitive values.
        print(f"FAIL: development contract ({type(error).__name__}); response suppressed", flush=True)
        if isinstance(error, RuntimeError):
            print(str(error), flush=True)
        raise SystemExit(1)
