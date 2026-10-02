#!/usr/bin/env python3
"""Run against an isolated Compose-enabled node; requires aenv on PATH.

AENV_API_URL=http://127.0.0.1:8001 AENV_API_KEY=... python3 compose-image/test_e2e.py
Creates and deletes only this test's sandboxes and snapshots.
"""
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request


COMPOSE = """services:
  a:
    image: busybox:1.37
    command: [sh, -c, "mkdir -p /www; echo a >/marker; echo a >/www/index.html; exec httpd -f -p 8080 -h /www"]
    environment:
      VALUE: ${VALUE}
      HOME:
    ports: ["8080:8080"]
    volumes: [data:/data]
    healthcheck:
      test: [CMD, wget, -q, -O, /dev/null, http://localhost:8080]
      interval: 1s
      timeout: 1s
      retries: 10
  b:
    image: busybox:1.37
    command: [sh, -c, "mkdir -p /www; echo b >/marker; echo b >/www/index.html; exec httpd -f -p 8080 -h /www"]
    ports: ["8081:8080"]
    depends_on:
      a:
        condition: service_healthy
volumes:
  data: {}
"""


@unittest.skipUnless(os.environ.get("AENV_API_URL") and os.environ.get("AENV_API_KEY"),
                     "set AENV_API_URL and AENV_API_KEY for an isolated test node")
class ComposeE2E(unittest.TestCase):
    def setUp(self):
        self.base = os.environ["AENV_API_URL"].rstrip("/")
        self.key = os.environ["AENV_API_KEY"]
        self.sandboxes = []
        self.snapshots = []
        self.credentials = tempfile.TemporaryDirectory(prefix="aenv-compose-e2e-")
        self.addCleanup(self.credentials.cleanup)
        config = Path(self.credentials.name) / "aenv"
        config.mkdir()
        credentials = config / "credentials"
        credentials.write_text(f"url = {json.dumps(self.base)}\napi_key = {json.dumps(self.key)}\n")
        credentials.chmod(0o600)
        self.env = dict(os.environ, XDG_CONFIG_HOME=self.credentials.name)

    def tearDown(self):
        for sandbox in reversed(self.sandboxes):
            self.request("DELETE", f"/sandboxes/{sandbox}", expected=(204, 404))
        for snapshot in reversed(self.snapshots):
            self.request("DELETE", f"/templates/{snapshot}", expected=(204, 404))

    def request(self, method, path, body=None, expected=(200,), headers=None, raw=False):
        req = urllib.request.Request(self.base + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={"X-API-Key": self.key, "Content-Type": "application/json",
                                              **(headers or {})})
        try:
            response = urllib.request.urlopen(req, timeout=360)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            self.assertIn(response.status, expected, data.decode(errors="replace"))
            return data.decode() if raw else json.loads(data) if data else None

    def create(self, compose=COMPOSE, **kwargs):
        result = self.request("POST", "/sandboxes-compose", {
            "compose": compose, "composeEnv": {"VALUE": "$literal"},
            "cpuCount": 2, "memoryMB": 2048, "timeout": 900, "startupTimeout": 300,
            **kwargs,
        }, expected=(201,))
        self.sandboxes.append(result["sandboxID"])
        return result["sandboxID"]

    def execute(self, sandbox, *command):
        result = subprocess.run([os.environ.get("AENV_CLI", "aenv"), "exec", sandbox, *command],
                                env=self.env, capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def proxy(self, sandbox, port):
        return self.request("GET", "/", headers={"x-agentenv-sandbox-id": sandbox,
                                                 "x-agentenv-target-port": str(port)}, raw=True).strip()

    def test_cli_compose_up(self):
        source = COMPOSE.replace("  b:\n", "  b:\n    profiles: [worker]\n")
        compose_file = Path(self.credentials.name) / "compose.yaml"
        compose_file.write_text(source)
        cli = os.environ.get("AENV_CLI", "aenv")
        result = subprocess.run([
            cli, "compose", "up", "-f", str(compose_file), "--profile", "worker",
            "--env", "VALUE=old", "--env", "VALUE=$literal=cli", "--cpu", "2",
            "--memory", "2048", "--timeout", "600", "--startup-timeout", "300",
        ], env=self.env, capture_output=True, text=True, timeout=370)
        self.assertEqual(result.returncode, 0, result.stderr)
        sandbox = result.stdout.strip()
        self.sandboxes.append(sandbox)
        self.assertEqual(len(sandbox.splitlines()), 1)
        self.assertEqual(self.proxy(sandbox, 8080), "a")
        self.assertEqual(self.proxy(sandbox, 8081), "b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "printenv", "VALUE"),
                         "$literal=cli")

        result = subprocess.run([cli, "compose", "up", "-f", "-"],
                                input="services: {app: {image: busybox:1.37, volumes: ['/etc:/host']}}",
                                env=self.env, capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("HTTP 400", result.stderr)
        print("CLI creates ready services with profiles and explicit environment; API errors propagate", flush=True)

    @unittest.skipUnless(os.environ.get("AENV_BUILD_IMAGE_REPOSITORY"),
                         "set AENV_BUILD_IMAGE_REPOSITORY to test remote image builds")
    def test_cli_compose_build(self):
        work = Path(self.credentials.name)
        context = work / "context"
        (context / "docker").mkdir(parents=True)
        (context / "payload").write_text("context-upload")
        (context / "excluded").write_text("must not be uploaded")
        (context / ".dockerignore").write_text("excluded\n")
        dockerfile = context / "docker" / "Custom"
        dockerfile.write_text("""FROM busybox:1.37 AS app
ARG VALUE=default
COPY . /context
RUN test ! -e /context/excluded && printf '%s' "$VALUE" >/build-value
HEALTHCHECK CMD false
FROM scratch AS unused
""")
        compose = work / "compose.yaml"
        compose.write_text("""services:
  app:
    build:
      context: ./context
      dockerfile: docker/Custom
      target: app
      args: {VALUE: "${VALUE}"}
    command: [sh, -c, 'mkdir -p /www; cp /context/payload /www/index.html; exec httpd -f -p 8080 -h /www']
    environment: {LITERAL: "${VALUE}"}
    healthcheck:
      test: [CMD, wget, -qO-, http://localhost:8080]
      interval: 1s
      timeout: 1s
      retries: 10
  client:
    image: busybox:1.37
    command: [sleep, infinity]
    depends_on: {app: {condition: service_healthy}}
""")
        original = compose.read_bytes()
        output = work / "built.yaml"
        cli = os.environ.get("AENV_CLI", "aenv")
        command = [cli, "build", "--compose", str(compose), "--output", str(output),
                   "--image-repository", os.environ["AENV_BUILD_IMAGE_REPOSITORY"],
                   "--env", "VALUE=$literal=a=b", "--progress", "plain"]
        if os.environ.get("AENV_BUILD_REGISTRY_INSECURE") == "1":
            command.append("--registry-insecure")
        result = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=900)
        self.assertEqual(result.returncode, 0, result.stderr)
        project = json.loads(output.read_text())
        self.assertNotIn("build", project["services"]["app"])
        self.assertEqual(project["services"]["client"]["image"], "busybox:1.37")
        self.assertEqual(compose.read_bytes(), original)
        sandbox = self.create(compose=output.read_text(), composeEnv={})
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-app-1", "cat", "/build-value"), "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-app-1", "printenv", "LITERAL"), "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-client-1", "wget", "-qO-", "http://app:8080"), "context-upload")

        # Refusing an existing destination must preserve it without allocating.
        saved = output.read_bytes()
        result = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already exists", result.stderr)
        self.assertNotIn("Allocated", result.stderr)
        self.assertEqual(output.read_bytes(), saved)

        output.unlink()
        dockerfile.write_text("FROM busybox:1.37 AS app\nRUN exit 42\n")
        result = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=900)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exit code: 42", result.stderr)
        self.assertFalse(output.exists(), "a partial build must not publish a runnable file")
        build_id = re.search(r"Allocated image build ([0-9a-f-]+)", result.stderr).group(1)
        self.request("GET", f"/templates/{build_id}/builds/{build_id}/status", expected=(404,))
        print("Compose build preserves args/target/context/dollars, skips image startup, and cleans failed exports", flush=True)

    def test_lifecycle(self):
        sandbox = self.create()
        print("created two services using the same source image", flush=True)
        self.assertEqual(self.proxy(sandbox, 8080), "a")
        self.assertEqual(self.proxy(sandbox, 8081), "b")
        self.assertEqual(self.execute(sandbox, "docker", "info", "--format", "{{.Driver}}"), "plain")
        for service in ("a", "b"):
            self.assertEqual(self.execute(sandbox, "docker", "exec", f"aenv-{service}-1", "cat", "/marker"), service)
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-b-1", "wget", "-qO-", "http://a:8080"), "a")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "printenv", "VALUE"), "$literal")
        self.execute(sandbox, "docker", "exec", "aenv-a-1", "sh", "-c", "echo persisted >/data/value")
        identity = self.execute(sandbox, "docker", "inspect", "--format", "{{.Id}} {{.State.Pid}}", "aenv-a-1")
        self.request("POST", f"/sandboxes/{sandbox}/pause", expected=(204,))
        self.request("POST", f"/sandboxes/{sandbox}/resume", {"timeout": 900}, expected=(201,))
        self.assertEqual(self.execute(sandbox, "docker", "inspect", "--format", "{{.Id}} {{.State.Pid}}", "aenv-a-1"), identity)
        self.assertEqual(self.proxy(sandbox, 8080), "a")
        print("pause/resume preserves running containers", flush=True)

        fork = self.request("POST", f"/sandboxes/{sandbox}/fork", {"count": 1, "timeout": 900}, expected=(201,))
        self.assertIn("sandbox", fork[0], fork)
        child = fork[0]["sandbox"]["sandboxID"]
        self.sandboxes.append(child)
        self.snapshots.append(fork[0]["sandbox"]["templateID"])
        self.assertEqual(self.proxy(child, 8081), "b")
        self.execute(child, "docker", "exec", "aenv-a-1", "sh", "-c", "echo child >/marker; echo child >/data/value")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "cat", "/marker"), "a")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "cat", "/data/value"), "persisted")
        print("fork isolates service rootfs and named volume writes", flush=True)

        snapshot = self.request("POST", f"/sandboxes/{sandbox}/snapshots", {}, expected=(201,))["snapshotID"]
        self.snapshots.append(snapshot)
        restored = self.request("POST", "/sandboxes", {"templateID": snapshot, "timeout": 900}, expected=(201,))["sandboxID"]
        self.sandboxes.append(restored)
        self.assertEqual(self.proxy(restored, 8080), "a")
        self.assertEqual(self.execute(restored, "docker", "inspect", "--format", "{{.Id}} {{.State.Pid}}", "aenv-a-1"), identity)
        self.assertEqual(self.execute(restored, "docker", "exec", "aenv-a-1", "cat", "/data/value"), "persisted")
        print("snapshot restore preserves services and volume data", flush=True)

    def test_rejection_and_failed_healthcheck_cleanup(self):
        before = self.request("GET", "/sandboxes")
        self.request("POST", "/sandboxes-compose", {
            "compose": "services: {app: {image: busybox:1.37, volumes: ['/etc:/host']}}",
        }, expected=(400,))
        started = time.monotonic()
        self.request("POST", "/sandboxes-compose", {
            "compose": """services:
  unhealthy:
    image: busybox:1.37
    command: [sleep, '600']
    healthcheck:
      test: [CMD, 'false']
      interval: 1s
      timeout: 1s
      retries: 1
""", "startupTimeout": 30, "cpuCount": 2, "memoryMB": 2048,
        }, expected=(500,))
        self.assertLess(time.monotonic() - started, 60)
        self.assertEqual(self.request("GET", "/sandboxes"), before)
        print("invalid Compose rejected; unhealthy sandbox reclaimed", flush=True)

    def test_image_defaults(self):
        # No command/entrypoint overrides: placeholders must carry the original
        # image configuration, including Redis's entrypoint and working directory.
        sandbox = self.create("""services:
  web:
    image: nginx:1.27-alpine
    ports: ['8080:80']
  cache:
    image: redis:7-alpine
    volumes: [data:/data]
    healthcheck:
      test: [CMD, redis-cli, ping]
      interval: 1s
      timeout: 1s
      retries: 30
volumes:
  data: {}
""")
        self.assertIn("Welcome to nginx", self.proxy(sandbox, 8080))
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-cache-1", "redis-cli", "ping"), "PONG")
        self.assertEqual(self.execute(sandbox, "docker", "inspect", "--format", "{{.Config.WorkingDir}}", "aenv-cache-1"), "/data")
        self.assertEqual(self.execute(sandbox, "docker", "inspect", "--format", "{{json .Config.Entrypoint}}", "aenv-cache-1"), '["docker-entrypoint.sh"]')
        self.execute(sandbox, "docker", "exec", "aenv-cache-1", "sh", "-c",
                     "mkdir /tmp/shared; printf readable >/tmp/shared/value")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "--user", "65534", "aenv-cache-1",
                                      "cat", "/tmp/shared/value"), "readable")
        print("original image entrypoint, command and working directory preserved", flush=True)
        print("exec-created files remain readable by non-root container users", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
