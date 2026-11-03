#!/usr/bin/env python3
"""Live Compose tests through `aenv compose up` against an isolated node or gateway.

Requires the CLI's credentials (`aenv auth`, or an isolated XDG_CONFIG_HOME) and
Python 3.11+. AENV_COMPOSE_BASE_IMAGE selects a guest image built from
images/compose and pullable by the nodes; without it, the first test builds the
bundled image with the server's builder. Run `make test-compose`, or directly:

  AENV_BIN=target/debug/aenv AENV_COMPOSE_PLANNER=services/bin/aenv-compose-plan \\
    AENV_COMPOSE_BASE_IMAGE=... python3 scripts/compose/test.py

Creates and deletes only this test's sandboxes, snapshots and templates.
"""

import json
import os
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import tomllib

AENV = os.environ.get("AENV_BIN", "aenv")
BASE_IMAGE = os.environ.get("AENV_COMPOSE_BASE_IMAGE", "")
PLANNER = os.environ.get("AENV_COMPOSE_PLANNER")

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

READ_IMAGE_CONFIGS = (
    "token=$(curl -fsS -X PUT -H 'X-metadata-token-ttl-seconds: 60' "
    "http://169.254.169.254/latest/api/token) && "
    "curl -fsS -H 'Accept: application/json' -H \"X-metadata-token: $token\" "
    "http://169.254.169.254/imageConfigs"
)


class ComposeTests(unittest.TestCase):
    def setUp(self):
        config = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        if sys.platform == "darwin" and "XDG_CONFIG_HOME" not in os.environ:
            config = Path.home() / "Library/Application Support"
        self.credentials = tomllib.loads((config / "aenv/credentials").read_text())
        self.sandboxes = []
        self.snapshots = []

    def tearDown(self):
        for sandbox in reversed(self.sandboxes):
            self.request("DELETE", f"/sandboxes/{sandbox}", expected=(204, 404))
        for snapshot in reversed(self.snapshots):
            self.request("DELETE", f"/templates/{snapshot}", expected=(204, 404))

    def request(self, method, path, body=None, expected=(200,), headers=None, raw=False):
        req = urllib.request.Request(
            self.credentials["url"].rstrip("/") + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"X-API-Key": self.credentials["api_key"],
                     "Content-Type": "application/json", **(headers or {})})
        try:
            response = urllib.request.urlopen(req, timeout=660)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            self.assertIn(response.status, expected, data.decode(errors="replace"))
            return data.decode() if raw else json.loads(data) if data else None

    def up(self, compose, *args, guest=None):
        if guest is None:
            image = ["--base-image", BASE_IMAGE] if BASE_IMAGE else []
            guest = [*image, "--cpu", "2", "--memory", "2048"]
        command = [AENV, "compose", "up", "-f", "-", *guest, "--timeout", "900", *args]
        if PLANNER:
            command += ["--planner", PLANNER]
        return subprocess.run(command, input=compose, capture_output=True, text=True,
                              timeout=900, check=False)

    def create(self, compose=COMPOSE, *args, guest=None):
        result = self.up(compose, *args, guest=guest)
        self.assertEqual(result.returncode, 0, result.stderr)
        sandbox = result.stdout.strip()
        self.sandboxes.append(sandbox)
        return sandbox

    def execute(self, sandbox, *command):
        return subprocess.check_output([AENV, "exec", sandbox, *command],
                                       text=True, timeout=90).strip()

    def proxy(self, sandbox, port):
        return self.request("GET", "/", headers={"x-agentenv-sandbox-id": sandbox,
                                                 "x-agentenv-target-port": str(port)},
                            raw=True).strip()

    def test_lifecycle(self):
        sandbox = self.create(COMPOSE, "--env", "VALUE=$literal")
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
        result = self.up("services: {app: {image: busybox:1.37, env_file: /etc/passwd}}")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid Compose project", result.stderr)
        self.assertEqual(result.stdout, "")
        started = time.monotonic()
        result = self.up("""services:
  unhealthy:
    image: busybox:1.37
    command: [sleep, '600']
    healthcheck:
      test: [CMD, 'false']
      interval: 1s
      timeout: 1s
      retries: 1
""")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(result.stdout, "")
        self.assertLess(time.monotonic() - started, 60)
        self.assertEqual(self.request("GET", "/sandboxes"), before)
        print("invalid Compose rejected locally; unhealthy sandbox deleted", flush=True)

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

    def drive_ids(self, sandbox):
        configs = json.loads(self.execute(sandbox, "sh", "-c", READ_IMAGE_CONFIGS))
        return sorted(entry["driveId"] for entry in configs if "driveId" in entry)

    def test_template_warm_start(self):
        if BASE_IMAGE:
            name = f"compose-warm-{uuid.uuid4().hex[:8]}"
            build = subprocess.run([AENV, "pull", BASE_IMAGE, "--name", name, "--cpu", "2",
                                    "--memory", "2048", "--timeout", "900"],
                                   capture_output=True, text=True, timeout=960, check=False)
            template = self.request("GET", f"/templates/aliases/{name}",
                                    expected=(200, 404)) or {}
            if "templateID" in template:
                self.snapshots.append(template["templateID"])
            self.assertEqual(build.returncode, 0, build.stderr)
            print("built a template from the Compose guest image", flush=True)
            guest = ["--template", name]
        else:
            # The bundled template is built on first use and kept for later runs.
            guest = ["--template"]

        sandbox = self.create(COMPOSE, "--env", "VALUE=warm", guest=guest)
        self.assertEqual(self.proxy(sandbox, 8080), "a")
        self.assertEqual(self.proxy(sandbox, 8081), "b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "printenv", "VALUE"), "warm")
        self.assertEqual(self.drive_ids(sandbox), ["compose_0", "compose_1"])
        print("warm start publishes service image configs and starts services", flush=True)

        # The planner's maximum exceeds the template's free drive slots unless the
        # node sets [volume].max_volume_count to 24.
        before = self.request("GET", "/sandboxes")
        services = "".join(f"  s{i}: {{image: busybox:1.37, command: [sleep, '600']}}\n"
                           for i in range(24))
        result = self.up(f"services:\n{services}", guest=guest)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("--base-image", result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.request("GET", "/sandboxes"), before)
        print("more services than drive slots rejected with a cold-start hint", flush=True)

        snapshot = self.request("POST", f"/sandboxes/{sandbox}/snapshots", {}, expected=(201,))["snapshotID"]
        self.snapshots.append(snapshot)
        restored = self.request("POST", "/sandboxes", {"templateID": snapshot, "timeout": 900}, expected=(201,))["sandboxID"]
        self.sandboxes.append(restored)
        self.assertEqual(self.proxy(restored, 8081), "b")
        self.assertEqual(self.drive_ids(restored), ["compose_0", "compose_1"])
        print("snapshots of warm-started sandboxes keep service image configs", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
