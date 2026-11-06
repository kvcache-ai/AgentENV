#!/usr/bin/env python3
"""Live Compose tests through `aenv compose up` against an isolated node or gateway.

Requires the CLI's credentials (`aenv auth`, or an isolated XDG_CONFIG_HOME) and
Python 3.11+. AENV_COMPOSE_BASE_IMAGE selects a guest image built from
images/compose and pullable by the nodes; without it, the first test builds the
bundled image with the server's builder. Run `make test-compose`, or directly:

  AENV_BIN=target/debug/aenv AENV_COMPOSE_PLANNER=services/bin/aenv-compose-plan \\
    AENV_COMPOSE_BASE_IMAGE=... python3 scripts/compose/test.py

Set AENV_COMPOSE_BUILD_TEST=1 to also test service builds with `aenv compose
build` and `aenv compose up`, whose builder must reach Docker Hub. Creates and deletes only this test's sandboxes,
snapshots and build records; built images stay in the repository.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
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


def overridden_images(override):
    """Maps each service of an `aenv compose build` override file to its image."""
    images, service = {}, None
    for line in override.splitlines():
        if match := re.fullmatch(r'  ("[^"]*"):', line):
            service = json.loads(match.group(1))
        elif match := re.fullmatch(r'    image: (".*")', line):
            images[service] = json.loads(match.group(1))
    return images


class ComposeTests(unittest.TestCase):
    def setUp(self):
        config = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        if sys.platform == "darwin" and "XDG_CONFIG_HOME" not in os.environ:
            config = Path.home() / "Library/Application Support"
        self.credentials = tomllib.loads((config / "aenv/credentials").read_text())
        self.sandboxes = []
        self.snapshots = []
        # The planner loads .env from the project directory, which is the
        # working directory for a stdin project.
        self.work = Path(self.enterContext(tempfile.TemporaryDirectory()))

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

    def up(self, compose, *args, project="e2e", files=(), cwd=None, timeout=900):
        """Reads the project from stdin, from files, or from the working directory."""
        sources = ["-f", "-"] if compose is not None else [arg for file in files for arg in ("-f", str(file))]
        image = ["--base-image", BASE_IMAGE] if BASE_IMAGE else []
        command = [AENV, "compose", "up", *sources, "-p", project, *image,
                   "--cpu", "2", "--memory", "2048", "--timeout", "900", *args]
        if PLANNER:
            command += ["--planner", PLANNER]
        return subprocess.run(command, input=compose, capture_output=True, text=True,
                              cwd=cwd or self.work, timeout=timeout, check=False)

    def create(self, compose=COMPOSE, *args, **options):
        result = self.up(compose, *args, **options)
        self.assertEqual(result.returncode, 0, result.stderr)
        sandbox = result.stdout.strip()
        self.sandboxes.append(sandbox)
        return sandbox

    def execute(self, sandbox, *command):
        return subprocess.check_output([AENV, "exec", sandbox, *command],
                                       text=True, timeout=90).strip()

    def build(self, *args, timeout=900):
        command = [AENV, "compose", "build", "--progress", "plain", *args]
        if PLANNER:
            command += ["--planner", PLANNER]
        return subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)

    def build_statuses(self, stderr):
        """Status of each image build the CLI created; tearDown deletes the build records."""
        builds = re.findall(r"Created image build (\S+)", stderr)
        self.snapshots.extend(builds)
        return [self.request("GET", f"/templates/{build}/builds/{build}/status") for build in builds]

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
            self.assertEqual(self.execute(sandbox, "docker", "exec", f"e2e-{service}-1", "cat", "/marker"), service)
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-b-1", "wget", "-qO-", "http://a:8080"), "a")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-a-1", "printenv", "VALUE"), "$literal")
        self.execute(sandbox, "docker", "exec", "e2e-a-1", "sh", "-c", "echo persisted >/data/value")
        identity = self.execute(sandbox, "docker", "inspect", "--format", "{{.Id}} {{.State.Pid}}", "e2e-a-1")
        self.request("POST", f"/sandboxes/{sandbox}/pause", expected=(204,))
        self.request("POST", f"/sandboxes/{sandbox}/resume", {"timeout": 900}, expected=(201,))
        self.assertEqual(self.execute(sandbox, "docker", "inspect", "--format", "{{.Id}} {{.State.Pid}}", "e2e-a-1"), identity)
        self.assertEqual(self.proxy(sandbox, 8080), "a")
        print("pause/resume preserves running containers", flush=True)

        fork = self.request("POST", f"/sandboxes/{sandbox}/fork", {"count": 1, "timeout": 900}, expected=(201,))
        self.assertIn("sandbox", fork[0], fork)
        child = fork[0]["sandbox"]["sandboxID"]
        self.sandboxes.append(child)
        self.assertEqual(self.proxy(child, 8081), "b")
        self.execute(child, "docker", "exec", "e2e-a-1", "sh", "-c", "echo child >/marker; echo child >/data/value")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-a-1", "cat", "/marker"), "a")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-a-1", "cat", "/data/value"), "persisted")
        print("fork isolates service rootfs and named volume writes", flush=True)

        snapshot = self.request("POST", f"/sandboxes/{sandbox}/snapshots", {}, expected=(201,))["snapshotID"]
        self.snapshots.append(snapshot)
        restored = self.request("POST", "/sandboxes", {"templateID": snapshot, "timeout": 900}, expected=(201,))["sandboxID"]
        self.sandboxes.append(restored)
        self.assertEqual(self.proxy(restored, 8080), "a")
        self.assertEqual(self.execute(restored, "docker", "inspect", "--format", "{{.Id}} {{.State.Pid}}", "e2e-a-1"), identity)
        self.assertEqual(self.execute(restored, "docker", "exec", "e2e-a-1", "cat", "/data/value"), "persisted")
        print("snapshot restore preserves services and volume data", flush=True)

    def test_project_loading(self):
        # Like Docker Compose: the default file and its override, the project's
        # .env file and the process environment, which --env extends.
        (self.work / "compose.yaml").write_text("""services:
  web:
    image: busybox:${TAG}
    command: [sh, -c, 'echo "$$GREETING" >/greeting; exec sleep infinity']
    environment:
      GREETING: ${GREETING:-unset}
      FROM_SHELL:
""")
        (self.work / "compose.override.yaml").write_text("""services:
  web:
    environment:
      OVERRIDDEN: "yes"
""")
        (self.work / ".env").write_text("TAG=1.37\nGREETING=from-dotenv\n")
        sandbox = self.create(None, "--env", "FROM_SHELL=shell")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-web-1", "cat", "/greeting"), "from-dotenv")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-web-1", "printenv", "FROM_SHELL"), "shell")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-web-1", "printenv", "OVERRIDDEN"), "yes")
        print("project loaded with its override file, .env and process environment", flush=True)

    def test_rejection_and_failed_healthcheck_cleanup(self):
        before = self.request("GET", "/sandboxes")
        result = self.up("services: {app: {image: busybox:1.37, platform: linux/arm64}}")
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
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-cache-1", "redis-cli", "ping"), "PONG")
        self.assertEqual(self.execute(sandbox, "docker", "inspect", "--format", "{{.Config.WorkingDir}}", "e2e-cache-1"), "/data")
        self.assertEqual(self.execute(sandbox, "docker", "inspect", "--format", "{{json .Config.Entrypoint}}", "e2e-cache-1"), '["docker-entrypoint.sh"]')
        self.execute(sandbox, "docker", "exec", "e2e-cache-1", "sh", "-c",
                     "mkdir /tmp/shared; printf readable >/tmp/shared/value")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "--user", "65534", "e2e-cache-1",
                                      "cat", "/tmp/shared/value"), "readable")
        print("original image entrypoint, command and working directory preserved", flush=True)
        print("exec-created files remain readable by non-root container users", flush=True)

    @unittest.skipUnless(os.environ.get("AENV_COMPOSE_BUILD_TEST") == "1",
                         "set AENV_COMPOSE_BUILD_TEST=1; the builder must reach Docker Hub")
    def test_build(self):
        work = Path(self.enterContext(tempfile.TemporaryDirectory()))
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
    image: e2e/shared-app:dev
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
  sidecar:
    image: e2e/shared-app:dev
    command: [sleep, infinity]
    healthcheck: {test: [CMD, "true"]}
  inline:
    build:
      dockerfile_inline: |
        FROM busybox:1.37
        RUN printf inline >/build-value
    command: [sleep, infinity]
  worker:
    build:
      context: ./context
      dockerfile: docker/Custom
      target: app
    command: [sleep, infinity]
    healthcheck: {test: [CMD, "true"]}
""")
        original = compose.read_bytes()
        output = work / "built.yaml"
        project_name = f"e2e-compose-{uuid.uuid4().hex[:12]}"
        args = ["-f", str(compose), "-p", project_name, "--output", str(output),
                "--env", "VALUE=$literal=a=b"]
        repository = os.environ.get("AENV_BUILD_IMAGE_REPOSITORY")
        if repository:
            args += ["--image-repository", repository]
            if os.environ.get("AENV_BUILD_REGISTRY_INSECURE") == "1":
                args.append("--registry-insecure")
        result = self.build(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        override = output.read_text()
        images = overridden_images(override)
        self.assertEqual(sorted(images), ["app", "inline", "sidecar", "worker"], override)
        self.assertEqual(override.count("build: !reset null"), 3, override)
        # As in Docker Compose, a service runs the image another service builds.
        self.assertEqual(images["sidecar"], images["app"])
        for service in ("app", "inline", "worker"):
            if repository:
                self.assertTrue(images[service].startswith(repository + ":aenv-"), override)
            else:
                self.assertRegex(images[service], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(compose.read_bytes(), original)
        statuses = self.build_statuses(result.stderr)
        self.assertEqual([status["status"] for status in statuses], ["ready"] * 3, result.stderr)
        for service in ("app", "inline", "worker"):
            self.assertIn(f"Building Compose service {service} as aenv/{project_name}-{service}", result.stderr)
        self.assertIn(f"-f {compose} -f {output}", result.stderr)
        print("built three services; the override file pins their images and the sidecar's", flush=True)

        # The unmodified project with the override file starts without building.
        result = self.up(None, "--env", "VALUE=$literal=a=b", files=[compose, output], cwd=work)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("Created image build", result.stderr)
        sandbox = result.stdout.strip()
        self.sandboxes.append(sandbox)
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-app-1", "cat", "/build-value"), "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-app-1", "printenv", "LITERAL"), "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-client-1", "wget", "-qO-", "http://app:8080"),
                         "context-upload")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-worker-1", "cat", "/build-value"), "default")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-sidecar-1", "cat", "/build-value"), "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "e2e-inline-1", "cat", "/build-value"), "inline")
        print("built services run with build args, target, context, .dockerignore, inline Dockerfiles "
              "and literal dollars", flush=True)

        # compose up builds services itself; a project read from stdin resolves
        # against the current directory.
        up_args = ["--env", "VALUE=$literal=a=b"]
        result = self.up(compose.read_text(), *up_args, project=project_name, cwd=work, timeout=1800)
        self.assertEqual(result.returncode, 0, result.stderr)
        sandbox = result.stdout.strip()
        self.sandboxes.append(sandbox)
        statuses = self.build_statuses(result.stderr)
        self.assertEqual([status["status"] for status in statuses], ["ready"] * 3, result.stderr)
        self.assertEqual(self.execute(sandbox, "docker", "exec", f"{project_name}-app-1", "cat", "/build-value"),
                         "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", f"{project_name}-client-1", "wget", "-qO-",
                                      "http://app:8080"), "context-upload")
        self.assertEqual(self.execute(sandbox, "docker", "exec", f"{project_name}-sidecar-1", "cat", "/build-value"),
                         "$literal=a=b")
        print("compose up built services of a stdin project relative to the current directory", flush=True)

        # --no-build starts the published latest images without building.
        result = self.up(compose.read_text(), *up_args, "--no-build", project=project_name, cwd=work)
        self.assertEqual(result.returncode, 0, result.stderr)
        sandbox = result.stdout.strip()
        self.sandboxes.append(sandbox)
        self.assertNotIn("Created image build", result.stderr)
        self.assertEqual(self.execute(sandbox, "docker", "exec", f"{project_name}-app-1", "cat", "/build-value"),
                         "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", f"{project_name}-worker-1", "cat", "/build-value"),
                         "default")
        self.assertEqual(self.execute(sandbox, "docker", "exec", f"{project_name}-sidecar-1", "cat", "/build-value"),
                         "$literal=a=b")
        result = self.up(compose.read_text(), "--no-build", project=f"{project_name}-missing", cwd=work)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("was not found", result.stderr)
        self.assertNotIn("Created image build", result.stderr)
        print("compose up --no-build uses the latest images and builds nothing", flush=True)

        # A file the command did not write and unsupported builds are refused
        # before allocation.
        foreign = work / "foreign.yaml"
        foreign.write_text("services: {}\n")
        result = self.build("-f", str(compose), "-p", project_name, "--output", str(foreign),
                            "--env", "VALUE=x", timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("was not written by aenv compose build", result.stderr)
        self.assertNotIn("Created image build", result.stderr)
        self.assertEqual(foreign.read_text(), "services: {}\n")
        remote = work / "remote.yaml"
        remote.write_text("services: {app: {build: {context: 'https://example.com/repo.git'}}}\n")
        result = self.build("-f", str(remote), "-p", project_name, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be a local directory", result.stderr)
        self.assertNotIn("Created image build", result.stderr)

        # A failed build reports BuildKit's error and keeps the previous override.
        saved = output.read_bytes()
        dockerfile.write_text("FROM busybox:1.37 AS app\nRUN exit 42\n")
        result = self.build(*args)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exit code: 42", result.stderr)
        self.assertEqual(output.read_bytes(), saved, "a failed build must not replace the override file")
        statuses = self.build_statuses(result.stderr)
        self.assertEqual([status["status"] for status in statuses], ["error"], result.stderr)
        print("invalid builds rejected before allocation; failed builds keep the override file", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
