#!/usr/bin/env python3
"""Run against an isolated Compose-enabled node; requires aenv on PATH.

AENV_API_URL=http://127.0.0.1:8001 AENV_API_KEY=... python3 compose-image/test_e2e.py
Creates and deletes only this test's sandboxes and snapshots.
Set AENV_IMAGE_CATALOG_TEST=1 to also check image deletion when that API is installed.
"""
import argparse
import hashlib
import sys
from contextlib import contextmanager
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
import uuid


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



@contextmanager
def isolated_client():
    api = os.environ["AENV_API_URL"].rstrip("/")
    key = os.environ["AENV_API_KEY"]
    with tempfile.TemporaryDirectory(prefix="aenv-compose-test-") as directory:
        config = Path(directory) / "aenv"
        config.mkdir()
        credentials = config / "credentials"
        credentials.write_text(f"url = {json.dumps(api)}\napi_key = {json.dumps(key)}\n", encoding="utf-8")
        credentials.chmod(0o600)
        yield Path(directory), dict(os.environ, XDG_CONFIG_HOME=directory), api, key


def execute(env, sandbox, *command, timeout=120):
    return subprocess.check_output(
        [os.environ.get("AENV_CLI", "aenv"), "exec", sandbox, *command],
        env=env, text=True, timeout=timeout).strip()



@unittest.skipUnless(os.environ.get("AENV_API_URL") and os.environ.get("AENV_API_KEY"),
                     "set AENV_API_URL and AENV_API_KEY for an isolated test node")
class ComposeE2E(unittest.TestCase):
    def setUp(self):
        self.work, self.env, self.base, self.key = self.enterContext(isolated_client())
        self.sandboxes = []
        self.snapshots = []

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
            response = urllib.request.urlopen(req, timeout=660)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            self.assertIn(response.status, expected, data.decode(errors="replace"))
            return data.decode() if raw else json.loads(data) if data else None

    def create(self, compose=COMPOSE, **kwargs):
        result = self.request("POST", "/sandboxes-compose", {
            "compose": compose, "composeEnv": {"VALUE": "$literal"},
            "cpuCount": 2, "memoryMB": 2048, "timeout": 900,
            **kwargs,
        }, expected=(201,))
        self.sandboxes.append(result["sandboxID"])
        return result["sandboxID"]

    def execute(self, sandbox, *command):
        return execute(self.env, sandbox, *command, timeout=90)

    def proxy(self, sandbox, port):
        return self.request("GET", "/", headers={"x-agentenv-sandbox-id": sandbox,
                                                 "x-agentenv-target-port": str(port)}, raw=True).strip()

    def build_statuses(self, stderr):
        """Status of each image build the CLI created; tearDown deletes the build records."""
        builds = re.findall(r"Created image build (\S+)", stderr)
        self.snapshots.extend(builds)
        return [self.request("GET", f"/templates/{build}/builds/{build}/status") for build in builds]

    def build_status(self, stderr):
        statuses = self.build_statuses(stderr)
        self.assertEqual(len(statuses), 1, stderr)
        return statuses[0]

    def test_build_planning_without_allocations(self):
        before = self.request("GET", "/sandboxes")
        source = "services: {main: {build: {context: ./missing, args: {VALUE: '${VALUE}'}}, profiles: [task]}}"
        body = {"compose": source, "composeEnv": {"VALUE": "$literal"}, "profiles": ["task"], "harbor": True}
        plan = self.request("POST", "/sandboxes-compose/plan", body)
        self.assertEqual(plan["services"][0]["context"], "./missing")
        self.assertEqual(plan["services"][0]["args"]["VALUE"], "$literal")
        self.assertNotIn("build", plan["compose"]["services"]["main"])
        self.assertEqual(plan["compose"]["services"]["main"]["command"], ["sh", "-c", "sleep infinity"])
        self.request("POST", "/sandboxes-compose/plan", body, expected=(401,), headers={"X-API-Key": "invalid"})
        self.request("POST", "/sandboxes-compose/plan", {"compose": "services: {main: {env_file: secret.env}}"}, expected=(400,))
        self.assertEqual(self.request("GET", "/sandboxes"), before)

    def test_cli_compose_up(self):
        source = COMPOSE.replace("  b:\n", "  b:\n    profiles: [worker]\n")
        compose_file = self.work / "compose.yaml"
        compose_file.write_text(source)
        cli = os.environ.get("AENV_CLI", "aenv")
        result = subprocess.run([
            cli, "compose", "up", "-f", str(compose_file), "--profile", "worker",
            "--env", "VALUE=old", "--env", "VALUE=$literal=cli", "--cpu", "2",
            "--memory", "2048", "--timeout", "600",
        ], env=self.env, capture_output=True, text=True, timeout=670)
        self.assertEqual(result.returncode, 0, result.stderr)
        sandbox = result.stdout.strip()
        self.sandboxes.append(sandbox)
        self.assertEqual(len(sandbox.splitlines()), 1)
        self.assertEqual(self.proxy(sandbox, 8080), "a")
        self.assertEqual(self.proxy(sandbox, 8081), "b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "printenv", "VALUE"),
                         "$literal=cli")

    def test_native_image_build_lifecycle(self):
        context = self.work / "native-image"
        context.mkdir()
        (context / "Dockerfile").write_text(
            f"FROM busybox:1.37\nRUN echo {self.work.name} >/marker\n"
            'ENV IMAGE_KIND=native\nCMD ["sleep", "infinity"]\n')
        result = subprocess.run([os.environ.get("AENV_CLI", "aenv"), "build", "--image",
                                 "--name", f"e2e-native-{uuid.uuid4().hex[:12]}", str(context),
                                 "--progress", "plain"], env=self.env, capture_output=True, text=True, timeout=900)
        self.assertEqual(result.returncode, 0, result.stderr)
        digest = result.stdout.strip()
        self.assertRegex(digest, r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(self.build_status(result.stderr)["imageDigest"], digest)
        catalog = os.environ.get("AENV_IMAGE_CATALOG_TEST") == "1"
        if catalog:
            self.addCleanup(self.request, "DELETE", f"/images/{digest}", expected=(204,))
            detail = self.request("GET", f"/images/{digest}")
            self.assertEqual(detail["imageDigest"], digest)
            encoded = json.dumps(detail["description"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            self.assertEqual("sha256:" + hashlib.sha256(encoded).hexdigest(), digest)
        project = json.dumps({"services": {name: {"image": digest} for name in ("a", "b")}})
        sandbox = self.create(compose=project, composeEnv={})
        self.execute(sandbox, "docker", "exec", "aenv-a-1", "sh", "-c", "echo private >/marker")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-b-1", "cat", "/marker"), self.work.name)
        snapshot = self.request("POST", f"/sandboxes/{sandbox}/snapshots", {}, expected=(201,))["snapshotID"]
        self.snapshots.append(snapshot)
        if catalog:
            self.request("DELETE", f"/images/{digest}", expected=(204,))
            self.request("GET", f"/images/{digest}", expected=(404,))
            missing = self.request("POST", "/sandboxes-cold", {"image": digest, "timeout": 60}, expected=(400,))
            self.assertIn(digest, missing["message"])
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "cat", "/marker"), "private")
        self.request("POST", f"/sandboxes/{sandbox}/pause", expected=(204,))
        self.request("POST", f"/sandboxes/{sandbox}/connect", {"timeout": 900}, expected=(201,))
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-b-1", "printenv", "IMAGE_KIND"), "native")
        fork = self.request("POST", f"/sandboxes/{sandbox}/fork", {"count": 1, "timeout": 900}, expected=(201,))[0]["sandbox"]
        self.sandboxes.append(fork["sandboxID"])
        self.snapshots.append(fork["templateID"])
        self.execute(fork["sandboxID"], "docker", "exec", "aenv-a-1", "sh", "-c", "echo child >/marker")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-a-1", "cat", "/marker"), "private")
        restored = self.request("POST", "/sandboxes", {"templateID": snapshot, "timeout": 900}, expected=(201,))["sandboxID"]
        self.sandboxes.append(restored)
        self.assertEqual(self.execute(restored, "docker", "exec", "aenv-a-1", "cat", "/marker"), "private")

    def test_cli_compose_build(self):
        work = self.work
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
        cli = os.environ.get("AENV_CLI", "aenv")
        project_name = f"e2e-compose-{uuid.uuid4().hex[:12]}"
        command = [cli, "build", "--compose", str(compose), "--name", project_name, "--output", str(output),
                   "--env", "VALUE=$literal=a=b", "--progress", "plain"]
        repository = os.environ.get("AENV_BUILD_IMAGE_REPOSITORY")
        if repository:
            command += ["--image-repository", repository]
            if os.environ.get("AENV_BUILD_REGISTRY_INSECURE") == "1":
                command.append("--registry-insecure")
        result = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=900)
        self.assertEqual(result.returncode, 0, result.stderr)
        project = json.loads(output.read_text())
        self.assertNotIn("build", project["services"]["app"])
        self.assertNotIn("build", project["services"]["worker"])
        self.assertEqual(project["services"]["client"]["image"], "busybox:1.37")
        if repository:
            self.assertTrue(project["services"]["app"]["image"].startswith(repository + ":aenv-"))
        else:
            self.assertRegex(project["services"]["app"]["image"], r"^sha256:[0-9a-f]{64}$")
            self.assertRegex(project["services"]["worker"]["image"], r"^sha256:[0-9a-f]{64}$")
        self.assertNotIn("x-aenv-build", project)
        self.assertEqual(compose.read_bytes(), original)
        statuses = self.build_statuses(result.stderr)
        self.assertEqual([status["status"] for status in statuses], ["ready", "ready"], result.stderr)
        for service in ("app", "worker"):
            self.assertIn(f"Building Compose service {service} as aenv/{project_name}-{service}", result.stderr)
        # Published references work in YAML and direct API requests, and each service
        # image is also tagged aenv/PROJECT-SERVICE:latest.
        project["services"]["worker"]["image"] = f"aenv/{project_name}-worker:latest"
        built_yaml = "\n".join(f"{json.dumps(key)}: {json.dumps(value)}"
                               for key, value in project.items())
        sandbox = self.create(compose=built_yaml, composeEnv={})
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-app-1", "cat", "/build-value"), "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-app-1", "printenv", "LITERAL"), "$literal=a=b")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-client-1", "wget", "-qO-", "http://app:8080"), "context-upload")
        self.assertEqual(self.execute(sandbox, "docker", "exec", "aenv-worker-1", "cat", "/build-value"), "default")

        # Refusing an existing destination must preserve it without allocating.
        saved = output.read_bytes()
        result = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already exists", result.stderr)
        self.assertNotIn("Created image build", result.stderr)
        self.assertEqual(output.read_bytes(), saved)

        output.unlink()
        dockerfile.write_text("FROM busybox:1.37 AS app\nRUN exit 42\n")
        result = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=900)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exit code: 42", result.stderr)
        self.assertFalse(output.exists(), "a partial build must not publish a runnable file")
        self.assertEqual(self.build_status(result.stderr)["status"], "error")
        print("Compose builds preserve args/target/context/dollars, publish and tag reusable service images, and accept YAML image references", flush=True)

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
""", "cpuCount": 2, "memoryMB": 2048,
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



def terminal_bench(argv):
    parser = argparse.ArgumentParser(description="Build and start unmodified Terminal-Bench Compose environments")
    parser.add_argument("--tasks", type=Path, required=True)
    parser.add_argument("--image-repository",
                        help="Distribute images through this registry in addition to publishing to the shared repository")
    parser.add_argument("--registry-insecure", action="store_true")
    parser.add_argument("--task", action="append", help="Only these task directory names")
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args(argv)
    cli = os.environ.get("AENV_CLI", "aenv")
    args.results.mkdir(parents=True, exist_ok=False)
    sources = sorted(args.tasks.glob("*/environment/docker-compose.yaml"))
    if args.task:
        sources = [p for p in sources if p.parent.parent.name in args.task]
        if {p.parent.parent.name for p in sources} != set(args.task):
            parser.error("a requested task has no environment/docker-compose.yaml")
    if not sources:
        parser.error("no Compose tasks found")
    report = []
    with isolated_client() as (_, env, api, key):
        for source in sources:
            name = source.parent.parent.name
            result = {"task": name, "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
            sandbox = None
            started = time.monotonic()
            log_path = args.results / f"{name}.log"
            built = args.results / f"{name}.built.yaml"
            print(f"[{name}] building original Compose configuration; log: {log_path}", flush=True)
            try:
                with log_path.open("w") as log:
                    # Every task file lives in environment/; name images after the task.
                    command = [cli, "build", "--compose", str(source.resolve()), "--harbor",
                               "--name", name, "--output", str(built.resolve()), "--progress", "plain"]
                    if args.image_repository:
                        command += ["--image-repository", args.image_repository]
                        if args.registry_insecure:
                            command.append("--registry-insecure")
                    subprocess.run(command, env=env, stdout=log, stderr=log,
                                   timeout=21600, check=True)
                    result["build_seconds"] = round(time.monotonic() - started, 2)
                    print(f"[{name}] images built; starting sandbox", flush=True)
                    up = subprocess.run([cli, "compose", "up", "-f", str(built),
                                         "--cpu", "8", "--memory", "16384", "--timeout", "900"],
                                        env=env, stdout=subprocess.PIPE, stderr=log,
                                        text=True, timeout=420, check=True)
                    sandbox = up.stdout.strip()
                    result["sandbox_id"] = sandbox
                    result["startup_seconds"] = round(time.monotonic() - started - result["build_seconds"], 2)
                    project = json.loads(built.read_text())
                    names = list(project["services"])
                    inspected = json.loads(execute(env, sandbox, "docker", "inspect",
                                                   *[f"aenv-{service}-1" for service in names]))
                    states = {}
                    for service, container in zip(names, inspected):
                        state = container["State"]
                        states[service] = {"status": state["Status"],
                                           "health": state.get("Health", {}).get("Status"),
                                           "exit_code": state["ExitCode"]}
                        assert state["Status"] == "running" or (
                            state["Status"] == "exited" and state["ExitCode"] == 0), states
                        if state.get("Health"):
                            assert state["Health"]["Status"] == "healthy", states
                    execute(env, sandbox, "docker", "exec", "aenv-main-1", "sh", "-c", "true")
                    result["services"] = states
                    result["status"] = "passed"
            except Exception as error:
                result.update(status="failed", error=str(error))
            finally:
                if sandbox:
                    request = urllib.request.Request(f"{api}/sandboxes/{sandbox}", method="DELETE",
                                                     headers={"X-API-Key": key})
                    with urllib.request.urlopen(request, timeout=120) as response:
                        assert response.status == 204
                assert hashlib.sha256(source.read_bytes()).hexdigest() == result["source_sha256"], "original task changed"
                result["total_seconds"] = round(time.monotonic() - started, 2)
                report.append(result)
                (args.results / "report.json").write_text(json.dumps(report, indent=2) + "\n")
                print(f"[{name}] {result['status']} ({result['total_seconds']}s)", flush=True)
    raise SystemExit(0 if all(result["status"] == "passed" for result in report) else 1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "terminal-bench":
        terminal_bench(sys.argv[2:])
    else:
        unittest.main(verbosity=2)
