"""Live BuildKit tests through the same authenticated API and CLI users run."""

import contextlib
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parent
AENV = os.environ.get("AENV_BIN", "aenv")
BUILDCTL = os.environ.get("BUILDCTL_BIN", "buildctl")


class BuildKitTests(unittest.TestCase):
    def setUp(self):
        self.work = Path(tempfile.mkdtemp(prefix="aenv-buildkit-test-"))
        self.prefix = self.work.name
        self.context = self.work / "context"
        shutil.copytree(ROOT / "fixture", self.context)
        self.sandboxes = set()
        config = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        if sys.platform == "darwin" and "XDG_CONFIG_HOME" not in os.environ:
            config = Path.home() / "Library/Application Support"
        self.credentials = tomllib.loads((config / "aenv/credentials").read_text())
        self.addCleanup(self.cleanup_sandboxes)
        print(f"\nBuildKit logs: {self.work}", flush=True)

        def idle():
            # The gateway may still report a heartbeat from the preceding suite.
            self.baseline = self.node_counts()
            return all(counts == (0, 0, 0) for counts in self.baseline.values())

        self.wait_for(idle, "idle nodes before capturing the worker baseline")

    def cli(self, *args):
        result = subprocess.run(
            [AENV, *args], capture_output=True, text=True, timeout=180, check=False
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout.strip()

    def api(self, method, path, body=None, key=None):
        request = urllib.request.Request(
            self.credentials["url"].rstrip("/") + path,
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={
                "X-API-Key": self.credentials["api_key"] if key is None else key,
                "Content-Type": "application/json",
            },
        )
        try:
            response = urllib.request.urlopen(request, timeout=70)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read().decode()
            if data and response.headers.get_content_type() == "application/json":
                data = json.loads(data)
            return response.status, data

    def node_counts(self):
        status, nodes = self.api("GET", "/nodes")
        self.assertEqual(status, 200, nodes)
        self.assertTrue(nodes, "Enable observability to verify worker cleanup")
        return {
            node["id"]: (
                node["sandboxCount"],
                node.get("sandboxStartingCount", 0),
                node["sandboxPausedCount"],
            )
            for node in nodes
        }

    def wait_for(self, predicate, description, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(1)
        self.fail(f"Timed out waiting for {description}; logs: {self.work}")

    def launch_build(self, name, *args, context=None, timeout=180, tty=False, buildctl=None):
        log = self.work / f"{name}.log"
        command = [
            AENV,
            "build",
            str(context or self.context),
            "--name",
            f"{self.prefix}-{name}",
            "--buildctl",
            buildctl or BUILDCTL,
            "--timeout",
            str(timeout),
            "--build-arg",
            f"CACHE_KEY={self.prefix}",
            "--progress",
            "auto" if tty else "plain",
            *args,
        ]
        if tty:
            command = ["script", "-q", "-e", "-c", shlex.join(command), "/dev/null"]
        with log.open("w") as output:
            process = subprocess.Popen(
                command,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env={**os.environ, "TERM": "xterm-256color"},
            )
        build = (process, log)
        self.addCleanup(self.cleanup_build, build)
        return build

    def build_id(self, build):
        match = re.search(
            r"Created template ([a-f0-9-]+)", build[1].read_text(), re.MULTILINE
        )
        self.assertIsNotNone(match, build[1].read_text())
        return match[1]

    def finish(self, build, success=True):
        code = build[0].wait(timeout=360)
        output = build[1].read_text()
        self.assertEqual(code == 0, success, output)
        build_id = self.build_id(build)
        expected = "ready" if success else "error"

        def terminal():
            status, info = self.api(
                "GET", f"/templates/{build_id}/builds/{build_id}/status"
            )
            self.assertEqual(status, 200, info)
            return info["status"] == expected

        self.wait_for(terminal, f"build {build_id} to become {expected}")
        return build_id

    def build(self, name, *args, **kwargs):
        build = self.launch_build(name, *args, **kwargs)
        self.finish(build)
        return build

    @contextlib.contextmanager
    def sandbox(self, build):
        sandbox = self.cli("start", self.build_id(build), "--detach")
        self.sandboxes.add(sandbox)
        try:
            yield sandbox
        finally:
            self.cli("delete", sandbox)
            self.sandboxes.remove(sandbox)

    def result(self, build, expected):
        with self.sandbox(build) as sandbox:
            self.cli(
                "exec",
                sandbox,
                "/bin/sh",
                "-c",
                'test "$BUILD_BACKEND" = buildkit && test ! -d /src '
                "&& test ! -d /compiler-cache && test -s /started",
            )
            result, seed, execution = self.cli(
                "exec", sandbox, "cat", "/result.txt", "/seed", "/execution"
            ).splitlines()
            self.assertEqual(result, expected)
            self.assertRegex(seed, r"^[a-f0-9-]{36}$")
            self.assertRegex(execution, r"^[a-f0-9-]{36}$")
            return seed, execution

    def wait_running(self, build):
        def running():
            self.assertIsNone(build[0].poll(), build[1].read_text())
            # Match output from the instruction, not BuildKit's queued-step description.
            return re.search(
                r"^#\d+ [0-9.]+ worker-running$", build[1].read_text(), re.MULTILINE
            )

        self.wait_for(running, "the RUN instruction to execute")

    def assert_released(self):
        def released():
            volumes = json.loads(self.cli("volume", "list", "--output", "json"))
            caches = [
                volume
                for volume in volumes
                if volume["name"].startswith("aenv-buildkit-")
            ]
            return (
                all(
                    volume["name"].startswith("aenv-buildkit-seed-")
                    and volume["status"] == "ready"
                    and volume["mode"] == "ro"
                    for volume in caches
                )
                and len(caches) == 1
                and self.node_counts() == self.baseline
            )

        self.wait_for(
            released, "workers and writable/retired cache volumes to be released"
        )

    def cleanup_sandboxes(self):
        for sandbox in self.sandboxes:
            self.cli("delete", sandbox)

    def cleanup_build(self, build):
        # unittest runs every registered cleanup, even if another cleanup fails.
        process, log = build
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        match = re.search(
            r"Created template ([a-f0-9-]+)", log.read_text(), re.MULTILINE
        )
        if not match:
            return
        build_id = match[1]
        path = f"/templates/{build_id}/builds/{build_id}/builder"

        def released():
            status, body = self.api("DELETE", path)
            # Publication cannot be cancelled, so wait for it before deleting.
            self.assertIn(status, (204, 404, 409), body)
            return status != 409

        self.wait_for(released, f"build {build_id} cleanup")
        status, body = self.api("DELETE", f"/templates/{build_id}")
        self.assertIn(status, (204, 404), body)

    def test_context_instruction_cache_and_cache_mounts(self):
        first = self.build("first")
        seed, execution = self.result(
            first, (self.context / "input.txt").read_text().strip()
        )
        self.assertNotIn("\x1b", first[1].read_text())
        (self.context / "input.txt").write_text("changed\n")
        changed = self.build("changed")
        next_seed, next_execution = self.result(changed, "changed")
        self.assertEqual(seed, next_seed, "cache mount was lost across workers")
        self.assertNotEqual(
            execution, next_execution, "changed COPY did not invalidate RUN"
        )
        unchanged = self.build("unchanged")
        self.assertEqual(self.result(unchanged, "changed"), (seed, next_execution))
        uncached = self.build("no-cache", "--no-cache")
        last_seed, last_execution = self.result(uncached, "changed")
        # BuildKit prunes cache mounts referenced by IgnoreCache vertices:
        # https://github.com/moby/buildkit/blob/v0.33.0/solver/llbsolver/vertex.go#L205
        self.assertNotEqual(last_seed, seed, "--no-cache did not reset the cache mount")
        self.assertNotEqual(
            last_execution, next_execution, "--no-cache did not rerun RUN"
        )
        self.assert_released()

    def test_publication_survives_client_exit_after_solve(self):
        wrapper = self.work / "buildctl-exit-client"
        wrapper.write_text(
            f"#!{sys.executable}\n"
            "import os, signal, subprocess, sys\n"
            f"result = subprocess.run([{BUILDCTL!r}, *sys.argv[1:]])\n"
            "if '--version' not in sys.argv and result.returncode == 0:\n"
            "    os.kill(os.getppid(), signal.SIGKILL)\n"
            "sys.exit(result.returncode)\n"
        )
        wrapper.chmod(0o700)
        build = self.launch_build("client-exit", buildctl=str(wrapper))
        self.assertEqual(build[0].wait(timeout=360), -signal.SIGKILL, build[1].read_text())
        build_id = self.build_id(build)

        def ready():
            status, info = self.api("GET", f"/templates/{build_id}/builds/{build_id}/status")
            self.assertEqual(status, 200, info)
            self.assertNotEqual(info["status"], "error", info)
            return info["status"] == "ready"

        self.wait_for(ready, "server publication after the client exits")
        self.result(build, (self.context / "input.txt").read_text().strip())
        self.assert_released()

    def test_startup_overrides(self):
        for name, start, ready in [
            (
                "override",
                "echo override >/override; exec sleep infinity",
                "test -s /override && test ! -e /started",
            ),
            ("disabled", "", "test ! -e /started && test ! -e /override"),
        ]:
            with self.subTest(name=name):
                build = self.build(name, "--start-cmd", start, "--ready-cmd", ready)
                with self.sandbox(build) as sandbox:
                    self.cli("exec", sandbox, "/bin/sh", "-c", ready)
        self.assert_released()

    def test_concurrent_builds(self):
        seed, _ = self.result(
            self.build("seed"), (self.context / "input.txt").read_text().strip()
        )
        builds = []
        for name in ("parallel-a", "parallel-b"):
            context = self.work / name
            shutil.copytree(self.context, context)
            (context / "input.txt").write_text(name + "\n")
            builds.append(
                (
                    name,
                    self.launch_build(
                        name, "--build-arg", "BUILD_DELAY=30", context=context
                    ),
                )
            )
        for _, build in builds:
            self.wait_running(build)
        self.assertTrue(
            all(build[0].poll() is None for _, build in builds),
            "workers did not overlap",
        )
        if os.environ.get("E2E_MODE") == "compose":
            self.wait_for(
                lambda: (
                    sum(
                        counts[0] > self.baseline[node][0]
                        for node, counts in self.node_counts().items()
                    )
                    >= 2
                ),
                "concurrent builders on different nodes",
            )
        for name, build in builds:
            self.finish(build)
            self.assertEqual(self.result(build, name)[0], seed)
        self.assert_released()

    def test_failure_cancellation_and_worker_isolation(self):
        seed, _ = self.result(
            self.build("seed"), (self.context / "input.txt").read_text().strip()
        )
        (self.context / "Dockerfile").write_text("FROM alpine:3.22\nRUN exit 17\n")
        failed = self.launch_build("failure")
        self.finish(failed, success=False)
        self.assertIn("exit code: 17", failed[1].read_text())
        (self.context / "Dockerfile").write_text(
            "FROM alpine:3.22\nRUN echo worker-running; sleep 300\n"
        )
        cancelled = self.launch_build("cancelled")
        self.wait_running(cancelled)
        build_id = self.build_id(cancelled)
        status, sandboxes = self.api("GET", "/sandboxes")
        self.assertEqual(status, 200, sandboxes)
        self.assertNotIn(build_id, json.dumps(sandboxes))
        status, body = self.api("GET", f"/sandboxes/{build_id}")
        self.assertEqual(status, 404, body)
        status, body = self.api(
            "DELETE", f"/templates/{build_id}/builds/{build_id}/builder", key="invalid"
        )
        self.assertEqual(status, 401, body)
        self.assertIsNone(
            cancelled[0].poll(), "unauthenticated request stopped the worker"
        )
        cancelled[0].terminate()
        self.finish(cancelled, success=False)
        self.assert_released()
        shutil.copyfile(ROOT / "fixture/Dockerfile", self.context / "Dockerfile")
        (self.context / "input.txt").write_text("after-failure\n")
        self.assertEqual(self.result(self.build("recovered"), "after-failure")[0], seed)
        self.assert_released()

    def test_deadline_releases_worker(self):
        self.build("warm")
        (self.context / "Dockerfile").write_text(
            "FROM alpine:3.22\nRUN echo worker-running; sleep 300\n"
        )
        expired = self.launch_build("deadline", timeout=30)
        self.wait_running(expired)
        expired[0].wait(timeout=90)
        self.finish(expired, success=False)
        self.assert_released()

    def test_numeric_users(self):
        for identity, uid, gid in [
            ("0", "0", "0"),
            ("65534", "65534", "65534"),
            ("12345", "12345", "0"),
            ("12345:23456", "12345", "23456"),
        ]:
            with self.subTest(identity=identity):
                build = self.build(
                    identity.replace(":", "-"),
                    "--build-arg",
                    f"RUN_AS={identity}",
                    context=ROOT / "users",
                )
                with self.sandbox(build) as sandbox:
                    self.assertEqual(
                        self.cli("exec", sandbox, "cat", "/tmp/uid", "/tmp/gid"),
                        f"{uid}\n{gid}",
                    )
                    self.assertEqual(self.cli("exec", sandbox, "id", "-u"), uid)
                    self.assertEqual(self.cli("exec", sandbox, "id", "-g"), gid)
        self.assert_released()

    def test_external_dockerfile_and_secret(self):
        secret = self.work / "secret"
        secret.write_text("buildkit-test-secret\n")
        dockerfile = self.work / "Custom.Dockerfile"
        dockerfile.write_text(
            "FROM alpine:3.22\nCOPY input.txt /result.txt\n"
            "RUN --mount=type=secret,id=token,required=true sha256sum /run/secrets/token | cut -d' ' -f1 >/secret-hash\n"
            'CMD ["sleep", "infinity"]\n'
        )
        build = self.build(
            "secret", "-f", str(dockerfile), "--secret", f"id=token,src={secret}"
        )
        with self.sandbox(build) as sandbox:
            self.assertEqual(
                self.cli("exec", sandbox, "cat", "/result.txt"),
                (self.context / "input.txt").read_text().strip(),
            )
            self.assertEqual(
                self.cli("exec", sandbox, "cat", "/secret-hash"),
                hashlib.sha256(secret.read_bytes()).hexdigest(),
            )
            self.cli(
                "exec",
                sandbox,
                "/bin/sh",
                "-c",
                "test ! -e /run/secrets/token && test ! -e /secret",
            )
        self.assert_released()

    @unittest.skipUnless(
        sys.platform == "linux" and shutil.which("script"), "requires util-linux script"
    )
    def test_terminal_progress(self):
        build = self.build("progress", tty=True)
        for stage in ("0/3", "1/3", "2/3"):
            self.assertIn(stage, build[1].read_text())
        self.result(build, (self.context / "input.txt").read_text().strip())
        self.assert_released()


if __name__ == "__main__":
    unittest.main(verbosity=2)
