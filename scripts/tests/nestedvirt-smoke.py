# Copyright 2026 AgentENV contributors.
# SPDX-License-Identifier: Apache-2.0
"""Prepare an OCI fixture and test nested KVM through a running AgentENV node."""
import argparse
import contextlib
import hashlib
import http.client
import http.server
import itertools
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

GUEST_SCRIPT = Path(__file__).with_name("nestedvirt-guest.sh")


def copy_binary(path, root):
    """Copy a host executable and its shared libraries into the fixture."""
    result = subprocess.run(["ldd", path], text=True, capture_output=True, check=True)
    for source in [path] + [word for word in result.stdout.split() if word.startswith("/")]:
        target = root / source.lstrip("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def prepare_fixture(work, inner_kernel):
    """Build fresh outer and inner roots inside this run's temporary directory."""
    root, inner = work / "root", work / "inner"
    for base in (root, inner):
        for name in ("bin", "dev", "proc", "sys", "run", "root"):
            (base / name).mkdir(parents=True)
        shutil.copy2("/bin/busybox", base / "bin/busybox")
        for name in subprocess.check_output(["/bin/busybox", "--list"], text=True).splitlines():
            if name != "busybox":
                (base / "bin" / name).symlink_to("busybox")
        shutil.copy2(GUEST_SCRIPT, base / "nestedvirt-guest")
        (base / "nestedvirt-guest").chmod(0o755)
    (root / "init").symlink_to("nestedvirt-guest")
    (inner / "init").write_text("#!/bin/sh\nexec /nestedvirt-guest inner-init\n")
    (inner / "init").chmod(0o755)
    with (root / "inner.cpio").open("wb") as output:
        subprocess.run(
            ["bash", "-o", "pipefail", "-c", "find . -print0 | cpio --null -o --format=newc"],
            cwd=inner, stdout=output, check=True,
        )
    shutil.copy2(inner_kernel, root / "inner-bzImage")
    copy_binary("/usr/bin/qemu-system-x86_64", root)
    copy_binary("/bin/bash", root)
    for source in (Path("/usr/share/qemu"), Path("/usr/share/seabios")):
        if source.exists():
            shutil.copytree(source, root / source.relative_to("/"))
    return root


@contextlib.contextmanager
def serve_fixture(root, port):
    """Serve the current root as an OCI image for the duration of the API test."""
    archive = root.parent / "fixture.tar"
    with tarfile.open(archive, "w") as output:
        output.add(root, arcname=".")
    layer = archive.read_bytes()
    digest = lambda data: "sha256:" + hashlib.sha256(data).hexdigest()
    ld = digest(layer)
    config = json.dumps({
        "architecture": "amd64", "os": "linux", "config": {},
        "rootfs": {"type": "layers", "diff_ids": [ld]},
    }).encode()
    cd = digest(config)
    manifest_type = "application/vnd.oci.image.manifest.v1+json"
    manifest = json.dumps({
        "schemaVersion": 2, "mediaType": manifest_type,
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": cd, "size": len(config)},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": ld, "size": len(layer)}],
    }).encode()
    blobs = {ld: layer, cd: config}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.respond(False)

        def do_GET(self):
            self.respond(True)

        def respond(self, include_body):
            if self.path == "/v2/":
                body, kind = b"{}", "application/json"
            elif self.path.startswith("/v2/nested/manifests/"):
                body, kind = manifest, manifest_type
            elif self.path.startswith("/v2/nested/blobs/") and self.path.rsplit("/", 1)[-1] in blobs:
                body, kind = blobs[self.path.rsplit("/", 1)[-1]], "application/octet-stream"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Docker-Content-Digest", digest(body))
            self.end_headers()
            if include_body:
                self.wfile.write(body)

    with http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield f"127.0.0.1:{server.server_port}/nested:latest"
        finally:
            server.shutdown()
            thread.join()


def check_fixture():
    """Check real OCI rebuilding and registry cleanup without KVM or the SDK."""
    with tempfile.TemporaryDirectory(prefix="nestedvirt-fixture-check-") as directory:
        root = Path(directory) / "root"
        root.mkdir()
        (root / "retained").write_text("current\n")
        (root / "removed").write_text("old\n")
        shutil.copy2(GUEST_SCRIPT, root / "nestedvirt-guest")
        (root / "init").symlink_to("nestedvirt-guest")
        for iteration in range(2):
            if iteration:
                (root / "removed").unlink()
                (root / "retained").write_text("updated\n")
            with serve_fixture(root, 0) as image:
                connection = http.client.HTTPConnection(image.split("/")[0], timeout=5)
                try:
                    connection.request("GET", "/v2/nested/manifests/latest")
                    response = connection.getresponse()
                    if response.status != 200:
                        raise RuntimeError(f"Manifest returned HTTP {response.status}")
                    manifest = json.loads(response.read())
                finally:
                    connection.close()
                archive_path = root.parent / "fixture.tar"
                if manifest["layers"][0]["size"] != archive_path.stat().st_size:
                    raise RuntimeError("Fixture layer size mismatch")
                with tarfile.open(archive_path) as archive:
                    if ("./removed" in archive.getnames()) != (iteration == 0):
                        raise RuntimeError("Fixture retained a removed file")
                    expected = b"updated\n" if iteration else b"current\n"
                    if archive.extractfile("./retained").read() != expected:
                        raise RuntimeError("Fixture content mismatch")
                    if archive.extractfile("./init").read() != GUEST_SCRIPT.read_bytes():
                        raise RuntimeError("Fixture init mismatch")
    print("PASS fixture rebuild and registry cleanup")


def run_api(args):
    endpoint = urlsplit(args.endpoint)
    if endpoint.scheme != "http":
        raise ValueError("This local hardware smoke expects an HTTP endpoint")
    expected_boot_args = tomllib.loads(args.config.read_text())["firecracker"][
        "boot_args"
    ].split()
    if "no-kvmapf" not in expected_boot_args:
        raise ValueError(
            "The selected node config must append no-kvmapf to its complete firecracker.boot_args"
        )

    from e2b import Sandbox

    key = args.api_key_file.read_text().strip()
    args.evidence.mkdir(parents=True, exist_ok=True)
    results = []
    clients = {}
    (args.evidence / "api-results.json").write_text("[]\n")


    def api(method, path, body=None):
        connection = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=180
        )
        connection.request(
            method,
            path,
            None if body is None else json.dumps(body),
            {"Content-Type": "application/json", "X-API-Key": key},
        )
        response = connection.getresponse()
        data = response.read()
        connection.close()
        if not 200 <= response.status < 300:
            raise RuntimeError(f"{method} {path}: {response.status} {data.decode()}")
        return json.loads(data) if data else None


    def command(sandbox, shell):
        sandbox_id = sandbox["sandboxID"]
        if sandbox_id not in clients:
            clients[sandbox_id] = Sandbox.connect(
                sandbox_id,
                timeout=600,
                api_key=key,
                api_url=args.endpoint,
                sandbox_url=args.endpoint,
                http_version="1.1",
                retries=0,
            )
        with (args.evidence / "api-commands.jsonl").open("a") as log:
            log.write(
                json.dumps(
                    {
                        "event": "start",
                        "sandbox": sandbox_id,
                        "command": shell,
                        "phase": phase,
                        "iteration": iteration,
                        "started_at": time.time(),
                    }
                )
                + "\n"
            )
        result = clients[sandbox_id].commands.run(
            shell, user="root", timeout=90, request_timeout=100
        )
        with (args.evidence / "api-commands.jsonl").open("a") as log:
            log.write(
                json.dumps(
                    {
                        "event": "complete",
                        "sandbox": sandbox_id,
                        "command": shell,
                        "phase": phase,
                        "iteration": iteration,
                        "exit_code": result.exit_code,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    }
                )
                + "\n"
            )
        if result.exit_code != 0:
            raise RuntimeError(f"Guest command failed with exit code {result.exit_code}")
        return result.stdout


    def ticks(sandbox):
        text = command(sandbox, "sleep 3; cat /run/nestedvirt-probe/inner.log")
        values = re.findall(r"INNER_TICK=(\d+)", text)
        if not values:
            raise RuntimeError(f"Inner guest did not run: {text[-4000:]}")
        return int(values[-1])


    def advancing_ticks(sandbox):
        first, second = ticks(sandbox), ticks(sandbox)
        if second <= first:
            raise RuntimeError(f"Inner guest stopped advancing: {first} -> {second}")
        return second


    def check(sandbox):
        text = command(
            sandbox,
            "test -c /dev/kvm && test $(grep -c '^processor' /proc/cpuinfo) -eq 2 && uname -r && cat /proc/cmdline",
        )
        if args.kernel_release not in text.splitlines():
            raise RuntimeError(f"Wrong guest kernel: {text}")
        missing = set(expected_boot_args) - set(text.split())
        if missing:
            raise RuntimeError(f"Guest boot arguments omit {sorted(missing)}")


    def start_inner(sandbox, stopped=False):
        command(
            sandbox,
            f"mkdir -p /run/nestedvirt-probe; /nestedvirt-guest start-inner{' -S' if stopped else ''} >/run/nestedvirt-probe/inner.log 2>&1 & echo $! >/run/nestedvirt-probe/inner.pid",
        )
        if stopped:
            initialized_inner(sandbox)
        if not stopped:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                text = command(
                    sandbox, "sleep 1; tail -n 30 /run/nestedvirt-probe/inner.log"
                )
                values = re.findall(r"INNER_TICK=(\d+)", text)
                if values:
                    return advancing_ticks(sandbox)
            raise TimeoutError(f"Inner guest did not execute: {text}")


    def initialized_inner(sandbox):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            text = command(
                sandbox,
                "printf '%s\\n' '{\"execute\":\"qmp_capabilities\"}' "
                "'{\"execute\":\"query-kvm\",\"id\":\"kvm\"}' "
                "'{\"execute\":\"query-status\",\"id\":\"status\"}' "
                "| /bin/busybox nc -w 1 127.0.0.1 4444; true",
            )
            replies = {}
            for line in text.splitlines():
                if line.startswith('{"'):
                    reply = json.loads(line)
                    if "id" in reply:
                        replies[reply["id"]] = reply.get("return", {})
            if (
                replies.get("kvm", {}).get("enabled") is True
                and replies.get("kvm", {}).get("present") is True
                and replies.get("status", {}).get("status") == "prelaunch"
                and replies.get("status", {}).get("running") is False
            ):
                return
        raise TimeoutError(f"Inner KVM did not initialize in stopped state: {text}")


    @contextlib.contextmanager
    def cleanup_resources():
        paths = []
        try:
            yield paths
        finally:
            original = sys.exc_info()[1]
            errors = []
            for path in reversed(paths):
                try:
                    api("DELETE", path)
                except Exception as error:
                    errors.append(error)
                finally:
                    if path.startswith("/sandboxes/"):
                        clients.pop(path.rsplit("/", 1)[-1], None)
            if errors:
                # Preserve the original failure and all cleanup errors; fail a successful
                # run if cleanup fails.
                failure = ExceptionGroup("Resource cleanup failed", errors)
                if original is not None:
                    original.add_note(str(failure) + ": " + repr(errors))
                else:
                    raise failure


    def replace_inner(sandbox, phase):
        if phase != "unused":
            command(sandbox, "/nestedvirt-guest stop-inner /run/nestedvirt-probe/inner.pid")
        start_inner(sandbox)
        command(sandbox, "/nestedvirt-guest stop-inner /run/nestedvirt-probe/inner.pid")


    with cleanup_resources() as templates:
        template = api(
            "POST",
            "/v3/templates",
            {"name": f"nestedvirt-smoke-{time.time_ns()}", "cpuCount": 2, "memoryMB": 512},
        )
        tid, bid = template["templateID"], template["buildID"]
        templates.append(f"/templates/{tid}")
        api("POST", f"/v2/templates/{tid}/builds/{bid}", {"fromImage": args.image})
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            status = api("GET", f"/templates/{tid}/builds/{bid}/status")
            if status["status"] == "ready":
                break
            if status["status"] == "error":
                raise RuntimeError(f"Template build failed: {status}")
            time.sleep(1)
        else:
            raise TimeoutError("Template build did not complete")
        print(json.dumps({"template": tid, "build": "PASS"}), flush=True)

        for phase, iteration in itertools.product(
            [args.phase] if args.phase else ("unused", "initialized", "running"),
            range(args.rounds),
        ):
            sandboxes = []
            with cleanup_resources() as sandbox_paths:
                source = api("POST", "/v2/sandboxes", {"templateID": tid, "timeout": 600})
                sandboxes.append(source)
                sandbox_paths.append(f"/sandboxes/{source['sandboxID']}")
                check(source)
                before = None
                if phase != "unused":
                    before = start_inner(source, phase == "initialized")
                forks = api(
                    "POST",
                    f"/sandboxes/{source['sandboxID']}/fork",
                    {"count": 2, "timeout": 600},
                )
                sandboxes.extend(
                    outcome["sandbox"] for outcome in forks if outcome.get("sandbox")
                )
                sandbox_paths.extend(
                    f"/sandboxes/{sandbox['sandboxID']}" for sandbox in sandboxes[1:]
                )
                if len(sandboxes) != 3:
                    raise RuntimeError(
                        f"Expected two fork children: {[o.get('error') for o in forks]}"
                    )
                for sandbox in sandboxes:
                    check(sandbox)
                    if phase == "initialized":
                        initialized_inner(sandbox)
                    if phase == "running" and advancing_ticks(sandbox) <= before:
                        raise RuntimeError("Inner guest failed to continue across fork")
                before_pause = ticks(source) if phase == "running" else None
                api("POST", f"/sandboxes/{source['sandboxID']}/pause")
                source = api(
                    "POST", f"/v2/sandboxes/{source['sandboxID']}/connect", {"timeout": 600}
                )
                clients.pop(source["sandboxID"], None)
                sandboxes[0] = source
                check(source)
                if phase == "initialized":
                    initialized_inner(source)
                if phase == "running" and advancing_ticks(source) <= before_pause:
                    raise RuntimeError("Inner guest failed to continue after pause/resume")
                for sandbox in sandboxes:
                    replace_inner(sandbox, phase)
            results.append(
                {
                    "phase": phase,
                    "iteration": iteration,
                    "template": tid,
                    "forks": 2,
                    "pause_resume": True,
                    "new_inner_after_restore": True,
                    "result": "PASS",
                }
            )
            print(json.dumps(results[-1]), flush=True)

    # Publish the complete results only after the template and every sandbox are cleaned up.
    (args.evidence / "api-results.json").write_text(json.dumps(results, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check-fixture", help="Check OCI rebuilding without KVM")
    run = commands.add_parser("run", help="Prepare the fixture and run the API matrix")
    run.add_argument("endpoint")
    run.add_argument("api_key_file", type=Path)
    run.add_argument("work", type=Path)
    run.add_argument("--inner-kernel", type=Path, required=True)
    run.add_argument("--kernel-release", required=True, help="Expected outer guest uname -r")
    run.add_argument("--port", type=int, default=18091, help="Loopback registry port")
    run.add_argument("--rounds", type=int, default=3)
    run.add_argument("--phase", choices=("unused", "initialized", "running"))
    run.add_argument(
        "--config", type=Path,
        default=Path(os.environ.get("AENV_CONFIG_PATH")
                     or Path(__file__).resolve().parents[2] / "config/default.toml"),
    )
    args = parser.parse_args()
    if args.command == "check-fixture":
        check_fixture()
        return
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    args.work = args.work.resolve()
    args.inner_kernel = args.inner_kernel.resolve(strict=True)
    args.evidence = args.work / "evidence"
    args.evidence.mkdir(parents=True, exist_ok=True)
    (args.evidence / "api-results.json").write_text("[]\n")
    with tempfile.TemporaryDirectory(prefix="fixture-", dir=args.work) as directory:
        root = prepare_fixture(Path(directory), args.inner_kernel)
        with serve_fixture(root, args.port) as args.image:
            run_api(args)


if __name__ == "__main__":
    main()
