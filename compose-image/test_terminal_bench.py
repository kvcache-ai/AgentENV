#!/usr/bin/env python3
"""Build and start unmodified Terminal-Bench 4.0 Compose task environments.

This checks environment compatibility, not agent task-solving scores. Clone
harbor-framework/terminal-bench at v4.0.0 separately, then point --tasks at its
tasks directory. Requires an isolated Compose-enabled node and a registry
reachable by both its builder VMs and image resolver. See the Compose docs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=Path, required=True)
    parser.add_argument("--image-repository", required=True)
    parser.add_argument("--registry-insecure", action="store_true")
    parser.add_argument("--task", action="append", help="Only these task directory names")
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    cli = os.environ.get("AENV_CLI", "aenv")
    api = os.environ["AENV_API_URL"].rstrip("/")
    key = os.environ["AENV_API_KEY"]
    args.results.mkdir(parents=True, exist_ok=False)
    sources = sorted(args.tasks.glob("*/environment/docker-compose.yaml"))
    if args.task:
        sources = [p for p in sources if p.parent.parent.name in args.task]
        if {p.parent.parent.name for p in sources} != set(args.task):
            parser.error("a requested task has no environment/docker-compose.yaml")
    if not sources:
        parser.error("no Compose tasks found")
    report = []
    with tempfile.TemporaryDirectory(prefix="aenv-tb4-") as credentials:
        config = Path(credentials) / "aenv"
        config.mkdir()
        credential_file = config / "credentials"
        credential_file.write_text(f"url = {json.dumps(api)}\napi_key = {json.dumps(key)}\n")
        credential_file.chmod(0o600)
        env = dict(os.environ, XDG_CONFIG_HOME=credentials)

        def execute(sandbox, *command):
            return subprocess.check_output([cli, "exec", sandbox, *command], env=env,
                                           text=True, timeout=120).strip()

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
                    command = [cli, "build", "--compose", str(source.resolve()), "--harbor",
                               "--image-repository", args.image_repository,
                               "--output", str(built.resolve()), "--progress", "plain"]
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
                    inspected = json.loads(execute(sandbox, "docker", "inspect",
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
                    execute(sandbox, "docker", "exec", "aenv-main-1", "sh", "-c", "true")
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
    main()
