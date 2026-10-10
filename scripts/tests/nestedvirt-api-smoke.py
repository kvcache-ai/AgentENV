# Copyright 2026 AgentENV contributors.
# SPDX-License-Identifier: Apache-2.0
import argparse
import http.client
import json
import re
import time
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

from e2b import Sandbox

parser = argparse.ArgumentParser()
parser.add_argument("endpoint")
parser.add_argument("api_key_file", type=Path)
parser.add_argument("image")
parser.add_argument("evidence", type=Path)
parser.add_argument("--rounds", type=int, default=3)
parser.add_argument("--phase", choices=("unused", "initialized", "running"))
args = parser.parse_args()
if args.rounds < 1:
    parser.error("--rounds must be positive")
endpoint = urlsplit(args.endpoint)
if endpoint.scheme != "http":
    raise ValueError("This local hardware smoke expects an HTTP endpoint")
key = args.api_key_file.read_text().strip()
configuration = Path(__file__).resolve().parents[2] / "config/nestedvirt.toml"
expected_boot_args = tomllib.loads(configuration.read_text())["firecracker"][
    "boot_args"
].split()
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
    if "6.18.0-agentenv-nestedvirt" not in text:
        raise RuntimeError(f"Wrong guest kernel: {text}")
    missing = set(expected_boot_args) - set(text.split())
    if missing:
        raise RuntimeError(f"Guest boot arguments omit {sorted(missing)}")


def start_inner(sandbox, stopped=False):
    command(
        sandbox,
        f"mkdir -p /run/nestedvirt-probe; /start-inner{' -S' if stopped else ''} >/run/nestedvirt-probe/inner.log 2>&1 & echo $! >/run/nestedvirt-probe/inner.pid",
    )
    command(sandbox, "sleep 2; kill -0 $(cat /run/nestedvirt-probe/inner.pid)")
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


def replace_inner(sandbox, phase):
    if phase != "unused":
        command(sandbox, "/stop-inner /run/nestedvirt-probe/inner.pid")
    start_inner(sandbox)
    command(sandbox, "/stop-inner /run/nestedvirt-probe/inner.pid")


template = api(
    "POST",
    "/v3/templates",
    {"name": f"nestedvirt-smoke-{time.time_ns()}", "cpuCount": 2, "memoryMB": 512},
)
tid, bid = template["templateID"], template["buildID"]
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

for phase in [args.phase] if args.phase else ("unused", "initialized", "running"):
    for iteration in range(args.rounds):
        sandboxes = []
        try:
            source = api("POST", "/v2/sandboxes", {"templateID": tid, "timeout": 600})
            sandboxes.append(source)
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
            if len(sandboxes) != 3:
                raise RuntimeError(
                    f"Expected two fork children: {[o.get('error') for o in forks]}"
                )
            for sandbox in sandboxes:
                check(sandbox)
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
            (args.evidence / "api-results.json").write_text(
                json.dumps(results, indent=2) + "\n"
            )
            print(json.dumps(results[-1]), flush=True)
        finally:
            for sandbox in sandboxes:
                api("DELETE", f"/sandboxes/{sandbox['sandboxID']}")
                clients.pop(sandbox["sandboxID"], None)
