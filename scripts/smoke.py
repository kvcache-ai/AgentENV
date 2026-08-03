#!/usr/bin/env python3
"""Smoke-test an AgentENV server through the official E2B Python SDK.

Covers the sandbox lifecycle an agent workload actually depends on: create,
exec, filesystem I/O, pause, resume (via connect), and kill. Each step is
timed so the snapshot-restore latency AgentENV advertises is visible.

Usage:
    pip install e2b
    export E2B_API_URL=http://127.0.0.1:8000
    export E2B_SANDBOX_URL=$E2B_API_URL
    export E2B_API_KEY=e2b_000000
    export E2B_ACCESS_TOKEN=dummy
    python3 scripts/smoke.py [--template <name-or-id>]
"""

import argparse
import json
import os
import sys
import time
import urllib.request

DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_API_KEY = "e2b_0000000000000000000000000000000000000000"


class StepFailed(Exception):
    pass


def env_defaults() -> None:
    os.environ.setdefault("E2B_API_URL", DEFAULT_API_URL)
    os.environ.setdefault("E2B_SANDBOX_URL", os.environ["E2B_API_URL"])
    os.environ.setdefault("E2B_API_KEY", DEFAULT_API_KEY)
    os.environ.setdefault("E2B_ACCESS_TOKEN", "dummy")


def http_get_json(path: str):
    request = urllib.request.Request(
        os.environ["E2B_API_URL"].rstrip("/") + path,
        headers={"X-API-Key": os.environ["E2B_API_KEY"]},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read())


def pick_template() -> str:
    templates = http_get_json("/templates")
    ready = [t for t in templates if t.get("buildStatus") == "ready"]
    if not ready:
        raise StepFailed(
            "no template with buildStatus=ready; build one first, e.g.\n"
            "  aenv pull <registry>/ubuntu:22.04 --name u22"
        )
    ready.sort(key=lambda t: t.get("createdAt", ""), reverse=True)
    chosen = ready[0]
    name = (chosen.get("names") or chosen.get("aliases") or [chosen["templateID"]])[0]
    print(f"  using template {name} ({chosen['templateID']})")
    return chosen["templateID"]


def heartbeat_count(sandbox) -> int:
    result = sandbox.commands.run("wc -l < /root/heartbeat")
    return int(result.stdout.strip() or 0)


class Timer:
    """Record step durations so the summary can show restore latency."""

    def __init__(self) -> None:
        self.steps: list[tuple[str, float]] = []

    def run(self, label: str, fn):
        print(f"[ .. ] {label}")
        started = time.monotonic()
        result = fn()
        elapsed = time.monotonic() - started
        self.steps.append((label, elapsed))
        print(f"[ ok ] {label} ({elapsed * 1000:.0f} ms)")
        return result

    def report(self) -> None:
        print("\n--- timings ---")
        for label, elapsed in self.steps:
            print(f"{elapsed * 1000:9.0f} ms  {label}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--template",
        help="template name or ID; defaults to the newest ready template",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="sandbox TTL in seconds (default: 300)",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="leave the sandbox running instead of killing it",
    )
    args = parser.parse_args()

    env_defaults()

    try:
        from e2b import Sandbox, SandboxQuery, SandboxState
    except ImportError:
        print("e2b SDK is missing; install it with `pip install e2b`", file=sys.stderr)
        return 1

    print("E2B_API_URL     =", os.environ["E2B_API_URL"])
    print("E2B_SANDBOX_URL =", os.environ["E2B_SANDBOX_URL"])

    timer = Timer()
    sandbox = None

    try:
        template = args.template or pick_template()

        sandbox = timer.run(
            "create sandbox",
            lambda: Sandbox.create(template, timeout=args.timeout, secure=False),
        )
        print("  sandbox id:", sandbox.sandbox_id)

        result = timer.run(
            "run command",
            lambda: sandbox.commands.run(
                "cat /etc/os-release | head -1; uname -r; id -u"
            ),
        )
        print("  stdout:", " | ".join(result.stdout.strip().splitlines()))
        if result.exit_code != 0:
            raise StepFailed(f"command exited with {result.exit_code}: {result.stderr}")

        marker = f"agentenv-e2b-{int(time.time())}"
        timer.run(
            "write file",
            lambda: sandbox.files.write("/root/smoke-marker", marker),
        )
        readback = timer.run(
            "read file", lambda: sandbox.files.read("/root/smoke-marker")
        )
        if readback.strip() != marker:
            raise StepFailed(f"file content mismatch: {readback!r} != {marker!r}")

        # A ticking background process is what distinguishes a real memory
        # snapshot from a filesystem-only save: after resume it must keep
        # appending without having been restarted.
        timer.run(
            "start background process",
            lambda: sandbox.commands.run(
                "nohup sh -c 'while true; do date +%s >> /root/heartbeat; sleep 1; done' "
                ">/dev/null 2>&1 &"
            ),
        )
        time.sleep(3)
        before_pause = heartbeat_count(sandbox)
        print(f"  heartbeat lines before pause: {before_pause}")

        timer.run("pause sandbox", sandbox.pause)
        timer.run("resume sandbox (connect)", sandbox.connect)

        after = timer.run(
            "verify file state after resume",
            lambda: sandbox.commands.run("cat /root/smoke-marker"),
        )
        if after.stdout.strip() != marker:
            raise StepFailed(f"marker lost across pause/resume: {after.stdout!r}")

        time.sleep(3)
        after_pause = heartbeat_count(sandbox)
        print(f"  heartbeat lines after resume: {after_pause}")
        if after_pause <= before_pause:
            raise StepFailed(
                "background process did not resume ticking "
                f"({before_pause} -> {after_pause} lines)"
            )

        running = timer.run(
            "list running sandboxes",
            lambda: Sandbox.list(
                query=SandboxQuery(state=[SandboxState.RUNNING]), limit=20
            ).next_items(),
        )
        ids = [s.sandbox_id for s in running]
        print(f"  running: {len(ids)} sandbox(es)")
        if sandbox.sandbox_id not in ids:
            raise StepFailed("sandbox missing from the running list")

        if args.keep:
            print(f"\nleaving sandbox {sandbox.sandbox_id} running (--keep)")
        else:
            timer.run("kill sandbox", sandbox.kill)
            sandbox = None

        timer.report()
        print("\nRESULT: PASS")
        return 0

    except Exception as error:
        print(f"\nRESULT: FAIL — {type(error).__name__}: {error}", file=sys.stderr)
        if sandbox is not None and not args.keep:
            try:
                sandbox.kill()
                print("cleaned up sandbox", sandbox.sandbox_id, file=sys.stderr)
            except Exception as cleanup_error:
                print(f"cleanup failed: {cleanup_error}", file=sys.stderr)
        timer.report()
        return 1


if __name__ == "__main__":
    sys.exit(main())
