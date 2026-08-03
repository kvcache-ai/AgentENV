#!/usr/bin/env python3
"""Concurrency benchmark for the AgentENV sandbox API.

Drives the HTTP API directly (no SDK in the loop) so the numbers reflect
server-side latency instead of client library overhead. Scenarios mirror the
lifecycle operations an agent workload depends on: create, pause, resume,
snapshot and fork.

Each invocation measures ONE concurrency tier so tiers can be swept from a
shell loop without cross-tier interference:

    python3 scripts/bench.py create -c 1  -n 20 -w 3
    python3 scripts/bench.py create -c 10 -n 200 -w 3 --no-header
    python3 scripts/bench.py pause-resume -c 10 --rounds 5

Usage:
    export AENV_API_URL=http://127.0.0.1:8000
    export AENV_API_KEY=e2b_000000
    export AENV_TEMPLATE_ID=<template-or-snapshot-id>   # optional
    python3 scripts/bench.py create -c 20 -n 200

Pass --metrics-url to additionally report the server-side stage breakdown
scraped from a node's Prometheus endpoint over the measured window.
"""

import argparse
import json
import math
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from urllib.parse import urlsplit

DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_API_KEY = "e2b_000000"

# 
# Server-side stage metrics worth reporting next to client-side latency. Each
# entry is a Prometheus metric name plus the label subset identifying one stage.
SERVER_STAGES = [
    ("agentenv_sandbox_stage_duration_seconds", {"stage": "load_snapshot"}),
    ("agentenv_sandbox_stage_duration_seconds", {"stage": "create_sandbox"}),
    ("agentenv_sandbox_stage_duration_seconds", {"stage": "pause"}),
    ("agentenv_sandbox_stage_duration_seconds", {"stage": "resume"}),
    ("agentenv_ublk_operation_duration_seconds", {"operation": "create_runtime_overlaybd"}),
    ("agentenv_ublk_operation_duration_seconds", {"operation": "acquire_shared_memory"}),
    ("agentenv_ublk_operation_duration_seconds", {"operation": "restack_snapshot"}),
    ("agentenv_ublk_operation_duration_seconds", {"operation": "release"}),
]


class BenchError(Exception):
    pass


class ApiClient:
    """Keep-alive HTTP client with one connection per worker thread.

    A fresh connection per request would add a TCP handshake to every sample,
    which is a large fraction of the operations being measured here.
    """

    def __init__(self, base_url: str, api_key: str, timeout: float = 120.0):
        split = urlsplit(base_url)
        if not split.hostname:
            raise BenchError(f"invalid API URL: {base_url!r}")
        self.host = split.hostname
        self.port = split.port or (443 if split.scheme == "https" else 80)
        self.https = split.scheme == "https"
        self.prefix = split.path.rstrip("/")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._local = threading.local()

    def _connection(self) -> HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            if self.https:
                from http.client import HTTPSConnection

                conn = HTTPSConnection(self.host, self.port, timeout=self.timeout)
            else:
                conn = HTTPConnection(self.host, self.port, timeout=self.timeout)
            self._local.conn = conn
        return conn

    def call(self, method: str, path: str, body=None):
        """Return (elapsed_ms, parsed_json_or_None). Raises BenchError on failure."""
        payload = json.dumps(body).encode() if body is not None else None
        headers = {"X-API-Key": self.api_key, "Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"

        for attempt in (0, 1):
            conn = self._connection()
            started = time.monotonic()
            try:
                conn.request(method, self.prefix + path, body=payload, headers=headers)
                response = conn.getresponse()
                raw = response.read()
            except (OSError, EOFError) as error:
                # A pooled connection closed by the server surfaces on the next
                # request; drop it and retry once before reporting a failure.
                self._local.conn = None
                try:
                    conn.close()
                except Exception:
                    pass
                if attempt == 0:
                    continue
                raise BenchError(f"{method} {path}: {error}") from error

            elapsed_ms = (time.monotonic() - started) * 1000
            if response.status >= 400:
                detail = raw.decode("utf-8", "replace")[:200]
                raise BenchError(f"{method} {path}: HTTP {response.status}: {detail}")
            parsed = json.loads(raw) if raw else None
            return elapsed_ms, parsed

        raise BenchError(f"{method} {path}: unreachable")

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None


def percentile(values, pct: float) -> float:
    ordered = sorted(values)
    index = int(math.ceil(len(ordered) * pct / 100.0)) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


class Samples:
    """Latency samples for one operation, in milliseconds."""

    def __init__(self):
        self.values: list[float] = []
        self.errors: list[str] = []
        self._lock = threading.Lock()

    def add(self, value: float) -> None:
        with self._lock:
            self.values.append(value)

    def fail(self, message: str) -> None:
        with self._lock:
            self.errors.append(message)

    @property
    def ok(self) -> int:
        return len(self.values)

    def stats(self) -> dict:
        summary = {
            "count": len(self.values),
            "errors": len(self.errors),
            "error_samples": list(dict.fromkeys(self.errors))[:3],
            # Completion-ordered samples expose drift that percentiles hide,
            # such as latency growing with the number of live sandboxes.
            "samples_ms": [round(value, 2) for value in self.values],
        }
        if not self.values:
            return summary
        summary.update(
            {
                "avg": statistics.fmean(self.values),
                "min": min(self.values),
                "p50": percentile(self.values, 50),
                "p90": percentile(self.values, 90),
                "p95": percentile(self.values, 95),
                "p99": percentile(self.values, 99),
                "max": max(self.values),
            }
        )
        return summary


def parse_prom_line(line: str):
    """Split one Prometheus sample line into (name, labels, value)."""
    if not line or line.startswith("#"):
        return None
    name, _, rest = line.partition("{")
    if rest:
        labels_text, _, value_text = rest.partition("}")
        labels = {}
        for pair in labels_text.split(","):
            key, _, raw = pair.partition("=")
            if key:
                labels[key.strip()] = raw.strip().strip('"')
    else:
        parts = line.split()
        if len(parts) != 2:
            return None
        name, value_text, labels = parts[0], parts[1], {}
    try:
        return name.strip(), labels, float(value_text.split()[0])
    except (ValueError, IndexError):
        return None


def scrape_metrics(metrics_url: str) -> dict:
    """Collect {(metric, frozen_labels): value} for histogram sum/count series."""
    request = urllib.request.Request(metrics_url)
    with urllib.request.urlopen(request, timeout=15) as response:
        text = response.read().decode("utf-8", "replace")

    collected = {}
    for line in text.splitlines():
        parsed = parse_prom_line(line)
        if parsed is None:
            continue
        name, labels, value = parsed
        if not (name.endswith("_sum") or name.endswith("_count")):
            continue
        collected[(name, tuple(sorted(labels.items())))] = value
    return collected


def server_stage_deltas(before: dict, after: dict) -> list[dict]:
    """Per-stage count and mean latency observed between two metric scrapes."""
    rows = []
    for metric, selector in SERVER_STAGES:
        sum_delta = 0.0
        count_delta = 0.0
        for (name, labels), value in after.items():
            if name not in (metric + "_sum", metric + "_count"):
                continue
            label_map = dict(labels)
            if any(label_map.get(key) != want for key, want in selector.items()):
                continue
            if label_map.get("status") not in (None, "ok"):
                continue
            delta = value - before.get((name, labels), 0.0)
            if name.endswith("_sum"):
                sum_delta += delta
            else:
                count_delta += delta
        if count_delta <= 0:
            continue
        stage = selector.get("stage") or selector.get("operation") or metric
        rows.append(
            {
                "stage": stage,
                "count": int(count_delta),
                "avg_ms": sum_delta / count_delta * 1000,
            }
        )
    return rows


def create_sandbox(client: ApiClient, template: str, timeout: int):
    elapsed_ms, body = client.call(
        "POST", "/sandboxes", {"templateID": template, "timeout": timeout}
    )
    return elapsed_ms, body["sandboxID"]


def delete_sandbox(client: ApiClient, sandbox_id: str) -> float:
    elapsed_ms, _ = client.call("DELETE", f"/sandboxes/{sandbox_id}")
    return elapsed_ms


def kill_all(client: ApiClient, sandbox_ids, concurrency: int) -> None:
    if not sandbox_ids:
        return
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        list(pool.map(lambda sid: _swallow(delete_sandbox, client, sid), sandbox_ids))


def _swallow(fn, *args):
    try:
        return fn(*args)
    except BenchError:
        return None


def run_parallel(concurrency: int, fn):
    """Run fn(index) `concurrency` times in parallel; return (wall_ms, results)."""
    started = time.monotonic()
    if concurrency == 1:
        results = [fn(0)]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(fn, range(concurrency)))
    return (time.monotonic() - started) * 1000, results


# --- scenarios ---------------------------------------------------------------


def scenario_create(client: ApiClient, args) -> dict:
    """cube-bench equivalent: N create (and optionally delete) iterations."""
    create = Samples()
    delete = Samples()
    created: list[str] = []
    created_lock = threading.Lock()

    def one(_index):
        try:
            elapsed_ms, sandbox_id = create_sandbox(client, args.template, args.timeout)
        except BenchError as error:
            create.fail(str(error))
            return
        create.add(elapsed_ms)
        if args.mode == "create-delete":
            try:
                delete.add(delete_sandbox(client, sandbox_id))
            except BenchError as error:
                delete.fail(str(error))
            return
        with created_lock:
            created.append(sandbox_id)

    def drain(count: int) -> None:
        """Keep `concurrency` requests in flight until `count` have completed."""
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            list(pool.map(one, range(count)))

    if args.warmup:
        drain(args.warmup * args.concurrency)
        create.values.clear()
        create.errors.clear()
        delete.values.clear()
        delete.errors.clear()
        kill_all(client, created, args.concurrency)
        created.clear()
        time.sleep(args.settle)

    started = time.monotonic()
    drain(args.total)
    wall_ms = (time.monotonic() - started) * 1000

    report = {
        "wall_ms": wall_ms,
        "per_op_ms": wall_ms / max(1, create.ok),
        "throughput_per_s": create.ok / (wall_ms / 1000) if wall_ms > 0 else 0.0,
        "operations": {"create": create.stats()},
    }
    if args.mode == "create-delete":
        report["operations"]["delete"] = delete.stats()
    else:
        report["kept_sandboxes"] = len(created)
        if not args.keep:
            kill_all(client, created, args.concurrency)
            report["kept_sandboxes"] = 0
    return report


def scenario_pause_resume(client: ApiClient, args) -> dict:
    """CubeSandbox 4.6 equivalent: pause then resume C sandboxes concurrently."""
    do_pause = args.scenario in ("pause", "pause-resume")
    do_resume = args.scenario in ("resume", "pause-resume")

    pause = Samples()
    resume = Samples()
    pause_walls: list[float] = []
    resume_walls: list[float] = []

    sandbox_ids = []
    try:
        for _ in range(args.concurrency):
            _, sandbox_id = create_sandbox(client, args.template, args.timeout)
            sandbox_ids.append(sandbox_id)

        def pause_one(index):
            try:
                elapsed_ms, _ = client.call("POST", f"/sandboxes/{sandbox_ids[index]}/pause")
                pause.add(elapsed_ms)
            except BenchError as error:
                pause.fail(str(error))

        def resume_one(index):
            try:
                elapsed_ms, _ = client.call(
                    "POST",
                    f"/sandboxes/{sandbox_ids[index]}/resume",
                    {"timeout": args.timeout},
                )
                resume.add(elapsed_ms)
            except BenchError as error:
                resume.fail(str(error))

        for round_index in range(args.warmup + args.rounds):
            measured = round_index >= args.warmup
            if do_pause:
                wall_ms, _ = run_parallel(args.concurrency, pause_one)
                if measured:
                    pause_walls.append(wall_ms)
            if do_resume:
                if not do_pause:
                    run_parallel(args.concurrency, pause_one)
                wall_ms, _ = run_parallel(args.concurrency, resume_one)
                if measured:
                    resume_walls.append(wall_ms)
            elif do_pause:
                # Restore the sandboxes so the next pause round has work to do.
                run_parallel(args.concurrency, resume_one)
            if not measured:
                pause.values.clear()
                pause.errors.clear()
                resume.values.clear()
                resume.errors.clear()
            time.sleep(args.settle)
    finally:
        kill_all(client, sandbox_ids, args.concurrency)

    report = {"operations": {}}
    if do_pause:
        report["operations"]["pause"] = pause.stats()
        report["pause_wall"] = wall_summary(pause_walls, args.concurrency)
    if do_resume:
        report["operations"]["resume"] = resume.stats()
        report["resume_wall"] = wall_summary(resume_walls, args.concurrency)
    return report


def scenario_snapshot(client: ApiClient, args) -> dict:
    """CubeSandbox 4.1 equivalent: snapshot C distinct sandboxes concurrently."""
    snapshot = Samples()
    walls: list[float] = []
    sandbox_ids = []
    snapshot_ids: list[str] = []
    snapshot_lock = threading.Lock()

    try:
        for _ in range(args.concurrency):
            _, sandbox_id = create_sandbox(client, args.template, args.timeout)
            sandbox_ids.append(sandbox_id)

        def snapshot_one(index):
            try:
                elapsed_ms, body = client.call(
                    "POST", f"/sandboxes/{sandbox_ids[index]}/snapshots", {}
                )
                snapshot.add(elapsed_ms)
                identifier = (body or {}).get("snapshotID")
                if identifier:
                    with snapshot_lock:
                        snapshot_ids.append(identifier)
            except BenchError as error:
                snapshot.fail(str(error))

        for round_index in range(args.warmup + args.rounds):
            wall_ms, _ = run_parallel(args.concurrency, snapshot_one)
            if round_index >= args.warmup:
                walls.append(wall_ms)
            else:
                snapshot.values.clear()
                snapshot.errors.clear()
            time.sleep(args.settle)
    finally:
        kill_all(client, sandbox_ids, args.concurrency)
        # Snapshots are exposed through the template API, which owns deletion.
        for identifier in snapshot_ids:
            _swallow(client.call, "DELETE", f"/templates/{identifier}")

    return {
        "operations": {"snapshot": snapshot.stats()},
        "snapshot_wall": wall_summary(walls, args.concurrency),
    }


def scenario_fork(client: ApiClient, args) -> dict:
    """CubeSandbox 4.5 (clone) equivalent: fork N children from one sandbox."""
    fork = Samples()
    walls: list[float] = []
    source_id = None
    try:
        _, source_id = create_sandbox(client, args.template, args.timeout)

        def fork_batch(_index):
            try:
                elapsed_ms, body = client.call(
                    "POST",
                    f"/sandboxes/{source_id}/fork",
                    {"count": args.total, "timeout": args.timeout},
                )
            except BenchError as error:
                fork.fail(str(error))
                return []
            children = [
                entry["sandbox"]["sandboxID"]
                for entry in (body or [])
                if entry.get("sandbox")
            ]
            failed = len(body or []) - len(children)
            for _ in range(failed):
                fork.fail("fork child failed to start")
            fork.add(elapsed_ms)
            return children

        for round_index in range(args.warmup + args.rounds):
            wall_ms, results = run_parallel(1, fork_batch)
            children = [sid for batch in results for sid in batch]
            if round_index >= args.warmup:
                walls.append(wall_ms)
            else:
                fork.values.clear()
                fork.errors.clear()
            kill_all(client, children, args.concurrency)
            time.sleep(args.settle)
    finally:
        if source_id:
            _swallow(delete_sandbox, client, source_id)

    return {
        "operations": {"fork": fork.stats()},
        "fork_wall": wall_summary(walls, args.total),
    }


def wall_summary(walls, batch_size: int) -> dict:
    if not walls:
        return {"rounds": 0}
    return {
        "rounds": len(walls),
        "batch_size": batch_size,
        "avg": statistics.fmean(walls),
        "min": min(walls),
        "p95": percentile(walls, 95),
        "max": max(walls),
        "per_op_avg": statistics.fmean(walls) / max(1, batch_size),
    }


SCENARIOS = {
    "create": scenario_create,
    "pause": scenario_pause_resume,
    "resume": scenario_pause_resume,
    "pause-resume": scenario_pause_resume,
    "snapshot": scenario_snapshot,
    "fork": scenario_fork,
}


# --- reporting ---------------------------------------------------------------


def print_operation_table(report: dict, header: bool) -> None:
    if header:
        print(
            f"{'operation':>12}  {'n':>5}  {'err':>4}  {'avg':>9}  {'min':>9}  "
            f"{'p50':>9}  {'p95':>9}  {'p99':>9}  {'max':>9}"
        )
        print("-" * 92)
    for name, stats in report.get("operations", {}).items():
        if not stats.get("count"):
            print(f"{name:>12}  {0:>5}  {stats.get('errors', 0):>4}  (no successful samples)")
        else:
            print(
                f"{name:>12}  {stats['count']:>5}  {stats['errors']:>4}  "
                f"{stats['avg']:>8.1f}  {stats['min']:>8.1f}  {stats['p50']:>8.1f}  "
                f"{stats['p95']:>8.1f}  {stats['p99']:>8.1f}  {stats['max']:>8.1f}   (ms)"
            )
        for message in stats.get("error_samples", []):
            print(f"{'':>12}  error: {message}")


def print_wall_tables(report: dict) -> None:
    walls = {key: value for key, value in report.items() if key.endswith("_wall")}
    if not walls:
        return
    print(
        f"\n{'batch':>14}  {'rounds':>6}  {'size':>4}  {'wall_avg':>9}  {'wall_min':>9}  "
        f"{'wall_p95':>9}  {'wall_max':>9}  {'per_op':>9}"
    )
    print("-" * 92)
    for key, wall in walls.items():
        if not wall.get("rounds"):
            continue
        print(
            f"{key[:-5]:>14}  {wall['rounds']:>6}  {wall['batch_size']:>4}  "
            f"{wall['avg']:>8.1f}  {wall['min']:>8.1f}  {wall['p95']:>8.1f}  "
            f"{wall['max']:>8.1f}  {wall['per_op_avg']:>8.1f}   (ms)"
        )


def print_server_stages(rows) -> None:
    if not rows:
        return
    print(f"\n{'server stage':>26}  {'count':>6}  {'avg':>9}")
    print("-" * 46)
    for row in rows:
        print(f"{row['stage']:>26}  {row['count']:>6}  {row['avg_ms']:>8.1f}   (ms)")


def resolve_template(client: ApiClient, explicit: str) -> str:
    if explicit:
        return explicit
    _, templates = client.call("GET", "/templates")
    ready = [t for t in templates or [] if t.get("buildStatus") == "ready"]
    if not ready:
        raise BenchError("no template with buildStatus=ready; build one first")
    ready.sort(key=lambda t: t.get("createdAt", ""), reverse=True)
    chosen = ready[0]
    name = (chosen.get("names") or [chosen["templateID"]])[0]
    print(f"  template: {name} ({chosen['templateID']})", file=sys.stderr)
    return chosen["templateID"]


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("-c", "--concurrency", type=int, default=1)
    parser.add_argument(
        "-n",
        "--total",
        type=int,
        default=None,
        help="create: total iterations; fork: children per fork (default: concurrency)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=3,
        help="measured rounds for batch scenarios (default: 3)",
    )
    parser.add_argument("-w", "--warmup", type=int, default=1, help="discarded rounds")
    parser.add_argument(
        "-m", "--mode", choices=["create-delete", "create-only"], default="create-delete"
    )
    parser.add_argument("-t", "--template", default=os.environ.get("AENV_TEMPLATE_ID", ""))
    parser.add_argument("--timeout", type=int, default=300, help="sandbox TTL in seconds")
    parser.add_argument("--settle", type=float, default=1.0, help="sleep between rounds")
    parser.add_argument("--keep", action="store_true", help="create-only: keep sandboxes")
    parser.add_argument("-o", "--output", help="write the JSON report to this file")
    parser.add_argument("--no-header", action="store_true")
    parser.add_argument(
        "--api-url",
        default=os.environ.get("AENV_API_URL", os.environ.get("E2B_API_URL", DEFAULT_API_URL)),
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("AENV_API_KEY", os.environ.get("E2B_API_KEY", DEFAULT_API_KEY)),
    )
    parser.add_argument(
        "--metrics-url",
        default=os.environ.get("AENV_METRICS_URL", ""),
        help="node Prometheus endpoint, e.g. http://127.0.0.1:8000/metrics",
    )
    args = parser.parse_args()

    if args.concurrency < 1:
        args.concurrency = 1
    if args.total is None:
        args.total = args.concurrency
    if args.total < 1:
        args.total = 1
    return args


def main() -> int:
    args = parse_args()
    client = ApiClient(args.api_url, args.api_key)

    try:
        args.template = resolve_template(client, args.template)
    except (BenchError, urllib.error.URLError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1

    before = scrape_metrics(args.metrics_url) if args.metrics_url else None
    started = time.time()
    try:
        report = SCENARIOS[args.scenario](client, args)
    except BenchError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    finally:
        client.close()

    stages = []
    if before is not None:
        stages = server_stage_deltas(before, scrape_metrics(args.metrics_url))

    report.update(
        {
            "scenario": args.scenario,
            "concurrency": args.concurrency,
            "total": args.total,
            "rounds": args.rounds,
            "warmup": args.warmup,
            "mode": args.mode,
            "template": args.template,
            "api_url": args.api_url,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
            "server_stages": stages,
        }
    )

    print(
        f"\n== {args.scenario}  c={args.concurrency}  n={args.total}  "
        f"rounds={args.rounds}  warmup={args.warmup} =="
    )
    print_operation_table(report, not args.no_header)
    print_wall_tables(report)
    print_server_stages(stages)
    if "throughput_per_s" in report:
        print(
            f"\nwall {report['wall_ms']:.0f} ms  |  per-op {report['per_op_ms']:.1f} ms  "
            f"|  throughput {report['throughput_per_s']:.1f} /s"
        )

    if args.output:
        with open(args.output, "w") as handle:
            json.dump(report, handle, indent=2)
        print(f"report written to {args.output}")

    failed = any(
        stats.get("errors") for stats in report.get("operations", {}).values()
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
