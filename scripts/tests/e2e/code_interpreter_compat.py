#!/usr/bin/env python3
"""Code interpreter compatibility checks for AgentENV.

Builds a template from the official e2b code-interpreter Docker image, then
runs correctness and performance checks against the resulting sandbox.
"""

from __future__ import annotations

import concurrent.futures
import importlib.metadata
import json
import os
import threading
import time
from typing import Callable, TypeVar

from httpx import Client
from e2b import Template, wait_for_url, Sandbox as _BaseSandbox
from e2b_code_interpreter import Sandbox as _BaseCodeSandbox
from e2b_code_interpreter.constants import JUPYTER_PORT
from e2b_code_interpreter.models import OutputMessage

T = TypeVar("T")

CI_IMAGE = "cube-sandbox-int.tencentcloudcr.com/cube-sandbox/sandbox-code:latest"
TEMPLATE_NAME_PREFIX = "e2b-code-interpreter-compat"

def _percentile(data: list[float], p: float) -> float:
    """Linear-interpolation percentile over an unsorted list."""
    if not data:
        return float("nan")
    s = sorted(data)
    idx = (len(s) - 1) * p / 100.0
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


def log(message: str) -> None:
    print(f"[code-interpreter-compat] {message}", flush=True)


def require(condition: object, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def retry(
    action: Callable[[], T],
    description: str,
    attempts: int = 10,
    delay: float = 1.0,
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:
            last_error = error
            log(f"{description} failed on attempt {attempt}/{attempts}: {error}")
            if attempt < attempts:
                time.sleep(delay)
    raise AssertionError(f"{description} failed after {attempts} attempts") from last_error


class CodeSandbox(_BaseCodeSandbox):
    """Routes e2b_code_interpreter through the AgentENV proxy."""

    @property
    def _jupyter_url(self) -> str:
        raw = self.connection_config._sandbox_url
        if raw is None:
            return super()._jupyter_url
        if "://" in raw:
            host_port = raw.split("://", 1)[1]
        elif raw.lower().startswith(("http:", "https:")):
            host_port = raw.split(":", 1)[1].lstrip("/")
        else:
            host_port = raw
        return "http://" + host_port.rstrip("/")

    @property
    def _client(self) -> Client:
        return Client(
            transport=self._transport,
            headers={
                "e2b-sandbox-id": self.sandbox_id,
                "e2b-sandbox-port": str(JUPYTER_PORT),
            },
        )

def correctness_tests(
    template_name: str,
    api_url: str,
    sandbox_url: str,
    api_key: str) -> None:
    
    correct_sandbox: CodeSandbox | None = None
    try:
        log("creating sandbox from template")
        correct_sandbox = CodeSandbox.create(
            template=template_name,
            timeout=300,
            api_url=api_url,
            sandbox_url=sandbox_url,
            api_key=api_key,
            request_timeout=60,
            secure=True,
        )
        require(correct_sandbox.sandbox_id, "sandbox create returned empty sandbox_id")
        log(f"sandbox created: {correct_sandbox.sandbox_id}")

        log("testing arithmetic")
        r = correct_sandbox.run_code("2 ** 10")
        require(r.error is None, f"arithmetic: unexpected error: {r.error}")
        require(r.text == "1024", f"arithmetic: expected 1024, got {r.text!r}")
        log("arithmetic: OK")

        log("testing pre-installed packages (pandas, numpy)")
        r = correct_sandbox.run_code(
            "import pandas as pd, numpy as np\n"
            "pd.DataFrame({'a': range(3)}).shape"
        )
        require(r.error is None, f"packages: unexpected error: {r.error}")
        require(r.text == "(3, 1)", f"packages: unexpected df.shape: {r.text!r}")
        log("pre-installed packages: OK")

        log("testing file I/O")
        r = correct_sandbox.run_code(
            "import pathlib\n"
            "p = pathlib.Path('/tmp/test_agentenv.txt')\n"
            "p.write_text('hello-agentenv')\n"
            "p.read_text() == 'hello-agentenv'"
        )
        require(r.error is None, f"file I/O: unexpected error: {r.error}")
        require(r.text == "True", f"file I/O: content mismatch, got {r.text!r}")
        log("file I/O: OK")

        log("testing per-execution env injection (envs=)")
        r = correct_sandbox.run_code(
            "import os; os.environ.get('_AENV_TEST_VAR') == 'injected'",
            envs={"_AENV_TEST_VAR": "injected"},
        )
        require(r.error is None, f"envs injection: unexpected error: {r.error}")
        require(r.text == "True", f"envs injection: expected True, got {r.text!r}")
        log("env injection: OK")

        log("testing multi-command state persistence")
        retry(lambda: correct_sandbox.run_code("_state = 0"), "state warm-up", attempts=5, delay=2.0)
        r = correct_sandbox.run_code("_state += 7; _state")
        require(r.error is None, f"state step-1: unexpected error: {r.error}")
        require(r.text == "7", f"state step-1: expected 7, got {r.text!r}")
        r = correct_sandbox.run_code("_state *= 3; _state")
        require(r.error is None, f"state step-2: unexpected error: {r.error}")
        require(r.text == "21", f"state step-2: expected 21, got {r.text!r}")
        log("state persistence: OK")

        log("testing missing commands")
        r = correct_sandbox.run_code("_undefined_var")
        require(r.error is not None, "NameError: expected error result, got None")
        require(
            "NameError" in r.error.name,
            f"NameError: got {r.error.name!r}",
        )
        log("missing commands (NameError): OK")

        log("testing process failure")
        r = correct_sandbox.run_code("import sys; sys.exit(1)")
        require(r.error is not None, "process failure: expected error result, got None")
        require(
            "SystemExit" in r.error.name,
            f"process failure: expected SystemExit, got {r.error.name!r}",
        )
        log("process failure: OK")

        log("testing oversized output")
        r = correct_sandbox.run_code("for i in range(10_000): print(i)")
        require(r.error is None, f"large output: unexpected error: {r.error}")
        all_lines = "".join(r.logs.stdout).splitlines()
        require(
            len(all_lines) == 10_000,
            f"large output: expected 10000 lines, got {len(all_lines)}",
        )
        require(
            all_lines[0] == "0" and all_lines[-1] == "9999",
            f"large output: boundary check failed (first={all_lines[0]!r}, last={all_lines[-1]!r})",
        )
        log("oversized output: OK")

        log("testing streaming output callback (on_stdout)")
        streamed: list[OutputMessage] = []
        correct_sandbox.run_code(
            "for i in range(5): print(f'stream {i}')",
            on_stdout=streamed.append,
        )
        require(len(streamed) >= 1, "streaming: expected at least 1 OutputMessage chunk, got 0")
        all_streamed = "".join(msg.line for msg in streamed)
        expected_streamed = "".join(f"stream {i}\n" for i in range(5))
        require(
            all_streamed == expected_streamed,
            f"streaming: unexpected content: {all_streamed!r}",
        )
        log("streaming output: OK")

        # E2B code-interpreter template does not support concurrency;
        # expected to execute commands successfully in serial
        log("testing concurrent run_code submissions")
        correct_sandbox.run_code("import pathlib; pathlib.Path('/tmp/rw.txt').write_text('')")
        def append_marker(marker: str) -> None:
            correct_sandbox.run_code(
                "import pathlib; p = pathlib.Path('/tmp/rw.txt')\n"
                f"p.write_text(p.read_text() + '{marker}')"
            )
        t_a = threading.Thread(target=append_marker, args=("A" * 100,))
        t_b = threading.Thread(target=append_marker, args=("B" * 100,))
        t_a.start(); t_b.start()
        t_a.join(timeout=30); t_b.join(timeout=30)
        require(not t_a.is_alive(), "concurrent writes: thread A timed out (still running after 30s)")
        require(not t_b.is_alive(), "concurrent writes: thread B timed out (still running after 30s)")
        r = correct_sandbox.run_code("import pathlib; len(pathlib.Path('/tmp/rw.txt').read_text())")
        require(r.text == "200", f"concurrent writes: expected 200 chars, got {r.text!r}")
        log("concurrent submissions: OK")
    finally:
        if correct_sandbox is not None:
            try:
                correct_sandbox.kill(api_url=api_url, api_key=api_key, request_timeout=60)
            except Exception as error:
                log(f"cleanup correct_sandbox kill failed: {error}")


def performance_tests(
    template_name: str,
    api_url: str,
    sandbox_url: str,
    api_key: str,
    *,
    warm_rounds: int = 100,
    concurrent_count: int = 5,
) -> None:
    """Measure cold-start, warm RTT, and concurrent throughput.

    Emits one ``PERF_METRICS: <json>`` log line for machine consumption.
    Hard-fails on: cold-to-first >= 30 s, warm RTT p99 >= 3000 ms, concurrent workload p99 >= 5000 ms.
    """
    metrics: dict = {}

    log("perf: measuring cold creation + first-command latency")
    perf_sandbox: CodeSandbox | None = None
    try:
        t0 = time.perf_counter()
        perf_sandbox = CodeSandbox.create(
            template=template_name,
            timeout=300,
            api_url=api_url,
            sandbox_url=sandbox_url,
            api_key=api_key,
            request_timeout=60,
            secure=True,
        )
        retry(lambda: perf_sandbox.run_code("1 + 1"), "perf first cmd", attempts=10, delay=2.0)
        cold_to_first_cmd_s = time.perf_counter() - t0
        metrics["cold_to_first_s"] = round(cold_to_first_cmd_s, 3)
        log(
            f"cold-to-first: {metrics['cold_to_first_s']:.2f}s"
        )

        log(f"perf: measuring warm RTT ({warm_rounds} rounds)")
        rtt: list[float] = []
        for _ in range(warm_rounds):
            t0 = time.perf_counter()
            perf_sandbox.run_code("1 + 1")
            rtt.append(time.perf_counter() - t0)

        metrics["warm_rtt_ms"] = {
            "n": warm_rounds,
            "min": round(min(rtt) * 1000, 1),
            "p50": round(_percentile(rtt, 50) * 1000, 1),
            "p95": round(_percentile(rtt, 95) * 1000, 1),
            "p99": round(_percentile(rtt, 99) * 1000, 1),
            "max": round(max(rtt) * 1000, 1),
        }
        w = metrics["warm_rtt_ms"]
        log(f"perf: warm RTT  p50={w['p50']}ms  p95={w['p95']}ms  p99={w['p99']}ms")

    finally:
        if perf_sandbox is not None:
            try:
                perf_sandbox.kill(api_url=api_url, api_key=api_key, request_timeout=60)
            except Exception as err:
                log(f"perf: cleanup perf_sandbox failed: {err}")

    log(f"perf: measuring concurrent throughput ({concurrent_count} sandboxes)")
    concurrent_sandboxes: list[CodeSandbox | None] = [None] * concurrent_count
    creation_times: list[float] = []

    def _create(idx: int) -> tuple[int, float, CodeSandbox | None]:
        t0 = time.perf_counter()
        try:
            sb = CodeSandbox.create(
                template=template_name,
                timeout=300,
                api_url=api_url,
                sandbox_url=sandbox_url,
                api_key=api_key,
                request_timeout=60,
                secure=True,
            )
            return idx, time.perf_counter() - t0, sb
        except Exception as err:
            log(f"perf: concurrent sandbox {idx} creation failed: {err}")
            return idx, time.perf_counter() - t0, None

    def _workload(sb: CodeSandbox) -> list[float]:
        try:
            retry(lambda: sb.run_code("1 + 1"), "concurrent warmup", attempts=10, delay=2.0)
            times = []
            for _ in range(5):
                t0 = time.perf_counter()
                sb.run_code("sum(range(1000))")
                times.append(time.perf_counter() - t0)
            return times
        except Exception as err:
            log(f"perf: workload on {sb.sandbox_id} failed: {err}")
            return []

    try:
        t_wall = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_count) as pool:
            for idx, elapsed, sb in pool.map(_create, range(concurrent_count)):
                concurrent_sandboxes[idx] = sb
                if sb is not None:
                    creation_times.append(elapsed)

        active = [sb for sb in concurrent_sandboxes if sb is not None]
        log(f"perf: {len(active)}/{concurrent_count} concurrent sandboxes created")
        require(len(active) > 0, f"perf: all {concurrent_count} concurrent creations failed")

        workload_times: list[float] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(active)) as pool:
            for times in pool.map(_workload, active):
                workload_times.extend(times)

        concurrent_wall_s = time.perf_counter() - t_wall
        metrics["concurrent"] = {
            "requested": concurrent_count,
            "active": len(active),
            "creation_p50_s": round(_percentile(creation_times, 50), 3),
            "creation_p99_s": round(_percentile(creation_times, 99), 3),
            "workload_p50_ms": round(_percentile(workload_times, 50) * 1000, 1),
            "workload_p95_ms": round(_percentile(workload_times, 95) * 1000, 1),
            "workload_p99_ms": round(_percentile(workload_times, 99) * 1000, 1),
            "wall_s": round(concurrent_wall_s, 3),
        }
        c = metrics["concurrent"]
        log(
            f"perf: concurrent  "
            f"workload p50={c['workload_p50_ms']}ms  "
            f"p99={c['workload_p99_ms']}ms  "
            f"wall={c['wall_s']}s"
        )

    finally:
        for sb in concurrent_sandboxes:
            if sb is not None:
                try:
                    sb.kill(api_url=api_url, api_key=api_key, request_timeout=60)
                except Exception as err:
                    log(f"perf: cleanup concurrent sandbox failed: {err}")

    log(f"PERF_METRICS: {json.dumps(metrics)}")
    require(
        metrics.get("cold_to_first_s", 9999) < 30,
        f"perf regression: cold to first command {metrics.get('cold_to_first_s')}s >= 30s",
    )
    warm_p99 = metrics.get("warm_rtt_ms", {}).get("p99", 0)
    require(
        warm_p99 < 3000,
        f"perf regression: warm RTT p99 {warm_p99}ms >= 3000ms",
    )
    concurrent_workload_p99 = metrics.get("concurrent", {}).get("workload_p99_ms", 0)
    require(
        concurrent_workload_p99 < 5000,
        f"perf regression: concurrent workload RTT p99 {concurrent_workload_p99}ms >= 5000ms",
    )

def main() -> int:
    sdk_version = importlib.metadata.version("e2b_code_interpreter")
    log(f"using e2b_code_interpreter {sdk_version}")

    api_url = os.environ["E2B_API_URL"]
    sandbox_url = os.environ["E2B_SANDBOX_URL"]
    api_key = os.environ["E2B_API_KEY"]
    # Use a stable name so the finally-block cleanup always targets the same template
    # and re-runs don't accumulate stale templates.  Override with
    # E2B_COMPAT_TEMPLATE_NAME when running multiple suites concurrently.
    template_name = os.environ.get("E2B_COMPAT_TEMPLATE_NAME") or f"{TEMPLATE_NAME_PREFIX}-{time.time_ns()}"
    build_info = None

    try:
        log(f"building template '{template_name}' from {CI_IMAGE}")
        template = (
            Template()
            .from_image(CI_IMAGE)
            .set_ready_cmd(
                wait_for_url("http://localhost:49999/health", 200)
            )
        )
        build_info = Template.build(
            template,
            template_name,
            cpu_count=2,
            memory_mb=1024,
            skip_cache=True,
            api_url=api_url,
            api_key=api_key,
            sandbox_url=sandbox_url,
            request_timeout=60,
        )
        require(build_info.template_id, "template build returned an empty template_id")
        require(build_info.build_id, "template build returned an empty build_id")
        require(build_info.name == template_name, "template build returned the wrong name")
        log(f"template ready: template_id={build_info.template_id} build_id={build_info.build_id}")

        correctness_tests(template_name, api_url, sandbox_url, api_key)
        performance_tests(template_name, api_url, sandbox_url, api_key)

        return 0

    finally:
        delete_ref = build_info.template_id if build_info is not None else template_name
        try:
            _BaseSandbox.delete_snapshot(
                delete_ref,
                api_url=api_url,
                api_key=api_key,
                request_timeout=60,
            )
            log(f"template {delete_ref!r} deleted")
        except Exception as error:
            log(f"cleanup template delete failed: {error}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        log(f"failed: {error}")
        raise
