#!/usr/bin/env python3
"""E2B Python SDK compatibility checks for AgentENV."""

from __future__ import annotations

import importlib.metadata
import os
import time
from typing import Callable, TypeVar

from e2b import Sandbox, Template


T = TypeVar("T")


def log(message: str) -> None:
    print(f"[e2b-python-sdk] {message}", flush=True)


def require(condition, message: str) -> None:
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
        except Exception as error:  # noqa: BLE001 - surface the last SDK/envd failure.
            last_error = error
            log(f"{description} failed on attempt {attempt}/{attempts}: {error}")
            if attempt < attempts:
                time.sleep(delay)
    raise AssertionError(f"{description} failed after {attempts} attempts") from last_error


def main() -> int:
    version = importlib.metadata.version("e2b")
    log(f"using e2b Python SDK {version}")

    api_url = os.environ["E2B_API_URL"]
    sandbox_url = os.environ["E2B_SANDBOX_URL"]
    api_key = os.environ["E2B_API_KEY"]

    template_name = os.environ.get("E2B_COMPAT_TEMPLATE_NAME") or f"e2b-python-sdk-{time.time_ns()}"
    public_template = (os.environ.get("AENV_TEMPLATE_ID") or "").strip()
    derived_template_name = f"{template_name}-from-template"
    base_image = os.environ.get("E2B_COMPAT_USER_IMAGE", "ghcr.io/linuxserver/baseimage-ubuntu:noble")
    workdir = f"/tmp/{template_name}"
    derived_workdir = f"/tmp/{derived_template_name}"
    build_marker = f"sdk-build-marker-{time.time_ns()}"
    derived_marker = f"sdk-from-template-marker-{time.time_ns()}"
    startup_marker = f"sdk-startup-marker-{time.time_ns()}"

    build_info = None
    derived_build_info = None
    sandbox = None
    derived_sandbox = None

    try:
        log(f"building template {template_name} from {base_image}")
        template = (
            Template()
            .from_image(base_image)
            .run_cmd(f"mkdir -p {workdir}")
            .set_workdir(workdir)
            .set_envs({"AENV_E2B_SDK_MARKER": build_marker})
            .run_cmd("printf '%s' \"$AENV_E2B_SDK_MARKER\" > marker.txt")
            .run_cmd("pwd > workdir.txt")
            .set_envs({"AENV_E2B_STARTUP_MARKER": startup_marker})
            .set_start_cmd(
                "printf '%s' \"$AENV_E2B_STARTUP_MARKER\" > startup-ready.txt; "
                "exec -a \"agentenv-startup-$AENV_E2B_STARTUP_MARKER\" sleep 1000000",
                "test -f startup-ready.txt && "
                "grep -qx \"$AENV_E2B_STARTUP_MARKER\" startup-ready.txt",
            )
        )

        build_info = Template.build(
            template,
            template_name,
            cpu_count=1,
            memory_mb=128,
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

        log("creating sandbox from SDK-built template")
        sandbox = Sandbox.create(
            template=template_name,
            timeout=90,
            metadata={"suite": "e2b-python-sdk", "template": template_name},
            api_url=api_url,
            api_key=api_key,
            sandbox_url=sandbox_url,
            request_timeout=60,
            secure=True,
        )
        require(sandbox.sandbox_id, "sandbox create returned an empty sandbox_id")
        log(f"sandbox created: {sandbox.sandbox_id}")

        listed = Sandbox.list(
            limit=20,
            api_url=api_url,
            api_key=api_key,
            sandbox_url=sandbox_url,
            request_timeout=60,
        ).next_items()
        listed_ids = {item.sandbox_id for item in listed}
        require(sandbox.sandbox_id in listed_ids, "Sandbox.list did not include created sandbox")
        log("sandbox list includes SDK-created sandbox")

        def read_build_artifacts():
            return sandbox.commands.run(
                f"pid_line=$(pgrep -af '[a]gentenv-startup-{startup_marker}' | head -1); "
                "test -n \"$pid_line\"; "
                "printf 'marker=' && cat marker.txt && "
                "printf '\\nworkdir=' && cat workdir.txt && "
                "printf '\\nstartup=' && cat startup-ready.txt && "
                "printf '\\nprocess=%s' \"$pid_line\"",
                cwd=workdir,
                timeout=30,
                request_timeout=60,
            )

        result = retry(read_build_artifacts, "command execution")
        require(result.exit_code == 0, f"command exited with {result.exit_code}")
        require(f"marker={build_marker}" in result.stdout, "build marker file did not match")
        require(f"workdir={workdir}" in result.stdout, "WORKDIR build step was not preserved")
        require(
            f"startup={startup_marker}" in result.stdout,
            "startup ready marker file did not match",
        )
        require(
            f"agentenv-startup-{startup_marker}" in result.stdout,
            "startCmd process was not preserved in the template snapshot",
        )
        log("command execution returned expected build artifacts and startup state")

        # OCI images routinely ship /etc/hosts absent or empty and rely on the
        # container runtime for localhost resolution; the guest bootstrap
        # (pivot-init) must provide the same guarantee. Assert the hosts file
        # carries a loopback -> localhost mapping.
        resolution = retry(
            lambda: sandbox.commands.run(
                r"/agentenv/bin/busybox grep -E "
                r"'^(127\.0\.0\.1|::1)[[:space:]]+localhost([[:space:]]|$)' /etc/hosts",
                timeout=30,
                request_timeout=60,
            ),
            "localhost resolution check",
        )
        require(
            resolution.exit_code == 0,
            f"/etc/hosts lacks a loopback localhost entry: {resolution.stdout!r}",
        )
        log("guest localhost resolution verified")

        if os.environ.get("E2B_COMPAT_TEST_PAUSE", "1") != "0":
            log("pausing and reconnecting sandbox through SDK lifecycle APIs")
            sandbox.beta_pause(api_url=api_url, api_key=api_key, request_timeout=60)
            sandbox = sandbox.connect(
                timeout=90,
                api_url=api_url,
                api_key=api_key,
                sandbox_url=sandbox_url,
                request_timeout=60,
            )
            resumed = retry(
                lambda: sandbox.commands.run("printf resumed", timeout=30, request_timeout=60),
                "command execution after reconnect",
            )
            require(resumed.stdout == "resumed", "sandbox did not run commands after reconnect")
            log("sandbox reconnect succeeded")

        killed = sandbox.kill(api_url=api_url, api_key=api_key, request_timeout=60)
        require(killed, "sandbox.kill returned false")
        sandbox = None
        log("sandbox killed")

        if public_template:
            log(f"building template {derived_template_name} from template {public_template}")
            derived_template = (
                Template()
                .from_template(public_template)
                .run_cmd(f"mkdir -p {derived_workdir}")
                .set_workdir(derived_workdir)
                .set_envs({"AENV_E2B_SDK_FROM_TEMPLATE_MARKER": derived_marker})
                .run_cmd("printf '%s' \"$AENV_E2B_SDK_FROM_TEMPLATE_MARKER\" > marker.txt")
                .run_cmd("pwd > workdir.txt")
            )

            derived_build_info = Template.build(
                derived_template,
                derived_template_name,
                cpu_count=1,
                memory_mb=128,
                skip_cache=True,
                api_url=api_url,
                api_key=api_key,
                sandbox_url=sandbox_url,
                request_timeout=60,
            )

            require(
                derived_build_info.template_id,
                "from_template build returned an empty template_id",
            )
            require(
                derived_build_info.build_id,
                "from_template build returned an empty build_id",
            )
            require(
                derived_build_info.name == derived_template_name,
                "from_template build returned the wrong name",
            )
            log(
                "from_template template ready: "
                f"template_id={derived_build_info.template_id} "
                f"build_id={derived_build_info.build_id}"
            )

            log("creating sandbox from SDK from_template build")
            derived_sandbox = Sandbox.create(
                template=derived_template_name,
                timeout=90,
                metadata={
                    "suite": "e2b-python-sdk",
                    "template": derived_template_name,
                    "base_template": public_template,
                },
                api_url=api_url,
                api_key=api_key,
                sandbox_url=sandbox_url,
                request_timeout=60,
                secure=True,
            )
            require(
                derived_sandbox.sandbox_id,
                "from_template sandbox create returned an empty sandbox_id",
            )
            log(f"from_template sandbox created: {derived_sandbox.sandbox_id}")

            def read_derived_build_artifacts():
                return derived_sandbox.commands.run(
                    "printf 'marker=' && cat marker.txt && printf '\\nworkdir=' && cat workdir.txt",
                    cwd=derived_workdir,
                    timeout=30,
                    request_timeout=60,
                )

            derived_result = retry(
                read_derived_build_artifacts,
                "from_template command execution",
            )
            require(
                derived_result.exit_code == 0,
                f"from_template command exited with {derived_result.exit_code}",
            )
            require(
                f"marker={derived_marker}" in derived_result.stdout,
                "from_template marker file did not match",
            )
            require(
                f"workdir={derived_workdir}" in derived_result.stdout,
                "from_template WORKDIR build step was not preserved",
            )
            log("from_template command execution returned expected build artifacts")

            killed = derived_sandbox.kill(api_url=api_url, api_key=api_key, request_timeout=60)
            require(killed, "from_template sandbox.kill returned false")
            derived_sandbox = None
            log("from_template sandbox killed")
        else:
            log("AENV_TEMPLATE_ID is not set; skipping from_template compatibility check")

        return 0
    finally:
        if derived_sandbox is not None:
            try:
                derived_sandbox.kill(api_url=api_url, api_key=api_key, request_timeout=60)
            except Exception as error:  # noqa: BLE001 - best-effort cleanup.
                log(f"cleanup from_template sandbox kill failed: {error}")

        if sandbox is not None:
            try:
                sandbox.kill(api_url=api_url, api_key=api_key, request_timeout=60)
            except Exception as error:  # noqa: BLE001 - best-effort cleanup.
                log(f"cleanup sandbox kill failed: {error}")

        if derived_build_info is not None:
            try:
                Sandbox.delete_snapshot(
                    derived_build_info.template_id,
                    api_url=api_url,
                    api_key=api_key,
                    request_timeout=60,
                )
            except Exception as error:  # noqa: BLE001 - best-effort cleanup.
                log(f"cleanup from_template template delete failed: {error}")

        if build_info is not None:
            try:
                Sandbox.delete_snapshot(
                    build_info.template_id,
                    api_url=api_url,
                    api_key=api_key,
                    request_timeout=60,
                )
            except Exception as error:  # noqa: BLE001 - best-effort cleanup.
                log(f"cleanup template delete failed: {error}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001 - make shell output actionable.
        log(f"failed: {error}")
        raise
