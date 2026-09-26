#!/usr/bin/env python3
"""Run or inspect the isolated POSIX startup-pack hardware smoke.

Prepare a private config and dependencies with server --setup-only first.
Example (from the repository root, on a KVM/ublk host):
  export AENV_CONFIG_PATH=/tmp/my-smoke/config.toml
  export AENV_HOME_PATH=/tmp/my-smoke/home
  export AENV_RUNTIME_PATH=/tmp/my-smoke/run
  export AENV_DEPS_PATH=/tmp/my-smoke/deps
  python3 scripts/verify-posix-startup-pack-smoke.py --log /tmp/my-smoke/test.log

Use --check-log LOG to validate a previously saved raw test log.
The check requires the one selected test to pass and exactly one positive
completion from the shared memory-device prefetch reader.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

TEST = "posix_startup_pack::posix_startup_pack_publish_resolve_and_resume"
EVENT = re.compile(
    r"agentenv::sandbox::firecracker::startup_pack.*?"
    r"memory device startup prefetch drained\s+"
    r"requested_bytes=(\d+)\s+read_bytes=(\d+)"
)
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def verify(log_path: Path) -> None:
    log = ANSI.sub("", log_path.read_text(errors="replace"))
    if not re.search(rf"test {re.escape(TEST)} \.\.\. ok", log):
        raise ValueError(f"{TEST} did not pass")
    if not re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", log):
        raise ValueError("expected one passed hardware test, with none failed or ignored")
    reads = EVENT.findall(log)
    if len(reads) != 1:
        raise ValueError(f"expected exactly one memory-device prefetch completion, got {len(reads)}")
    requested, read = map(int, reads[0])
    if not 0 < read <= requested:
        raise ValueError(f"invalid device read: requested={requested}, read={read}")
    print(f"PASS: target memory-device prefetch requested={requested}, read={read}")


def run(log_path: Path) -> None:
    repo = Path(__file__).resolve().parent.parent
    required = ("AENV_CONFIG_PATH", "AENV_HOME_PATH", "AENV_RUNTIME_PATH", "AENV_DEPS_PATH")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError(f"set private test paths before running: {', '.join(missing)}")
    if not Path(os.environ["AENV_CONFIG_PATH"]).is_file():
        raise ValueError("AENV_CONFIG_PATH must point to a prepared private config file")
    home = Path(os.environ["AENV_HOME_PATH"]).resolve()
    runtime = Path(os.environ["AENV_RUNTIME_PATH"]).resolve()
    if home == runtime:
        raise ValueError("AENV_HOME_PATH and AENV_RUNTIME_PATH must differ")
    cargo = shutil.which("cargo")
    if cargo is None:
        raise ValueError("cargo is unavailable; load the host Rust environment")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["RUST_LOG"] = "agentenv::sandbox::firecracker::startup_pack=debug"
    cmd = [
        str(repo / "scripts/run-with-capabilities.sh"),
        cargo,
        "test",
        "-p",
        "agentenv",
        "--test",
        "integration",
        TEST,
        "--",
        "--exact",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]
    with log_path.open("w") as output:
        result = subprocess.run(cmd, cwd=repo, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
    if result.returncode:
        raise ValueError(f"hardware test failed with exit {result.returncode}; see {log_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--log", type=Path, help="run the isolated hardware test and save its raw log")
    choice.add_argument("--check-log", type=Path, help="validate a saved raw hardware-test log")
    args = parser.parse_args()
    path = args.log or args.check_log
    try:
        if args.log:
            run(path)
        verify(path)
    except (OSError, ValueError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
