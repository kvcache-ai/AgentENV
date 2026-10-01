#!/usr/bin/env python3
"""Reuse Cargo source timestamps only when their contents match the cached build."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def restore(root: Path, manifest: Path) -> None:
    previous = json.loads(manifest.read_text()) if manifest.exists() else {}
    current = {}
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
    for name in tracked.decode().split("\0"):
        path = root / name
        if not name or path.is_symlink() or not path.is_file():
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        cached = previous.get(name)
        if cached and cached["sha256"] == digest:
            os.utime(path, ns=(path.stat().st_atime_ns, cached["mtime_ns"]))
        current[name] = {"sha256": digest, "mtime_ns": path.stat().st_mtime_ns}
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps(current))


if __name__ == "__main__":
    restore(Path.cwd(), Path(sys.argv[1]))
