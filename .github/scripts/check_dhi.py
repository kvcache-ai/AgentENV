#!/usr/bin/env python3
"""Require literal, digest-pinned DHI roots in the deployment Dockerfiles."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILES = tuple(
    ROOT / "deploy/docker" / f"Dockerfile.{name}"
    for name in ("agentenv", "gateway", "scheduler")
)
DHI = re.compile(r"dhi\.io/[a-z0-9_./-]+:[a-zA-Z0-9_.-]+@sha256:[0-9a-f]{64}")


def check(text: str) -> None:
    """Reject unpinned roots, variable roots, and external stage imports."""
    stages = set()
    count = 0
    # This policy intentionally accepts the simple syntax used by our Dockerfiles.
    # Unsupported syntax fails closed instead of guessing how BuildKit resolves it.
    if (
        re.search(r"^\s*#\s*escape\s*=", text, re.MULTILINE | re.IGNORECASE)
        or "<<" in text
    ):
        raise ValueError(
            "custom escapes and heredocs are unsupported by the DHI policy"
        )
    text = re.sub(r"\\\n\s*", " ", text)
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        words = line.split()
        if words[0].upper() == "ONBUILD":
            raise ValueError("ONBUILD is unsupported by the DHI policy")
        if words[0].upper() == "FROM":
            if len(words) not in (2, 4) or (
                len(words) == 4 and words[2].upper() != "AS"
            ):
                raise ValueError(f"unsupported FROM: {line}")
            base = words[1]
            if base.lower() not in stages and not DHI.fullmatch(base):
                raise ValueError(
                    f"base must be a pinned dhi.io image or earlier stage: {base}"
                )
            if len(words) == 4:
                alias = words[3].lower()
                if not re.fullmatch(r"[a-z][a-z0-9_-]*", alias) or alias in stages:
                    raise ValueError(f"invalid or repeated stage: {alias}")
                stages.add(alias)
            count += 1
        if words[0].upper() in ("COPY", "ADD", "RUN"):
            for source in re.findall(r"\bfrom=([^,\s]+)", line, flags=re.IGNORECASE):
                if source.lower() not in stages:
                    raise ValueError(
                        f"external stage imports are not allowed: {source}"
                    )
    if not count:
        raise ValueError("Dockerfile has no FROM instruction")


if __name__ == "__main__":
    for path in DOCKERFILES:
        check(path.read_text())
        print(f"DHI roots verified: {path.relative_to(ROOT)}")
