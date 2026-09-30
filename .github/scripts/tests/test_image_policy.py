"""Check that deployment builds reject non-DHI image inputs."""

import importlib.util
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("check_dhi", SCRIPTS / "check_dhi.py")
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)
BASE = "dhi.io/debian-base:dev@sha256:" + "a" * 64


class DhiPolicy(unittest.TestCase):
    def test_deployment_files(self):
        for path in policy.DOCKERFILES:
            with self.subTest(path=path):
                policy.check(path.read_text())

    def test_stage_reuse(self):
        policy.check(f"FROM {BASE} AS build\nFROM build\nCOPY --from=build /a /b")

    def test_disallowed_sources(self):
        for base in (
            "ubuntu:24.04",
            "scratch",
            "dhi.io/static:latest",
            "${BASE}",
            "dhi.io.evil/static@sha256:" + "a" * 64,
        ):
            with self.subTest(base=base), self.assertRaises(ValueError):
                policy.check(f"FROM {base}")

    def test_external_stage_imports(self):
        for instruction in (
            "COPY --from=ubuntu:24.04 /a /b",
            "RUN --mount=type=bind,from=ubuntu:24.04 echo hi",
            "COPY \\\n --from=ubuntu:24.04 /a /b",
        ):
            with self.subTest(instruction=instruction), self.assertRaises(ValueError):
                policy.check(f"FROM {BASE}\n{instruction}")

    def test_unsupported_syntax(self):
        for text in (
            "",
            "# escape=`\n",
            f"FROM {BASE}\nRUN <<EOF\nEOF",
            f"FROM {BASE}\nONBUILD FROM ubuntu:24.04",
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                policy.check(text)


if __name__ == "__main__":
    unittest.main()
