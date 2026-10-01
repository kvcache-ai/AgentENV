"""Exercise Cargo invalidation with the real timestamp helper and stamp script."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[3]


class BuildReuseTests(unittest.TestCase):
    def test_checkout_reuse_and_commit_stamp_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "library/src").mkdir(parents=True)
            (root / "server/src").mkdir(parents=True)
            (root / "Cargo.toml").write_text(
                '[workspace]\nmembers = ["library", "server"]\nresolver = "2"\n'
            )
            (root / "library/Cargo.toml").write_text(
                '[package]\nname = "fixture-library"\nversion = "0.1.0"\nedition = "2021"\n'
            )
            source = root / "library/src/lib.rs"
            source.write_text('pub fn value() -> u8 { 1 }\n')
            (root / "server/Cargo.toml").write_text(
                '[package]\nname = "fixture-server"\nversion = "0.1.0"\nedition = "2021"\n'
                '[dependencies]\nfixture-library = { path = "../library" }\n'
            )
            (root / "server/src/main.rs").write_text(
                'fn main() { println!("{} {}", fixture_library::value(), '
                'env!("AENV_GIT_COMMIT")); }\n'
            )
            shutil.copy(REPO / "crates/server/build.rs", root / "server/build.rs")
            shutil.copy(REPO / "rust-toolchain.toml", root / "rust-toolchain.toml")
            env = os.environ.copy()
            for name in ("AENV_BUILD_COMMIT", "CARGO_TARGET_DIR", "RUSTC_WRAPPER"):
                env.pop(name, None)
            env["CARGO_INCREMENTAL"] = "0"

            def run(*args):
                return subprocess.run(
                    args, cwd=root, env=env, check=True, text=True, capture_output=True
                )

            run("git", "init", "-q")
            run("git", "config", "user.email", "fixture@example.invalid")
            run("git", "config", "user.name", "Fixture")
            run("git", "add", ".")
            run("git", "commit", "-qm", "Initial source")
            manifest = root / "source-state.json"

            def restore():
                run("python3", str(REPO / ".github/scripts/restore_rust_source_mtimes.py"), str(manifest))

            restore()
            run("cargo", "build", "--offline", "-v")
            # Simulate fresh checkout timestamps while retaining a restored target cache.
            for path in (root / "library").rglob("*"):
                if path.is_file():
                    os.utime(path, None)
            restore()
            warm = run("cargo", "build", "--offline", "-v").stderr
            self.assertIn("Fresh fixture-library", warm)
            self.assertIn("Fresh fixture-server", warm)

            run("git", "commit", "--allow-empty", "-qm", "Metadata only")
            stamped = run("cargo", "build", "--offline", "-v").stderr
            self.assertIn("Fresh fixture-library", stamped)
            self.assertIn("Compiling fixture-server", stamped)
            commit = run("git", "rev-parse", "--short", "HEAD").stdout.strip()
            env["AENV_GIT_COMMIT"] = "runtime-override"
            self.assertEqual(run("target/debug/fixture-server").stdout.strip(), f"1 {commit}")

            source.write_text('pub fn value() -> u8 { 2 }\n')
            restore()
            changed = run("cargo", "build", "--offline", "-v").stderr
            self.assertIn("Compiling fixture-library", changed)
            self.assertEqual(run("target/debug/fixture-server").stdout.strip(), f"2 {commit}")

            env["AENV_BUILD_COMMIT"] = "docker-revision"
            overridden = run("cargo", "build", "--offline", "-v").stderr
            self.assertIn("Fresh fixture-library", overridden)
            self.assertEqual(run("target/debug/fixture-server").stdout.strip(), "2 docker-revision")
