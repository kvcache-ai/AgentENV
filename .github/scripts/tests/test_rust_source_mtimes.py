import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "restore_rust_source_mtimes.py"
spec = importlib.util.spec_from_file_location("restore_rust_source_mtimes", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SourceTimestampTests(unittest.TestCase):
    def test_only_identical_tracked_files_recover_cached_timestamps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            unchanged = root / "unchanged.rs"
            changed = root / "changed.rs"
            untracked = root / "untracked.rs"
            symlink = root / "symlink.rs"
            for path in (unchanged, changed, untracked):
                path.write_text("old source")
                os.utime(path, ns=(1_000_000_000, 1_000_000_000))
            symlink.symlink_to(unchanged.name)
            subprocess.run(
                ["git", "add", "unchanged.rs", "changed.rs", "symlink.rs"],
                cwd=root,
                check=True,
            )
            manifest = root / "cache.json"
            module.restore(root, manifest)
            changed.write_text("new source")  # Same length; timestamps are not enough.
            for path in (unchanged, changed, untracked):
                os.utime(path, ns=(2_000_000_000, 2_000_000_000))
            module.restore(root, manifest)
            self.assertEqual(unchanged.stat().st_mtime_ns, 1_000_000_000)
            self.assertEqual(changed.stat().st_mtime_ns, 2_000_000_000)
            self.assertEqual(untracked.stat().st_mtime_ns, 2_000_000_000)
            self.assertTrue(symlink.is_symlink())
            # The updated cache records the changed source for the next checkout.
            os.utime(changed, ns=(3_000_000_000, 3_000_000_000))
            module.restore(root, manifest)
            self.assertEqual(changed.stat().st_mtime_ns, 2_000_000_000)
