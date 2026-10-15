# Copyright 2026 AgentENV contributors.
# SPDX-License-Identifier: Apache-2.0
import argparse
import http.client
import json
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("work", type=Path)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.work, prefix="fixture-test-") as directory:
        work = Path(directory)
        root = work / "probe-root"
        root.mkdir()
        (root / "retained").write_text("current\n")
        (root / "removed").write_text("old\n")
        for iteration in range(2):
            if iteration:
                (root / "removed").unlink()
                (root / "retained").write_text("updated\n")
            port_file = work / f"server-{iteration}.port"
            with (work / f"server-{iteration}.log").open("w") as log:
                process = subprocess.Popen(
                    [sys.executable, str(Path(__file__).with_name("serve-nestedvirt-fixture.py")),
                     str(work), "--port", "0", "--port-file", str(port_file)],
                    stdout=log, stderr=log,
                )
                try:
                    deadline = time.monotonic() + 10
                    while True:
                        if process.poll() is not None:
                            raise RuntimeError("Fixture server exited before readiness")
                        if not port_file.exists():
                            if time.monotonic() >= deadline:
                                raise TimeoutError("Fixture server did not publish its port")
                            time.sleep(0.05)
                            continue
                        port = int(port_file.read_text())
                        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                        try:
                            connection.request("GET", "/v2/nested/manifests/latest")
                            response = connection.getresponse()
                            if response.status != 200:
                                raise RuntimeError(
                                    f"Fixture manifest returned HTTP {response.status}, expected 200"
                                )
                            manifest = json.loads(response.read())
                            break
                        except OSError:
                            if time.monotonic() >= deadline:
                                raise
                            time.sleep(0.05)
                        finally:
                            connection.close()
                    size = manifest["layers"][0]["size"]
                    actual_size = (work / "fixture.tar").stat().st_size
                    if size != actual_size:
                        raise RuntimeError(
                            f"Fixture layer size {size} differs from archive size {actual_size}"
                        )
                    with tarfile.open(work / "fixture.tar") as archive:
                        names = set(archive.getnames())
                        if ("./removed" in names) != (iteration == 0):
                            raise RuntimeError(
                                f"Unexpected removed-file presence in iteration {iteration}: {sorted(names)}"
                            )
                        expected = b"current\n" if iteration == 0 else b"updated\n"
                        if archive.extractfile("./retained").read() != expected:
                            raise RuntimeError(
                                f"Retained file content mismatch in iteration {iteration}"
                            )
                        if archive.extractfile("./init").read() != Path(__file__).with_name(
                            "nestedvirt-fixture-init.sh"
                        ).read_bytes():
                            raise RuntimeError(
                                f"Fixture init content mismatch in iteration {iteration}"
                            )
                finally:
                    process.terminate()
                    process.wait(timeout=10)
    print("PASS fixture rebuild removes deleted files and preserves current content")


if __name__ == "__main__":
    main()
