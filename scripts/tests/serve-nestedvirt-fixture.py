# Copyright 2026 AgentENV contributors.
# SPDX-License-Identifier: Apache-2.0
import argparse
import hashlib
import http.server
import json
import shutil
import tarfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("work", type=Path)
parser.add_argument("--port", type=int, default=18091)
args = parser.parse_args()
work = args.work.resolve()
root = work / "oci-root"
shutil.copytree(work / "probe-root", root, dirs_exist_ok=True)
shutil.copy2(Path(__file__).with_name("nestedvirt-fixture-init.sh"), root / "init")
(root / "init").chmod(0o755)
archive = work / "fixture.tar"
with tarfile.open(archive, "w") as output:
    output.add(root, arcname=".")
digest = lambda data: "sha256:" + hashlib.sha256(data).hexdigest()
layer = archive.read_bytes()
ld = digest(layer)
config = json.dumps(
    {
        "architecture": "amd64",
        "os": "linux",
        "config": {},
        "rootfs": {"type": "layers", "diff_ids": [ld]},
    }
).encode()
cd = digest(config)
manifest_type = "application/vnd.oci.image.manifest.v1+json"
manifest = json.dumps(
    {
        "schemaVersion": 2,
        "mediaType": manifest_type,
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": cd,
            "size": len(config),
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": ld,
                "size": len(layer),
            }
        ],
    }
).encode()
blobs = {ld: layer, cd: config}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

    def respond(self, include_body):
        if self.path == "/v2/":
            body, kind = b"{}", "application/json"
        elif self.path.startswith("/v2/nested/manifests/"):
            body, kind = manifest, manifest_type
        elif (
            self.path.startswith("/v2/nested/blobs/")
            and self.path.rsplit("/", 1)[-1] in blobs
        ):
            body, kind = blobs[self.path.rsplit("/", 1)[-1]], "application/octet-stream"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Docker-Content-Digest", digest(body))
        self.end_headers()
        if include_body:
            self.wfile.write(body)


http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
