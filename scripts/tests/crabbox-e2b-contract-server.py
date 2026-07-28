#!/usr/bin/env python3
"""Strict local E2B contract server for the Crabbox CI smoke test."""

from __future__ import annotations

import argparse
import base64
import json
import ssl
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

SANDBOX_ID = "019c6f83-4df1-7e70-8000-000000000035"
API_KEY = "agentenv-ci-placeholder"
ACCESS_TOKEN = "agentenv-contract-access-token"
MARKER = "crabbox-agentenv-contract-ok"


class ContractState:
    def __init__(self, data_port: int) -> None:
        self.data_port = data_port
        self.metadata: dict[str, str] = {}
        self.counts = {
            "list": 0,
            "create": 0,
            "connect": 0,
            "upload": 0,
            "process": 0,
            "marker_commands": 0,
            "delete": 0,
        }
        self.errors: list[str] = []
        self.lock = threading.Lock()

    def record(self, name: str) -> None:
        with self.lock:
            self.counts[name] += 1

    def record_error(self, error: BaseException) -> None:
        with self.lock:
            self.errors.append(str(error))

    def snapshot(self) -> dict[str, object]:
        with self.lock:
            return {
                **self.counts,
                "errors": list(self.errors),
            }

    def sandbox(self) -> dict[str, object]:
        return {
            "templateID": "ubuntu",
            "sandboxID": SANDBOX_ID,
            "clientID": "",
            "envdVersion": "contract",
            "envdAccessToken": ACCESS_TOKEN,
            "domain": f"localhost:{self.data_port}",
            "metadata": dict(self.metadata),
        }


class ContractServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        state: ContractState,
        plane: str,
    ) -> None:
        self.state = state
        self.plane = plane
        super().__init__(server_address, handler)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def contract_server(self) -> ContractServer:
        assert isinstance(self.server, ContractServer)
        return self.server

    @property
    def state(self) -> ContractState:
        return self.contract_server.state

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send_bytes(200, b"ok")
                return
            if parsed.path == "/contract-state":
                self._send_json(200, self.state.snapshot())
                return
            assert self.contract_server.plane == "control", self.path
            if parsed.path == "/v2/sandboxes":
                self._assert_api_key()
                query = parse_qs(parsed.query)
                assert query.get("limit") == ["100"], query
                assert query.get("state") == ["running,paused"], query
                metadata = parse_qs(query.get("metadata", [""])[0])
                assert metadata == {
                    "crabbox": ["true"],
                    "provider": ["e2b"],
                }, metadata
                self.state.record("list")
                self._send_json(200, [])
                return
            if parsed.path == f"/sandboxes/{SANDBOX_ID}":
                self._assert_api_key()
                self._send_json(200, self.state.sandbox())
                return
            raise AssertionError(f"unexpected GET {self.path}")
        except BaseException as error:
            self._fail(error)

    def do_POST(self) -> None:
        try:
            if self.contract_server.plane == "data":
                parsed = urlparse(self.path)
                if parsed.path == "/files":
                    self._handle_upload(parsed.query)
                else:
                    self._handle_process()
                return

            self._assert_api_key()
            parsed = urlparse(self.path)
            body = self._read_json()
            if parsed.path == "/sandboxes":
                assert body["templateID"] == "ubuntu", body
                assert body["secure"] is True, body
                assert body["allow_internet_access"] is True, body
                assert body["metadata"]["crabbox"] == "true", body
                assert body["metadata"]["provider"] == "e2b", body
                self.state.metadata = body["metadata"]
                self.state.record("create")
                self._send_json(201, self.state.sandbox())
                return
            if parsed.path == f"/sandboxes/{SANDBOX_ID}/connect":
                assert body["timeout"] > 0, body
                self.state.record("connect")
                self._send_json(200, self.state.sandbox())
                return
            raise AssertionError(f"unexpected POST {self.path}")
        except BaseException as error:
            self._fail(error)

    def do_DELETE(self) -> None:
        try:
            assert self.contract_server.plane == "control", self.path
            self._assert_api_key()
            assert urlparse(self.path).path == f"/sandboxes/{SANDBOX_ID}", self.path
            self.state.record("delete")
            self._send_bytes(204, b"")
        except BaseException as error:
            self._fail(error)

    def _handle_process(self) -> None:
        parsed = urlparse(self.path)
        assert parsed.path == "/process.Process/Start", self.path
        self._assert_data_plane_headers()
        assert self.headers.get("Connect-Protocol-Version") == "1", self.headers

        raw = self._read_body()
        assert len(raw) >= 5 and raw[0] == 0, raw
        size = struct.unpack(">I", raw[1:5])[0]
        assert size == len(raw) - 5, (size, len(raw))
        request = json.loads(raw[5:])
        process = request["process"]
        assert process["cmd"] == "/bin/bash", process
        assert process["args"][:2] == ["-l", "-c"], process
        command = process["args"][2]

        self.state.record("process")
        output = b""
        if MARKER in command:
            self.state.record("marker_commands")
            output = f"{MARKER}\n".encode()

        response = b"".join(
            [
                self._connect_envelope({"event": {"start": {"pid": 35}}}),
                self._connect_envelope(
                    {
                        "event": {
                            "data": {
                                "stdout": base64.b64encode(output).decode(),
                            }
                        }
                    }
                ),
                self._connect_envelope(
                    {
                        "event": {
                            "end": {
                                "exitCode": 0,
                                "exited": True,
                                "status": "exited",
                            }
                        }
                    }
                ),
                bytes([2]) + struct.pack(">I", 0),
            ]
        )
        self._send_bytes(200, response, "application/connect+json")

    def _handle_upload(self, raw_query: str) -> None:
        self._assert_data_plane_headers()
        query = parse_qs(raw_query)
        target = query.get("path", [""])[0]
        assert target.startswith("/tmp/crabbox-") and target.endswith(".tgz"), query
        assert self.headers.get("Content-Type", "").startswith(
            "multipart/form-data;"
        ), self.headers
        body = self._read_body()
        assert len(body) > 100, len(body)
        self.state.record("upload")
        self._send_json(200, {})

    def _assert_data_plane_headers(self) -> None:
        expected_host = f"49983-{SANDBOX_ID}.localhost:{self.state.data_port}"
        assert self.headers.get("Host") == expected_host, self.headers
        assert self.headers.get("E2b-Sandbox-Id") == SANDBOX_ID, self.headers
        assert self.headers.get("E2b-Sandbox-Port") == "49983", self.headers
        assert self.headers.get("X-Access-Token") == ACCESS_TOKEN, self.headers

    def _assert_api_key(self) -> None:
        assert self.headers.get("X-API-Key") == API_KEY, self.headers

    def _read_body(self) -> bytes:
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size_line = self.rfile.readline()
                assert size_line, "chunked body ended before its zero chunk"
                size = int(size_line.split(b";", 1)[0].strip(), 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b""):
                        pass
                    break
                chunks.append(self.rfile.read(size))
                assert self.rfile.read(2) == b"\r\n", "invalid chunk terminator"
            return b"".join(chunks)
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length)

    def _read_json(self) -> dict[str, Any]:
        return json.loads(self._read_body())

    @staticmethod
    def _connect_envelope(payload: dict[str, object]) -> bytes:
        data = json.dumps(payload, separators=(",", ":")).encode()
        return bytes([0]) + struct.pack(">I", len(data)) + data

    def _send_json(self, status: int, payload: object) -> None:
        self._send_bytes(
            status,
            json.dumps(payload, separators=(",", ":")).encode(),
            "application/json",
        )

    def _send_bytes(
        self,
        status: int,
        body: bytes,
        content_type: str = "text/plain",
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _fail(self, error: BaseException) -> None:
        self.state.record_error(error)
        self._send_json(500, {"error": str(error)})

    def log_message(self, format: str, *args: object) -> None:
        del format, args


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--control-port", type=int, default=18080)
    parser.add_argument("--data-port", type=int, default=18443)
    args = parser.parse_args()

    state = ContractState(args.data_port)
    control = ContractServer(
        ("127.0.0.1", args.control_port), Handler, state, "control"
    )
    data = ContractServer(("127.0.0.1", args.data_port), Handler, state, "data")
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(args.cert, args.key)
    data.socket = tls.wrap_socket(data.socket, server_side=True)

    data_thread = threading.Thread(target=data.serve_forever, daemon=True)
    data_thread.start()
    print(
        f"contract server ready control={args.control_port} data={args.data_port}",
        flush=True,
    )
    try:
        control.serve_forever()
    finally:
        data.shutdown()
        data.server_close()
        control.server_close()


if __name__ == "__main__":
    main()
