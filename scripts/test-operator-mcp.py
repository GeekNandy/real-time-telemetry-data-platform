#!/usr/bin/env python3
"""Exercise the packaged Java SDK stdio server against real HTTP peers; no external services."""
import http.server
import json
import os
from pathlib import Path
import queue
import ssl
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "synthetic.observe.token"
PRIVATE = "synthetic-private-value-never-return"


class Peer(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        self.server.requests.append((self.command, self.path, self.headers.get("Authorization")))
        mode = self.server.mode
        status = 200
        if mode == "redirect":
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/private-target")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/actuator/health":
            data = {"status": "UP", "components": {"private": PRIVATE}}
        else:
            if self.headers.get("Authorization") != "Bearer " + self.server.expected_token:
                status = 401
            if mode == "forbidden":
                status = 403
            data = {
                "component": "collector", "instanceId": PRIVATE,
                "observedAt": "2026-10-29T00:00:00Z", "acceptedTotal": 12,
                "bufferRecords": 0, "bufferBytes": 0, "oldestBufferedAgeSeconds": 0,
                "appendPendingRecords": 1, "appendPendingBytes": 900,
                "tenantId": PRIVATE, "events": [{"payload": PRIVATE}],
                "stages": {"ingress": {"completed": 12, "failures": 0, "payload": PRIVATE}, PRIVATE: {"completed": 9}},
            }
        if status != 200:
            data = {"error": PRIVATE, "authorization": TOKEN}
        encoded = json.dumps(data).encode()
        if mode == "oversized":
            encoded = b"x" * (128 * 1024)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        try:
            if mode == "drip":
                for byte in encoded:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    if self.server.stop_drip.wait(0.04):
                        break
            else:
                self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass


class Client:
    def __init__(self, env, directory):
        self.errors = open(Path(directory) / f"stderr-{time.monotonic_ns()}.txt", "w+")
        try:
            self.process = subprocess.Popen([str(ROOT / "scripts/operator-mcp.sh")], cwd=ROOT,
                                            env={**os.environ, **env}, stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=self.errors, text=True, bufsize=1)
        except BaseException:
            self.errors.close()
            raise
        self.lines = queue.Queue()
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()
        self.next_id = 1

    def read(self):
        for line in self.process.stdout:
            try:
                self.lines.put(json.loads(line))
            except json.JSONDecodeError:
                self.lines.put({"invalid_stdout": line})

    def send(self, method, params=None, notification=False):
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        if not notification:
            message["id"] = self.next_id
            self.next_id += 1
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()
        if notification:
            return
        result = self.lines.get(timeout=12)
        if "invalid_stdout" in result or result.get("id") != message["id"]:
            raise AssertionError(result)
        return result

    def initialize(self):
        response = self.send("initialize", {"protocolVersion": "2025-11-25", "clientInfo": {"name": "contract-test", "version": "1"}, "capabilities": {}})
        assert response["result"]["serverInfo"]["name"] == "telemetry-operator", response
        self.send("notifications/initialized", notification=True)

    def call(self, name, arguments=None):
        return self.send("tools/call", {"name": name, "arguments": arguments or {}})

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.process.stdin.close()
        self.process.stdout.close()
        self.reader.join(timeout=1)
        self.errors.seek(0)
        errors = self.errors.read()
        self.errors.close()
        return errors


class OperatorContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.token = Path(self.temp.name) / "observe.token"
        self.token.write_text(TOKEN + "\n")
        self.token.chmod(0o600)
        self.peer = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Peer)
        self.peer.daemon_threads = True
        self.peer.mode = "normal"
        self.peer.expected_token = TOKEN
        self.peer.requests = []
        self.peer.stop_drip = threading.Event()
        self.thread = threading.Thread(target=self.peer.serve_forever, daemon=True)
        self.thread.start()
        url = f"http://127.0.0.1:{self.peer.server_port}"
        self.env = {"TELEMETRY_COLLECTOR_URL": url, "TELEMETRY_WORKER_URL": url,
                    "TELEMETRY_OBSERVE_TOKEN_FILE": str(self.token)}
        self.client = Client(self.env, self.temp.name)
        self.client.initialize()

    def tearDown(self):
        self.peer.stop_drip.set()
        errors = self.client.close()
        self.assertNotIn(TOKEN, errors)
        self.assertNotIn(PRIVATE, errors)
        self.peer.shutdown()
        self.peer.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    @staticmethod
    def output(response):
        result = response["result"]
        assert result.get("isError") is False, response
        return result["structuredContent"]

    def assertToolError(self, response, code=None):
        self.assertTrue("error" in response or response.get("result", {}).get("isError"), response)
        if code:
            self.assertEqual(code, response["result"]["structuredContent"]["error"])
        self.assertNotIn(PRIVATE, json.dumps(response))
        self.assertNotIn(TOKEN, json.dumps(response))

    def test_discovery_all_four_tools_and_public_bundled_data(self):
        tools = self.client.send("tools/list")["result"]["tools"]
        self.assertEqual({t["name"] for t in tools}, {"get_platform_health", "get_ingestion_metrics", "get_event_schema", "get_benchmark_results"})
        for tool in tools:
            self.assertTrue(tool["annotations"]["readOnlyHint"])
            self.assertFalse(tool["inputSchema"]["additionalProperties"])
        for version in (1, 2):
            schema = self.output(self.client.call("get_event_schema", {"version": version}))
            self.assertEqual("TelemetryEvent", schema["name"])
            self.assertEqual("collectorSource" in {x["name"] for x in schema["fields"]}, version == 2)
        benchmark = self.output(self.client.call("get_benchmark_results"))
        self.assertFalse(benchmark["evidence"]["release_ready"])
        self.assertEqual(500, benchmark["profiles"]["scale"]["rate_per_second"])
        self.assertEqual([], self.peer.requests)

    def test_health_has_no_credentials_and_metrics_are_filtered(self):
        self.assertEqual({"collector": {"status": "UP"}, "worker": {"status": "UP"}}, self.output(self.client.call("get_platform_health")))
        metrics = self.output(self.client.call("get_ingestion_metrics", {"component": "collector"}))
        self.assertEqual(12, metrics["acceptedTotal"])
        self.assertEqual(1, metrics["appendPendingRecords"])
        self.assertNotIn(PRIVATE, json.dumps(metrics))
        self.assertEqual({"ingress": {"completed": 12, "failures": 0}}, metrics["stages"])
        self.assertEqual([("GET", "/actuator/health", None)] * 2 + [("GET", "/v1/operations/metrics", "Bearer " + TOKEN)], self.peer.requests)

    def test_schema_validation_refuses_extra_fields_urls_paths_and_write_tools(self):
        for name, arguments in [
            ("get_event_schema", {"version": 3}), ("get_event_schema", {"version": "../../secret"}),
            ("get_platform_health", {"url": "http://example.com"}),
            ("get_ingestion_metrics", {"component": "arbitrary"}),
            ("get_ingestion_metrics", {"component": "collector", "tenantId": "someone-else"}),
            ("delete_events", {}),
        ]:
            self.assertToolError(self.client.call(name, arguments))
        self.assertEqual([], self.peer.requests)

    def test_api_authorization_is_enforced_and_token_file_can_rotate(self):
        self.token.write_text("invalid.token.value")
        self.assertToolError(self.client.call("get_ingestion_metrics", {"component": "collector"}), "AUTHENTICATION_REQUIRED")
        self.token.write_text(TOKEN)
        self.peer.mode = "forbidden"
        self.assertToolError(self.client.call("get_ingestion_metrics", {"component": "worker"}), "OBSERVE_PERMISSION_REQUIRED")
        self.peer.mode = "normal"
        self.peer.expected_token = "new.synthetic.token"
        self.token.write_text(self.peer.expected_token)
        self.assertEqual(12, self.output(self.client.call("get_ingestion_metrics", {"component": "worker"}))["acceptedTotal"])

    def test_private_bounded_token_file_required(self):
        self.token.chmod(0o644)
        self.assertToolError(self.client.call("get_ingestion_metrics", {"component": "collector"}), "OBSERVE_TOKEN_FILE_NOT_PRIVATE")
        self.token.chmod(0o600)
        self.token.write_text("x" * 9000)
        self.assertToolError(self.client.call("get_ingestion_metrics", {"component": "collector"}), "OBSERVE_TOKEN_INVALID")
        self.assertEqual([], self.peer.requests)

    def test_no_token_keeps_public_reads_available_but_denies_metrics(self):
        client = Client({**self.env, "TELEMETRY_OBSERVE_TOKEN_FILE": ""}, self.temp.name)
        try:
            client.initialize()
            self.assertToolError(client.call("get_ingestion_metrics", {"component": "collector"}), "OBSERVE_TOKEN_NOT_CONFIGURED")
            self.assertEqual([], self.peer.requests)
            self.assertEqual("UP", self.output(client.call("get_platform_health"))["collector"]["status"])
        finally:
            client.close()

    def test_redirect_and_large_body_refused_without_exposing_upstream(self):
        for mode in ("redirect", "oversized"):
            self.peer.mode = mode
            self.assertToolError(self.client.call("get_ingestion_metrics", {"component": "collector"}))
        self.assertFalse(any(path == "/private-target" for _, path, _ in self.peer.requests))
        self.peer.mode = "normal"
        self.assertEqual(12, self.output(self.client.call("get_ingestion_metrics", {"component": "collector"}))["acceptedTotal"])

    def test_drip_body_has_absolute_deadline_and_next_call_recovers(self):
        self.peer.mode = "drip"
        began = time.monotonic()
        self.assertToolError(self.client.call("get_ingestion_metrics", {"component": "collector"}))
        self.assertLess(time.monotonic() - began, 5)
        self.peer.stop_drip.set()
        self.peer.mode = "normal"
        self.assertEqual(12, self.output(self.client.call("get_ingestion_metrics", {"component": "collector"}))["acceptedTotal"])

    def test_oversized_unterminated_stdio_message_closes_process(self):
        self.client.process.stdin.write("x" * 65537)
        self.client.process.stdin.flush()
        self.client.process.wait(timeout=5)
        self.assertTrue(self.client.lines.empty())

    def test_https_does_not_trust_an_unapproved_certificate(self):
        directory = Path(self.temp.name)
        cert, key = directory / "server.pem", directory / "server.key"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", str(key), "-out", str(cert), "-days", "1",
                        "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        tls = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Peer)
        tls.daemon_threads = True
        tls.mode, tls.expected_token, tls.requests = "normal", TOKEN, []
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        tls.socket = context.wrap_socket(tls.socket, server_side=True)
        thread = threading.Thread(target=tls.serve_forever, daemon=True)
        thread.start()
        client = Client({**self.env, "TELEMETRY_COLLECTOR_URL": f"https://127.0.0.1:{tls.server_port}"}, self.temp.name)
        try:
            client.initialize()
            self.assertToolError(client.call("get_ingestion_metrics", {"component": "collector"}), "UPSTREAM_UNAVAILABLE")
            self.assertEqual([], tls.requests)
        finally:
            errors = client.close()
            self.assertNotIn(TOKEN, errors)
            tls.shutdown()
            tls.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
