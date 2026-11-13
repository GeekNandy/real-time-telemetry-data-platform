#!/usr/bin/env python3
"""Exercise the exact Compose broker probe against real TCP protocol peers."""
from contextlib import contextmanager
from pathlib import Path
import socket
import struct
import subprocess
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "scripts/kafka-healthcheck.sh"
REQUEST = struct.pack(">ihhih", 10, 18, 0, 1, 0)
BROKER_APIS = ((0, 3, 11), (3, 0, 12), (18, 0, 4))


def response(apis=BROKER_APIS, *, correlation=1, error=0, count=None):
    body = struct.pack(">ihi", correlation, error, len(apis) if count is None else count)
    body += b"".join(struct.pack(">hhh", *api) for api in apis)
    return struct.pack(">i", len(body)) + body


@contextmanager
def peer(handler):
    """One peer records the actual request before returning a test response."""
    stop = threading.Event()
    requests = []
    errors = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(5)

        def serve():
            try:
                with listener.accept()[0] as connection:
                    connection.settimeout(5)
                    request = bytearray()
                    while len(request) < len(REQUEST):
                        chunk = connection.recv(len(REQUEST) - len(request))
                        if not chunk:
                            break
                        request.extend(chunk)
                    requests.append(bytes(request))
                    handler(connection, stop)
            except (BrokenPipeError, ConnectionResetError):
                # A timed-out probe is expected to close a slow peer.
                pass
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            yield listener.getsockname()[1], requests
        finally:
            stop.set()
            thread.join(timeout=6)
            if thread.is_alive():
                raise AssertionError("socket peer failed to terminate")
            if errors:
                raise errors[0]


class KafkaHealthcheckContracts(unittest.TestCase):
    def run_probe(self, handler, *, healthy=False, maximum_seconds=2):
        with peer(handler) as (port, requests):
            started = time.monotonic()
            result = subprocess.run(
                ["bash", str(PROBE), "127.0.0.1", str(port)],
                capture_output=True, timeout=6, check=False,
            )
            elapsed = time.monotonic() - started
            self.assertEqual(result.returncode == 0, healthy, result.stderr.decode())
            self.assertLess(elapsed, maximum_seconds, "probe exceeded its deadline")
            self.assertEqual(requests, [REQUEST], "probe must send ApiVersions v0")

    def reply(self, payload, **kwargs):
        self.run_probe(lambda connection, stop: connection.sendall(payload), **kwargs)

    def test_valid_broker_api_response(self):
        self.reply(response(), healthy=True)

    def test_fragmented_response_does_not_require_one_socket_read(self):
        def fragmented(connection, stop):
            for byte in response():
                connection.sendall(bytes([byte]))
                if stop.wait(0.005):
                    return
        self.run_probe(fragmented, healthy=True)

    def test_tcp_accept_or_immediate_eof_is_not_healthy(self):
        self.reply(b"")

    def test_refused_connection_is_unhealthy(self):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            result = subprocess.run(
                ["bash", str(PROBE), "127.0.0.1", str(reserved.getsockname()[1])],
                capture_output=True, timeout=6, check=False,
            )
            self.assertNotEqual(result.returncode, 0)

    def test_truncated_frame_header_and_body_are_unhealthy(self):
        for payload in (b"\x00\x00\x00", response()[:-1]):
            with self.subTest(payload=payload):
                self.reply(payload)

    def test_invalid_lengths_fail_before_waiting_for_body(self):
        for length in (0, 9, 4097, 2147483647, -1):
            with self.subTest(length=length):
                def oversized(connection, stop):
                    connection.sendall(struct.pack(">i", length))
                    stop.wait(5)
                self.run_probe(oversized)

    def test_unrelated_protocol_or_nonmatching_correlation_is_unhealthy(self):
        for payload in (b"HTTP/1.1 200 OK\r\n\r\n", response(correlation=2)):
            with self.subTest(payload=payload):
                self.reply(payload)

    def test_broker_error_is_unhealthy(self):
        self.reply(response(error=35))  # UNSUPPORTED_VERSION

    def test_missing_broker_apis_and_controller_only_response_are_unhealthy(self):
        for apis in ((), ((0, 3, 11),), ((3, 0, 12),), ((18, 0, 4), (55, 0, 1))):
            with self.subTest(apis=apis):
                self.reply(response(apis))

    def test_malformed_api_array_is_unhealthy(self):
        for payload in (
            response(count=-1), response(count=2), response(count=4),
            response(BROKER_APIS + ((0, 3, 11),)),
            response(BROKER_APIS + ((-1, 0, 1),)),
            response(BROKER_APIS + ((42, -1, 1),)),
            response(BROKER_APIS + ((42, 0, -1),)),
            response(BROKER_APIS + ((42, 2, 1),)),
        ):
            with self.subTest(payload=payload):
                self.reply(payload)

    def test_partial_entry_or_extra_body_byte_is_unhealthy(self):
        valid = response()
        for body in (valid[4:-1], valid[4:] + b"\x00"):
            with self.subTest(body=body):
                self.reply(struct.pack(">i", len(body)) + body)

    def test_silent_peer_is_bounded(self):
        self.run_probe(lambda connection, stop: stop.wait(5), maximum_seconds=4.5)

    def test_partial_body_stall_is_bounded(self):
        def stalled(connection, stop):
            connection.sendall(response()[:-1])
            stop.wait(5)
        self.run_probe(stalled, maximum_seconds=4.5)

    def test_slow_trickle_cannot_extend_whole_probe_deadline(self):
        def trickle(connection, stop):
            for byte in response():
                connection.sendall(bytes([byte]))
                if stop.wait(0.15):
                    return
        self.run_probe(trickle, maximum_seconds=4.5)


if __name__ == "__main__":
    unittest.main()
