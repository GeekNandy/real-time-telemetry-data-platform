#!/usr/bin/env python3
"""Real HTTP startup-reset and never-ready contracts for the readiness helper."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
from pathlib import Path
import socket
import struct
import threading
import unittest

spec = importlib.util.spec_from_file_location('wait_ready', Path(__file__).with_name('wait-ready.py'))
wait_ready = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wait_ready)


class ReadinessContracts(unittest.TestCase):
    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server, thread in self.servers:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def serve(self, reset_first=False, never_ready=False):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.server.requests += 1
                if reset_first and self.server.requests == 1:
                    self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                    self.connection.close()
                    return
                self.send_response(503 if never_ready else 200)
                self.send_header('Content-Length', '2')
                self.end_headers()
                self.wfile.write(b'{}')

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.daemon_threads = True
        server.requests = 0
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        thread.start()
        self.servers.append((server, thread))
        return server

    def test_retries_a_startup_reset_and_checks_both_roles(self):
        collector = self.serve(reset_first=True)
        worker = self.serve()
        wait_ready.wait_for_roles([collector.server_port, worker.server_port], timeout=3, interval=0.02)
        self.assertGreaterEqual(collector.requests, 2)
        self.assertGreaterEqual(worker.requests, 1)

    def test_never_ready_role_fails_at_the_deadline(self):
        worker = self.serve(never_ready=True)
        with self.assertRaises(SystemExit):
            wait_ready.wait_for_roles([worker.server_port], timeout=0.2, interval=0.02)
        self.assertGreater(worker.requests, 0)


if __name__ == '__main__':
    unittest.main()
