#!/usr/bin/env python3
"""Actual HTTP contracts for credentialed script clients and redirect rejection."""
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from http_transport import open_without_redirects


def module(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), Path(__file__).with_name(name + '.py'))
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


CAPACITY = module('capacity-run')
SLO = module('slo-run')
OBSERVABILITY = module('check-observability')


class HttpTransportContracts(unittest.TestCase):
    def setUp(self):
        self.servers = []
        self.destination_requests = []
        self.origin_requests = []
        self.status = 302
        self.body = b'{"bufferRecords":0}'
        owner = self

        class Destination(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                owner.destination_requests.append(self.headers.get('Authorization'))
                self.send_response(200)
                self.end_headers()

            do_POST = do_GET

        destination = self.serve(Destination)

        class Origin(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                owner.origin_requests.append((self.command, self.path, self.headers.get('Authorization')))
                self.rfile.read(int(self.headers.get('Content-Length', '0')))
                self.send_response(owner.status)
                if owner.status != 200:
                    self.send_header('Location', f'http://127.0.0.1:{destination.server_port}/capture')
                self.send_header('Content-Length', str(len(owner.body)))
                self.end_headers()
                self.wfile.write(owner.body)

            do_POST = do_GET

        self.origin = self.serve(Origin)
        self.url = f'http://127.0.0.1:{self.origin.server_port}'

    def serve(self, handler):
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        server.daemon_threads = True
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        thread.start()
        self.servers.append((server, thread))
        return server

    def tearDown(self):
        for server, thread in self.servers:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def test_redirect_codes_never_forward_authorization_or_rewrite_posts(self):
        for status in (301, 302, 303, 307, 308):
            for method in ('GET', 'POST'):
                with self.subTest(status=status, method=method):
                    self.status = status
                    request = urllib.request.Request(self.url, method=method,
                        data=b'{}' if method == 'POST' else None,
                        headers={'Authorization': 'Bearer synthetic-transport-token'})
                    with self.assertRaises(urllib.error.HTTPError) as failure:
                        open_without_redirects(request, timeout=1)
                    self.assertEqual(status, failure.exception.code)
                    failure.exception.close()
                    self.assertEqual((method, '/', 'Bearer synthetic-transport-token'), self.origin_requests[-1])
                    self.assertEqual([], self.destination_requests)

    def test_capacity_observer_rejects_redirect_and_keeps_valid_metrics(self):
        self.assertIsNone(CAPACITY.operations('synthetic-observer-token', self.url))
        self.assertEqual([], self.destination_requests)
        self.status = 200
        self.assertEqual({'bufferRecords': 0}, CAPACITY.operations('synthetic-observer-token', self.url))

    def test_slo_metrics_reject_redirect_before_creating_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / 'histograms.prom'
            with self.assertRaises(urllib.error.HTTPError) as failure:
                SLO.metrics('synthetic-observer-token', self.url, output)
            failure.exception.close()
            self.assertEqual([], self.destination_requests)
            self.assertFalse(output.exists())
            self.status = 200
            self.body = b'telemetry_append_phase_seconds_count 1\n'
            SLO.metrics('synthetic-observer-token', self.url, output)
            self.assertEqual(self.body.decode(), output.read_text())

    def test_observability_never_redirects_basic_credentials(self):
        credential = base64.b64encode(b'operator:synthetic-grafana-password').decode()
        with self.assertRaises(urllib.error.HTTPError) as failure:
            OBSERVABILITY.read(self.url, credential)
        failure.exception.close()
        self.assertEqual([], self.destination_requests)
        self.assertEqual('Basic ' + credential, self.origin_requests[-1][2])
        self.status = 200
        self.body = json.dumps({'meta': {'provisioned': True}}).encode()
        self.assertEqual({'meta': {'provisioned': True}}, OBSERVABILITY.read(self.url, credential))


if __name__ == '__main__':
    unittest.main()
