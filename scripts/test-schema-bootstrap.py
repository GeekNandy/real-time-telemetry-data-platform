#!/usr/bin/env python3
"""Exercise the schema migration CLI against real HTTP peers, using synthetic credentials."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import unittest


class SchemaBootstrapContracts(unittest.TestCase):
    def setUp(self):
        self.servers = []
        self.mode = 'normal'
        self.redirect_method = 'GET'
        self.requests = []
        self.destination_requests = []
        self.token = 'synthetic-schema-migration-credential'
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

        class Registry(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, value):
                data = json.dumps(value).encode()
                self.send_response(status)
                if owner.mode == 'redirect':
                    self.send_header('Location', f'http://127.0.0.1:{destination.server_port}/capture')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def handle_request(self):
                raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                body = json.loads(raw) if raw else None
                owner.requests.append((self.command, self.path, self.headers.get('Authorization'), body))
                if owner.mode == 'redirect' and self.command == owner.redirect_method:
                    self.reply(302, {'upstream': owner.token})
                elif owner.mode == 'oversized':
                    self.reply(200, {'padding': 'x' * 1_048_576})
                elif owner.mode == 'invalid':
                    self.reply(200, [owner.token])
                elif self.command == 'GET':
                    self.reply(404, {})
                elif self.command == 'PUT':
                    self.reply(200, {'compatibility': 'FULL_TRANSITIVE'})
                elif self.path.startswith('/compatibility/'):
                    self.reply(404, {})
                elif owner.mode == 'registration-response':
                    self.reply(200, owner.registration_response)
                else:
                    self.reply(200, {'id': len([r for r in owner.requests if r[1].startswith('/subjects/')])})

            do_GET = handle_request
            do_PUT = handle_request
            do_POST = handle_request

        self.registry = self.serve(Registry)

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

    def bootstrap(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('schema-bootstrap.py')),
            '--registry-url', f'http://127.0.0.1:{self.registry.server_port}', '--allow-local-http'],
            env=dict(os.environ, SCHEMA_REGISTRY_AUTHORIZATION=self.token),
            capture_output=True, text=True, timeout=10)
        self.assertNotIn(self.token, result.stdout + result.stderr)
        return result

    def test_normal_bootstrap_sets_policy_and_registers_both_bundled_schemas(self):
        result = self.bootstrap()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual([{'schema': 'telemetry-v1.avsc', 'id': 1}, {'schema': 'telemetry-v2.avsc', 'id': 2}],
                         [json.loads(line) for line in result.stdout.splitlines()])
        self.assertTrue(all(r[2] == self.token for r in self.requests))
        writes = [r for r in self.requests if r[0] == 'PUT']
        self.assertEqual([{'compatibility': 'FULL_TRANSITIVE'}], [r[3] for r in writes])
        registrations = [r for r in self.requests if r[1].startswith('/subjects/')]
        self.assertEqual(2, len(registrations))
        self.assertTrue(all(r[3]['schemaType'] == 'AVRO' for r in registrations))

    def test_redirected_reads_and_writes_never_reach_another_origin(self):
        for method in ('GET', 'POST'):
            with self.subTest(method=method):
                self.mode = 'redirect'
                self.redirect_method = method
                result = self.bootstrap()
                self.assertNotEqual(0, result.returncode)
                self.assertIn('registry HTTP 302', result.stderr)
                self.assertEqual([], self.destination_requests)

    def test_oversized_registry_body_stops_migration(self):
        self.mode = 'oversized'
        result = self.bootstrap()
        self.assertNotEqual(0, result.returncode)
        self.assertIn('response exceeds the size bound', result.stderr)
        self.assertEqual(1, len(self.requests))

    def test_nonobject_registry_body_fails_without_echoing_upstream_content(self):
        self.mode = 'invalid'
        result = self.bootstrap()
        self.assertNotEqual(0, result.returncode)
        self.assertIn('invalid registry response', result.stderr)
        self.assertEqual(1, len(self.requests))

    def test_invalid_schema_ids_fail_without_echoing_upstream_content(self):
        self.mode = 'registration-response'
        for value in ({}, {'id': self.token}, {'id': True}, {'id': 0}, {'id': -1},
                      {'id': 2_147_483_648}, {'id': 1.0}, {'id': None}):
            with self.subTest(value=value):
                self.registration_response = value
                result = self.bootstrap()
                self.assertNotEqual(0, result.returncode)
                self.assertIn('invalid registry response', result.stderr)
                self.assertEqual('', result.stdout)


if __name__ == '__main__':
    unittest.main()
