#!/usr/bin/env python3
"""Contract tests for the experiment client, independent of deployment tests."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid


class LoadContracts(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.token = self.root / 'token'
        self.token.write_text('synthetic-load-contract-token')
        self.mode = 'visible'
        self.ids = set()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, value):
                body = json.dumps(value).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                event = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                owner.ids.add(event['eventId'])
                if owner.mode == 'truncated-receipt':
                    self.send_response(202)
                    self.send_header('Content-Length', '100')
                    self.end_headers()
                    self.wfile.write(b'{')
                    self.close_connection = True
                elif owner.mode == 'missing-receipt':
                    self.reply(202, {})
                elif owner.mode == 'mismatched-receipt':
                    self.reply(202, {'eventId': '00000000-0000-0000-0000-000000000000'})
                else:
                    self.reply(202, {'eventId': event['eventId']})

            def do_GET(self):
                event_id = self.path.rsplit('/', 1)[-1]
                if owner.mode == 'missing' or event_id not in owner.ids:
                    self.reply(404, {})
                elif owner.mode == 'mismatch':
                    self.reply(200, {'eventId': '00000000-0000-0000-0000-000000000000'})
                else:
                    self.reply(200, {'eventId': event_id})

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.folder.cleanup()

    def run_client(self, extra=()):
        url = 'http://127.0.0.1:' + str(self.server.server_port)
        output = self.root / str(uuid.uuid4())
        script = Path(__file__).with_name('load.py')
        command = [sys.executable, str(script), '--collector-url', url, '--worker-url', url,
                   '--token-file', str(self.token), '--rate', '100', '--duration', '0.02',
                   '--concurrency', '2', '--timeout', '1', '--settle', '0.3', '--output', str(output)]
        result = subprocess.run([*command, *extra], capture_output=True, text=True, timeout=10)
        self.assertNotIn(self.token.read_text(), result.stdout + result.stderr)
        return result, output

    def test_reconciles_valid_accepted_ids_and_omits_tokens(self):
        result, output = self.run_client()
        self.assertEqual(0, result.returncode, result.stderr)
        summary = json.loads((output / 'summary.json').read_text())
        self.assertEqual({'202': 2}, summary['outcomes'])
        self.assertEqual(2, summary['accepted_reconciliation']['visible'])
        self.assertTrue(summary['accepted_reconciliation']['passed'])
        self.assertNotIn(self.token.read_text(), (output / 'requests.jsonl').read_text())

    def test_missing_accepted_ids_fail_reconciliation(self):
        self.mode = 'missing'
        result, output = self.run_client()
        self.assertEqual(1, result.returncode, result.stderr)
        summary = json.loads((output / 'summary.json').read_text())
        self.assertFalse(summary['accepted_reconciliation']['passed'])
        self.assertEqual(2, len(summary['accepted_reconciliation']['unresolved_ids']))

    def test_a_mismatched_query_id_does_not_count_as_visible(self):
        self.mode = 'mismatch'
        result, output = self.run_client()
        self.assertEqual(1, result.returncode, result.stderr)
        summary = json.loads((output / 'summary.json').read_text())
        self.assertEqual(0, summary['accepted_reconciliation']['visible'])
        self.assertFalse(summary['accepted_reconciliation']['passed'])
        self.assertGreater(summary['query_outcomes'].get('invalid_response', 0), 0)

    def test_invalid_settings_are_rejected_before_sending(self):
        result, _ = self.run_client(['--rate', '0'])
        self.assertEqual(2, result.returncode)
        self.assertFalse(self.ids)

    def test_invalid_acceptance_receipts_remain_ambiguous(self):
        for mode in ('missing-receipt', 'mismatched-receipt'):
            with self.subTest(mode=mode):
                self.mode = mode
                result, output = self.run_client()
                self.assertEqual(1, result.returncode, result.stderr)
                summary = json.loads((output / 'summary.json').read_text())
                self.assertEqual({'invalid_response': 2}, summary['outcomes'])
                self.assertEqual(0, summary['accepted_reconciliation']['accepted'])
                self.assertFalse(summary['accepted_reconciliation']['passed'])
                self.assertEqual({'count': 2, 'later_visible': 2}, summary['ambiguous_client_outcomes'])

    def test_truncated_receipts_record_every_scheduled_outcome(self):
        self.mode = 'truncated-receipt'
        result, output = self.run_client()
        self.assertEqual(1, result.returncode, result.stderr)
        summary = json.loads((output / 'summary.json').read_text())
        self.assertEqual(2, summary['scheduled'])
        self.assertEqual(2, sum(summary['outcomes'].values()))
        self.assertEqual(2, summary['ambiguous_client_outcomes']['count'])
        self.assertEqual(2, summary['ambiguous_client_outcomes']['later_visible'])
        self.assertFalse(summary['accepted_reconciliation']['passed'])


if __name__ == '__main__':
    unittest.main()
