#!/usr/bin/env python3
"""Exercise the real pinned k6 client against bounded actual HTTP fixtures."""
import copy
from datetime import datetime, timezone
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('slo_tool', ROOT / 'scripts/slo-tool.py')
TOOL = importlib.util.module_from_spec(spec); spec.loader.exec_module(TOOL)

class Peer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    def handle_error(self, request, client_address): pass

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args): pass
    def reply(self, code, value):
        encoded = json.dumps(value).encode()
        self.send_response(code); self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(encoded))); self.end_headers()
        try: self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError): pass
    def slot(self):
        return int(self.headers['Authorization'].rsplit('-', 1)[1])
    def do_POST(self):
        length = int(self.headers['Content-Length'])
        if length > 16384: return self.reply(413, {})
        event = json.loads(self.rfile.read(length)); slot = self.slot()
        with self.server.lock:
            self.server.ports.add(self.client_address[1]); self.server.lengths.append(length)
            self.server.rows[event['eventId']] = (slot, event)
        if self.server.mode == 'transport': time.sleep(.2)
        if self.server.mode == 'slow': time.sleep(.3)
        event_id = str(uuid.uuid4()) if self.server.mode == 'receipt' else event['eventId']
        tenant = 'foreign' if self.server.mode == 'receipt-tenant' else 'fixture-' + str(slot)
        return self.reply(202, {'eventId': event_id, 'tenantId': tenant,
            'position': {'stream': 'telemetry.events.v1', 'partition': 0, 'offset': 1}})
    def do_GET(self):
        if self.server.mode == 'visible-overload': time.sleep(.1)
        event_id = self.path.rsplit('/', 1)[-1]
        with self.server.lock:
            self.server.ports.add(self.client_address[1]); stored = self.server.rows.get(event_id)
            self.server.gets[event_id] = self.server.gets.get(event_id, 0) + 1
        if self.server.mode == 'missing' or not stored: return self.reply(404, {})
        slot, event = stored
        if slot != self.slot() and self.server.mode != 'leak': return self.reply(404, {})
        result = copy.deepcopy(event)
        if self.server.mode == 'stored': result['attributes']['value'] = 'incorrect'
        if self.server.mode == 'stored-id': result['eventId'] = str(uuid.uuid4())
        if self.server.mode == 'stored-tenant': result['tenantId'] = 'foreign'
        return self.reply(200, result)

class ClientContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.k6 = Path(os.environ['SLO_K6_BINARY']) if 'SLO_K6_BINARY' in os.environ else TOOL.install(ROOT / 'data/slo-tools')
    def run_case(self, mode, vus=4, request_timeout=1000, rate=10, reconcile=False, visibility_budget=150):
        with tempfile.TemporaryDirectory(prefix='slo-client-test-') as temporary:
            folder = Path(temporary)
            peer = Peer(('127.0.0.1', 0), Handler); peer.mode = mode; peer.rows = {}
            peer.ports = set(); peer.lengths = []; peer.gets = {}; peer.lock = threading.Lock()
            thread = threading.Thread(target=peer.serve_forever, daemon=True); thread.start()
            try:
                config = TOOL.profile('reference'); config.update(mode='measure', kind='fixture',
                    rate_per_second=rate, duration_seconds=2, virtual_users=vus, tenants=2,
                    collector_url='http://127.0.0.1:' + str(peer.server_port),
                    worker_url='http://127.0.0.1:' + str(peer.server_port),
                    run_prefix=str(uuid.uuid4())[:24], event_time=datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
                    request_timeout_ms=request_timeout, settle_seconds=5,
                    visibility_p99_ms=visibility_budget, acceptance_p99_ms=1000, acceptance_p999_ms=1500,
                    generator_delay_p99_ms=100)
                TOOL.validate(config)
                config_file = folder/'config.json'; config_file.write_text(json.dumps(config))
                token_file = folder/'tokens.json'; token_file.write_text(json.dumps([
                    {'tenant':'fixture-0','token':'fixture-token-0'}, {'tenant':'fixture-1','token':'fixture-token-1'}]))
                token_file.chmod(0o600)
                env = os.environ.copy(); env.update(SLO_CONFIG=str(config_file), SLO_TOKENS=str(token_file), K6_NO_USAGE_REPORT='true')
                log = folder/'load.log'
                with log.open('w') as output:
                    process = subprocess.run([str(self.k6), 'run', '--quiet', '--no-usage-report',
                        '--log-output=stdout', '--log-format=raw', str(ROOT/'tools/slo/workload.js')],
                        env=env, stdout=output, stderr=subprocess.STDOUT, timeout=30)
                self.assertIn(process.returncode, (0,99), log.read_text()[-4000:])
                summary, records = TOOL.audit(log, config)
                self.assertEqual(len(records), TOOL.value(summary, 'ingest_started'))
                self.assertEqual(len(records), TOOL.value(summary, 'ingest_completed'))
                self.assertEqual(len(records), TOOL.value(summary, 'audit_records'))
                self.assertLessEqual(abs(TOOL.offered(config) - (len(records)+TOOL.value(summary, 'dropped_iterations'))), 1)
                self.assertEqual(len(records), len({r['id'] for r in records}))
                self.assertTrue(all(n==512 for n in peer.lengths))
                self.assertNotIn('fixture-token-', log.read_text())
                if mode == 'missing':
                    self.assertLessEqual(max(peer.gets.values()), 6,
                        'Missing visibility must use bounded exponential polling')
                first = process.returncode, summary, records, len(peer.ports)
                if not reconcile: return first
                manifest = [{key:r[key] for key in ('id','tenant_slot','http_status')} for r in records]
                manifest.sort(key=lambda r:r['http_status'] != 202)
                manifest_file = folder/'manifest.json'; manifest_file.write_text(json.dumps(manifest))
                env['SLO_MANIFEST'] = str(manifest_file)
                config['mode'] = 'reconcile'; config['request_timeout_ms'] = max(1000, request_timeout)
                config_file.write_text(json.dumps(config))
                reconciliation_log = folder/'reconcile.log'
                with reconciliation_log.open('w') as output:
                    reconciliation = subprocess.run([str(self.k6), 'run', '--quiet', '--no-usage-report',
                        '--log-output=stdout', '--log-format=raw', str(ROOT/'tools/slo/workload.js')],
                        env=env, stdout=output, stderr=subprocess.STDOUT, timeout=20)
                checked, queried = TOOL.reconcile_audit(reconciliation_log, manifest)
                self.assertNotIn('fixture-token-', reconciliation_log.read_text())
                return first, (reconciliation.returncode, checked, queried)
            finally:
                peer.shutdown(); peer.server_close(); thread.join(timeout=5)
    def test_valid_receipts_content_and_connection_reuse(self):
        code, summary, records, connections = self.run_case('correct')
        self.assertEqual(0, code)
        self.assertEqual(len(records), TOOL.value(summary,'valid_receipts'))
        self.assertEqual(0, TOOL.value(summary,'contract_failures'))
        self.assertEqual(1, TOOL.value(summary,'isolation_denied'))
        self.assertLess(connections, len(records))
    def test_wrong_receipt_identity_is_a_contract_failure(self):
        code, summary, _, _ = self.run_case('receipt')
        self.assertEqual(99,code); self.assertGreater(TOOL.value(summary,'contract_failures'),0)
    def test_wrong_receipt_tenant_is_a_contract_failure(self):
        code, summary, _, _ = self.run_case('receipt-tenant')
        self.assertEqual(99,code); self.assertGreater(TOOL.value(summary,'contract_failures'),0)
    def test_wrong_stored_content_and_identity_are_rejected(self):
        for mode in ('stored','stored-id','stored-tenant'):
            with self.subTest(mode=mode):
                code, summary, _, _ = self.run_case(mode)
                self.assertEqual(99,code); self.assertGreater(TOOL.value(summary,'contract_failures'),0)
    def test_cross_tenant_visibility_is_rejected(self):
        code, summary, _, _ = self.run_case('leak')
        self.assertEqual(99,code); self.assertGreater(TOOL.value(summary,'isolation_leaks'),0)
    def test_missing_visibility_fails(self):
        code, summary, _, _ = self.run_case('missing')
        self.assertEqual(99,code); self.assertEqual(0,TOOL.value(summary,'live_visibility_good','rate'))
    def test_transport_ambiguity_retains_auditable_identities(self):
        code, summary, records, _ = self.run_case('transport', request_timeout=30)
        self.assertEqual(99,code); self.assertTrue(any(r['http_status']==0 for r in records))
        self.assertLess(TOOL.value(summary,'valid_receipts'), len(records))
    def test_bounded_virtual_users_report_unsent_work(self):
        code, summary, _, _ = self.run_case('slow',vus=1,rate=20)
        self.assertEqual(99,code); self.assertGreater(TOOL.value(summary,'dropped_iterations'),0)

    def test_live_visibility_does_not_inherit_the_nominal_clock_after_dropped_arrivals(self):
        code, summary, rows, _ = self.run_case('visible-overload',vus=1,rate=30,visibility_budget=400)
        self.assertEqual(99,code); self.assertGreater(TOOL.value(summary,'dropped_iterations'),0)
        observed=[r['live_visibility_ms'] for r in rows if r['live_observed']]
        self.assertGreater(len(observed),2)
        self.assertLessEqual(max(observed),400)
        self.assertTrue(any(r['scheduled_to_response_ms']>400 for r in rows))

    def test_reconciliation_checks_every_accepted_identity_and_full_content(self):
        first, (code, summary, rows) = self.run_case('correct', reconcile=True)
        self.assertEqual(0, code)
        self.assertEqual(len(first[2]), len(rows))
        self.assertEqual(len(rows), TOOL.value(summary, 'reconciled_202'))
    def test_reconciliation_cannot_hide_missing_or_wrong_content(self):
        for mode in ('missing', 'stored'):
            with self.subTest(mode=mode):
                _, (code, summary, _) = self.run_case(mode, reconcile=True)
                self.assertEqual(99, code)
                self.assertGreater(TOOL.value(summary, 'missing_202'), 0)
                if mode == 'stored': self.assertGreater(TOOL.value(summary, 'wrong_stored_content'), 0)
    def test_ambiguous_timeout_is_reported_separately_from_lost_acceptance(self):
        _, (code, summary, rows) = self.run_case('transport', request_timeout=30, reconcile=True)
        self.assertEqual(0, code)
        self.assertEqual(0, TOOL.value(summary, 'missing_202'))
        self.assertEqual(len(rows), TOOL.value(summary, 'non202_later_visible'))

if __name__ == '__main__': unittest.main()
