#!/usr/bin/env python3
"""Fail closed on invalid workload inputs and incomplete benchmark/reconciliation evidence."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('slo_tool', ROOT/'scripts/slo-tool.py')
TOOL = importlib.util.module_from_spec(spec); spec.loader.exec_module(TOOL)

class ProfileContracts(unittest.TestCase):
    def config(self):
        config = TOOL.profile('reference')
        config.update(mode='measure', duration_seconds=1, rate_per_second=2, tenants=2,
            collector_url='http://127.0.0.1:8080', worker_url='http://localhost:8081',
            run_prefix='12345678-1234-4234-8234-', request_timeout_ms=5000, settle_seconds=600)
        return config
    def write(self, folder, rows, metrics, mode='measure'):
        path = folder/'audit.log'
        text = ''.join('SLO_RECORD '+json.dumps(row)+'\n' for row in rows)
        result = {'schema_version':1,'mode':mode,'metrics':
            {key:{'values':{'count':value}} for key,value in metrics.items()}}
        path.write_text(text+'SLO_SUMMARY '+json.dumps(result)+'\n')
        return path
    def rows(self):
        return [dict(index=i,id=self.config()['run_prefix']+format(i,'012x'),tenant_slot=i%2,
            payload_bytes=512,http_status=202,receipt_valid=True,live_visible=True,live_observed=True,
            contract_failure=False,scenario_start_ms=100,started_ms=100+i*500,http_elapsed_ms=10,
            scheduled_to_response_ms=11,live_visibility_ms=12) for i in range(2)]
    def metrics(self):
        return dict(ingest_started=2,ingest_completed=2,audit_records=2,iterations=2,http_202=2,valid_receipts=2)
    def test_fixed_profiles_and_offer_counts(self):
        self.assertEqual(60000,TOOL.offered(TOOL.profile('reference')))
        self.assertEqual(300000,TOOL.offered(TOOL.profile('scale')))
        self.assertEqual(900000,TOOL.offered(TOOL.profile('soak')))
        self.assertEqual(63000,TOOL.offered(TOOL.profile('burst')))
        self.assertEqual(94,TOOL.profile('burst')['duration_seconds'])
        for name in ('reference','scale','soak','burst'):
            self.assertEqual(250,TOOL.profile(name)['acceptance_p99_ms'])
            self.assertEqual(.999,TOOL.profile(name)['acceptance_fraction'])
        changed=TOOL.profile('scale'); changed['duration_seconds']=1
        self.assertEqual(600,TOOL.profile('scale')['duration_seconds'])
    def test_remote_credential_and_malformed_endpoints_are_rejected(self):
        for url in ('http://example.com','file:///tmp/source','http://user:secret@localhost',
                    'http://localhost/?token=private','http://localhost/#x','http://localhost/path'):
            with self.subTest(url=url):
                config=self.config(); config['collector_url']=url
                with self.assertRaises(ValueError): TOOL.validate(config)
    def test_allocation_and_runtime_limits_are_enforced(self):
        for key,value in [('virtual_users',257),('tenants',33),('payload_bytes',769),
                          ('duration_seconds',1801),('rate_per_second',True),('request_timeout_ms',10001),
                          ('settle_seconds',901),('run_prefix','invalid')]:
            with self.subTest(key=key):
                config=self.config(); config[key]=value
                with self.assertRaises(ValueError): TOOL.validate(config)
    def test_stage_runtime_and_offered_cap_are_enforced(self):
        config=self.config();config['stages']=[{'target':1,'duration_seconds':1800}]
        with self.assertRaises(ValueError): TOOL.validate(config)
        config=self.config();config.update(rate_per_second=5000,duration_seconds=1800)
        with self.assertRaises(ValueError): TOOL.validate(config)
    def test_integral_offer_count_is_required(self):
        config=self.config();config['stages']=[{'target':3,'duration_seconds':1}]
        with self.assertRaises(ValueError): TOOL.offered(config)
    def test_complete_measurement_is_auditable(self):
        with tempfile.TemporaryDirectory() as folder:
            path=self.write(Path(folder),self.rows(),self.metrics())
            _,rows=TOOL.audit(path,self.config());self.assertEqual(2,len(rows))
    def test_timer_boundary_is_bounded_and_incomplete_audit_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            config=self.config();config['rate_per_second']=1
            path=self.write(Path(folder),self.rows(),self.metrics())
            TOOL.audit(path,config)
            for counter in ('ingest_started','ingest_completed','audit_records','iterations','http_202','valid_receipts'):
                with self.subTest(counter=counter):
                    metrics=self.metrics();metrics[counter]=1
                    path=self.write(Path(folder),self.rows(),metrics)
                    with self.assertRaises(ValueError): TOOL.audit(path,self.config())
    def test_duplicate_wrong_tenant_and_wrong_content_accounting_is_rejected(self):
        for field,value in [('index',0),('id','foreign'),('tenant_slot',0),('http_status',201),
                            ('payload_bytes',1),('live_observed',False),('http_elapsed_ms',float('nan'))]:
            with self.subTest(field=field),tempfile.TemporaryDirectory() as folder:
                rows=self.rows();rows[1][field]=value
                path=self.write(Path(folder),rows,self.metrics())
                with self.assertRaises(ValueError): TOOL.audit(path,self.config())
    def test_missing_duplicate_and_oversized_summaries_fail_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'log';path.write_text('')
            with self.assertRaises(ValueError):TOOL.summary(path)
            summary='SLO_SUMMARY '+json.dumps({'schema_version':1,'metrics':{}})+'\n'
            path.write_text(summary*2)
            with self.assertRaises(ValueError):TOOL.summary(path)
            path.write_text('x'*(TOOL.MAX_LOG_LINE+1))
            with self.assertRaises(ValueError):list(TOOL.events(path))
    def test_corrupt_cached_tool_cannot_run_or_be_silently_trusted(self):
        with tempfile.TemporaryDirectory() as folder:
            binary=Path(folder)/'k6';binary.write_bytes(b'corrupt cache')
            with patch.object(TOOL.urllib.request,'urlopen',return_value=io.BytesIO(b'wrong archive')):
                with self.assertRaisesRegex(ValueError,'checksum'):TOOL.install(folder)
    def test_reconciliation_must_match_every_original_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            manifest=[{'id':'one','tenant_slot':0,'http_status':202}]
            path=Path(folder)/'reconcile.log'
            row=dict(index=0,id='one',tenant_slot=0,http_status=202,visible=True,mismatch=False)
            summary={'schema_version':1,'mode':'reconcile','metrics':{key:{'values':{'count':1}}
                for key in ('iterations','reconciliation_records','reconciled_202')}}
            def write():path.write_text('SLO_RECONCILE '+json.dumps(row)+'\nSLO_SUMMARY '+json.dumps(summary)+'\n')
            write();TOOL.reconcile_audit(path,manifest)
            row['tenant_slot']=1;write()
            with self.assertRaises(ValueError):TOOL.reconcile_audit(path,manifest)
            row['tenant_slot']=0;row['visible']=False;write()
            with self.assertRaises(ValueError):TOOL.reconcile_audit(path,manifest)
            path.write_text('SLO_SUMMARY '+json.dumps(summary)+'\n')
            with self.assertRaises(ValueError):TOOL.reconcile_audit(path,manifest)

if __name__ == '__main__':unittest.main()
