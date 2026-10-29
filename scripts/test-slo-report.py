#!/usr/bin/env python3
"""Exercise failed SLOs, missing observations, retained heap and incomplete matrix evidence."""
import copy
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
def module(name):
    spec=importlib.util.spec_from_file_location(name.replace('-','_'),ROOT/'scripts'/f'{name}.py')
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
REPORT=module('slo-report');RELEASE=module('slo-release');TOOL=REPORT.TOOL
RUN=module('slo-run')

class SloReports(unittest.TestCase):
    def test_warmup_drain_waits_for_unwritten_intents_even_when_disk_and_source_lag_are_zero(self):
        clock=[0]
        def sleep(seconds):clock[0]+=seconds
        states=[dict(bufferRecords=0,appendPendingRecords=n) for n in (2,1,0,0)]
        with patch.object(RUN.CAPACITY,'operations',side_effect=states) as observed, \
             patch.object(RUN.REPORT.CAPACITY,'read_jsonl',return_value=[dict(available=True,lag_records=0,observed_at='1970-01-01T00:16:40+00:00')]), \
             patch.object(RUN.time,'monotonic',side_effect=lambda:clock[0]), \
             patch.object(RUN.time,'time',side_effect=lambda:1000+clock[0]), \
             patch.object(RUN.time,'sleep',side_effect=sleep):
            result=RUN.drain_warmup(['synthetic'],'synthetic-token',Path('/unused'))
        self.assertEqual(4,observed.call_count)
        self.assertEqual(6,result['drain_seconds'])
        self.assertEqual(2,result['zero_lag_samples'])

    def fixture(self,folder):
        config=TOOL.profile('reference');config.update(duration_seconds=10,rate_per_second=10,
            run_prefix='12345678-1234-4234-8234-',tenants=2,payload_bytes=512)
        start=1_700_000_000_000
        rows=[dict(index=i,id=config['run_prefix']+format(i,'012x'),tenant_slot=i%2,http_status=202,
            receipt_valid=True,live_visible=True,live_observed=True,contract_failure=False,payload_bytes=512,
            scenario_start_ms=start,started_ms=start+i*100,http_elapsed_ms=10,scheduled_to_response_ms=10,live_visibility_ms=20)
            for i in range(100)]
        summary={'schema_version':1,'mode':'measure','metrics':{key:{'values':{'count':value}} for key,value in
            dict(ingest_started=100,ingest_completed=100,audit_records=100,iterations=100,http_202=100,
                 valid_receipts=100,isolation_checks=1,isolation_denied=1).items()}}
        load=folder/'load.log';recon=folder/'reconcile.log'
        queries=[{key:r[key] for key in ('index','id','tenant_slot','http_status')}|dict(visible=True,mismatch=False) for r in rows]
        checked={'schema_version':1,'mode':'reconcile','metrics':{key:{'values':{'count':100}}
            for key in ('iterations','reconciliation_records','reconciled_202')}}
        samples=[dict(observed_at=datetime.fromtimestamp((start+i*1000)/1000,timezone.utc).isoformat(),
            containers=[dict(Name=f'slo-reference-1-tuned-{role}-1',CPUPerc='20%',MemPerc='30%') for role in ('collector','worker')]) for i in range(10)]
        lag=[dict(observed_at=s['observed_at'],available=True,lag_records=1) for s in samples]
        states=[dict(service=role,running=True,oom_killed=False,restart_count=0,nano_cpus=1_000_000_000,memory_limit_bytes=768*1048576) for role in ('collector','worker')]
        def write():
            load.write_text(''.join('SLO_RECORD '+json.dumps(r)+'\n' for r in rows)+'SLO_SUMMARY '+json.dumps(summary)+'\n')
            recon.write_text(''.join('SLO_RECONCILE '+json.dumps(r)+'\n' for r in queries)+'SLO_SUMMARY '+json.dumps(checked)+'\n')
        write()
        return config,load,recon,samples,lag,states,rows,summary,queries,checked,write
    def test_every_gate_passes_only_with_complete_healthy_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            f=self.fixture(Path(folder));r=REPORT.report(*f[:6]);self.assertTrue(r['passed'],r['gates'])
            self.assertEqual(10,r['scheduled_latency_ms']['p99']);self.assertEqual(100,r['http_202'])
    def test_latency_and_success_fail_without_hiding_durability(self):
        with tempfile.TemporaryDirectory() as folder:
            f=self.fixture(Path(folder));f[6][0]['scheduled_to_response_ms']=1000;f[6][1]['scheduled_to_response_ms']=1000;f[10]()
            r=REPORT.report(*f[:6]);self.assertFalse(r['gates']['acceptance_p99']);self.assertTrue(r['gates']['all_202_reconciled'])
            f[7]['metrics']['dropped_iterations']={'values':{'count':1}};f[10]()
            r=REPORT.report(*f[:6]);self.assertFalse(r['gates']['valid_acceptance_fraction']);self.assertIsNone(r['scheduled_latency_ms'])
    def test_missing_observers_and_lag_must_fail(self):
        with tempfile.TemporaryDirectory() as folder:
            f=self.fixture(Path(folder));r=REPORT.report(*f[:3],[],[],f[5])
            self.assertFalse(r['gates']['resources_observed']);self.assertFalse(r['gates']['source_offsets_observed'])
            f[4][0]['lag_records']=1000;r=REPORT.report(*f[:6]);self.assertFalse(r['gates']['bounded_source_lag'])
    def test_invalid_schedule_fails_without_discarding_receipt_reconciliation(self):
        with tempfile.TemporaryDirectory() as folder:
            f=self.fixture(Path(folder));f[7]['metrics']['dropped_iterations']={'values':{'count':3}};f[10]()
            with self.assertRaises(ValueError):TOOL.audit(f[1],f[0])
            r=REPORT.report(*f[:6])
            self.assertFalse(r['passed']);self.assertFalse(r['gates']['scheduled_offers_accounted'])
            self.assertTrue(r['gates']['all_202_reconciled']);self.assertEqual(100,r['reconciliation']['reconciled_202'])
            self.assertEqual(103,r['observed_offered']);self.assertIsNone(r['scheduled_latency_ms'])
            f[7]['metrics']['audit_records']['values']['count']=99;f[10]()
            with self.assertRaises(ValueError):TOOL.audit(f[1],f[0],require_schedule=False)
    def test_memory_cpu_oom_and_restart_are_independent_gates(self):
        with tempfile.TemporaryDirectory() as folder:
            f=self.fixture(Path(folder))
            for s in f[3]:s['containers'][0]['CPUPerc']='95%';s['containers'][1]['MemPerc']='85%'
            f[5][0]['restart_count']=1
            r=REPORT.report(*f[:6])
            for key in ('cpu_headroom','bounded_memory','no_oom_or_restart'):self.assertFalse(r['gates'][key])
    def test_lost_202_cannot_pass_even_with_fast_latency(self):
        with tempfile.TemporaryDirectory() as folder:
            f=self.fixture(Path(folder));f[8][0]['visible']=False
            f[9]['metrics']['reconciled_202']['values']['count']=99;f[9]['metrics']['missing_202']={'values':{'count':1}};f[10]()
            r=REPORT.report(*f[:6]);self.assertFalse(r['gates']['all_202_reconciled']);self.assertTrue(r['gates']['acceptance_p99'])
    def test_quantile_uses_nearest_rank_without_interpolation(self):
        self.assertEqual(999,REPORT.quantiles(range(1,1001))['p99.9'])
    def test_retained_heap_growth_and_sparse_coverage(self):
        config=TOOL.profile('soak');end=1_700_000_000_000
        def events(growth):
            return [dict(type='jdk.GCHeapSummary',values=dict(startTime=datetime.fromtimestamp((end-600000+i*60000)/1000,timezone.utc).isoformat(),
                when='After GC',heapUsed=20*1048576+i*growth*1048576)) for i in range(11)]
        self.assertTrue(REPORT.heap_guard(events(.25),end,config)['passed'])
        self.assertFalse(REPORT.heap_guard(events(2),end,config)['passed'])
        self.assertFalse(REPORT.heap_guard(events(0)[:2],end,config)['passed'])
    def matrix(self,root):
        protocol=hashlib.sha256((ROOT/'tools/slo/workload.js').read_bytes()).hexdigest()
        profiles=hashlib.sha256((ROOT/'tools/slo/profiles.json').read_bytes()).hexdigest()
        for p,r in [(p,r) for p in ('reference','scale') for r in (1,2,3)]+[('soak',1),('burst',1)]:
            folder=root/f'{p}-{r}';folder.mkdir();phases=['baseline','tuned','tuned-profiled'] if p in ('reference','scale') else ['tuned-profiled']
            (folder/'case.json').write_text(json.dumps(dict(mode='full',profile=p,repeat=r,phase_order=phases)))
            for phase in phases:
                target=folder/phase;target.mkdir()
                (target/'config.json').write_text(json.dumps(TOOL.profile(p)))
                (target/'report.json').write_text(json.dumps(dict(profile=p,repeat=r,phase=phase,mode='full',eligible=True,nominal_offered=TOOL.offered(TOOL.profile(p)),window_start_ms=0,window_end_ms=TOOL.profile(p)['duration_seconds']*1000,
                    protocol_sha256=protocol,profiles_sha256=profiles,gates={'fixture_gate':True},passed=True,
                    source_revision=TOOL.BASELINE if phase=='baseline' else 'a'*40)))
    def test_release_requires_all_cases_repetitions_phases_and_pinned_protocol(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.matrix(root)
            self.assertTrue(RELEASE.aggregate(root,'full')['passed'])
            path=root/'scale-2/tuned/report.json';original=json.loads(path.read_text())
            failed=copy.deepcopy(original);failed.update(gates={'fixture_gate':False},passed=False);path.write_text(json.dumps(failed))
            self.assertFalse(RELEASE.aggregate(root,'full')['passed'])
            drift=copy.deepcopy(original);drift['protocol_sha256']='different';path.write_text(json.dumps(drift))
            with self.assertRaises(ValueError):RELEASE.aggregate(root,'full')
            path.write_text(json.dumps(original));(root/'soak-1/case.json').unlink()
            with self.assertRaises(ValueError):RELEASE.aggregate(root,'full')
    def test_scale_pilot_cannot_be_released_or_substituted_for_the_complete_matrix(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.matrix(root)
            # A separate root retains exactly one explicit pilot case and its actual phase files.
            import shutil
            pilot=root/'pilot';pilot.mkdir();shutil.move(str(root/'scale-1'),str(pilot/'scale-1'))
            for path in (pilot/'scale-1').rglob('*.json'):
                value=json.loads(path.read_text())
                if path.name=='case.json':value['mode']='pilot'
                if path.name=='report.json':value.update(mode='pilot',eligible=False)
                path.write_text(json.dumps(value))
            result=RELEASE.aggregate(pilot,'pilot','scale')
            self.assertFalse(result['passed']);self.assertFalse(result['capacity_eligible'])
            with self.assertRaises(ValueError):RELEASE.aggregate(pilot,'pilot','reference')
            with self.assertRaises(ValueError):RELEASE.aggregate(pilot,'full','scale')

if __name__=='__main__':unittest.main()
