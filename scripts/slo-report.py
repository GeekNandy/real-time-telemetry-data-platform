#!/usr/bin/env python3
"""Calculate release gates from complete raw audits, live offsets and bounded observations."""
import argparse
from datetime import datetime
import importlib.util
import json
import math
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
def module(name):
    spec=importlib.util.spec_from_file_location(name.replace('-','_'),ROOT/'scripts'/f'{name}.py')
    loaded=importlib.util.module_from_spec(spec);spec.loader.exec_module(loaded);return loaded
TOOL=module('slo-tool')
CAPACITY=module('capacity-report')


def quantiles(values):
    ordered=sorted(values)
    return {'count':len(ordered),**{f'p{p}':ordered[math.ceil(Fraction(str(p))*len(ordered)/100)-1] if ordered else None
        for p in (50,95,99,99.9)},'max':ordered[-1] if ordered else None}


def millis(value):return datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()*1000


def coverage(times,start,end,interval):
    times=sorted(t for t in times if start<=t<=end)
    expected=max(1,(end-start)/interval)
    return bool(times) and len(times)>=.9*expected and times[0]<=start+interval*2 and times[-1]>=end-interval*2


def resources(samples,start,end,config):
    result={}
    for role in ('collector','worker'):
        times=[];cpu=[];memory=[]
        for sample in samples:
            observed=millis(sample['observed_at'])
            if not start<=observed<=end:continue
            for container in sample.get('containers') or []:
                if container['Name'].rsplit('-',2)[-2]!=role:continue
                times.append(observed)
                cpu.append(float(container['CPUPerc'].rstrip('%'))/(config['role_cpus']*100))
                memory.append(float(container['MemPerc'].rstrip('%'))/100)
        result[role]={'coverage':coverage(times,start,end,5000),'cpu_fraction_of_quota':quantiles(cpu),
            'working_memory_fraction_of_limit':quantiles(memory)}
    return result


def heap_guard(events,end,config):
    start=end-config['retained_heap_growth_window_seconds']*1000
    points=[]
    for event in events:
        if event['type']!='jdk.GCHeapSummary':continue
        v=event['values'];at=millis(v['startTime'])
        if start<=at<=end and v.get('when')=='After GC':points.append((at/1000,float(v['heapUsed'])))
    points.sort()
    if len(points)<3 or points[-1][0]-points[0][0]<config['retained_heap_growth_window_seconds']*.5:
        return {'passed':False,'reason':'Insufficient after-GC coverage','points':len(points)}
    xs=[x-points[0][0] for x,_ in points];ys=[y for _,y in points]
    mean_x=sum(xs)/len(xs);mean_y=sum(ys)/len(ys)
    denominator=sum((x-mean_x)**2 for x in xs)
    slope=sum((x-mean_x)*(y-mean_y) for x,y in zip(xs,ys))/denominator*60/1048576
    return {'passed':slope<=config['retained_heap_growth_mib_per_minute'],
        'after_gc_growth_mib_per_minute':slope,'points':len(points),
        'window_span_seconds':points[-1][0]-points[0][0],
        'interpretation':'OLS retained-heap regression guard, not proof of absence of leaks'}


def report(config,load_log,reconcile_log,samples,lag,states,heap=None):
    # A scheduling discrepancy fails a release gate, but cannot discard the receipt identities
    # needed to reconcile durable work. Counter/identity/content integrity remains mandatory.
    summary,rows=TOOL.audit(load_log,config,require_schedule=False)
    manifest=[{key:r[key] for key in ('id','tenant_slot','http_status')} for r in rows]
    manifest.sort(key=lambda r:r['http_status']!=202)
    checked,reconciled=TOOL.reconcile_audit(reconcile_log,manifest)
    starts={r['scenario_start_ms'] for r in rows}
    if len(starts)!=1:raise ValueError('Require one local measurement scenario')
    start=starts.pop();end=start+config['duration_seconds']*1000
    dropped=TOOL.value(summary,'dropped_iterations');offered=len(rows)+dropped
    schedule_valid=abs(TOOL.offered(config)-offered)<=1
    accepted=sum(r['http_status']==202 for r in rows);valid=sum(r['receipt_valid'] for r in rows)
    visible=[r['live_visibility_ms'] for r in rows if r['live_observed']]
    live_good=sum(r['live_visible'] for r in rows)
    http=quantiles([r['http_elapsed_ms'] for r in rows]);scheduled=quantiles([r['scheduled_to_response_ms'] for r in rows])
    delay=quantiles([max(0,r['started_ms']-r['scenario_start_ms']-r['index']*1000/config['rate_per_second']) for r in rows])
    resource=resources(samples,start,end,config)
    source=[p for p in lag if p.get('available') and start<=millis(p['observed_at'])<=end]
    source_coverage=coverage([millis(p['observed_at']) for p in source],start,end,1000)
    source_lag=quantiles([p['lag_records'] for p in source])
    gates={
        'complete_raw_audit':True,
        'scheduled_offers_accounted':schedule_valid,
        'zero_contract_failures':TOOL.value(summary,'contract_failures')==0 and not any(r['contract_failure'] for r in rows),
        'all_202_reconciled':accepted>0 and TOOL.value(checked,'missing_202')==0 and TOOL.value(checked,'reconciled_202')==accepted,
        'zero_wrong_stored_content':TOOL.value(checked,'wrong_stored_content')==0,
        'cross_tenant_denied':TOOL.value(summary,'isolation_checks')>0 and TOOL.value(summary,'isolation_checks')==TOOL.value(summary,'isolation_denied') and TOOL.value(summary,'isolation_leaks')==0,
        'resources_observed':all(r['coverage'] for r in resource.values()),
        'source_offsets_observed':source_coverage,
        'fixed_role_limits':all(any(s['service']==role and s.get('nano_cpus')==config['role_cpus']*1_000_000_000 and s.get('memory_limit_bytes')==config['role_memory_mib']*1048576 for s in states) for role in ('collector','worker')),
        'no_oom_or_restart':all(s['running'] and not s['oom_killed'] and s['restart_count']==0 for s in states) and {'collector','worker'}.issubset({s['service'] for s in states}),
        'bounded_memory':all(r['working_memory_fraction_of_limit']['max'] is not None and r['working_memory_fraction_of_limit']['max']<config['memory_fraction'] for r in resource.values()),
    }
    if 'stages' in config:
        tail=[r for r in rows if end-10000<=r['started_ms']<end]
        tail_lag=[p['lag_records'] for p in source if millis(p['observed_at'])>=end-10000]
        gates.update(
            allowed_overload_outcomes=all(r['http_status'] in config['allowed_http_statuses'] for r in rows),
            bounded_response_p99=http['p99']<=config['response_p99_ms'],
            bounded_backlog=source_lag['max'] is not None and source_lag['max']<=max(s['target'] for s in config['stages'])*config['recovery_seconds'],
            healthy_acceptance_recovered=bool(tail) and sum(r['receipt_valid'] for r in tail)/(config['rate_per_second']*10)>=config['acceptance_fraction'] and quantiles([r['http_elapsed_ms'] for r in tail])['p99']<=config['acceptance_p99_ms'],
            live_visibility_recovered=bool(tail) and sum(r['live_visible'] for r in tail)/max(1,sum(r['http_status']==202 for r in tail))>=config['visibility_fraction'],
            source_backlog_recovered=len(tail_lag)>=8 and max(tail_lag)<=config['rate_per_second']*config['lag_seconds'])
    else:
        gates.update(
            zero_generator_drops=dropped==0,
            valid_acceptance_fraction=valid/offered>=config['acceptance_fraction'],
            acceptance_p99=dropped==0 and scheduled['p99']<=config['acceptance_p99_ms'],
            acceptance_p999=dropped==0 and scheduled['p99.9']<=config['acceptance_p999_ms'],
            generator_delay_p99=dropped==0 and delay['p99']<=config['generator_delay_p99_ms'],
            live_visibility_p99=bool(visible) and quantiles(visible)['p99']<=config['visibility_p99_ms'],
            live_visibility_fraction=accepted>0 and live_good/accepted>=config['visibility_fraction'],
            bounded_source_lag=source_lag['max'] is not None and source_lag['max']<=config['rate_per_second']*config['lag_seconds'],
            cpu_headroom=all(r['cpu_fraction_of_quota']['p95'] is not None and r['cpu_fraction_of_quota']['p95']<config['cpu_p95_fraction'] for r in resource.values()))
    if config['profile']=='soak':gates['retained_heap_guard']=bool(heap) and all(v['passed'] for v in heap.values()) and set(heap)=={'collector','worker'}
    outcomes={}
    for row in rows:outcomes[str(row['http_status'])]=outcomes.get(str(row['http_status']),0)+1
    return dict(schema_version=1,profile=config['profile'],window_start_ms=start,window_end_ms=end,
        nominal_offered=TOOL.offered(config),observed_offered=offered,attempted=len(rows),dropped=dropped,
        http_outcomes=outcomes,http_202=accepted,valid_receipts=valid,acceptance_fraction=valid/offered,
        live_visibility_fraction=live_good/accepted if accepted else None,http_latency_ms=http,
        scheduled_latency_ms=scheduled if schedule_valid and not dropped and 'stages' not in config else None,
        generator_delay_ms=delay if schedule_valid and not dropped and 'stages' not in config else None,
        live_visibility_ms=quantiles(visible),resources=resource,source_lag_records=source_lag,
        reconciliation={key:TOOL.value(checked,key) for key in ('reconciled_202','missing_202','wrong_stored_content','non202_later_visible')},
        heap=heap,gates=gates,passed=all(gates.values()),
        latency_convention='Nearest-rank quantiles from raw attempts; live visibility starts at the actual request; scheduled time is invalid after drops, ramps or unaccounted offers')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('reports',nargs='+',type=Path)
    args=parser.parse_args();reports=[json.loads(p.read_text()) for p in args.reports]
    print(json.dumps({'reports':len(reports),'all_passed':all(r['passed'] for r in reports)},indent=2))
