#!/usr/bin/env python3
"""Require the complete repeated matrix and fixed protocol before a green SLO claim."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('slo_tool',ROOT/'scripts/slo-tool.py')
TOOL=importlib.util.module_from_spec(spec);spec.loader.exec_module(TOOL)

def aggregate(root,mode,pilot_profile='reference'):
    if pilot_profile not in ('reference','scale'):raise ValueError('Invalid pilot profile')
    expected={(p,r) for p in ('reference','scale') for r in (1,2,3)}|{('soak',1),('burst',1)} if mode=='full' else {(pilot_profile,1)}
    found={};phases=[];revisions=set();errors=[]
    protocol=hashlib.sha256((ROOT/'tools/slo/workload.js').read_bytes()).hexdigest()
    profiles=hashlib.sha256((ROOT/'tools/slo/profiles.json').read_bytes()).hexdigest()
    for path in Path(root).rglob('case.json'):
        case=json.loads(path.read_text());key=case.get('profile'),case.get('repeat')
        if key in found or key not in expected:raise ValueError('Duplicate or unexpected experiment case')
        found[key]=case
        if case.get('mode')!=mode:raise ValueError('Mixed experiment modes')
        wanted={'baseline','tuned','tuned-profiled'} if key[0] in ('reference','scale') else {'tuned-profiled'}
        if set(case.get('phase_order',[]))!=wanted or len(case['phase_order'])!=len(wanted):raise ValueError('Incomplete phase plan')
        for name in case['phase_order']:
            result=json.loads((path.parent/name/'report.json').read_text())
            if mode=='full':
                config=json.loads((path.parent/name/'config.json').read_text());fixed=TOOL.profile(key[0])
                if any(config.get(k)!=v for k,v in fixed.items()):raise ValueError('Fixed workload profile drift')
                if result.get('nominal_offered')!=TOOL.offered(fixed) or result.get('window_end_ms',0)-result.get('window_start_ms',0)!=fixed['duration_seconds']*1000:raise ValueError('Incomplete fixed measurement window')
            if result.get('profile')!=key[0] or result.get('repeat')!=key[1] or result.get('phase')!=name:raise ValueError('Wrong phase identity')
            if result.get('protocol_sha256')!=protocol or result.get('profiles_sha256')!=profiles:raise ValueError('Measurement protocol drift')
            if bool(result.get('eligible'))!=(mode=='full') or result.get('mode')!=mode:raise ValueError('Invalid experiment eligibility')
            if not result.get('gates') or any(type(v) is not bool for v in result['gates'].values()) or result.get('passed')!=all(result['gates'].values()):raise ValueError('Invalid gate accounting')
            if name=='baseline':
                if result['source_revision']!=TOOL.BASELINE:raise ValueError('Wrong baseline acknowledgement contract')
            else:
                revisions.add(result['source_revision'])
                if not result['passed']:errors.append({'profile':key[0],'repeat':key[1],'phase':name,
                    'failed_gates':[k for k,v in result['gates'].items() if not v]})
            phases.append({'profile':key[0],'repeat':key[1],'phase':name,'passed':result['passed']})
    if set(found)!=expected:raise ValueError('Missing experiment cases')
    if len(revisions)!=1:raise ValueError('Mixed tuned source revisions')
    return dict(schema_version=1,mode=mode,capacity_eligible=mode=='full',source_revision=revisions.pop(),
        baseline=TOOL.BASELINE,protocol_sha256=protocol,profiles_sha256=profiles,cases=len(found),phases=phases,
        passed=not errors and mode=='full',failures=errors,
        interpretation='All fixed workload gates passed' if not errors and mode=='full' else 'Pilot or failed workload; no green SLO claim')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--mode',choices=('pilot','full'),required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--pilot-profile',choices=('reference','scale'),default='reference')
    args=parser.parse_args();result=aggregate(args.root,args.mode,args.pilot_profile)
    args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
    if args.mode=='full' and not result['passed']:raise SystemExit(1)
