#!/usr/bin/env python3
"""Run reproducible SLO experiments on ephemeral local Docker, with no cloud provisioning."""
import argparse
from datetime import datetime,timezone
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.request
import uuid

ROOT=Path(__file__).resolve().parents[1]
def module(name):
    spec=importlib.util.spec_from_file_location(name.replace('-','_'),ROOT/'scripts'/f'{name}.py')
    loaded=importlib.util.module_from_spec(spec);spec.loader.exec_module(loaded);return loaded
TOOL=module('slo-tool');REPORT=module('slo-report');CAPACITY=module('capacity-run')
run=CAPACITY.run

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda:source.read(1048576),b''):h.update(chunk)
    return h.hexdigest()


def private_json(path,value):path.write_text(json.dumps(value));path.chmod(0o600)


def metrics(token,url,path):
    request=urllib.request.Request(url+'/actuator/prometheus',headers={'Authorization':'Bearer '+token})
    with urllib.request.urlopen(request,timeout=3) as response:
        body=response.read(2097153)
        if len(body)>2097152:raise ValueError('Metrics response exceeds the bound')
    lines=[line for line in body.decode().splitlines() if line.startswith(('telemetry_append_phase_','telemetry_spool_','telemetry_dispatch_','telemetry_persistence_batch_'))]
    path.write_text('\n'.join(lines)+'\n')


def client(config,private,output,args,manifest=None):
    TOOL.validate(config);config_file=private/'config.json';private_json(config_file,config)
    command=['docker','run','--rm','--network','host','--cpus','1','--memory','1g',
        '--cpuset-cpus',args.generator_cpu,'--read-only','--cap-drop','ALL','--security-opt','no-new-privileges:true',
        '--user',f'{os.getuid()}:{os.getgid()}',
        '--mount',f'type=bind,src={private},dst=/inputs,readonly',
        '--mount',f'type=bind,src={ROOT / "tools/slo"},dst=/scripts,readonly',
        '-e','SLO_CONFIG=/inputs/config.json','-e','SLO_TOKENS=/inputs/tokens.json',
        '-e','K6_NO_USAGE_REPORT=true']
    if manifest is not None:
        private_json(private/'manifest.json',manifest);command+=['-e','SLO_MANIFEST=/inputs/manifest.json']
    command+=['telemetry-slo:generator','run','--quiet','--no-usage-report','--log-output=stdout','--log-format=raw','/scripts/workload.js']
    duration=config['settle_seconds'] if manifest is not None else config['duration_seconds']
    process=run(command,output=output,timeout=duration+60,check=False)
    if process.returncode not in (0,99):raise RuntimeError('Client runtime failed; retain its bounded audit log')
    return process.returncode


def stop_probe(process,log):
    if process:
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=10)
    if log:log.close()


def jfr(compose,directory,artifact,config,start,end):
    result={}
    profiles=directory/'profiles';run(compose+['cp','collector:/profiles',str(profiles)],timeout=60)
    for role in ('collector','worker'):
        recording=profiles/(role+'.jfr')
        measurement={'started_at_utc':datetime.fromtimestamp(start/1000,timezone.utc).isoformat(),
            'generator':{'scheduled_duration_s':config['duration_seconds']}}
        analyzed=REPORT.CAPACITY.jfr_report(recording,measurement,artifact/(role+'-jfr.json'))
        if not analyzed['cpu_sample_coverage'] or not analyzed['event_counts'].get('jdk.ExecutionSample'):
            raise RuntimeError('JFR did not cover the measured window')
        result[role]={'sha256':digest(recording),'bytes':recording.stat().st_size,'coverage':True}
    return result,profiles


def retained_heap(profiles,artifact,config,end):
    results={}
    for role in ('collector','worker'):
        recording=profiles/(role+'-heap.jfr')
        with tempfile.TemporaryFile(mode='w+') as expanded:
            subprocess.run(['jfr','print','--json','--events','jdk.GCHeapSummary',str(recording)],stdout=expanded,check=True,timeout=120)
            expanded.seek(0)
            results[role]=REPORT.heap_guard(REPORT.CAPACITY.iter_events(expanded),end,config)
        results[role].update(sha256=digest(recording),bytes=recording.stat().st_size)
        CAPACITY.compress(recording,artifact/(role+'-heap.jfr.gz'))
    return results


def drain_warmup(compose,token,directory):
    started=time.monotonic();stable=0
    while time.monotonic()-started<900:
        observed=CAPACITY.operations(token,'http://127.0.0.1:8080')
        samples=REPORT.CAPACITY.read_jsonl(directory/'source-lag.raw.log')
        last=samples[-1] if samples else {}
        fresh=last.get('available') and time.time()*1000-REPORT.millis(last['observed_at'])<10000
        if observed and observed.get('bufferRecords')==0 and observed.get('appendPendingRecords',0)==0 and fresh and last['lag_records']==0:stable+=1
        else:stable=0
        if stable>=2:return {'drain_seconds':round(time.monotonic()-started,3),'zero_lag_samples':stable}
        time.sleep(2)
    raise RuntimeError('Warmup did not drain its durable backlog within the bound')


def phase(args,name,env_file,private):
    directory=args.output/name;directory.mkdir()
    artifact=args.output/'artifact'/name;artifact.mkdir(parents=True)
    config=TOOL.profile(args.profile)
    if args.mode=='pilot':
        if args.profile not in ('reference','scale'):raise ValueError('Pilot requires reference or scale')
        config.update(duration_seconds=30,virtual_users=256)
        config.pop('stages',None)
    config.update(mode='measure',kind='measure',collector_url='http://127.0.0.1:8080',
        worker_url='http://127.0.0.1:8082',request_timeout_ms=5000,settle_seconds=900,
        run_prefix=str(uuid.uuid4())[:24],event_time=datetime.now(timezone.utc).isoformat(timespec='milliseconds'))
    TOOL.validate(config);profiled=name=='tuned-profiled'
    override={'services':{}}
    for dependency in ('kafka','kafka2','kafka3','registry','postgres','jaeger'):
        override['services'][dependency]={'cpuset':args.service_cpus}
    for role in ('collector','worker'):
        options=f'-XX:+UseG1GC -XX:InitialRAMPercentage=40 -XX:MaxRAMPercentage=65 -XX:MaxGCPauseMillis=50 -XX:ActiveProcessorCount={config["role_cpus"]}'
        if profiled:
            options+=' '+CAPACITY.jfr_repository_option(role)
            options+=f' -XX:StartFlightRecording=name=runtime,filename=/profiles/{role}.jfr,settings=/app/profile-safe.jfc,dumponexit=true,maxsize=128m,maxage=60m'
        if args.profile=='soak':
            options+=f' -XX:StartFlightRecording=name=retained,filename=/profiles/{role}-heap.jfr,settings=/gc-safe.jfc,dumponexit=true,maxsize=16m,maxage=60m'
        override['services'][role]={'image':'telemetry-slo:'+('baseline' if name=='baseline' else 'tuned'),
            'cpus':config['role_cpus'],'mem_limit':str(config['role_memory_mib'])+'m','cpuset':args.service_cpus,
            'environment':{'JAVA_TOOL_OPTIONS':options,'TRACING_SAMPLE_RATIO':'0.1','LOGGING_LEVEL_ORG_APACHE_KAFKA':'WARN'},
            'volumes':[str(ROOT/'tools/slo/gc-safe.jfc')+':/gc-safe.jfc:ro']}
    override_file=directory/'override.json';override_file.write_text(json.dumps(override,indent=2)+'\n')
    shutil.copyfile(override_file,artifact/'runtime-override.json')
    project=f'slo-{args.profile}-{args.repeat}-{name}'
    compose=['docker','compose','-f','compose.yaml','-f','compose.replicated.yaml','-f',str(override_file),
        '--env-file',str(env_file),'-p',project]
    credentials=[]
    for slot in range(config['tenants']):
        tenant='slo-'+str(slot)
        token=run(['python3','scripts/demo-token.py','--env-file',str(env_file),'--tenant',tenant,'--console']).stdout.strip()
        credentials.append({'tenant':tenant,'token':token})
    private_json(private/'tokens.json',credentials)
    (artifact/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    monitor=None;probe=None;probe_log=None;stopped=threading.Event()
    try:
        print(f'Starting fresh {args.profile} / {name}, repetition {args.repeat}',flush=True)
        run(compose+['up','-d','--no-build','collector','worker'],output=directory/'startup.log',timeout=420)
        run(['python3','scripts/wait-ready.py'],output=directory/'readiness.log',timeout=240)
        before=CAPACITY.container_states(compose)
        filesystem=run(compose+['exec','-T','collector','stat','-f','-c','%T %S %b %a','/app/data/wal'],timeout=10).stdout.strip()
        probe,probe_log=CAPACITY.start_probe(compose,directory,seconds=3600)
        monitor=threading.Thread(target=CAPACITY.sample,args=(compose,credentials[0]['token'],directory/'resources.jsonl',stopped),daemon=True)
        monitor.start()
        warmup=dict(config,kind='warmup',duration_seconds=60,
            rate_per_second=min(config['rate_per_second'],100),run_prefix=str(uuid.uuid4())[:24])
        warmup.pop('stages',None)
        warmup_status=client(warmup,private,directory/'warmup.log',args)
        warmup_summary,warmup_rows=TOOL.audit(directory/'warmup.log',warmup,require_schedule=False)
        warmup_drain=drain_warmup(compose,credentials[0]['token'],directory)
        warm_manifest=[{key:r[key] for key in ('id','tenant_slot','http_status')} for r in warmup_rows]
        warm_manifest.sort(key=lambda r:r['http_status']!=202)
        warm_reconciliation=client(dict(warmup,mode='reconcile'),private,directory/'warmup-reconcile.log',args,warm_manifest)
        TOOL.reconcile_audit(directory/'warmup-reconcile.log',warm_manifest)
        if warm_reconciliation:raise RuntimeError('Warmup accepted-ID reconciliation failed')
        if TOOL.value(warmup_summary,'contract_failures') or any(r['http_status']==202 and not r['receipt_valid'] for r in warmup_rows):
            raise RuntimeError('Warmup violated identity/content contracts')
        for role,url in [('collector',config['collector_url']),('worker',config['worker_url'])]:
            metrics(credentials[0]['token'],url,artifact/(role+'-histograms-before.prom'))
        measured_status=client(config,private,directory/'load.log',args)
        summary,rows=TOOL.audit(directory/'load.log',config,require_schedule=False)
        manifest=[{key:r[key] for key in ('id','tenant_slot','http_status')} for r in rows]
        manifest.sort(key=lambda r:r['http_status']!=202)
        after=CAPACITY.container_states(compose)
        (artifact/'states-after.json').write_text(json.dumps(after,indent=2)+'\n')
        metric_errors={}
        for role,url in [('collector',config['collector_url']),('worker',config['worker_url'])]:
            try:metrics(credentials[0]['token'],url,artifact/(role+'-histograms-after.prom'))
            except Exception as error:metric_errors[role]=type(error).__name__
        if probe.poll() is not None:raise RuntimeError('Offset observer stopped during measurement')
        probe_log.flush();lag=REPORT.CAPACITY.read_jsonl(directory/'source-lag.raw.log')
        stopped.set();monitor.join(timeout=30)
        if monitor.is_alive():raise RuntimeError('Resource observer did not stop within its bound')
        samples=REPORT.CAPACITY.read_jsonl(directory/'resources.jsonl')
        reconciliation_status=client(dict(config,mode='reconcile'),private,directory/'reconcile.log',args,manifest)
        TOOL.reconcile_audit(directory/'reconcile.log',manifest)
        run(compose+['stop','-t','30','collector','worker'],output=directory/'stop.log',timeout=90)
        start=rows[0]['scenario_start_ms'];end=start+config['duration_seconds']*1000
        profiles=None;jfr_evidence=None;heap=None;profile_error=None
        if profiled:
            try:jfr_evidence,profiles=jfr(compose,directory,artifact,config,start,end)
            except Exception as error:profile_error=type(error).__name__
        if args.profile=='soak':
            try:
                if profiles is None:
                    profiles=directory/'profiles';run(compose+['cp','collector:/profiles',str(profiles)],timeout=60)
                heap=retained_heap(profiles,artifact,config,end)
            except Exception as error:profile_error=type(error).__name__
        result=REPORT.report(config,directory/'load.log',directory/'reconcile.log',samples,lag,after,heap)
        result['gates'].update(complete_stage_observations=not metric_errors,
            profile_coverage=not profiled or profile_error is None and bool(jfr_evidence))
        result['passed']=all(result['gates'].values())
        result.update(phase=name,repeat=args.repeat,mode=args.mode,
            eligible=args.mode=='full',profiled=profiled,source_revision=TOOL.BASELINE if name=='baseline' else args.revision,
            states_before=before,states_after=after,spool_filesystem_stat=filesystem,jfr=jfr_evidence,
            observation_errors=metric_errors,profile_error=profile_error,
            client_exit=measured_status,reconciliation_exit=reconciliation_status,
            warmup=dict(warmup_drain,client_exit=warmup_status,attempted=len(warmup_rows),
                http_202=TOOL.value(warmup_summary,'http_202'),dropped=TOOL.value(warmup_summary,'dropped_iterations')),
            protocol_sha256=digest(ROOT/'tools/slo/workload.js'),profiles_sha256=digest(ROOT/'tools/slo/profiles.json'))
        (artifact/'report.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
        for name_ in ('warmup.log','warmup-reconcile.log','load.log','reconcile.log','resources.jsonl','source-lag.raw.log'):
            CAPACITY.compress(directory/name_,artifact/(name_+'.gz'))
        print(f'Measured gates: {result["passed"]}; failed: {[k for k,v in result["gates"].items() if not v]}',flush=True)
        return result
    finally:
        stopped.set()
        if monitor:monitor.join(timeout=30)
        stop_probe(probe,probe_log)
        if not (artifact/'states-after.json').exists():
            try:
                states=CAPACITY.container_states(compose)
                (artifact/'states-after.json').write_text(json.dumps(states,indent=2)+'\n')
            except Exception as error:
                (artifact/'state-capture-error.json').write_text(json.dumps({'error_type':type(error).__name__})+'\n')
        if profiled and not (artifact/'report.json').exists() and (directory/'warmup.log').is_file():
            try:
                for role,url in [('collector',config['collector_url']),('worker',config['worker_url'])]:
                    metrics(credentials[0]['token'],url,artifact/(role+'-diagnostic-histograms.prom'))
                run(compose+['stop','-t','30','collector','worker'],output=directory/'diagnostic-stop.log',timeout=90)
                diagnostic_summary,diagnostic_rows=TOOL.audit(directory/'warmup.log',warmup)
                diagnostic_start=diagnostic_rows[0]['scenario_start_ms']
                diagnostic_jfr,profiles=jfr(compose,directory,artifact,warmup,diagnostic_start,diagnostic_start+warmup['duration_seconds']*1000)
                for role in ('collector','worker'):CAPACITY.compress(profiles/(role+'.jfr'),artifact/(role+'-diagnostic.jfr.gz'))
                (artifact/'diagnostic.json').write_text(json.dumps(dict(kind='warmup',capacity_eligible=False,jfr=diagnostic_jfr),indent=2)+'\n')
            except Exception as diagnostic_error:
                (artifact/'diagnostic-error.json').write_text(json.dumps({'error_type':type(diagnostic_error).__name__})+'\n')
        run(compose+['logs','--no-color','--tail','60'],output=artifact/'application-tail.log',timeout=30,check=False)
        # On harness errors, preserve only audit/observation logs; never archive private token/env files.
        for name_ in ('warmup.log','warmup-reconcile.log','load.log','reconcile.log','resources.jsonl','source-lag.raw.log'):
            source=directory/name_
            if source.is_file() and not (artifact/(name_+'.gz')).exists():CAPACITY.compress(source,artifact/(name_+'.gz'))
        run(compose+['down','--volumes','--remove-orphans'],output=directory/'cleanup.log',timeout=120,check=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=('pilot','full'),required=True)
    parser.add_argument('--profile',choices=('reference','scale','soak','burst'),required=True)
    parser.add_argument('--repeat',type=int,choices=(1,2,3),required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
    artifact=args.output/'artifact';artifact.mkdir()
    args.revision=run(['git','rev-parse','HEAD']).stdout.strip()
    if run(['git','status','--porcelain']).stdout.strip():raise RuntimeError('Require a clean tested source tree')
    cpus=sorted(os.sched_getaffinity(0))
    if len(cpus)<4:raise RuntimeError('Require at least four CPUs for a separated generator CPU')
    args.generator_cpu=str(cpus[-1]);args.service_cpus=','.join(map(str,cpus[:-1]))
    metadata=dict(schema_version=1,mode=args.mode,profile=args.profile,repeat=args.repeat,
        revision=args.revision,tree=run(['git','rev-parse','HEAD^{tree}']).stdout.strip(),baseline=TOOL.BASELINE,
        started_at=datetime.now(timezone.utc).isoformat(),
        workflow_url=os.environ.get('GITHUB_SERVER_URL','https://github.com')+'/'+os.environ.get('GITHUB_REPOSITORY','GeekNandy/real-time-telemetry-data-platform')+'/actions/runs/'+os.environ.get('GITHUB_RUN_ID','local'),
        cpu=run(['lscpu','-J']).stdout,docker=run(['docker','version','--format','{{json .Server.Version}}']).stdout.strip(),
        runner=run(['docker','info','--format','{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"storage_driver":{{json .Driver}}}']).stdout.strip(),
        generator_cpu=args.generator_cpu,service_cpus=args.service_cpus,k6_version=TOOL.VERSION,k6_archive_sha256=TOOL.TOOL_SHA256,k6_binary_sha256=TOOL.BINARY_SHA256,
        limitations=['Shared standard runner and storage; unknown disk IOPS and noisy-neighbor effects',
            'Synthetic eight-tenant, 512-byte workload only; no production ceiling or monthly SLA claim',
            'Three data brokers share one controller and machine; no multi-AZ/controller-quorum availability claim',
            'Local demo JWT mode; production OIDC/TLS is exercised in separate security contracts'])
    (artifact/'environment.json').write_text(json.dumps(metadata,indent=2,sort_keys=True)+'\n')
    baseline=args.output/'baseline-source';baseline.mkdir();archive=args.output/'baseline.tar'
    run(['git','archive','--format=tar','--output',str(archive),TOOL.BASELINE])
    with tarfile.open(archive) as source:source.extractall(baseline,filter='data')
    for label,source in [('baseline',baseline),('tuned',ROOT)]:
        run(['docker','build','-t','telemetry-slo:'+label,str(source)],output=args.output/(label+'-build.log'),timeout=1200)
    k6=TOOL.install(args.output/'generator');shutil.copyfile(ROOT/'tools/slo/runner.Dockerfile',k6.parent/'Dockerfile')
    run(['docker','build','-t','telemetry-slo:generator',str(k6.parent)],output=args.output/'generator-build.log',timeout=60)
    metadata['images']={label:run(['docker','image','inspect','--format','{{.Id}}','telemetry-slo:'+label]).stdout.strip() for label in ('baseline','tuned','generator')}
    (artifact/'environment.json').write_text(json.dumps(metadata,indent=2,sort_keys=True)+'\n')
    phases=['baseline','tuned','tuned-profiled'] if args.profile in ('reference','scale') else ['tuned-profiled']
    if args.repeat%2==0:phases.reverse()
    if args.mode=='pilot':phases=['tuned-profiled','tuned','baseline']
    results=[]
    with tempfile.TemporaryDirectory(prefix='telemetry-slo-') as temporary:
        private=Path(temporary);env_file=private/'env'
        env_file.write_text('DEMO_JWT_SECRET='+secrets.token_hex(32)+'\nDATABASE_PASSWORD='+secrets.token_hex(24)+'\n');env_file.chmod(0o600)
        for name in phases:results.append(phase(args,name,env_file,private))
    case=dict(schema_version=1,mode=args.mode,profile=args.profile,repeat=args.repeat,phase_order=phases,
        tuned_gates_passed=all(r['passed'] for r in results if r['phase']!='baseline'),
        results=[{'phase':r['phase'],'passed':r['passed']} for r in results])
    (artifact/'case.json').write_text(json.dumps(case,indent=2)+'\n')
    print('Experiment complete. Failed SLOs are retained as measurements; the aggregate release gate decides eligibility.',flush=True)

if __name__=='__main__':main()
