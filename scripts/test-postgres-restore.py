#!/usr/bin/env python3
"""Restore acknowledged synthetic events to a scratch database and compare complete rows."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
ROOT=Path(__file__).resolve().parents[1]
SQL="SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY tenant_id,event_id),'[]'::jsonb)::text FROM telemetry_events t"
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--project',required=True)
    parser.add_argument('--env-file',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    if not re.fullmatch(r'telemetry-compose-ci-[0-9]+',args.project):parser.error('Only disposable synthetic Compose CI projects are authorized')
    compose=['docker','compose','-f','compose.yaml','-f','compose.replicated.yaml','-p',args.project,'--env-file',str(args.env_file)]
    def command(values,check=True):return subprocess.run(compose+['exec','-T','postgres',*values],cwd=ROOT,text=True,capture_output=True,timeout=60,check=check)
    def rows(database):return json.loads(command(['psql','-X','-A','-t','-U','telemetry','-d',database,'-c',SQL]).stdout)
    before=rows('telemetry')
    if not before:raise ValueError('Require nonempty acknowledged synthetic data')
    restored='telemetry_restore_ci';started=time.monotonic();created=False
    try:
        command(['pg_dump','-U','telemetry','-d','telemetry','--format=custom','--no-owner','--no-privileges','--file=/tmp/telemetry-restore.dump'])
        archive_sha=command(['sha256sum','/tmp/telemetry-restore.dump']).stdout.split()[0]
        if not re.fullmatch(r'[a-f0-9]{64}',archive_sha):raise ValueError('Invalid backup digest')
        command(['createdb','-U','telemetry',restored]);created=True
        command(['pg_restore','-U','telemetry','--exit-on-error','--no-owner','--no-privileges','--dbname='+restored,'/tmp/telemetry-restore.dump'])
        after=rows(restored)
        if before!=after or before!=rows('telemetry'):raise ValueError('Restored content differs or source changed during fixture backup')
        command(['sh','-c','head -c 32 /tmp/telemetry-restore.dump > /tmp/telemetry-corrupt.dump'])
        if command(['pg_restore','--list','/tmp/telemetry-corrupt.dump'],check=False).returncode==0:raise ValueError('Corrupt archive was not rejected')
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(dict(schema_version=1,source_revision=os.environ.get('GITHUB_SHA'),
            observed_at=datetime.now(timezone.utc).isoformat(),synthetic_rows=len(before),complete_row_comparison=True,
            backup_sha256=archive_sha,restored_rows=len(after),corrupt_archive_rejected=True,elapsed_seconds=round(time.monotonic()-started,3),passed=True,
            limitations=['Logical restore on the same disposable instance; no production or cross-region RPO/RTO claim']),indent=2)+'\n')
    finally:
        if created:command(['dropdb','-U','telemetry','--if-exists',restored],check=False)
        command(['rm','-f','/tmp/telemetry-restore.dump','/tmp/telemetry-corrupt.dump'],check=False)
if __name__=='__main__':main()
