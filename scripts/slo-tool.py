#!/usr/bin/env python3
"""Pinned local k6 installation and bounded workload inputs; never prints credentials."""
import argparse
import copy
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import tarfile
import tempfile
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
VERSION = '2.3.0'
TOOL_URL = 'https://github.com/grafana/k6/releases/download/v2.3.0/k6-v2.3.0-linux-amd64.tar.gz'
TOOL_SHA256 = '39c3117b6af817592dcd0ce4242105c0a7af10948c2a425306f0be8f7a8a8ab1'
BINARY_SHA256 = 'e001bbf39ba67424a85c9c8516440ba5c73ecf7a6021be96c76194380c15f260'
BASELINE = '41d7728d47b923e54a2cb8ef8e3eaa7e34b001cf'
MAX_RECORDS = 1_000_000
MAX_LOG_LINE = 65536


def profile(name):
    values = json.loads((ROOT / 'tools/slo/profiles.json').read_text())
    if name not in values or not isinstance(values[name], dict):
        raise ValueError('Unknown SLO profile')
    selected = copy.deepcopy(values[name])
    parent = selected.pop('extends', None)
    if parent:
        if parent not in ('reference', 'scale'):
            raise ValueError('Invalid profile inheritance')
        inherited = copy.deepcopy(values[parent]); inherited.update(selected); selected = inherited
    if 'stages' in selected:
        selected['duration_seconds'] = sum(s['duration_seconds'] for s in selected['stages'])
    selected['profile'] = name
    return selected


def offered(config):
    rate = config['rate_per_second']
    if 'stages' not in config:
        count = rate * config['duration_seconds']
    else:
        count = 0
        for stage in config['stages']:
            count += (rate + stage['target']) * stage['duration_seconds'] / 2
            rate = stage['target']
    if count != int(count) or not 0 < count <= MAX_RECORDS:
        raise ValueError('Require bounded integral offered traffic')
    return int(count)


def validate(config):
    for key, lower, upper in [('rate_per_second', 1, 5000), ('duration_seconds', 1, 1800),
                              ('virtual_users', 1, 256), ('tenants', 1, 32), ('payload_bytes', 350, 768)]:
        if type(config.get(key)) is not int or not lower <= config[key] <= upper:
            raise ValueError('Invalid bounded workload field: ' + key)
    for key in ('collector_url', 'worker_url'):
        value = urllib.parse.urlsplit(config[key])
        if value.scheme not in ('http', 'https') or value.hostname not in ('127.0.0.1', 'localhost', '::1'):
            raise ValueError('Benchmark targets must be local forwarded service endpoints')
        if value.username or value.password or value.query or value.fragment or value.path not in ('', '/'):
            raise ValueError('Invalid benchmark endpoint')
    if not re.fullmatch(r'[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-', config.get('run_prefix', '')):
        raise ValueError('Invalid synthetic identity prefix')
    if 'stages' in config:
        if not 1 <= len(config['stages']) <= 10:
            raise ValueError('Invalid stage count')
        for stage in config['stages']:
            if (type(stage.get('target')) is not int or not 1 <= stage['target'] <= 5000
                    or type(stage.get('duration_seconds')) is not int or not 1 <= stage['duration_seconds'] <= 1800):
                raise ValueError('Invalid stage limit')
    if 'stages' in config and sum(s['duration_seconds'] for s in config['stages']) != config['duration_seconds']:
        raise ValueError('Stage duration must match the bounded scenario duration')
    for key, lower, upper in [('request_timeout_ms', 1, 10000), ('settle_seconds', 1, 900)]:
        if type(config.get(key)) is not int or not lower <= config[key] <= upper:
            raise ValueError('Invalid bounded workload field: ' + key)
    return offered(config)


def install(directory):
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    binary = directory / 'k6'
    if binary.is_file() and hashlib.sha256(binary.read_bytes()).hexdigest() == BINARY_SHA256:
        return binary
    with tempfile.TemporaryDirectory(prefix='slo-tool-') as temporary:
        archive = Path(temporary) / 'k6.tar.gz'
        h = hashlib.sha256(); total = 0
        with urllib.request.urlopen(TOOL_URL, timeout=60) as response, archive.open('wb') as output:
            for chunk in iter(lambda: response.read(1048576), b''):
                total += len(chunk)
                if total > 104857600: raise ValueError('Tool archive exceeds limit')
                h.update(chunk); output.write(chunk)
        if h.hexdigest() != TOOL_SHA256: raise ValueError('k6 release checksum mismatch')
        with tarfile.open(archive) as contents:
            member = contents.getmember('k6-v2.3.0-linux-amd64/k6')
            if not member.isfile() or member.size > 209715200: raise ValueError('Invalid k6 binary member')
            with contents.extractfile(member) as source, binary.open('wb') as output:
                shutil.copyfileobj(source, output)
        binary.chmod(0o755)
        if hashlib.sha256(binary.read_bytes()).hexdigest() != BINARY_SHA256:
            binary.unlink(); raise ValueError('k6 binary checksum mismatch')
    return binary


def events(path):
    rows = 0
    with Path(path).open() as stream:
        while line := stream.readline(MAX_LOG_LINE + 1):
            if len(line) > MAX_LOG_LINE: raise ValueError('Benchmark log line exceeds limit')
            for marker in ('SLO_RECORD ', 'SLO_RECONCILE ', 'SLO_SUMMARY '):
                if line.startswith(marker):
                    value = json.loads(line[len(marker):])
                    if not isinstance(value, dict): raise ValueError('Invalid benchmark event')
                    rows += 1
                    if rows > MAX_RECORDS + 1: raise ValueError('Benchmark audit exceeds row limit')
                    yield marker.strip(), value
                    break


def summary(path):
    found = [value for kind, value in events(path) if kind == 'SLO_SUMMARY']
    if len(found) != 1 or found[0].get('schema_version') != 1 or not isinstance(found[0].get('metrics'), dict):
        raise ValueError('Require one complete k6 summary')
    return found[0]


def value(summary, name, statistic='count', default=0):
    return summary['metrics'].get(name, {}).get('values', {}).get(statistic, default)


def audit(path, config, *, require_schedule=True):
    """Require complete, internally consistent identities before calculating SLOs."""
    result = summary(path)
    rows = [row for kind, row in events(path) if kind == 'SLO_RECORD']
    if not rows or result.get('mode') != 'measure':
        raise ValueError('Require a nonempty measurement audit')
    n = len(rows)
    for counter in ('ingest_started', 'ingest_completed', 'audit_records', 'iterations'):
        if value(result, counter) != n:
            raise ValueError('Incomplete workload audit: ' + counter)
    dropped = value(result, 'dropped_iterations')
    if type(dropped) not in (int, float) or dropped < 0 or dropped != int(dropped):
        raise ValueError('Invalid dropped iteration count')
    identities = set(); indices = set()
    for row in rows:
        index = row.get('index')
        if type(index) is not int or not 0 <= index < MAX_RECORDS:
            raise ValueError('Invalid audit index')
        if index in indices or row.get('id') in identities:
            raise ValueError('Duplicate audit identity')
        indices.add(index); identities.add(row.get('id'))
        if row.get('id') != config['run_prefix'] + format(index, '012x'):
            raise ValueError('Wrong synthetic identity')
        if row.get('tenant_slot') != index % config['tenants'] or row.get('payload_bytes') != config['payload_bytes']:
            raise ValueError('Wrong audit tenant or payload')
        status = row.get('http_status')
        if type(status) is not int or status != 0 and not 100 <= status <= 599:
            raise ValueError('Invalid audit HTTP status')
        for field in ('receipt_valid', 'live_visible', 'live_observed', 'contract_failure'):
            if type(row.get(field)) is not bool:
                raise ValueError('Invalid audit boolean')
        if row['receipt_valid'] and status != 202 or row['live_visible'] and not row['live_observed']:
            raise ValueError('Contradictory audit outcome')
        for field in ('scenario_start_ms', 'started_ms', 'http_elapsed_ms', 'scheduled_to_response_ms'):
            number = row.get(field)
            if type(number) not in (int, float) or not math.isfinite(number) or number < 0:
                raise ValueError('Invalid audit time')
        if row['live_observed']:
            number = row.get('live_visibility_ms')
            if type(number) not in (int, float) or not math.isfinite(number) or number < 0:
                raise ValueError('Invalid live visibility time')
    if indices != set(range(n)):
        raise ValueError('Require one local contiguous iteration sequence')
    for counter, field in [('http_202', 'http_status'), ('valid_receipts', 'receipt_valid')]:
        expected = sum(row[field] == (202 if field == 'http_status' else True) for row in rows)
        if value(result, counter) != expected:
            raise ValueError('Audit counter mismatch: ' + counter)
    if value(result, 'contract_failures') < sum(row['contract_failure'] for row in rows):
        raise ValueError('Audit contract counter mismatch')
    if require_schedule and abs(offered(config) - n - dropped) > 1:
        raise ValueError('Observed offered count exceeds timer boundary allowance')
    rows.sort(key=lambda row: row['index'])
    return result, rows


def reconcile_audit(path, manifest):
    result = summary(path)
    rows = [row for kind, row in events(path) if kind == 'SLO_RECONCILE']
    if result.get('mode') != 'reconcile' or len(rows) != len(manifest):
        raise ValueError('Incomplete reconciliation')
    if value(result, 'reconciliation_records') != len(rows) or value(result, 'iterations') != len(rows):
        raise ValueError('Reconciliation counter mismatch')
    seen = set()
    for row in rows:
        index = row.get('index')
        if type(index) is not int or not 0 <= index < len(manifest) or index in seen:
            raise ValueError('Invalid reconciliation index')
        seen.add(index)
        original = manifest[index]
        if any(row.get(key) != original[key] for key in ('id', 'tenant_slot', 'http_status')):
            raise ValueError('Wrong reconciliation identity')
        if type(row.get('visible')) is not bool or type(row.get('mismatch')) is not bool:
            raise ValueError('Invalid reconciliation outcome')
    for counter, expected in (
        ('reconciled_202', sum(row['http_status'] == 202 and row['visible'] for row in rows)),
        ('missing_202', sum(row['http_status'] == 202 and not row['visible'] for row in rows)),
        ('wrong_stored_content', sum(row['mismatch'] for row in rows)),
        ('non202_later_visible', sum(row['http_status'] != 202 and row['visible'] for row in rows))):
        if value(result, counter) != expected:
            raise ValueError('Reconciliation outcome counter mismatch')
    return result, rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install', type=Path)
    args = parser.parse_args()
    if args.install: print(install(args.install))
    else: parser.error('Provide --install DIRECTORY')
