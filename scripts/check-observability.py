#!/usr/bin/env python3
"""Verify provisioned local Grafana and authenticated Prometheus scrapes without logging secrets."""
import argparse
import base64
import json
from pathlib import Path
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--password-file', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
credential = base64.b64encode(('operator:' + args.password_file.read_text().strip()).encode()).decode()


def read(url, authenticated=False):
    headers = {'Authorization': 'Basic ' + credential} if authenticated else {}
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=2) as response:
        payload = response.read(65537)
        if len(payload) > 65536:
            raise ValueError('Oversized observability response')
        return json.loads(payload)


deadline = time.monotonic() + 90
while True:
    try:
        dashboard = read('http://127.0.0.1:3000/api/dashboards/uid/telemetry-platform', True)
        datasource = read('http://127.0.0.1:3000/api/datasources/uid/telemetry-prometheus', True)
        scrape = read('http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22telemetry%22%7D')
        ready = {item['metric']['component'] for item in scrape['data']['result'] if item['value'][1] == '1'}
        assert dashboard['meta']['provisioned'], 'Dashboard was not provisioned'
        assert len(dashboard['dashboard']['panels']) == 6, 'Expected the six operational panels'
        assert datasource['uid'] == 'telemetry-prometheus', 'Unexpected datasource'
        assert ready == {'collector', 'worker'}, 'Both authenticated scrape targets must be up'
        break
    except urllib.error.HTTPError as error:
        error.close()
        if time.monotonic() >= deadline:
            raise SystemExit('Grafana provisioning did not become available') from None
    except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError, KeyError, AssertionError):
        if time.monotonic() >= deadline:
            raise SystemExit('Observability provisioning or authenticated scraping failed') from None
    time.sleep(1)

args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps({'grafanaDashboardProvisioned': True,
                                  'grafanaDatasourceProvisioned': True,
                                  'dashboardPanels': 6,
                                  'authenticatedScrapeTargetsUp': sorted(ready)}, indent=2) + '\n')
print('PASS provisioned Grafana dashboard and datasource; both authenticated scrape targets are up')
