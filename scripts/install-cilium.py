#!/usr/bin/env python3
"""Install a checksum-pinned enforcing CNI into an explicitly isolated local kind context."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import tempfile
import urllib.request
VERSION='1.20.4'
SHA256='505abff8385811df1a949776fc42e9f10697989a332acc86d6e8933efcb855ed'
URL='https://helm.cilium.io/cilium-1.20.4.tgz'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--context',required=True);args=parser.parse_args()
    if not args.context.startswith('kind-telemetry-ci-'):parser.error('Only isolated telemetry CI kind contexts are authorized')
    with tempfile.TemporaryDirectory(prefix='cilium-chart-') as temporary:
        chart=Path(temporary)/'cilium.tgz'
        with urllib.request.urlopen(URL,timeout=60) as response:archive=response.read(16777217)
        if len(archive)>16777216 or hashlib.sha256(archive).hexdigest()!=SHA256:raise ValueError('Cilium chart checksum mismatch')
        chart.write_bytes(archive)
        subprocess.run(['helm','upgrade','--install','cilium',str(chart),'--kube-context',args.context,'--namespace','kube-system',
            '--set','ipam.mode=kubernetes','--set','kubeProxyReplacement=false','--set','operator.replicas=1',
            '--set','envoy.enabled=false','--set','hubble.enabled=false','--wait','--timeout','300s'],check=True,timeout=330)
        subprocess.run(['kubectl','--context',args.context,'-n','kube-system','rollout','status','daemonset/cilium','--timeout=180s'],check=True,timeout=200)
if __name__=='__main__':main()
