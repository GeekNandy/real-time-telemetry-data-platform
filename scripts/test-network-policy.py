#!/usr/bin/env python3
"""Prove policy enforcement with independently healthy allowed paths and denied peers."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[1]
def command(arguments,**kwargs):return subprocess.run(arguments,cwd=ROOT,text=True,capture_output=True,timeout=120,check=True,**kwargs)
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--context',required=True)
    parser.add_argument('--cluster',required=True);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args()
    if args.context!='kind-'+args.cluster or not args.cluster.startswith('telemetry-ci-'):parser.error('Only isolated telemetry kind contexts are authorized')
    args.output.mkdir(parents=True,exist_ok=True);kube=['kubectl','--context',args.context]
    with tempfile.TemporaryDirectory(prefix='network-probe-') as temporary:
        folder=Path(temporary)
        command(['javac','--release','21','-Xlint:all','-Werror','-d',str(folder),'tools/network-probe/src/NetworkProbe.java'])
        (folder/'Dockerfile').write_text('FROM real-time-telemetry-data-platform:ci\nCOPY NetworkProbe.class /probe/NetworkProbe.class\nUSER 10001:10001\n')
        command(['docker','build','-t','telemetry-network-probe:ci',str(folder)])
        command(['kind','load','docker-image','--name',args.cluster,'telemetry-network-probe:ci'])
    def pod(name,namespace,labels,mode,target,expected):
        return dict(apiVersion='v1',kind='Pod',metadata=dict(name=name,namespace=namespace,labels=labels),
            spec=dict(automountServiceAccountToken=False,restartPolicy='Never',activeDeadlineSeconds=90,
                securityContext=dict(runAsNonRoot=True,runAsUser=10001,runAsGroup=10001,seccompProfile={'type':'RuntimeDefault'}),
                containers=[dict(name='probe',image='telemetry-network-probe:ci',imagePullPolicy='Never',
                    command=['java','-Xmx32m','-XX:ActiveProcessorCount=1','-cp','/probe','NetworkProbe',mode,target,expected],
                    resources=dict(requests={'cpu':'50m','memory':'64Mi'},limits={'cpu':'500m','memory':'128Mi'}),
                    securityContext=dict(allowPrivilegeEscalation=False,readOnlyRootFilesystem=True,capabilities={'drop':['ALL']}),
                    volumeMounts=[dict(name='tmp',mountPath='/tmp')])],volumes=[dict(name='tmp',emptyDir={'sizeLimit':'16Mi'})]))
    server=pod('network-denied-target','telemetry-client',{'app':'network-denied-target'},'serve','8080','unused')
    server['spec']['containers'][0]['readinessProbe']={'tcpSocket':{'port':8080},'periodSeconds':1}
    service=dict(apiVersion='v1',kind='Service',metadata=dict(name='network-denied-target',namespace='telemetry-client'),
        spec=dict(selector={'app':'network-denied-target'},ports=[{'port':8080}]))
    application={'app.kubernetes.io/instance':'ci','app.kubernetes.io/name':'telemetry','app.kubernetes.io/component':'network-probe'}
    cases=[('ingress-allowed','telemetry-client',{'access':'allowed'},'http','http://ci-telemetry-collector.telemetry:8080/actuator/health','allow'),
           ('ingress-wrong-label','telemetry-client',{'access':'denied'},'http','http://ci-telemetry-collector.telemetry:8080/actuator/health','deny'),
           ('ingress-wrong-namespace','telemetry-deps',{'access':'allowed'},'http','http://ci-telemetry-collector.telemetry:8080/actuator/health','deny'),
           ('egress-target-healthy','telemetry-client',{'access':'allowed'},'tcp','network-denied-target.telemetry-client:8080','allow'),
           ('egress-dependency-allowed','telemetry',application,'tcp','postgres.telemetry-deps:5432','allow'),
           ('egress-other-namespace-denied','telemetry',application,'tcp','network-denied-target.telemetry-client:8080','deny')]
    results=[]
    try:
        for resource in (server,service):command(kube+['apply','-f','-'],input=json.dumps(resource))
        command(kube+['-n','telemetry-client','wait','--for=condition=Ready','pod/network-denied-target','--timeout=60s'])
        for name,namespace,labels,mode,target,expected in cases:
            command(kube+['apply','-f','-'],input=json.dumps(pod(name,namespace,labels,mode,target,expected)))
            try:command(kube+['-n',namespace,'wait','--for=jsonpath={.status.phase}=Succeeded','pod/'+name,'--timeout=90s'])
            finally:(args.output/(name+'.log')).write_text(command(kube+['-n',namespace,'logs',name]).stdout)
            results.append(dict(name=name,mode=mode,expected=expected,passed=True))
        (args.output/'policies.json').write_text(command(kube+['-n','telemetry','get','networkpolicy','-o','json']).stdout)
        (args.output/'results.json').write_text(json.dumps(dict(schema_version=1,enforcing_cni='Cilium 1.20.4',cases=results),indent=2)+'\n')
    finally:
        for name,namespace,*_ in cases:subprocess.run(kube+['-n',namespace,'delete','pod',name,'--ignore-not-found','--wait=false'],capture_output=True,timeout=15)
        subprocess.run(kube+['-n','telemetry-client','delete','pod,service','network-denied-target','--ignore-not-found','--wait=false'],capture_output=True,timeout=15)
if __name__=='__main__':main()
