#!/usr/bin/env python3
"""Exercise production deployment guardrails through the real Helm renderer."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
valid = {
    "image": "example.invalid/telemetry@sha256:" + "0" * 64,
    "runtime": {"kafkaBootstrapServers": "broker.example.invalid:9093",
                "schemaRegistryUrl": "https://registry.example.invalid", "issuer": "https://issuer.example.invalid"},
    "networkPolicy": {"ingressPeers": [{"namespaceSelector": {"matchLabels": {"purpose": "ingress"}}}],
                      "egress": [{"to": [{"ipBlock": {"cidr": "192.0.2.0/24"}}],
                                  "ports": [{"protocol": "TCP", "port": 9093}]}]}}
cases = [("valid production configuration", valid, None)]
for name, path, value, message in [
    ("mutable image", ("image",), "example.invalid/telemetry:latest", "pinned by sha256"),
    ("plaintext issuer", ("runtime", "issuer"), "http://issuer.example.invalid", "issuer requires HTTPS"),
    ("plaintext registry", ("runtime", "schemaRegistryUrl"), "http://registry.example.invalid", "registry requires HTTPS"),
    ("plaintext broker", ("runtime", "kafkaSecurityProtocol"), "PLAINTEXT", "Kafka requires TLS"),
    ("missing ingress policy", ("networkPolicy", "ingressPeers"), [], "application ingress peers"),
    ("missing egress policy", ("networkPolicy", "egress"), [], "dependency egress rules"),
    ("disabled network policy", ("networkPolicy", "enabled"), False, "networkPolicy.enabled"),
    ("single collector", ("collector", "replicas"), 1, "at least two collectors"),
    ("single worker", ("worker", "replicas"), 1, "at least two workers"),
]:
    config = deepcopy(valid)
    target = config
    for key in path[:-1]:
        target = target.setdefault(key, {})
    target[path[-1]] = value
    cases.append((name, config, message))
with tempfile.TemporaryDirectory(prefix="telemetry-chart-") as directory:
    for name, values, expected in cases:
        file = Path(directory) / "values.json"
        file.write_text(json.dumps(values))
        result = subprocess.run(["helm", "template", "review", "deploy/helm/telemetry", "-f", str(file)],
                                cwd=root, capture_output=True, text=True, timeout=15)
        if expected is None:
            assert result.returncode == 0, result.stderr
            assert "kind: NetworkPolicy" in result.stdout and "whenDeleted: Retain" in result.stdout
            (Path(directory) / "production.yaml").write_text(result.stdout)
            subprocess.run(["kubeconform", "-strict", "-summary", str(Path(directory) / "production.yaml")], check=True)
        else:
            assert result.returncode != 0 and expected in result.stderr, (name, result.stderr)
        print("PASS", name)
