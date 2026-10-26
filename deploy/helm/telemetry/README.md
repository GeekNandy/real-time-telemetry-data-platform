# Deployment contract

Use a built image from this source revision and provision Kafka, Schema Registry
and PostgreSQL separately. Production defaults intentionally fail rendering until
the image digest, HTTPS endpoints, Kafka endpoints and network peers are supplied.

Create the existing Secret through the environment's secret manager with:
`database-url`, `database-user`, `database-password`, `kafka-jaas` and
`registry-authorization`. Do not commit secret values. PostgreSQL requires
`sslmode=verify-full` in its URL. For private CAs, extend the deployment with the
appropriate mounted trust material and JVM/client settings before use.

Set explicit `networkPolicy.ingressPeers` for the ingress controller, trusted
clients and metrics scraper. Set `networkPolicy.egress` to complete Kubernetes
rules for Kafka, registry, issuer discovery/JWKS, database and optional OTLP.
Rules must include each required destination and port. DNS is separately allowed
to kube-dns; adapt that rule if the cluster uses a different DNS topology.
NetworkPolicies need an enforcing CNI. Use a trusted TLS ingress/service mesh for
client traffic; do not expose the application HTTP port directly to the internet.

Run the separate schema bootstrap before read-only production services start.
Then render, validate and review the release:

```bash
helm lint deploy/helm/telemetry -f /path/to/reviewed-values.yaml
helm template telemetry deploy/helm/telemetry -f /path/to/reviewed-values.yaml > /tmp/telemetry.yaml
kubeconform -strict -summary /tmp/telemetry.yaml
```

The collector StatefulSet uses retained exclusive PVCs. Replicas do not replicate
one another's buffers; preserve every ordinal and its volume during changes.
Workers use independent deployments, manual Kafka acknowledgements and a bounded
JDBC scheduler. Startup/readiness/liveness have distinct purposes. Graceful stop
rejects new intake, settles admitted appends and retains pending buffered records.

`values-ci.yaml` deliberately uses demo auth and plaintext local dependencies in
an isolated kind cluster. It must not be used for production. The kind script
checks rollout, HTTP/persistence reconciliation, restart and PVC mounting; it
does not establish HA, controller-quorum safety or cloud deployability.
