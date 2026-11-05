# Three-controller KRaft reference (optional)

The default demo uses one combined Kafka broker/controller. The existing
[replicated Compose overlay](../compose.replicated.yaml) adds two **data** brokers
with RF3, minISR2 and `acks=all`, but still has only one metadata controller.

This optional [quorum overlay](../compose.kraft-quorum.yaml) adds **three dedicated
KRaft controllers** (IDs 101–103) and makes all three data brokers (IDs 1–3)
broker-only. Every controller has its own retained volume. All six processes use
the existing pinned `apache/kafka:4.0.0` image. There is **no new application
Raft server or custom Kafka image**.

Apache Kafka 4.0's official Docker image examples use a **static**
`controller.quorum.voters` configuration. All six nodes receive the same complete
voter list; a controller quorum requires a majority (two out of three). This
prototype intentionally uses static membership, not KIP-853 dynamic
membership. See [Kafka KRaft operations](https://kafka.apache.org/40/operations/kraft/)
and the [Kafka 4.0 isolated example](https://github.com/apache/kafka/blob/4.0.0/docker/examples/docker-compose-files/cluster/isolated/plaintext/docker-compose.yml).

## Start on a fresh project

Requires Docker Compose v2, Java 21, Python 3, sufficient free RAM for six
Kafka processes and the existing collector, worker and dependencies. Use a
**new Compose project name**, not the previous single-controller project's volumes.

```bash
./scripts/dev-up.sh -p telemetry-kraft-demo \
  -f compose.yaml -f compose.replicated.yaml -f compose.kraft-quorum.yaml
./scripts/demo-event.sh
```

The existing API endpoints and authentication behavior do not change.
Controller ports are internal to the Compose network and are not published to
the host. The controller quorums store metadata, **not telemetry event bodies**.
Persistent event data still lives on the three data brokers, subject to the
replication/ISR policy.

To inspect the elected controller from within the broker:

```bash
docker compose -p telemetry-kraft-demo \
  -f compose.yaml -f compose.replicated.yaml -f compose.kraft-quorum.yaml \
  exec -T kafka /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status
```

Expect a `LeaderId` in 101–103 and `CurrentVoters` containing all three
controller IDs. A controller that is reachable at startup may need time to
finish catching up before its healthcheck turns healthy.

The project **must not** reuse Kafka data volumes formatted for the
single-controller topology. This is not an in-place quorum migration script.
Never destroy an existing project with `down -v` as a shortcut for migrating.
For existing data, plan and separately validate the proper Kafka quorum
migration/backup procedure, including the format/feature version. In
particular, using the same cluster ID does not make distinct metadata logs
interchangeable.

## Verify topology and failover

The zero-startup Compose configuration contract checks every controller and
broker's role, voter set, durability volumes, broker replication settings,
startup dependencies and collector/worker bootstrap peers:

```bash
python3 scripts/test-kraft-quorum.py
```

With Docker available, the isolated smoke uses fresh synthetic credentials and
volumes. It waits for registry HTTP health, then runs the existing schema
bootstrap with bounded retries, since
the registry's HTTP health endpoint can respond before its Kafka-backed store
confirms writes. A temporary smoke-only Compose override then runs the collector
and worker in read-only registry mode; it leaves the demo defaults unchanged.

After HTTP readiness, it warms the first producer, durable checkpoint
and worker query path with up to six single-event probes. Each probe must confirm
an HTTP 202 receipt and a matching query; a timed-out event that later becomes
visible is insufficient. Warm-up attempts have bounded request and settle times
and retain separate reports under `warmup/`.
Kafka setup CLI calls allow up to 60 seconds for JVM startup on a shared runner;
topic-readiness polling stops starting new attempts after three minutes. These
setup bounds do not change application acceptance deadlines or broker ISR policy.

The smoke then measures a small pre-failure accepted-ID cohort, force-stops the
**currently elected** metadata controller, verifies a **different** leader is
elected, and reconciles another accepted-ID cohort while one controller remains
down. It then restarts the controller. Its cleanup removes only the
randomly named smoke project volumes and temporary credentials.
Both measured cohorts still require all 20 events to receive matching HTTP 202
receipts and become queryable; neither cohort retries failed requests.

```bash
bash scripts/kraft-quorum-smoke.sh
```

Reports are written under ignored `data/kraft-quorum-smoke/` by default,
or `TELEMETRY_KRAFT_REPORTS`. The script uses local loopback ports, so do not
run it alongside the ordinary demo. The smoke intentionally does **not**
force two simultaneous controller failures: that should be a separately scoped,
bounded quorum-loss exercise verifying fail-closed metadata operations and
eventual recovery without making unsafe assumptions about broker cache behavior.

This is a **single-host failure exercise**, not a multi-AZ test, a capacity
experiment, a controller-durability backup policy, or a production RPO/RTO
measurement. It does not replace the fixed 100/s, 500/s, soak and burst SLO
release gates. Do not weaken broker ISR policy to obtain a green result.

## Service boundaries and operational follow-up

The collector, worker, and the optional [local read-only MCP operator](operator-mcp.md)
remain separate processes. KRaft is Kafka's internal metadata consensus;
placing a second Raft implementation in the ingest/acknowledgement path would
add failure modes without improving the existing correctness contract.

Next SRE gates for a production-oriented topology include controller leader/
follower lag telemetry, controller-quorum loss alerting, metadata fault
injection, multi-fault recovery, backup and restore drills, and actual
multi-failure-domain deployment evidence. A remote MCP service would need a
separate authentication and network-admission design; the current stdio adapter
does not justify an always-on externally reachable MCP container.
