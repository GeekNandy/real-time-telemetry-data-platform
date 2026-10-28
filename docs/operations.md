# Operating the platform

## Local demonstration

Use Java 21, Python3, Docker Engine and Compose v2. Run `./scripts/dev-up.sh` from
the repository root. It generates ignored local credentials, starts dependencies
and waits for both application readiness endpoints. It enables the console and
observability profiles, including Grafana and Prometheus. The demo has one Kafka
broker and local volumes; it is not HA.

- Collector HTTP: `http://127.0.0.1:8080`.
- Worker queries: `http://127.0.0.1:8082`.
- Schema Registry: `http://127.0.0.1:8081`.
- Jaeger traces: `http://127.0.0.1:16686` (transient in-memory storage).
- Optional Prometheus: `http://127.0.0.1:9090`.
- Operator console: `http://127.0.0.1:8088`.
- Provisioned Grafana: `http://127.0.0.1:3000`.

See [the console guide](console.md) for generated credentials and the interactive
event workflow. The console uses explicit bearer tokens; event and observer
scopes are enforced by the backend. Aggregate metrics are platform-wide.

Run `./scripts/demo-event.sh`; retrieve its ID from the worker using the token
from `python3 scripts/demo-token.py --tenant demo`. A 202 response means the
broker acknowledged publication and the collector checkpointed delivery. It does not mean PostgreSQL is already
queryable. Query 404 can be transient immediately after acceptance.

To run the three-broker reference on a fresh set of volumes, stop the single-broker
demo and run:

```bash
./scripts/dev-up.sh -p telemetry-replicated -f compose.yaml -f compose.replicated.yaml
```

This uses RF 3, minISR 2 and `acks=all`. Both application roles verify the effective
producer and actual topic policy at startup. Keep that policy under administrative
access control; changing it after verification changes the fault assumptions. The
reference has one controller and does not establish controller-quorum or AZ HA.

`docker compose stop` retains state. `docker compose down -v` destroys all local
volumes, including Kafka's accepted records and unconfirmed spool entries; use it only when
that data can be discarded. Demo credentials and bearer tokens must never be
committed. The issuer and HS256 mode are explicitly for local development.

## Kafka outage or volume pressure

Watch `telemetry_buffer_records`, `telemetry_buffer_bytes`,
`telemetry_buffer_oldest_age` (seconds) and dispatch failures. Inspect broker
leaders, ISR and producer errors. A blocked dispatcher retains its head record;
confirmed Kafka publication precedes checkpoint advancement and reclamation.
Requests wait up to `ACCEPTANCE_DEADLINE_MS` (default 5000, maximum 30000) from spool
append start for confirmed publication/checkpointing. Unconfirmed requests receive
503 with `Retry-After`; their buffered records remain eligible for later delivery.
No 202 is returned during a broker outage or insufficient ISR. The volume or record
budget can also reject with 503 before the deadline. Transient concurrent
admission exhaustion returns 429. Retry with the same event ID and identical
content. A timeout or 503 may be ambiguous; never generate a replacement ID.

Restore broker connectivity or ISR before resizing. Do not lower minISR, enable
unclean election, delete pending files, share a spool between collectors or
scale a collector onto an empty volume to make an alert disappear. Graceful
shutdown leaves unpublished records on its retained volume. If a volume is
corrupt, preserve a snapshot, stop its owner, reconcile accepted IDs against
Kafka/PostgreSQL and investigate the failed checksum before attempting recovery.
There is no automatic repair that can prove unconfirmed contents of a lost volume.
Records that received 202 already exist in Kafka; preserve its replicas and reconcile
against Kafka/PostgreSQL after replacing a collector volume.

The replica test covers data-plane leader failure and loss of minISR. It has one
controller and does not establish controller-quorum or availability-zone HA.

## Database outage and offsets

Workers retry database failures without committing the affected source offset.
Intake can continue through Kafka while retention and capacity permit it. Query
storage failures return 503. Compare consumer-group committed offsets with the
source log end, database pool waits and persistence failures. Restore storage,
then require lag to drain and reconcile acknowledged IDs; a green process alone
is insufficient. Do not skip offsets to clear lag.

The transaction inserts `(tenant_id,event_id)` once and compares a canonical
fingerprint on replay. Identical replay is a duplicate; different content under
the same identity is quarantined. This is idempotent storage within the retained
identity window, not a global exactly-once claim across all external systems.

## Registry, poison records and DLT

Deploy schema changes with a separate migration identity:
`python3 scripts/schema-bootstrap.py --registry-url https://registry.example`.
Supply `SCHEMA_REGISTRY_AUTHORIZATION` through the secret system. Applications
in production are read-only registry clients. Both bundled versions must be
registered before cold startup. The bootstrap refuses a conflicting existing
compatibility policy and registers versions under FULL_TRANSITIVE.

A registry outage is retryable infrastructure failure. An invalid wire envelope,
unknown schema ID or mismatched partition key is poison. Poison and fingerprint
collisions route to the DLT; its confirmed publication precedes source offset
advancement. DLT publication failure must not be worked around by committing the
source. Restrict DLT access as strictly as source access: it contains original
payloads. Inspect and repair in an isolated environment, preserve the tenant and
event identity and replay through a controlled producer. Do not replay collisions
until the conflicting producer behavior is understood.

## Metrics and authentication

JWT verification requires issuer, audience, expiry, a valid `tenant_id` and the
appropriate scope: `telemetry.write`, `telemetry.read` or `telemetry.observe`.
Health endpoints expose no component details. Every query is tenant-qualified;
clients cannot choose an ingestion tenant in the body.

The shared telemetry library uses stable stage labels, not tenant IDs or event
IDs. Only bounded W3C trace context crosses the spool and Kafka; baggage and
Authorization are not propagated. Prometheus in the demo uses a metrics-only JWT
that expires after one hour. Refresh it by rerunning `dev-up.sh`; production
scrapers require issuer-supported token rotation. A scrape failure may indicate
token expiry rather than an application outage.

The observability profile provisions `deploy/observability/dashboard.json` with
the fixed `telemetry-prometheus` datasource UID. Grafana requires the generated
administrator credentials; anonymous access is disabled. The alert rules are
starting thresholds for the demo, not measured SLOs.
They need routing and workload-specific tuning before operational use.

## Kubernetes rollout and production transport

See `deploy/helm/telemetry`. Collectors are StatefulSets with exclusive retained
PVCs; workers are separately scalable Deployments. Retained PVCs and EBS volumes
remain after scale-down or release deletion. Inventory and drain them before
intentional decommissioning. A node/AZ outage can leave a volume unavailable;
PDBs and replicas do not replicate collector buffers or make an RWO volume
multi-zone. Preserve every old ordinal's volume during a rollout.

The chart requires image digests, HTTPS issuer/registry, TLS Kafka, explicit
network peers and at least two replicas per role in production. Put secrets in
the existing Kubernetes Secret; mount private CA material and configure JVM,
Kafka and PostgreSQL trust when public CAs do not suffice. The database URL must
use `sslmode=verify-full`. Terminate client TLS in a trusted ingress or service
mesh; the application HTTP port is internal. Restrict access to that port to the
configured ingress and metrics peers. Do not expose it with an unrestricted
LoadBalancer. Kubernetes NetworkPolicies require an enforcing CNI.

The private EKS reference enables VPC CNI network-policy support and encrypted
EBS storage. Its prerequisite networking, private-runner access, managed Kafka,
registry, database, backups, restore procedures and secret rotation remain
operator responsibilities. Read `infra/aws/README.md` before a cloud plan.

## Retention, privacy and incident exercises

Source and DLT retention defaults to seven days. PostgreSQL rows are removed in
bounded batches after thirty days measured from storage time; the retention
index supports that sweep. Spool records are reclaimed only after confirmed
publication. Persistent outages can therefore retain spool data longer than
normal retention. Alert and reconcile before any manual deletion.

The attribute allowlist and length limits reduce collection; they do not prevent
a producer from putting personal data into an allowed value. Use synthetic data
for demonstrations and agree purpose, lawful basis, tenant lifecycle and retention
with the real service owner. Deleting a PostgreSQL row does not erase Kafka,
DLT, spool files, traces, snapshots or backups. An erasure request needs a data
inventory and a coordinated workflow for each copy. Backup expiry and encrypted
volume snapshots must be included. No GDPR certification or compliance conclusion
is implied by this reference project.

Incident exercise: simulate Kafka loss, full buffer, a collector kill, database
pause and a schema outage; record detection, accepted IDs, recovery and unresolved
outcomes. Mark it as a simulation. The executed crash/broker/registry checks are
published with the PR CI reports; they are not a record of production on-call work.

Registry calls have a deadline for the full response body. A delivered checkpoint
and file reclamation are separate recoverable states; failed reclamation is retried.


## Operational exercise protocol

Kubernetes CI installs checksum-pinned Cilium in an ephemeral kind cluster with
its default CNI disabled. Application pods are admitted under the restricted Pod
Security Standard; development dependencies use a separate namespace. Six probes
require healthy allowed ingress/dependency paths, reject wrong labels/namespaces,
and deny egress to an independently verified healthy unapproved target. HTTP
401/403 responses are not accepted as evidence of network isolation.

The exercise reconciles synthetic acknowledgements during a worker rolling
restart, verifies that a deliberately invalid image leaves old replicas available,
undoes the rollout, scales workers out/in, and reconciles a fresh accepted set.
Collector restart checks its retained process-owned WAL volume. Three-broker
and insufficient-ISR fault contracts run separately. Passing CI proves these
behaviors on its cited source, not multi-AZ or production cloud operation.

Compose CI also takes a logical PostgreSQL backup after its synthetic journeys,
restores it into a separate scratch database and compares every complete event
row. A truncated archive must be rejected. Both archives and the scratch database
are removed; counts, digest and elapsed time remain. This same-instance logical
restore does not establish production backup policy or cross-region RPO/RTO.
