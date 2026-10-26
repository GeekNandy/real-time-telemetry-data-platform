# Operator console

The console makes the Java/WebFlux telemetry pipeline inspectable through real
API requests. It is a local operator and developer workflow, with no simulated
production statistics or per-event Kafka publication status.

## Run the complete demo

With Docker Engine and Compose v2 installed:

```bash
./scripts/dev-up.sh
```

Open **http://127.0.0.1:8088**. Select **Connect session**, then paste the contents
of `data/auth/console.token`. This generated, one-hour local token grants
`telemetry.write`, `telemetry.read` and `telemetry.observe`. To issue a different
local tenant's token, run `python3 scripts/demo-token.py --tenant other --console`.
The demo signing key remains on the server; the browser cannot mint tokens.

Grafana is at **http://127.0.0.1:3000**, with user `operator` and the generated
password in `data/auth/grafana-admin-password`. Its datasource and six-panel
Telemetry dashboard are provisioned automatically. Prometheus is on port 9090
and Jaeger is on port 16686. The console's diagnostics links use the current
hostname and these local demo ports. All published ports bind to loopback.

The generated credentials are ignored by Git. Rerun `dev-up.sh` to refresh demo
JWTs; Prometheus also uses an expiring observer token. The Grafana administrator
password is retained across starts. Replacing its file does not reset an already
initialized Grafana database; use Grafana's supported password-reset workflow.

## Workflows and guarantees

| View | Observed behavior |
| --- | --- |
| Overview | Both readiness endpoints plus fixed aggregate metrics from each role |
| Event playground | Submit a schema-shaped synthetic event, verify the exact HTTP 202 receipt ID, then query the worker for that ID |
| Event inspector | Retrieve one event using the JWT's verified tenant and read scope |
| Diagnostics | Open provisioned dashboards, metrics queries and distributed traces |

An acceptance receipt proves a synchronized local-volume append. A matching
worker response proves query visibility through the PostgreSQL-backed store.
Kafka acknowledgement is not exposed as a per-event UI milestone. Application
readiness does not establish broker health or high availability. A transient 404
after acceptance is displayed as pending, not as confirmed loss.

If the request times out or the receipt is invalid, delivery remains unconfirmed.
The console retains the exact ID and payload for an explicit retry. It never
automatically resubmits a POST. Event query polling is bounded to a 30-second
observation window, with a maximum six-second request budget and backoff capped
at two seconds. Users can check visibility again. The editor stays locked for a
submitted event; **New event** creates a fresh identity and payload.

Metrics are platform-wide and require `telemetry.observe`; event scopes alone do
not grant them. A separate observer token can be supplied. Tokens and inspected
payloads stay in tab memory, with no localStorage, sessionStorage, cookies or
service worker. Clearing/replacing the session aborts requests, discards late
results and clears displayed event data. The API remains the authentication and
authorization authority.

## Bounded operation

`GET /v1/operations/metrics` reads a fixed set of existing Micrometer meters. It
performs no filesystem, registry, Kafka or JDBC operation on the event loop and
never enumerates tenants or accepts user-selected metric names. Missing or
nonfinite values are `null`, not manufactured zeros. Snapshots include a process
instance ID so counter resets do not produce false throughput spikes.

The overview refreshes after the preceding refresh completes, with a five-second
delay, only while its tab and overview are visible. It maintains at most four
in-flight reads and sixty-one chart samples from the last five minutes. Rates
are observations between successful samples, not capacity or latency SLOs.
Stage attempts include failures and are not a distinct-event persistence count.

The browser bounds JSON responses at 64 KiB and applies an absolute deadline to
headers and body. Returned attributes render as text. CSP permits only local
scripts, styles, images and connections. The non-root, read-only Nginx container
proxies only six fixed API routes; it does not offer an arbitrary upstream proxy,
copy browser cookies or enable cross-origin API access. JWT verification remains
on both backend services. The proxy intentionally omits request access logs.

## Verification and role alignment

The feature exercises the job's backend concerns through an operator workflow:
Java/Spring authorization and metrics, event schema validation, asynchronous
delivery boundaries, tenant isolation, W3C trace propagation and containerized
execution. It does not add a UI framework solely to expand the technology list.

```bash
npm ci --prefix console --ignore-scripts
npm --prefix console test
./console/node_modules/.bin/playwright install chromium
npm --prefix console run test:browser
```

The eight browser contracts use explicitly synthetic HTTP fixtures for error,
identity, cancellation, rendering and mobile cases. Their screenshots are labeled
`fixture-*`. These are separate from the actual Compose browser check, which
clicks through submission, receipt validation and tenant-scoped query visibility
against the real Kafka/PostgreSQL pipeline. That check saves `live-*` screenshots
without credential dialogs, and also verifies proxy restrictions, observer
authentication, automatic Grafana provisioning and both authenticated scrapes.

This change packages the local demo, not a hosted production identity flow.
Serving it in another environment requires reviewed TLS termination, an actual
issuer and its token-acquisition flow, deployment-specific diagnostic links and
the existing production transport/network controls. No cloud availability or
capacity result is implied by a browser smoke.
