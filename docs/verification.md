# Verification evidence

Results identify source, execution revision, content tree and environment. The
latest completed runtime verification is [CI 40688919946](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40688919946):
**136 distinct Java tests, zero failures, errors or skips** (120 unit/contracts
and 16 integrations). Source `839cec2b9e89`, execution `c4c87be2b4ef` and merged
revision `9d0524bfeea9` share tree `e9f6fb6175b664b8a1e79aa87d596c20f91647af`.
The standalone core runner repeats 27 existing cases and is not added again.

All four required checks passed: `console-contracts`,
`contracts-and-integration`, `deployment-contracts` and `kubernetes-smoke`.
The [persistence verification record](../evidence/principal-verification-2026-10-28/persistence-batching.json)
retains suite counts and SHA-256 digests of the downloaded reports.

| Layer | Executed verification |
| --- | --- |
| Durable intake | Checksummed spool recovery, process termination, quotas, bounded waiters, complete-prefix checkpoints and all-or-nothing receipt completion |
| Kafka durability | Actual leader/minISR faults; RF3/minISR2 acceptance; local collector-volume removal after 202 and complete downstream ID reconciliation |
| Persistence | Sequential bounded poll batches, durable-prefix offset recovery, actual mixed valid/poison/suffix poll, confirmed DLT, retries, deduplication and dependency outages |
| Reactive Java | Startup BlockHound agent with a positive blocking canary; actual Netty HTTP remains responsive while four JDBC operations block; cancelling HTTP interrupts its own JDBC work |
| JWT/OIDC | Seven actual HTTPS OIDC/JWKS cases: rotation, claims, trust/SAN, redirects/origins, whole-body deadlines, oversized bodies and HTTP scope enforcement |
| Tenant/privacy controls | Ten adversarial security cases, token-derived tenant access, read/write/observe separation, private/no-store responses and bounded trace metadata |
| Schemas | Avro reader compatibility, actual registry rejection of breaking changes and restart; five transport cases including stalled and oversized responses |
| Browser | Client contracts and actual browser workflows for the live console, recorded explorer, private sessions and logout |
| Deployment policy | Helm lint, ten production guardrails, strict Kubernetes schemas and two mocked Terraform policy runs |

The [reactive/identity record](../evidence/principal-verification-2026-10-28/reactive-identity.json)
links its 128-case run to the source and execution trees. The blocking detector is
test-only and is absent from the runtime image. Data-copy, retention, replay and
erasure boundaries are documented in [security.md](security.md); these controls
are not a claim of GDPR certification.

## Container and Kubernetes operations

[CI 40683420012](https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/40683420012)
executed the [operations evidence](../evidence/principal-verification-2026-10-28/operations.json)
on source `34ca0580d910`, execution `b9fb464137b2` and merged revision
`1681d387fe82`, all with tree `799d5d8f203e86e1c4915fa9bf292697ea6b1a97`.

- Compose ran three data brokers with RF3/minISR2, reconciled 20/20 accepted IDs,
  exercised actual browser submission and authenticated observability, and checked
  private spool ownership.
- Kind used checksum-pinned Cilium 1.20.4 with its default CNI disabled. Six actual
  peer tests passed: allowed ingress, two denied ingress paths, a healthy egress
  target, allowed dependency egress and denied cross-namespace egress. An HTTP 401
  is not accepted as evidence of a network-policy denial.
- A two-replica worker rollout under live traffic reconciled 150/150 HTTP 202 IDs.
  An invalid image rollout retained two available replicas; rollback, scale to
  three and back to two, and another 20/20 reconciliation passed. Collector PVC
  ownership and restart recovery also passed.
- A logical PostgreSQL backup restored all 21 synthetic rows with complete content
  equality into a separate database on the same disposable instance. A truncated
  archive was rejected. This establishes the restore procedure, not cross-region
  availability or a production RPO/RTO.

Kind uses one development broker; it does not inherit the Compose replication
guarantee. AWS validation and mocked plans create no resources and do not establish
actual major-cloud production operations. The protected free Render service hosts
captured evidence, not the live ingestion backend.

## Workload measurements

The [fixed SLO protocol](slo.md) requires repeated 100/s and 500/s workloads, a
30-minute soak and a burst/recovery case, including live visibility, every accepted
identity, CPU/memory/lag, JFR and raw scheduling audits. Short pilots are ineligible
for those sustained claims. Passing a pilot workflow means the harness completed;
its individual workload gates must still be read.

The [earlier measured workload](performance-results.md) retains the original
source, client, budgets and all observed outcomes. It is not a measurement of the
current tuned revision. Current fixed-target results must supply their own source
and protocol hashes before a throughput readiness claim.

Owner merge authorization and automated checks do not establish independent human
review. See the [review guide](review-guide.md) and the
[CI run index](../evidence/verification-runs.json) for archived scopes and mappings.
