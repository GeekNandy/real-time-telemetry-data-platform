# Verification matrix

This file separates executable checks from results actually observed. A written
test is a pending claim until it runs successfully on the stated revision.

| Layer | Check | Current evidence |
| --- | --- | --- |
| Core | JDK compilation, all warnings are errors | Passed locally on Java 17.0.20 |
| Core | 25 delivery/admission/replay cases | Passed locally |
| Service unit | JWT signatures, issuer, audience, expiry and tenant validation | 10 tests passed on Java 21 |
| Service unit | Strict wire contract and invalid broker envelopes | 3 tests passed on Java 21 |
| Integration | HTTP acceptance, replay, tenant-scoped retrieval | Written; not run |
| Integration | Reconcile accepted and stored event ID sets | Written; not run |
| Integration | Inject DB-commit/offset-commit gap and replay | Written; not run |
| Integration | Confirm collision and poison delivery to DLT | Written; not run |
| Integration | Pause real PostgreSQL, verify no offset advance, resume | Written; not run |
| Integration | Stop the broker, require HTTP 503 | Written; not run |
| Packaging | Boot JAR and container build | Boot JAR passed; container build pending |
| Performance | Core with an immediate-ack broker double | Reproducible harness only; see performance notes |
| End-to-end | Open-loop load, consumer lag, reconciliation and resource usage | Not run |
| Resilience | Multi-broker leader failure, ISR loss, disk failure | Not run |
| Resilience | DLT publish failure, service SIGKILL and restart, rebalance | Not run |

## Core cases

The JDK runner and JUnit adapter execute the same cases; they are not two
independent suites. The cases cover schema/attribute validation, storage-safe
timestamps and Unicode, immutable
events, canonical fingerprints, invalid capacity, atomic tenant/global limits,
idempotent lease release, the broker-ack boundary, cancellation, synchronous and
asynchronous broker errors, null broker completions, tenant isolation, drain,
shutdown deadlines, concurrent saturation, offset ordering, commit-gap replay,
storage failure, payload collision, tenant-qualified keys and randomized replay.

The randomized replay case uses seed 42, 500 unique events and 1,200 deliveries.
It uses an in-memory sink double and proves the processor contract, not
PostgreSQL concurrency behavior.

## Integration acceptance criteria

The integration suite uses real containers and HTTP requests with signed JWTs.
Its commit-gap test injects the database state that a crash would leave, then
restarts listener consumption. It is not a separate-process SIGKILL test.

The database outage test pauses its container for longer than the socket
timeout, checks the actual consumer-group offset, resumes PostgreSQL and checks
for exactly one stored row. The poison/collision tests inspect the real DLT and
the committed source offset. The broker outage test runs last because it stops
the shared broker.

## Before claiming application readiness

1. Run the complete build and both suites on the feature commit; retain reports.
2. Build the image and execute the documented Compose demonstration.
3. Investigate failures before changing any status here.
4. Run an end-to-end load experiment and publish raw results with its source
   revision, dependency image digests, machine configuration and limitations.
5. Obtain an independent technical review where possible; do not represent an
   author's PR description as peer review.

The authoring environment now has a verified Temurin JDK 21.0.13 and Maven 3.9.12 toolchain. The full baseline build and all 38 unit/core cases pass. Docker execution remains unavailable.
