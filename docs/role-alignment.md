# Requirement-to-evidence map

Source: [NVIDIA JR2027710](https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/job/Germany-Munich/Senior-Software-Engineer--Real-Time-Data-Platform_JR2027710?source=jobboardlinkedin),
checked 25 October 2026. This document maps technical signals to implementation
and verification, not to a hiring probability.

| Signal | Implementation | Required evidence |
| --- | --- | --- |
| Java concurrency and profiling | Immutable core, atomic admission, bounded executor, queue deadline, cancellation and drain | Core contracts; whole-service load and JFR analysis |
| Durable event intake | Checksummed file/directory-synced spool, Kafka acknowledgements, transactional sink and replay | Process kill, dependency outage, leader/ISR loss and ID reconciliation |
| Reactive services | WebFlux intake; filesystem and JDBC work isolated from event-loop threads | Real HTTP contract checks and load behavior |
| Event-schema lifecycle | Avro versions, compatible evolution and registry client | Reader compatibility, registry rejection of breaking changes and registry restart |
| Shared platform libraries | Configuration, metrics, traces, schemas and event contracts | Module tests plus propagation across the actual pipeline |
| Containers and orchestration | Separate collector/worker profiles and persistent-volume ownership | Image build, Kubernetes rollout, readiness and drain checks |
| Cloud operation | Production transport policy and deployment infrastructure | Configuration validation, reviewed plan and actual deployment evidence |
| Testing and observability | Unit/contract/fault suites, CI, metrics and trace propagation | Published reports on the feature commit |
| Security/privacy foundations | Verified JWT identity, scopes, tenant-qualified access, restricted trace metadata and retention controls | Adversarial API checks, production-policy checks and documented data flow |
| Coding-agent proficiency | Scoped issue, implementation/test cycle and explicit human review gate | Actual change log and command outcomes |

Production tenure, on-call history, mentoring, design leadership and fluent
communication must be demonstrated through the applicant's real experience.
A new repository cannot manufacture those qualifications.

The current execution status for every gate is in [verification.md](verification.md).
Only observed outcomes may be marked passed.
