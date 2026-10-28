# Requirement-to-evidence checklist

This maps the supplied NVIDIA Telemetry Platform role to demonstrated project
capabilities. A checked project capability establishes the named tests and
procedures; employment history and production scale require separate evidence.

| Project checklist | Evidence | Boundary |
| --- | --- | --- |
| ⏳ High throughput and tuning | Bounded publication/checkpoint and consumer batches, stage histograms, fixed k6 SLOs, whole-service JFR and raw audit gates | Full repeated sustained, soak and burst gates must pass before claiming the target rates |
| ✅ Docker and Kafka durability | Actual images, three data brokers, RF3/minISR2, leader/ISR faults and collector-volume-loss reconciliation | Shared controller and host; no controller-quorum or availability-zone HA claim |
| ✅ Kubernetes operation | Enforced Cilium policy, rollout under traffic, failed-rollout recovery, rollback, scaling, private PVC ownership and restart | Executed in kind; AWS infrastructure has validation and mocked plans, not an actual production deployment |
| ✅ Reactive Java implementation | Java 21, WebFlux/Netty, test-only BlockHound, real HTTP/JDBC isolation and request-cancellation checks | Production experience and long-term service ownership belong in the résumé and interview evidence |
| ⏳ Telemetry at the target volume | Schema-valid events, eight-tenant workload, ingestion/visibility separation and exact accepted-ID/content reconciliation | Full target-volume evidence is pending; synthetic events do not represent millions of real users |
| ✅ Security and privacy foundations | Actual HTTPS OIDC/JWKS adversarial tests, JWT claims/scopes, tenant isolation, bounded identity transport, no-store responses, network policy and retention/erasure design | Organizational GDPR duties and production security review remain external to repository tests |
| ✅ Schema management and shared libraries | Compatible Avro evolution, actual registry rejection/restart, shared configuration, metrics and tracing modules | Compatibility covers the retained schemas and stated policies |
| ✅ Automated verification and operational procedures | Protected main, four required checks, source/report digests, fault suites and verified logical restore | AI self-review and owner merge authorization do not replace independent human review |
| ✅ Agentic engineering workflow | Scoped changes, tested failure contracts, reviewable PRs and reproducible evidence | Assess engineering decisions and outcomes, not the number of generated files |

See [verification.md](verification.md) for executed outcomes and exact source
mappings, [slo.md](slo.md) for target budgets, and
[operations.md](operations.md) for procedures. Major-cloud production operation,
shared on-call and incident leadership, mentoring, education and seven years of
production backend experience must be supported by actual professional history.
