# Verification records

Each record identifies its tested source revision, observation date and execution scope.

| Record | Contents |
| --- | --- |
| [verification-runs.json](verification-runs.json) | Selected passing CI runs with tested source revisions and available merge metadata |
| [replicated-acceptance-2026-10-27.json](replicated-acceptance-2026-10-27.json) | Kafka-confirmed acceptance, 107 Java cases, replicated failure scope and deployment observations |
| [hosted-run-2026-10-27/manifest.json](hosted-run-2026-10-27/manifest.json) | Original deployment/browser reports and digests used by the protected recorded-run explorer |
| [ci-2026-10-26.json](ci-2026-10-26.json) | Dated CI job outcomes, reconciliation results and artifact identifiers |
| [verification.json](verification.json) | Dated local test results and toolchain versions |
| [published-source.json](published-source.json) | Local-to-published revision mappings for archived results |
| Text and JSONL reports | Captured contract, schema and synthetic request outcomes |

Read each result against its recorded revision and environment.
Component tests and twenty-event correctness smokes do not establish pipeline
capacity. See [verification evidence](../docs/verification.md) for the gate descriptions.
