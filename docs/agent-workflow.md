# Agent-assisted development

AI assistance is used for scoped implementation and verification tasks.
Maintainers are responsible for the code, review decisions and published claims.

1. Define the behavior, acceptance boundary and failure cases.
2. Implement the change on a feature branch with bounded resource use and relevant tests.
3. Run unit and contract checks, then the required integration and deployment gates.
4. Record commands, source revisions, environments and observed outcomes.
5. Open a PR covering the resulting behavior, verification and operational implications.

Follow the [working agreement](../AGENTS.md), [contribution workflow](../CONTRIBUTING.md)
and [technical review guide](review-guide.md). A successful package build verifies
packaging; runtime behavior requires the relevant process and dependency checks.

CI has read-only repository permissions and does not deploy to AWS.
[Verification records](verification.md) identify the tested revisions and the
scope of each result. Report unavailable checks as pending and distinguish
correctness checks from capacity measurements.
