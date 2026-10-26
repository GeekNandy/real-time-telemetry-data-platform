# Working agreement

All implementation, documentation and CI changes belong on a feature branch and
are proposed through a PR. Main receives changes through reviewed, tested PRs.
The owner has authorized the eight-part foundation sequence to merge after its checks pass.

- Keep each PR coherent, buildable and accompanied by its relevant tests.
- Treat acceptance, replay and offset ordering as contracts; test failure paths.
- Bound resources and preserve real durable work after client cancellation.
- Keep credentials, baggage and payloads out of logs, traces and metric labels.
- Preserve schema compatibility and separate infrastructure outages from poison data.
- Record the tested revision, environment and observed outcome.
- Written tests, manifests and component benchmarks are not evidence of deployment or capacity.
- The author's self-review and owner merge authorization are not independent human approval.
