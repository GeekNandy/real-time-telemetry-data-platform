# Working agreement

All implementation, documentation and CI changes belong on a feature branch.
Keep the initial main tree empty while the foundation is under review.

- Work against the acceptance gates in issue #1 and docs/role-alignment.md.
- Treat acceptance, replay and offset ordering as contracts; test failure paths.
- Bound resources and preserve the actual durable operation after client cancellation.
- Keep authentication material, baggage and payloads out of logs, traces and metric labels.
- Preserve schema compatibility and distinguish registry outage from poison data.
- Record the exact command, revision, environment and observed outcome.
- Do not turn a written test, a manifest or synthetic component benchmark into a claim of successful deployment or production capacity.
- Keep source changes reviewable and open a PR. The author's self-review is not independent approval.
