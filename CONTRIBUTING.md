# Contribution workflow

1. Create a feature branch from the latest main.
2. Make a coherent change with its tests and documentation.
3. Run the relevant checks; delivery, persistence and security changes require their integration gates.
4. Open a PR describing behavior, observed verification and remaining limits.
5. Merge only the checked revision after owner authorization and passing CI.

Keep each PR focused on one coherent change. Record the tested revision,
environment and results so reviewers can assess its behavior and operational impact.
