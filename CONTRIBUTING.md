# Contribution workflow

The initial main commit is intentionally empty. Work begins on a feature branch,
then enters main through a pull request.

1. Branch from the current main for an independent change.
2. Make cohesive commits that explain behavior and rationale.
3. Run the relevant core checks and the full integration build for delivery,
   persistence, configuration or security changes.
4. Open a PR with observed results, pending checks and failure implications.
5. Resolve review comments and required CI checks before merging.

Do not backdate commits, manufacture review conversations or count a self-review
as an independent approval. A one-person project can honestly use PRs without
pretending to have a larger team.

Branch protection must be configured in repository settings once the workflow
is published. This document does not establish or claim an enforced rule.
