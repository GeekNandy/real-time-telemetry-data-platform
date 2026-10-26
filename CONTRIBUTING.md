# Contribution workflow

The initial main commit contains only README.md. Implementation, build, CI and
operational changes belong on a feature branch and are proposed through a pull request.
Keep main limited to README.md until the owner explicitly approves a merge.

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
