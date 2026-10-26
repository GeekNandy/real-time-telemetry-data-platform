# Contribution workflow

1. Create a feature branch from the latest main.
2. Make a coherent change with its tests and documentation.
3. Run the relevant checks; delivery, persistence and security changes require their integration gates.
4. Open a PR describing behavior, observed verification and remaining limits.
5. Merge only the checked revision after owner authorization and passing CI.

The foundation is introduced as eight sequential PRs, each based on the previous
merged main. Original work remains available in PR #2; this decomposition does
not pretend those changes were originally developed in eight separate reviews.

Do not backdate commits, manufacture review conversations or count self-review as
independent approval. Branch protection is a repository setting; this document
does not claim that a rule is enforced.
