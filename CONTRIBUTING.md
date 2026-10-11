# Contributing

1. Preserve the documented state, lease, attempt, durability, and pagination contracts.
2. Add a regression test for every correctness fix.
3. Run `make quality` and `make test`.
4. Update OpenAPI/docs when public behavior changes.
5. Regenerate `evidence/MANIFEST.sha256` after final changes.
6. Do not weaken a failing test to obtain green CI; classify the failure and repair the earliest invalidated contract/code layer.

Changes that introduce multi-node coordination, authentication, destructive migration semantics, or persistent-format incompatibility require an explicit contract revision rather than an incidental implementation change.
