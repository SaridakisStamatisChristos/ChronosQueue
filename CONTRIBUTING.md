# Contributing

Preserve the documented state-machine and durability invariants. Every correctness change must add or update regression tests. Do not weaken concurrency tests or change exact-capacity/lease semantics merely to obtain green CI.

Before proposing changes run the commands in `docs/development.md`. New API routes must be added to both the runtime route contract and OpenAPI; CI rejects divergence.
