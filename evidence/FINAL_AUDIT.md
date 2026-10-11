# Final falsification gate

Local pre-CI audit, 2026-10-11.

1. **Can two workers hold a valid lease simultaneously?** No counterexample found; schema stores one lease and required concurrency races pass.
2. **Can a stale worker complete a reassigned job?** Required stale completion and concurrent expiry/reassignment regressions pass.
3. **Can a scheduled job execute early?** Exact fake-clock boundary passes; claim is unavailable until `now >= scheduled_at`.
4. **Can retries exceed maximum?** Attempt/dead-letter tests and schema bounds pass.
5. **Can cancellation resurrect a job?** No implicit transition exists; only explicit administrative requeue from cancelled/dead-lettered is allowed.
6. **Can restart lose an acknowledged job?** Repository restart tests pass and pre-commit rollback is verified; physical durability remains bounded by SQLite/filesystem/device sync semantics.
7. **Can pagination repeat or skip incorrectly?** No counterexample under concurrent inserts/transitions for unfiltered traversal within the captured ceiling. Live filter-membership changes across pages remain documented semantics.
8. **Can malformed/corrupt state become executable?** Strong schema checks reject partial lease metadata; injected corruption fails readiness; malformed external inputs fail safely.
9. **Can Docker default unintentionally expose the service?** Compose configuration is host-loopback-only by inspection. Runtime validation remains PENDING until CI because local Docker is unavailable.
10. **Can documentation overclaim?** Claims and verdicts explicitly separate local code evidence, container evidence, production proof, SOTA, and commercial readiness.

### Gate result

No unresolved **core code-correctness blocker** remains in local evidence. Final repository completion is still contingent on green GitHub Actions Docker/Compose/container smoke evidence.
