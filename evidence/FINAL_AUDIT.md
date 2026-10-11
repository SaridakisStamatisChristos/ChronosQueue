# Final falsification gate — evidence closure

Executable verification revision: `f1a87f60e5f65bb606d90b798d242f7102853e90`  
GitHub Actions closure run: `38146181915`

1. **Can two workers ever hold a valid lease simultaneously?** No counterexample found. SQLite write serialization plus the 32-worker single-job race and the many-worker/many-job race yielded one active lease per job.
2. **Can a stale worker complete a reassigned job?** No observed path. Completion at/after expiry and after reassignment is rejected by lease generation/token validation; idempotent replay is limited to the durable receipt for the already accepted completion.
3. **Can a scheduled job execute early?** No. At `T-1µs` the job is not claimable; at exact `T` it becomes eligible under the deterministic manual clock.
4. **Can retries exceed the maximum?** No counterexample remains. F-003 was reproduced and repaired; code and schema now enforce the attempt bound, including explicit-requeue edge cases.
5. **Can cancellation resurrect a job?** No observed path. Cancellation invalidates lease authority and maintenance does not promote terminal states; explicit administrative requeue is a separate audited transition.
6. **Can a process restart lose an acknowledged job?** No counterexample found for the declared SQLite durability model. The restart test retains the job and submission acknowledgement occurs after transaction commit.
7. **Can pagination repeat or skip records incorrectly?** Concurrent inserts are excluded by the pinned maximum creation sequence and traversal produced no repeats. Mutable filter membership is intentionally not described as MVCC snapshot isolation.
8. **Can corrupted or malformed state incorrectly become executable?** Tested illegal state/lease combinations are rejected by SQLite CHECK constraints and malformed external inputs fail closed. Arbitrary physical database corruption is not claimed to be self-healed.
9. **Can the Docker default unintentionally expose the service?** The committed Compose mapping publishes host `127.0.0.1` only. CI run `38146181915` validated Compose, built and booted the real image, reached readiness, exercised submit → claim → complete, and shut it down successfully.
10. **Can documentation make a stronger guarantee than the implementation proves?** The documentation limits the topology to one server with local SQLite, limits the trust boundary to trusted local/internal use, avoids throughput/SLO claims, distinguishes readiness from production-proven status, and does not claim SOTA.

## Closure evidence

GitHub Actions run `38146181915` passed all three jobs:

- Python 3.11: package install, quality/compile checks, all 41 tests, evidence manifest verification, wheel build.
- Python 3.13: package install, quality/compile checks, all 41 tests, evidence manifest verification, wheel build.
- Container: Compose validation, Docker build, real deployment boot, readiness, real submit → claim → complete smoke, clean shutdown.

The evidence manifest verified all 54 authoritative repository files after package installation.

## Verdicts

- **Code Quality:** PASS. No unresolved code-quality blocker was found by the declared invariant, concurrency, migration, API/OpenAPI, malformed-input, recovery, packaging and container gates.
- **SOTA Code Quality:** NOT ESTABLISHED. No comparative SOTA benchmark or independent state-of-the-art codebase comparison was performed.
- **Production Readiness:** PASS, **scope-bounded** to the declared single-server, persistent-local-SQLite, trusted-local/internal deployment model.
- **Production-Proven:** NOT ESTABLISHED. Sustained real operational deployment, upgrade history, incident evidence, load envelope and backup/restore drills in an actual environment remain outside CI evidence.
- **Commercial Readiness:** NOT READY. Licensing, untrusted-network authentication/authorization, release-signing/supply-chain policy, support/SLA posture and commercial packaging remain undecided.

Final code-quality gate: **PASS with no unresolved code-quality blocker known on the verified revision.**
