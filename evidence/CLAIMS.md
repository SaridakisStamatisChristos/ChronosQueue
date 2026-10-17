# Claims ledger

| Claim | State | Evidence / qualification |
|---|---|---|
| Durable acceptance for declared SQLite target | VERIFIED | Restart test after successful submit; commit precedes service return. |
| At-most-one active lease | VERIFIED | 32-worker race and 120-job concurrent claim test on independent SQLite connections. |
| Stale workers cannot complete reassigned work | VERIFIED | expiry/reassignment and generation-bound completion falsifiers. |
| Retry/attempt limits are exact | VERIFIED | fixed/exponential/max-bound tests plus database `attempt_count <= max_attempts`. |
| Deterministic no-early scheduling | VERIFIED | manual-clock exact boundary tests. |
| Cursor pagination does not repeat due to concurrent inserts | VERIFIED | snapshot-max-sequence concurrency test. Mutable filter membership is explicitly not an MVCC snapshot claim. |
| API/OpenAPI route set matches | VERIFIED | executed route-set equivalence test. |
| Docker deployment works | VERIFIED | The CI container lane validates Compose, builds the real image, boots the deployment, reaches readiness, exercises submit → claim → complete, and shuts down cleanly. |
| Production-ready for declared target | VERIFIED | Scope-bounded verdict for one server + persistent local SQLite + concurrent workers on a trusted local/internal network. Unit, concurrency, migration, evidence, packaging, Docker, health and E2E gates are required by CI. This is not a production-proven claim. |
| Production-proven | UNVERIFIED | Requires sustained real deployment/operations evidence, not CI. |
| Commercial-ready | UNVERIFIED | A proprietary all-rights-reserved license is selected, but built-in authentication/authorization, support policy, release-signing posture, and commercial packaging are not established. |
| SOTA code quality | UNVERIFIED | No comparative state-of-the-art benchmark or independent comparative audit justifies the SOTA label; completion alone is insufficient. |
| Multi-node/distributed coordination | N/A | Explicit non-goal; no such claim is made. |
| Performance superiority or throughput SLO | N/A | No comparative performance claim is made. |
