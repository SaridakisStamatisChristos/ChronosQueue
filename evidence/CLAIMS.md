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
| Docker deployment works | UNVERIFIED | Docker executable unavailable in the local R7 execution environment; GitHub Actions is the planned independent oracle. |
| Production-ready for declared target | UNVERIFIED | Await container/CI execution before this claim can close. |
| Production-proven | UNVERIFIED | Requires real sustained deployment/operations evidence, not CI. |
| Commercial-ready | UNVERIFIED | No product license/auth/support/commercial packaging decision supplied. |
| SOTA code quality | UNVERIFIED | No comparative state-of-the-art benchmark/audit justifies the label; completion alone is insufficient. |
| Multi-node/distributed coordination | N/A | Explicit non-goal; no such claim is made. |
| Performance superiority or throughput SLO | N/A | No comparative performance claim is made. |
