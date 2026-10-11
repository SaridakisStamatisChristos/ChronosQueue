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
| Docker deployment works | VERIFIED | GitHub Actions run `38146181915`: Compose validation, real image build, container boot/readiness, submit → claim → complete smoke, and clean shutdown all passed. |
| Production-ready for declared target | VERIFIED | Scope-bounded verdict for one server + persistent local SQLite + concurrent workers on a trusted local/internal network. Unit, concurrency, migration, evidence, packaging, Docker, health and E2E gates pass on run `38146181915`. This is not a production-proven claim. |
| Production-proven | UNVERIFIED | Requires sustained real deployment/operations evidence, not CI. |
| Commercial-ready | UNVERIFIED | No product license, authentication/authorization, support policy, release-signing posture, or commercial packaging decision has been established. |
| SOTA code quality | UNVERIFIED | No comparative state-of-the-art benchmark or independent comparative audit justifies the SOTA label; completion alone is insufficient. |
| Multi-node/distributed coordination | N/A | Explicit non-goal; no such claim is made. |
| Performance superiority or throughput SLO | N/A | No comparative performance claim is made. |
