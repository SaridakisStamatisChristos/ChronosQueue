# Requirement matrix

Status meanings: VERIFIED = backed by executed evidence; PARTIAL = implemented but not fully exercised to the requested operational depth; UNVERIFIED = not executed; N/A = outside declared scope.

| Requirement | Status | Evidence |
|---|---|---|
| Durable acceptance/restart | VERIFIED | restart persistence + submission rollback tests |
| At-most-one active lease | VERIFIED | 20-worker one-job race; 24-worker/60-job stress |
| Lease expiry/recovery | VERIFIED | exact-expiry recovery + restart-with-active-lease |
| No premature execution | VERIFIED | fake-clock exact timestamp boundary |
| Fixed and exponential retry/max attempts | VERIFIED | delay boundaries + dead-letter ceiling |
| Terminal-state safety | VERIFIED | cancel/dead-letter/succeeded requeue guards |
| Cancellation races | VERIFIED | concurrent cancel-vs-claim and cancel-vs-complete |
| Idempotent completion/failure | VERIFIED | store + HTTP duplicate-token regressions |
| Persistence consistency | VERIFIED | schema constraints, corruption readiness, restart tests |
| Deterministic ordering | VERIFIED | priority → due time → sequence regression |
| Idempotent submission race | VERIFIED | 16 concurrent same-key submitters |
| Stable pagination | VERIFIED | keyset ceiling + concurrent insert/transition traversal |
| Worker heartbeat stale-generation safety | VERIFIED | sequential + true concurrent expiry/reassignment race |
| Concurrent retry eligibility | VERIFIED | 20-way due-retry reclaim race |
| Crash atomicity | VERIFIED | pre-commit failpoints across all required mutation classes |
| REST API | VERIFIED | live in-process API lifecycle/error/admin tests |
| CLI | VERIFIED | CLI subprocess submit/list/claim/complete integration tests |
| Migrations/reversible rehearsal | VERIFIED | current→previous→current→initial→current + legacy-row retention |
| Readiness/liveness split | VERIFIED | schema incompatibility and storage failure tests |
| Input/payload bounds | VERIFIED | malformed JSON/value, oversized payload, lease/policy bounds |
| OpenAPI/route consistency | VERIFIED | `scripts/check_repo.py` |
| Static/format checks | VERIFIED | repository scripts executed locally |
| Package metadata/module install | VERIFIED | local target install + migrate/help smoke |
| Real-process server workflow | VERIFIED | local submit→claim→complete with structured logs |
| Docker build/Compose validation/boot | UNVERIFIED | GitHub Actions gate defined; local Docker unavailable |
| Container submit→claim→complete | UNVERIFIED | GitHub Actions gate defined |
| Production deployment evidence | UNVERIFIED | no real operational deployment/SLO evidence |
| Commercial readiness | UNVERIFIED | auth/TLS/licensing/support/compliance decisions remain |
