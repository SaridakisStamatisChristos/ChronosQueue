# Requirement coverage matrix

Evidence state meanings: **VERIFIED** = directly executed on this revision locally and/or in the referenced GitHub CI; **PARTIAL** = meaningful evidence exists but a material lane is pending; **UNVERIFIED** = not executed yet; **N/A** = explicitly outside the declared product contract.

| ID | Requirement | Evidence/artifact | State |
|---|---|---|---|
| R-001 | Successful job creation is durable before acknowledgement. | `test_durable_acceptance_survives_restart`; SQLite commit-before-return design. | VERIFIED |
| R-002 | At most one simultaneously valid lease per job. | 32-worker one-job race + many-worker/many-job tests. | VERIFIED |
| R-003 | Expired leases recover without zombie jobs and attempt accounting stays correct. | expiry/reassignment, restart recovery, final-attempt expiry tests. | VERIFIED |
| R-004 | Scheduled jobs are never claimable before T. | deterministic exact-boundary clock test. | VERIFIED |
| R-005 | Fixed/exponential retry and maximum attempts are enforced and durable. | retry boundary tests, schema attempt check, restart persistence. | VERIFIED |
| R-006 | Succeeded/cancelled/dead-lettered terminal safety; requeue explicit. | terminal and explicit-requeue tests. | VERIFIED |
| R-007 | Cancellation races with claim/completion/expiry safely. | cancellation-after-claim and concurrent cancellation/completion falsifiers. | VERIFIED |
| R-008 | Completion is idempotent; stale worker cannot complete after reassignment. | durable receipt replay + stale generation/expiry tests. | VERIFIED |
| R-009 | State, attempts, lease, retry, terminal metadata remain persistence-consistent. | schema CHECK constraints, restart tests, migration tests. | VERIFIED |
| R-010 | Deterministic claim order: priority DESC, schedule ASC, creation sequence ASC. | ordering test + documented SQL. | VERIFIED |
| R-011 | Immediate/delayed/absolute jobs, queue, priority, tags, retry and idempotency submission. | service/API implementation and suite. | VERIFIED |
| R-012 | Payload size and malformed/non-finite JSON fail cleanly. | payload bound + malformed HTTP + non-finite tests. | VERIFIED |
| R-013 | Worker can claim multiple queues with configurable bounded lease duration. | implementation plus zero/overlong lease falsifiers. | VERIFIED |
| R-014 | Heartbeat extends only current unexpired lease generation. | expiry boundary + heartbeat/reassignment race. | VERIFIED |
| R-015 | Worker success/failure may attach result/error; server controls retry/dead-letter. | core retry/completion tests. | VERIFIED |
| R-016 | Dead-lettered jobs can be inspected and explicitly requeued. | list state filter + requeue tests/CLI. | VERIFIED |
| R-017 | Get/list filters cover queue/state/tags/time/priority/worker. | query implementation; core tag/filter code paths. | VERIFIED |
| R-018 | Pagination is keyset/cursor, non-repeating under concurrent inserts. | concurrent-insert pagination test, malformed/filter-bound cursor tests. | VERIFIED |
| R-019 | Cancel/requeue/purge/stats/active-leases admin operations exist with narrow semantics. | service/API/CLI; terminal-only purge falsifier. | VERIFIED |
| R-020 | Versioned HTTP API includes required v1, health and metrics routes. | OpenAPI + live API tests. | VERIFIED |
| R-021 | OpenAPI and runtime route declarations cannot silently diverge. | `test_openapi_route_set_equals_declared_runtime_route_set`. | VERIFIED |
| R-022 | CLI supports serve/submit/get/list/cancel/requeue/claim/heartbeat/complete/fail/queues/leases/health/migrate/purge. | Installed `chronosqueue` entry point exercised against a live server: submit → claim → complete. | VERIFIED |
| R-023 | SQLite uses WAL, foreign keys, transactional writes, busy timeout, synchronous durability. | store initialization + executed suite. | VERIFIED |
| R-024 | Persistent timestamps are unambiguous sortable integers. | schema + schedule/order tests. | VERIFIED |
| R-025 | Clock abstraction deterministically controls schedule/lease/retry/purge boundaries. | `ManualClock` across tests; no sleep-based correctness tests. | VERIFIED |
| R-026 | Crash recovery derives from persisted authoritative state. | active-lease restart recovery test. | VERIFIED |
| R-027 | Concurrent claim/expiry/idempotency/cancellation falsifiers execute under real threads. | concurrency test module (32 workers, 24 workers/120 jobs, scanners/races). | VERIFIED |
| R-028 | Structured logs, queue/lease/outcome/retry/dead-letter metrics, health endpoints. | logging/metrics implementation + live HTTP metrics test. | VERIFIED |
| R-029 | Trusted-local boundary stated; CLI binds loopback by default; inputs bounded; no HTTP filesystem paths. | code/docs/security review and tests. | VERIFIED |
| R-030 | Real reversible migrations and legacy-row upgrade test. | `0→1→2→1→0→2` executed test with v1 row. | VERIFIED |
| R-031 | Liveness differs from readiness; storage/schema failure makes readiness fail. | live HTTP health test with broken DB path. | VERIFIED |
| R-032 | Dockerfile is non-root and has healthcheck/persistent data path. | GitHub Actions run `38146181915` built and booted the real image and reached readiness. | VERIFIED |
| R-033 | Compose persists data and publishes host loopback only. | `docker compose config --quiet` and real Compose boot passed in run `38146181915`; committed mapping is `127.0.0.1`. | VERIFIED |
| R-034 | CI enforces quality, tests, compile, migrations, route consistency, manifest, wheel, Docker, Compose and container E2E. | GitHub Actions run `38146181915`: Python 3.11, Python 3.13 and container jobs all passed. | VERIFIED |
| R-035 | Documentation covers architecture, API, worker semantics, operations, development, security and contributing. | committed docs set. | VERIFIED |
| R-036 | Evidence assumptions/requirements/claims/failure ledger/real SHA-256 manifest exist. | Manifest verifier passed for all 54 authoritative files in both Python lanes of run `38146181915`. | VERIFIED |
| R-037 | No multi-node coordination claim. | README/architecture/claims ledger. | VERIFIED |
| R-038 | No unsupported performance claim. | claims ledger/README; no throughput/SLO assertion. | VERIFIED |
| R-039 | Corrupt/malformed persistent state cannot silently become executable where schema can prevent it. | SQLite CHECK falsifier for illegal state + strict lease metadata constraint. | VERIFIED |
| R-040 | Database unavailable/unwritable-like open failure is surfaced safely and readiness fails. | broken storage path health test. | VERIFIED |
| R-041 | Partial/ambiguous HTTP completion can be retried safely. | durable completion-receipt duplicate test and HTTP replay test. | VERIFIED |
| R-042 | Requeue cannot create attempt `max+1`. | schema bound + exhausted-budget requeue regression test. | VERIFIED |
| R-043 | Worker IDs are safe for path-based protocol. | bounded regex validation + regression test. | VERIFIED |
| R-044 | Authentication/authorization for untrusted-network exposure. | Explicitly outside trusted-local v0.1. | N/A |
| R-045 | Multi-node consensus/distributed leasing. | Explicitly outside single-server v0.1. | N/A |
| R-046 | Production-Proven status from real sustained deployment. | Requires external operational runtime evidence. | UNVERIFIED |
| R-047 | Commercial readiness including chosen license/support/auth posture. | No license/auth/support/release-signing decision selected. | UNVERIFIED |
