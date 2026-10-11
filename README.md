# ChronosQueue

ChronosQueue is a local-first durable job queue and task scheduler for software teams. It is deliberately scoped to **one server process + one persistent SQLite database + many concurrent workers**. It does not claim multi-node coordination.

## Guarantees in the declared deployment model

- acknowledged submissions are committed before success is returned;
- one active lease per job, enforced by serialized SQLite write transactions;
- lease expiry and crash recovery are derived from persisted state;
- scheduled jobs are not claimable before their exact integer-microsecond deadline;
- fixed and exponential retries persist across restart and stop at `max_attempts`;
- completion is authorized by an opaque lease token and stale workers are rejected;
- cancellation, completion, lease expiry, heartbeat, and requeue transitions are transactional;
- deterministic claim ordering: **priority DESC → available time ASC → creation sequence ASC**;
- keyset pagination avoids offset drift under concurrent inserts;
- corrupt persisted invariants fail readiness instead of becoming executable.

SQLite is configured with WAL, foreign keys, `synchronous=FULL`, and a busy timeout. Guarantees remain bounded by SQLite, the host filesystem, and the storage device honoring sync semantics.

## Quick start

```bash
python -m pip install .
chronosqueue migrate --db ./data/chronosqueue.db
chronosqueue serve --db ./data/chronosqueue.db
```

The server binds to `127.0.0.1:8080` by default. There is **no authentication in v0.1**; do not bind publicly unless an authenticated trusted proxy or equivalent boundary is added.

In another shell:

```bash
chronosqueue submit --queue build --payload '{"repo":"demo"}' --max-attempts 4 --retry-kind exponential --retry-delay-ms 500
chronosqueue claim --worker worker-1 --queue build --lease-ms 30000 --json
chronosqueue complete --worker worker-1 --job JOB_ID --lease-token LEASE_TOKEN --data '{"artifact":"ok"}'
chronosqueue list --queue build --json
```

Delayed and absolute scheduling are supported:

```bash
chronosqueue submit --queue reports --payload '{}' --delay-ms 60000
chronosqueue submit --queue reports --payload '{}' --scheduled-at 2026-10-12T08:00:00+03:00
```

## Docker Compose

```bash
docker compose up --build
```

Compose publishes only `127.0.0.1:8080` by default and stores SQLite data in a named volume. The image runs directly from the stdlib-only source tree and does not need package downloads at runtime.

## Worker lifecycle

1. `POST /v1/worker/claim` returns a job plus a secret `lease_token`.
2. The worker may renew only that current lease generation.
3. Success/failure requires the same worker ID and lease token.
4. If the lease expires, maintenance recovers the job. A later worker receives a new token/generation.
5. Any old worker is permanently stale for that generation.

A duplicate completion request with the same completed lease token is idempotently accepted without applying the completion twice.

## State model

```mermaid
stateDiagram-v2
    [*] --> scheduled
    [*] --> ready
    scheduled --> ready: due
    ready --> leased: claim
    leased --> succeeded: complete
    leased --> failed: failure + retry remaining
    leased --> ready: lease expiry + attempts remaining
    leased --> dead-lettered: failure/expiry at max attempts
    leased --> cancelled: cancel wins race
    failed --> ready: retry deadline
    failed --> cancelled
    scheduled --> cancelled
    ready --> cancelled
    cancelled --> scheduled: explicit admin requeue
    dead-lettered --> scheduled: explicit admin requeue
```

`failed` is a persisted retry-waiting state. `succeeded`, `cancelled`, and `dead-lettered` are terminal unless an explicitly allowed administrative requeue is invoked. `succeeded` cannot be requeued in v0.1.

## API and documentation

- OpenAPI: [`openapi.json`](openapi.json)
- Architecture: [`docs/architecture.md`](docs/architecture.md)
- API semantics: [`docs/api.md`](docs/api.md)
- Worker protocol: [`docs/worker-protocol.md`](docs/worker-protocol.md)
- Operations / backup / recovery: [`docs/operations.md`](docs/operations.md)
- Development / verification: [`docs/development.md`](docs/development.md)
- Security boundary: [`SECURITY.md`](SECURITY.md)
- Executed evidence: [`evidence/`](evidence/)

## Verification

```bash
PYTHONPATH=src python -W error::ResourceWarning -m unittest discover -s tests -v
PYTHONPATH=src python scripts/check_repo.py
python scripts/check_format.py
python scripts/static_check.py
python scripts/verify_manifest.py
```

CI additionally validates Docker Compose, builds the production image, boots it, waits for readiness, and executes a real submit → claim → complete smoke workflow.

## Current limits

ChronosQueue v0.1 intentionally has no authentication, TLS termination, multi-node leader election, cross-host distributed lease coordinator, replicated storage, or exactly-once execution guarantee. Worker execution is **at least once across crashes**; completion mutation is idempotent per lease token, but external side effects performed by jobs must use their own idempotency strategy.

Production-proven and commercial-readiness claims require evidence beyond repository tests; see `evidence/CLAIMS.md` and `evidence/FINAL_AUDIT.md`.
