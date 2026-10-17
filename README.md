# ChronosQueue

ChronosQueue is a local-first durable job queue and task scheduler for software teams. It runs as one server process backed by SQLite and supports multiple concurrent workers through a versioned REST API, a CLI, and a lease-based worker protocol.

The v0.1 deployment target is deliberately **single server + persistent local storage + concurrent workers**. It does not claim multi-node coordination.

## Properties

- A successful submit is acknowledged only after the SQLite transaction commits.
- Claim uses `BEGIN IMMEDIATE` and a single authoritative database to prevent overlapping active leases.
- Lease validity is half-open: `issued_at <= now < expires_at`. At exact expiry the lease is stale.
- Worker completion is authorized by worker ID + opaque lease token + monotonically increasing lease generation.
- Completion/failure requests are idempotent through durable completion receipts.
- Fixed and exponential retries survive restart; attempt budget is enforced in code and schema.
- Delayed jobs cannot become claimable before their stored microsecond UTC schedule.
- Cancelled and dead-lettered jobs require an explicit administrative requeue. Succeeded jobs cannot be requeued in v0.1.
- Pagination is cursor/keyset based and pins an immutable maximum creation sequence so concurrent inserts do not repeat or contaminate an in-progress traversal.
- JSON logs, Prometheus text metrics, readiness/liveness, audit events, and active-lease inspection are built in.

## Quick start

```bash
python -m pip install .
chronosqueue serve
```

The CLI server defaults to `127.0.0.1:8080` and `./data/chronosqueue.db`.

Submit and process a job:

```bash
chronosqueue submit --payload '{"task":"index","repo":"example"}' --queue builds --max-attempts 3
chronosqueue claim --worker worker-1 --queue builds
chronosqueue complete --worker worker-1 --job <JOB_ID> --token <LEASE_TOKEN> --generation <GENERATION> --result '{"ok":true}'
```

Use `--json` for automation. Set `CHRONOSQUEUE_URL` to point the CLI at another server.

## Docker Compose

```bash
docker compose up --build -d
curl --fail http://127.0.0.1:8080/health/ready
```

Compose publishes only on host loopback by default and stores SQLite state in a named volume. The process inside the container binds `0.0.0.0` so the container port is reachable through the loopback-only host mapping; the image itself does not publish a host port.

## Development verification

```bash
make quality
make test
python scripts/generate_manifest.py
python scripts/verify_manifest.py
```

See `docs/architecture.md`, `docs/worker-protocol.md`, `docs/operations.md`, and the `evidence/` directory for the exact guarantees, limitations, and verification model.

## Evidence model

The active `evidence/` directory contains durable product assurance material: requirements, assumptions, supported claims, the current verification contract, and a SHA-256 manifest of authoritative repository files. One-off implementation-session chronology is intentionally left to Git history instead of being presented as current product evidence.

See `evidence/VERIFICATION.md` for the required verification lanes and evidence boundaries. `python scripts/verify_manifest.py` fails closed if an authoritative tracked file differs from the recorded manifest.

## Trust boundary

v0.1 has **no authentication or authorization**. It is intended for trusted local/internal use and binds to loopback by default outside a container. Do not expose it to an untrusted network without an authenticating reverse proxy or a future authenticated release. See `SECURITY.md`.

## License

ChronosQueue is currently **proprietary software — all rights reserved**. No open-source license is granted. See [`LICENSE`](LICENSE) for the current terms. The copyright holder may relicense the project under different terms in the future.
