# Operations

## Deployment

The supported v0.1 topology is one ChronosQueue server with persistent local SQLite storage and multiple concurrent workers. Use a local filesystem suitable for SQLite locking; network filesystems with unclear locking semantics are outside the verified target.

CLI server default: `127.0.0.1:8080`. Compose publishes `127.0.0.1:8080:8080` and uses a named volume.

## Health

`/health/live` only proves the process can answer HTTP. `/health/ready` additionally opens a write transaction, verifies the exact schema version, and touches the health sentinel. Missing/unwritable storage or incompatible schema returns 503 readiness while liveness remains 200.

## Migrations

Schema version is stored in SQLite `PRAGMA user_version` and migrations are bundled as explicit up/down SQL files.

```bash
chronosqueue migrate --db ./data/chronosqueue.db
```

Downgrades can destroy schema/data and therefore require explicit authority:

```bash
chronosqueue migrate --db ./data/chronosqueue.db --target 1 --allow-destructive
```

CI exercises `0 -> 1 -> 2 -> 1 -> 0 -> 2` and verifies a legacy version-1 row survives the supported `1 -> 2` upgrade.

## Backup and recovery

For a consistent online backup, use Python's SQLite backup API against the database while the service remains running, or stop the service cleanly and copy the database after shutdown. Do not copy only the main `.db` file while WAL writes are active unless you also use a SQLite-aware backup procedure.

Recovery is: restore the database, start the same/newer compatible ChronosQueue version, run migrations, then wait for readiness. Active leases are not trusted merely because they existed in memory; persisted expiration timestamps govern recovery.

## Purge

Purge accepts an explicit terminal-time threshold and a bounded limit. It only deletes `succeeded`, `dead-lettered`, and `cancelled` jobs older than the threshold. Ready/scheduled/leased work cannot be purged through this endpoint.

## Metrics

`/metrics` exposes durable event counters plus current per-queue/state gauges and claim latency aggregate. No performance SLO is claimed in v0.1.

## Failure modes

SQLite write contention waits up to the configured busy timeout. A storage failure causes the affected request to fail and readiness to go unhealthy. Atomic SQLite transactions mean an acknowledged submit/claim/completion is committed; if the HTTP response is lost after commit, idempotency keys and completion receipts resolve ambiguity.
