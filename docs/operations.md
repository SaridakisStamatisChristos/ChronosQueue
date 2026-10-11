# Operations

## Storage

Keep the SQLite database on a local filesystem with reliable fsync semantics. WAL and `synchronous=FULL` are enabled. Network filesystems with ambiguous locking/durability semantics are outside the supported v0.1 deployment model.

## Backup

Use SQLite's online backup API or a storage snapshot mechanism known to be SQLite-safe. Do not copy only the main database file while a live WAL is active unless the backup procedure explicitly accounts for the WAL.

## Recovery

On restart, authoritative state is read from SQLite. Expired leases are reconciled transactionally on subsequent operational calls. Readiness fails on schema-version mismatch or persisted lease/terminal invariant corruption detectable by the readiness audit.

## Migrations

`chronosqueue migrate` upgrades to the current schema. Downgrades are destructive and require `--allow-destructive-downgrade`. Test downgrades on backups/copies before operational use.

## Exposure

The default host server is loopback-only. The container listens on all interfaces **inside its network namespace** so Docker forwarding works, while Compose publishes the port only on host `127.0.0.1`. There is no built-in authentication or TLS in v0.1. Wider exposure requires an authenticated trusted proxy or another explicit trust boundary.

## Crash/response ambiguity semantics

Every state mutation is one SQLite write transaction. An interruption before commit leaves the prior durable state; an interruption after commit leaves the new durable state even if the HTTP response is lost. Operation-specific consequences:

| Operation | Lost before commit | Commit succeeded but response lost |
|---|---|---|
| submit | no accepted job | retry with the same idempotency key returns the retained job; without a key the client must reconcile |
| claim | job remains claimable | lease remains active until completion/heartbeat/expiry; a worker that lost the token must let it expire |
| heartbeat | previous expiry remains | retry with the same current token is safe while the lease remains current |
| complete | job remains leased | same completion token is idempotently accepted |
| fail/retry scheduling | job remains leased | consumed lease receipt prevents double failure application |
| cancel | prior state remains | repeated cancel of an already-cancelled job is harmless |
| requeue | terminal state remains | retry may receive transition conflict; operator can reconcile with GET |
| purge | rows remain | repeating the same threshold is safe and deletes only remaining qualifying terminal rows |

Test-only pre-commit failpoints exercise these rollback boundaries without changing production behavior.
