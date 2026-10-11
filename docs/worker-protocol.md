# Worker protocol

## Claim

Workers call `POST /v1/worker/claim` with a path-safe worker ID, queue list, and optional lease duration. The server promotes due jobs and recovers expired leases inside the same write transaction before selecting one job.

A successful lease contains job ID, attempt count, lease owner, lease token, generation, and exact expiration. The lease interval is half-open: it becomes invalid exactly at expiration.

## Heartbeat

Heartbeat supplies job ID, lease token, and generation. Only the current owner/generation can renew, and an already-expired lease cannot be resurrected by heartbeat.

## Completion

Completion authorization is the tuple `(job_id, worker_id, lease_token, generation)`. A valid completion moves the job to `succeeded`, clears lease metadata, records a receipt, and commits before the HTTP success is written.

Retrying the same completion token returns the original receipt. Reusing the token for a different action is rejected. A token from an expired, cancelled, or reassigned lease is stale.

## Failure and retry

Workers report failure information but do not choose whether to bypass retry policy. The server records the `leased -> failed` transition in audit history, then in the same transaction either schedules a retry or dead-letters after the configured attempt budget is exhausted.

Fixed delay uses a constant delay. Exponential delay uses `base * 2^(attempt_count-1)` capped by `max_delay_ms`.

## Crash semantics

A claim is an attempt. If the worker crashes, that attempt remains counted. When its lease expires, the job is re-eligible if the attempt budget remains; final-attempt expiry dead-letters. Restart does not extend leases.
