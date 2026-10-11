# Worker protocol

Workers poll `POST /v1/worker/claim` with a worker ID, one or more queues, and a positive bounded lease duration. A successful response contains the job and an opaque `lease_token`.

The token is authorization for **that lease generation only**. A heartbeat may extend only the current unexpired lease. Success and failure use the same worker ID and token. At the exact expiration timestamp the lease is stale.

Successful completion is idempotent by consumed lease token: a network retry of the same completion is acknowledged without applying the state mutation twice. Failure requests are also receipt-protected; workers cannot choose whether a failure retries or dead-letters—the server applies persisted retry policy.

Execution is at-least-once across crash/reassignment boundaries. ChronosQueue prevents stale workers from mutating queue state, but it cannot make an arbitrary external side effect exactly once. Job handlers should use application-level idempotency when required.
