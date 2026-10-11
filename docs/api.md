# HTTP API

The committed machine-readable contract is [`../openapi.json`](../openapi.json). The service binds to `127.0.0.1:8080` by default.

## Submission

`POST /v1/jobs` supports immediate, `delay_ms`, or absolute offset-aware `scheduled_at`; queue, signed 32-bit priority, tags, arbitrary JSON payload, idempotency key, maximum attempts, and fixed/exponential retry delay. Duplicate `(queue, idempotency_key)` submissions return the already accepted retained job and do not create another row. The key remains reserved until that job is purged. Callers should treat an idempotency key as the identity of one logical submission.

## Querying

`GET /v1/jobs` supports queue, state, tag, priority, creation-time bounds, lease owner, limit, and opaque cursor filters. The first page captures an immutable creation-sequence ceiling; later inserts are excluded from that traversal. State/tag predicates remain live across pages, as documented in the architecture.

## Administration

Single-job cancellation and requeue use `/v1/jobs/{id}/cancel` and `/v1/jobs/{id}/requeue`. Batch requeue is an all-or-nothing operation over explicitly selected cancelled/dead-lettered IDs at `POST /v1/admin/requeue`. Purge deletes only terminal jobs strictly older than the supplied threshold and is bounded by a caller-supplied limit.

## Errors

Errors are JSON objects with `error` and `message`. Validation failures use 422, missing resources 404, illegal/stale transitions 409, readiness/storage failures 503, and unexpected errors are reduced to a safe generic 500. Backend storage details are not returned to clients.
