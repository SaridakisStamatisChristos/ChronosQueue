# REST API

The committed contract is `src/chronosqueue/openapi.yaml`, also served at `GET /openapi.yaml`. CI compares the declared route/method set with the runtime route registry.

Important endpoints:

- `POST /v1/jobs` — submit immediate, delayed, or absolute-schedule jobs.
- `GET /v1/jobs` — keyset list with queue/state/tag/time/priority/worker filters.
- `GET /v1/jobs/{id}` — inspect a job.
- `GET /v1/jobs/{id}/events` — audit transition history.
- `POST /v1/jobs/{id}/cancel` — cancel scheduled, ready, or leased work.
- `POST /v1/jobs/{id}/requeue` — explicit requeue of cancelled/dead-lettered work.
- `POST /v1/worker/claim` — atomically claim work from one or more queues.
- `POST /v1/worker/{id}/heartbeat` — renew only the current live lease generation.
- `POST /v1/worker/{id}/complete` / `fail` — idempotent worker outcome requests.
- `GET /v1/queues`, `GET /v1/leases` — queue and active lease observability.
- `POST /v1/admin/purge` — bounded deletion of old terminal jobs only.
- `/health/live`, `/health/ready`, `/metrics` — operations endpoints.

All JSON-facing numeric values must be finite. Payload/result size limits are enforced after canonical JSON encoding. Error responses have `{ "error": { "code", "message", "details" } }` and do not expose tracebacks.

A client that receives an ambiguous response to submission should retry with the same `dedupe_key` or client-supplied job UUID. A client that receives an ambiguous completion response should retry with the same lease token and generation; the durable receipt returns the original committed outcome.
