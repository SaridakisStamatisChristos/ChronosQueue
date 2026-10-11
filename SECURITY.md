# Security policy and trust boundary

ChronosQueue v0.1 is intended for trusted-local/internal use. It has **no authentication or TLS**. The host server binds to loopback by default. The container listens on `0.0.0.0` inside its isolated network namespace, but Docker Compose publishes only host `127.0.0.1:8080`. Do not expose it directly to untrusted networks.

External input is JSON-size bounded; queue, worker, tag, retry, priority, lease, cursor, and identifier fields are validated. Clients cannot supply filesystem paths. SQLite statements use parameters for runtime values. Lease tokens are capability secrets and are not exposed by normal job-query responses.

Report vulnerabilities privately to the repository owner. Do not include live secrets, production payloads, or exploit data from third parties in public issues.
