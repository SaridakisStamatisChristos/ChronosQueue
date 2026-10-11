# Security policy and trust boundary

ChronosQueue v0.1 is an unauthenticated trusted-local/internal service. The CLI server binds to `127.0.0.1` by default. Docker Compose publishes only to host loopback. Do not bind to a public interface or place the API on an untrusted network without an authenticating/authorizing reverse proxy.

Security-relevant implementation controls include bounded HTTP bodies, finite JSON enforcement, bounded payload/results, validated worker IDs and UUID lease credentials, parameterized SQL, no HTTP-controlled filesystem paths, safe generic 500 responses, generation-bound leases, durable idempotency receipts, and strict state-transition rules.

Secrets are not required by the base product and none are embedded in the repository.

## Reporting

Report suspected vulnerabilities privately to the repository owner rather than opening a public exploit issue. Include a minimal reproducer, affected version/commit, and impact. No response-time SLA is asserted by this repository.
