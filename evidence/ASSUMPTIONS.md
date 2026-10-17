# Assumption ledger

| ID | Statement | Criticality | Confidence | Affected surface | Invalidation trigger |
|---|---|---:|---:|---|---|
| A-001 | v0.1 is trusted-local/internal and has no built-in authentication. | HIGH | High | API, security, deployment | Requirement to expose directly to untrusted networks. |
| A-002 | Supported topology is exactly one server process + one local SQLite database + multiple concurrent workers. | HIGH | High | durability, leasing | Multi-server coordination requirement. |
| A-003 | Persistent timestamps use UTC Unix microseconds in SQLite INTEGER columns. | HIGH | High | scheduling, ordering, leases | Need for a different persistence compatibility contract. |
| A-004 | Lease validity is half-open: `now < expires_at`; equality means expired. | HIGH | High | heartbeat/completion/recovery | Contract change requested. |
| A-005 | A committed claim consumes one attempt even if the worker later crashes. | HIGH | High | attempts, retry, recovery | Different billing/attempt semantics requested. |
| A-006 | Explicit administrative requeue is allowed from cancelled/dead-lettered states; succeeded jobs stay final in v0.1. | HIGH | High | terminal safety | Requirement to replay successful jobs. |
| A-007 | Keyset pagination guarantees immutable creation-window traversal, not MVCC snapshot membership for mutable state filters. | MEDIUM | High | querying | Requirement for transaction-snapshot pagination. |
| A-008 | Docker process may bind all container interfaces; the supported Compose deployment publishes only to host loopback. | HIGH | High | trust boundary | Orchestrator/network model changes. |
| A-009 | ChronosQueue is distributed under a proprietary all-rights-reserved license; no open-source license is granted. | HIGH | High | licensing, commercial readiness | Copyright holder relicenses the project. |
| A-010 | Local filesystems with SQLite-compatible locking are the supported storage substrate; ambiguous network filesystem locking is out of scope. | HIGH | High | persistence | Need for NFS/shared-volume support. |
