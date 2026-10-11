# Claims ledger

## Supported by executed repository evidence

- Single-server transactional leasing with SQLite writer serialization.
- No duplicate active lease observed under the required one-job and many-job concurrency falsifiers.
- Stale completion and stale heartbeat are rejected at/after exact expiry, including true concurrent reassignment races.
- Fixed/exponential retry state and dead-letter limits persist across restart.
- Pre-commit interruption rolls back atomically across every required mutation class.
- Insert-stable keyset pagination uses a first-page sequence ceiling and survived concurrent insert/state-transition churn.
- Loopback-only host exposure is the default; Compose publishes only host loopback.
- CLI and REST lifecycle workflows execute successfully outside direct Store calls.

## Explicit non-claims

- No multi-node coordination.
- No exactly-once execution of external side effects.
- No protection from a malicious trusted-local peer; built-in authentication is absent.
- No immunity to arbitrary backwards system-clock jumps.
- No production-proven availability, durability-under-hardware-failure, RTO/RPO, or SLO claim.
- No SOTA performance claim.
- No commercial-readiness claim.
