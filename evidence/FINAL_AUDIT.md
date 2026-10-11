# Final falsification gate — local execution

1. **Can two workers ever hold a valid lease simultaneously?** No counterexample found. SQLite write serialization plus 32-worker and many-job races yielded one lease per job.
2. **Can a stale worker complete a reassigned job?** Falsified attempt rejected at exact expiry/reassignment and by generation/token mismatch.
3. **Can a scheduled job execute early?** Exact `T-1µs` claim returns none; at `T` it becomes eligible.
4. **Can retries exceed maximum?** Product defect F-003 was found and repaired; code plus schema now enforce the bound.
5. **Can cancellation resurrect a job?** No observed path. Cancellation clears/invalidate lease; maintenance never promotes terminal states.
6. **Can restart lose an acknowledged job?** Restart test retained submitted job; SQLite transaction commit precedes acknowledgement.
7. **Can pagination repeat or offset-skip because of concurrent inserts?** Concurrent inserts are excluded by snapshot max sequence and cursor traversal had no repeats. Mutable state-filter membership is not claimed as MVCC snapshot isolation.
8. **Can malformed/corrupted state become executable?** Illegal state insertion is rejected by schema; non-leased rows cannot carry active lease metadata.
9. **Can Docker default unintentionally expose the service?** Compose artifact maps host `127.0.0.1` only. Real Compose validation remains pending CI.
10. **Can documentation overclaim guarantees?** Docs explicitly limit topology, trust boundary, pagination snapshot semantics, and do not claim production-proven/SOTA/performance superiority.

Local gate conclusion: **code/invariant evidence is strong; container and GitHub CI execution remain open evidence lanes.**
