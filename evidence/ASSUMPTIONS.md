# Assumptions

- Deployment is one ChronosQueue server process backed by one local SQLite database, with multiple concurrent workers.
- The host filesystem and storage device honor SQLite locking and sync semantics.
- Wall-clock time can move in production; v0.1 uses persisted Unix microseconds and does not claim protection from arbitrary backward system-clock jumps. Deterministic fake time is used for correctness tests.
- Workers may crash, retry HTTP requests, and duplicate completion/failure calls.
- Authentication/TLS are outside v0.1 and loopback binding is the default safety boundary.
- External job side effects are not made exactly once by the queue.
- Licensing was not specified by the product challenge; no license grant is added automatically. This is a commercial-readiness decision, not a queue-correctness assumption.
