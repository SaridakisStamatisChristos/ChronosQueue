# Development

ChronosQueue has no third-party runtime dependencies.

```bash
python -m pip install -e . --no-build-isolation
make quality
make test
```

The test suite uses deterministic manual clocks instead of timing sleeps for schedule, lease, retry, and expiry boundaries. Concurrency falsifiers use real threads and independent SQLite connections.

`make quality` performs dependency-free formatting hygiene, AST checks, and bytecode compilation. `make test` runs unit, integration, HTTP, migration, crash-recovery, concurrency, pagination, and adversarial tests.

Before changing evidence-tracked files:

```bash
python scripts/generate_manifest.py
python scripts/verify_manifest.py
```

Docker verification is intentionally performed against the real built image in CI: Compose validation, image build, boot, readiness wait, and submit→claim→complete smoke.
