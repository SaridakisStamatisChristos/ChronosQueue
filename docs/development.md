# Development and verification

```bash
python -m pip install --no-build-isolation .
python -m compileall -q src tests scripts
python -W error::ResourceWarning -m unittest discover -s tests -v
python scripts/check_repo.py
python scripts/verify_manifest.py
```

The concurrency suite deliberately races 20+ workers on one job and 24 workers over many jobs. Fake time drives scheduled execution, exact lease expiry, and retry boundaries deterministically.

CI additionally validates Compose, builds the real image, boots it, waits for readiness, and executes submit → claim → complete against the container.

## Local evidence environment

The R7 build campaign executed the full suite under CPython 3.13 in a Linux container environment. Docker itself was not installed in that environment, so Docker/Compose execution is intentionally delegated to GitHub Actions and remains unverified until that workflow is green.
