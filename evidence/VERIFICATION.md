# Verification evidence

ChronosQueue treats verification as a property of a repository revision, not as the transcript of a particular implementation session. The authoritative automated gate is `.github/workflows/ci.yml`; the requirement and claim ledgers describe what those checks support.

## Required CI lanes

Every candidate revision is expected to pass all of the following:

- Python 3.11: package install, formatting/static sanity, bytecode compilation, full test suite, evidence-manifest verification, and wheel build.
- Python 3.13: the same package, quality, test, manifest, and wheel gates.
- Container: Compose validation, real image build, Compose boot, readiness, submit → claim → complete smoke, and clean shutdown.

The workflow runs on both pushes and pull requests. A green workflow is evidence for that exact commit only.

## Durable correctness evidence

The current suite directly exercises the product invariants represented in `REQUIREMENTS.md`, including:

- acknowledged jobs survive restart after durable commit;
- concurrent workers cannot hold overlapping valid leases for one job;
- stale lease holders cannot complete reassigned work;
- scheduling, lease expiry, heartbeat, retry and purge boundaries are deterministic under a manual clock;
- cancellation and completion races fail safely;
- completion replay is idempotent through durable receipts;
- attempt budgets remain bounded in code and schema;
- keyset pagination does not repeat records because of concurrent inserts;
- malformed input and invalid persistent-state combinations fail closed where the schema or API can enforce them;
- migrations are reversible across the supported schema path;
- runtime routes and the OpenAPI contract are checked for equivalence;
- the real container deployment reaches readiness and completes a submit → claim → complete smoke path.

The SHA-256 manifest is a separate integrity gate. `scripts/verify_manifest.py` must match every authoritative tracked file represented by `evidence/MANIFEST.sha256`; generated build/install artifacts are excluded by the manifest tooling.

## Evidence boundaries

The verified target is deliberately narrow: one server process, one persistent local SQLite database, multiple concurrent workers, and a trusted local/internal network.

A green repository CI run does **not** establish:

- production-proven operation under sustained real workloads;
- multi-node/distributed coordination;
- performance superiority or a throughput SLO;
- built-in security for direct exposure to an untrusted network;
- commercial readiness, support/SLA posture, or release-signing maturity;
- state-of-the-art code quality relative to external systems.

See `CLAIMS.md` for claim states and `ASSUMPTIONS.md` for the environmental and product assumptions that bound those claims.
