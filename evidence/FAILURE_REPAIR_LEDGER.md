# Failure / repair ledger

Failures are retained rather than erased from the development record.

## F-001 — purge falsifier selected wrong job

- Reproducer: adversarial purge test created two equal-priority ready jobs, called `claim()`, then assumed the second-created job was leased.
- Classification: **test oracle defect**, not product defect; ChronosQueue correctly used FIFO sequence ordering.
- Earliest invalidated phase: VERIFY/FALSIFY test construction.
- Repair: make the intended completion target deterministically higher priority.
- Reverification: full suite passed.

## F-002 — immediate idempotency fingerprint depended on current time

- Reproducer: submit immediate job with dedupe key, advance deterministic clock, retry identical request.
- Violated invariant: duplicate HTTP submission after ambiguous response must be idempotent.
- Root cause: request fingerprint included the resolved absolute `scheduled_at_us=now` rather than the caller's scheduling intent.
- Earliest invalidated phase: SPEC/GENERATE (idempotency semantics).
- Repair: fingerprint `{mode: immediate}`, `{mode: delay, delay_ms}`, or explicit absolute schedule; add client-supplied job-ID replay equivalence.
- Regression: `test_immediate_idempotency_replay_after_time_advance` and client-ID replay test.
- Reverification: full suite passed.

## F-003 — exhausted attempt budget could be preserved by admin requeue

- Reproducer: dead-letter after final attempt, call administrative requeue with `reset_attempts=false`, then claim would have been able to exceed the intended budget.
- Violated invariant: retries/claims must never exceed maximum attempts absent an explicit fresh budget.
- Earliest invalidated phase: SPEC/GENERATE.
- Repair: reject keep-attempts requeue when `attempt_count >= max_attempts`; add schema CHECK `attempt_count <= max_attempts`.
- Regression: `test_exhausted_attempt_budget_cannot_be_kept_on_requeue`.
- Reverification: full suite passed.

## F-004 — worker identifier accepted path-unsafe characters

- Reproducer: claim accepted worker ID containing `/`, while completion protocol embeds worker ID in the URL path.
- Classification: public-contract mismatch/input-validation defect.
- Earliest invalidated phase: SPEC/INTEGRATE.
- Repair: constrain worker IDs to `[A-Za-z0-9._:@-]{1,128}` and document OpenAPI pattern.
- Regression: `test_worker_identifier_is_path_safe`.
- Reverification: full suite passed.

## F-005 — repair edit temporarily introduced API indentation error

- Reproducer: `python -m py_compile` failed after a scripted timestamp-validation edit.
- Classification: generation/repair tooling error, caught before repository integration.
- Earliest invalidated phase: GENERATE.
- Repair: replace the affected submit/requeue dispatch blocks coherently instead of incremental textual nesting.
- Reverification: bytecode compilation and full suite passed.

No unresolved code-quality blocker is known at local evidence closure. Docker/CI evidence remains a separate pending gate until executed.

## F-006 — bootstrap archive transport was corrupted

- Reproducer: GitHub Actions R7 Bootstrap runs `38141739368`, `38142056768`, and `38142199929` failed before source integration; text-chunk reconstruction produced gzip CRC/length errors and the first large binary connector transfer did not match its local SHA-256.
- Classification: repository-delivery/transport defect, not a ChronosQueue runtime defect.
- Earliest invalidated phase: INTEGRATE/PACKAGE.
- Repair: abandon archive-as-source transport and construct the final Git tree from independently addressable source files; retain checksum verification for the repository evidence manifest.
- Reverification: final source revision must pass the normal CI workflow; failed bootstrap runs remain visible in Actions history.

## F-007 — editable-install metadata leaked into evidence manifest

- Reproducer: `src/chronosqueue.egg-info/` appeared in a locally regenerated manifest after editable/package installation.
- Classification: packaging/repository-hygiene defect.
- Earliest invalidated phase: PACKAGE/MANIFEST.
- Repair: remove generated egg-info from the source tree; `.gitignore` already excludes `*.egg-info/`; regenerate and verify the manifest from the clean source set.
- Reverification: local quality gate, 40-test suite, and manifest verification passed after cleanup.

## F-008 — service-module split omitted worker regex import

- Reproducer: post-split suite failed claims with `NameError: re is not defined` in `worker.py`.
- Classification: integration/refactor defect, caught before Git branch replacement.
- Earliest invalidated phase: INTEGRATE.
- Repair: add the required `re` import to the worker transition module.
- Regression/evidence: the existing worker-ID and full concurrency suites exercise the path.
- Reverification: full local suite rerun after repair.

## F-009 — second module split dropped QueueConfig dataclass decorator

- Reproducer: payload-bound test failed because `QueueConfig(max_payload_bytes=32)` no longer accepted constructor fields after extraction.
- Classification: refactor/extraction defect caught before branch replacement.
- Earliest invalidated phase: INTEGRATE.
- Repair: restore `@dataclass(frozen=True)` on the extracted configuration type.
- Reverification: full local suite rerun after repair.

## F-010 — worker split omitted completion-receipt JSON import

- Reproducer: duplicate completion replay raised `NameError: json is not defined` in extracted `outcome.py`.
- Classification: refactor/integration defect caught before branch replacement.
- Earliest invalidated phase: INTEGRATE.
- Repair: restore `json` dependency in outcome module.
- Reverification: full local suite rerun after repair.

## F-011 — lease extraction omitted rare-path exception imports

- Reproducer: structural review of extracted `lease.py` showed `Conflict` and `NotFound` references without local imports; the common success path did not exercise the missing-job branch.
- Classification: refactor/integration defect discovered by adversarial code review.
- Earliest invalidated phase: INTEGRATE/FALSIFY.
- Repair: restore both imports and add a missing-job heartbeat regression proving the path raises `NotFound`, not `NameError`.
- Reverification: full local suite rerun after repair.

## F-012 — installed-package metadata contaminated CI manifest file-set check

- Reproducer: CI run `38143259452` passed package install, quality, all 41 tests, and the full real-container lane, but both Python matrix jobs failed manifest verification because `pip install .` generated untracked `src/chronosqueue.egg-info/*` after checkout.
- Classification: evidence-verifier/environment-artifact defect; runtime, concurrency, migration, and container checks had passed.
- Earliest invalidated phase: MANIFEST/EVIDENCE.
- Repair: manifest generation and verification explicitly ignore generated `*.egg-info` directories, matching the repository `.gitignore` source boundary.
- Regression/evidence: reproduce by installing the package in-tree before manifest verification; verifier must still validate the 54 authoritative source/evidence files.
- Reverification: quality + full 41-test suite + post-install manifest verification rerun locally; final GitHub CI rerun required.

## F-013 — GitHub source blob differed from locally verified source bytes

- Reproducer: CI run `38143479074` passed package install, quality, all 41 tests, and the full container lane, then the evidence manifest reported a SHA-256 mismatch for `src/chronosqueue/base.py`.
- Classification: Git-tree integration/evidence mismatch; semantic tests remained green, but repository byte identity was not proven.
- Earliest invalidated phase: INTEGRATE / MANIFEST-EVIDENCE.
- Repair: compare remote Git blob identities against the locally verified source set; replace `base.py` with a Git blob created from the exact local bytes and leave all matching blobs untouched.
- Reverification: local quality, 41-test suite, and manifest verification rerun before commit; final GitHub CI required to independently close the gate.

## F-014 — manifest retained pre-integration hash for storage module

- Reproducer: CI run `38143853890` passed package installation, quality checks, all 41 tests on Python 3.11/3.13, and the complete real-container workflow, but manifest verification reported `hash mismatch: src/chronosqueue/storage.py`.
- Classification: Git-tree integration/evidence mismatch; queue runtime semantics remained green, but the evidence manifest no longer described the exact repository bytes.
- Earliest invalidated phase: MANIFEST/EVIDENCE.
- Repair: replace the stale `storage.py` digest in `evidence/MANIFEST.sha256` with the SHA-256 of the repository source bytes and record this failure without altering queue behavior.
- Regression/evidence: the existing manifest verifier remains fail-closed and checks every authoritative source/evidence file after package installation.
- Reverification: GitHub CI must independently pass both Python verification lanes and the container lane before this gate is closed.

## F-015 — Python 3.13 wheel gate assumed globally installed setuptools

- Reproducer: CI run `38146002036` passed package install, quality, all 41 tests, manifest verification, and the complete container lane; Python 3.13 then failed only at `pip wheel --no-build-isolation` with `BackendUnavailable: Cannot import 'setuptools.build_meta'`.
- Classification: CI packaging-environment defect, not a ChronosQueue runtime defect.
- Earliest invalidated phase: PACKAGE / CI.
- Repair: build the verification wheel with normal PEP 517 build isolation, matching the already successful package-install path and the declared `pyproject.toml` build requirements.
- Regression/evidence: Python 3.11 and 3.13 must both build the wheel in CI after all tests and manifest verification.
- Reverification: final GitHub CI rerun required.

## Evidence closure — run 38146181915

- GitHub Actions run `38146181915` on executable verification revision `f1a87f60e5f65bb606d90b798d242f7102853e90` passed the Python 3.11, Python 3.13, and real-container jobs.
- Both Python lanes passed package installation, quality/compile checks, all 41 tests, SHA-256 manifest verification, and wheel build.
- The container lane passed Compose validation, real image build, Compose boot, readiness, submit → claim → complete smoke, and shutdown.
- This closes the pending reverification items in F-012 through F-015. Historical failed runs remain part of the ledger and are not erased.

## F-016 — evidence closure introduced trailing whitespace

- Reproducer: CI run `38146489106` failed the formatting/static sanity gate on `evidence/FINAL_AUDIT.md:3` before the test suite because the closure document used a Markdown hard-break with trailing spaces.
- Classification: evidence/documentation quality defect; no runtime queue semantics were implicated.
- Earliest invalidated phase: MANIFEST/EVIDENCE / FINAL AUDIT.
- Repair: remove the trailing whitespace and refresh the evidence manifest while retaining the same closure claims.
- Regression/evidence: the repository quality checker must pass on both Python lanes before tests; the full CI workflow is rerun rather than accepting a documentation-only exception.
- Reverification: final GitHub CI rerun required.
