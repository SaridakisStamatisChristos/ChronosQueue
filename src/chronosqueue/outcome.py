from __future__ import annotations

import json
import sqlite3
from typing import Any

from .errors import ChronosError, Conflict, NotFound, StaleLease
from .models import JobState, RetryKind, RetryPolicy
from .state import validate_transition


class OutcomeMixin:
    def _finish(self, *, action: str, job_id: str, worker_id: str, lease_token: str, lease_generation: int,
                value: Any) -> dict[str, Any]:
        self._validate_lease_credentials(job_id, worker_id, lease_token, lease_generation)
        now = self.clock.now_us()
        value_json = self._bounded_json(value, self.config.max_result_bytes, "result" if action == "complete" else "error")
        try:
            with self.store.tx() as conn:
                receipt = conn.execute("SELECT * FROM completion_receipts WHERE lease_token=?", (lease_token,)).fetchone()
                if receipt is not None:
                    if receipt["job_id"] == job_id and receipt["worker_id"] == worker_id and receipt["lease_generation"] == lease_generation and receipt["action"] == action:
                        return json.loads(receipt["response_json"])
                    raise StaleLease("completion token has already been consumed")
                row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                if row is None:
                    raise NotFound("job not found")
                if not self._lease_matches(row, worker_id, lease_token, lease_generation, now):
                    raise StaleLease("lease is stale or no longer owned by this worker")
                if action == "complete":
                    validate_transition(JobState.LEASED, JobState.SUCCEEDED)
                    conn.execute(
                        """UPDATE jobs SET state='succeeded',result_json=?,error_json=NULL,terminal_at_us=?,updated_at_us=?,
                           lease_owner=NULL,lease_token=NULL,lease_expires_at_us=NULL WHERE id=?""",
                        (value_json, now, now, job_id),
                    )
                    self._event(conn, job_id, now, "completed", JobState.LEASED, JobState.SUCCEEDED)
                    self._metric(conn, "jobs_succeeded")
                else:
                    validate_transition(JobState.LEASED, JobState.FAILED)
                    self._event(conn, job_id, now, "failed_attempt", JobState.LEASED, JobState.FAILED,
                                {"attempt_count": row["attempt_count"]})
                    self._metric(conn, "jobs_failed")
                    if row["attempt_count"] >= row["max_attempts"]:
                        validate_transition(JobState.FAILED, JobState.DEAD_LETTERED)
                        conn.execute(
                            """UPDATE jobs SET state='dead-lettered',error_json=?,terminal_at_us=?,updated_at_us=?,
                               lease_owner=NULL,lease_token=NULL,lease_expires_at_us=NULL WHERE id=?""",
                            (value_json, now, now, job_id),
                        )
                        self._event(conn, job_id, now, "dead_lettered", JobState.FAILED, JobState.DEAD_LETTERED)
                        self._metric(conn, "jobs_dead_lettered")
                    else:
                        policy = RetryPolicy(RetryKind(row["retry_kind"]), row["retry_delay_ms"], row["retry_max_delay_ms"])
                        retry_at = now + policy.delay_for_attempt(row["attempt_count"]) * 1000
                        validate_transition(JobState.FAILED, JobState.SCHEDULED)
                        conn.execute(
                            """UPDATE jobs SET state='scheduled',scheduled_at_us=?,error_json=?,terminal_at_us=NULL,updated_at_us=?,
                               lease_owner=NULL,lease_token=NULL,lease_expires_at_us=NULL WHERE id=?""",
                            (retry_at, value_json, now, job_id),
                        )
                        self._event(conn, job_id, now, "retry_scheduled", JobState.FAILED, JobState.SCHEDULED,
                                    {"retry_at_us": retry_at})
                        self._metric(conn, "jobs_retried")
                fresh = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                response = self._row_to_job(fresh)
                conn.execute(
                    "INSERT INTO completion_receipts(lease_token,job_id,lease_generation,worker_id,action,response_json,created_at_us) VALUES(?,?,?,?,?,?,?)",
                    (lease_token, job_id, lease_generation, worker_id, action, self._dump(response), now),
                )
                return response
        except StaleLease:
            self._record_stale()
            raise

    def complete(self, **kwargs: Any) -> dict[str, Any]:
        return self._finish(action="complete", value=kwargs.pop("result", None), **kwargs)

    def fail(self, **kwargs: Any) -> dict[str, Any]:
        return self._finish(action="fail", value=kwargs.pop("error", None), **kwargs)

    def cancel(self, job_id: str) -> dict[str, Any]:
        now = self.clock.now_us()
        with self.store.tx() as conn:
            self._maintenance_tx(conn, now)
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound("job not found")
            before = JobState(row["state"])
            if before == JobState.CANCELLED:
                return self._row_to_job(row)
            if before in {JobState.SUCCEEDED, JobState.DEAD_LETTERED}:
                raise Conflict(f"cannot cancel terminal job in state {before}")
            validate_transition(before, JobState.CANCELLED)
            conn.execute(
                """UPDATE jobs SET state='cancelled',terminal_at_us=?,updated_at_us=?,lease_owner=NULL,lease_token=NULL,
                   lease_expires_at_us=NULL,lease_generation=lease_generation+1 WHERE id=?""",
                (now, now, job_id),
            )
            self._event(conn, job_id, now, "cancelled", before, JobState.CANCELLED)
            self._metric(conn, "jobs_cancelled")
            fresh = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row_to_job(fresh)

    def requeue(self, job_id: str, *, scheduled_at_us: int | None = None, reset_attempts: bool = True) -> dict[str, Any]:
        now = self.clock.now_us()
        scheduled = now if scheduled_at_us is None else scheduled_at_us
        if not isinstance(scheduled, int) or scheduled < 0:
            raise ChronosError("scheduled_at must be a non-negative timestamp")
        with self.store.tx() as conn:
            self._maintenance_tx(conn, now)
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound("job not found")
            before = JobState(row["state"])
            if before not in {JobState.DEAD_LETTERED, JobState.CANCELLED}:
                raise Conflict("explicit requeue is only allowed from dead-lettered or cancelled")
            if not reset_attempts and row["attempt_count"] >= row["max_attempts"]:
                raise Conflict("requeue must reset attempts when the prior attempt budget is exhausted")
            validate_transition(before, JobState.SCHEDULED, administrative=True)
            target = JobState.READY if scheduled <= now else JobState.SCHEDULED
            conn.execute(
                """UPDATE jobs SET state=?,scheduled_at_us=?,attempt_count=?,terminal_at_us=NULL,result_json=NULL,error_json=NULL,
                   lease_owner=NULL,lease_token=NULL,lease_expires_at_us=NULL,lease_generation=lease_generation+1,updated_at_us=? WHERE id=?""",
                (target, scheduled, 0 if reset_attempts else row["attempt_count"], now, job_id),
            )
            self._event(conn, job_id, now, "admin_requeue", before, target, {"reset_attempts": reset_attempts})
            fresh = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row_to_job(fresh)
