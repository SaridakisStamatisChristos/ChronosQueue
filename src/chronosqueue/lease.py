from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from typing import Any, Sequence

from .errors import ChronosError, Conflict, NotFound, StaleLease
from .models import JobState
from .state import validate_transition


class LeaseMixin:
    def _maintenance_tx(self, conn: sqlite3.Connection, now: int) -> dict[str, int]:
        promoted_rows = conn.execute("SELECT id FROM jobs WHERE state='scheduled' AND scheduled_at_us<=?", (now,)).fetchall()
        if promoted_rows:
            conn.execute("UPDATE jobs SET state='ready',updated_at_us=? WHERE state='scheduled' AND scheduled_at_us<=?", (now, now))
            for row in promoted_rows:
                self._event(conn, row["id"], now, "schedule_due", JobState.SCHEDULED, JobState.READY)

        expired = conn.execute(
            "SELECT id,attempt_count,max_attempts FROM jobs WHERE state='leased' AND lease_expires_at_us<=? ORDER BY seq", (now,)
        ).fetchall()
        requeued = dead = 0
        for row in expired:
            if row["attempt_count"] >= row["max_attempts"]:
                validate_transition(JobState.LEASED, JobState.DEAD_LETTERED)
                conn.execute(
                    """UPDATE jobs SET state='dead-lettered',lease_owner=NULL,lease_token=NULL,lease_expires_at_us=NULL,
                       terminal_at_us=?,updated_at_us=?,error_json=? WHERE id=? AND state='leased'""",
                    (now, now, self._dump({"type": "lease_expired", "message": "worker lease expired on final attempt"}), row["id"]),
                )
                self._event(conn, row["id"], now, "lease_expired", JobState.LEASED, JobState.DEAD_LETTERED)
                self._metric(conn, "jobs_dead_lettered")
                dead += 1
            else:
                validate_transition(JobState.LEASED, JobState.READY)
                conn.execute(
                    """UPDATE jobs SET state='ready',lease_owner=NULL,lease_token=NULL,lease_expires_at_us=NULL,
                       updated_at_us=?,error_json=? WHERE id=? AND state='leased'""",
                    (now, self._dump({"type": "lease_expired", "message": "worker lease expired"}), row["id"]),
                )
                self._event(conn, row["id"], now, "lease_expired", JobState.LEASED, JobState.READY)
                requeued += 1
            self._metric(conn, "lease_expirations")
        return {"promoted": len(promoted_rows), "expired_requeued": requeued, "expired_dead_lettered": dead}

    def maintenance(self) -> dict[str, int]:
        with self.store.tx() as conn:
            return self._maintenance_tx(conn, self.clock.now_us())

    def claim(self, *, worker_id: str, queues: Sequence[str] | None = None, lease_ms: int | None = None) -> dict[str, Any] | None:
        if not isinstance(worker_id, str) or not re.fullmatch(r"[A-Za-z0-9._:@-]{1,128}", worker_id):
            raise ChronosError("worker_id must match [A-Za-z0-9._:@-]{1,128}")
        queues_v = [self._validate_queue(q) for q in (queues or ["default"])]
        if not queues_v or len(queues_v) > 64:
            raise ChronosError("claim requires 1..64 queues")
        duration = self.config.default_lease_ms if lease_ms is None else lease_ms
        if not isinstance(duration, int) or isinstance(duration, bool) or duration <= 0 or duration > self.config.max_lease_ms:
            raise ChronosError("lease_ms is out of range")
        start_ns = time.perf_counter_ns()
        now = self.clock.now_us()
        token = str(uuid.uuid4())
        with self.store.tx() as conn:
            self._maintenance_tx(conn, now)
            placeholders = ",".join("?" for _ in queues_v)
            row = conn.execute(
                f"""SELECT * FROM jobs WHERE state='ready' AND queue_name IN ({placeholders}) AND scheduled_at_us<=?
                     ORDER BY priority DESC, scheduled_at_us ASC, seq ASC LIMIT 1""",
                (*queues_v, now),
            ).fetchone()
            self._metric(conn, "claim_requests")
            latency_us = max(0, (time.perf_counter_ns() - start_ns) // 1000)
            self._metric(conn, "claim_latency_us_total", latency_us)
            if row is None:
                return None
            validate_transition(JobState.READY, JobState.LEASED)
            generation = row["lease_generation"] + 1
            expiry = now + duration * 1000
            result = conn.execute(
                """UPDATE jobs SET state='leased',attempt_count=attempt_count+1,lease_owner=?,lease_token=?,
                   lease_generation=?,lease_expires_at_us=?,updated_at_us=? WHERE id=? AND state='ready'""",
                (worker_id, token, generation, expiry, now, row["id"]),
            )
            if result.rowcount != 1:
                raise Conflict("claim lost a transactional race")
            self._event(conn, row["id"], now, "claimed", JobState.READY, JobState.LEASED,
                        {"worker_id": worker_id, "lease_generation": generation, "lease_expires_at_us": expiry})
            self._metric(conn, "jobs_leased")
            leased = conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
        return self._row_to_job(leased)

    def heartbeat(self, *, job_id: str, worker_id: str, lease_token: str, lease_generation: int, lease_ms: int | None = None) -> dict[str, Any]:
        self._validate_lease_credentials(job_id, worker_id, lease_token, lease_generation)
        duration = self.config.default_lease_ms if lease_ms is None else lease_ms
        if not isinstance(duration, int) or isinstance(duration, bool) or duration <= 0 or duration > self.config.max_lease_ms:
            raise ChronosError("lease_ms is out of range")
        now = self.clock.now_us()
        new_expiry = now + duration * 1000
        with self.store.tx() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound("job not found")
            if not self._lease_matches(row, worker_id, lease_token, lease_generation, now):
                raise StaleLease("lease is stale or no longer owned by this worker")
            conn.execute("UPDATE jobs SET lease_expires_at_us=?,updated_at_us=? WHERE id=?", (new_expiry, now, job_id))
            self._event(conn, job_id, now, "heartbeat", JobState.LEASED, JobState.LEASED,
                        {"worker_id": worker_id, "lease_generation": lease_generation, "lease_expires_at_us": new_expiry})
            fresh = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row_to_job(fresh)

    @staticmethod
    def _validate_lease_credentials(job_id: str, worker_id: str, lease_token: str, generation: int) -> None:
        try:
            uuid.UUID(job_id)
            uuid.UUID(lease_token)
        except (ValueError, AttributeError) as exc:
            raise ChronosError("job_id and lease_token must be UUIDs") from exc
        if not isinstance(worker_id, str) or not re.fullmatch(r"[A-Za-z0-9._:@-]{1,128}", worker_id):
            raise ChronosError("worker_id must match [A-Za-z0-9._:@-]{1,128}")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
            raise ChronosError("lease_generation must be a positive integer")

    @staticmethod
    def _lease_matches(row: sqlite3.Row, worker_id: str, lease_token: str, generation: int, now: int) -> bool:
        return (
            row["state"] == JobState.LEASED
            and row["lease_owner"] == worker_id
            and row["lease_token"] == lease_token
            and row["lease_generation"] == generation
            and row["lease_expires_at_us"] is not None
            and now < row["lease_expires_at_us"]
        )

    def _record_stale(self) -> None:
        with self.store.tx() as conn:
            self._metric(conn, "stale_completion_attempts")
