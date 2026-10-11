from __future__ import annotations

import json
import uuid
from typing import Any, Sequence

from .errors import ChronosError, Conflict
from .models import JobState


class SubmissionMixin:
    def submit(
        self,
        *,
        payload: Any,
        queue: str = "default",
        scheduled_at_us: int | None = None,
        delay_ms: int | None = None,
        priority: int = 0,
        tags: Sequence[str] | None = None,
        dedupe_key: str | None = None,
        max_attempts: int = 3,
        retry: dict[str, Any] | None = None,
        job_id: str | None = None,
    ) -> dict[str, Any]:
        queue = self._validate_queue(queue)
        payload_json = self._bounded_json(payload, self.config.max_payload_bytes, "payload")
        tags_v = self._validate_tags(tags)
        retry_v = self._validate_retry(retry)
        if not isinstance(priority, int) or isinstance(priority, bool) or not self.config.min_priority <= priority <= self.config.max_priority:
            raise ChronosError("priority is out of range")
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or not 1 <= max_attempts <= self.config.max_attempts:
            raise ChronosError("max_attempts is out of range")
        if dedupe_key is not None and (not isinstance(dedupe_key, str) or not dedupe_key or len(dedupe_key) > 256):
            raise ChronosError("dedupe_key must be a non-empty string of at most 256 characters")
        if delay_ms is not None and scheduled_at_us is not None:
            raise ChronosError("use either delay_ms or scheduled_at, not both")
        now = self.clock.now_us()
        if delay_ms is not None:
            if not isinstance(delay_ms, int) or isinstance(delay_ms, bool) or delay_ms < 0 or delay_ms > self.config.max_retry_delay_ms:
                raise ChronosError("delay_ms is out of range")
            scheduled = now + delay_ms * 1000
            schedule_fingerprint = {"mode": "delay", "delay_ms": delay_ms}
        elif scheduled_at_us is not None:
            scheduled = scheduled_at_us
            if not isinstance(scheduled, int) or isinstance(scheduled, bool) or scheduled < 0:
                raise ChronosError("scheduled_at must be a non-negative timestamp")
            schedule_fingerprint = {"mode": "absolute", "scheduled_at_us": scheduled}
        else:
            scheduled = now
            schedule_fingerprint = {"mode": "immediate"}
        jid = job_id or str(uuid.uuid4())
        try:
            uuid.UUID(jid)
        except (ValueError, AttributeError) as exc:
            raise ChronosError("job id must be a UUID") from exc
        state = JobState.READY if scheduled <= now else JobState.SCHEDULED
        request_shape = {
            "queue": queue, "schedule": schedule_fingerprint, "payload": json.loads(payload_json), "priority": priority,
            "tags": tags_v, "max_attempts": max_attempts,
            "retry": {"kind": retry_v.kind, "delay_ms": retry_v.delay_ms, "max_delay_ms": retry_v.max_delay_ms},
        }
        fp = self._fingerprint(request_shape)
        with self.store.tx() as conn:
            if dedupe_key is not None:
                old = conn.execute("SELECT * FROM jobs WHERE queue_name=? AND dedupe_key=?", (queue, dedupe_key)).fetchone()
                if old is not None:
                    if old["request_fingerprint"] != fp:
                        raise Conflict("idempotency key was already used with different job semantics")
                    return self._row_to_job(old, deduplicated=True)
            existing_id = conn.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
            if existing_id is not None:
                if existing_id["request_fingerprint"] == fp:
                    return self._row_to_job(existing_id, deduplicated=True)
                raise Conflict("job id already exists with different job semantics")
            conn.execute(
                """INSERT INTO jobs(id,queue_name,created_at_us,scheduled_at_us,payload_json,state,max_attempts,
                   retry_kind,retry_delay_ms,retry_max_delay_ms,priority,tags_json,dedupe_key,request_fingerprint,updated_at_us)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (jid, queue, now, scheduled, payload_json, state, max_attempts, retry_v.kind, retry_v.delay_ms,
                 retry_v.max_delay_ms, priority, self._dump(tags_v), dedupe_key, fp, now),
            )
            conn.executemany("INSERT INTO job_tags(job_id,tag) VALUES(?,?)", [(jid, tag) for tag in tags_v])
            self._event(conn, jid, now, "submitted", None, state)
            self._metric(conn, "jobs_submitted")
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
        return self._row_to_job(row)
