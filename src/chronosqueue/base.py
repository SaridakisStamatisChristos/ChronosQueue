from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Sequence

from .clock import Clock, SystemClock, us_to_rfc3339
from .errors import ChronosError, PayloadTooLarge
from .models import JobState, RetryKind, RetryPolicy
from .storage import QueueConfig, SQLiteStore


class QueueServiceBase:
    def __init__(self, db_path: str, *, clock: Clock | None = None, config: QueueConfig | None = None) -> None:
        self.clock = clock or SystemClock()
        self.config = config or QueueConfig()
        self.store = SQLiteStore(db_path)
        self.store.initialize()

    @staticmethod
    def _dump(value: Any) -> str:
        try:
            return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ChronosError("value must be valid finite JSON") from exc

    def _bounded_json(self, value: Any, limit: int, field: str) -> str:
        encoded = self._dump(value)
        if len(encoded.encode("utf-8")) > limit:
            raise PayloadTooLarge(f"{field} exceeds configured size limit", details={"limit_bytes": limit})
        return encoded

    def _validate_queue(self, value: str) -> str:
        if not isinstance(value, str) or not value or len(value) > self.config.max_queue_length:
            raise ChronosError(f"queue must be 1..{self.config.max_queue_length} characters")
        if any(ord(ch) < 32 for ch in value):
            raise ChronosError("queue contains control characters")
        return value

    def _validate_tags(self, tags: Sequence[str] | None) -> list[str]:
        values = sorted(set(tags or []))
        if len(values) > self.config.max_tags:
            raise ChronosError(f"at most {self.config.max_tags} tags are allowed")
        for tag in values:
            if not isinstance(tag, str) or not tag or len(tag) > self.config.max_tag_length:
                raise ChronosError("tags must be non-empty bounded strings")
        return values

    def _validate_retry(self, retry: dict[str, Any] | None) -> RetryPolicy:
        retry = retry or {}
        try:
            kind = RetryKind(retry.get("kind", "fixed"))
        except ValueError as exc:
            raise ChronosError("retry.kind must be fixed or exponential") from exc
        delay = retry.get("delay_ms", 1000)
        max_delay = retry.get("max_delay_ms", max(delay if isinstance(delay, int) else 0, 86_400_000))
        if not isinstance(delay, int) or isinstance(delay, bool) or delay < 0 or delay > self.config.max_retry_delay_ms:
            raise ChronosError("retry.delay_ms is out of range")
        if not isinstance(max_delay, int) or isinstance(max_delay, bool) or max_delay < delay or max_delay > self.config.max_retry_delay_ms:
            raise ChronosError("retry.max_delay_ms is out of range")
        return RetryPolicy(kind=kind, delay_ms=delay, max_delay_ms=max_delay)

    @staticmethod
    def _fingerprint(value: dict[str, Any]) -> str:
        blob = json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
        return hashlib.sha256(blob).hexdigest()

    @staticmethod
    def _metric(conn: sqlite3.Connection, name: str, amount: int = 1) -> None:
        conn.execute("UPDATE metrics_counters SET value=value+? WHERE name=?", (amount, name))

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        job_id: str,
        now: int,
        event_type: str,
        before: JobState | str | None,
        after: JobState | str | None,
        details: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO job_events(job_id,at_us,event_type,from_state,to_state,details_json) VALUES(?,?,?,?,?,?)",
            (job_id, now, event_type, str(before) if before else None, str(after) if after else None,
             json.dumps(details or {}, separators=(",", ":"), sort_keys=True)),
        )

    @staticmethod
    def _row_to_job(row: sqlite3.Row, *, deduplicated: bool = False) -> dict[str, Any]:
        state = row["state"]
        lease = None
        if state == JobState.LEASED:
            lease = {
                "owner": row["lease_owner"],
                "token": row["lease_token"],
                "generation": row["lease_generation"],
                "expires_at": us_to_rfc3339(row["lease_expires_at_us"]),
                "expires_at_us": row["lease_expires_at_us"],
            }
        return {
            "id": row["id"],
            "seq": row["seq"],
            "queue": row["queue_name"],
            "created_at": us_to_rfc3339(row["created_at_us"]),
            "created_at_us": row["created_at_us"],
            "scheduled_at": us_to_rfc3339(row["scheduled_at_us"]),
            "scheduled_at_us": row["scheduled_at_us"],
            "payload": json.loads(row["payload_json"]),
            "state": state,
            "attempt_count": row["attempt_count"],
            "max_attempts": row["max_attempts"],
            "lease": lease,
            "retry": {
                "kind": row["retry_kind"],
                "delay_ms": row["retry_delay_ms"],
                "max_delay_ms": row["retry_max_delay_ms"],
            },
            "priority": row["priority"],
            "tags": json.loads(row["tags_json"]),
            "dedupe_key": row["dedupe_key"],
            "result": json.loads(row["result_json"]) if row["result_json"] is not None else None,
            "error": json.loads(row["error_json"]) if row["error_json"] is not None else None,
            "terminal_at": us_to_rfc3339(row["terminal_at_us"]),
            "terminal_at_us": row["terminal_at_us"],
            "deduplicated": deduplicated,
        }

