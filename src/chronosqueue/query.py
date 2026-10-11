from __future__ import annotations

import base64
import contextlib
import json
import sqlite3
from typing import Any, Sequence

from .errors import ChronosError, Conflict, NotFound
from .migration import LATEST_VERSION, current_version
from .models import JobState


class QueryMixin:
    def get_job(self, job_id: str) -> dict[str, Any]:
        self.maintenance()
        with contextlib.closing(self.store.connect()) as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFound("job not found")
        return self._row_to_job(row)

    @staticmethod
    def _cursor_encode(data: dict[str, Any]) -> str:
        raw = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _cursor_decode(value: str) -> dict[str, Any]:
        try:
            raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
            data = json.loads(raw)
            if not isinstance(data, dict) or data.get("v") != 1:
                raise ValueError
            return data
        except Exception as exc:
            raise ChronosError("malformed cursor") from exc

    def list_jobs(self, *, queue: str | None = None, state: str | None = None, tags: Sequence[str] | None = None,
                  created_after_us: int | None = None, created_before_us: int | None = None,
                  min_priority: int | None = None, max_priority: int | None = None, worker_id: str | None = None,
                  limit: int = 50, cursor: str | None = None) -> dict[str, Any]:
        if not isinstance(limit, int) or not 1 <= limit <= self.config.page_limit_max:
            raise ChronosError(f"limit must be 1..{self.config.page_limit_max}")
        if queue is not None:
            queue = self._validate_queue(queue)
        if state is not None:
            try:
                JobState(state)
            except ValueError as exc:
                raise ChronosError("invalid state filter") from exc
        tags_v = self._validate_tags(tags)
        filters = {"queue": queue, "state": state, "tags": tags_v, "created_after_us": created_after_us,
                   "created_before_us": created_before_us, "min_priority": min_priority, "max_priority": max_priority,
                   "worker_id": worker_id}
        filter_hash = self._fingerprint(filters)
        self.maintenance()
        with contextlib.closing(self.store.connect()) as conn:
            if cursor:
                cur = self._cursor_decode(cursor)
                if cur.get("f") != filter_hash:
                    raise ChronosError("cursor does not match current filters")
                upper, last = int(cur["u"]), int(cur["l"])
            else:
                upper = int(conn.execute("SELECT COALESCE(MAX(seq),0) FROM jobs").fetchone()[0])
                last = upper + 1
            where = ["j.seq<=?", "j.seq<?"]
            params: list[Any] = [upper, last]
            if queue is not None:
                where.append("j.queue_name=?"); params.append(queue)
            if state is not None:
                where.append("j.state=?"); params.append(state)
            if created_after_us is not None:
                where.append("j.created_at_us>=?"); params.append(created_after_us)
            if created_before_us is not None:
                where.append("j.created_at_us<?"); params.append(created_before_us)
            if min_priority is not None:
                where.append("j.priority>=?"); params.append(min_priority)
            if max_priority is not None:
                where.append("j.priority<=?"); params.append(max_priority)
            if worker_id is not None:
                where.append("j.lease_owner=?"); params.append(worker_id)
            for tag in tags_v:
                where.append("EXISTS (SELECT 1 FROM job_tags t WHERE t.job_id=j.id AND t.tag=?)")
                params.append(tag)
            rows = conn.execute(
                f"SELECT j.* FROM jobs j WHERE {' AND '.join(where)} ORDER BY j.seq DESC LIMIT ?",
                (*params, limit + 1),
            ).fetchall()
        page = rows[:limit]
        next_cursor = None
        if len(rows) > limit and page:
            next_cursor = self._cursor_encode({"v": 1, "u": upper, "l": page[-1]["seq"], "f": filter_hash})
        return {"items": [self._row_to_job(r) for r in page], "next_cursor": next_cursor, "snapshot_max_seq": upper}

    def leases(self) -> list[dict[str, Any]]:
        self.maintenance()
        now = self.clock.now_us()
        with contextlib.closing(self.store.connect()) as conn:
            rows = conn.execute("SELECT * FROM jobs WHERE state='leased' AND lease_expires_at_us>? ORDER BY lease_expires_at_us,seq", (now,)).fetchall()
        return [self._row_to_job(r) for r in rows]

    def queue_stats(self) -> list[dict[str, Any]]:
        self.maintenance()
        with contextlib.closing(self.store.connect()) as conn:
            rows = conn.execute("SELECT queue_name,state,COUNT(*) AS n FROM jobs GROUP BY queue_name,state ORDER BY queue_name,state").fetchall()
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            q = grouped.setdefault(row["queue_name"], {"queue": row["queue_name"], "states": {}, "depth": 0})
            q["states"][row["state"]] = row["n"]
            if row["state"] in {JobState.READY, JobState.SCHEDULED, JobState.LEASED}:
                q["depth"] += row["n"]
        return list(grouped.values())

    def purge(self, *, terminal_before_us: int, limit: int = 1000) -> dict[str, int]:
        if not isinstance(terminal_before_us, int) or terminal_before_us < 0:
            raise ChronosError("terminal_before_us must be non-negative")
        if not isinstance(limit, int) or not 1 <= limit <= 10_000:
            raise ChronosError("purge limit must be 1..10000")
        with self.store.tx() as conn:
            ids = [r["id"] for r in conn.execute(
                """SELECT id FROM jobs WHERE state IN ('succeeded','dead-lettered','cancelled')
                   AND terminal_at_us IS NOT NULL AND terminal_at_us<? ORDER BY terminal_at_us,seq LIMIT ?""",
                (terminal_before_us, limit),
            ).fetchall()]
            if ids:
                conn.executemany("DELETE FROM jobs WHERE id=?", [(jid,) for jid in ids])
            return {"purged": len(ids)}

    def readiness(self) -> tuple[bool, str]:
        try:
            with self.store.tx() as conn:
                if current_version(conn) != LATEST_VERSION:
                    return False, "schema version mismatch"
                conn.execute("UPDATE schema_meta SET value=value WHERE key='health_sentinel'")
            return True, "ready"
        except Exception:
            return False, "storage unavailable or schema incompatible"

    def metrics_text(self) -> str:
        self.maintenance()
        with contextlib.closing(self.store.connect()) as conn:
            counters = {r["name"]: r["value"] for r in conn.execute("SELECT name,value FROM metrics_counters")}
            states = conn.execute("SELECT queue_name,state,COUNT(*) AS n FROM jobs GROUP BY queue_name,state").fetchall()
        lines = ["# TYPE chronosqueue_events_total counter"]
        for name, value in sorted(counters.items()):
            if name in {"claim_latency_us_total", "claim_requests"}:
                continue
            lines.append(f'chronosqueue_events_total{{event="{name}"}} {value}')
        lines.append("# TYPE chronosqueue_claim_latency_microseconds_total counter")
        lines.append(f"chronosqueue_claim_latency_microseconds_total {counters.get('claim_latency_us_total', 0)}")
        lines.append("# TYPE chronosqueue_claim_requests_total counter")
        lines.append(f"chronosqueue_claim_requests_total {counters.get('claim_requests', 0)}")
        lines.append("# TYPE chronosqueue_jobs gauge")
        for row in states:
            q = row["queue_name"].replace('\\', '\\\\').replace('"', '\\"')
            lines.append(f'chronosqueue_jobs{{queue="{q}",state="{row["state"]}"}} {row["n"]}')
        return "\n".join(lines) + "\n"

    def audit_events(self, job_id: str) -> list[dict[str, Any]]:
        with contextlib.closing(self.store.connect()) as conn:
            rows = conn.execute("SELECT * FROM job_events WHERE job_id=? ORDER BY event_id", (job_id,)).fetchall()
        return [{"event_id": r["event_id"], "at_us": r["at_us"], "event_type": r["event_type"],
                 "from_state": r["from_state"], "to_state": r["to_state"], "details": json.loads(r["details_json"])} for r in rows]
