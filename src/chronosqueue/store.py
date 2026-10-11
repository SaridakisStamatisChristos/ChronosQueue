from __future__ import annotations

import base64
import json
import os
import secrets
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .clock import Clock, SystemClock, format_timestamp_us
from .errors import Conflict, InvalidTransition, NotFound, StaleLease, StorageUnavailable, ValidationError
from .migrations import CURRENT_VERSION, configure, migrate, version
from .state import ensure_transition

TERMINAL = {"succeeded", "dead-lettered", "cancelled"}
MAX_PAYLOAD_BYTES = 1_048_576
MAX_LEASE_US = 24 * 60 * 60 * 1_000_000
MAX_RETRY_DELAY_US = 30 * 24 * 60 * 60 * 1_000_000


@dataclass(frozen=True)
class Claim:
    job: dict[str, Any]
    lease_token: str


class Store:
    def __init__(self, db_path: str, *, clock: Clock | None = None, payload_limit: int = MAX_PAYLOAD_BYTES, fault_hook: Callable[[str], None] | None = None) -> None:
        self.db_path = db_path
        self.clock = clock or SystemClock()
        self.payload_limit = payload_limit
        self._fault_hook = fault_hook
        self._metrics_lock = threading.Lock()
        self._metrics: dict[str, int] = {}

    def _connect(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(self.db_path, timeout=5, isolation_level=None, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            configure(conn)
            return conn
        except sqlite3.Error as exc:
            raise StorageUnavailable(str(exc)) from exc

    def migrate(self, target: int = CURRENT_VERSION, *, allow_destructive: bool = False) -> None:
        path = Path(self.db_path)
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            migrate(conn, target, allow_destructive=allow_destructive)
        finally:
            conn.close()

    @contextmanager
    def _tx(self):
        conn = self._connect()
        try:
            if version(conn) != CURRENT_VERSION:
                raise StorageUnavailable(f"schema version {version(conn)} != required {CURRENT_VERSION}")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _fault(self, point: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)

    def _inc(self, key: str, n: int = 1) -> None:
        with self._metrics_lock:
            self._metrics[key] = self._metrics.get(key, 0) + n

    def metrics(self) -> dict[str, int]:
        with self._metrics_lock:
            return dict(self._metrics)

    @staticmethod
    def _json(value: Any) -> str:
        try:
            return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValidationError("value is not valid JSON") from exc

    @staticmethod
    def _decode_json(value: str | None) -> Any:
        return None if value is None else json.loads(value)

    def _row(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        for key in ("payload_json", "tags_json", "result_json", "error_json"):
            raw = d.pop(key)
            d[key[:-5]] = self._decode_json(raw)
        for key in ("created_at_us", "scheduled_at_us", "available_at_us", "lease_expires_at_us", "terminal_at_us", "updated_at_us"):
            d[key[:-3]] = format_timestamp_us(d.pop(key))
        d.pop("lease_token", None)
        return d

    def _audit(self, conn: sqlite3.Connection, job_id: str | None, event: str, now: int, detail: dict[str, Any] | None = None) -> None:
        conn.execute(
            "INSERT INTO audit_events(job_id,event,at_us,detail_json) VALUES(?,?,?,?)",
            (job_id, event, now, self._json(detail or {})),
        )

    def ready(self) -> bool:
        try:
            conn = self._connect()
            try:
                if version(conn) != CURRENT_VERSION:
                    return False
                bad = conn.execute(
                    """SELECT 1 FROM jobs WHERE
                    (state='leased' AND (lease_owner IS NULL OR lease_token IS NULL OR lease_expires_at_us IS NULL)) OR
                    (state!='leased' AND (lease_owner IS NOT NULL OR lease_token IS NOT NULL OR lease_expires_at_us IS NOT NULL)) OR
                    (state IN ('succeeded','dead-lettered','cancelled') AND terminal_at_us IS NULL) OR
                    (state NOT IN ('succeeded','dead-lettered','cancelled') AND terminal_at_us IS NOT NULL)
                    LIMIT 1"""
                ).fetchone()
                return bad is None
            finally:
                conn.close()
        except Exception:
            return False

    def submit(
        self,
        *,
        queue: str,
        payload: Any,
        scheduled_at_us: int | None = None,
        priority: int = 0,
        tags: list[str] | None = None,
        dedupe_key: str | None = None,
        max_attempts: int = 3,
        retry_kind: str = "fixed",
        retry_delay_us: int = 1_000_000,
        job_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        if not queue or len(queue) > 128:
            raise ValidationError("queue must be 1..128 characters")
        if retry_kind not in {"fixed", "exponential"}:
            raise ValidationError("retry_kind must be fixed or exponential")
        if max_attempts < 1 or max_attempts > 10_000:
            raise ValidationError("max_attempts out of range")
        if retry_delay_us < 0 or retry_delay_us > MAX_RETRY_DELAY_US:
            raise ValidationError("retry_delay out of range")
        if priority < -2_147_483_648 or priority > 2_147_483_647:
            raise ValidationError("priority out of range")
        tags = tags or []
        if not isinstance(tags, list):
            raise ValidationError("tags must be a JSON array")
        if len(tags) > 64 or any((not isinstance(t, str) or not t or len(t) > 128) for t in tags):
            raise ValidationError("invalid tags")
        if dedupe_key is not None and (not dedupe_key or len(dedupe_key) > 256):
            raise ValidationError("invalid dedupe key")
        payload_json = self._json(payload)
        if len(payload_json.encode("utf-8")) > self.payload_limit:
            raise ValidationError("payload exceeds configured size limit")
        now = self.clock.now_us()
        due = now if scheduled_at_us is None else scheduled_at_us
        if not isinstance(due, int) or due < -(1 << 63) or due > (1 << 63) - 1:
            raise ValidationError("scheduled timestamp out of range")
        state = "ready" if due <= now else "scheduled"
        jid = job_id or str(uuid.uuid4())
        try:
            uuid.UUID(jid)
        except ValueError as exc:
            raise ValidationError("job id must be UUID") from exc
        with self._tx() as conn:
            if dedupe_key is not None:
                existing = conn.execute("SELECT * FROM jobs WHERE queue=? AND dedupe_key=?", (queue, dedupe_key)).fetchone()
                if existing is not None:
                    return self._row(existing), False
            try:
                conn.execute(
                    """INSERT INTO jobs(id,queue,created_at_us,scheduled_at_us,available_at_us,payload_json,state,
                    max_attempts,retry_kind,retry_delay_us,priority,tags_json,dedupe_key,updated_at_us)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (jid, queue, now, due, due, payload_json, state, max_attempts, retry_kind, retry_delay_us, priority, self._json(tags), dedupe_key, now),
                )
            except sqlite3.IntegrityError as exc:
                if dedupe_key is not None:
                    existing = conn.execute("SELECT * FROM jobs WHERE queue=? AND dedupe_key=?", (queue, dedupe_key)).fetchone()
                    if existing is not None:
                        return self._row(existing), False
                raise Conflict("job already exists") from exc
            self._audit(conn, jid, "submitted", now, {"state": state})
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
            self._fault("submit.before_commit")
        self._inc("jobs_submitted_total")
        return self._row(row), True

    def _promote_due(self, conn: sqlite3.Connection, now: int) -> int:
        ensure_transition("scheduled", "ready")
        ensure_transition("failed", "ready")
        cur = conn.execute(
            "UPDATE jobs SET state='ready',updated_at_us=? WHERE state IN ('scheduled','failed') AND available_at_us<=?",
            (now, now),
        )
        return cur.rowcount

    def _recover_expired(self, conn: sqlite3.Connection, now: int) -> int:
        rows = conn.execute(
            "SELECT id,attempt_count,max_attempts FROM jobs WHERE state='leased' AND lease_expires_at_us<=? ORDER BY seq",
            (now,),
        ).fetchall()
        for row in rows:
            terminal = row["attempt_count"] >= row["max_attempts"]
            new_state = "dead-lettered" if terminal else "ready"
            ensure_transition("leased", new_state)
            conn.execute(
                """UPDATE jobs SET state=?,available_at_us=?,lease_owner=NULL,lease_token=NULL,lease_expires_at_us=NULL,
                terminal_at_us=?,updated_at_us=? WHERE id=? AND state='leased' AND lease_expires_at_us<=?""",
                (new_state, now, now if terminal else None, now, row["id"], now),
            )
            self._audit(conn, row["id"], "lease_expired", now, {"state": new_state})
        if rows:
            self._inc("lease_expirations_total", len(rows))
        return len(rows)

    def claim(self, *, worker_id: str, queues: Iterable[str], lease_us: int) -> Claim | None:
        if isinstance(queues, (str, bytes)):
            raise ValidationError("queues must be an array")
        try:
            queue_list = list(dict.fromkeys(queues))
        except (TypeError, ValueError) as exc:
            raise ValidationError("queues must be an array of strings") from exc
        if not worker_id or not isinstance(worker_id, str) or len(worker_id) > 128:
            raise ValidationError("worker_id must be 1..128 characters")
        if not queue_list or any(not isinstance(q, str) or not q or len(q) > 128 for q in queue_list):
            raise ValidationError("at least one valid queue is required")
        if lease_us <= 0 or lease_us > MAX_LEASE_US:
            raise ValidationError("lease duration out of range")
        now = self.clock.now_us()
        token = secrets.token_urlsafe(32)
        placeholders = ",".join("?" for _ in queue_list)
        with self._tx() as conn:
            self._recover_expired(conn, now)
            self._promote_due(conn, now)
            row = conn.execute(
                f"""SELECT * FROM jobs WHERE state='ready' AND available_at_us<=? AND queue IN ({placeholders})
                ORDER BY priority DESC, available_at_us ASC, seq ASC LIMIT 1""",
                [now, *queue_list],
            ).fetchone()
            if row is None:
                return None
            expires = now + lease_us
            ensure_transition("ready", "leased")
            cur = conn.execute(
                """UPDATE jobs SET state='leased',attempt_count=attempt_count+1,lease_owner=?,lease_token=?,
                lease_generation=lease_generation+1,lease_expires_at_us=?,updated_at_us=? WHERE id=? AND state='ready'""",
                (worker_id, token, expires, now, row["id"]),
            )
            if cur.rowcount != 1:
                raise Conflict("claim lost concurrent race")
            self._audit(conn, row["id"], "claimed", now, {"worker_id": worker_id, "expires_at_us": expires})
            leased = conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            self._fault("claim.before_commit")
        self._inc("jobs_leased_total")
        job = self._row(leased)
        return Claim(job=job, lease_token=token)

    def _active_lease(self, conn: sqlite3.Connection, job_id: str, worker_id: str, token: str, now: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFound("job not found")
        if row["state"] != "leased" or row["lease_owner"] != worker_id or row["lease_token"] != token or row["lease_expires_at_us"] <= now:
            self._inc("stale_lease_attempts_total")
            raise StaleLease("lease is stale or not owned by worker")
        return row

    def heartbeat(self, *, job_id: str, worker_id: str, lease_token: str, lease_us: int) -> dict[str, Any]:
        if lease_us <= 0 or lease_us > MAX_LEASE_US:
            raise ValidationError("lease duration out of range")
        now = self.clock.now_us()
        with self._tx() as conn:
            self._recover_expired(conn, now)
            row = self._active_lease(conn, job_id, worker_id, lease_token, now)
            expires = now + lease_us
            conn.execute("UPDATE jobs SET lease_expires_at_us=?,updated_at_us=? WHERE id=?", (expires, now, job_id))
            self._audit(conn, job_id, "heartbeat", now, {"generation": row["lease_generation"], "expires_at_us": expires})
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            self._fault("heartbeat.before_commit")
        return self._row(out)

    def complete(self, *, job_id: str, worker_id: str, lease_token: str, result: Any = None) -> tuple[dict[str, Any], bool]:
        now = self.clock.now_us()
        with self._tx() as conn:
            receipt = conn.execute("SELECT * FROM lease_receipts WHERE lease_token=?", (lease_token,)).fetchone()
            if receipt is not None:
                if receipt["job_id"] != job_id or receipt["worker_id"] != worker_id or receipt["outcome"] != "succeeded":
                    raise StaleLease("lease token was already consumed for a different outcome")
                row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                return self._row(row), False
            self._recover_expired(conn, now)
            row = self._active_lease(conn, job_id, worker_id, lease_token, now)
            conn.execute(
                """UPDATE jobs SET state='succeeded',result_json=?,error_json=NULL,terminal_at_us=?,lease_owner=NULL,
                lease_token=NULL,lease_expires_at_us=NULL,updated_at_us=? WHERE id=?""",
                (self._json(result), now, now, job_id),
            )
            conn.execute(
                "INSERT INTO lease_receipts(lease_token,job_id,worker_id,generation,outcome,committed_at_us) VALUES(?,?,?,?,?,?)",
                (lease_token, job_id, worker_id, row["lease_generation"], "succeeded", now),
            )
            self._audit(conn, job_id, "completed", now)
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            self._fault("complete.before_commit")
        self._inc("jobs_succeeded_total")
        return self._row(out), True

    def fail(self, *, job_id: str, worker_id: str, lease_token: str, error: Any = None) -> tuple[dict[str, Any], bool]:
        now = self.clock.now_us()
        with self._tx() as conn:
            receipt = conn.execute("SELECT * FROM lease_receipts WHERE lease_token=?", (lease_token,)).fetchone()
            if receipt is not None:
                if receipt["job_id"] != job_id or receipt["worker_id"] != worker_id or receipt["outcome"] not in {"failed", "dead-lettered"}:
                    raise StaleLease("lease token was already consumed for a different outcome")
                row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                return self._row(row), False
            self._recover_expired(conn, now)
            row = self._active_lease(conn, job_id, worker_id, lease_token, now)
            error_json = self._json(error)
            if len(error_json.encode("utf-8")) > self.payload_limit:
                raise ValidationError("error exceeds configured size limit")
            if row["attempt_count"] >= row["max_attempts"]:
                state = "dead-lettered"
                available = now
                terminal = now
                outcome = "dead-lettered"
            else:
                exponent = max(0, row["attempt_count"] - 1) if row["retry_kind"] == "exponential" else 0
                delay = row["retry_delay_us"] * (1 << min(exponent, 30))
                delay = min(delay, MAX_RETRY_DELAY_US)
                available = now + delay
                state = "failed"
                terminal = None
                outcome = "failed"
            ensure_transition("leased", state)
            conn.execute(
                """UPDATE jobs SET state=?,available_at_us=?,error_json=?,terminal_at_us=?,lease_owner=NULL,
                lease_token=NULL,lease_expires_at_us=NULL,updated_at_us=? WHERE id=?""",
                (state, available, error_json, terminal, now, job_id),
            )
            conn.execute(
                "INSERT INTO lease_receipts(lease_token,job_id,worker_id,generation,outcome,committed_at_us) VALUES(?,?,?,?,?,?)",
                (lease_token, job_id, worker_id, row["lease_generation"], outcome, now),
            )
            self._audit(conn, job_id, "failed", now, {"state": state, "available_at_us": available})
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            self._fault("fail.before_commit")
        self._inc("jobs_failed_total")
        if state == "dead-lettered":
            self._inc("jobs_dead_lettered_total")
        else:
            self._inc("jobs_retried_total")
        return self._row(out), True

    def cancel(self, job_id: str) -> tuple[dict[str, Any], bool]:
        now = self.clock.now_us()
        with self._tx() as conn:
            self._recover_expired(conn, now)
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound("job not found")
            if row["state"] == "cancelled":
                return self._row(row), False
            if row["state"] in {"succeeded", "dead-lettered"}:
                raise InvalidTransition(f"cannot cancel terminal state {row['state']}")
            ensure_transition(row["state"], "cancelled")
            conn.execute(
                """UPDATE jobs SET state='cancelled',terminal_at_us=?,lease_owner=NULL,lease_token=NULL,
                lease_expires_at_us=NULL,updated_at_us=? WHERE id=?""",
                (now, now, job_id),
            )
            self._audit(conn, job_id, "cancelled", now, {"from": row["state"]})
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            self._fault("cancel.before_commit")
        self._inc("jobs_cancelled_total")
        return self._row(out), True

    def requeue(self, job_id: str, *, scheduled_at_us: int | None = None) -> dict[str, Any]:
        now = self.clock.now_us()
        due = now if scheduled_at_us is None else scheduled_at_us
        new_state = "ready" if due <= now else "scheduled"
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound("job not found")
            if row["state"] not in {"cancelled", "dead-lettered"}:
                raise InvalidTransition("only cancelled or dead-lettered jobs may be explicitly requeued")
            ensure_transition(row["state"], new_state, administrative=True)
            conn.execute(
                """UPDATE jobs SET state=?,scheduled_at_us=?,available_at_us=?,attempt_count=0,lease_owner=NULL,lease_token=NULL,
                lease_expires_at_us=NULL,result_json=NULL,error_json=NULL,terminal_at_us=NULL,updated_at_us=? WHERE id=?""",
                (new_state, due, due, now, job_id),
            )
            self._audit(conn, job_id, "requeued", now, {"from": row["state"], "state": new_state})
            out = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            self._fault("requeue.before_commit")
        return self._row(out)

    def requeue_many(self, job_ids: list[str], *, scheduled_at_us: int | None = None) -> list[dict[str, Any]]:
        if not isinstance(job_ids, list) or not job_ids or len(job_ids) > 1000 or any(not isinstance(j, str) for j in job_ids) or len(set(job_ids)) != len(job_ids):
            raise ValidationError("job_ids must contain 1..1000 unique string IDs")
        now = self.clock.now_us()
        due = now if scheduled_at_us is None else scheduled_at_us
        new_state = "ready" if due <= now else "scheduled"
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE id IN (" + ",".join("?" for _ in job_ids) + ")", job_ids
            ).fetchall()
            by_id = {r["id"]: r for r in rows}
            missing = [jid for jid in job_ids if jid not in by_id]
            if missing:
                raise NotFound(f"jobs not found: {missing[:5]}")
            illegal = [jid for jid in job_ids if by_id[jid]["state"] not in {"cancelled", "dead-lettered"}]
            if illegal:
                raise InvalidTransition(f"jobs not requeueable: {illegal[:5]}")
            for jid in job_ids:
                row = by_id[jid]
                ensure_transition(row["state"], new_state, administrative=True)
                conn.execute(
                    """UPDATE jobs SET state=?,scheduled_at_us=?,available_at_us=?,attempt_count=0,lease_owner=NULL,lease_token=NULL,
                    lease_expires_at_us=NULL,result_json=NULL,error_json=NULL,terminal_at_us=NULL,updated_at_us=? WHERE id=?""",
                    (new_state, due, due, now, jid),
                )
                self._audit(conn, jid, "requeued", now, {"from": row["state"], "state": new_state, "batch": True})
            out = [conn.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone() for jid in job_ids]
            self._fault("requeue_many.before_commit")
        return [self._row(r) for r in out]

    def get(self, job_id: str) -> dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound("job not found")
            return self._row(row)
        finally:
            conn.close()

    @staticmethod
    def _cursor_encode(after: int, ceiling: int) -> str:
        raw = json.dumps({"a": after, "c": ceiling}, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _cursor_decode(cursor: str) -> tuple[int, int]:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            obj = json.loads(base64.urlsafe_b64decode(padded.encode()))
            after, ceiling = int(obj["a"]), int(obj["c"])
            if after < 0 or ceiling < after:
                raise ValueError
            return after, ceiling
        except Exception as exc:
            raise ValidationError("malformed cursor") from exc

    def list_jobs(
        self,
        *,
        queue: str | None = None,
        state: str | None = None,
        tag: str | None = None,
        priority: int | None = None,
        created_after_us: int | None = None,
        created_before_us: int | None = None,
        lease_owner: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        if limit < 1 or limit > 500:
            raise ValidationError("limit must be 1..500")
        conn = self._connect()
        try:
            if cursor:
                after, ceiling = self._cursor_decode(cursor)
            else:
                after = 0
                ceiling = int(conn.execute("SELECT COALESCE(MAX(seq),0) FROM jobs").fetchone()[0])
            clauses = ["seq>?", "seq<=?"]
            args: list[Any] = [after, ceiling]
            for expr, value in (("queue=?", queue), ("state=?", state), ("priority=?", priority), ("lease_owner=?", lease_owner)):
                if value is not None:
                    clauses.append(expr)
                    args.append(value)
            if created_after_us is not None:
                clauses.append("created_at_us>=?")
                args.append(created_after_us)
            if created_before_us is not None:
                clauses.append("created_at_us<=?")
                args.append(created_before_us)
            if tag is not None:
                clauses.append("EXISTS (SELECT 1 FROM json_each(tags_json) WHERE value=?)")
                args.append(tag)
            rows = conn.execute(
                "SELECT * FROM jobs WHERE " + " AND ".join(clauses) + " ORDER BY seq ASC LIMIT ?",
                [*args, limit + 1],
            ).fetchall()
            more = len(rows) > limit
            rows = rows[:limit]
            next_cursor = self._cursor_encode(rows[-1]["seq"], ceiling) if more and rows else None
            return {"items": [self._row(r) for r in rows], "next_cursor": next_cursor}
        except sqlite3.OperationalError as exc:
            raise StorageUnavailable(str(exc)) from exc
        finally:
            conn.close()

    def queues(self) -> list[dict[str, Any]]:
        now = self.clock.now_us()
        with self._tx() as conn:
            self._recover_expired(conn, now)
            self._promote_due(conn, now)
            rows = conn.execute(
                "SELECT queue,state,COUNT(*) AS count FROM jobs GROUP BY queue,state ORDER BY queue,state"
            ).fetchall()
        acc: dict[str, dict[str, Any]] = {}
        for r in rows:
            acc.setdefault(r["queue"], {"queue": r["queue"], "states": {}, "total": 0})
            acc[r["queue"]]["states"][r["state"]] = r["count"]
            acc[r["queue"]]["total"] += r["count"]
        return list(acc.values())

    def leases(self) -> list[dict[str, Any]]:
        now = self.clock.now_us()
        with self._tx() as conn:
            self._recover_expired(conn, now)
            rows = conn.execute(
                "SELECT id,queue,lease_owner,lease_generation,lease_expires_at_us,attempt_count FROM jobs WHERE state='leased' ORDER BY lease_expires_at_us,seq"
            ).fetchall()
        return [
            {
                "job_id": r["id"], "queue": r["queue"], "lease_owner": r["lease_owner"],
                "lease_generation": r["lease_generation"], "lease_expires_at": format_timestamp_us(r["lease_expires_at_us"]),
                "attempt_count": r["attempt_count"],
            }
            for r in rows
        ]

    def purge_terminal(self, *, older_than_us: int, limit: int = 1000) -> int:
        if limit < 1 or limit > 10_000:
            raise ValidationError("limit out of range")
        with self._tx() as conn:
            ids = [r[0] for r in conn.execute(
                "SELECT id FROM jobs WHERE state IN ('succeeded','dead-lettered','cancelled') AND terminal_at_us<? ORDER BY terminal_at_us,seq LIMIT ?",
                (older_than_us, limit),
            ).fetchall()]
            for jid in ids:
                self._audit(conn, jid, "purged", self.clock.now_us())
                conn.execute("DELETE FROM jobs WHERE id=?", (jid,))
            self._fault("purge.before_commit")
        return len(ids)
