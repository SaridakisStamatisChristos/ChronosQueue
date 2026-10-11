from __future__ import annotations

import json
import logging
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .clock import parse_timestamp_us
from .errors import ChronosError, NotFound, StorageUnavailable, ValidationError
from .store import Store

LOG = logging.getLogger("chronosqueue")
MAX_REQUEST_BYTES = 2 * 1024 * 1024

ROUTE_SPEC = {
    ("POST", "/v1/jobs"), ("GET", "/v1/jobs"), ("GET", "/v1/jobs/{id}"),
    ("POST", "/v1/jobs/{id}/cancel"), ("POST", "/v1/jobs/{id}/requeue"),
    ("POST", "/v1/worker/claim"), ("POST", "/v1/worker/{id}/heartbeat"),
    ("POST", "/v1/worker/{id}/complete"), ("POST", "/v1/worker/{id}/fail"),
    ("GET", "/v1/queues"), ("GET", "/v1/leases"),
    ("GET", "/health/live"), ("GET", "/health/ready"), ("GET", "/metrics"),
    ("POST", "/v1/admin/purge"), ("POST", "/v1/admin/requeue"),
}


class Handler(BaseHTTPRequestHandler):
    server_version = "ChronosQueue/0.1"

    @property
    def store(self) -> Store:
        return self.server.store  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.info("http", extra={"remote": self.client_address[0], "request": fmt % args})

    def _json_body(self) -> dict[str, Any]:
        raw_len = self.headers.get("Content-Length")
        if raw_len is None:
            return {}
        try:
            length = int(raw_len)
        except ValueError as exc:
            raise ValidationError("invalid Content-Length") from exc
        if length < 0 or length > MAX_REQUEST_BYTES:
            raise ValidationError("request body too large")
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValidationError("malformed JSON") from exc
        if not isinstance(obj, dict):
            raise ValidationError("JSON body must be an object")
        return obj

    def _send_json(self, status: int, obj: Any) -> None:
        data = json.dumps(obj, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _error(self, exc: Exception) -> None:
        if isinstance(exc, StorageUnavailable):
            self._send_json(exc.status, {"error": exc.code, "message": "persistent storage is unavailable"})
        elif isinstance(exc, ChronosError):
            self._send_json(exc.status, {"error": exc.code, "message": str(exc)})
        elif isinstance(exc, (ValueError, TypeError, KeyError)):
            self._send_json(422, {"error": "validation_error", "message": "invalid request input"})
        else:
            LOG.exception("unhandled request error")
            self._send_json(500, {"error": "internal_error", "message": "internal server error"})

    def do_GET(self) -> None:
        started = time.perf_counter()
        try:
            self._do_get()
        except Exception as exc:
            self._error(exc)
        finally:
            self.store._inc("http_requests_total")
            self.store._inc("http_request_microseconds_total", int((time.perf_counter() - started) * 1_000_000))

    def _do_get(self) -> None:
        u = urlparse(self.path)
        if u.path == "/health/live":
            return self._send_json(200, {"status": "alive"})
        if u.path == "/health/ready":
            ready = self.store.ready()
            return self._send_json(200 if ready else 503, {"status": "ready" if ready else "not_ready"})
        if u.path == "/metrics":
            metrics = self.store.metrics()
            lines = [f"chronosqueue_{k} {v}" for k, v in sorted(metrics.items())]
            for q in self.store.queues():
                label = str(q["queue"]).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
                lines.append(f'chronosqueue_queue_depth{{queue="{label}"}} {q["total"]}')
            data = ("\n".join(lines) + "\n").encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            return self.wfile.write(data)
        if u.path == "/v1/queues":
            return self._send_json(200, {"items": self.store.queues()})
        if u.path == "/v1/leases":
            return self._send_json(200, {"items": self.store.leases()})
        if u.path == "/v1/jobs":
            q = parse_qs(u.query)
            kwargs: dict[str, Any] = {}
            for key in ("queue", "state", "tag", "cursor", "lease_owner"):
                if key in q:
                    kwargs[key] = q[key][0]
            if "created_after" in q:
                kwargs["created_after_us"] = parse_timestamp_us(q["created_after"][0])
            if "created_before" in q:
                kwargs["created_before_us"] = parse_timestamp_us(q["created_before"][0])
            if "priority" in q:
                kwargs["priority"] = int(q["priority"][0])
            if "limit" in q:
                kwargs["limit"] = int(q["limit"][0])
            return self._send_json(200, self.store.list_jobs(**kwargs))
        prefix = "/v1/jobs/"
        if u.path.startswith(prefix) and "/" not in u.path[len(prefix):]:
            return self._send_json(200, self.store.get(u.path[len(prefix):]))
        raise NotFound("route not found")

    def do_POST(self) -> None:
        started = time.perf_counter()
        try:
            self._do_post()
        except Exception as exc:
            self._error(exc)
        finally:
            self.store._inc("http_requests_total")
            self.store._inc("http_request_microseconds_total", int((time.perf_counter() - started) * 1_000_000))

    def _do_post(self) -> None:
        u = urlparse(self.path)
        body = self._json_body()
        if u.path == "/v1/jobs":
            scheduled = parse_timestamp_us(body["scheduled_at"]) if body.get("scheduled_at") else None
            if scheduled is None and body.get("delay_ms") is not None:
                delay_ms = int(body["delay_ms"])
                if delay_ms < 0:
                    raise ValidationError("delay_ms must be non-negative")
                scheduled = self.store.clock.now_us() + delay_ms * 1000
            retry = body.get("retry", {})
            if not isinstance(retry, dict):
                raise ValidationError("retry must be an object")
            job, created = self.store.submit(
                queue=body.get("queue", "default"), payload=body.get("payload"), scheduled_at_us=scheduled,
                priority=int(body.get("priority", 0)), tags=body.get("tags") or [], dedupe_key=body.get("idempotency_key"),
                max_attempts=int(body.get("max_attempts", 3)), retry_kind=retry.get("kind", "fixed"),
                retry_delay_us=int(retry.get("delay_ms", 1000)) * 1000,
            )
            return self._send_json(201 if created else 200, {"job": job, "created": created})
        if u.path == "/v1/worker/claim":
            claim = self.store.claim(worker_id=body.get("worker_id", ""), queues=body.get("queues") or [], lease_us=int(body.get("lease_ms", 30000)) * 1000)
            if claim is None:
                self.send_response(204)
                self.end_headers()
                return None
            return self._send_json(200, {"job": claim.job, "lease_token": claim.lease_token})
        if u.path == "/v1/admin/purge":
            if not body.get("older_than"):
                raise ValidationError("older_than is required")
            count = self.store.purge_terminal(older_than_us=parse_timestamp_us(body["older_than"]), limit=int(body.get("limit", 1000)))
            return self._send_json(200, {"purged": count})
        if u.path == "/v1/admin/requeue":
            ids = body.get("job_ids") or []
            scheduled = parse_timestamp_us(body["scheduled_at"]) if body.get("scheduled_at") else None
            return self._send_json(200, {"items": self.store.requeue_many(ids, scheduled_at_us=scheduled)})
        parts = [p for p in u.path.split("/") if p]
        if len(parts) == 4 and parts[:2] == ["v1", "jobs"] and parts[3] in {"cancel", "requeue"}:
            jid, action = parts[2], parts[3]
            if action == "cancel":
                job, changed = self.store.cancel(jid)
                return self._send_json(200, {"job": job, "changed": changed})
            scheduled = parse_timestamp_us(body["scheduled_at"]) if body.get("scheduled_at") else None
            return self._send_json(200, {"job": self.store.requeue(jid, scheduled_at_us=scheduled)})
        if len(parts) == 4 and parts[:2] == ["v1", "worker"] and parts[3] in {"heartbeat", "complete", "fail"}:
            worker_id, action = parts[2], parts[3]
            jid = body.get("job_id", "")
            token = body.get("lease_token", "")
            if action == "heartbeat":
                job = self.store.heartbeat(job_id=jid, worker_id=worker_id, lease_token=token, lease_us=int(body.get("lease_ms", 30000)) * 1000)
                return self._send_json(200, {"job": job})
            if action == "complete":
                job, changed = self.store.complete(job_id=jid, worker_id=worker_id, lease_token=token, result=body.get("result"))
                return self._send_json(200, {"job": job, "changed": changed})
            job, changed = self.store.fail(job_id=jid, worker_id=worker_id, lease_token=token, error=body.get("error"))
            return self._send_json(200, {"job": job, "changed": changed})
        raise NotFound("route not found")


def serve(store: Store, host: str = "127.0.0.1", port: int = 8080) -> None:
    server = ThreadingHTTPServer((host, port), Handler)
    server.store = store  # type: ignore[attr-defined]
    LOG.info("server_started", extra={"host": host, "port": port})
    server.serve_forever()
