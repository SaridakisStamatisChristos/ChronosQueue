from __future__ import annotations

import json
import logging
import re
import signal
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .clock import rfc3339_to_us
from .errors import ChronosError, PayloadTooLarge
from .service import QueueService

LOG = logging.getLogger("chronosqueue.http")
MAX_HTTP_BODY = 2_200_000

IMPLEMENTED_ROUTES = {
    ("GET", "/health/live"), ("GET", "/health/ready"), ("GET", "/metrics"), ("GET", "/openapi.yaml"),
    ("POST", "/v1/jobs"), ("GET", "/v1/jobs"), ("GET", "/v1/jobs/{id}"),
    ("GET", "/v1/jobs/{id}/events"), ("POST", "/v1/jobs/{id}/cancel"), ("POST", "/v1/jobs/{id}/requeue"),
    ("POST", "/v1/worker/claim"), ("POST", "/v1/worker/{worker_id}/heartbeat"),
    ("POST", "/v1/worker/{worker_id}/complete"), ("POST", "/v1/worker/{worker_id}/fail"),
    ("GET", "/v1/queues"), ("GET", "/v1/leases"), ("POST", "/v1/admin/purge"),
}


class ChronosHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], service: QueueService) -> None:
        self.service = service
        super().__init__(address, ChronosHandler)


class ChronosHandler(BaseHTTPRequestHandler):
    server: ChronosHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.info("http_access", extra={"http": {"client": self.client_address[0], "message": fmt % args}})

    def _json(self, status: int, body: Any, *, request_id: str | None = None) -> None:
        data = json.dumps(body, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if request_id:
            self.send_header("X-Request-ID", request_id)
        self.end_headers()
        self.wfile.write(data)

    def _text(self, status: int, body: str, content_type: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        raw_len = self.headers.get("Content-Length")
        if raw_len is None:
            raise ChronosError("Content-Length is required")
        try:
            length = int(raw_len)
        except ValueError as exc:
            raise ChronosError("invalid Content-Length") from exc
        if length < 0 or length > MAX_HTTP_BODY:
            raise PayloadTooLarge("request body is too large", details={"limit_bytes": MAX_HTTP_BODY})
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ChronosError("malformed JSON") from exc
        if not isinstance(value, dict):
            raise ChronosError("JSON request body must be an object")
        return value

    @staticmethod
    def _one(qs: dict[str, list[str]], key: str) -> str | None:
        values = qs.get(key)
        return values[0] if values else None

    @staticmethod
    def _int(value: str | None, field: str) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError as exc:
            raise ChronosError(f"{field} must be an integer") from exc

    @staticmethod
    def _lease_args(body: dict[str, Any], worker_id: str) -> dict[str, Any]:
        for field in ("job_id", "lease_token", "lease_generation"):
            if field not in body:
                raise ChronosError(f"missing field: {field}")
        return {
            "job_id": body["job_id"],
            "worker_id": worker_id,
            "lease_token": body["lease_token"],
            "lease_generation": body["lease_generation"],
        }

    def _dispatch(self) -> None:
        rid = self.headers.get("X-Request-ID") or str(uuid.uuid4())
        parsed = urlsplit(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query, keep_blank_values=True)
        s = self.server.service

        if self.command == "GET" and path == "/health/live":
            self._json(200, {"status": "live"}, request_id=rid); return
        if self.command == "GET" and path == "/health/ready":
            ok, reason = s.readiness()
            self._json(200 if ok else 503, {"status": "ready" if ok else "not_ready", "reason": reason}, request_id=rid); return
        if self.command == "GET" and path == "/metrics":
            self._text(200, s.metrics_text(), "text/plain; version=0.0.4; charset=utf-8"); return
        if self.command == "GET" and path == "/openapi.yaml":
            text = resources.files("chronosqueue").joinpath("openapi.yaml").read_text(encoding="utf-8")
            self._text(200, text, "application/yaml; charset=utf-8"); return

        if self.command == "POST" and path == "/v1/jobs":
            b = self._body()
            if "payload" not in b:
                raise ChronosError("missing field: payload")
            scheduled = b.get("scheduled_at_us")
            if "scheduled_at" in b:
                if scheduled is not None:
                    raise ChronosError("use scheduled_at or scheduled_at_us, not both")
                try:
                    scheduled = rfc3339_to_us(b["scheduled_at"])
                except (ValueError, TypeError) as exc:
                    raise ChronosError("scheduled_at must be a timezone-aware RFC3339 timestamp") from exc
            job = s.submit(payload=b["payload"], queue=b.get("queue", "default"), scheduled_at_us=scheduled,
                           delay_ms=b.get("delay_ms"), priority=b.get("priority", 0), tags=b.get("tags"),
                           dedupe_key=b.get("dedupe_key"), max_attempts=b.get("max_attempts", 3), retry=b.get("retry"),
                           job_id=b.get("id"))
            self._json(200 if job["deduplicated"] else 201, job, request_id=rid); return

        if self.command == "GET" and path == "/v1/jobs":
            tags = qs.get("tag", [])
            result = s.list_jobs(queue=self._one(qs, "queue"), state=self._one(qs, "state"), tags=tags,
                                 created_after_us=self._int(self._one(qs, "created_after_us"), "created_after_us"),
                                 created_before_us=self._int(self._one(qs, "created_before_us"), "created_before_us"),
                                 min_priority=self._int(self._one(qs, "min_priority"), "min_priority"),
                                 max_priority=self._int(self._one(qs, "max_priority"), "max_priority"),
                                 worker_id=self._one(qs, "worker_id"),
                                 limit=self._int(self._one(qs, "limit"), "limit") or 50,
                                 cursor=self._one(qs, "cursor"))
            self._json(200, result, request_id=rid); return

        m = re.fullmatch(r"/v1/jobs/([0-9a-fA-F-]+)", path)
        if self.command == "GET" and m:
            self._json(200, s.get_job(m.group(1)), request_id=rid); return
        m = re.fullmatch(r"/v1/jobs/([0-9a-fA-F-]+)/events", path)
        if self.command == "GET" and m:
            self._json(200, {"items": s.audit_events(m.group(1))}, request_id=rid); return
        m = re.fullmatch(r"/v1/jobs/([0-9a-fA-F-]+)/cancel", path)
        if self.command == "POST" and m:
            self._body()
            self._json(200, s.cancel(m.group(1)), request_id=rid); return
        m = re.fullmatch(r"/v1/jobs/([0-9a-fA-F-]+)/requeue", path)
        if self.command == "POST" and m:
            b = self._body()
            scheduled = b.get("scheduled_at_us")
            if "scheduled_at" in b:
                if scheduled is not None:
                    raise ChronosError("use scheduled_at or scheduled_at_us, not both")
                try:
                    scheduled = rfc3339_to_us(b["scheduled_at"])
                except (ValueError, TypeError) as exc:
                    raise ChronosError("scheduled_at must be a timezone-aware RFC3339 timestamp") from exc
            self._json(200, s.requeue(m.group(1), scheduled_at_us=scheduled, reset_attempts=b.get("reset_attempts", True)), request_id=rid); return

        if self.command == "POST" and path == "/v1/worker/claim":
            b = self._body()
            job = s.claim(worker_id=b.get("worker_id", ""), queues=b.get("queues"), lease_ms=b.get("lease_ms"))
            self._json(200, {"job": job}, request_id=rid); return

        m = re.fullmatch(r"/v1/worker/([^/]+)/(heartbeat|complete|fail)", path)
        if self.command == "POST" and m:
            worker_id, action = m.group(1), m.group(2)
            b = self._body()
            args = self._lease_args(b, worker_id)
            if action == "heartbeat":
                out = s.heartbeat(**args, lease_ms=b.get("lease_ms"))
            elif action == "complete":
                out = s.complete(**args, result=b.get("result"))
            else:
                out = s.fail(**args, error=b.get("error"))
            self._json(200, out, request_id=rid); return

        if self.command == "GET" and path == "/v1/queues":
            self._json(200, {"items": s.queue_stats()}, request_id=rid); return
        if self.command == "GET" and path == "/v1/leases":
            self._json(200, {"items": s.leases()}, request_id=rid); return
        if self.command == "POST" and path == "/v1/admin/purge":
            b = self._body()
            if "terminal_before_us" not in b:
                raise ChronosError("terminal_before_us is required")
            self._json(200, s.purge(terminal_before_us=b["terminal_before_us"], limit=b.get("limit", 1000)), request_id=rid); return

        self._json(404, {"error": {"code": "not_found", "message": "route not found"}}, request_id=rid)

    def _safe_dispatch(self) -> None:
        try:
            self._dispatch()
        except ChronosError as exc:
            self._json(exc.status, {"error": {"code": exc.code, "message": exc.message, "details": exc.details}})
        except BrokenPipeError:
            LOG.info("client_disconnected")
        except Exception:
            LOG.exception("unhandled_request_error")
            with contextlib.suppress(Exception):
                self._json(500, {"error": {"code": "internal_error", "message": "internal server error"}})

    do_GET = _safe_dispatch
    do_POST = _safe_dispatch


# contextlib import intentionally kept near use to make the top-level surface obvious.
import contextlib  # noqa: E402


def serve(service: QueueService, host: str = "127.0.0.1", port: int = 8080, *, maintenance_interval: float = 0.5) -> None:
    server = ChronosHTTPServer((host, port), service)
    stop = threading.Event()

    def maintenance() -> None:
        while not stop.wait(maintenance_interval):
            try:
                service.maintenance()
            except Exception:
                LOG.exception("maintenance_failed")

    thread = threading.Thread(target=maintenance, name="chronosqueue-maintenance", daemon=True)
    thread.start()

    def shutdown(_signum: int, _frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    old_int = old_term = None
    if threading.current_thread() is threading.main_thread():
        old_int = signal.signal(signal.SIGINT, shutdown)
        old_term = signal.signal(signal.SIGTERM, shutdown)
    LOG.info("server_start", extra={"listen": {"host": host, "port": port}})
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        stop.set()
        server.server_close()
        thread.join(timeout=2)
        if old_int is not None:
            signal.signal(signal.SIGINT, old_int)
        if old_term is not None:
            signal.signal(signal.SIGTERM, old_term)
